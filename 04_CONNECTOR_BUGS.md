# lance-flink Connector 的两个严重 Bug

压测过程中发现 `lance-format/lance-flink` HEAD (`fc3d064`, 2026-01-08) 有两个 **生产级** bug，**不打 patch 整个 connector 根本跑不起来**。

## Bug 1: `Paths.get("s3://...")` 本地 FS 检查永远返回 false

### 位置
[`LanceSink.java#L120`](https://github.com/lance-format/lance-flink/blob/fc3d064ace4bbdbf29e22a489db2c5bf61a36990/src/main/java/org/apache/flink/connector/lance/LanceSink.java#L120)

### 原始代码
```java
// Check if dataset exists
String datasetPath = options.getPath();
if (datasetPath == null || datasetPath.isEmpty()) {
    throw new IllegalArgumentException("Lance dataset path cannot be empty");
}

Path path = Paths.get(datasetPath);
this.datasetExists = Files.exists(path);  // ← BUG
```

### 问题
`java.nio.file.Paths.get("s3://bucket/key")` 不会抛异常（URL scheme 被解析成文件路径），但 `Files.exists(Paths.get("s3://..."))` 永远返回 `false`（因为系统上没有 `s3:` 这个本地文件）。

### 后果
每次 `LanceSink.open()` 都判定 dataset 不存在。之后第一次 `flush()` 走 `FragmentOperation.Overwrite` 而不是 `Append`：

```java
if (!datasetExists) {
    FragmentOperation.Overwrite overwrite = new FragmentOperation.Overwrite(...);
    dataset = overwrite.commit(...);
}
```

**影响**:
- **Parallelism=N 时**，每个 TaskManager subtask 独立运行 `open()` 和第一次 `flush()`
- N 个 subtask 几乎同时 Overwrite，**互相覆盖**
- 只剩一个赢家的数据，其它 (N-1)/N 的数据全丢
- 任何用 `s3://` URL + parallelism > 1 的配置都会静默丢数据

### Patch
```diff
- Path path = Paths.get(datasetPath);
- this.datasetExists = Files.exists(path);
+ this.datasetExists = true; // PATCH: assume exists for S3 paths
```

**约束**: 必须先用 Python/Rust 预先创建 dataset（哪怕是空表），然后 Flink 作业启动。否则 APPEND 会失败（没有 schema）。

### 官方修复状态
[PR #15](https://github.com/lance-format/lance-flink/pull/15) 改用 `Dataset.open()` 做真正的 S3 检查（见 `sink/LanceSinkWriter.java#L256-L272`），但该 PR 仍未合并。

---

## Bug 2: `Append.commit(..., Optional.empty(), ...)` 被 lance-core ≥ 0.23.3 直接拒绝

### 位置
[`LanceSink.java#L188`](https://github.com/lance-format/lance-flink/blob/fc3d064ace4bbdbf29e22a489db2c5bf61a36990/src/main/java/org/apache/flink/connector/lance/LanceSink.java#L188)

### 原始代码
```java
// Append mode
FragmentOperation.Append append = new FragmentOperation.Append(fragments);
dataset = append.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
//                                              ^^^^^^^^^^^^^^^^
//                                              没传 read_version!
```

### 问题

`FragmentOperation.Append.commit()` 的第三个参数是 `Optional<Long> readVersion`，connector HEAD 传的是 `Optional.empty()`。

Lance Rust 层在 [`rust/lance/src/dataset.rs:665`](https://github.com/lance-format/lance/blob/main/rust/lance/src/dataset.rs) 明确要求 append 必须指定 `read_version`：

```
Caused by: java.lang.IllegalArgumentException: 
  Invalid user input: read_version must be specified for this operation, 
  rust/lance/src/dataset.rs:665:21
    at com.lancedb.lance.Dataset.commitAppend(Native Method)
    at com.lancedb.lance.FragmentOperation$Append.commit(FragmentOperation.java:58)
    at org.apache.flink.connector.lance.LanceSink.flush(LanceSink.java:188)
```

### 后果
**HEAD 版本的 LanceSink + 任何 lance-core ≥ 0.23.3 = APPEND 模式完全无法工作**。

- Job 启动后第一次 flush 直接抛异常
- 被包装成 `IOException("Failed to write Lance dataset", e)` 从 `snapshotState()` 抛出
- 第一次 checkpoint 就失败
- 达到 `tolerable-failed-checkpoints=0` 阈值 → job fail

**这意味着 lance-flink HEAD 开箱即用 = 不可用**。

### Patch
```diff
  // Append mode
+ long readVersion;
+ try (Dataset existing = Dataset.open(datasetPath, allocator)) {
+     readVersion = existing.version();
+ }
  FragmentOperation.Append append = new FragmentOperation.Append(fragments);
- dataset = append.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
+ dataset = append.commit(allocator, datasetPath, Optional.of(readVersion), Collections.emptyMap());
```

### Patch 的副作用
每次 flush 都要 `Dataset.open()` 拿 version，而 open 一个有 2000+ manifest version 的 dataset **需要读所有 version 的 metadata**，非常慢：
- 空表: open 耗时 ~100ms
- 2000+ version: open 耗时 **几秒**
- 这直接导致压测中 checkpoint 超时

**这是 connector 设计的根本缺陷**: 即使 patch 修复了正确性，性能仍然拉胯，因为每次 commit 前都要开一次 dataset 去拿 version。正确的做法是**在算子状态里维护当前 version**，只在 recovery 时读一次。PR #15 也没完全解决这个问题。

### 官方修复状态
[PR #15](https://github.com/lance-format/lance-flink/pull/15) 的 `sink/LanceSinkWriter.java#L191-L203` 修了这个：

```java
// Append mode: need to get the current dataset version
Dataset existingDataset = LanceDatasetFactory.open(datasetPath, allocator);
long readVersion;
try { readVersion = existingDataset.version(); }
finally { existingDataset.close(); }
FragmentOperation.Append append = new FragmentOperation.Append(fragments);
dataset = append.commit(allocator, datasetPath, Optional.of(readVersion), ...);
```

但：
1. PR 未合并
2. 仍然是"每次 flush 都 open dataset"的反模式

---

## Bug 影响评估

| Bug | 生产影响 | 绕过难度 | 官方修复状态 |
|---|---|---|---|
| #1 S3 path check | 🔴 静默丢数据（parallelism > 1 时） | 🟡 中等（打 patch + 预建表） | 🟡 PR #15 修了但未合 |
| #2 read_version 缺失 | 🔴 connector 完全不可用 | 🟢 简单（打 patch 就能跑） | 🟡 PR #15 修了但未合 |

## 这两个 Bug 如何影响实测

- Bug 2 让我压测前浪费了 30 分钟排查"为什么 job 一启动就 fail"
- Bug 1 让我必须先用 Python 预建表（否则 parallelism=4 的压测会丢 75% 数据）
- 打 patch 后的 connector 行为是"最乐观情况"的下限 —— 实际用户直接 clone HEAD 压根连数据都写不进去

## 如何应用 Patch

见本 repo 的 [patches/](patches/) 目录。应用：

```bash
cd lance-flink
git apply ../patches/0001-s3-path-check.patch
git apply ../patches/0002-read-version.patch
mvn clean package -DskipTests
```

或直接 edit `LanceSink.java` 对应行。
