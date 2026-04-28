# 压测前的源码 + 文档研究

压测之前通过 librarian agent 对 Lance 源码、lance-flink connector 源码、Flink 2PC 机制做的深度研究。这些结论驱动了压测设计，也是压测结果解读的依据。

## 1. Lance 的并发模型 (Multi-Version Concurrency Control)

Lance 使用 **MVCC + 乐观并发控制 (OCC) + 自动 transaction rebase**，无锁。

### 冲突矩阵（源码实测证据）

来源：[`rust/lance/src/io/commit/conflict_resolver.rs`](https://github.com/lance-format/lance/blob/main/rust/lance/src/io/commit/conflict_resolver.rs)

```rust
// check_rewrite_txn (compaction 提交时检查其它并发事务)
if let Operation::Rewrite { .. } = &self.transaction.operation {
    match &other_transaction.operation {
        Operation::Append { .. }
        | Operation::ReserveFragments { .. }
        | Operation::Project { .. }
        | ... => Ok(()),   // ← compaction 和 append 兼容
```

```rust
// check_append_txn (append 提交时检查其它并发事务)
Operation::Append { .. }
| Operation::Rewrite { .. }   // ← append 和 compaction 兼容
| Operation::CreateIndex { .. }
| ... => Ok(()),
```

**关键结论**: 在 Lance 层面，Append 和 Rewrite (compaction) 是**显式兼容**的，永远不会抛"incompatible"错误。

### 完整冲突矩阵

| 并发操作 vs Rewrite (compaction) | 结果 |
|---|---|
| **Append (streaming write)** | ✅ 永远兼容 |
| Delete / Update (不同 fragment) | ✅ 兼容 |
| Delete / Update (同一 fragment) | ⚠️ Retryable（Lance 自动 rebase） |
| 另一个 Rewrite (不同 fragment) | ✅ 兼容 |
| 另一个 Rewrite (重叠 fragment) | ⚠️ Retryable |
| CreateIndex | ⚠️ Retryable |
| Overwrite / Restore | ❌ Incompatible（硬失败） |

## 2. S3 上的原子提交机制

### 时间线

| 时期 | 机制 | 是否需要 DynamoDB |
|---|---|---|
| 2024-11 前 | `UnsafeCommitHandler`（不安全）或 `s3+ddb://` + DynamoDB CAS | 是（安全起见） |
| **2024-11 后** | AWS S3 原生 `PutObject` + `If-None-Match: *` 条件写 | — |
| **2025-02 (PR #3483) 后** | **默认** `ConditionalPutCommitHandler`，直接用 S3 条件写 | **不再需要** |

### 关键源码

[`rust/lance-table/src/io/commit.rs`](https://github.com/lance-format/lance/blob/main/rust/lance-table/src/io/commit.rs):

```rust
match url.scheme() {
    "s3" | "gs" | "az" | "abfss" | "memory" | "oss" | "cos" => {
        Ok(Arc::new(ConditionalPutCommitHandler))
    }
}
```

```rust
impl CommitHandler for ConditionalPutCommitHandler {
    async fn commit(&self, ...) -> Result<ManifestLocation, CommitError> {
        let res = object_store.inner.put_opts(
            &path, dummy_data.into(),
            PutOptions { mode: object_store::PutMode::Create, .. },
        ).await.map_err(|err| match err {
            ObjectStoreError::AlreadyExists { .. }
            | ObjectStoreError::Precondition { .. } => CommitError::CommitConflict,
            _ => CommitError::OtherError(err.into()),
        })?;
```

使用 S3 的 `PutMode::Create`（If-None-Match: *），冲突时返回 `CommitConflict` → 触发上层 rebase 重试。

## 3. Lance 的重试机制

### 默认配置

- **`RetryConfig::default()` 在 `retry.rs`**: `max_retries=10, retry_timeout=30s`
- **`CommitConfig::default()` 在 `commit.rs`**: `num_retries=20, timeout=30s`
- 实际 CommitBuilder 用的是 `CommitConfig`，所以生效的是 **20 次重试 / 30 秒超时**

### 重试循环

[`rust/lance/src/dataset/write/retry.rs`](https://github.com/lance-format/lance/blob/main/rust/lance/src/dataset/write/retry.rs):

```rust
while backoff.attempt() <= config.max_retries {
    match commit_future.await? {
        Ok(result) => return Ok(result),
        Err(Error::RetryableCommitConflict { .. }) => {
            if start.elapsed() > config.retry_timeout {
                return Err(timeout_error(...));
            }
            tokio::time::sleep(backoff.next_backoff()).await;
        }
    }
}
```

**使用 `SlotBackoff`**: 50ms → 100ms → 200ms → 400ms → ...

### 错误类型

| Rust LanceError variant | 是否自动重试 | Java 异常映射 |
|---|---|---|
| `RetryableCommitConflict` | ✅ 是，最多 20 次 | 透明（重试成功）或 `RuntimeException` |
| `CommitConflict` | ❌ 否，重试耗尽时转成这个 | **`java.lang.IllegalArgumentException`** ⚠️ |
| `TooMuchWriteContention` | ❌ 否（就是超时错误） | `RuntimeException` |
| `IncompatibleTransaction` | ❌ 否 | `RuntimeException` |
| `IO { source: ObjectStore }` | 不在 commit 层重试 | `IOException` |

**注意**: 没有专门的 `CommitConflictException` Java 类！`CommitConflict` 和普通 bad input 都映射到 `IllegalArgumentException`，需要 parse error message 来区分。

## 4. lance-flink Connector 的架构（压测前的研究）

### 设计
- **NOT SinkV2, NOT TwoPhaseCommittingSink** — 用老式 `RichSinkFunction + CheckpointedFunction`
- Commit 在 `snapshotState()` 里同步执行（Flink sync phase）
- 无 `Committer` 类，无 precommit/commit 分阶段
- `initializeState()` 是空的 — 没注册任何 `ListState`/`OperatorState`
- 所有冲突处理由 Java 层全部吞掉为 `IOException("Failed to write Lance dataset")`

### 每次 flush 做什么
1. 把 buffer 转成 Arrow `VectorSchemaRoot`
2. 同步调 `Fragment.create(...)` 写数据文件到 S3
3. 同步调 `append.commit(...)` 写新 manifest 到 S3
4. 清 buffer

**问题**: 每 1024 行就 commit 一次 manifest，高并发下是**极其密集**的 CAS 竞争。

### "Exactly-Once" 是谎言
README 写"✅ Exactly-Once"，但：
1. 无算子状态持久化未完成的 committable
2. commit 在 `snapshotState` 里执行，不在 `notifyCheckpointComplete`
3. Batch 触发的 mid-checkpoint flush 立即提交，不跟 checkpoint 绑定
4. 重启时 source replay → 重复数据入 Lance

**实测**: at-least-once，重复率 **40-68%**。

## 5. 修复中的 PR #15

[lance-flink PR #15](https://github.com/lance-format/lance-flink/pull/15) "refactor to FLIP-143/FLIP-27":
- 迁移到 Flink SinkV2
- 拆分 `SinkWriter` + `Committer`
- **修复** Bug 2（`read_version` 显式传递）
- **但截至 2026-04 仍未合并**
- 甚至 PR #15 的 HEAD 里顶级 `LanceSink` 仍是 `RichSinkFunction`（迁移不完整）

## 6. Iceberg-Flink 的可参考模式

Iceberg 也是 OCC 存储，和 Flink 集成了 10 年，有成熟的 `IcebergFilesCommitter`:

```java
// Iceberg-Flink 的模式：
private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();
```

**精华**:
1. `snapshotState()` 里把 `DataFile` 列表持久化到 Flink state，**不 commit**
2. `notifyCheckpointComplete(checkpointId)` 时才真正 commit 到 Iceberg
3. Recovery 时读 state + 查表的 `flink.max-committed-checkpoint-id` 属性做幂等
4. 遇到 `CommitFailedException` 在 catalog 层重试（default 4 次）

这是 **lance-flink 应该学习但还没做到** 的模式。

## 7. 生产关键配置 (Flink)

### Checkpoint 容忍度的坑
[Flink 1.20 官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/checkpointing/):

> `execution.checkpointing.tolerable-failed-checkpoints` only applies to **IOException on the Job Manager, failures in the async phase on the Task Managers, and checkpoint expiration due to timeout**. Failures originating from the **sync phase on the Task Managers** are always forcing failover.

**含义**: 如果 Lance commit 在 sync phase (snapshotState) 里抛异常，**无论 `tolerable-failed-checkpoints` 设多大，都会强制 failover**。

**但实测发现**（见 [01_REPORT_lance_0.23.3.md](01_REPORT_lance_0.23.3.md)）:
- 实际触发 restart 的不是 sync phase 的 Java 异常
- 而是 **checkpoint 60s timeout**（commit 太慢）
- Timeout 是 Flink 自己检测的，属于 async phase 失败，受 `tolerable-failed-checkpoints` 控制
- 所以提高这个值能**减少** restart 频率，但不解决重复问题

## 参考引用

### Lance 源码
- 冲突矩阵: [`rust/lance/src/io/commit/conflict_resolver.rs`](https://github.com/lance-format/lance/blob/main/rust/lance/src/io/commit/conflict_resolver.rs) L643, L873
- 重试循环: [`rust/lance/src/dataset/write/retry.rs`](https://github.com/lance-format/lance/blob/main/rust/lance/src/dataset/write/retry.rs) L73-L130
- S3 commit handler: [`rust/lance-table/src/io/commit.rs`](https://github.com/lance-format/lance/blob/main/rust/lance-table/src/io/commit.rs) L745, L1058-L1102
- Transaction types: [`rust/lance/src/dataset/transaction.rs`](https://github.com/lance-format/lance/blob/main/rust/lance/src/dataset/transaction.rs)

### Java Bindings
- `FragmentOperation`: [`java/src/main/java/com/lancedb/lance/FragmentOperation.java`](https://github.com/lance-format/lance/blob/main/java/src/main/java/com/lancedb/lance/FragmentOperation.java)
- JNI error mapping: [`java/lance-jni/src/error.rs`](https://github.com/lance-format/lance/blob/main/java/lance-jni/src/error.rs)
- JNI commit entry: [`java/lance-jni/src/blocking_dataset.rs`](https://github.com/lance-format/lance/blob/main/java/lance-jni/src/blocking_dataset.rs)

### lance-flink
- HEAD sink: [`LanceSink.java`](https://github.com/lance-format/lance-flink/blob/fc3d064ace4bbdbf29e22a489db2c5bf61a36990/src/main/java/org/apache/flink/connector/lance/LanceSink.java)
- Config options: [`LanceOptions.java`](https://github.com/lance-format/lance-flink/blob/fc3d064ace4bbdbf29e22a489db2c5bf61a36990/src/main/java/org/apache/flink/connector/lance/config/LanceOptions.java)

### Iceberg 参考
- `IcebergFilesCommitter`: [flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java](https://github.com/apache/iceberg/blob/main/flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java)
- Retry config: [`TableProperties.java`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/TableProperties.java) (`commit.retry.num-retries=4`)

### Lance 规范
- [Lance Transactions spec](https://lance.org/format/table/transaction/)
- [Compaction wiki](https://deepwiki.com/lancedb/lance/2.5-compaction-and-optimization)

### 相关 Issues/PRs
- [lance#3397](https://github.com/lancedb/lance/issues/3397) — 重试化冲突解决伞形 issue
- [lance#3483](https://github.com/lancedb/lance/pull/3483) — ConditionalPutCommitHandler for S3
- [lance#3614](https://github.com/lancedb/lance/pull/3614) — Retryable vs Incompatible 分类
- [lance#6150](https://github.com/lance-format/lance/issues/6150) — rewrite ↔ update/delete 自动 rebase（open）
- [lance-flink#15](https://github.com/lance-format/lance-flink/pull/15) — SinkV2 重构（open）
- [lance-flink#10](https://github.com/lance-format/lance-flink/issues/10) — 支持 OPTIMIZE（open）
- [lance-flink#11](https://github.com/lance-format/lance-flink/issues/11) — 支持 VACUUM（open）
- [lancedb#3086](https://github.com/lancedb/lancedb/issues/3086) — 真实生产环境的 S3+DDB CRUD 问题
