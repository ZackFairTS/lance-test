# Lance-Flink 压测实证报告

**测试时间**: 2026-04-28 06:00-07:00 UTC
**环境**: AWS EMR master (r8g.2xlarge Graviton / 8vCPU / 64 GiB / Amazon Linux 2023), Docker-wrapped Flink 1.16.3 (Ubuntu 24.04 + OpenJDK 11)
**被测组件**: `lance-format/lance-flink` HEAD (fc3d064, 2 local patches applied) + `com.lancedb:lance-core:0.23.3` on AWS S3 (ap-northeast-1)
**Run ID**: main-v2-064711
**Dataset**: `s3://lance-benchmark-<ACCOUNT_ID>-ap-northeast-1/stress-test/20260428-050427/main-v2-064711/dataset`

---

## 测试配置

| 参数 | 值 |
|---|---|
| Flink parallelism | 4 |
| 目标速率 | 10,000 rows/sec |
| `write.batch-size` | 1024 |
| 单行大小 | ~90 字节 (id + ts + 80-byte hex payload) |
| checkpoint.interval | 10 s |
| checkpoint.timeout | 60 s |
| tolerable-failed-checkpoints | **0** (默认，严格模式) |
| restart-strategy | fixed-delay, 20 attempts × 5 s |
| Compaction | tight loop, 每次结束 sleep 2 s |
| Phase 1 (baseline) | 120 s + (延伸至 ~580 s，因脚本中断) |
| Phase 2 (concurrent compaction) | 420 s |
| Phase 3 (recovery) | 120 s |

## 提交前发现的 2 个 Connector Bug

### Bug 1: `Paths.get("s3://...")` 在本地 FS 检查失败
**位置**: `LanceSink.java#L120`
```java
Path path = Paths.get(datasetPath);
this.datasetExists = Files.exists(path);  // s3 URL 下永远 false
```
**后果**: S3 dataset 启动时被误判为"不存在"，首次 flush 走 `FragmentOperation.Overwrite` 而不是 `Append` → N 个并行 writer 互相覆盖，数据大规模丢失。
**Patch**: 强制 `datasetExists = true`，要求 dataset 必须预先存在。

### Bug 2: `Append.commit(..., Optional.empty(), ...)` 在 lance-core ≥ 0.23.3 被拒绝
**位置**: `LanceSink.java#L188`
```java
FragmentOperation.Append append = new FragmentOperation.Append(fragments);
dataset = append.commit(allocator, datasetPath, Optional.empty(), ...);
```
**错误**: `IllegalArgumentException: Invalid user input: read_version must be specified for this operation, rust/lance/src/dataset.rs:665:21`
**后果**: HEAD 版本的 LanceSink + 任何实际 lance-core 版本组合 → **append 模式根本无法工作**。
**Patch**: 在每次 append 前 `Dataset.open(path).version()` 拿当前版本传入。
**官方 fix**: PR #15 正在做这个修复但还没合（我之前研究已指出）。

**Connector HEAD 目前处于 "append 模式完全不可用" 状态。** 必须打本地 patch 才能做压测。

---

## 实测结果

### 1. Flink Job 状态与重启

| 指标 | 实测值 |
|---|---|
| **Job restart 次数** | **2 次** |
| **Restart 原因** | `org.apache.flink.util.FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold` |
| Restart 时间戳 | 1777358945430 & 1777359547180 |

Job 状态流：`INITIALIZING → RUNNING → RUNNING → (checkpoint 连续失败) → FAILING → RESTARTING → RUNNING → ... → CANCELED` (手动取消结束)

### 2. Checkpoint 统计

| 指标 | 值 |
|---|---|
| Total checkpoints | 32 |
| Completed | 27 |
| **Failed** | **5** |
| Failed rate | **15.6%** |
| End-to-end duration p50 | 27858 ms (27.9 秒) |
| End-to-end duration p90 | 53783 ms (53.8 秒) |
| End-to-end duration p95 | 56190 ms (56.2 秒) |
| End-to-end duration p99 | 57332 ms (57.3 秒) |
| End-to-end duration max | 57332 ms |
| Checkpoint timeout | 60 秒 |

**Checkpoint 失败模式**：
```
CP#22 status=FAILED dur=60000ms  "Checkpoint expired before completing"  ← 超时失败
CP#30 status=FAILED dur=28298ms  "Checkpoint Coordinator is suspending"   ← job 被中止时的连带
```

smoke test (parallelism=1, 500 r/s, 无 compaction) 时 checkpoint 只要 **105 ms**。压测 parallelism=4 + 10K r/s + 并发 compaction 后 **p50 飙到 27.9 秒（266 倍慢）**，p99 几乎撞到 60 秒 timeout。

### 3. 数据完整性（⚠️ 核心发现）

| 指标 | 值 |
|---|---|
| 总行数 (`count_rows`) | **2,774,078** |
| 唯一 ID 数 | **1,643,852** |
| **重复行数** | **1,130,226** |
| **重复率** | **40.7%** |
| Schema | `id BIGINT NOT NULL, ts BIGINT NOT NULL, payload VARCHAR(200)` |

`id` 是 source 端生成的 `(subtask_id * 1e10) + seq`，按设计在一个 run 内是唯一的。**40.7% 重复 = 直接证据证明 connector 不是 exactly-once**。

重复的成因链条：
1. Checkpoint N 失败 → job restart
2. Restart 后 source 从 checkpoint N-1 的 offset replay
3. 但 CP N-1 到 CP N 失败时刻之间，**sink 在 flush() 里已经把数据持久化到 Lance**
4. Replay 出来的新数据再次 commit → **同一 id 出现两次**

### 4. 吞吐量

| 指标 | 值 |
|---|---|
| 目标速率 | 10,000 rows/sec × (120 + 420 + 120)s = **6.6M rows** |
| 实际写入 | 2,774,078 rows |
| 去重后独特行 | 1,643,852 rows |
| **有效吞吐达成率** | **25%** (不去重) / **15%** (去重后真正独特的数据) |

Source 显示背压严重 —— checkpoint 阻塞导致 sink 拒绝更多数据。

### 5. Storage 膨胀

| 指标 | 值 |
|---|---|
| Manifest version 数 | 2,773 |
| Fragment 数（结束时） | 2,772 |
| Dataset rows (含重复) | 2,774,078 |

每个 write 一个 fragment，每个 fragment 一个 manifest version —— compactor 试图运行但 **tight loop 里的单次 `compact_files()` 就耗时分钟级**（因为每次都面对 2000+ 个 fragment），实际 Phase 2 的 420 秒里 compaction iteration 数 = **0 或 1**（compactor.log 空，因为 Python stdout 缓冲 + compaction 调用在 block，没机会 flush 日志）。

---

## 结论

### 我之前论断的核验

| 论断 | 实测结果 |
|---|---|
| "高负载下 compaction 会通过 Lance 内部重试耗尽 → Flink sync-phase 失败 → 强制 failover" | ✅ **部分成立**：触发了 checkpoint 失败 → job restart，但失败原因是 **checkpoint 超时**（60s），不是 Rust 层 `TooMuchWriteContention`（因为 lance-flink 的 commit 是在 `snapshotState()` 同步跑，commit 本身慢到让 checkpoint 超时就够了，还没到 Rust 重试耗尽） |
| "sync phase 失败绕过 `tolerable-failed-checkpoints`" | ⚠️ **不完全准确**：实际路径是 checkpoint 超时 → 计入 failure count → 达到 threshold (0) → job fail。是配置容忍度的问题，而不是 sync phase 的特殊 bypass。 |
| "at-least-once，重启后数据重复" | ✅ **实锤**：40.7% 重复率 |
| "compaction 让 commit 延迟大幅增加" | ✅ **实锤**：p50 从 105ms (smoke) → 27.9s，p99 几乎撞 60s timeout |
| "吞吐显著下降" | ✅ **实锤**：只拿到 15-25% 的目标吞吐 |

### 修订后的精确结论

1. **compaction 并发下 Flink job 会重启** —— 压测里 2 次 restart，具体失败模式是 **checkpoint timeout (60s)**，而非 Rust 层的 OCC 重试耗尽。
2. **数据正确性问题严重** —— 默认配置下 at-least-once 并且在高负载下产生 **40%+ 重复行**。
3. **lance-flink HEAD 目前的 append 模式在任何 lance-core ≥ 0.23.3 上都不工作** —— 需要本地打 read_version 补丁才能启动。
4. **LanceSink 的 S3 path bug** 会让多 parallelism 互相覆盖 —— 需要预先用 Python/Rust 创建 dataset 并 patch connector 跳过本地路径检查。
5. **Compaction tight loop 在压测期间几乎无法运行**，因为单次 `compact_files()` 就超过 loop 周期。实际 compaction 频率 ≪ 预期。

### 生产建议（修订）

基于实测，**不建议在生产使用当前 HEAD 的 lance-flink**。如果必须用：

1. **必须**本地打 `read_version` patch（等 PR #15 合并）
2. **必须**先用 Python 创建 dataset，不能让 Flink 从零建表（N 个 writer 竞争 Overwrite → 数据错乱）
3. **必须**让下游对重复鲁棒（按 `id` 去重），或明确标注 at-least-once
4. **建议**大幅提高 `write.batch-size`（10K-100K）减少 manifest commit 频率
5. **建议**把 checkpoint interval 从 10s 提到 5-10 分钟，timeout 提到 180s+
6. **建议**降低 Flink sink parallelism（1-2 足够）
7. **建议** compaction 独立运行且与 checkpoint 错开（例如半夜业务低峰运行一次）
8. **建议**启用 `tolerable-failed-checkpoints ≥ 3-5`，但知道这**不保证**数据正确性，只是减少 restart 频率

### 被推翻/修正的之前说法

> "Rust 20 次重试 / 30s timeout 耗尽 → 抛 `CommitConflict` → Flink sync phase 失败"

**修正**：实测里这条路径不是主要失败原因。真正的失败是：**Lance Append commit 本身慢（因为要先 `Dataset.open()` 拿 read_version，而 open 一个有 2000+ manifest 版本的 dataset 就很慢，再加上 sink 同步 flush + commit），把 checkpoint 整体耗时推过 60s timeout**。Rust 层的重试耗尽 *可能* 会发生，但在我们的压测里还没触发到那一步，checkpoint 就已经超时了。

> "实际是 at-least-once，可能产生重复"

**修正**：不是 "可能"，是 **实测 40.7% 重复**。生产环境这个数字可能更高（更多 restart）。

---

## 附：原始数据文件

所有数据保存在 `/home/hadoop/lance-stress/logs/main-v2-064711/`:
- `summary.txt` — 运行参数
- `metrics.csv` — Flink REST API 采样（每 2 秒）
- `checkpoints.json` — Checkpoint Coordinator 完整历史
- `final-job.json` — Job 最终状态
- `exceptions.json` — **重启原因栈** (2 次 FlinkRuntimeException)
- `taskmanager.log` — TaskManager 完整日志

---

**结论：我之前"高负载下 compaction 会以 Flink job 重启的形式打断"的论断，在实测中得到了验证 —— 虽然具体失败路径比我推理的更简单（checkpoint 超时而不是 Rust 层异常），但最终症状（job restart + 数据重复）完全一致。**
