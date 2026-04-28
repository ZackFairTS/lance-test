# 压测结果总结 (Executive Summary)

**测试日期**: 2026-04-28
**测试人**: EMR master 节点直接执行
**主要结论**: Lance on S3 在高并发写入 + compaction 场景下，Flink job 会反复重启并产生大量数据重复，问题与 lance-core 版本无关，是 `lance-flink` connector 的架构性缺陷。

---

## 核心数字对比

| 指标 | lance-core 0.23.3 (connector 默认版) | lance-core 0.39.0 (最新版) |
|---|---|---|
| Total checkpoints | 32 | 24 |
| **Failed checkpoints** | **5 (15.6%)** | **11 (45.8%)** |
| **Job restart 次数** | **2** | **5** |
| CP 延迟 p50 | 27,858 ms | 8,252 ms |
| CP 延迟 p99 | 57,332 ms | 36,398 ms |
| CP 延迟 max | 57,332 ms（几乎撞 60s timeout） | 36,398 ms |
| 总写入行数 | 2,774,078 | 1,896,521 |
| **唯一 ID 数** | **1,643,852** | **600,082** |
| **重复行数** | **1,130,226** | **1,296,439** |
| **重复率** | **40.7%** | **68.4%** |
| 有效吞吐达成率（目标 10K rows/s） | 25% | ~20% |

## 验证的假设

> "高负载下 compaction 会以 Flink job restart 的形式打断流式写入"

**✅ 成立。** 两个版本都实测到 restart，新版更严重。

## 推翻/修正的推理

| 之前的推理 | 实测真相 |
|---|---|
| "Rust 20 次重试耗尽 → 抛 `TooMuchWriteContention` → sync phase 失败" | ❌ 实际失败路径更朴素：**Lance commit 本身慢**（开 2000+ version 的 dataset 耗时）→ **checkpoint 60s timeout** → 累计达阈值 → restart。根本没走到 Rust 重试耗尽。 |
| "sync phase 失败 bypass `tolerable-failed-checkpoints`" | ⚠️ 不准确。实测就是标准的 "failed checkpoints 计数 ≥ 阈值 → job fail"。提高 tolerable 值能减少 restart 频率。 |
| "at-least-once，可能有重复" | ❌ 不是"可能"，是**实测 40-68% 重复**。 |

## 意外发现

### 1. lance-flink HEAD 自带两个严重 bug，不打 patch 根本跑不起来

详见 [04_CONNECTOR_BUGS.md](04_CONNECTOR_BUGS.md)：

- **Bug 1**: `Paths.get("s3://...")` 本地 FS 检查永远返回 false → N 个 parallel writer 互相 Overwrite
- **Bug 2**: `append.commit(..., Optional.empty(), ...)` 在任何 lance-core ≥ 0.23.3 上都被拒绝，直接抛 `IllegalArgumentException: read_version must be specified`

HEAD APPEND 模式**开箱即用等于不可用**。

### 2. 升级 lance-core 到最新版反而更糟

直觉上新版应该更好，实测结果反直觉：

| 维度 | 0.23.3 → 0.39.0 变化 |
|---|---|
| 单次 commit 延迟 | ✅ p50 下降（28s → 8s） |
| Checkpoint 失败率 | 🔴 上升（15.6% → 45.8%） |
| Restart 次数 | 🔴 上升（2 → 5） |
| 数据重复率 | 🔴 上升（40.7% → 68.4%） |

**可能的原因**（推测，未完全验证）：
1. 新版 commit 更快 → 单位时间生成更多 manifest version → compaction 竞争更激烈
2. 新版冲突检测更细致 → 原先静默 rebase 的场景现在变 retryable → 消耗重试预算
3. 新版 restart 后 recover overhead 更大（需要 open 有更多 version 的 dataset）

**关键洞察**：Lance 自身的 Rust-level 优化（PR #3483, #3397, #3614）有效 —— commit 确实更快。但 **`lance-flink` 的架构缺陷让这些优化的收益被放大成副作用**。

### 3. Connector 本身已 3+ 个月无更新

- lance-flink HEAD: `fc3d064` (2026-01-08)
- 距今 3 个月多没提交
- 关键重构 PR [#15](https://github.com/lance-format/lance-flink/pull/15)（SinkV2 + 2PC）至今未合并

## 生产建议

### 🔴 绝对不要做

1. 直接用 lance-flink HEAD（APPEND 模式根本不能用，必须打 patch）
2. 让 Flink 从零创建 dataset（N 个 parallel writer 会互相 Overwrite）
3. 相信 README 的 "Exactly-Once" 宣传（实际是 at-least-once，且高重复率）
4. 期待升级 lance-core 解决问题（实测新版更糟）

### ✅ 必须做（如果一定要用）

1. 打 `read_version` patch（见 [04_CONNECTOR_BUGS.md](04_CONNECTOR_BUGS.md) Bug 2 章节）
2. 预先用 Python/Rust 创建 dataset，让 connector 跳过本地 FS 检查
3. **下游必须按 `id` 幂等去重**（假定 at-least-once）
4. `write.batch-size` 从 1024 提到 **64K-1M** 级别
5. `checkpoint.interval` 从 10s 提到 **5-10 分钟**
6. `checkpoint.timeout` 提到 **180s+**
7. `tolerable-failed-checkpoints` 设为 **3-5**
8. Sink parallelism **降到 1-2**
9. Compaction 独立进程跑，**业务低峰期** + 低频
10. 监控 `syncCheckpointDuration` 和 `numberOfFailedCheckpoints`

### 🎯 真正的长期方案

**等 [lance-flink PR #15](https://github.com/lance-format/lance-flink/pull/15) 合并**，把 sink 从 `RichSinkFunction` 重构为 `SinkV2 + TwoPhaseCommittingSink`：
- commit 从 `snapshotState()`（sync phase）搬到 `notifyCheckpointComplete()`（async phase）
- 引入 operator state 持久化未完成的 committable
- 实现真正的 exactly-once

这是 **架构级改造**，不是依赖升级能修的。

## 附：替代方案对照

如果你等不及 PR #15，考虑其他架构：

| 方案 | 优点 | 缺点 |
|---|---|---|
| Flink → Kafka → (离线) Python 批写 Lance | 解耦写入和流处理，易做幂等 | 延迟变高，复杂度增加 |
| Spark Structured Streaming + lance-spark | lance-spark 更成熟，支持 OPTIMIZE | Spark 不是 Flink，需要生态迁移 |
| Flink → Iceberg → Lance（离线同步） | Iceberg-Flink 成熟，有真正的 2PC | 引入中间表，存储翻倍 |
| 等 PR #15 合并 | 根本解决 | 时间不确定 |
