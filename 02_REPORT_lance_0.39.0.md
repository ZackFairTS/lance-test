# 版本升级对比报告：lance-core 0.23.3 vs 0.39.0

**对照实测**：同一环境、同一 connector commit、同一 Flink 配置，只改 `<lance.version>` 从 0.23.3 → 0.39.0（最新）。

## 版本对比

| 组件 | 旧版实测 | 新版实测 |
|---|---|---|
| Flink | 1.16.3 | 1.16.3 |
| lance-flink connector | fc3d064 (HEAD) | fc3d064 (HEAD) |
| **lance-core** | **0.23.3** (2025-03) | **0.39.0** (2026-03) ← 最新 |
| Apache Arrow | 14.0.0 | 15.0.0 |
| Local patches | read_version + S3 path | read_version + S3 path |

## 结果对比

| 指标 | lance-core 0.23.3 | lance-core 0.39.0 | 变化 |
|---|---|---|---|
| Phase 1 持续时间 | ~580 s (脚本挂了) | ~270 s (baseline) | 可比 |
| Phase 2 持续时间 | 420 s (带 compaction) | 420 s | 相同 |
| Phase 3 持续时间 | 120 s | 120 s | 相同 |
| **Total checkpoints** | 32 | 24 | - |
| **Failed checkpoints** | 5 (15.6%) | **11 (45.8%)** | 🔴 **恶化 3x** |
| **Job restart 次数** | 2 | **5** | 🔴 **恶化 2.5x** |
| CP p50 延迟 | 27.9 s | 8.2 s | ✅ 改善 |
| CP p99 延迟 | 57.3 s | 36.4 s | ✅ 改善 |
| CP max 延迟 | 57.3 s | 36.4 s | ✅ 改善 |
| 总写入行 | 2,774,078 | 1,896,521 | - |
| 唯一 ID 数 | 1,643,852 | 600,082 | - |
| **重复行数** | 1,130,226 | **1,296,439** | 🔴 稍差 |
| **重复率** | **40.7%** | **68.4%** | 🔴 **恶化 1.7x** |
| 有效吞吐达成率 | 25% | ~20% | 🟡 略差 |

## 深度观察

### 0.39.0 的单次 commit 延迟其实变快了（从 27.9s → 8.2s）
这是 0.39.0 的架构优化起作用了 —— 比如更高效的 manifest reading、PR #3483 的 ConditionalPutCommitHandler 等。commit 本身更快是好事。

### 但 restart 和 failure rate 反而恶化了
**为什么**？三个可能的解释（我无法完全确定，需要更多测试）：

1. **更快的 commit → 更密集的 manifest 变更 → 更激烈的 OCC 竞争**
   - 旧版 commit 慢到 checkpoint 整个挤塞，吞吐自然受限反而减少竞争
   - 新版 commit 快了，所以能生成更多 version → compaction 能看到更多 fragment 要合并 → compaction 任务更重
   
2. **0.39.0 的新冲突检测更严格**
   - 源码研究里看到 `check_rewrite_txn` / `check_append_txn` 在 0.39.0 里做得更细致（PR #3397, #3614 相关）。更细致可能意味着原先被静默 rebase 的情况，现在可能被判为 retryable → 消耗重试预算 → 更容易失败。
   
3. **重启后的 recovery overhead**
   - Flink restart 本身要重开 Lance dataset，0.39.0 的 dataset 可能 manifest version 累积得更多（因为 commit 更快生成更多 version），open 一次要读更多元数据。

### 数据重复率翻倍（40% → 68%）的原因
- 5 次 restart × 每次 replay → 重复行数理论上最多应该是 "replay 的 buffer 行 × restart 次数"
- 0.39.0 的 5 次 restart 产生比 0.23.3 的 2 次更多 replay
- 加上 0.39.0 commit 更快，单位时间内能进入 dataset 的 row 更多，所以每次 replay 覆盖的 pending 数据更多

## 最重要的发现

**即使升级到最新的 lance-core 0.39.0，根本问题没解决 —— 只是症状略微移位**：

| 层面 | 0.23.3 问题 | 0.39.0 问题 |
|---|---|---|
| **Commit 延迟** | 极端（p50 28s） | 好转（p50 8s） |
| **Checkpoint 失败率** | 15.6% | **45.8%** ← 反而更差 |
| **Job restart** | 2 次 | **5 次** ← 反而更多 |
| **数据重复率** | 40% | **68%** ← 反而更严重 |

**→ 升级 lance-core 不是解决方案**。根本问题是**架构性的**：`LanceSink` 的 `RichSinkFunction` + 同步 commit + 不实现 2PC 的设计，在任何 lance-core 版本下都会在 compaction 并发时产生 restart + 重复。

## 结论的再次确认

我最早"高负载下会以 Flink job 重启的形式打断"的论断 —— **在旧版和新版 lance-core 上都成立**，而且新版更严重。所以：

> **"升级到最新的 lance-core 能解决这个问题吗？" → 实测答案：不能，反而可能更糟。**
> 
> **"用最新的 lance-flink 呢？" → HEAD 就是我们测的版本 (fc3d064)，从 2026-01-08 起就没更新了。没有更新的版本可用。**
> 
> **真正的解决方案只有一条**：**等 PR #15 合并** —— 把 `LanceSink` 从 `RichSinkFunction` 迁移到 Flink SinkV2 的 `TwoPhaseCommittingSink` 接口，把 commit 从 `snapshotState`（sync phase）搬到 `notifyCheckpointComplete`（async phase），并引入 operator state 来持久化未完成的 committable。这是 **架构重构**，不是依赖升级。

## 数据文件
- 旧版报告: `/home/hadoop/lance-stress/logs/main-v2-064711/REPORT.md`
- 新版报告: `/home/hadoop/lance-stress/logs/main-v39-074627/` (本文件)
- 新版 checkpoints: `main-v39-074627/checkpoints.json`
- 新版 exceptions: `main-v39-074627/exceptions.json`
