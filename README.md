# Lance-Flink 压测实证报告

> 针对 `lance-format/lance-flink` connector 在 **Flink 流式写入 + Lance compaction 并发** 场景下的完整压测报告。
>
> 回答核心问题：**Lance table on S3 流式写入时，同时做 compaction，是否会打断写入？**
>
> **结论（实测）：会。不仅会通过 Flink job restart 的形式打断，还会导致 40-68% 的数据重复。**

## TL;DR

| 关键结论 | 实测数据 |
|---|---|
| Flink job 是否会被打断 | ✅ **会**，压测 12 分钟内 restart 2-5 次 |
| 数据是否会重复 | 🔴 **会**，重复率 40.7%（旧版）/ 68.4%（新版 lance-core） |
| 升级到最新 lance-core 能否解决 | ❌ **不能**，反而恶化（restart 次数 2→5，重复率 40%→68%） |
| 根本原因 | `LanceSink` 用 `RichSinkFunction` + 同步 commit，非 Flink 2PC，架构性问题 |
| 真正解决方案 | 等 [PR #15](https://github.com/lance-format/lance-flink/pull/15) 合并（SinkV2 + TwoPhaseCommittingSink 重构） |

## 文档索引

### 📋 主报告
- **[00_SUMMARY.md](00_SUMMARY.md)** — 所有测试结果的总结（推荐先读）
- **[01_REPORT_lance_0.23.3.md](01_REPORT_lance_0.23.3.md)** — lance-core 0.23.3（connector 默认）完整压测报告
- **[02_REPORT_lance_0.39.0.md](02_REPORT_lance_0.39.0.md)** — lance-core 0.39.0（最新版本）对比压测报告

### 🔬 研究背景
- **[03_BACKGROUND_research.md](03_BACKGROUND_research.md)** — 压测前的源码 + 文档研究（librarian agent 产出）
- **[04_CONNECTOR_BUGS.md](04_CONNECTOR_BUGS.md)** — 实测中发现的 2 个 lance-flink 严重 Bug

### 🧪 复现
- **[05_HOW_TO_REPRODUCE.md](05_HOW_TO_REPRODUCE.md)** — 在 AWS EMR 上复现压测的完整步骤
- **[scripts/](scripts/)** — 压测脚本（Java + Python + Bash）

### 📊 原始数据
- **[data/run1_lance_0.23.3/](data/run1_lance_0.23.3/)** — 0.23.3 run 的 raw Flink metrics / checkpoints / exceptions
- **[data/run2_lance_0.39.0/](data/run2_lance_0.39.0/)** — 0.39.0 run 的 raw data

## 测试环境

| 项 | 值 |
|---|---|
| 硬件 | AWS EMR master, r8g.2xlarge (Graviton ARM64, 8vCPU, 64 GiB) |
| OS | Amazon Linux 2023 (GLIBC 2.34，压测在 Docker Ubuntu 24.04 / GLIBC 2.39 内) |
| Region | ap-northeast-1 (东京) |
| Flink | 1.16.3 (local standalone in Docker) |
| lance-flink | [`fc3d064`](https://github.com/lance-format/lance-flink/commit/fc3d064) (HEAD, 2026-01-08, "translate Chinese comments") + 2 本地 patch |
| lance-core | 两轮分别测 0.23.3 (2025-03) 和 0.39.0 (2026-03) |
| JDK | OpenJDK 11.0.30 (Corretto for client, Debian for Flink runtime) |
| S3 bucket | `s3://lance-benchmark-<ACCOUNT_ID>-ap-northeast-1/stress-test/` |

## 压测配置

| 参数 | 值 |
|---|---|
| Flink parallelism | 4 |
| 目标速率 | 10,000 rows/sec |
| `write.batch-size` | 1024 |
| 单行 | `id BIGINT, ts BIGINT, payload VARCHAR(200)` (~90 字节) |
| checkpoint.interval | 10s |
| checkpoint.timeout | 60s |
| tolerable-failed-checkpoints | 0（严格） |
| restart-strategy | fixed-delay, 20 attempts × 5s |
| Compactor | Python tight loop, `dataset.optimize.compact_files()`, sleep 2s |
| Phase 1 (baseline) | ~120-580s |
| Phase 2 (concurrent) | 420s |
| Phase 3 (recovery) | 120s |

## 引用

如引用本报告数据，请注明来源 commit 和测试日期：
- Repo: https://github.com/ZackFairTS/lance-test
- 测试日期: 2026-04-28
- lance-flink HEAD: `fc3d064ace4bbdbf29e22a489db2c5bf61a36990`

## License

MIT, 原始测试数据按 AS-IS 提供。
