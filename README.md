# Lance 压测实证报告集

这个 repo 包含两个独立的 Lance 压测研究，对应两个不同的生产问题。

---

## 📚 研究索引

### 1️⃣ [lance-flink Connector 压测](.) — Flink 流式写入 vs Compaction 并发

> **核心问题**: Lance table on S3 流式写入时，同时做 compaction，是否会打断写入？
>
> **实测结论**: **会**，会以 Flink job restart 的形式打断，并导致 40-68% 的数据重复。

**关键数据**:
- Flink job 12 分钟内 restart 2-5 次
- 数据重复率: 40.7% (lance-core 0.23.3) / 68.4% (lance-core 0.39.0)
- 升级 lance-core 到最新反而更糟
- 根本原因是 `lance-flink` 的架构缺陷（非 SinkV2, sync-phase commit）

**文档**:
- [00_SUMMARY.md](00_SUMMARY.md) — 总结（先读这个）
- [01_REPORT_lance_0.23.3.md](01_REPORT_lance_0.23.3.md) — 0.23.3 完整报告
- [02_REPORT_lance_0.39.0.md](02_REPORT_lance_0.39.0.md) — 0.39.0 对比报告
- [03_BACKGROUND_research.md](03_BACKGROUND_research.md) — 压测前源码研究
- [04_CONNECTOR_BUGS.md](04_CONNECTOR_BUGS.md) — 实测发现的 2 个严重 Bug
- [05_HOW_TO_REPRODUCE.md](05_HOW_TO_REPRODUCE.md) — 复现步骤

**Raw data**: `data/run1_lance_0.23.3/`, `data/run2_lance_0.39.0/`
**脚本**: `scripts/`, `stress-job/`, `Dockerfile`, `patches/`

---

### 2️⃣ [Lance 小文件读性能测试](read-perf-bench/) — Fragment 数量对读性能的影响

> **核心问题**: 频繁 commit 导致几千个小 fragment 后，读性能会差多少？Compaction 能恢复多少？
>
> **实测结论**: **强烈依赖读取方式**。Python 单进程读 5000 fragments 比 1 fragment 慢 10-18 倍，但 Spark 分布式读几乎无差。

**关键数据**:

| 读方式 | 5000 frag vs 1 frag |
|---|---|
| Python 单进程全表扫描 | 🔴 10.0x 慢 (1182→118 MB/s) |
| Python 范围查询 | 🔴 17.7x 慢 (2.3→40.5 秒) |
| Python 单列扫描 | 🔴 15.0x 慢 |
| Dataset.open() | 🟡 1.7x 慢 (80→134 ms) |
| Python 点查 | 🟢 几乎无差 |
| **Spark 分布式全表读** | 🟢 **几乎无差** (7.4→6.4 秒) |

**洞察**: 小文件问题 = 单线程 I/O 串行化问题。Spark 的并行度完全掩盖了这个问题。

**文档**: [read-perf-bench/README.md](read-perf-bench/README.md), [read-perf-bench/REPORT.md](read-perf-bench/REPORT.md)

**Raw data**: `read-perf-bench/data/`
**可视化**: `read-perf-bench/plots/performance_plot.png`

---

## 测试环境

| 项 | 值 |
|---|---|
| 硬件 | AWS EMR master, r8g.2xlarge (Graviton ARM64, 8vCPU, 64 GiB) |
| OS | Amazon Linux 2023 (GLIBC 2.34) |
| Region | ap-northeast-1 (东京) |
| Python Lance | pylance 4.0.1 (lance-core 0.39.0 native) |
| Spark | 3.5.5-amzn-1 + lance-spark 0.0.15 |
| Flink | 1.16.3 (local standalone in Docker Ubuntu 24.04) |
| S3 bucket | `s3://lance-benchmark-<ACCOUNT_ID>-ap-northeast-1/` |

## 两个测试的共同启示

两个完全不同角度的测试都指向同一个底层问题：**Lance 的设计在小文件/高频 commit 场景下有明显代价**。

- **写侧**: lance-flink connector 的同步 commit + sync-phase checkpoint 在高并发下触发 Flink restart
- **读侧**: Python 单进程读小文件 dataset 被 I/O 串行化拖死

**两者的解决方案也有共性**:
- **减少 commit 频率**（大 batch_size, 长 checkpoint interval）
- **定期 compact**（读侧直接改善读性能；写侧减少冲突）
- **用并行 runtime 读取**（Spark / 多线程 worker pool）

## 引用

如引用本报告数据：
- Repo: https://github.com/ZackFairTS/lance-test
- 压测 1（Flink）日期: 2026-04-28
- 压测 2（读性能）日期: 2026-04-29
- lance-flink HEAD: `fc3d064ace4bbdbf29e22a489db2c5bf61a36990`
- lance-core: 0.23.3 & 0.39.0

## License

MIT, 原始测试数据按 AS-IS 提供。
