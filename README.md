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

**关键数据**（主指标: wall-clock 延迟）:

| 读方式 | A (1 frag) | E (5000 frags) | 退化 |
|---|---|---|---|
| Python 单进程全表扫描 | 804 ms | 8003 ms | 🔴 **10.0x 慢** |
| Python 范围查询 | 2.3 秒 | 40.5 秒 | 🔴 **17.7x 慢** |
| Python 单列扫描 | 142 ms | 2126 ms | 🔴 **15.0x 慢** |
| Python 点查 (take 1000) | 944 ms | 968 ms | 🟢 几乎无差 |
| Dataset.open() | 80 ms | 134 ms | 🟡 1.7x |
| **Spark 分布式全表读** | 7.4 s | 6.4 s | 🟢 **无退化** |

**洞察**: 小文件问题 = 单线程 I/O 串行化 + per-fragment ~40ms 打开开销 ([lance#4090](https://github.com/lancedb/lance/issues/4090))。Spark 的并行读反而让小文件成了"更细粒度的并行度"，完全掩盖了这个问题。

> ⚠️ **MB/s 数字的正确解读**：报告里的 "1182 MB/s → 118 MB/s" 是 `pyarrow.Table.nbytes / elapsed`（Arrow 内存吞吐），**不是 S3 网络带宽**。Lance 默认开 64 并行 S3 GET，且 on-disk 有压缩，实际 S3 传输量比 Arrow 字节数小 2-5 倍。Lance 官方 benchmark 和 arXiv paper 都用 **ms + rows/sec**，不用 MB/s。详见 [read-perf-bench/REPORT.md](read-perf-bench/REPORT.md)。

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


---

### 3️⃣ [Lance ML 训练场景压测](ml-training-bench/) — 宽列 blob 点查 + PyTorch DataLoader

> **核心问题**: 图片 blob + 随机 batch sampling 训练，Lance 是不是好方案？
>
> **实测结论**: **比 Parquet 快 2.13x, 比 raw files 快 1.49x**。但远低于 LanceDB 官方声称的 "100-2000x vs Parquet"。

**实测稳态 (20K images × 200KB JPEG, S3 ap-northeast-1, batch=256, workers=8)**:

| 方案 | img/s | 相对 Lance |
|---|---|---|
| **Lance v2.2** | **237** ⭐ | 1.00x |
| Raw S3 files | 159 | 0.67x |
| Parquet | 111 | 0.47x |

**关键 bug 发现**:
- pylance 4.0.1 `take_blobs` **不接受乱序 indices**（shuffle DataLoader 直接不能用，必须 workaround）
- `SafeLanceDataset` 对 blob 列只返回 descriptor 不是 bytes
- v2.2 不再接受旧的 `lance-encoding:blob=true` metadata

详见 [ml-training-bench/REPORT.md](ml-training-bench/REPORT.md)。
