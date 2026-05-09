# Lance 压测实证报告集

这个 repo 包含四个独立的 Lance 压测研究，外加扩展对比（格式矩阵 + Lance vs Iceberg）。

> **🎯 先看 [META_ANALYSIS.md](META_ANALYSIS.md)** —— 跨所有 5 个研究的综合分析与生产建议。
>
> **🐛 [issues/decimal_sorted_bloat.md](issues/decimal_sorted_bloat.md)** —— 本次研究发现的最大 bug，待提交到 lance-format/lance。
>
> **🔴 [extended-bench/REPORT_N_compact_index.md](extended-bench/REPORT_N_compact_index.md)** —— **NEW**：Compaction × Index 交互审计，发现 5 个 silent correctness bug + 1 个 v6 回归，跨 pylance 4.0.1 + 6.0.0-rc.4 确认。

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

### 4️⃣ [Lance 扩展压测 Top 4](extended-bench/) — update/filter/vector-search/prefilter

4 个关键未测能力，每个脚本通过 **opencode AI review**：

| 测试 | 核心发现 |
|---|---|
| A. `update()` + stable_row_ids bug | 🔴 **11,494x 慢**（复现 issue #6404）|
| B. Filter vs Parquet | 🔴 Lance v2.1 仍**慢 2.49x**（issue #738 3 年未解）|
| D. 向量搜索 Pareto (SIFT-1M) | ✅ IVF_HNSW_SQ 赢家，recall≥0.95 时 535 QPS |
| E. Prefilter + HNSW | 🟡 10% 边界**相位跃迁实锤**，p50 翻倍 |

详见 [extended-bench/REPORT.md](extended-bench/REPORT.md)。

**Tier 2 六项补充测试** (F/G/H/I/J/K) 见 [extended-bench/REPORT_tier2.md](extended-bench/REPORT_tier2.md)：
- 🔴 **Merge-insert + BTREE = 慢 ~500x**（违反官方建议）
- ✅ Schema evolution 零写（比 Parquet 快 130x）
- 🔴 Lance 对 vector/embedding **完全无压缩**（证实 [#3705](https://github.com/lance-format/lance/discussions/3705)）
- 🔴 Lance **v2.1 full scan 比 v2.0 慢 2.44x**（又一个 v2.1 回归）
- `list_versions()` 版本数**平方级增长**
- FTS 内存 envelope 3-5x input size

**Fair-Filter 修正** ([extended-bench/REPORT_fair_filter.md](extended-bench/REPORT_fair_filter.md)) - 回应"加了 scalar index 才公平"：
- Lance **BITMAP 在低选择率** (<1%) 大幅领先 (最多 14x)
- **Parquet 在中高选择率** (>1.5%) 反超
- 🔴 **发现 Lance query planner bug**：高选择率 (>10%) 下 BITMAP 反而拖慢 11x，说明 Lance 不会根据选择率自动跳过索引

**🎯 Spark 中立引擎修正** ([extended-bench/REPORT_spark_neutral.md](extended-bench/REPORT_spark_neutral.md)) - 回应"之前 Lance 用 DataFusion, Parquet 用 PyArrow, 引擎不同"：
- 在**同一 Spark SQL 引擎**下重跑
- **结论显著改变**: Lance BITMAP 在低选择率 (≤10%) 全面领先 Parquet **1.4-3.1x**
- 高选择率 (50%) 仍是 Parquet 赢（query planner bug 跨引擎存在）
- **PyArrow 给 Parquet 隐形加持 5-6x** —— 引擎公平性确实很关键

**L2 — 格式版本 × 工作负载矩阵** ([extended-bench/REPORT_L2_format_compare.md](extended-bench/REPORT_L2_format_compare.md)) - `4 workloads × 5 formats × 6 ops` 的完整矩阵：
- 🔴 **Lance v2.2 Blob V2 在少量随机 take 比 v2.0 large_binary 慢 20x**（ML notebook 场景反效果）
- 🔴 **Lance v2.1 在 vector full_scan 比 v2.0 慢 54%**（新的 v2.1 回归，不同于 J 里的 flat 回归）
- ✅ **Lance 向量列读比 Parquet 快 10-14x**（最大的单项胜利）
- ✅ **v2.2 是唯一支持 map 的版本**；nested full_scan 比 Parquet 快 1.8-2.2x
- 🔴 **nested subread Lance 慢 1.6x**；struct 子字段下推不如 Parquet

**M 系列 — Lance v2.2 vs Iceberg v2 MoR on TPC-DS** ([extended-bench/REPORT_M_lance_vs_iceberg.md](extended-bench/REPORT_M_lance_vs_iceberg.md)) ⭐ —— 第一次跟 Iceberg 完整 stack 对比，sf1 + sf10 真实 TPC-DS：
- 🔴 **Lance 存储大 2.4x** (same zstd-3)；decimal 列膨胀 **5.7x**，低基数 string 列膨胀 **27-35x**
- 🔴 **col_scan 慢 1.57x**（sf10），**filter 全选择率都慢 1.33-2.69x**
- ✅ **DELETE 快 5-8x + 几乎无读放大**（Iceberg MoR 删 10% 后读变慢 51%，Lance 不变慢）
- ✅ **20 次 append + compact 读比 Iceberg 快 2.6x**；compact 本身快 3x
- 🟡 Lance `compact_files()` 不 GC 旧数据，size 反而大 76%

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

**另外验证了 Lance 官方声称的 "20-25K rows/s on S3" 数字** (见 [REPORT_pure_take.md](ml-training-bench/REPORT_pure_take.md)): 
- 纯 `take_blobs` (256 per-row workers) 最高 **4,391 rows/s**，是官方数字 ~20%
- Lance 比 raw S3 files 快 10x（真实价值范围）
- 官方 "20-25K" 对应的可能是小记录（KB 级 embedding），不是图片 blob

---

### 5️⃣ [Compaction × Index 交互审计 (N 系列)](extended-bench/REPORT_N_compact_index.md) ⭐⭐⭐ NEW — 9 index × 2 path × 2 version 矩阵

> **核心问题**: `compact_files()` 后，已有的标量/向量索引是否需要重建？
>
> **实测结论**: **大部分 index 不需要重建**（上游文档承诺为真），**但 ZONEMAP / BLOOMFILTER / IVF_* defer 路径有严重 bug，跨 pylance 4.0.1 + 6.0.0-rc.4 (源码编译) 都存在，6 个月未修**。
>
> **脚本已通过 opencode ai-slop-remover review**：review 发现并修复 4 处断言弱化（pre==gt 未 assert、pre-error 误 pass、FTS 用 substring 近似 BM25、bitmap 用 intersection 代替 containment），修复后所有 bug 结论保留，**未引入新的 false positive**。

**核心数据** (100K 行 × 10 fragments → compact 到 1 fragment，基于 upstream Rust 测试的 6 条不变式 assertion)：

| Index 类型 | default 路径 | defer 路径 | 状态 |
|---|---|---|---|
| BTREE / BITMAP / LABEL_LIST / NGRAM / INVERTED | ✅ | ✅ | 全部正确 |
| IVF_HNSW_SQ | ✅ | ✅ (4.0.1) / 🔴 **v6 新回归** | 跨版本不稳定 |
| IVF_PQ | ✅ | 🔴 Rust error "fragment id 0" | defer 路径崩溃 |
| **ZONEMAP** | 🔴 UUID/bitmap 不更新 | 🔴 **查询返回 0 行** | 🔴 两个版本都坏 |
| **BLOOMFILTER** | 🔴 UUID/bitmap 不更新 | 🔴 **查询返回 0 行** | 🔴 两个版本都坏 |

**关键发现**:
- 🔴 **ZONEMAP / BLOOMFILTER + `defer_index_remap=True` 静默返回错误结果**（silent correctness bug，生产环境最危险的一类 bug）
- 🔴 Upstream `rust/lance/src/dataset/optimize.rs` 里 **没有 CI 测试** 的 index 类型（ZONEMAP、BLOOMFILTER、RTree）实测 bug 率 100%
- 🔴 **IVF_HNSW_SQ defer 是 v6 新回归**（4.0.1 → 6.0.0-rc.4 期间某个 commit 引入）
- ✅ 5/9 主流 index（BTREE/BITMAP/LABEL_LIST/NGRAM/INVERTED）在两条 compact 路径两个版本下都完全正确

**实用建议**:
- ✅ **不用手动 `create_index(replace=True)` 重建** —— 上游文档承诺是真实的（对能工作的 index）
- ⛔ **不要把 `defer_index_remap=True` 带到生产环境**（节省时间极小，bug 暴露面大）
- ⛔ **ZONEMAP / BLOOMFILTER + compact 任何路径都不安全**（哪怕文档说 supported）

**文档**: [extended-bench/REPORT_N_compact_index.md](extended-bench/REPORT_N_compact_index.md) —— 包含可直接提交到 `lance-format/lance` 的 issue 草稿。

**Raw data**: `extended-bench/data/N_compact_index_pylance_4.0.1.json`, `N_compact_index_pylance_6.0.0rc4.json`
**脚本**: [extended-bench/scripts/N_compact_index.py](extended-bench/scripts/N_compact_index.py)
