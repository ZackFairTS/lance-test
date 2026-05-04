# Lance 扩展压测 — Top 4 + Tier 2 + Fair Filter + L2 格式矩阵 + M 系列 TPC-DS vs Iceberg

每个脚本通过 **opencode quick review + ai-slop-remover**。

## 📋 测试清单

### Tier 0 + 1 (Top 4)
- **A. `update()` + stable_row_ids** → 🔴 **11,494x 慢**（复现 [#6404](https://github.com/lance-format/lance/issues/6404)）
- **B. Filter vs Parquet (native)** → Lance 慢 2.49x（⚠️ 引擎不对称）
- **D. 向量搜索 Pareto** → ✅ IVF_HNSW_SQ 赢家
- **E. Prefilter + HNSW** → 🟡 10% 边界相位跃迁

### Tier 2
- **F. Merge-insert + BTREE** → 🔴 **慢 500x**
- **G. Schema evolution 零重写** → ✅ 比 Parquet 快 130x
- **H. Version 爆炸 open()** → `list_versions` 平方级
- **I. Compression** → 🔴 vector 无压缩
- **J. Format v2.0 vs v2.1** → 🔴 v2.1 scan 慢 2.44x
- **K. FTS 内存 envelope** → 3-5x input size

### Fair Filter 三层修正
- **B2. Filter + scalar index** → Lance BITMAP 最优但仍慢 1.73x (native engine)
- **B3. 选择率扫描 (native)** → 低选择率 Lance 赢 14x，高选择率 Parquet 赢 11x
- **B4. Spark 中立引擎** → **真正公平对比**，结论显著改变：
  - Lance BITMAP 在低选择率 (<10%) 全面领先 Parquet 1.4-3.1x
  - 只在高选择率 (50%) 被 Parquet 反超
  - PyArrow 给 Parquet "隐形加持" 5-6x

### L2 — 格式版本 × 工作负载矩阵 ⭐ (NEW 2026-05-02)
**4 workloads × 5 formats × 6 ops 的完整矩阵**（1M rows）
- 🔴 **Lance v2.2 Blob V2 比 v2.0 large_binary 慢 20x**（少量随机 take 场景）
- 🔴 **Lance v2.1 在 vector full_scan 上比 v2.0 慢 54%**（新的 v2.1 回归，与 J 不同路径）
- ✅ **Lance 比 Parquet 在 vector workload 上快 10-14x**（最大胜利）
- ✅ **v2.2 是唯一支持 map 的版本**，nested full_scan 比 Parquet 快 1.8-2.2x
- 🔴 **nested subread Lance 慢 1.6x**，struct 子字段下推不如 Parquet

### M 系列 — Lance v2.2 vs **Iceberg v2 MoR** on TPC-DS ⭐⭐ (NEW 2026-05-02)
**第一次把 Lance 跟完整 Iceberg stack 对比**，sf1 (2.88M) + sf10 (28.8M) 真实 TPC-DS 数据
- 🔴 **Lance 存储比 Iceberg 大 2.4x** (sf10 store_sales, same zstd-3)
  - Decimal 列膨胀 5.7x, 低基数 string 膨胀 27-35x
- 🔴 **Lance col_scan 比 Iceberg 慢 1.57x** (sf10 store_sales)
- 🔴 **Lance BITMAP filter 在 1%-50% 选择率上都比 Iceberg min/max 慢 1.33-2.69x**
- ✅ **Lance DELETE 比 Iceberg MoR 快 5-8x**，且读放大几乎为 0（Iceberg MoR 读变慢 51%）
- ✅ **Lance 20 次 append + compact 读比 Iceberg 快 2.6x**
- 🟡 **Lance `compact_files()` 不 GC 旧数据**，size 反而大 76%

## 📊 报告

| 报告 | 覆盖 |
|---|---|
| [REPORT.md](REPORT.md) | Top 4（A/B/D/E） |
| [REPORT_tier2.md](REPORT_tier2.md) | Tier 2 六项（F/G/H/I/J/K） |
| [REPORT_fair_filter.md](REPORT_fair_filter.md) | 加 scalar index 的 native engine 对比 |
| [REPORT_spark_neutral.md](REPORT_spark_neutral.md) | Spark 中立引擎真正公平对比 |
| **[REPORT_L2_format_compare.md](REPORT_L2_format_compare.md)** ⭐ | **L2 格式 × 工作负载矩阵** |
| **[REPORT_M_lance_vs_iceberg.md](REPORT_M_lance_vs_iceberg.md)** ⭐⭐ | **M 系列 Lance vs Iceberg (TPC-DS)** |

## 🐛 Review 价值

Review 在多个脚本中 catch 到致命 bug：
- **E**: 两个 index 互相覆盖（会让 PQ vs HNSW 对比完全无效）
- **G**: alter_columns API param 写错
- **H**: 破坏性 append 污染测量
- **K**: ru_maxrss 累积 peak 不能 delta
- **B4**: `count()` 触发 ColumnPruning 不测 filter，改用 `write.format("noop")`
- **M1**: `.using("iceberg")` 静默丢 tableProperty (Spark 3.5 DSv2 hint 冲突)
- **M4**: selectivity calibration 不可行时必须 skip，不能伪造数据
- **M5**: pyiceberg 不能写（NoopCatalog + CoW fallback），必须 Spark 写 + pyiceberg 读
- **M5 B1 级 bug**: `aws s3 cp` + pyiceberg write 会写回源表的 metadata.location，需要 CTAS + location guard
- **M6**: Spark createDataFrame silent promote BIGINT→DOUBLE，EMR YARN worker 看不见 driver `/tmp`（必须走 HDFS）
