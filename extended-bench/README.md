# Lance 扩展压测 - Top 4 + Tier 2 + Fair Filter 三层修正（共 12 个测试）

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

### Fair Filter 三层修正 ⭐
- **B2. Filter + scalar index** → Lance BITMAP 最优但仍慢 1.73x (native engine)
- **B3. 选择率扫描 (native)** → 低选择率 Lance 赢 14x，高选择率 Parquet 赢 11x
- **B4. Spark 中立引擎** → **真正公平对比**，结论显著改变：
  - Lance BITMAP 在低选择率 (<10%) 全面领先 Parquet 1.4-3.1x
  - 只在高选择率 (50%) 被 Parquet 反超
  - PyArrow 给 Parquet "隐形加持" 5-6x

## 📊 报告

- [REPORT.md](REPORT.md) - Top 4 报告
- [REPORT_tier2.md](REPORT_tier2.md) - Tier 2 六项
- [REPORT_fair_filter.md](REPORT_fair_filter.md) - 加 scalar index 但仍 native engine
- [REPORT_spark_neutral.md](REPORT_spark_neutral.md) ⭐ - **Spark 作为中立引擎的真正公平对比**

## 🐛 Review 价值

Review 在多个脚本中 catch 到致命 bug：
- **E**: 两个 index 互相覆盖（会让 PQ vs HNSW 对比完全无效）
- **G**: alter_columns API param 写错
- **H**: 破坏性 append 污染测量
- **K**: ru_maxrss 累积 peak 不能 delta
- **B4**: `count()` 触发 ColumnPruning 不测 filter，改用 `write.format("noop")`
