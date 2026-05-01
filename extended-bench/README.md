# Lance 扩展压测 - Top 4 + Tier 2（共 10 项）

10 个测试覆盖 Lance 核心能力，每个脚本通过 **opencode quick review + ai-slop-remover**。

## 📋 测试清单

### Tier 0 + 1 (Top 4)
- **A. `update()` + stable_row_ids** → 🔴 实测 **11,494x 慢**（复现 [#6404](https://github.com/lance-format/lance/issues/6404)）
- **B. Filter vs Parquet** → 🔴 Lance v2.1 仍**慢 2.49x**（[#738](https://github.com/lance-format/lance/issues/738) 3 年未解）
- **D. 向量搜索 Pareto** → ✅ IVF_HNSW_SQ 赢家（recall≥0.95 时 535 QPS）
- **E. Prefilter + HNSW** → 🟡 10% 边界**相位跃迁实锤**

### Tier 2（本轮）
- **F. Merge-insert 吞吐** → 🔴 **BTREE index 让 merge_insert 慢 ~500x**（违反官方建议）
- **G. Schema evolution 零重写** → ✅ 真的零写（0.6-0.8 ms），比 Parquet 快 130x
- **H. Version 爆炸 open() 成本** → `list_versions()` **平方级增长**
- **I. Compression ratio** → 🔴 Lance 对 vector/embedding **完全无压缩**（印证 [#3705](https://github.com/lance-format/lance/discussions/3705)）
- **J. Format v2.0 vs v2.1** → 🔴 v2.1 full scan **慢 2.44x**（和 B 一致）
- **K. FTS 内存 envelope** → Peak RSS = 3-5x input（印证 [#5502](https://github.com/lance-format/lance/issues/5502)）

## 🐛 opencode Review 价值证明

Review 在 6 个脚本中捕获关键 bug，包括：
- **E 脚本 ✴️ 致命**：两个 index 用同一 dataset + replace=True **互相覆盖**，如未修复整个 PQ-vs-HNSW 对比完全无效
- **G 脚本**：alter_columns API 用错 param name + 试 rename 不存在的列
- **H 脚本**：append 破坏性操作污染后续测量
- **K 脚本**：`ru_maxrss` 是累积 peak，非递增配置下 `rss_delta` 无意义

## 📊 报告

- [REPORT.md](REPORT.md) - Top 4 完整分析（update bug + filter vs parquet + vector search + prefilter）
- [REPORT_tier2.md](REPORT_tier2.md) - Tier 2 六项分析 + 跨 10 项总结

## 📁 数据

- `scripts/` - 10 个经 review 的脚本 (A-K)
- `data/` - 10 个 raw JSON 结果
