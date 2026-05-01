# Lance 扩展压测 - Top 4 + Tier 2 + Fair-Filter（11 个测试）

每个脚本通过 **opencode quick review + ai-slop-remover**。

## 📋 测试清单

### Tier 0 + 1 (Top 4)
- **A. `update()` + stable_row_ids** → 🔴 **11,494x 慢**（复现 [#6404](https://github.com/lance-format/lance/issues/6404)）
- **B. Filter vs Parquet (初版, 无索引)** → 🔴 Lance v2.1 **慢 2.49x**
- **D. 向量搜索 Pareto** → ✅ IVF_HNSW_SQ 赢家
- **E. Prefilter + HNSW** → 🟡 10% 边界相位跃迁

### Tier 2
- **F. Merge-insert + BTREE** → 🔴 **慢 ~500x**（违反官方建议）
- **G. Schema evolution 零重写** → ✅ 比 Parquet 快 130x
- **H. Version 爆炸 open() 成本** → `list_versions` 平方级增长
- **I. Compression** → 🔴 vector/embedding 无压缩（证实 #3705）
- **J. Format v2.0 vs v2.1** → 🔴 v2.1 full scan 慢 2.44x
- **K. FTS 内存 envelope** → 3-5x input size

### Fair-Filter 修正（B 的公平修正版 ⭐）
- **B2. Filter + 各种 scalar index** → Lance BITMAP 最优，但仍比 Parquet **慢 1.73x**
- **B3. 选择率扫描** → 🔴 发现 **Lance query planner bug**：
  - 选择率 < 1% → Lance BITMAP 快 14x
  - **选择率 > 10% → Lance BITMAP 反而慢 11x**（索引在高选择率不该用）

## 📊 报告

- [REPORT.md](REPORT.md) - Top 4
- [REPORT_tier2.md](REPORT_tier2.md) - Tier 2 六项
- [REPORT_fair_filter.md](REPORT_fair_filter.md) - **公平对比后的 Filter vs Parquet**（修正 B）

## 🐛 Review 价值证明

Review 在 6+ 个脚本里 catch 到致命/严重 bug：
- **E 致命**: 两个 index 互相覆盖
- **G**: alter_columns API 用错
- **H**: append 破坏性测量
- **K**: ru_maxrss 累积 peak 不能 delta
- **B2**: 加 rows_returned 一致性校验
