# Lance 扩展压测 Top 4

验证 Lance 4 个关键未测能力，每个脚本通过 **opencode code review**：

| 测试 | 核心发现 |
|---|---|
| **A. `update()` + stable_row_ids bug** | 实测 **11,494x 慢**（500K 行 TIMEOUT > 300s）- 复现 issue [#6404](https://github.com/lance-format/lance/issues/6404) |
| **B. Filter vs Parquet** | Lance v2.1 仍**慢 2.49x** - 证伪 LanceDB v2.2 blog "tied with Parquet" 声称，issue [#738](https://github.com/lance-format/lance/issues/738) 3 年未解 |
| **D. 向量搜索 Pareto (SIFT-1M)** | IVF_HNSW_SQ 赢家，recall≥0.95 时 535 QPS（vs IVF_PQ 317, IVF_RQ 340）|
| **E. Prefilter + HNSW** | 10% 边界**相位跃迁实锤**，p50 在 5%→10% 翻倍 (2.8→5.4ms)，证实源码 `remained < self.len() * 10 / 100` |

## Review-Driven Development

每个脚本都过 `Sisyphus-Junior quick` category + `ai-slop-remover` skill 审查。发现的关键 bug：
- **E 脚本致命 bug**: 两个 index 用同一 dataset + replace=True 互相覆盖（如未修复，整个 PQ vs HNSW 对比**全部无效**）
- **B 脚本**: p99 at n=5 返回的是 max（误导性指标）
- **A 脚本**: 缺 timeout，500K rows TIMEOUT 的 case 会挂死

## 文档

- [REPORT.md](REPORT.md) - 完整报告
- `scripts/` - A/B/D/E 4 个脚本
- `data/` - 原始 JSON 结果
