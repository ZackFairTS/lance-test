# lance-docs

Lance 相关的调研 / benchmark / 设计文档集合。所有文档均基于源码证据 + 实测数据，非二手转述。

---

## 文档列表

### [Lance Table 组合字段查询方案设计 →](composite-key-index.html)

针对 `WHERE video_id = X AND frame_id = Y` 这类多字段组合查询，在 Lance Table
上如何最高效实现？是否应通过"拼接字段成单列"的方式绕过 Lance 缺乏原生联合索引
的限制？

**结论**：不应拼接。维护者推荐"每列独立索引 + 引擎 AND 相交"，实测 V1 双 BTREE
方案在 10M 行数据达到 1.80 ms p50，与拼接方案的 1.65 ms 几乎持平且保留所有单列
查询能力。

涵盖：Lance 7 种标量索引的谓词形态分类、`ScalarIndexExpr::And` 源码机制、五种
实测方案对比（V0-V4）、V4 排序+BTREE 的工作原理与 compaction 行为。

### [小文件对读性能影响 - 压测报告 →](read-perf-bench.html)

频繁 commit 导致大量小 fragment，对读性能的打击有多大？compaction 能恢复多少？

**实测**（10M 行，同一 S3 dataset 的 5 个 fragment 档位 1 / 10 / 100 / 1000 / 5000
× 6 种读操作 + Spark 对照）：**严重依赖读取方式**。Python 单进程 full scan / range
query 在 5000 fragments 下比 1 fragment 慢 **10–17.7x**；但 **Spark 分布式读几乎
无退化**（7.4s → 6.4s）。Per-fragment open ~40ms 固定开销（lance#4090）是根因，
单线程串行累积，并行读完全掩盖。

配套实测：compact 成本 (5000 → 1 frag 全程 73 秒)，远低于 append 耗时。

### [并发 mutation × compaction 冲突实测 →](q-concurrent-mutation-compact.html)

高并发写入并伴随 compaction 是否存在冲突导致任务失败的风险？

**实测**（500k 行 × 16 scenarios × 63,338 总操作数，pylance 4.0.1）：writer 端
**100% 成功率**（10×30s 外层 retry 兜住所有语义冲突）；但 **compactor 本身 1–3%
失败率** 在 concurrent update/merge_insert 下（`compact_files()` 无外层 retry，
RetryableCommitConflict 直接抛 RuntimeError）。

结论：writer 不需要冲突处理，但 compaction scheduler 必须 try/except 并重试。

### [N — Lance Compaction × Index 审计 →](n-compact-index-audit.html)

Lance compact_files() 和 scalar/vector index 在 9 种索引 × 2 个读路径 × 2 个版本
下的交互行为审计（2026-05-09 初版）。

揭示 ZONEMAP / BLOOMFILTER 在 defer-index-remap 模式下的已知问题（返回 0 行，
6 个月未修）以及其它 compact 后的 index 状态边界。

包含独立的 MVCC 字节记账修正（区分 MVCC-active 与 total-on-disk size）。
