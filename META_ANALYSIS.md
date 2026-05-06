# Lance in Production: What I Learned from 5 Independent Stress Tests

**作者**: Lance 压测实证项目
**测试期**: 2026-04-28 → 2026-05-02
**环境**: AWS EMR r8g.2xlarge (Graviton), S3 ap-northeast-1
**版本**: lance-core 0.23.3 + 0.39.0, pylance 4.0.1, lance-spark 0.0.15, lance-flink HEAD (fc3d064)

这是一篇跨 5 个独立研究的综合分析。每个子研究都有自己的详细报告，这里把它们放在一起，回答一个问题：**Lance 在 2026 年的生产就绪度到底如何？**

> TL;DR：**Lance 不是 Parquet 替代品**，它是一个专注于 mutation + vector + blob 的新品类。在它擅长的领域（DELETE、小文件治理、向量搜索、ML training blob access）它显著领先；在传统 OLAP（存储、扫描、过滤）它仍比 Parquet 落后 2-3x。

---

## 目录

1. [五个独立研究](#五个独立研究)
2. [跨研究的共同发现](#跨研究的共同发现)
3. [Lance 擅长的场景](#lance-擅长的场景)
4. [Lance 不擅长的场景](#lance-不擅长的场景)
5. [Lance 的结构性缺陷](#lance-的结构性缺陷)
6. [方法论教训](#方法论教训)
7. [生产建议](#生产建议)

---

## 五个独立研究

### 1. [Flink 流写 vs Compaction 并发](../01_REPORT_lance_0.23.3.md) (2026-04-28)
**问题**: 流式写入 + compaction 并发能工作吗？
**结论**: lance-flink HEAD 自带 2 个严重 bug 让 connector 开箱不可用；即使打 patch，高负载下 12 分钟内 Flink job restart 2-5 次、数据重复率 40-68%。**升级 lance-core 0.23.3 → 0.39.0 让情况更糟**（checkpoint 失败率 15.6% → 45.8%）。

### 2. [Fragment 数量对读性能的影响](../read-perf-bench/REPORT.md) (2026-04-29)
**问题**: 几千个小 fragment 后读性能退化多少？Compaction 能恢复多少？
**结论**: 强烈依赖读取方式。Python 单进程读 5000 fragments 比 1 fragment 慢 10-18x；**但 Spark 分布式读几乎无差别**。Per-fragment open 固定开销 ~40ms 是罪魁。

### 3. [ML 训练场景（宽列 blob 点查）](../ml-training-bench/REPORT.md) (2026-04-30)
**问题**: Lance 是不是 ML DataLoader 的好方案？
**结论**: 比 Parquet 快 2.13x，比 raw S3 files 快 1.49x。但 `take_blobs` **不接受乱序 indices**（shuffle 直接不能用），且**远低于官方声称的 20-25K rows/s**（实测 237）。

### 4. [扩展压测 Top 4 + Tier 2 + Fair Filter](../extended-bench/REPORT.md) (2026-04-30 ~ 2026-05-01)
**问题**: update / filter / vector / prefilter + 其余 6 项的全面覆盖
**结论**: `update()+stable_row_ids` 实测 **11,494x 慢** (issue #6404)；filter 比 Parquet 慢 2.49x (issue #738 三年未解)；IVF_HNSW_SQ 在向量搜索是帕累托赢家；发现 **Lance BITMAP 在高选择率反而拖慢 11x** 的 query planner bug。

### 5. [L2 格式矩阵 + M Lance vs Iceberg](../extended-bench/REPORT_L2_format_compare.md) (2026-05-02)
**问题**: 格式版本 × 工作负载完整矩阵 + 跟 Iceberg v2 MoR 正面对抗
**结论**: **Lance v2.2 Blob V2 在少量随机 take 比 v2.0 large_binary 慢 20x**；v2.1 在 vector full_scan 上比 v2.0 **回归 54%**；TPC-DS 上 Lance 存储比 Iceberg 大 **2.4x**（decimal 列 5.7x，低基数 string 27-35x）；但 **Lance DELETE 比 Iceberg MoR 快 5-8x**，读放大几乎为零。

---

## 跨研究的共同发现

### 🎯 共同主题 1：小文件 / 高频 commit 是 Lance 的基因弱点

**三个独立研究全部指向同一点**：

| 研究 | 证据 |
|---|---|
| [Flink 压测](../01_REPORT_lance_0.23.3.md) | 2000+ 个 manifest version 让 `Dataset.open()` 慢到让 checkpoint 超时 60s |
| [read-perf-bench](../read-perf-bench/REPORT.md) | 5000 fragments 下 Python 单进程读慢 10-18x，build 本身 append 速率从 3.5/s 退化到 2.4/s |
| [M6 compact](../extended-bench/REPORT_M_lance_vs_iceberg.md#m6-small-files-pathology--compaction) | 20 次 append 后 Lance 比 Iceberg 读快 2.6x、compact 快 3x、append 快 5x。Compact 后目录字节涨 76% 是 **MVCC 历史保留**（旧 version fragments 支撑 time travel），不是 bug —— [`cleanup_old_versions()`](../extended-bench/REPORT_M_lance_vs_iceberg.md#compact-后到底发生了什么) 回收历史后 active size 反而比 pre-compact 小 5%。Iceberg 对称（`expire_snapshots`）。|

**根因**: Lance 的 manifest-per-commit 设计在 high-frequency 写场景下把 cost 累积到读侧（open overhead）和 storage 侧（MVCC 历史保留 —— 这是设计不是 bug，但高频 commit 意味着用户必须周期性调 `cleanup_old_versions()`，Iceberg 用户也要调 `expire_snapshots`，两者对称）。

### 🎯 共同主题 2：v2.1 的回归不只一处

| 测试 | 路径 | 回归幅度 |
|---|---|---|
| [J format versions](../extended-bench/REPORT_tier2.md) | tab_flat full_scan | v2.1 比 v2.0 慢 **2.44x** |
| [L2](../extended-bench/REPORT_L2_format_compare.md) | tab_vec full_scan | v2.1 比 v2.0 慢 **54%** |
| L2 | tab_vec col_scan(vector) | v2.1 比 v2.0 慢 **42%** |

**这说明 v2.0 → v2.1 的优化不是统一方向的**。v2.1 的 changeset 在某些路径上是回归，而 regression test 覆盖不到这些路径。v2.2 在 flat 和 vec 上部分恢复（但 vec 仍然不如 v2.0）。

**生产含义**: 如果你现在用 v2.0 并且读密集 → **不要**升级到 v2.1 或 v2.2，除非你需要 v2.2 的 map 类型或 Blob V2。

### 🎯 共同主题 3：文档和营销与实际行为严重脱钩

| 官方宣称 | 实际 |
|---|---|
| lance-flink "✅ Exactly-Once" ([README](https://github.com/lance-format/lance-flink)) | 40-68% 数据重复率 |
| "LanceDB reads 20-25K rows/s on S3" | 实测 DataLoader 下 237 img/s，纯 take_blobs 4391 rows/s（18% 达成） |
| "Lance is a drop-in replacement for Parquet" | TPC-DS 存储大 2.4x，decimal 列大 5.7x |
| "Lance v2.2 blob Inline/Packed/Dedicated modes excel at random access" | take_blobs 少量随机 take 比 v2.0 large_binary 慢 20x |
| "LanceDB v2.2 blog: scan_filter tied with Parquet" ([blog](https://lancedb.github.io/lancedb/blog/2024-lance-v2.2)) | 3 年后 v2.1 仍慢 2.49x，v2.2 没改善 |
| "Stable row IDs is production-ready" | update 在 1M 行表上慢 **11,494x** (issue #6404, 未修) |

**这不是说 Lance 团队不诚实**，而是营销材料定位于"v0 vision"，而生产就绪度还在那里。读者要能区分 "这是路线图" 和 "这是当前行为"。

### 🎯 共同主题 4：连接器 ecosystem 远未成熟

所有"非 pylance 原生"的路径都有问题：

| 组件 | 状态 |
|---|---|
| lance-flink HEAD (2026-01-08 起 3 个月未更新) | 2 个必 patch 的 bug，全是架构缺陷 |
| lance-spark 0.0.15 | `data_storage_version` 选项缺失；`CatalogNotFoundException` on Spark 3.5 |
| pyiceberg 读 Lance | 不存在（格式不兼容，要两张表并存） |
| SafeLanceDataset + Blob V2 | 只返回 descriptor 不返回 bytes，**官方 doc 没说** |

结果是每个测试最后都被迫走 **pylance 单机路径**。**"Lance 适合谁"** 的答案因此被压缩到：**会写 pylance 脚本的 Python 开发者**。

---

## Lance 擅长的场景

实测确认 Lance **显著优于** Parquet 或 Iceberg 的场景：

### ✅ 向量列存储与扫描（10-14x 比 Parquet 快）

[L2 tab_vec](../extended-bench/REPORT_L2_format_compare.md#tab_vec-1m-rows--128d-float32)：
- col_scan(vector) Lance 461ms vs Parquet 6483ms → **14x 快**
- full_scan Lance 500ms vs Parquet 6460ms → **13x 快**
- point_take Lance 393ms vs Parquet 6442ms → **16x 快**

**为什么**: Parquet 把 FixedSizeList\<f32, 128\> 当 1M × 128 个独立 float32 存，每次扫描要跨 128 列 row group 合并。Lance 用连续 ArrayEncoding 一次读。

### ✅ 高频 DELETE + 低读放大（DELETE 快 5-8x，读几乎不变慢）

[M5](../extended-bench/REPORT_M_lance_vs_iceberg.md#m5-delete--读放大)：

| Delete fraction | Lance DELETE | Iceberg MoR DELETE | Lance post-scan | Iceberg post-scan |
|---|---|---|---|---|
| 0.1% | 0.38s | 2.99s | 332ms | 999ms |
| 10% | 0.47s | 2.50s | 313ms | 1518ms (慢 **51%**) |

- **Lance deletion vector** 是 per-fragment 位图，读时直接对齐跳过，几乎无开销
- **Iceberg position-delete** 是独立 Parquet 文件，reader 要做 hash anti-join
- **Lance 越删越快**（删 10% 扫 313ms vs 删 0.1% 扫 332ms），因为要读的行更少
- **Iceberg 越删越慢**（删 10% 扫 1518ms vs 删 0.1% 扫 999ms），position-delete 文件累积

**这是 Lance 对 Iceberg 的结构性胜利**，不是调参能解决的。

### ✅ 小文件 append + compact（比 Iceberg 快 2.6x 读、3x compact）

[M6](../extended-bench/REPORT_M_lance_vs_iceberg.md#m6-small-files-pathology--compaction)：20 次小 batch append 后
- Lance pre-compact 读 186ms / Iceberg 491ms → **Lance 快 2.6x**
- Lance compact 1.2s / Iceberg rewrite_data_files 3.8s → **Lance 快 3x**
- Lance 20 次 append 总时间 5.5s / Iceberg 26.4s → **Lance 快 4.8x**

Spark 的每次 append 开销 ~1s 是 Iceberg 的死穴。Lance 的 pylance 单机路径摊销几乎为零。

### ✅ 向量搜索 + 大批量顺序 blob 读

[D vector search + ml-training-bench](../extended-bench/REPORT.md#d-向量搜索-recall-vs-qps-pareto-sift-1m)：
- IVF_HNSW_SQ 在 SIFT-1M 上 recall≥0.95 时 535 QPS
- ML training batch=256 workers=8 稳态 237 img/s（比 Parquet 2.13x、raw S3 1.49x）

---

## Lance 不擅长的场景

实测确认 Lance **显著劣于** Parquet 或 Iceberg 的场景：

### 🔴 Decimal 列存储（慢 5.7x，sorted 下可达 36x）

这是**本次研究发现的最大 bug**，有独立的[最小复现](issues/decimal_sorted_bloat.md)：

| 数据分布 | Lance v2.2 | Parquet zstd3 | Ratio |
|---|---|---|---|
| iid 随机 | 6.39 MB | 5.94 MB | 1.07x ✅ |
| TPC-DS-like 聚集 | 5.90 MB | 5.79 MB | 1.02x ✅ |
| **sorted 单调** | **14.57 MB** | **0.40 MB** | **36.73x** 🔴 |

**sorted data 下 Lance 反而比 random 更大**（14.57 > 6.39）—— 这是 smoking gun，说明 Lance 的 decimal encoding 完全没利用 locality。TPC-DS 实测 5.7x 正是这条路径的自然体现（销售数据按日期/店铺聚集 → 列内强 locality）。

### 🔴 低基数 string 列（慢 27-35x）

[M2 customer table](../extended-bench/REPORT_M_lance_vs_iceberg.md#sf1-customer-100k-rows--wide-dimension)：

| 列 | Lance | Iceberg | Ratio |
|---|---|---|---|
| c_login | 0.20 MB | 0.01 MB | **35.20x** 🔴 |
| c_preferred_cust_flag (Y/N) | 0.52 MB | 0.02 MB | **27.77x** 🔴 |
| c_customer_id | 1.10 MB | 0.07 MB | **16.13x** 🔴 |

Parquet 的 dict encoding 把 2-value flag 压到几 bytes；Lance 没做这条路径。

### 🔴 Filter / scan（慢 1.3-2.7x）

3 年前的 [issue #738](https://github.com/lance-format/lance/issues/738) 在 v2.2 仍成立：

| 选择率 | Lance BITMAP | Iceberg min/max | L/I |
|---|---|---|---|
| 1% | 413ms | 311ms | 1.33x 慢 |
| 10% | 503ms | 318ms | 1.58x 慢 |
| 50% | 784ms | 292ms | **2.69x 慢** |

而且存在 [**query planner bug**](../extended-bench/REPORT_fair_filter.md)：**选择率越高，BITMAP 反而拖慢**（50% 时 784ms 比 full-scan 的 634ms 还慢）—— planner 不会根据选择率跳过 index。

### 🔴 Update + stable_row_ids（慢 11,494x）

[Issue #6404](https://github.com/lance-format/lance/issues/6404)，A 测试实锤：

| rows updated | stable_off | stable_on |
|---|---|---|
| 100,000 on 1M table | 8ms | **93.1s** (**11,494x**) |
| 500,000 on 1M table | 16ms | TIMEOUT >300s (**>18,750x**) |

**这是 O(N × M) 复杂度 bug**，PR #6628 未合。

### 🔴 Merge-insert + BTREE（慢 500x）

[Tier 2 F](../extended-bench/REPORT_tier2.md)：违反官方建议的组合慢 500x。

---

## Lance 的结构性缺陷

不是 "bug 修了就好" 类问题，而是**架构/设计决策**决定的：

### 1. Manifest-per-commit + 乐观并发 → 高频写场景 open() 慢

每次 commit 一个新 manifest（Lance 和 Iceberg 都这样）。在 Flink 10K rows/s 场景下，1 分钟 600 个 version。`Dataset.open()` 要读 manifest 链，version 数多了就慢到秒级。**整个 lance-flink bug 链的根源**。

这不是 Lance 独有的设计 —— Iceberg 同样用 snapshot 链。但 Iceberg 有更成熟的调度工具（snapshot expiration、manifest 合并），Lance 的 `cleanup_old_versions` 需要用户显式调用且 docs 推广不足。两者都是 MVCC，历史保留本身是功能不是 bug，但高频 commit 意味着必须配套自动化的历史回收策略。

### 2. Sync-phase commit（lance-flink 的选择）

lance-flink 用 `RichSinkFunction` + `snapshotState()` sync commit，**不是 SinkV2 + 2PC**。这让 Lance commit 延迟直接吃 checkpoint 超时预算。在 60s checkpoint timeout 下，高并发 compaction 让 commit 频频超 60s → Flink restart → at-least-once → **实测 40-68% 重复**。

官方修复 PR #15 存在但 3 个月未合。

### 3. Per-fragment open 40ms 固定开销

[read-perf-bench](../read-perf-bench/REPORT.md) 证据 + [#4090](https://github.com/lancedb/lance/issues/4090)。这是 per-fragment 串行开销，不是 bandwidth 问题。**单进程读** 5000 fragments 累积 3-8 秒；**并行读**（Spark）完全掩盖。

**深层原因**：Lance 的 FileReader 构造 + metadata 初始化是 eager 的，没做 metadata cache。

### 4. Encoding picker 不做 locality analysis

[Decimal bloat 的 issue](issues/decimal_sorted_bloat.md) 和 [#3705 (vector 无压缩)](https://github.com/lancedb/lance/discussions/3705) 是同一类问题：**物理类型没有 specialized locality-aware encoding path**。Parquet 通过 dict + delta + RLE + page stats 处理大量边角情况，Lance 的 encoding picker 只在少数主路径上工作。

### 5. Blob V2 的 S3 Range-GET-per-blob

L2 实测 **take_blobs(100 blobs) = 2986ms**（每 blob ~30ms S3 latency）。**每个 blob 一次独立 S3 round-trip**，没有批量/预取。对"少量随机 take"场景是结构性慢。

官方的 "random access friendly" 实际成立条件：**batch_size ≥ 256 才能摊销 S3 开销**。

---

## 方法论教训

做这 5 个测试也积累了一套方法论：

### ✅ 始终验证"公平性契约"

[Spark-neutral rewrite](../extended-bench/REPORT_spark_neutral.md) 是本项目最大的方法学贡献：在同一引擎下重跑，**结论从"Lance 慢 2.49x"变成"Lance 低选择率下快 1.4-3.1x"**。之前是 PyArrow 偷偷给 Parquet 加持 5-6x。

> **启示**: 跨格式对比时，**引擎必须显式固定**。默认每个格式用自己的 native reader 就是作弊。

### ✅ Calibration 优于硬编码

M4 脚本先扫 CDF 再挑选择率阈值。TPC-DS `ss_quantity` 只有 100 个值 → 1% 以下选择率根本不可达 → **显式 skip + mark infeasible，不伪造数据**。

> **启示**: 声称 "0.01% selectivity" 不等于"真的 0.01%"。先看数据分布。

### ✅ Review 是必需的

opencode review 在本项目多处 catch 致命 bug：
- E 测试两个 index 用同一 dataset 互相覆盖
- B4 用 `count()` 触发 ColumnPruning 根本不测 filter
- H 破坏性 append 污染测量
- M5 aws s3 cp + pyiceberg write 会写回源表（B1 级数据破坏 bug）

> **启示**: benchmark 代码的 review ROI 极高，一个 bug 可能让整个报告无效。

### ✅ 暴露极端数据分布

[Decimal bloat 复现](issues/decimal_sorted_bloat.md)最大的价值来自"**扫 3 种分布**"而不是只用一种。iid 随机下 Lance 正常；sorted 下 36x 爆炸 —— 不扫分布就看不见。

> **启示**: 声称"格式 A 比格式 B 大 X%"时，记得问：**哪种数据分布？生产数据 resemble 哪种？**

### ✅ 承认 smoke test 的范围

每份报告都诚实列了 "未覆盖的扩展点"：[ml-training](../ml-training-bench/REPORT.md#未验证的扩展点smoke-test-阶段未覆盖) 列 8 条，[M 系列](../extended-bench/REPORT_M_lance_vs_iceberg.md) 明确说 M4/M5/M6 只跑了 sf1 不是 sf10。

---

## 生产建议

### 用 Lance 的场景
- ✅ 向量搜索 / 嵌入检索（Lance 有专门的 vector index，Parquet 没有）
- ✅ ML training 顺序大 batch blob read（大 batch 摊销 S3 开销）
- ✅ **需要大量 row-level DELETE 的表**（CDC sink, GDPR 删除, soft-delete 模式）
- ✅ 小文件频繁 append 场景（前提：不是 Flink streaming，用 pylance 单机 append）

### 不用 Lance 的场景
- ❌ 经典 data warehouse OLAP（Iceberg + Parquet 存储小 2-3x）
- ❌ **decimal-heavy schema**（finance, metrics）—— 先验证你的数据是否有 locality
- ❌ **大量 low-cardinality string 列** —— Lance 不做 dict encoding
- ❌ Flink streaming write（lance-flink 架构缺陷 + 3 个月无更新）
- ❌ 需要 non-Python ecosystem（lance-spark 0.0.15 broken, 无 lance-trino/presto）

### 用 Lance 但要小心
- ⚠️ **不要**无脑升级 `data_storage_version`（v2.1 对 vector 有 54% 回归）
- ⚠️ **不要**相信 "Exactly-Once"（at-least-once）
- ⚠️ **必须**按 id 在下游做幂等去重
- ⚠️ Blob V2 适合 ML DataLoader 不适合 notebook random take
- ⚠️ 高频 commit 后必须定期调 `cleanup_old_versions()` 回收不再需要的历史 version（对应 Iceberg 的 `expire_snapshots` —— 两者都是 MVCC 系统，历史保留是功能不是 bug，但都不自动回收）
- ⚠️ 下游如果要用 Spark 读，**不要用 lance-spark 0.0.15**；考虑 pylance → parquet export

### 真正的最佳实践
不要把 Lance 当 Parquet 的替代，而是把它当**一个专门的存储引擎**：
1. **热数据** (高 mutation, 向量索引) → Lance
2. **冷数据** (历史归档, 报表查询) → Iceberg / Parquet
3. **中间层定期从 Lance 导出到 Parquet**（用 `ds.to_table().write_parquet()` 或 Spark `read.lance().write.parquet()`）

这条路线跟 Druid/Pinot + Snowflake 的双层架构类似。不是 Lance 能不能取代 Parquet，而是**它们服务不同生命周期阶段**。

---

## 开放问题 (留给后续研究)

1. **v2.1 vector 回归是哪个 PR 引入的？** 用 `git bisect` 定位到具体 commit，交给维护者
2. **Decimal sorted bloat 在 Lance 的哪个代码路径？** 是 `lance-encoding` 里的 decimal primitive 选择器，需要读源码确认 fallback 到 plain layout 的原因
3. **Blob V2 的批量 S3 GET 是否值得做？** 现在是 per-blob 串行 Range GET；把一次 take_blobs 合并成一个 multi-range GET 或预取 pipeline 可以大幅改善 notebook 场景
4. **lance-flink PR #15 合并后的实测**（[计划在前序测试留作 followup](../README.md)）
5. **TPC-DS sf100 + 完整 22-query** 才是真正的 DW benchmark，现在只跑了 store_sales + customer 两张表的微基准
6. **`ds.optimize.compact_files()` 为什么让 size 大 76%？** ✅ **已解答** (2026-05-06, 2 次修订)：

   - **初次解答（错）**：称 Lance 不 GC，`cleanup_old_versions` 修复。措辞把 MVCC 历史保留叫 "bug"。
   - **修正（正确）**：这是**正确的 MVCC 行为**。Lance 和 Iceberg 都保留旧 version/snapshot 以支持时间旅行、审计、回滚。用户不需要历史时调 `cleanup_old_versions(older_than=...)`（Lance）或 `CALL expire_snapshots` + `remove_orphan_files`（Iceberg）回收。两种格式架构对称。真正需要注意的是区分两个指标：
     - **Active size**：当前 version 引用的字节，真实"当前表大小"
     - **Total on-disk size**：目录总字节含 MVCC 历史，真实计费成本
     M6 最早看到的 73-76% "膨胀" 是混淆了这两者。用 [`measure_active_size.py`](../extended-bench/scripts/measure_active_size.py) 正确区分后，Lance compact 的 active size 实际比 pre-compact 还小 5%。

---

## 附：所有报告索引

| 报告 | 主题 | 日期 |
|---|---|---|
| [00_SUMMARY.md](../00_SUMMARY.md) | Flink 压测执行摘要 | 2026-04-28 |
| [01_REPORT_lance_0.23.3.md](../01_REPORT_lance_0.23.3.md) | Flink 压测完整报告（0.23.3） | 2026-04-28 |
| [02_REPORT_lance_0.39.0.md](../02_REPORT_lance_0.39.0.md) | Flink 压测对比报告（0.39.0） | 2026-04-28 |
| [03_BACKGROUND_research.md](../03_BACKGROUND_research.md) | 压测前源码研究 | 2026-04-28 |
| [04_CONNECTOR_BUGS.md](../04_CONNECTOR_BUGS.md) | lance-flink 2 个必 patch 的 bug | 2026-04-28 |
| [05_HOW_TO_REPRODUCE.md](../05_HOW_TO_REPRODUCE.md) | Flink 压测复现步骤 | 2026-04-28 |
| [read-perf-bench/REPORT.md](../read-perf-bench/REPORT.md) | Fragment 数量 vs 读性能 | 2026-04-29 |
| [ml-training-bench/REPORT.md](../ml-training-bench/REPORT.md) | ML 训练 blob 点查 | 2026-04-30 |
| [ml-training-bench/REPORT_pure_take.md](../ml-training-bench/REPORT_pure_take.md) | 纯 take_blobs 吞吐验证 | 2026-04-30 |
| [extended-bench/REPORT.md](../extended-bench/REPORT.md) | 扩展压测 Top 4 | 2026-04-30 |
| [extended-bench/REPORT_tier2.md](../extended-bench/REPORT_tier2.md) | Tier 2 六项 | 2026-04-30 |
| [extended-bench/REPORT_fair_filter.md](../extended-bench/REPORT_fair_filter.md) | 加 scalar index 的 fair filter | 2026-05-01 |
| [extended-bench/REPORT_spark_neutral.md](../extended-bench/REPORT_spark_neutral.md) | Spark 中立引擎 | 2026-05-01 |
| [extended-bench/REPORT_L2_format_compare.md](../extended-bench/REPORT_L2_format_compare.md) | L2 格式 × 工作负载矩阵 | 2026-05-02 |
| [extended-bench/REPORT_M_lance_vs_iceberg.md](../extended-bench/REPORT_M_lance_vs_iceberg.md) | M 系列 Lance vs Iceberg TPC-DS | 2026-05-02 |
| [issues/decimal_sorted_bloat.md](issues/decimal_sorted_bloat.md) | Decimal sorted 36x 膨胀 issue | 2026-05-04 |

**总数**: 5 个独立研究、16 份详细报告、1 个待提交 issue、40+ 个可复现脚本。

---

## 授权与引用

代码 MIT，报告数据 AS-IS。如引用：
- Repo: https://github.com/ZackFairTS/lance-test
- 测试期: 2026-04-28 → 2026-05-02
- 硬件: AWS EMR r8g.2xlarge, S3 ap-northeast-1
- Lance: lance-core 0.23.3 + 0.39.0, pylance 4.0.1
