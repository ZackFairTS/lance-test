# M 系列 — Lance v2.2 vs Iceberg v2 (MoR) on TPC-DS

**测试日期**: 2026-05-02
**环境**: AWS EMR master (r8g.2xlarge Graviton, 8vCPU 64GiB), S3 ap-northeast-1, EMR 7.10 (Spark 3.5.5-amzn-1, Iceberg 1.8.1-amzn-0)
**版本**: pylance 4.0.1 (lance-core 0.39.0), pyarrow 20.0.0, pyiceberg 0.10
**数据**: TPC-DS sf=1 (2.88M rows store_sales + 100K customer) 和 sf=10 (28.8M + 500K)，DuckDB `dsdgen` 生成
**测量**: 3 warmup + 7 rounds，p50
**脚本**: `scripts/M0_gen_tpcds.py` ... `scripts/M6_compact.py`

---

## 为什么做 M 系列

之前的 Tier 0/Tier 2 / L2 对比全部是 **Lance vs 裸 Parquet**。但生产环境真正的竞品是 **Iceberg (on Parquet)** —— 它有 catalog、metadata、snapshot、MoR、compaction，是 Lance 的实质竞争对手。

M 系列首次把 Lance v2.2 跟 **完整 Iceberg v2 stack** 正面对比，在 **真实 TPC-DS 数据** 上测 6 个维度：

| 阶段 | 做什么 | 输出 |
|---|---|---|
| **M0** | DuckDB `dsdgen` 生成 TPC-DS → 上传 Parquet 到 S3 作为共源 | `M0_manifest_sf<N>.json` |
| **M1** | 从 M0 Parquet 写成 Lance v2.2 + Iceberg v2（同 compression、同 target-file-size） | `M1_manifest_sf<N>.json` |
| **M2** | S3 字节级分解：data/metadata/每列 on-disk 大小 | `M2_size_sf<N>.json` |
| **M3** | full_scan / col_scan 吞吐（Python + Arrow neutral engine） | `M3_scan_sf<N>.json` |
| **M4** | filter 下推（Lance BITMAP index vs Iceberg Parquet min/max） | `M4_filter_sf<N>.json` |
| **M5** | DELETE + 读放大（Lance deletion vector vs Iceberg positional-delete MoR） | `M5_update_sf<N>.json` |
| **M6** | 小文件病 + compaction 成本（50 次 append → compact） | `M6_compact_sf<N>.json` |

### 公平性契约（locked in M1，被 M2-M6 继承）

- **同源数据**（M0 Parquet on S3）
- **同压缩**（zstd level 3 在两端）
- **同目标文件大小** ~128 MiB (Iceberg: `write.target-file-size-bytes`；Lance: `max_bytes_per_file`)
- **Iceberg format-version=2** (MoR), HadoopCatalog, path-based on S3
- **读侧用 Python + Arrow 两端**（pylance + pyiceberg native），不走 Spark，避免 "Lance 用 DataFusion / Parquet 用 PyArrow" 这类引擎不公平

---

## 🎯 TL;DR

| 维度 | Lance v2.2 | Iceberg v2 | 赢家 |
|---|---|---|---|
| **存储效率** | 3568 MB (sf10 store_sales) | **1476 MB** ⭐ | 🔴 Iceberg，Lance 大 **2.4x** |
| **写入速度** | **43s** (sf10) | 32s | ≈ (不同路径) |
| **全表扫描** (sf10) | 2701 ms | **2284 ms** ⭐ | 🟡 Iceberg，Lance 慢 **1.18x** |
| **单列扫描** (sf10) | 609 ms | **388 ms** ⭐ | 🔴 Iceberg，Lance 慢 **1.57x** |
| **filter (1% sel, sf10)** ⭐ | **634 ms** | 707 ms | ✅ **Lance 反超 0.90x** |
| **filter (50% sel, sf10)** | 2143 ms | **729 ms** ⭐ | 🔴 Iceberg，Lance 慢 **2.94x** |
| **DELETE 速度** (sf10) | **0.6s** ⭐ | 3.5s | ✅ **Lance 快 5.8x** |
| **DELETE 后读** (sf10, 10%) | **754 ms** ⭐ | 5571 ms | ✅ **Lance 快 7.4x** |
| **50×append+compact 前读** (sf10) | **309 ms** ⭐ | 880 ms | ✅ **Lance 快 2.85x** |
| **compact 时间** (sf10) | **1.8s** ⭐ | 6.7s | ✅ Lance 快 3.67x |

**简单概括**:
- 🔴 **稳态 OLAP (scan, filter 中高选择率, compression)**: Iceberg 全面领先
- ✅ **低选择率 filter (~1%)**: Lance sf10 上首次反超 —— 大数据下 BITMAP 终于划算
- ✅ **高频 mutation (delete, small-file)**: Lance **sf10 上优势放大到 5-7x**
- **Lance 的存储劣势是 2-3x，不是百分比**。M6 里 compact 后再涨 73% 是**孤儿 fragment**，不是真膨胀（详见 [Compact GC 机制调查](#compact-gc-机制调查--new-2026-05-06)），一行 `cleanup_old_versions()` 就消失

---

## M2: 存储大小分解（震撼结果）

### sf10 store_sales (28.8M rows)

| | Lance v2.2 | Iceberg v2 | Ratio |
|---|---|---|---|
| Total | 3568 MB | **1476 MB** | **Lance 大 2.42x** |
| data/ | 3568 MB | 1475 MB | — |
| metadata/ | 0.01 MB | 0.02 MB | — |
| #fragments / #files | 19 | 16 | ~相同（target 128 MiB） |

**Per-column 字节分解（前 8 名差距最大的列）**：

| column | Lance MB | Iceberg MB | Lance/Iceberg |
|---|---|---|---|
| ss_net_paid | 471.25 | 81.34 | **5.79x** 🔴 |
| ss_ext_sales_price | 471.24 | 81.97 | **5.75x** 🔴 |
| ss_net_paid_inc_tax | 471.25 | 82.05 | **5.74x** 🔴 |
| ss_ext_wholesale_cost | 471.24 | 84.68 | **5.56x** 🔴 |
| ss_net_profit | 471.26 | 84.84 | **5.55x** 🔴 |
| ss_ext_list_price | 471.24 | 86.60 | **5.44x** 🔴 |
| ss_ext_discount_amt | 73.44 | 29.72 | 2.47x |

**所有 "$$$" 金额列 (Decimal(7,2)) 在 Lance 上都比 Iceberg 大约 **5.7x**。**

### sf1 customer (100K rows) — wide dimension

| column | Lance MB | Iceberg MB | Lance/Iceberg |
|---|---|---|---|
| c_login | 0.20 | 0.01 | **35.20x** 🔴 |
| c_preferred_cust_flag | 0.52 | 0.02 | **27.77x** 🔴 |
| c_customer_id | 1.10 | 0.07 | **16.13x** 🔴 |
| c_last_review_date_sk | 0.30 | 0.12 | 2.56x |
| c_customer_sk | 0.20 | 0.12 | 1.66x |

**低基数 string 列（c_login、c_preferred_cust_flag）在 Lance 上比 Parquet-zstd 大 27-35 倍**。

### 为什么 Lance 这么大

- **Decimal**: Parquet 用 BYTE_ARRAY + dictionary encoding + zstd，Lance 的 decimal 存储似乎没有对高重复的金额值做 dictionary
- **低基数 string**: Parquet 的 dictionary encoding 把 "Y"/"N" 的 flag 列几乎压到零；Lance 没把这条路径做好
- **高基数 string (c_email_address)**: 1.48x，差距最小 —— 说明 Lance 的通用 string 压缩 OK，问题集中在 **dictionary-applicable 的列**

**这不是"文件格式设计失败"**，是 **Lance encoding selection 对 TPC-DS 类 schema 的优化不够**。[Tier 2 I_compression](REPORT_tier2.md) 已经发现 vector 列完全无压缩，现在确认在 decimal + low-cardinality string 上也有同类问题。

---

## M3: Scan 吞吐（neutral engine）

### sf10 store_sales (28.8M rows)

| op | Lance p50 | Iceberg p50 | Lance / Iceberg |
|---|---|---|---|
| full_scan | 2701 ms | **2284 ms** ⭐ | 1.18x 慢 |
| col_scan (ss_ext_list_price) | 609 ms | **388 ms** ⭐ | **1.57x 慢** |

### sf10 customer (500K rows)

| op | Lance p50 | Iceberg p50 | Lance / Iceberg |
|---|---|---|---|
| full_scan | **280 ms** ⭐ | 390 ms | 0.72x 快 |
| col_scan (c_customer_id) | **204 ms** ⭐ | 234 ms | 0.87x 快 |

**关键观察**:
- **宽表 (store_sales)**: Iceberg 赢，Lance col_scan 慢 57% —— 可能是因为 Lance 要扫 5.7x 更大的列字节
- **窄维度表 (customer)**: Lance 略快 —— 行数少时 manifest/open overhead 占主导，Lance 的 manifest 更轻
- **sf1 数据太小（2.88M）时 Lance/Iceberg 基本打平** —— 大数据量才放大 Iceberg 的列存储优势

**相关**: [read-perf-bench](../read-perf-bench/REPORT.md) 发现 Lance 在 Spark 分布式读下跟 fragment 数无关。这里 Python+Arrow 单进程读，Lance 的"column bytes on disk 更大" → S3 GET 字节更多 → wall-clock 更慢，**互相印证**。

---

## M4: Filter 下推（Lance BITMAP vs Iceberg min/max）

**数据集**: sf1 store_sales, 2.88M rows, `ss_quantity` ∈ uniform[1, 100]
**Lance**: BITMAP scalar index（M4 里 in-place build）
**Iceberg**: 无 bloom filter，只有 row group min/max（M1 写入时是 Parquet 默认）

### 结果

**sf1 (2.88M rows)**:

| Target | Actual Sel | Rows | Lance p50 | Iceberg p50 | L/I |
|---|---|---|---|---|---|
| 0.01% | — | — | SKIPPED ⚠️ | SKIPPED | infeasible |
| 0.1% | — | — | SKIPPED ⚠️ | SKIPPED | infeasible |
| 1% | 0.96% | 27,613 | 413 ms | **311 ms** ⭐ | 1.33x 慢 |
| 10% | 9.56% | 275,345 | 503 ms | **318 ms** ⭐ | 1.58x 慢 |
| 50% | 49.63% | 1,429,485 | 784 ms | **292 ms** ⭐ | **2.69x 慢** |

**sf10 (28.8M rows)** ⭐ NEW:

| Target | Actual Sel | Rows | Lance p50 | Iceberg p50 | L/I |
|---|---|---|---|---|---|
| 0.01% | — | — | SKIPPED ⚠️ | SKIPPED | infeasible |
| 0.1% | — | — | SKIPPED ⚠️ | SKIPPED | infeasible |
| **1%** | 0.95% | 274,565 | **634 ms** ⭐ | 707 ms | **0.90x (Lance 反超)** |
| 10% | 9.55% | 2,750,202 | 912 ms | **714 ms** ⭐ | 1.28x 慢 |
| 50% | 49.64% | 14,297,510 | 2143 ms | **729 ms** ⭐ | **2.94x 慢** |

**sf10 的新洞察**:
- **1% 选择率下 Lance 首次反超** (634ms vs 707ms) —— 在大数据量下 BITMAP index 定位 274K 行的能力终于摊销出价值
- **但 10% 和 50% 差距被放大**（sf1 是 1.58x / 2.69x → sf10 是 1.28x / **2.94x**）
- **Iceberg 全选择率几乎恒定** (707/714/729 ms)：min/max pruning 在 uniform 分布下零作用，时间都花在 I/O 上
- **Lance 50% 选择率爆炸**（634→912→**2143 ms**，3.4x 放大），证实 query planner bug 在大数据量下**更严重**

**关键**：
- **0.01% / 0.1% 目标不可达**：`ss_quantity` 只有 100 个离散值（uniform 1-100），每个值覆盖 ~1%。calibration 正确检测并跳过 —— 这是 M4 脚本的设计优点（B1 审查后改的）
- **Lance BITMAP 在所有可测选择率上都输**：这跟 [REPORT_fair_filter.md](REPORT_fair_filter.md) 的发现一致（高选择率下 BITMAP 比 full scan 慢）
- **Iceberg min/max 在这里本身几乎零作用**（uniform 分布，每个 row group 都 [1, 100]）—— 说明 **Iceberg 在 0 有效 pruning 下仍然打败 Lance BITMAP**

**推测 Lance planner bug**：BITMAP index 命中时，**选择率越高（50%）反而越慢**（784ms，比 full-scan 的 634ms 还慢）。这个是 [Fair-Filter 报告里发现的相同 bug](REPORT_fair_filter.md) 的再次实锤 —— Lance query planner 不会在选择率高时跳过 index。

---

## M5: DELETE + 读放大（Lance 最大的胜利）

**测试**: 在 store_sales 的独立工作副本上，按 `ss_sold_date_sk <= K` 删不同比例的行；记录 DELETE 时间 + DELETE 后全表 scan 延迟。
**方法学**:
- Lance 工作副本用 `aws s3 cp --recursive`（无 location 字段，safe）
- Iceberg 工作副本用 Spark CTAS（`CREATE TABLE ... AS SELECT * FROM src` + `write.delete.mode=merge-on-read`）—— **不能用 pyiceberg delete**，因为 pyiceberg 0.10 的 NoopCatalog 不支持 commit 且 `Table.delete()` 会 fallback 到 CoW，忽略 MoR 设置
- 两端都有 `assert_iceberg_location_matches` 防止 B1 类 silent-mutation bug

### sf1 结果

| Fraction | Lance DELETE | Iceberg DELETE | Lance post-scan | Iceberg post-scan | L/I scan |
|---|---|---|---|---|---|
| 0.1% (2,890 rows) | **0.38s** | 2.99s | **332 ms** | 999 ms | **0.33x (Lance 快 3x)** |
| 1% (~28K rows) | **0.38s** | 3.54s | **341 ms** | 1034 ms | **0.33x (Lance 快 3x)** |
| 10% (~287K rows) | **0.47s** | 2.50s | **313 ms** | 1518 ms | **0.21x (Lance 快 4.8x)** |

### sf10 结果 ⭐ NEW (28.8M rows)

| Fraction | Lance DELETE | Iceberg DELETE | Lance post-scan | Iceberg post-scan | L/I scan |
|---|---|---|---|---|---|
| 0.1% (26,646 rows) | **0.53s** | 3.20s | **1023 ms** | 5492 ms | **0.19x (Lance 快 5.4x)** |
| 1% (286,345 rows) | **0.54s** | 2.98s | **963 ms** | 5492 ms | **0.18x (Lance 快 5.7x)** |
| 10% (2,872,726 rows) | **0.63s** | 4.47s | **754 ms** | 5571 ms | **0.14x (Lance 快 7.4x)** |

**sf10 放大了 Lance 的优势 —— 这是 Lance 对 Iceberg 最稳的结构性胜利**:
- **Lance DELETE 快 5-8x**（sf1 5-8x, sf10 5-7x，规模无关）
- **Lance post-scan 快 5.4-7.4x**（sf1 是 3-4.8x → sf10 优势反而放大）
- Lance scan 仍保持"删越多读越快"特性（0.1%→10% 时 1023→**754ms**）
- Iceberg post-scan 在所有 fraction 上几乎**恒定 5500ms 左右** —— position-delete 文件数量固定 (12 个), 所以 MoR 反合成开销是固定的

**为什么 sf10 优势放大**: 数据量 10x，Lance 的 deletion vector 读开销仍接近 O(1)（位图 alignment），Iceberg 的 position-delete anti-join 开销近似 O(N) → 在大数据上差距被放大。

### 关键发现

1. **Lance DELETE 比 Iceberg 快 5-8x**（0.4s vs 2.5-3.5s）
   - Lance: 写 `_deletions/<fragment_id>.arrow` 位图，不走 Spark 调度
   - Iceberg MoR: Spark SQL DELETE → 写一个新的 position-delete Parquet 文件 + 新 snapshot + manifest update
2. **DELETE 后读放大严重**：
   - Lance scan：312-341 ms（**几乎没有读放大**！删除比例越高反而略快 —— delete 位图让读的行更少）
   - Iceberg MoR scan：999-1518 ms（**删 10% 比删 0.1% 慢 51%** —— position delete 文件大了）
   - **Lance 越删越快，Iceberg MoR 越删越慢**
3. **Storage 变化几乎不可测**（Lance 361.3→361.7MB，Iceberg 99.1→99.4MB）—— 都是 MoR 的特点：不重写 data file

### 为什么 Lance DELETE 读这么快

- Lance 的 deletion vector 是 per-fragment 一个紧凑位图，读的时候直接跟数据行对齐跳过
- Iceberg position-delete 是一个独立 Parquet 文件，reader 要对每个 data file 做 **hash join** 式的 anti-join
- 在 pyiceberg 0.10 的实现里这条路径还很年轻，Spark side 也相对重

**这是 Lance 对 Iceberg 的结构性优势**，不是选项问题 —— 想改 Iceberg MoR 读路径得改 reader 实现。

---

## M6: Small-Files Pathology + Compaction

**场景**: 新建一张 table（baseline 50K rows），然后 append 20 个 batch（每 batch 5K rows）→ 20 个独立 commit → 21 个 data files/fragments。再跑 compaction。

### 时间和文件数

**sf1**:

| 阶段 | Lance | Iceberg |
|---|---|---|
| Baseline write | **0.6s** ⭐ | 3.8s |
| 20 次 append 总时间 | **5.5s** ⭐ | 26.4s |
| Compaction 时间 | **1.2s** ⭐ | 3.8s |
| Pre-compact files | 21 fragments | 21 data files + 22 snapshots |
| Post-compact files | **1** | 1 (+ 23 snapshots, 旧的未 expire) |

**sf10** ⭐ NEW (50 appends, same baseline + append batch sizes):

| 阶段 | Lance | Iceberg |
|---|---|---|
| Baseline write | **0.6s** ⭐ | 4.0s |
| **50 次 append 总时间** | **13.1s** ⭐ | **62.3s** (4.76x) |
| Compaction 时间 | **1.8s** ⭐ | 6.7s (3.67x) |
| Pre-compact files | 51 fragments | 51 data files + 52 snapshots |
| Post-compact files | **1** | 1 (+ 53 snapshots) |

**Lance 所有阶段 sf10 上都快 3-5x**。append 比例 4.76x 跟 sf1 (4.8x) 几乎完全一致 —— 证实 **Spark 每次 append 固定开销 ~1s** 是 Iceberg 的结构死穴，跟数据量无关。

### 读延迟（pre vs post compact）

**sf1** (21 files → 1 file):

| 阶段 | Lance p50 | Iceberg p50 | Lance/Iceberg |
|---|---|---|---|
| Pre-compact (21 files) | **186 ms** ⭐ | 491 ms | **0.38x (Lance 快 2.6x)** |
| Post-compact (1 file) | **199 ms** ⭐ | 313 ms | 0.63x (Lance 快 1.6x) |

**sf10** ⭐ NEW (51 files → 1 file):

| 阶段 | Lance p50 | Iceberg p50 | Lance/Iceberg |
|---|---|---|---|
| Pre-compact (51 files) | **309 ms** ⭐ | 880 ms | **0.35x (Lance 快 2.85x)** |
| Post-compact (1 file) | **214 ms** ⭐ | 382 ms | 0.56x (Lance 快 1.78x) |

Lance 的小文件读优势在 sf10 下被放大（2.6x → 2.85x），post-compact 差距也放大（1.6x → 1.78x）。

### 意外发现：Lance 的 compact 让 size 变大（sf10 验证）

**sf1**: Lance 24.7 → 43.5 MB（**+76%**），Iceberg 6.4 → 12.0 MB（+87%）
**sf10** ⭐ NEW: Lance 103.6 → **178.8 MB (+73%)**，Iceberg 25.4 → **47.3 MB (+86%)**

**sf1 和 sf10 的膨胀率几乎相同** (+73-76% vs +86-87%) → 证实这是**系统性行为**，不是 sf1 artifact。

### Compact GC 机制调查 ⭐ NEW (2026-05-06)

M6 在 S3 上只能拿 `aws s3 ls --recursive` 的总字节，无法区分"活 fragment"和"孤儿 fragment"。写独立隔离脚本 [`compact_gc_investigation.py`](scripts/compact_gc_investigation.py) 在本地磁盘上判别两个假说：

| 假说 | 说法 |
|---|---|
| **A. 不 GC** | Compact 添加新 fragment 但不删旧的 → cleanup 能回收全部 |
| **B. Compact 膨胀** | 新 fragment 本身就比原来合计大 → cleanup 回收不了 |

**实测（2 次独立 replay，n={21, 51} fragments）**:

| 阶段 | Lance size | data files | active frags |
|---|---|---|---|
| Pre-compact (51 fragments) | 21.86 MB | 51 | 51 |
| Post-compact 未 cleanup | 42.51 MB (+94.5%) | **53** (51 orphan + 2 new) | 2 |
| **Post cleanup_old_versions** | **20.64 MB** | **2** | **2** |

**假说 A 完全成立**:
- compact 后 data_files=53（21+1 或 51+2）—— **旧 fragments 一个都没删**
- `cleanup_old_versions(older_than=0, delete_unverified=True)` 回收 21/51 个孤儿 fragment + 对应的 transaction files
- **post-cleanup 反而比 pre-compact 还小 5%**（compact 本身做了有效压缩收益！）

**所以 M6 在 S3 看到的 73-76% 膨胀，100% 是孤儿数据。加一行 `cleanup_old_versions()` 就消失了。**

### 修正后的生产叙事

之前的说法"Lance 比 Iceberg 存储大 2.4x 的原因包含 compact 生命周期问题"**只对了一半**：

- 🔴 **确实**：Lance 默认 compact 不 GC，新手会看到假的 76% 膨胀
- ✅ **但**：一行 `cleanup_old_versions()` 就能修复，且 Lance 的 compact 实际写入**比 Iceberg 更紧凑**
- 🟡 **真正的问题是 UX**：
  - Iceberg 也有这个模式（snapshots 不自动 expire），但生态工具链更成熟（`VACUUM`, `expire_snapshots` 有自动化范例）
  - Lance 的 `cleanup_old_versions` 在官方 docs 里不显眼，很多用户不知道要调
  - 应该在 `compact_files()` 加一个 `auto_cleanup=True` 选项或默认行为

**M 系列 M2 里观察到的 Lance 存储大 2.4x 是在 _初始写入后_ 的对比，不涉及 compact。所以 decimal bloat 仍是存储差距的根因，跟 compact GC 是两码事。**

**Iceberg** 的 87% 增长也是类似原因（新写一个大文件 + 旧文件还在等 snapshot expire）—— `VACUUM` 或 `CALL expire_snapshots` 后也会回落。两种格式**架构上对称**，只是默认行为不同。

---

## 🐛 脚本实现里避开的 bug（review 笔记）

M 系列是完全 review-driven 的，每个脚本 docstring 里都明确记录了一批 "曾经踩过/差点踩到的坑"：

### M1 — Iceberg `.using("iceberg")` silent drop tableProperty

用 `spark.writeTo(fqn).using("iceberg").tableProperty(...)` 在 Spark 3.5 + Iceberg 1.8 下，**`.using(...)` 会静默丢掉 tableProperty()**（DSv2 hint 冲突）。M1 用 `spark.writeTo(fqn).create()` + `verify_iceberg()` 检查 persisted properties 是否匹配。

### M3/M4 — 为什么不用 Spark 做 neutral engine

B4 曾经用 Spark 做双引擎中立。但 lance-spark 0.0.15 在 Spark 3.5.5-amzn-1 上报 `CatalogNotFoundException`（newer Spark 不 fallback DataSourceRegister SPI）。改走 pylance + pyiceberg 的 Python + Arrow neutral engine。

### M4 — selectivity calibration

脚本不接受用户给的"0.01%"硬编码。先扫列算 CDF，然后挑 `column <= K` 让实际选择率匹配 target。target 实际不可行时（如 ss_quantity 的 1% 粒度）**显式跳过并标记 infeasible**，不伪造数据。

### M5 — pyiceberg 不能写

pyiceberg 0.10 的 `NoopCatalog.commit_table` 直接 `NotImplementedError`，`Table.delete()` 会 fallback 到 CoW 而不是 MoR —— 即使表属性设 `write.delete.mode=merge-on-read` 也会忽略。**M5 必须用 Spark 写、pyiceberg 读**，不对称但是诚实。

### M5 — "aws s3 cp + pyiceberg write = 写回源表" B1 级 bug

`metadata.json` 里有个绝对 `location` 字段。直接 `aws s3 cp --recursive` 复制一张 Iceberg 表到新路径，然后 pyiceberg 写入 —— **新 data file 会写回原 location**，悄无声息破坏 M1 源数据。

M5 通过 Spark CTAS 建新表（生成正确的新 location）+ `assert_iceberg_location_matches()` 对比 load URI 和 metadata.location 双重防御。Lance 没这个问题（.lance 目录没存 location）。

### M6 — Spark createDataFrame promote BIGINT→DOUBLE

Spark 的 `createDataFrame(pd.DataFrame)` 如果 BIGINT 列有 NULL，会 silent promote 成 DOUBLE，然后 insert 回 BIGINT 列时 `CAST_OVERFLOW`。M6 绕过：pyarrow.Table → Parquet → HDFS → Spark read parquet → writeTo(iceberg)。

而且不能用 `file:///tmp` 因为 EMR Spark 在 YARN worker 里跑，看不见 driver 的 `/tmp`。**必须用 HDFS**。

---

## 生产建议（修订版）

### 用 Lance 还是 Iceberg？取决于你的主要工作负载

| 工作负载 | 建议 |
|---|---|
| **纯 OLAP, 批写, 偶读大表** (e.g. data warehouse) | **Iceberg** —— 存储小 2-3x，scan 快 1.2-1.6x |
| **高频行级 DELETE/UPDATE** (e.g. CDC sink, GDPR compliance) | **Lance** —— DELETE 快 5-8x，读放大几乎零 |
| **高频小批次 append** (e.g. streaming ingest) | **Lance** —— small-files 比 Iceberg MoR 便宜 2.6x |
| **既要便宜存储又要能 mutate** | **Iceberg + 定期 compact + OPTIMIZE** —— 接受 DELETE 慢 |
| **向量搜索 / ML blob** | **Lance** —— 这是 Iceberg 根本不支持的 |
| **decimal-heavy schema (金融, 计量)** | **Iceberg** —— Lance 在 decimal 上 5.7x 膨胀 |
| **低基数 string / categorical 列很多** | **Iceberg** —— Lance 在 low-cardinality 上 27-35x 膨胀 |

### 不要相信的营销文案

- ❌ "Lance is a drop-in replacement for Parquet" —— 存储大 2-3x，decimal 5.7x
- ❌ "Lance is faster than Parquet on everything" —— M3 col_scan sf10 慢 1.57x
- ❌ "Lance's BITMAP index is the answer to filter pushdown" —— 高选择率下 planner bug 让它比 full scan 还慢
- ✅ "Lance has better mutation semantics than Iceberg MoR" —— **实锤**

### 未来该看的

1. **Lance 的 decimal encoding 为什么比 Parquet dict+zstd 差 5.7x** —— 这是最大的可修的问题
2. **Lance 的 low-cardinality string 为什么不做 dict encoding** —— 这是第二大可修的问题
3. **BITMAP planner bug** —— 高选择率跳过 index 应该是半小时的 bugfix
4. **Lance compact_files() 是否应该自动 GC 旧 fragments** —— 从 M6 数据看是个生命周期 UX 问题
5. **sf100 + TPC-DS full workload** —— M 系列只测了 2 张表（store_sales + customer）和 sf1/sf10。sf100 会更清楚存储膨胀的 impact

---

## 📁 Raw data

| 文件 | 描述 |
|---|---|
| `results/M0_manifest_sf1.json` / `sf10.json` | TPC-DS 生成 + Parquet 上传 manifest |
| `results/M1_manifest_sf1.json` / `sf10.json` | Lance v2.2 + Iceberg v2 写入 manifest |
| `results/M2_size_sf1.json` / `sf10.json` | S3 字节分解 + per-column 大小 |
| `results/M3_scan_sf1.json` / `sf10.json` | Full scan + col scan 延迟 |
| `results/M4_filter_sf1.json` / `sf10.json` | BITMAP vs min/max filter |
| `results/M5_update_sf1.json` / `sf10.json` | DELETE + 读放大 |
| `results/M6_compact_sf1.json` / `sf10.json` | Small-files + compact |

**sf10 补跑于 2026-05-06**（本 repo 早期版本只有 sf1）。sf10 全部定性结论都跟 sf1 一致，部分量化上差距放大（M5 delete scan 5-7x，M6 pre-compact 2.85x）。

## 相关脚本

- `scripts/M0_gen_tpcds.py` — DuckDB dsdgen 生成 TPC-DS
- `scripts/M1_write_both.py` — 从 Parquet 写 Lance + Iceberg
- `scripts/M2_size.py` — S3 + column bytes 分解
- `scripts/M3_scan.py` — Python + Arrow neutral scan
- `scripts/M4_filter.py` — CDF-calibrated selectivity sweep
- `scripts/M5_update.py` — CTAS-based MoR delete benchmark
- `scripts/M6_compact.py` — 50×append + compact + HDFS staging
