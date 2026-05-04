# L2 — Lance 格式版本 vs Parquet 的"工作负载 × 格式"矩阵

**测试日期**: 2026-05-02
**环境**: AWS EMR master (r8g.2xlarge Graviton, 8vCPU 64GiB), S3 ap-northeast-1
**版本**: pylance 4.0.1 (lance-core 0.39.0), pyarrow 20.0.0
**Run ID**: `20260502-133710`
**样本规模**: 1,000,000 行（flat/vec/nested），5,000 行（blob，但总 payload 3.7 GB）
**测量**: 3 warmup + 7 rounds，p50（中位数）
**脚本**: `scripts/L2_write.py` + `scripts/L2_measure.py`（manifest-driven 两阶段）

---

## 为什么做这个测试

Tier 0 的 J（`J_format_versions.py`）已经发现 **Lance v2.1 在 flat full-scan 上比 v2.0 慢 2.44x**，属于 v2.1 回归。但 J 只测了 flat 工作负载。

L2 把"4 种工作负载 × 5 种格式"做成完整矩阵，每一格都跑同样的 6 种操作，**一次性看清各格式在各 shape 数据上的所有强/弱项**：

| 格式 | 覆盖 |
|---|---|
| `lance_2.0` / `lance_2.1` / `lance_2.2` | 相同 pylance 二进制下写出的三种 data_storage_version |
| `parquet_snappy` / `parquet_zstd` | pyarrow / Spark 写出的 Parquet，row_group=1M，data_page_version 2.0 |

**4 种工作负载**：

| workload | 描述 | 规模 |
|---|---|---|
| `tab_flat` | 10 列 int/float/string/int8 （典型 OLAP 窄列） | 1M rows |
| `tab_vec` | flat + FixedSizeList\<f32, 128\> 向量列 | 1M rows |
| `tab_nested` | struct / list / map 组合 | 1M rows |
| `tab_blob` | flat + large_binary payload（60% inline / 30% packed / 10% dedicated） | 5K rows, 3.7 GB |

---

## 🎯 结果速览

### 读延迟 p50 (ms) — **越小越好**

#### `tab_flat` (1M rows, 10 typed cols)

| format | full_scan | col_scan(amount) | filter(category=cat_0) | point_take(1000) |
|---|---|---|---|---|
| lance_2.0 | 199 | 89 | 99 | 159 |
| **lance_2.1** | **93** ⭐ | 85 | 94 | 88 |
| **lance_2.2** | 96 ⭐ | 86 | 93 | 90 |
| parquet_snappy | 101 | 38 ⭐ | 64 | 104 |
| parquet_zstd | 86 ⭐ | **37** ⭐ | **65** | 89 |

- **flat full-scan**: lance_2.1/2.2 与 parquet_zstd 打平（85-96ms），**但 lance_2.0 慢 2.1x** —— 跟 J 测试的回归方向相反，**flat 场景下 2.1/2.2 反而比 2.0 更快**（与 J 里结论矛盾，详见"讨论"节）
- **col_scan**: Parquet 快 2.3x（37 vs 85ms）—— Lance 没有 Parquet-like 精准单列 I/O 定位
- **filter**: Parquet 更快（64 vs 93ms），原因见 [Tier 0 B + Fair-Filter](REPORT.md#b-filter-性能-vs-parquet-issue-738-3-年未解)
- **point_take**: lance_2.1/2.2 比 Parquet 略快（88 vs 89ms）；lance_2.0 退化

#### `tab_vec` (1M rows × 128d float32)

| format | full_scan | col_scan(vector) | col_scan(amount) | point_take |
|---|---|---|---|---|
| **lance_2.0** | **500** ⭐ | 461 | 86 | 393 |
| lance_2.1 | 773 | 653 | 86 | 462 |
| lance_2.2 | 636 | 610 | 86 | 458 |
| parquet_snappy | 6460 | 6483 | 105 | 6442 |
| parquet_zstd | 6487 | 6519 | 104 | 6504 |

**这是 Lance 最大的胜利**：
- vector col_scan: **Lance 比 Parquet 快 10-14x**（461 vs 6483ms）
- vector full_scan: **Lance 比 Parquet 快 10-13x**（500 vs 6460ms）
- point_take: Lance **快 14-16x**（393-462 vs 6442ms）
- **但 lance_2.1 反而最慢**（773ms vs 2.0 的 500ms）—— **v2.1 在 vector 上 54% 回归**，是新发现

**原因推测**: Parquet 把每个 float32 当作独立列存到 plain/delta 里，1M × 128 × 4B = 512 MB 在单个 row group 要全读；Lance 的 FixedSizeList encoding 用连续布局 + 读一次。

#### `tab_nested` (struct / list / map, 1M rows)

| format | full_scan | nested_subread |
|---|---|---|
| lance_2.0 | ❌ `PanicException: not yet implemented: map encoding` |
| lance_2.1 | ❌ `OSError: Map data type is only supported in Lance file format 2.2+` |
| **lance_2.2** | **199** ⭐ | 156 |
| parquet_snappy | 443 | **95** ⭐ |
| parquet_zstd | 365 | 95 |

- **Lance v2.0/v2.1 根本不能写 map** —— 分别 panic / 报错
- **v2.2 是 Lance 第一次支持 map**，且 full_scan **比 Parquet 快 1.8-2.2x**
- **但 nested_subread（只读 nested 列）Parquet 快 1.6x**（95 vs 156ms）—— Lance 在 struct 内部子字段下推上不如 Parquet

#### `tab_blob` (5K rows, 60% inline ≤60KB / 30% packed ≤1MB / 10% dedicated ≤6MB)

| format | scan_non_blob(id) | blob_take(100 random rows) |
|---|---|---|
| **lance_2.0** | 27 | **133** ⭐ |
| **lance_2.1** | 23 ⭐ | **151** ⭐ |
| lance_2.2 (Blob V2) | 26 | **2986** 🔴 |
| parquet_snappy | 40 | 9706 |
| parquet_zstd | 43 | 9717 |

**两个大发现**:

1. **Lance v2.2 Blob V2 比 v2.0/v2.1 的 large_binary 慢 20x** (2986ms vs 133-151ms)！
   - v2.0/v2.1 用 `large_binary` 列 → `take()` 直接按 row index 走列存
   - v2.2 用 Blob V2 extension type + `take_blobs()` → 返回 descriptor + 额外 S3 GET 拉 payload
   - 每个 blob 是一次独立的 S3 Range GET，100 个 blob = 100 个 round-trip
   - **"Blob V2 = 更好的 ML 工作流"的宣传在"少量随机 take"场景下反效果**

2. **Lance 的 large_binary 比 Parquet 快 65-73x** (133-151ms vs 9706-9717ms)
   - Parquet 必须解压整个 row_group（这里 1024 rows × 大 blob）才能拿 1 行
   - Lance v2.0/v2.1 的 large_binary 有 per-row offset 索引
   - 所以 **想要"随机读 blob"的 Lance 用户，反而应该用 v2.0 的 large_binary 而不是 v2.2 的 Blob V2** —— 违反常识的建议

---

## 写入延迟 (seconds) 和存储大小

### `tab_flat` (1M rows, 静态分布)

| format | write_s | size_mb | 压缩率 (vs Parquet zstd) |
|---|---|---|---|
| lance_2.0 | 1.4 | 58.0 | 2.23x 大 🔴 |
| lance_2.1 | 0.7 | 29.4 | 1.13x 大 |
| **lance_2.2** | **0.8** | **29.5** ⭐ | 1.13x 大 |
| parquet_snappy | 42† | 33.2 | 1.28x 大 |
| parquet_zstd | 38† | **26.0** ⭐ | 1.00x |

† Parquet 走 Spark 路径（createDataFrame + write）—— 写入慢主要是 Spark 初始化，不完全代表 Parquet 写入成本。Lance 走 pylance 单机路径所以快。

- **lance_2.1/2.2 压缩明显提升** (58 → 29MB, **49% 减少**)
- 但仍比 parquet_zstd 大 13%

### `tab_vec` (1M × 128d float32 ≈ 512 MB raw)

| format | write_s | size_mb | 压缩率 |
|---|---|---|---|
| lance_2.0 | 2.4 | 570 | 无压缩 (原始 512MB) |
| lance_2.1 | 2.2 | 541 | 无压缩 |
| lance_2.2 | 2.4 | 541 | 无压缩 |
| parquet_snappy | 2.1 | 549 | 无压缩 |
| parquet_zstd | **2.7** | **504** ⭐ | 2% 压缩（float32 熵高） |

- **Lance 对 FixedSizeList\<f32\> 无压缩** —— 跟 Tier 0 I（`I_compression.py`）结论一致，证实 [#3705](https://github.com/lance-format/lance/discussions/3705)
- parquet_zstd 反而略小（随机 float32 熵高，压缩无效）

### `tab_nested` (struct/list/map)

| format | write_s | size_mb |
|---|---|---|
| lance_2.0/2.1 | ❌ 不支持 map | — |
| lance_2.2 | 0.9 | 45.9 |
| parquet_snappy | 0.8 | 49.1 |
| parquet_zstd | **0.9** | **37.6** ⭐ |

- v2.2 可用，**比 parquet_snappy 小 7%**，但比 parquet_zstd 大 22%

### `tab_blob` (5K rows × ~700KB avg = 3.7GB raw payload)

| format | write_s | size_mb |
|---|---|---|
| lance_2.0 | **8.7** ⭐ | 3727 |
| lance_2.1 | 12.4 | 3727 |
| **lance_2.2 (Blob V2)** | **104** 🔴 | 3727 |
| parquet_snappy | 11.0 | 3727 |
| parquet_zstd | 11.3 | 3727 |

- **lance_2.2 blob 写入 104 秒，比 v2.0 慢 12x** —— Blob V2 的 dedicated mode 为每个 dedicated blob（>4MB）写独立 S3 object，530 个 dedicated blob = 530 次 S3 PUT round-trip
- 所有格式最终 size 相同（随机 bytes 不可压）

---

## 🐛 发现的 Bug / 设计坑

### 1. 🔴 Blob V2 在"少量随机 take"场景比 large_binary 慢 20x

**场景**: 图片/文档的随机抽取（常见于 notebook 做 sanity check）
**现象**: `ds.take_blobs("payload", indices=[100 random])` 在 v2.2 上 2986 ms，v2.0/v2.1 的 `ds.take(indices, columns=["payload"])` 只要 133-151 ms
**原因**: Blob V2 的 take_blobs 先返回 descriptor，然后对每个 row 做独立 S3 Range GET。100 rows × S3 latency ~30ms = 3 秒
**LanceDB 官方声称**: "Blob V2 excels at random access" —— **在 ≤100 rows 级别不成立**
**工作流建议**:
- **小批量随机访问** → v2.0/v2.1 + large_binary 列
- **大批量顺序训练** (DataLoader 1000+ rows/batch) → v2.2 + Blob V2（摊销 S3 固定开销）
- 这跟 [ml-training-bench](../ml-training-bench/REPORT.md) 20K images batch=256 时 Lance 赢的结论一致：**Blob V2 需要大 batch 才值得**

### 2. 🔴 Lance v2.1 在 vector full_scan 上比 v2.0 慢 54%

**场景**: `tab_vec` full_scan p50：v2.0=500ms，v2.1=773ms
**一致性**: 跟 J（format_versions）发现的"v2.1 flat full_scan 回归"是同一类问题，但这里在 vec 上表现更严重
**意味着**: v2.0 → v2.1 的优化有方向性退化，vector 这条路径没被 regression test 覆盖

### 3. 🔴 Lance v2.0/v2.1 无法写 map 类型

- v2.0: `PanicException: not yet implemented`（未 graceful error）
- v2.1: `OSError: Map data type is only supported in Lance file format 2.2+`（graceful）
- **只有 v2.2 可用** —— 想用 map 就必须吃 v2.2 的其它回归
- 对应 release note 里的 "v2.2 adds map support"，实锤

### 4. 🟡 Lance struct/nested 子字段下推不如 Parquet

`nested_subread` p50：Lance v2.2 = 156 ms；Parquet = 95 ms（1.6x 更快）
说明 Parquet 的 "只读 struct.inner.a" 精准列定位在 Lance v2.2 里还是全列解码

### 5. 🟡 Lance v2.0 的 tab_flat 压缩率明显差

tab_flat size: v2.0=58 MB, v2.1/2.2=29 MB —— **v2.1 对 flat schema 压缩改善一倍**，但同样的改进没反映到 vector（tab_vec 各版本一致 ~541MB）

---

## 与之前测试的关系

L2 在方法论上**精确覆盖了**之前几个测试的空白：

| 之前的测试 | 局限 | L2 如何补 |
|---|---|---|
| [Tier 0 B](REPORT.md#b-filter-性能-vs-parquet-issue-738-3-年未解) | 只测 flat filter | L2 把 filter 扩到 4 种 shape |
| [Tier 2 I](REPORT_tier2.md) | 只测 I_compression 单列 | L2 在 nested/blob/vec 上验证压缩 |
| [Tier 2 J](REPORT_tier2.md) | 只在 flat full_scan 上报告 v2.1 回归 | L2 在 vec 上发现 v2.1 **更严重的**回归 |
| [ml-training-bench](../ml-training-bench/REPORT.md) | 只测 take_blobs，无 Blob V2 vs large_binary | L2 实锤 **Blob V2 在少量 random take 场景反而更慢** |

---

## 生产建议

### 根据工作负载选格式版本

| 你的数据形状 | 推荐 | 为什么 |
|---|---|---|
| **纯扁平 OLAP 表** (tab_flat) | **lance_2.1 或 2.2** | 压缩比 2.0 好一倍，读性能相当 |
| **向量检索表** (tab_vec) | **lance_2.0** ⭐ | v2.1/2.2 在 vector 上回归，2.0 最快 10-14x vs Parquet |
| **含 map 的 nested 表** | **lance_2.2** | 只有 2.2 支持 |
| **含 struct/list 的 nested 表** | **parquet_zstd** | Lance nested sub-read 慢 1.6x，压缩也差 22% |
| **少量随机读 blob** (≤100 rows/call) | **lance_2.0 + large_binary** | 比 v2.2 Blob V2 快 20x |
| **大批量顺序读 blob** (ML training, ≥256/batch) | **lance_2.2 + Blob V2** | 摊销后跟 large_binary 相近，且元数据更干净 |

### 不要做的

- ❌ **不要**对向量列升级到 lance_2.1/2.2，除非需要 map
- ❌ **不要**对"notebook 里随便 take 10-100 个 blob"场景用 Blob V2 —— **慢 20x**
- ❌ **不要**相信"新版本一定更好"，Lance 的 storage_version 不同子系统成熟度差很大

### 未来应该看什么

- v2.1 vector 回归到底是什么 PR 引入的（git bisect + regression test gap）
- Blob V2 的 "dedicated" mode 在 dedicated count > 100 时 write 时间是否还能接受
- nested sub-read 在 Lance 的 roadmap 上有没有优化 plan

---

## 📁 Raw data

- `results/L2_manifest_1m.json` — 写入侧 manifest（哪个 workload × format 在 S3 哪里）
- `results/L2_format_compare_1m.json` — 测量侧完整 p50/p99/runs
- `results/L2_manifest_smoke2.json` / `L2_format_compare_smoke2.json` — 10K 行 smoke 验证（通过后才跑 1M）

## 相关脚本

- `scripts/L2_write.py` — 分 5 种格式写 4 种 workload
- `scripts/L2_measure.py` — 对 manifest 里每个 URI 跑标准化测量

## 设计决策（记录在脚本 docstring 里）

- **Lance 所有 workload 都走 pylance 单机**: lance-spark 0.0.15 不支持传 `data_storage_version`（`SparkOptions` 里没这个 key）；且它的 `SupportsCatalogOptions` 在 Spark 3.5 下会抛 `CatalogNotFoundException`
- **Parquet tab_vec/tab_nested/tab_blob 走 pyarrow 单机**: Spark `createDataFrame` 对 FixedSizeList 和 nested pandas 类型推断失败
- **Parquet tab_flat 走 Spark**: 展示 Spark 写 flat schema 的可行性（与 M 系列一致）
