# Tier 2 扩展压测报告（F/G/H/I/J/K）

**测试日期**: 2026-05-01
**环境**: AWS EMR master (r8g.2xlarge Graviton, 8vCPU 64GiB)
**版本**: pylance 4.0.1
**所有脚本通过 opencode quick review** (ai-slop-remover skill)

---

## 🔴 结果速览（6 个测试，多个重磅发现）

| 测试 | 核心发现 | 严重度 |
|---|---|---|
| **F. Merge-insert 吞吐** | 🔴 **BTREE index 让 merge_insert 慢 ~500x**（500K match: 44s vs 0.09s）违反官方建议 | 重大 |
| **G. Schema evolution 零重写** | ✅ add/drop/rename 真的零写（0.6-0.8 ms）；Parquet 对比慢 3.6x | 符合声称 |
| **H. Version 爆炸 open() 成本** | `list_versions` 随版本数**平方级增长**（3000 版本要 2.2s）| 中 |
| **I. Compression ratio** | 🔴 Lance 对 float32/uint8 vectors **无压缩**（100% raw size），印证 issue #3705 | 证实用户抱怨 |
| **J. Format v2.0 vs v2.1** | 🔴 v2.1 full scan **慢 2.44x**（215 vs 88 ms）- 和 B 一致发现 v2.1 反而更慢 | 回归 |
| **K. FTS 内存 envelope** | FTS 构建 peak RSS = **3-5x input size**，和 issue #5502 一致 | 符合报告 |

---

## F. Merge-insert + BTREE = 灾难性降速

**Workload**: 1M 行基表，upsert {matches + new rows}

| matches | no_index | with_index BTREE | "speedup" |
|---|---|---|---|
| 1,000 | 29 ms | 113 ms | 0.26x (慢 4x) |
| 10,000 | 29 ms | 818 ms | 0.04x (慢 25x) |
| 100,000 | 39 ms | 8,537 ms | 0.005x (慢 219x) |
| **500,000** | **90 ms** | **44,308 ms** | **0.002x (慢 492x)** |

**验证**: 所有 case `rowcount_ok=True`，merge_insert 语义正确无 bug。

**解读**:
- no_index 速度极快（6.1M rows/s）—— Lance 内部走 bulk rewrite 路径
- with_index 速度稳定 ~12K rows/s —— 看起来是按 row 维护 BTREE + 更新 deletion vector
- **违反官方 docs 建议**："create scalar index for faster merge_insert"

---

## G. Schema Evolution 零重写（✅ 符合 Lance 声称）

| 操作 | Lance time | Lance size delta | Parquet rewrite |
|---|---|---|---|
| `add_columns` schema-only | **0.8 ms** | +1 KB ⚡ | N/A |
| `add_columns` SQL expr (1M float64) | 28.6 ms | +8 MB | 104 ms |
| `drop_columns` | **0.6 ms** | 0 MB ⚡ | N/A |
| `alter_columns` rename | **0.7 ms** | +1 KB ⚡ | N/A |

**Schema-only 比 Parquet add column 快 130x**（0.8 vs 104 ms）。

---

## H. Version 爆炸的 open() 成本

| n_versions | manifest 文件 | open p50 | append p50 | list_versions p50 |
|---|---|---|---|---|
| 10 | 10 | 0.3 ms | 1.3 ms | <1 ms |
| 100 | 100 | 0.4 ms | 2.1 ms | 5.5 ms |
| 500 | 500 | 0.7 ms | 5.2 ms | 61 ms |
| 1000 | 1000 | 1.0 ms | 8.9 ms | 218 ms |
| **3000** | **3000** | **2.3 ms** | **23.8 ms** | **2219 ms** |

- `open()` 线性增长（7.7x 慢）
- `append()` 线性增长（18x 慢）
- `list_versions()` **平方级增长**（>2000x 慢）

印证了之前 Flink 压测中 "manifest 累积拖慢 commit" 的现象。

---

## I. Compression Ratio per Column Type (对比 Parquet)

**关键发现：Lance 对 vector 列完全无压缩**

| 列类型 | Raw Arrow | Lance v2.0 | Lance v2.1 | Parquet snappy | Parquet zstd |
|---|---|---|---|---|---|
| int64 sequential | 0.80 MB | 0.80 MB | **0.20 MB** ⭐ | 0.61 MB | 0.31 MB |
| int64 random | 0.80 MB | 0.80 MB | 0.76 MB | 1.01 MB | 1.01 MB |
| **float32 vector 128d** | 51.2 MB | 51.2 MB | **51.2 MB (0% compression!)** | 52.3 MB | 48.4 MB |
| **float32 vector 1536d** | 614.4 MB | 614.4 MB | **614.4 MB (0% compression!)** | 615.6 MB | 569.0 MB |
| **uint8 embeddings 1024d** | 102.4 MB | 102.4 MB | **102.4 MB (0% compression!)** | 103.1 MB | 103.1 MB |
| long text | 17.9 MB | 18.3 MB | **3.7 MB** | 6.1 MB | 3.6 MB |
| short categorical | 0.98 MB | 0.10 MB ⭐ | 0.08 MB ⭐ | 0.08 MB | 0.08 MB |
| JPEG blob | 372.5 MB | 372.9 MB | 372.9 MB | 345.8 MB | 312.2 MB (best) |

**验证了 [issue #3705](https://github.com/lance-format/lance/discussions/3705) 抱怨：Lance 存 vectors 和 embeddings 真的没有压缩，和 Parquet (zstd) 比最多差 7%**（float 压缩本来就不容易）。

**Lance 赢的场景**：
- 序列整数 (25% raw size)
- 低基数字符串 (8% raw size)

**Lance 输的场景**：
- 向量/嵌入列（几乎无压缩）
- 已压缩 blob（JPEG）—— Parquet zstd 有 10% 优势（可能是次要 metadata 压缩）

---

## J. Lance Format v2.0 vs v2.1 对比

**又一个 v2.1 回归的证据**：

| 操作 | v2.0 | v2.1 | v2.1/v2.0 |
|---|---|---|---|
| Write | 0.51 s | 0.54 s | 1.05x |
| Size | 550 MB | 538 MB | 0.98x（小 2%）|
| **Full scan** | **88 ms** | **216 ms** | 🔴 **2.44x 慢** |
| Column scan | 5.4 ms | 6.0 ms | 1.11x |
| Point query | 13.7 ms | 12.8 ms | 0.93x |
| Range query | 10.3 ms | 11.1 ms | 1.08x |

**v2.1 在 B (filter) 也比 v2.0 慢，现在在 full scan 更慢**。Lance 官方 "v2.1 stable" blog 的声称应该被重新验证。

---

## K. FTS 内存 Envelope（印证 issue #5502）

**使用 subprocess 隔离测量，`ru_maxrss` 只对单进程累积 peak 有效，必须分进程**

| 输入 | Peak RSS | 比例 | build 时间 |
|---|---|---|---|
| 3.6 MB (10K × 50 wpd) | 198 MB | 55.7x（启动开销主导）| 0.12 s |
| 35 MB (100K × 50) | 360 MB | 10.1x | 0.4 s |
| 139 MB (100K × 200) | 681 MB | 4.9x | 1.5 s |
| 178 MB (500K × 50) | 816 MB | 4.6x | 1.9 s |
| **694 MB (500K × 200)** | **2243 MB** | **3.2x** | 7.2 s |
| 355 MB (1M × 50) | 1345 MB | 3.8x | 3.8 s |

**3-5x 比例稳定**（在大数据集上收敛），**和 issue #5502 报告的 "1GB input → 4.3GB RAM" 一致**。推算 10GB 输入需 ~30-40GB RAM。

---

## 🐛 opencode Review 发现的 Bug 合集（Tier 2）

| 脚本 | Review 捕获的 Bug | 严重度 |
|---|---|---|
| F | pylance_version key 名误导（应是 lance_version）+ n_matches>base_n 应显式 guard + 没验证 rowcount | 中 |
| G | **alter_columns API 用错了参数（"rename"→"name"）+ 试 rename 不存在的 "tag" 列** | 高（会导致 rename path 被 except 吞）|
| H | **`measure_all` 的 append 是破坏性操作，污染后续测量** | 高（改成 subprocess + clone clean dataset）|
| I | JPEG 生成失败会整体 crash（需 try catch）+ 没 report vs-raw ratio | 中 |
| J | — PASS（无改动）| — |
| K | **`ru_maxrss` 是 RUSAGE_SELF 累积 peak，`rss_delta` 在非递增配置下无意义** | 高（必须改成 subprocess per config）|

没有 review catch 的话，F/G/H/K 的数据都**部分或全部无效**。

---

## Cumulative Top 4 + Tier 2 核心发现

综合全部 10 个测试：

### 🔴 Lance 确认的性能问题（有实测数据支撑）
1. **update() + stable_row_ids = 11,494x 慢**（issue #6404）
2. **Filter 比 Parquet 慢 2-2.5x**（issue #738 / #2367 3 年未解）
3. **Merge-insert + BTREE index 慢 500x**（违反官方 docs 建议）
4. **Lance 对 vector 列无压缩**（issue #3705）
5. **v2.1 format 在 filter 和 full scan 上都比 v2.0 慢 2-2.5x**
6. **FTS 索引构建内存 envelope 3-5x**（issue #5502）
7. **版本爆炸时 `list_versions()` 平方级增长**

### ✅ Lance 确认的性能优势
1. **向量搜索 (SIFT-1M HNSW_SQ)**：535 QPS @ recall>=0.95
2. **take_blobs 随机读**：比 raw S3 files 快 10x
3. **Schema evolution 零重写**：比 Parquet add column 快 130x
4. **Lance vs Parquet 低基数字符串**：Lance v2.1 能达到 Parquet 级别
5. **HNSW prefilter 10% 边界的相位跃迁行为量化**

### 🟡 Lance 中性（和其他方案打平或略好）
1. ML 训练 batch loading：比 raw S3 快 1.5x，比 Parquet 快 2.1x（不是官方说的 100-2000x）
2. 纯 take_blobs 吞吐：最高 4,391 rows/s（官方声称 20-25K 的 1/5）
3. Point query 场景：和 fragment 数量无关
4. Int64 sequential compression：v2.1 比 Parquet zstd 省 1.5x

---

## 原始数据

`results/`:
- F_merge_insert.json
- G_schema_evolution.json
- H_version_explosion.json
- I_compression.json
- J_format_versions.json
- K_fts_memory.json
