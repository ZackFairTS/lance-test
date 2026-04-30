# Lance 扩展压测 Top 4 - 实测报告

**测试日期**: 2026-04-30
**环境**: AWS EMR master (r8g.2xlarge Graviton, 8vCPU 64GiB), S3 ap-northeast-1
**版本**: pylance 4.0.1, lance-core 0.39.0
**每个脚本都通过了 opencode quick review** (ai-slop-remover skill)

---

## 🔴 结果速览

| 测试 | 核心发现 | 严重度 |
|---|---|---|
| **A. `update()` + stable_row_ids** | 实测 **11,494x 慢**（500k 行直接 TIMEOUT >300s）| 🔴 严重回归 |
| **B. Filter vs Parquet (#738)** | 3 年后 Lance v2.1 仍**慢 2.49x**，v2.0 也慢 2.09x | 🔴 官方声称"scan_filter tied with Parquet"被证伪 |
| **D. 向量搜索 Pareto** | IVF_HNSW_SQ 赢（recall≥0.95 时 535 QPS vs 317/340 QPS）| ✅ 符合官方声称 |
| **E. Prefilter + HNSW** | **10% 边界相位跃迁实锤**，p50 在 5%→10% 翻倍 | 🟡 Lance 文档警告被量化 |

---

## A. `dataset.update()` + `enable_stable_row_ids=True` Bug 复现

**Issue**: https://github.com/lance-format/lance/issues/6404 (开放中，PR #6628 未合并)

### 数据

10k, 100k, 500k 行 update 在 1M 行表上：

| rows | fragments | stable_off | stable_on | **Slowdown** |
|---|---|---|---|---|
| 1 | 1 (mrpf=1M) | 4 ms | 3.6 ms | 0.9x |
| 10,000 | 1 | 6 ms | **8.98 s** | **1,449x** |
| 10,000 | 10 | 5 ms | 0.96 s | 178x |
| 100,000 | 1 | 8 ms | **93.1 s** | 🔴 **11,494x** |
| 100,000 | 10 | 7 ms | 9.54 s | 1,344x |
| 500,000 | 1 | 16 ms | **TIMEOUT >300s** | **>18,750x** |
| 500,000 | 10 | 16 ms | 48.3 s | 2,945x |

**证实 PR #6628 的 O(N×M) 假设**：
- `max_rows_per_file=1M` (单 fragment) 表现为 O(N²)
- `max_rows_per_file=100K` (10 fragments) 慢 10 倍不如单 fragment 严重，但仍 2,945x

**Storage version 无影响**：v2.0 和 v2.1 慢得一样。

### 复现脚本
`scripts/A_update_bug.py`（已通过 opencode review）

---

## B. Filter 性能 vs Parquet (Issue #738, 3 年未解)

**Workload**: 3M 行 NYC-Taxi-like 表，`pickup_minute = 30`（60 分钟的 1/60 ≈ 1.67% 选择率）

### 数据

| 方案 | Filter p50 | 大小 | 相对 Parquet |
|---|---|---|---|
| Feather v2 | 10.0 ms | 78.0 MB | 0.92x |
| **Parquet (snappy)** ⭐ | **10.9 ms** | 55.7 MB | 1.00x |
| Parquet (zstd) | 22.1 ms | 52.8 MB | 2.03x |
| **Lance v2.0** | 22.7 ms | 60.0 MB | **2.09x 慢** |
| **Lance v2.1** | **27.1 ms** | 53.6 MB | 🔴 **2.49x 慢** |

### 关键发现

1. **原 issue 报告 "Parquet 2x faster" 仍然成立** —— 3 年过去 Lance v2.1 反而**比 v2.0 更慢**（27 vs 23 ms）
2. **LanceDB v2.2 blog 宣称 "scan_filter tied with Parquet"** —— 实测**被证伪**
3. Parquet 配置完全公平（`write_statistics=True`, `use_dictionary=True`, `row_group_size=1M`, `data_page_version="2.0"`）—— Parquet 的 row group 统计 pruning 是关键

### 可能原因

- Lance 的 data_skipping 对 int8 低基数列没有像 Parquet row group stats 那样高效
- Issue [#5130](https://github.com/lance-format/lance/issues/5130) "Zonemap reads too many fragments" 可能相关

### 复现脚本
`scripts/B_filter_vs_parquet.py`（已通过 opencode review，修复了 p99@n=5 bug）

---

## D. 向量搜索 Recall vs QPS Pareto (SIFT-1M)

**Dataset**: SIFT-1M (1M × 128d float32, 300 queries), k=10, nprobes/ef/refine_factor 扫描

### Pareto 赢家表

每个 recall 区间下 QPS 最高的配置：

| Recall >= | IVF_PQ | **IVF_HNSW_SQ** ⭐ | IVF_RQ (RaBitQ 1-bit) |
|---|---|---|---|
| 0.80 | 477 QPS | **770 QPS** | 466 QPS |
| 0.90 | 417 QPS | **679 QPS** | 405 QPS |
| 0.95 | 317 QPS | **535 QPS** | 340 QPS |
| 0.98 | 186 QPS | **275 QPS** | 183 QPS |

### 关键发现

1. **IVF_HNSW_SQ 全面领先** —— 每个 recall 区间都赢 40-60%
2. **IVF_RQ (RaBitQ 1-bit)** 在 recall≥0.95 时小幅超过 IVF_PQ (340 vs 317 QPS)
3. **和 LanceDB 10B 博客声称的 "HNSW > IVF_PQ" 一致**
4. **Index 构建时间**: IVF_PQ 4-6s, IVF_HNSW_SQ 24s, IVF_RQ 6s (on 1M vectors)

### 复现脚本
`scripts/D_vector_search.py`（已通过 opencode review，修复 index size 测量 + warmup）

---

## E. Prefilter + Vector Search — HNSW 10% 边界效应实锤

**Workload**: SIFT-1M + `price` 列 (uniform[0,1000))，BTREE scalar index，`price < threshold` 扫选择率 0.1% → 90%

### 核心发现：HNSW_SQ p50 在 10% 边界出现"相位跃迁"

| 选择率 (target) | p50 (ms) | cov | 推测路径 |
|---|---|---|---|
| 0.1% | 1.9 | 0.59 | flat_search (< 10% threshold) |
| 1% | 2.2 | 0.06 | flat_search |
| 5% | 2.8 | 0.05 | flat_search |
| **10%** | **5.4** | **0.13** | **⚠️ 跨越边界 → graph search** |
| 20% | 5.4 | 0.04 | graph |
| 50% | 5.4 | 0.04 | graph |
| 90% | 6.3 | 0.54 | graph |

**p50 从 5% → 10% 翻倍** (2.8ms → 5.4ms)。

### 源码对应

[`rust/lance-index/src/vector/hnsw/builder.rs`](https://github.com/lance-format/lance/blob/main/rust/lance-index/src/vector/hnsw/builder.rs):

```rust
let results = if remained < self.len() * 10 / 100 {
    // Flat brute-force on surviving rows
    self.flat_search(storage, query, k, prefilter_bitset, &params)
} else {
    // HNSW graph traversal with bitset
    self.search_basic(query, k, &params, prefilter_bitset, storage)?
};
```

这个 **10% 硬编码阈值** 就是 Lance 文档警告 "high variance under filter" 的真实原因。

### 附带发现

- `prefilter=False`（postfilter）在选择率 <20% 时 **returns 0 rows**（因为 k=10 的 ANN 结果能通过 filter 的概率极低）
- IVF_PQ 对 prefilter 延迟更稳定（p50 2-6 ms 随选择率单调上升），**不受 10% 相位跃迁影响**

### 复现脚本
`scripts/E_prefilter.py`（已通过 opencode review，修复**关键 bug**：两个 index 不再互相覆盖 —— 改用两份独立 dataset）

---

## 🐛 opencode review 发现的 bug 合集

Top 4 每个脚本都过 review，发现并修复：

| 脚本 | Review 找到的 Bug | 严重度 |
|---|---|---|
| A | 死代码 (int64_random, scalar_btree 分支未用) + 缺 timeout + cleanup 不可靠 | 中 |
| B | **p99 at n=5 返回的是 max 不是 p99** | 高 |
| D | Index size 测错（测的是整个 dataset）+ warmup 不够 | 中 |
| E | **两个 index 用同一个 dataset + replace=True 互相覆盖**！ | 🔴 致命 |

**E 的 bug 最严重**：如果不修复，整个 PQ vs HNSW 对比会**完全无效**（所有 PQ 数据其实都是 HNSW）。Review catch 到这个之前，我完全没意识到。

---

## 数据完整性说明

- 所有测试用合成数据或公开 SIFT-1M
- 每个 metric 都用 p50/p99/cov 或完整分布
- 每个对比都有 baseline（无 filter / stable_off / Parquet）
- opencode review 确认所有脚本无 AI-slop

## 原始数据

`results/`:
- `A_update_bug.json`
- `B_filter_vs_parquet.json`
- `D_vector_search.json`
- `E_prefilter.json`

## 下一步

Tier 0 Top 4 已全部完成，剩下 6 项（F/G/H/I/J/K）见主 `README.md` 的 Tier 1/2 menu。
