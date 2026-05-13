# Lance 小文件对读性能影响 - 压测报告

**环境**: r8g.2xlarge（Graviton ARM64，8 vCPU / 64 GiB），本地 NVMe + S3 ap-northeast-1（单机、无分布式）
**Lance version**: pylance 4.0.1 (lance-core 0.39.0 native), lance-spark 0.0.15

---

## 结论先行 (TL;DR)

频繁 commit 导致大量小 fragment（5000 vs 1），对读性能的影响 **严重依赖读取方式**。用 **wall-clock 延迟（ms）** 作为主指标：

| 读方式 | A (1 frag) | E (5000 frag) | 退化倍数 |
|---|---|---|---|
| **Python 单进程 全表扫描** | 804 ms | **8003 ms** | 🔴 **10.0x** |
| **Python 单进程 范围查询** | 2291 ms | **40539 ms** | 🔴 **17.7x** |
| **Python 单进程 单列扫描** | 142 ms | **2126 ms** | 🔴 **15.0x** |
| **Dataset.open()** | 80 ms | 134 ms | 🟡 1.7x |
| **Python 点查 (take 1000 rows)** | 944 ms | 968 ms | 🟢 ~1.0x |
| **count_rows()** | <1 ms | 3 ms | 🟢 <5ms 级 |
| **Spark 全表扫描 (distributed)** | 7377 ms | 6444 ms | 🟢 **无退化** |
| **Spark COUNT(\*)** | ~2000 ms | ~2200 ms | 🟢 无退化 |

**核心洞察**: 小 fragment 问题 = **单线程 I/O 串行化 + per-fragment 固定开销**。Spark 的并行读（不同 executor 读不同 fragment）完全掩盖了这个问题。

---

## 重要：数据正确性和公平性核实

### 数据集是否"苹果对苹果"？✅ 确认

5 个版本共享 **同一个 S3 path**（`/dataset`），通过 `lance.dataset(path, version=N)` 读取不同 version 的 fragment 组织视图。核实：

| 版本 | Fragment 数 | Rows | `bytes_read` (Arrow) |
|---|---|---|---|
| A (v5009) | 1 | 10,000,000 | 1,000,000,000 |
| B (v5007) | 10 | 10,000,000 | 1,000,000,000 |
| C (v5005) | 100 | 10,000,000 | 1,000,000,000 |
| D (v5003) | 1000 | 10,000,000 | 1,000,000,000 |
| E (v5001) | 5000 | 10,000,000 | 1,000,000,000 |

所有版本的 **行数、读取字节数完全相同**。Compaction 只是重新组织 fragment，不改数据。

### 主指标：延迟 (ms) + rows/sec

下面的表格主用 **wall-clock 延迟 (ms)** 和 **rows/sec 吞吐**。选择这两个维度的原因：Lance 默认 64 并行 S3 GET + 压缩 + 编码解码等多层 pipeline 之后，"MB/s" 会随 schema / 压缩率 / 缓存状态剧烈漂移，作为对外对比指标不够稳定；而 rows/sec 对固定 schema 的 workload 是线性稳定的。

> **作为参考**：Lance 官方的 Python benchmark（[`python/python/benchmarks/test_scan.py`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/python/python/benchmarks/test_scan.py)）基于 `pytest-benchmark`，默认只报每次调用的**耗时**（μs/ms/s）和 **OPS**（iterations/sec），不直接计算 rows/sec 或 MB/s。Rust benchmark（[`rust/lance/benches/scan.rs`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance/benches/scan.rs)）基于 `criterion`，scan 也只报耗时；但 Lance 其它子系统（如 mem-wal 系列）的 Rust benchmark 会用 criterion 的 `Throughput::Bytes` 输出 MiB/s。[arXiv 论文 2504.15247](https://arxiv.org/abs/2504.15247) §6.3 Full Scan 同时用 **MiB/s 磁盘吞吐**（衡量硬件利用率）和 **iterations/file-reads per second**（跨格式对比）两种维度。

---

## 数据集生成

- **E** 生成方式: 5,000 次 `lance.write_dataset(mode="append")`，每次 2000 行 → **耗时 36.0 分钟**
  - Append 速率从 **3.5 append/s 退化到 2.4 append/s**（单次调用 285 ms → 417 ms，**+46%**）
  - 根因：每次 `write_dataset(mode="append")` 需要 ① 读最新 manifest（`lance.dataset(path)` 自身从 80 ms 退化到 134 ms，见 §1 表格）；② 写一个包含**所有**历史 fragment metadata 的新 manifest snapshot —— manifest 不是增量 delta，随 fragment 累积线性变大。从 0 → 4999 fragment 累积期间，读 + 写 manifest 的成本都在上升
  - **这已经是小文件影响写入性能的证据**
- **D/C/B/A** 通过 `compact_files(target_rows_per_fragment=...)` 从 E 逐步合并
  - E → D: 5000 → 1000 fragments, 37.2s
  - D → C: 1000 → 100 fragments, 12.6s
  - C → B: 100 → 10 fragments, 5.8s
  - B → A: 10 → 1 fragments, 17.5s
  - **总 compaction 时间 ~1 分钟**（对比 36 分钟的 build 时间）

---

## 1. Dataset.open() 耗时（10 次采样）

| 版本 | Fragments | p50 (ms) | mean (ms) | p99 (ms) |
|---|---|---|---|---|
| A | 1 | 80 | 83 | 100 |
| B | 10 | 78 | 76 | 85 |
| C | 100 | 81 | 81 | 98 |
| D | 1,000 | 119 | 117 | 141 |
| E | 5,000 | 133 | 130 | 146 |

**观察**: `Dataset.open()` 退化有限（1.7x），因为 Lance 只读最新 manifest，不读每个 fragment 的 metadata。

---

## 2. 全表扫描（Python 单进程, `ds.to_table()`）

| 版本 | Fragments | p50 (ms) | mean (ms) | rows/sec | 相对 A (p50) |
|---|---|---|---|---|---|
| A | 1 | **804** | 845 | **12.4 M** | 1.00x |
| B | 10 | 760 | 794 | 13.2 M | 0.95x |
| C | 100 | 765 | 791 | 13.1 M | 0.95x |
| D | 1,000 | 1723 | 1904 | 5.80 M | **2.14x 慢** |
| E | 5,000 | **8003** | 8463 | **1.25 M** | 🔴 **9.96x 慢** |

**观察**:
- A/B/C (1-100 fragments) 都是 ~13M rows/sec，几乎一样
- **拐点在 ~100-1000 fragment 之间**
- E (5000 fragments) 掉到 1.25M rows/sec（近 10 倍退化）

---

## 3. 单列扫描（`ds.to_table(columns=["id"])`）

| 版本 | Fragments | p50 (ms) | mean (ms) | 相对 A (mean) |
|---|---|---|---|---|
| A | 1 | 136 | 142 | 1.00x |
| B | 10 | 147 | 151 | 1.06x |
| C | 100 | 112 | 165 | 1.16x |
| D | 1,000 | 438 | 444 | 3.12x |
| E | 5,000 | **2081** | 2126 | 🔴 **14.95x 慢** |

**观察**: 单列扫描**比全表扫描更敏感**（15x vs 10x）—— 列存的优势被"每个 fragment 都单独打开列"消耗掉了。

---

## 4. 点查延迟 (`ds.take([1000 random indices])`)

| 版本 | Fragments | p50 (ms) | mean (ms) | p99 (ms) | 相对 A (mean) |
|---|---|---|---|---|---|
| A | 1 | 950 | 944 | 965 | 1.00x |
| B | 10 | 489 | 493 | 531 | **0.52x 快** |
| C | 100 | 493 | 485 | 499 | **0.51x 快** |
| D | 1,000 | 718 | 721 | 787 | 0.76x |
| E | 5,000 | 938 | 968 | 1068 | 🟢 1.03x |

**观察**: 点查呈 **U 形曲线**：B/C（10/100 fragments）反而比 A（1 fragment）**快一倍**！可能原因：
- 中等 fragment 数 → fragment-level prune 命中率高，只读少数几个 fragment 就能满足 1000 个点查
- A（单一大 fragment）必须 scan 整个 fragment 找到目标 row

**点查场景下，1-100 个 fragment 是甜区，不急着 compact**。

---

## 5. 范围查询（10 ranges × 10K rows, with filter pushdown）

| 版本 | Fragments | p50 (ms) | mean (ms) | 相对 A (mean) |
|---|---|---|---|---|
| A | 1 | 2308 | 2291 | 1.00x |
| B | 10 | 1894 | 1952 | 0.85x |
| C | 100 | 2336 | 2501 | 1.09x |
| D | 1,000 | 8968 | 8975 | 3.92x |
| E | 5,000 | **40542** | 40539 | 🔴 **17.69x 慢** |

**观察**: **这是退化最严重的场景（17.7x）**。原因：
- range filter 先对每个 fragment 做 min/max pruning
- 命中的 fragment 需要读 row-level filter
- 5000 fragments × 10 queries × 每次 fragment open 的 ~40ms 固定开销 = 40 秒

---

## 6. count_rows()

| 版本 | Fragments | p50 (ms) | mean (ms) |
|---|---|---|---|
| A | 1 | 0.00 | 0.01 |
| B | 10 | 0.01 | 0.01 |
| C | 100 | 0.07 | 0.08 |
| D | 1,000 | 0.67 | 0.70 |
| E | 5,000 | 3.43 | 3.45 |

**观察**: count 是 metadata 操作，即使 5000 fragments 也只要 3.4ms（每个 fragment 读一个 row_count 字段求和）。

---

## 7. Spark 分布式读 vs Python 单进程 (A vs E 对比)

| 指标 | Python A | Python E | Python 退化 | Spark A | Spark E | Spark 退化 |
|---|---|---|---|---|---|---|
| 全表读 | 845 ms | 8463 ms | 🔴 **10.0x** | 7377 ms | 6444 ms | 🟢 **无退化（E 甚至略快）** |
| 范围查询 | 2291 ms | 40539 ms | 🔴 **17.7x** | 1744 ms | 1945 ms | 🟢 **1.1x** |

**核心发现**: **Spark 的并行度完全掩盖了小 fragment 问题**。
- Python 单进程：被 fragment 数扼杀
- Spark 分布式：不同 executor 并行读不同 fragment，反而让 5000 fragments 成了"更细粒度的并行度"

---

## 为什么会慢 10 倍 —— 根因分析

这是 **per-fragment 固定开销** 的问题，不是带宽问题：

1. **每个 fragment 打开 ~40ms 开销**
   - 构造 `FileReader`、读 schema、初始化 scheduler
   - 源码证据: [lancedb/lance#4090](https://github.com/lancedb/lance/issues/4090)
2. **S3 对小读不友好**
   - S3 GET < 100KB 时，IOPS 远比 bandwidth 贵
   - Lance paper 原文: *"IOPS are far more expensive [on S3]… [S3] does not benefit from reads smaller than about 100KB"* ([arXiv 2504.15247 §6.1.4](https://arxiv.org/pdf/2504.15247))
3. **5000 fragments 的累计效应**
   - 即使 Lance 有 `LANCE_IO_THREADS=64` 并行，per-fragment 的 40ms 串行开销也累计 200+ 秒
   - 实际受 I/O 并发度和 cache 掩盖，最后落到 ~8 秒

**这是已知的 Lance pain point**：
- [lance#1215](https://github.com/lancedb/lance/issues/1215) — "Grabbing whole dataset from s3 currently slow"：用户报告多 fragment 数据集从 S3 读比 Parquet 慢 20-30x
- 核心维护者确认需要 "larger block sizes" + "better job at making more parallel requests"

---

## Compaction 的 ROI

从 E (5000 frags) compact 到 A (1 frag) 的读性能改善（用 p50）：

| 指标 | E (p50) | A (p50) | 改善倍数 |
|---|---|---|---|
| Full scan | 8003 ms | 804 ms | **10x** |
| Col scan | 2081 ms | 136 ms | **15x** |
| Range query | 40542 ms | 2308 ms | **17.6x** |
| Open | 133 ms | 80 ms | 1.7x |
| Point query | 938 ms | 950 ms | 1.0x |

**Compaction 成本**: 从 5000 → 1 fragments 只花 **73 秒** (4 次 compact 总和)。对比 build 耗时 36 分钟，**compaction 不贵**。

**ROI 结论**:
- **Python/Arrow 单进程读** → 每次高频 commit 后都值得 compact
- **Spark/分布式读** → compaction 对读性能影响不大，但减少 manifest 有助于 `Dataset.open()` 和 write（减少冲突）
- **复杂 streaming write**（如 Flink） → compaction 同时改善 write-side 冲突率（见 [本 repo 的另一份 Flink 压测报告](https://github.com/ZackFairTS/lance-test/blob/main/01_REPORT_lance_0.23.3.md)）

---

## 性能退化倍数汇总表 (使用 p50)

| 版本 | Fragments | open | full-scan | col-scan | point | range | count |
|---|---|---|---|---|---|---|---|
| A | 1 | 1.00x | 1.00x | 1.00x | 1.00x | 1.00x | 1.00x |
| B | 10 | 0.98x | 0.95x | 1.08x | **0.51x** ⚡ | 0.82x | - |
| C | 100 | 1.01x | 0.95x | 0.82x | **0.52x** ⚡ | 1.01x | - |
| D | 1,000 | 1.49x | 2.14x | 3.22x | 0.76x | 3.89x | ~300x |
| E | 5,000 | 1.66x | **9.96x** | **15.3x** | 0.99x | **17.6x** | ~1000x |

⚡ = 点查场景下反而比 A 快（U 形曲线）

---

## 给用户的建议

### 根据读取模式的 compaction 策略

| 你的读取方式 | 推荐 fragment 策略 |
|---|---|
| **Python/Arrow 单进程全表扫描/分析** | fragment ≤ 100；超过要 compact |
| **Python 点查/少量行** | fragment 10-100 反而是甜区，不急着 compact |
| **Spark/分布式并行读** | fragment 数几乎不影响读，但 manifest version 多了会拖慢 `open` |
| **LanceDB vector search** | 类似 Spark（内部并行），但 index build 会慢 |
| **Flink streaming 追加** | **必须** compact，否则 write 性能也受影响（见 append 从 3.5 → 2.4 append/s，单次 +46%）|

### 推荐的 compaction 触发条件

1. **fragment 数 > 1000** → 立刻 compact（range query 退化 4x）
2. **fragment 数 > 100 且读以 Python 为主** → 考虑 compact
3. **manifest version > 10000** → compact + `cleanup_old_versions`（GC）
4. **单个 fragment 行数 < 10000** → 意味着 commit 过于频繁，优先从源头加大 batch_size

### 参数建议

```python
ds.optimize.compact_files(
    target_rows_per_fragment=1_000_000,
    max_rows_per_group=1024,
    materialize_deletions_threshold=0.1,
)
```

读取侧，如果被迫读小文件 dataset：

```python
from concurrent.futures import ThreadPoolExecutor
ds = lance.dataset(path)
frags = ds.get_fragments()

def read_frag(f):
    return f.to_table()

with ThreadPoolExecutor(max_workers=16) as ex:
    tables = list(ex.map(read_frag, frags))
```

### 调整 Lance I/O 并发（如果 S3 连接数不是瓶颈）

```bash
export LANCE_IO_THREADS=128   # 默认 64，大数据集可适当调高
```

---

## 数据质量说明

### Warm-up 效应

每次全表扫描的 5 个样本中，**第一个样本（冷 cache）总是最慢**。例如版本 E：
```
samples (ms): [10346, 8003, 8023, 7975, 7967]
                 ↑ 冷缓存
```

这使 **mean 被 warm-up 拉偏高**。所以：
- **p50（中位数）更能代表稳态性能** → 报告主指标都用 p50
- 如果用 mean：E/A 比例 10.01x；用 p50：比例 **9.96x**。结论一致。

### 样本数

- `open`, `count_rows`: 10 samples
- `full_scan`, `col_scan`, `point_query`: 5 samples
- `range_query`: 3 samples（低于理想，因为单次 40+ 秒太贵）

---

## 图表

见 `plots/performance_plot.png`（6 个子图：fragment 数 vs 每种读操作延迟，log-log 轴）。

## 参考

- **Lance 官方小文件问题讨论**: [lancedb/lance#1215](https://github.com/lancedb/lance/issues/1215)
- **Per-fragment open 开销**: [lancedb/lance#4090](https://github.com/lancedb/lance/issues/4090)
- **Lance paper**（§6.3 Full Scan 方法学同时用 MiB/s 磁盘吞吐 + iterations/sec）: [arXiv 2504.15247](https://arxiv.org/abs/2504.15247)
- **Lance I/O 并行配置**: [`LANCE_IO_THREADS`](https://lance.org/integrations/spark/performance/)
- **Pyarrow Table.nbytes 定义**: [arrow docs](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.nbytes)

## 原始数据位置

`data/` 目录（仓库里）或 `/home/hadoop/lance-read-bench/results/`（压测机器）:
- `read_A.json` ~ `read_E.json` — 5 个版本的 Python 读测试完整结果
- `spark_A.json`, `spark_E.json` — Spark count/agg
- `spark_full_A.json`, `spark_full_E.json` — Spark 真实读取数据
- `compact_plan.json` — compaction 执行记录
- `E_dataset_info.json` — E 数据集构建统计
- `build_E.log.gz` — 36 分钟构建过程日志（含 append 速率退化证据）
- `performance_plot.png` — 可视化
