# Lance 小文件对读性能影响 - 压测报告

**测试日期**: 2026-04-29
**环境**: AWS EMR master (r8g.2xlarge Graviton, 8vCPU 64GiB), S3 ap-northeast-1
**Lance version**: pylance 4.0.1 (lance-core 0.39.0 native), lance-spark 0.0.15

## 结论先行 (TL;DR)

频繁 commit 导致大量小 fragment，对读性能的影响 **严重依赖于读取方式**：

| 读方式 | 5000 fragments vs 1 fragment 性能退化 |
|---|---|
| **Python 单进程全表扫描** | 🔴 **10.0x 慢** (1182 → 118 MB/s) |
| **Python 单进程范围查询** | 🔴 **17.7x 慢** (2.3s → 40.5s) |
| **Python 单进程单列扫描** | 🔴 **15.0x 慢** |
| **Dataset.open()** | 🟡 1.7x 慢 (80ms → 134ms) |
| **Python 点查 (take)** | 🟢 **几乎无差** (950ms → 938ms) |
| **count_rows()** | 🟢 都是毫秒级 |
| **Spark 分布式全表扫描** | 🟢 **几乎无差** (7.4s → 6.4s) |
| **Spark COUNT(*)** | 🟢 **几乎无差** (都是 ~2s) |

**核心洞察**: 小 fragment 问题 = **单线程 I/O 串行化问题**。Spark 的并行读（不同 executor 读不同 fragment）完全掩盖了这个问题。

## 数据集对比

| 版本 | 总行数 | Fragment 数 | 行/fragment | Lance version |
|---|---|---|---|---|
| A | 10,000,000 | 1 | 10,000,000 | 5009 |
| B | 10,000,000 | 10 | 1,000,000 | 5007 |
| C | 10,000,000 | 100 | 100,000 | 5005 |
| D | 10,000,000 | 1,000 | 10,000 | 5003 |
| E | 10,000,000 | 5,000 | 2,000 | 5001 |

## 数据生成信息

- **E** 生成方式: 5,000 次 append，每次 2000 行 → **耗时 36.0 分钟**
  - Append 速率从 **3.5/s 退化到 2.4/s**（31% 下降），因为 `write_dataset` 每次需要打开 dataset 读 manifest
- **D/C/B/A** 通过 `compact_files(target_rows_per_fragment=...)` 从 E 逐步合并生成
  - D: 5000 → 1000 fragments, 37.2s
  - C: 1000 → 100 fragments, 12.6s
  - B: 100 → 10 fragments, 5.8s
  - A: 10 → 1 fragments, 17.5s
  - **总 compaction 时间 ~1 分钟**（对比 36 分钟的 build 时间，说明 compact 不贵）

## 1. Dataset.open() 耗时（10 次采样）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | p99 (ms) |
|---|---|---|---|---|
| A | 1 | 80 | 83 | 100 |
| B | 10 | 78 | 76 | 85 |
| C | 100 | 81 | 81 | 98 |
| D | 1,000 | 119 | 117 | 141 |
| E | 5,000 | 133 | 130 | 146 |

**观察**: `Dataset.open()` 退化有限。即使 5000 fragments 也只是 134ms（比 1 fragment 慢 1.7x），因为 Lance 只读最新 manifest 文件，不读每个 fragment 的 metadata。

## 2. 全表扫描吞吐（Python 单进程）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | 吞吐 (MB/s) | 相对 A |
|---|---|---|---|---|---|
| A | 1 | 804 | 845 | 1182.8 | 1.00x |
| B | 10 | 760 | 794 | 1260.0 | 0.94x |
| C | 100 | 765 | 791 | 1264.2 | 0.94x |
| D | 1,000 | 1723 | 1904 | 525.1 | 2.25x |
| E | 5,000 | 8003 | 8463 | 118.2 | 10.01x |

**观察**:
- A/B/C (1-100 fragments) 都是 ~1200 MB/s，几乎一样
- D (1000 fragments) 降到 525 MB/s（2.3x 慢）
- E (5000 fragments) 降到 **118 MB/s（10x 慢）**
- **拐点大致在 100-1000 fragment 之间**

## 3. 单列扫描（id）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | 吞吐 (MB/s) | 相对 A |
|---|---|---|---|---|---|
| A | 1 | 136 | 142 | 562.4 | 1.00x |
| B | 10 | 147 | 151 | 531.0 | 1.06x |
| C | 100 | 112 | 165 | 484.5 | 1.16x |
| D | 1,000 | 438 | 444 | 180.4 | 3.12x |
| E | 5,000 | 2081 | 2126 | 37.6 | 14.95x |

**观察**: 单列扫描**更敏感**于小文件（E 达到 15x 退化），因为列存的优势被"每个 fragment 都要单独打开列读取"消耗掉了。

## 4. 点查延迟 (`ds.take([1000 random indices])`)

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | p99 (ms) | 相对 A |
|---|---|---|---|---|---|
| A | 1 | 950 | 944 | 965 | 1.00x |
| B | 10 | 489 | 493 | 531 | 0.52x |
| C | 100 | 493 | 485 | 499 | 0.51x |
| D | 1,000 | 718 | 721 | 787 | 0.76x |
| E | 5,000 | 938 | 968 | 1068 | 1.03x |

**观察**: 点查**反而在小文件场景下没退化**（E 和 A 都是 ~950ms）。B/C 甚至比 A 快一倍（U 形曲线，可能与 prefetch/cache 有关）。这是因为 point query 命中的数据量小，不敏感于 fragment 分布。

## 5. 范围查询（10 ranges × 10K rows, filter pushdown）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | 相对 A |
|---|---|---|---|---|
| A | 1 | 2308 | 2291 | 1.00x |
| B | 10 | 1894 | 1952 | 0.85x |
| C | 100 | 2336 | 2501 | 1.09x |
| D | 1,000 | 8968 | 8975 | 3.92x |
| E | 5,000 | 40542 | 40539 | 17.69x |

**观察**:
- **这是退化最严重的场景**（17.7x）
- 原因：range filter 需要逐 fragment 做 min/max pruning，再对命中的 fragment 做 row-level filter
- 5000 fragments × 10 queries × 每次 open fragment 的开销 = 40 秒

## 6. count_rows()

| 版本 | Fragment 数 | p50 (ms) | mean (ms) |
|---|---|---|---|
| A | 1 | 0.00 | 0.01 |
| B | 10 | 0.01 | 0.01 |
| C | 100 | 0.07 | 0.08 |
| D | 1,000 | 0.67 | 0.70 |
| E | 5,000 | 3.43 | 3.45 |

**观察**: count 是 metadata 操作，即使 5000 fragments 也只要 3.4ms（每个 fragment 读一个 row_count 字段求和）。

## 7. Spark 分布式读 vs Python 单进程 (A vs E 对比)

| 指标 | Python A | Python E | Python 退化 | Spark A | Spark E | Spark 退化 |
|---|---|---|---|---|---|---|
| 全表 read (sum payload) | 845ms | 8463ms | **10.0x** | 7377ms | 6444ms | 0.87x |
| Range filter (1M rows) | 2291ms | 40539ms | **17.7x** | 1744ms | 1945ms | 1.11x |

**核心发现**: Spark 的并行度完全掩盖了小 fragment 问题。**生产如果用 Spark 读**，小 fragment 影响可以忽略。**生产如果用 Python/Arrow 单进程读**，小 fragment 是致命的。

## Compaction 的收益

从 E (5000 frags) compact 到 A (1 frag) 的读性能改善：

| 指标 | E 之前 | A 之后 | 改善倍数 |
|---|---|---|---|
| Full scan | 8463 ms | 845 ms | **10x** |
| Col scan | 2126 ms | 142 ms | **15x** |
| Range query | 40539 ms | 2291 ms | **17.7x** |
| Open | 130 ms | 83 ms | 1.6x |
| Point query | 968 ms | 944 ms | 1.0x |

**Compaction 成本**: 36 分钟的数据积累只需要 ~1 分钟 compact。

**ROI 结论**:
- 如果读取用 Python/Arrow 单进程 → **每次高频 commit 后都值得 compact**
- 如果读取用 Spark → compaction 主要价值在**减少 manifest 数**（对 write 时的 `Dataset.open` 有帮助），对读本身影响不大
- 复杂的 Flink streaming write 场景，compaction 还能减少 commit 冲突（见之前的 Flink 压测）

## 性能退化倍数汇总表

| 版本 | Fragment 数 | open | full-scan | col-scan | point | range | count |
|---|---|---|---|---|---|---|---|
| A | 1 | 1.00x | 1.00x | 1.00x | 1.00x | 1.00x | 1.00x |
| B | 10 | 0.92x | 0.94x | 1.06x | 0.52x | 0.85x | 1.21x |
| C | 100 | 0.98x | 0.94x | 1.16x | 0.51x | 1.09x | 6.99x |
| D | 1,000 | 1.42x | 2.25x | 3.12x | 0.76x | 3.92x | 60.77x |
| E | 5,000 | 1.57x | 10.01x | 14.95x | 1.03x | 17.69x | 301.33x |


## 给用户的建议

### 根据读取模式的 compaction 策略

| 你的读取方式 | 推荐 fragment 策略 |
|---|---|
| **Python/Arrow 单进程全表扫描/分析** | fragment ≤ 100，超过要 compact |
| **Python 点查/少量行** | fragment 数不敏感，不急着 compact |
| **Spark/分布式并行读** | fragment 数几乎不影响读性能，但 manifest version 多了会拖慢 `open` |
| **LanceDB vector search** | 类似 Spark（内部并行），但 index build 会慢 |
| **Flink streaming 追加** | **必须** compact，否则 write 本身会变慢（commit 放大） |

### 推荐的 compaction 触发条件

1. **fragment 数 > 1000** → 立刻 compact（range query 退化 4x）
2. **fragment 数 > 100 且读以 Python 为主** → 考虑 compact
3. **manifest version > 10000** → compact + `cleanup_old_versions`（GC）
4. **单个 fragment 行数 < 10000** → 一般意味着 commit 过于频繁，源头处理 (加大 batch_size)

### 参数建议

```python
ds.optimize.compact_files(
    target_rows_per_fragment=1_000_000,     # 1M rows per fragment (对应 B 级别)
    max_rows_per_group=1024,
    materialize_deletions_threshold=0.1,
)
```

读取侧：
```python
# 如果被迫读小文件 dataset，开 worker pool 并行读 fragment
from concurrent.futures import ThreadPoolExecutor
ds = lance.dataset(path)
frags = ds.get_fragments()
def read_frag(f): return f.to_table()
with ThreadPoolExecutor(max_workers=16) as ex:
    tables = list(ex.map(read_frag, frags))
```

## 图表

见 `results/performance_plot.png`（6 个子图：fragment 数 vs 每种读操作延迟，log-log 轴）。

## 原始数据位置

`/home/hadoop/lance-read-bench/results/`:
- `read_A.json` ~ `read_E.json` — 5 个版本的 Python 读测试完整结果
- `spark_A.json`, `spark_E.json` — Spark count/agg
- `spark_full_A.json`, `spark_full_E.json` — Spark 真正读取数据
- `compact_plan.json` — compaction 执行记录
- `E_dataset_info.json` — E 数据集构建统计
- `build_E.log` — 36 分钟构建过程日志
- `performance_plot.png` — 可视化
