import json, sys, os, glob

results_dir = "/home/hadoop/lance-read-bench/results"
out_md = "/home/hadoop/lance-read-bench/REPORT.md"

files = sorted(glob.glob(os.path.join(results_dir, "read_*.json")))
python_data = {}
for f in files:
    with open(f) as fp:
        r = json.load(fp)
    if r["tag"] in ["A", "B", "C", "D", "E"]:
        python_data[r["tag"]] = r

spark_data = {}
for f in sorted(glob.glob(os.path.join(results_dir, "spark_full_*.json"))):
    with open(f) as fp:
        r = json.load(fp)
    spark_data[r["tag"]] = r

order = ["A", "B", "C", "D", "E"]
py_rows = [(t, python_data[t]) for t in order if t in python_data]

def fmt(v, p=0):
    if v is None: return "-"
    if p == 0: return f"{v:.0f}"
    return f"{v:.{p}f}"

with open("/home/hadoop/lance-read-bench/results/compact_plan.json") as f:
    cp = json.load(f)

with open("/home/hadoop/lance-read-bench/results/E_dataset_info.json") as f:
    ei = json.load(f)

md = [f"""# Lance 小文件对读性能影响 - 压测报告

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
|---|---|---|---|---|"""]

for tag, r in py_rows:
    rpf = r["total_rows"] // r["fragments"]
    md.append(f"| {tag} | {r['total_rows']:,} | {r['fragments']:,} | {rpf:,} | {r['version_actual']} |")

md.append(f"""
## 数据生成信息

- **E** 生成方式: {ei['target_fragments']:,} 次 append，每次 2000 行 → **耗时 {ei['build_elapsed_s']/60:.1f} 分钟**
  - Append 速率从 **3.5/s 退化到 2.4/s**（31% 下降），因为 `write_dataset` 每次需要打开 dataset 读 manifest
- **D/C/B/A** 通过 `compact_files(target_rows_per_fragment=...)` 从 E 逐步合并生成
  - D: 5000 → 1000 fragments, 37.2s
  - C: 1000 → 100 fragments, 12.6s
  - B: 100 → 10 fragments, 5.8s
  - A: 10 → 1 fragments, 17.5s
  - **总 compaction 时间 ~1 分钟**（对比 36 分钟的 build 时间，说明 compact 不贵）

## 1. Dataset.open() 耗时（10 次采样）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | p99 (ms) |
|---|---|---|---|---|""")

for tag, r in py_rows:
    o = r["open"]
    md.append(f"| {tag} | {r['fragments']:,} | {fmt(o['p50_ms'])} | {fmt(o['mean_ms'])} | {fmt(o['p99_ms'])} |")

md.append("""
**观察**: `Dataset.open()` 退化有限。即使 5000 fragments 也只是 134ms（比 1 fragment 慢 1.7x），因为 Lance 只读最新 manifest 文件，不读每个 fragment 的 metadata。

## 2. 全表扫描吞吐（Python 单进程）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | 吞吐 (MB/s) | 相对 A |
|---|---|---|---|---|---|""")

a_fs = py_rows[0][1]["full_scan"]["mean_ms"]
for tag, r in py_rows:
    f = r["full_scan"]
    slowdown = f["mean_ms"] / a_fs
    md.append(f"| {tag} | {r['fragments']:,} | {fmt(f['p50_ms'])} | {fmt(f['mean_ms'])} | {f['throughput_mbps']:.1f} | {slowdown:.2f}x |")

md.append("""
**观察**:
- A/B/C (1-100 fragments) 都是 ~1200 MB/s，几乎一样
- D (1000 fragments) 降到 525 MB/s（2.3x 慢）
- E (5000 fragments) 降到 **118 MB/s（10x 慢）**
- **拐点大致在 100-1000 fragment 之间**

## 3. 单列扫描（id）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | 吞吐 (MB/s) | 相对 A |
|---|---|---|---|---|---|""")

a_cs = py_rows[0][1]["col_scan"]["mean_ms"]
for tag, r in py_rows:
    f = r["col_scan"]
    slowdown = f["mean_ms"] / a_cs
    md.append(f"| {tag} | {r['fragments']:,} | {fmt(f['p50_ms'])} | {fmt(f['mean_ms'])} | {f['throughput_mbps']:.1f} | {slowdown:.2f}x |")

md.append("""
**观察**: 单列扫描**更敏感**于小文件（E 达到 15x 退化），因为列存的优势被"每个 fragment 都要单独打开列读取"消耗掉了。

## 4. 点查延迟 (`ds.take([1000 random indices])`)

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | p99 (ms) | 相对 A |
|---|---|---|---|---|---|""")

a_p = py_rows[0][1]["point_query"]["mean_ms"]
for tag, r in py_rows:
    f = r["point_query"]
    slowdown = f["mean_ms"] / a_p
    md.append(f"| {tag} | {r['fragments']:,} | {fmt(f['p50_ms'])} | {fmt(f['mean_ms'])} | {fmt(f['p99_ms'])} | {slowdown:.2f}x |")

md.append("""
**观察**: 点查**反而在小文件场景下没退化**（E 和 A 都是 ~950ms）。B/C 甚至比 A 快一倍（U 形曲线，可能与 prefetch/cache 有关）。这是因为 point query 命中的数据量小，不敏感于 fragment 分布。

## 5. 范围查询（10 ranges × 10K rows, filter pushdown）

| 版本 | Fragment 数 | p50 (ms) | mean (ms) | 相对 A |
|---|---|---|---|---|""")

a_r = py_rows[0][1]["range_query"]["mean_ms"]
for tag, r in py_rows:
    f = r["range_query"]
    slowdown = f["mean_ms"] / a_r
    md.append(f"| {tag} | {r['fragments']:,} | {fmt(f['p50_ms'])} | {fmt(f['mean_ms'])} | {slowdown:.2f}x |")

md.append("""
**观察**:
- **这是退化最严重的场景**（17.7x）
- 原因：range filter 需要逐 fragment 做 min/max pruning，再对命中的 fragment 做 row-level filter
- 5000 fragments × 10 queries × 每次 open fragment 的开销 = 40 秒

## 6. count_rows()

| 版本 | Fragment 数 | p50 (ms) | mean (ms) |
|---|---|---|---|""")

for tag, r in py_rows:
    c = r["count_rows"]
    md.append(f"| {tag} | {r['fragments']:,} | {fmt(c['p50_ms'], 2)} | {fmt(c['mean_ms'], 2)} |")

md.append("""
**观察**: count 是 metadata 操作，即使 5000 fragments 也只要 3.4ms（每个 fragment 读一个 row_count 字段求和）。

## 7. Spark 分布式读 vs Python 单进程 (A vs E 对比)

| 指标 | Python A | Python E | Python 退化 | Spark A | Spark E | Spark 退化 |
|---|---|---|---|---|---|---|""")

if "A" in spark_data and "E" in spark_data:
    spa = spark_data["A"]
    spe = spark_data["E"]
    pya = python_data["A"]
    pye = python_data["E"]

    fs_a = pya["full_scan"]["mean_ms"]
    fs_e = pye["full_scan"]["mean_ms"]
    md.append(f"| 全表 read (sum payload) | {fs_a:.0f}ms | {fs_e:.0f}ms | **{fs_e/fs_a:.1f}x** | {spa['full_read']['mean_ms']:.0f}ms | {spe['full_read']['mean_ms']:.0f}ms | {spe['full_read']['mean_ms']/spa['full_read']['mean_ms']:.2f}x |")
    rq_a = pya["range_query"]["mean_ms"]
    rq_e = pye["range_query"]["mean_ms"]
    md.append(f"| Range filter (1M rows) | {rq_a:.0f}ms | {rq_e:.0f}ms | **{rq_e/rq_a:.1f}x** | {spa['range_filter']['mean_ms']:.0f}ms | {spe['range_filter']['mean_ms']:.0f}ms | {spe['range_filter']['mean_ms']/spa['range_filter']['mean_ms']:.2f}x |")

md.append("""
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
|---|---|---|---|---|---|---|---|""")

a = python_data["A"]
for tag, r in py_rows:
    row = f"| {tag} | {r['fragments']:,} "
    for op in ["open", "full_scan", "col_scan", "point_query", "range_query", "count_rows"]:
        ratio = r[op]["mean_ms"] / a[op]["mean_ms"]
        row += f"| {ratio:.2f}x "
    row += "|"
    md.append(row)

md.append("""

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
""")

with open(out_md, "w") as f:
    f.write("\n".join(md))
print(f"Report written: {out_md}, {os.path.getsize(out_md)} bytes")
