import sys
import os
import time
import json
import statistics
import random
import lance
import pyarrow as pa
import pyarrow.compute as pc

S3_PATH = sys.argv[1]
VERSION = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] != "latest" else None
TAG = sys.argv[3] if len(sys.argv) > 3 else "unknown"
OUT_JSON = sys.argv[4] if len(sys.argv) > 4 else f"/home/hadoop/lance-read-bench/results/read_{TAG}.json"

REPEATS = 5
POINT_QUERY_N = 1000
RANGE_QUERY_N = 10
RANGE_SIZE = 10000

print(f"=== Reading {S3_PATH} @ version={VERSION} (tag={TAG})")

def time_op(fn, repeats=REPEATS):
    durs = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        durs.append(time.perf_counter() - t0)
    return {
        "mean_ms": statistics.mean(durs) * 1000,
        "p50_ms": statistics.median(durs) * 1000,
        "p99_ms": sorted(durs)[min(len(durs)-1, int(len(durs)*0.99))] * 1000,
        "min_ms": min(durs) * 1000,
        "max_ms": max(durs) * 1000,
        "samples": [round(d*1000, 2) for d in durs],
    }

results = {"tag": TAG, "path": S3_PATH, "version": VERSION}

print("[1/6] Dataset.open()")
def do_open():
    return lance.dataset(S3_PATH, version=VERSION) if VERSION else lance.dataset(S3_PATH)
results["open"] = time_op(do_open, repeats=10)
ds = do_open()
results["total_rows"] = ds.count_rows()
results["fragments"] = len(ds.get_fragments())
results["version_actual"] = ds.version
print(f"  rows={results['total_rows']} fragments={results['fragments']} v={results['version_actual']}")
print(f"  open p50={results['open']['p50_ms']:.1f}ms mean={results['open']['mean_ms']:.1f}ms")

print("[2/6] Full-table scan (all columns)")
def full_scan():
    t = ds.to_table()
    return (t.num_rows, t.nbytes)
results["full_scan"] = time_op(full_scan, repeats=REPEATS)
rows, nbytes = full_scan()
results["full_scan"]["rows_read"] = rows
results["full_scan"]["bytes_read"] = nbytes
results["full_scan"]["throughput_mbps"] = (nbytes / 1e6) / (results["full_scan"]["mean_ms"] / 1000)
print(f"  p50={results['full_scan']['p50_ms']:.0f}ms throughput={results['full_scan']['throughput_mbps']:.1f} MB/s")

print("[3/6] Single-column scan (id only)")
def col_scan():
    t = ds.to_table(columns=["id"])
    return (t.num_rows, t.nbytes)
results["col_scan"] = time_op(col_scan, repeats=REPEATS)
rows, nbytes = col_scan()
results["col_scan"]["rows_read"] = rows
results["col_scan"]["bytes_read"] = nbytes
results["col_scan"]["throughput_mbps"] = (nbytes / 1e6) / (results["col_scan"]["mean_ms"] / 1000)
print(f"  p50={results['col_scan']['p50_ms']:.0f}ms throughput={results['col_scan']['throughput_mbps']:.1f} MB/s")

print(f"[4/6] Point queries ({POINT_QUERY_N} random indices × {REPEATS} repeats)")
random.seed(42)
total_rows = results["total_rows"]
def point_query():
    idxs = sorted(random.sample(range(total_rows), POINT_QUERY_N))
    t = ds.take(idxs)
    return t.num_rows
results["point_query"] = time_op(point_query, repeats=REPEATS)
results["point_query"]["n_per_call"] = POINT_QUERY_N
print(f"  p50={results['point_query']['p50_ms']:.0f}ms for {POINT_QUERY_N} takes")

print(f"[5/6] Range queries ({RANGE_QUERY_N} ranges × {RANGE_SIZE} rows each)")
def range_query():
    rows_read = 0
    for _ in range(RANGE_QUERY_N):
        start = random.randint(0, total_rows - RANGE_SIZE)
        end = start + RANGE_SIZE
        t = ds.scanner(filter=f"id >= {start} AND id < {end}").to_table()
        rows_read += t.num_rows
    return rows_read
results["range_query"] = time_op(range_query, repeats=3)
results["range_query"]["n_ranges_per_call"] = RANGE_QUERY_N
results["range_query"]["range_size"] = RANGE_SIZE
print(f"  p50={results['range_query']['p50_ms']:.0f}ms for {RANGE_QUERY_N} ranges")

print("[6/6] count_rows()")
def just_count():
    return ds.count_rows()
results["count_rows"] = time_op(just_count, repeats=10)
print(f"  p50={results['count_rows']['p50_ms']:.1f}ms")

with open(OUT_JSON, "w") as f:
    json.dump(results, f, indent=2)
print(f"\nSaved to {OUT_JSON}")
