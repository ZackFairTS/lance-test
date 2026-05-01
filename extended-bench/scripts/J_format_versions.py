import argparse
import gc
import json
import os
import shutil
import statistics
import tempfile
import time

import lance
import numpy as np
import pyarrow as pa


def build_mixed_table(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    return pa.table({
        "id_seq": pa.array(range(n_rows), type=pa.int64()),
        "id_rand": pa.array(rng.integers(0, n_rows * 10, n_rows, dtype=np.int64)),
        "score": pa.array(rng.standard_normal(n_rows).astype(np.float32)),
        "price": pa.array(rng.uniform(0, 1000, n_rows)),
        "category": pa.array(rng.choice([f"C{i}" for i in range(20)], n_rows)),
        "tag": pa.array(rng.choice(["a", "b", "c"], n_rows)),
        "timestamp": pa.array(rng.integers(0, 10_000_000, n_rows, dtype=np.int64)),
        "vector": pa.FixedSizeListArray.from_arrays(
            pa.array(rng.standard_normal(n_rows * 128).astype(np.float32)),
            list_size=128,
        ),
    })


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def timed(fn, warmup=2, rounds=5):
    for _ in range(warmup):
        fn()
        gc.collect()
    runs = []
    for _ in range(rounds):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        runs.append(time.perf_counter() - t0)
    return {
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms": statistics.mean(runs) * 1000,
        "min_ms": min(runs) * 1000,
        "max_ms": max(runs) * 1000,
        "stdev_ms": statistics.stdev(runs) * 1000 if len(runs) > 1 else 0.0,
    }


def full_scan(path):
    return lance.dataset(path).to_table()


def col_scan(path, cols):
    return lance.dataset(path).to_table(columns=cols)


def point_query(path, n):
    ds = lance.dataset(path)
    rng = np.random.default_rng(7)
    indices = sorted(rng.choice(n, size=1000, replace=False).tolist())
    return ds.take(indices)


def range_query(path, n):
    ds = lance.dataset(path)
    return ds.to_table(filter=f"id_seq >= {n // 4} AND id_seq < {n // 4 + n // 10}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=1_000_000)
    ap.add_argument("--work-dir", default=tempfile.mkdtemp(prefix="j_fmt_"))
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/J_format_versions.json")
    args = ap.parse_args()

    print(f"Building {args.n_rows}-row mixed-schema table")
    tbl = build_mixed_table(args.n_rows)

    results = {}
    versions = ["2.0", "2.1"]
    for v in versions:
        uri = os.path.join(args.work_dir, f"mixed_v{v}.lance")
        if os.path.exists(uri):
            shutil.rmtree(uri)
        print(f"\n=== Lance v{v}")
        t0 = time.perf_counter()
        lance.write_dataset(tbl, uri, mode="overwrite", data_storage_version=v)
        write_s = time.perf_counter() - t0
        size_mb = du(uri) / 1e6

        r = {"write_seconds": round(write_s, 3), "size_mb": round(size_mb, 2)}

        print(f"  write={write_s:.2f}s  size={size_mb:.1f} MB")

        r["full_scan"] = timed(lambda: full_scan(uri))
        print(f"  full_scan:    p50={r['full_scan']['median_ms']:7.1f} ms")

        r["col_scan_score"] = timed(lambda: col_scan(uri, ["score"]))
        print(f"  col_scan:     p50={r['col_scan_score']['median_ms']:7.1f} ms")

        r["point_query"] = timed(lambda: point_query(uri, args.n_rows))
        print(f"  point_query:  p50={r['point_query']['median_ms']:7.1f} ms")

        r["range_query"] = timed(lambda: range_query(uri, args.n_rows))
        print(f"  range_query:  p50={r['range_query']['median_ms']:7.1f} ms")

        results[f"v{v}"] = r

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "pylance_version": lance.__version__,
            "n_rows": args.n_rows,
            "results": results,
        }, f, indent=2)

    print(f"\nSaved to {args.out}")
    if "v2.0" in results and "v2.1" in results:
        print("\n=== v2.1 / v2.0 comparison:")
        for op in ["write_seconds", "size_mb"]:
            v0 = results["v2.0"][op]
            v1 = results["v2.1"][op]
            print(f"  {op:18s}: v2.0={v0}  v2.1={v1}  ratio={v1/v0:.2f}x")
        for op in ["full_scan", "col_scan_score", "point_query", "range_query"]:
            v0 = results["v2.0"][op]["median_ms"]
            v1 = results["v2.1"][op]["median_ms"]
            print(f"  {op:18s}: v2.0={v0:7.1f}ms  v2.1={v1:7.1f}ms  ratio={v1/v0:.2f}x")


if __name__ == "__main__":
    main()
