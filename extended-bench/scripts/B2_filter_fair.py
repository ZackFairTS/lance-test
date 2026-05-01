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
import pyarrow.compute as pc
import pyarrow.feather as feather
import pyarrow.parquet as pq


def build_nyctaxi_like(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    return pa.table({
        "pickup_minute":   rng.integers(0, 60, n_rows, dtype=np.int8),
        "fare_amount":     rng.uniform(2.5, 150.0, n_rows).astype(np.float64),
        "trip_distance":   rng.exponential(3.0, n_rows).astype(np.float64),
        "passenger_count": rng.integers(1, 6, n_rows, dtype=np.int8),
        "pickup_hour":     rng.integers(0, 24, n_rows, dtype=np.int8),
        "vendor_id":       pa.array(rng.choice(["VTS", "CMT", "DDS"], n_rows)),
    })


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def timed(fn, warmup=3, rounds=10):
    for _ in range(warmup):
        fn()
        gc.collect()
    runs = []
    for _ in range(rounds):
        gc.collect()
        t0 = time.perf_counter()
        out = fn()
        runs.append(time.perf_counter() - t0)
    return {
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms": statistics.mean(runs) * 1000,
        "min_ms": min(runs) * 1000,
        "max_ms": max(runs) * 1000,
        "stdev_ms": statistics.stdev(runs) * 1000 if len(runs) > 1 else 0.0,
        "rows_returned": out.num_rows,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=3_000_000)
    ap.add_argument("--out-root", default=None)
    ap.add_argument("--result-json", default="/home/hadoop/lance-extended-bench/results/B_filter_with_index.json")
    args = ap.parse_args()
    if args.out_root is None:
        args.out_root = tempfile.mkdtemp(prefix="b_fair_")

    print(f"Building {args.n_rows}-row NYC-Taxi-like table...")
    tbl = build_nyctaxi_like(args.n_rows)
    os.makedirs(args.out_root, exist_ok=True)

    results = {}
    filter_expr = "pickup_minute = 30"
    read_columns = ["pickup_minute", "fare_amount", "trip_distance"]

    print("\n=== Parquet variants (baseline, RG stats always on)")
    for variant, compression in [("parquet_snappy", "snappy"), ("parquet_zstd", "zstd")]:
        path = os.path.join(args.out_root, f"{variant}.parquet")
        if os.path.exists(path):
            os.remove(path)
        t0 = time.perf_counter()
        pq.write_table(
            tbl, path,
            compression=compression,
            row_group_size=1_048_576,
            data_page_size=1024 * 1024,
            write_statistics=True,
            use_dictionary=True,
            data_page_version="2.0",
        )
        write_s = time.perf_counter() - t0
        size_mb = du(path) / 1e6

        def q_parquet():
            return pq.read_table(
                path, columns=read_columns,
                filters=[("pickup_minute", "=", 30)], use_threads=True,
            )

        r = timed(q_parquet)
        r.update(write_seconds=round(write_s, 3), size_mb=round(size_mb, 2),
                 build_index_seconds=0, index_overhead_mb=0)
        results[variant] = r
        print(f"  {variant:25s}  write={write_s:.2f}s  size={size_mb:.1f}MB  "
              f"p50={r['median_ms']:.1f}ms  rows={r['rows_returned']}")

    print("\n=== Lance v2.1 variants (with scalar indexes)")
    lance_configs = [
        ("lance_no_index", None, None),
        ("lance_zonemap", "pickup_minute", "ZONEMAP"),
        ("lance_btree", "pickup_minute", "BTREE"),
        ("lance_bitmap", "pickup_minute", "BITMAP"),
        ("lance_bloomfilter", "pickup_minute", "BLOOMFILTER"),
    ]

    for variant, col, index_type in lance_configs:
        path = os.path.join(args.out_root, f"{variant}.lance")
        if os.path.exists(path):
            shutil.rmtree(path)

        t0 = time.perf_counter()
        lance.write_dataset(tbl, path, mode="overwrite", data_storage_version="2.1")
        write_s = time.perf_counter() - t0
        size_before_mb = du(path) / 1e6

        build_index_s = 0.0
        if index_type:
            ds_for_build = lance.dataset(path)
            t0 = time.perf_counter()
            try:
                ds_for_build.create_scalar_index(col, index_type=index_type, replace=True)
                build_index_s = time.perf_counter() - t0
            except Exception as e:
                print(f"  {variant}: index build FAILED: {e}")
                results[variant] = {"error": str(e)[:300]}
                continue

        size_after_mb = du(path) / 1e6
        index_overhead_mb = size_after_mb - size_before_mb

        ds = lance.dataset(path)

        def q_lance():
            return ds.to_table(columns=read_columns, filter=filter_expr)

        try:
            r = timed(q_lance)
        except Exception as e:
            print(f"  {variant}: query FAILED: {e}")
            results[variant] = {"error_query": str(e)[:300],
                                "build_index_seconds": round(build_index_s, 3)}
            continue

        r.update(
            write_seconds=round(write_s, 3),
            size_mb=round(size_after_mb, 2),
            build_index_seconds=round(build_index_s, 3),
            index_overhead_mb=round(index_overhead_mb, 2),
        )
        results[variant] = r

        status = f"[{index_type}]" if index_type else "[no index]"
        print(f"  {variant:25s}  {status:14s}  write={write_s:.2f}s  "
              f"size={size_after_mb:.1f}MB (+{index_overhead_mb:.2f})  "
              f"build_idx={build_index_s:.2f}s  "
              f"p50={r['median_ms']:.1f}ms  rows={r['rows_returned']}")

    os.makedirs(os.path.dirname(args.result_json), exist_ok=True)
    with open(args.result_json, "w") as f:
        json.dump({
            "lance_version": lance.__version__,
            "pyarrow_version": pa.__version__,
            "n_rows": args.n_rows,
            "filter": filter_expr,
            "columns_read": read_columns,
            "results": results,
        }, f, indent=2)
    print(f"\nSaved to {args.result_json}")

    baseline = results.get("parquet_snappy", {}).get("median_ms")
    baseline_rows = results.get("parquet_snappy", {}).get("rows_returned")
    if baseline:
        print(f"\n=== Relative to parquet_snappy p50 = {baseline:.1f} ms")
        for name, r in results.items():
            if isinstance(r, dict) and "median_ms" in r:
                ratio = r["median_ms"] / baseline
                marker = "⚡" if ratio < 1.0 else "  "
                print(f"  {marker} {name:25s}  p50={r['median_ms']:7.1f}ms  "
                      f"({ratio:5.2f}x)  idx_build={r.get('build_index_seconds', 0):.2f}s  "
                      f"idx_mb=+{r.get('index_overhead_mb', 0):.2f}")

    print("\n=== Correctness check (rows returned must match baseline)")
    mismatched = []
    for name, r in results.items():
        if isinstance(r, dict) and "rows_returned" in r:
            if baseline_rows is not None and r["rows_returned"] != baseline_rows:
                mismatched.append((name, r["rows_returned"]))
    if mismatched:
        print(f"  WARN: {len(mismatched)} variants returned different row counts (baseline={baseline_rows}):")
        for name, count in mismatched:
            print(f"    {name}: {count}")
    else:
        print(f"  OK: all variants returned {baseline_rows} rows")


if __name__ == "__main__":
    main()
