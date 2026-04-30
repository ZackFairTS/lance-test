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


def write_lance(tbl, path, version):
    if os.path.exists(path):
        shutil.rmtree(path)
    lance.write_dataset(tbl, path, mode="overwrite", data_storage_version=version)


def write_parquet(tbl, path, compression):
    if os.path.exists(path):
        os.remove(path)
    pq.write_table(
        tbl, path,
        compression=compression,
        row_group_size=1_048_576,
        data_page_size=1024 * 1024,
        write_statistics=True,
        use_dictionary=True,
        data_page_version="2.0",
    )


def write_feather(tbl, path):
    if os.path.exists(path):
        os.remove(path)
    feather.write_feather(tbl, path, compression="uncompressed", version=2)


def q_lance_filter(path):
    ds = lance.dataset(path)
    return ds.to_table(
        columns=["pickup_minute", "fare_amount", "trip_distance"],
        filter="pickup_minute = 30",
    )


def q_parquet_filter(path):
    return pq.read_table(
        path,
        columns=["pickup_minute", "fare_amount", "trip_distance"],
        filters=[("pickup_minute", "=", 30)],
        use_threads=True,
    )


def q_feather_filter(path):
    t = feather.read_table(path, columns=["pickup_minute", "fare_amount", "trip_distance"])
    return t.filter(pc.equal(t["pickup_minute"], 30))


def timed(fn, warmup=2, rounds=5):
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
        "runs_ms": [round(r * 1000, 2) for r in runs],
        "rows_returned": out.num_rows,
    }


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=3_000_000)
    ap.add_argument("--out-root", default=None)
    ap.add_argument("--result-json", default="/home/hadoop/lance-extended-bench/results/B_filter_vs_parquet.json")
    args = ap.parse_args()
    if args.out_root is None:
        args.out_root = tempfile.mkdtemp(prefix="b738_")

    print(f"Building {args.n_rows}-row NYC-Taxi-like table...")
    tbl = build_nyctaxi_like(args.n_rows)
    os.makedirs(args.out_root, exist_ok=True)

    variants = []
    variants.append(("lance_v2_0", os.path.join(args.out_root, "nyctaxi.v20.lance"),
                     lambda p: write_lance(tbl, p, "2.0"), q_lance_filter))
    variants.append(("lance_v2_1", os.path.join(args.out_root, "nyctaxi.v21.lance"),
                     lambda p: write_lance(tbl, p, "2.1"), q_lance_filter))
    variants.append(("parquet_snappy", os.path.join(args.out_root, "nyctaxi.snappy.parquet"),
                     lambda p: write_parquet(tbl, p, "snappy"), q_parquet_filter))
    variants.append(("parquet_zstd", os.path.join(args.out_root, "nyctaxi.zstd.parquet"),
                     lambda p: write_parquet(tbl, p, "zstd"), q_parquet_filter))
    variants.append(("feather_v2", os.path.join(args.out_root, "nyctaxi.feather"),
                     lambda p: write_feather(tbl, p), q_feather_filter))

    results = {}
    for name, path, writer, reader in variants:
        print(f"\n=== {name}")
        t_write = time.perf_counter()
        writer(path)
        write_elapsed = time.perf_counter() - t_write
        size_mb = du(path) / 1e6

        r = timed(lambda: reader(path))
        if r["rows_returned"] == 0:
            print(f"  WARNING: {name} returned 0 rows — reader may be broken")
        r["write_seconds"] = round(write_elapsed, 3)
        r["size_mb"] = round(size_mb, 2)
        results[name] = r
        print(f"  write={write_elapsed:.2f}s  size={size_mb:.1f} MB  "
              f"filter_p50={r['median_ms']:.1f}ms  rows_ret={r['rows_returned']}")

    os.makedirs(os.path.dirname(args.result_json), exist_ok=True)
    with open(args.result_json, "w") as f:
        json.dump({
            "pylance_version": lance.__version__,
            "pyarrow_version": pa.__version__,
            "n_rows": args.n_rows,
            "results": results,
        }, f, indent=2)
    print(f"\nSaved to {args.result_json}")

    baseline = results.get("parquet_snappy", {}).get("median_ms")
    if baseline is not None and baseline > 0:
        print(f"\n=== Relative to parquet_snappy p50 = {baseline:.1f} ms")
        for name, r in results.items():
            ratio = r["median_ms"] / baseline
            print(f"  {name:18s}  p50={r['median_ms']:7.1f} ms  ({ratio:5.2f}x)")


if __name__ == "__main__":
    main()
