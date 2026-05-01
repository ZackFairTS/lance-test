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
import pyarrow.parquet as pq


def build_nyctaxi_with_rare_column(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    rare_values = [f"V{i}" for i in range(1000)]
    return pa.table({
        "pickup_minute":   rng.integers(0, 60, n_rows, dtype=np.int8),
        "rare_1k":         pa.array(rng.choice(rare_values, n_rows), type=pa.string()),
        "rare_100":        pa.array(rng.choice(rare_values[:100], n_rows), type=pa.string()),
        "fare_amount":     rng.uniform(2.5, 150.0, n_rows).astype(np.float64),
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


def make_queries_for_selectivities(n_rows):
    queries = []
    for target_sel, predicate, col, value in [
        (0.0001, "pickup_minute = 30 AND rare_1k = 'V0'", "rare_1k", "V0"),
        (0.001,  "rare_1k = 'V0'", "rare_1k", "V0"),
        (0.01,   "rare_100 = 'V0'", "rare_100", "V0"),
        (0.0167, "pickup_minute = 30", "pickup_minute", 30),
        (0.10,   "pickup_minute < 6", "pickup_minute", 6),
        (0.50,   "pickup_minute < 30", "pickup_minute", 30),
    ]:
        queries.append({
            "selectivity_target": target_sel,
            "predicate": predicate,
            "indexed_column": col,
            "indexed_value": value,
        })
    return queries


def bench_variant(tbl, path_root, variant_name, index_cols_types, queries):
    path = os.path.join(path_root, f"{variant_name}.lance")
    if os.path.exists(path):
        shutil.rmtree(path)
    lance.write_dataset(tbl, path, mode="overwrite", data_storage_version="2.1")
    ds = lance.dataset(path)

    total_idx_build_s = 0.0
    for col, idx_type in index_cols_types:
        t0 = time.perf_counter()
        try:
            ds.create_scalar_index(col, index_type=idx_type, replace=True)
            total_idx_build_s += time.perf_counter() - t0
        except Exception as e:
            print(f"  {variant_name}: failed to build {idx_type} on {col}: {e}")

    size_mb = du(path) / 1e6
    ds = lance.dataset(path)

    per_query = []
    for q in queries:
        def run():
            return ds.to_table(
                columns=["pickup_minute", "fare_amount"],
                filter=q["predicate"],
            )
        try:
            r = timed(run)
            r.update(**q)
            per_query.append(r)
        except Exception as e:
            per_query.append({**q, "error": str(e)[:200]})
    return {
        "size_mb": round(size_mb, 2),
        "build_index_seconds": round(total_idx_build_s, 3),
        "queries": per_query,
    }


def bench_parquet(tbl, path_root, compression, queries):
    path = os.path.join(path_root, f"parquet_{compression}.parquet")
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
    size_mb = du(path) / 1e6

    per_query = []
    for q in queries:
        if isinstance(q["indexed_value"], int):
            filters = [(q["indexed_column"], "=", q["indexed_value"])]
        else:
            filters = [(q["indexed_column"], "=", q["indexed_value"])]

        def run(filters=filters, q=q):
            if " AND " in q["predicate"]:
                tbl = pq.read_table(path, columns=["pickup_minute", "fare_amount", "rare_1k"],
                                    filters=[("pickup_minute", "=", 30),
                                             ("rare_1k", "=", "V0")], use_threads=True)
                return tbl
            if "<" in q["predicate"]:
                col, val = q["indexed_column"], q["indexed_value"]
                return pq.read_table(path, columns=["pickup_minute", "fare_amount"],
                                     filters=[(col, "<", val)], use_threads=True)
            return pq.read_table(path, columns=["pickup_minute", "fare_amount"],
                                 filters=filters, use_threads=True)

        r = timed(run)
        r.update(**q)
        per_query.append(r)

    return {"size_mb": round(size_mb, 2), "build_index_seconds": 0.0, "queries": per_query}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=3_000_000)
    ap.add_argument("--out-root", default=None)
    ap.add_argument("--result-json", default="/home/hadoop/lance-extended-bench/results/B3_selectivity_sweep.json")
    args = ap.parse_args()
    if args.out_root is None:
        args.out_root = tempfile.mkdtemp(prefix="b3_sel_")

    print(f"Building {args.n_rows}-row table with pickup_minute + rare cols...")
    tbl = build_nyctaxi_with_rare_column(args.n_rows)

    queries = make_queries_for_selectivities(args.n_rows)
    print(f"Queries: {len(queries)} selectivity levels")

    results = {}

    print("\n=== Parquet (with RG stats)")
    results["parquet_snappy"] = bench_parquet(tbl, args.out_root, "snappy", queries)

    lance_variants = [
        ("lance_no_index", []),
        ("lance_bitmap", [("pickup_minute", "BITMAP"), ("rare_1k", "BITMAP"), ("rare_100", "BITMAP")]),
        ("lance_btree", [("pickup_minute", "BTREE"), ("rare_1k", "BTREE"), ("rare_100", "BTREE")]),
    ]

    for name, idx_cols in lance_variants:
        print(f"\n=== {name} (indexes: {idx_cols})")
        results[name] = bench_variant(tbl, args.out_root, name, idx_cols, queries)

    os.makedirs(os.path.dirname(args.result_json), exist_ok=True)
    with open(args.result_json, "w") as f:
        json.dump({
            "lance_version": lance.__version__,
            "pyarrow_version": pa.__version__,
            "n_rows": args.n_rows,
            "results": results,
        }, f, indent=2)
    print(f"\nSaved to {args.result_json}")

    print("\n=== Selectivity sweep summary")
    header = f"{'Selectivity':<10}" + "".join(f"{name[:18]:>20}" for name in results.keys())
    print(header)
    for qi in range(len(queries)):
        q = queries[qi]
        row = f"{q['selectivity_target']:<10.4f}"
        for name, r in results.items():
            if qi < len(r["queries"]):
                qr = r["queries"][qi]
                if "median_ms" in qr:
                    row += f"  {qr['median_ms']:>7.2f}ms ({qr['rows_returned']:>7})"
                else:
                    row += f"  {'ERROR':>18}"
        print(row)


if __name__ == "__main__":
    main()
