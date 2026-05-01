import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import lance

from pyspark.sql import SparkSession


def build_nyctaxi_with_rare(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    rare_values = [f"V{i}" for i in range(1000)]
    return pa.table({
        "pickup_minute":   rng.integers(0, 60, n_rows, dtype=np.int8),
        "rare_1k":         pa.array(rng.choice(rare_values, n_rows), type=pa.string()),
        "rare_100":        pa.array(rng.choice(rare_values[:100], n_rows), type=pa.string()),
        "fare_amount":     rng.uniform(2.5, 150.0, n_rows).astype(np.float64),
    })


def build_parquet(tbl, path):
    if os.path.exists(path):
        os.remove(path)
    pq.write_table(
        tbl, path,
        compression="snappy",
        row_group_size=1_048_576,
        data_page_size=1024 * 1024,
        write_statistics=True,
        use_dictionary=True,
        data_page_version="2.0",
    )


def build_lance(tbl, path, indexes=None):
    if os.path.exists(path):
        shutil.rmtree(path)
    lance.write_dataset(tbl, path, mode="overwrite", data_storage_version="2.1")
    if indexes:
        ds = lance.dataset(path)
        for col, idx_type in indexes:
            ds.create_scalar_index(col, index_type=idx_type, replace=True)


def make_spark_session(jars_dir):
    builder = (
        SparkSession.builder
        .appName("lance-vs-parquet-fair")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "false")
    )
    return builder.getOrCreate()


def register_sources(spark, parquet_path, lance_paths):
    spark.read.parquet("file://" + parquet_path).createOrReplaceTempView("parquet_view")
    for name, path in lance_paths.items():
        df = spark.read.format("lance").option("path", "file://" + path).load()
        df.createOrReplaceTempView(name)


def bench_spark(spark, sql, warmup=3, rounds=7):
    for _ in range(warmup):
        spark.sql(sql).write.format("noop").mode("overwrite").save()
    runs = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        spark.sql(sql).write.format("noop").mode("overwrite").save()
        runs.append(time.perf_counter() - t0)
    return {
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms": statistics.mean(runs) * 1000,
        "min_ms": min(runs) * 1000,
        "max_ms": max(runs) * 1000,
        "stdev_ms": statistics.stdev(runs) * 1000 if len(runs) > 1 else 0.0,
    }
    for _ in range(warmup):
        spark.sql(sql).write.format("noop").mode("overwrite").save()
    runs = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        spark.sql(sql).write.format("noop").mode("overwrite").save()
        runs.append(time.perf_counter() - t0)
    return {
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms": statistics.mean(runs) * 1000,
        "min_ms": min(runs) * 1000,
        "max_ms": max(runs) * 1000,
        "stdev_ms": statistics.stdev(runs) * 1000 if len(runs) > 1 else 0.0,
    }


def explain_plan(spark, sql):
    try:
        plan = spark.sql(sql)
        return plan._jdf.queryExecution().toString()
    except Exception as e:
        return f"EXPLAIN failed: {e}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=3_000_000)
    ap.add_argument("--work-dir", default=None)
    ap.add_argument("--jars-dir", default="/home/hadoop/lance-read-bench/spark-libs")
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/B4_spark_neutral.json")
    args = ap.parse_args()
    if args.work_dir is None:
        args.work_dir = tempfile.mkdtemp(prefix="b4_spark_")

    os.makedirs(args.work_dir, exist_ok=True)
    print(f"Building {args.n_rows}-row table...")
    tbl = build_nyctaxi_with_rare(args.n_rows)

    parquet_path = os.path.join(args.work_dir, "data.parquet")
    build_parquet(tbl, parquet_path)
    print(f"  parquet: {os.path.getsize(parquet_path)/1e6:.1f} MB")

    lance_paths = {}
    for name, indexes in [
        ("lance_no_idx", None),
        ("lance_bitmap", [("pickup_minute", "BITMAP"), ("rare_1k", "BITMAP"), ("rare_100", "BITMAP")]),
        ("lance_btree", [("pickup_minute", "BTREE"), ("rare_1k", "BTREE"), ("rare_100", "BTREE")]),
    ]:
        p = os.path.join(args.work_dir, f"{name}.lance")
        build_lance(tbl, p, indexes=indexes)
        lance_paths[name] = p
        print(f"  {name}: built")

    print("\nStarting Spark session...")
    spark = make_spark_session(args.jars_dir)
    spark.sparkContext.setLogLevel("WARN")
    print("  session ready")

    register_sources(spark, parquet_path, lance_paths)

    queries = [
        {
            "selectivity_target": 0.0001,
            "label": "pickup_minute=30 AND rare_1k='V0'",
            "template": "SELECT pickup_minute, fare_amount FROM {src} WHERE pickup_minute = 30 AND rare_1k = 'V0'",
        },
        {
            "selectivity_target": 0.001,
            "label": "rare_1k='V0'",
            "template": "SELECT pickup_minute, fare_amount FROM {src} WHERE rare_1k = 'V0'",
        },
        {
            "selectivity_target": 0.01,
            "label": "rare_100='V0'",
            "template": "SELECT pickup_minute, fare_amount FROM {src} WHERE rare_100 = 'V0'",
        },
        {
            "selectivity_target": 0.0167,
            "label": "pickup_minute=30",
            "template": "SELECT pickup_minute, fare_amount FROM {src} WHERE pickup_minute = 30",
        },
        {
            "selectivity_target": 0.10,
            "label": "pickup_minute<6",
            "template": "SELECT pickup_minute, fare_amount FROM {src} WHERE pickup_minute < 6",
        },
        {
            "selectivity_target": 0.50,
            "label": "pickup_minute<30",
            "template": "SELECT pickup_minute, fare_amount FROM {src} WHERE pickup_minute < 30",
        },
    ]

    sources = ["parquet_view"] + list(lance_paths.keys())

    results = []
    plans = {}
    for q in queries:
        for src in sources:
            sql = q["template"].format(src=src)
            try:
                r = bench_spark(spark, sql)
                r.update(source=src, selectivity_target=q["selectivity_target"],
                         label=q["label"])
                results.append(r)
                print(f"  sel={q['selectivity_target']:.4f}  {src:18s}  "
                      f"p50={r['median_ms']:7.1f}ms")
            except Exception as e:
                print(f"  sel={q['selectivity_target']:.4f}  {src}  ERROR: {str(e)[:100]}")
                results.append({"source": src, "selectivity_target": q["selectivity_target"],
                                "label": q["label"], "error": str(e)[:300]})

        for src in sources:
            sample_sql = q["template"].format(src=src)
            plans[f"{src}_sel{q['selectivity_target']}"] = explain_plan(spark, sample_sql)[:2000]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "lance_version": lance.__version__,
            "spark_version": spark.version,
            "n_rows": args.n_rows,
            "engine": "spark-sql",
            "results": results,
            "sample_plans": plans,
        }, f, indent=2)
    print(f"\nSaved to {args.out}")

    print("\n=== Summary: Spark-SQL filter p50 (ms) across selectivities")
    header = f"{'Sel':<8}" + "".join(f"{s[:16]:>18}" for s in sources)
    print(header)
    for q in queries:
        row_str = f"{q['selectivity_target']:<8.4f}"
        for src in sources:
            matches = [r for r in results
                       if r.get("source") == src and r.get("selectivity_target") == q["selectivity_target"]]
            if matches and "median_ms" in matches[0]:
                row_str += f"  {matches[0]['median_ms']:>12.1f}ms  "
            else:
                row_str += f"  {'ERR':>14}  "
        print(row_str)

    spark.stop()


if __name__ == "__main__":
    main()
