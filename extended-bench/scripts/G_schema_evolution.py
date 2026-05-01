import argparse
import gc
import json
import os
import shutil
import tempfile
import time

import lance
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def build_table(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    return pa.table({
        "id": pa.array(range(n_rows), type=pa.int64()),
        "score": pa.array(rng.standard_normal(n_rows).astype(np.float32)),
        "price": pa.array(rng.uniform(0, 1000, n_rows)),
        "category": pa.array(rng.choice([f"C{i}" for i in range(20)], n_rows)),
    })


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def timed_once(fn):
    gc.collect()
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=1_000_000)
    ap.add_argument("--work-dir", default=tempfile.mkdtemp(prefix="g_schema_"))
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/G_schema_evolution.json")
    args = ap.parse_args()

    os.makedirs(args.work_dir, exist_ok=True)
    tbl = build_table(args.n_rows)
    results = {"n_rows": args.n_rows}

    lance_uri = os.path.join(args.work_dir, "base.lance")
    parquet_path = os.path.join(args.work_dir, "base.parquet")

    lance.write_dataset(tbl, lance_uri, mode="overwrite")
    pq.write_table(tbl, parquet_path, compression="snappy", use_dictionary=True,
                   write_statistics=True, row_group_size=1 << 20)
    base_lance_size = du(lance_uri)
    base_parquet_size = du(parquet_path)
    print(f"Base sizes: lance={base_lance_size/1e6:.1f}MB  parquet={base_parquet_size/1e6:.1f}MB")

    results["base"] = {
        "lance_mb": round(base_lance_size / 1e6, 2),
        "parquet_mb": round(base_parquet_size / 1e6, 2),
    }

    print("\n=== Lance add_columns: schema-only (new nullable field, no data)")
    ds = lance.dataset(lance_uri)
    try:
        t_add_schema = timed_once(lambda: ds.add_columns(
            pa.schema([pa.field("new_nullable", pa.float64())]),
        ))
        ds = lance.dataset(lance_uri)
        after_schema_size = du(lance_uri)
        print(f"  add_columns (schema only): {t_add_schema*1000:.1f} ms")
        print(f"  size delta: {((after_schema_size-base_lance_size)/1e6):+.3f} MB (expect ~0 for pure metadata)")
        results["lance_add_column_schema_only"] = {
            "add_ms": round(t_add_schema * 1000, 3),
            "size_delta_mb": round((after_schema_size - base_lance_size) / 1e6, 4),
        }
    except Exception as e:
        print(f"  (schema-only add not supported: {str(e)[:120]})")
        results["lance_add_column_schema_only"] = {"error": str(e)[:200]}

    print("\n=== Lance add_columns with SQL expression (computes + writes new column file)")
    ds = lance.dataset(lance_uri)
    base_before_expr = du(lance_uri)
    t_add_expr = timed_once(lambda: ds.add_columns({"price_doubled": "price * 2"}))
    after_expr_size = du(lance_uri)
    ds = lance.dataset(lance_uri)

    read_new_col_elapsed = timed_once(lambda: ds.to_table(columns=["price_doubled"]))
    read_old_col_elapsed = timed_once(lambda: ds.to_table(columns=["price"]))

    print(f"  add_columns (SQL expr): {t_add_expr*1000:.1f} ms")
    print(f"  size delta: {((after_expr_size-base_before_expr)/1e6):+.1f} MB (~8 MB expected for float64 * 1M rows)")
    print(f"  read new col: {read_new_col_elapsed*1000:.1f} ms")
    print(f"  read old col: {read_old_col_elapsed*1000:.1f} ms")

    results["lance_add_column_sql"] = {
        "add_ms": round(t_add_expr * 1000, 2),
        "size_after_mb": round(after_expr_size / 1e6, 2),
        "size_delta_mb": round((after_expr_size - base_before_expr) / 1e6, 2),
        "read_new_col_ms": round(read_new_col_elapsed * 1000, 2),
        "read_old_col_ms": round(read_old_col_elapsed * 1000, 2),
    }

    print("\n=== Parquet 'add column': must rewrite entire file")
    tbl_with_new = tbl.append_column(
        "price_doubled",
        pa.array(np.array(tbl["price"]) * 2, type=pa.float64()),
    )
    parquet_new_path = os.path.join(args.work_dir, "rewrite.parquet")
    t_parquet_rewrite = timed_once(lambda: pq.write_table(
        tbl_with_new, parquet_new_path,
        compression="snappy", use_dictionary=True,
        write_statistics=True, row_group_size=1 << 20,
    ))
    parquet_new_size = du(parquet_new_path)
    print(f"  rewrite time: {t_parquet_rewrite*1000:.1f} ms")
    print(f"  new file size: {parquet_new_size/1e6:.1f} MB")

    results["parquet_rewrite"] = {
        "rewrite_ms": round(t_parquet_rewrite * 1000, 2),
        "new_size_mb": round(parquet_new_size / 1e6, 2),
    }

    print("\n=== Lance drop_columns")
    t_drop = timed_once(lambda: ds.drop_columns(["category"]))
    ds = lance.dataset(lance_uri)
    after_drop_size = du(lance_uri)
    print(f"  drop_columns: {t_drop*1000:.1f} ms")
    print(f"  size: {after_expr_size/1e6:.1f} MB -> {after_drop_size/1e6:.1f} MB  "
          f"(delta={((after_drop_size-after_expr_size)/1e6):+.1f} MB)")

    results["lance_drop_column"] = {
        "drop_ms": round(t_drop * 1000, 2),
        "size_after_mb": round(after_drop_size / 1e6, 2),
        "size_delta_mb": round((after_drop_size - after_expr_size) / 1e6, 2),
    }

    print("\n=== Lance alter_columns rename (metadata-only)")
    try:
        t_rename = timed_once(lambda: ds.alter_columns(
            {"path": "price_doubled", "name": "price_2x"},
        ))
        ds = lance.dataset(lance_uri)
        after_rename_size = du(lance_uri)
        print(f"  rename 'price_doubled' -> 'price_2x': {t_rename*1000:.1f} ms")
        print(f"  size delta: {((after_rename_size-after_drop_size)/1e6):+.3f} MB")
        results["lance_rename_column"] = {
            "rename_ms": round(t_rename * 1000, 3),
            "size_delta_mb": round((after_rename_size - after_drop_size) / 1e6, 4),
        }
    except Exception as e:
        print(f"  ERROR: {e}")
        results["lance_rename_column"] = {"error": str(e)[:200]}

    print("\n=== Summary")
    ratio = results["parquet_rewrite"]["rewrite_ms"] / results["lance_add_column_sql"]["add_ms"]
    print(f"  Parquet rewrite / Lance add_column: {ratio:.1f}x "
          f"({results['parquet_rewrite']['rewrite_ms']:.0f}ms vs {results['lance_add_column_sql']['add_ms']:.0f}ms)")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({"pylance_version": lance.__version__, **results}, f, indent=2)
    print(f"\nSaved to {args.out}")


if __name__ == "__main__":
    main()
