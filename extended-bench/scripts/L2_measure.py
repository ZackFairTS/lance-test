"""L2 measure phase: consume L2_manifest.json and benchmark each dataset.

Measurements per dataset (as applicable):
  - full_scan:   to_table() on all columns
  - col_scan:    single numeric column
  - point_take:  take(1000 random indices)  (deterministic seed, sorted)
  - blob_take:   take_blobs / take on the blob column (only tab_blob + lance_2.2)
  - filter:      simple predicate on a categorical column
  - nested_read: read only a deep-nested subfield (tab_nested only)

All timings are median of 7 rounds after 3 warmup rounds on pyarrow tables
materialized (to force reads to completion; avoids lazy-scan undercounting).
"""
import argparse
import gc
import json
import os
import statistics
import time

import lance
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pa_ds

WARMUP = 3
ROUNDS = 7
POINT_TAKE_N = 1000
BLOB_TAKE_N = 100
POINT_TAKE_SEED = 7


def _stats(runs):
    out = {
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms":   statistics.mean(runs) * 1000,
        "min_ms":    min(runs) * 1000,
        "max_ms":    max(runs) * 1000,
        "stdev_ms": (statistics.stdev(runs) * 1000
                     if len(runs) > 1 else 0.0),
        "runs_ms":  [round(r * 1000, 2) for r in runs],
    }
    return {k: (round(v, 3) if isinstance(v, float) else v)
            for k, v in out.items()}


def timed(fn, warmup=WARMUP, rounds=ROUNDS):
    """Run fn warmup + rounds times; return stats dict.

    The previous-round result is set to None BEFORE gc.collect() so that
    the deallocation cost does not leak into the next round's timed
    interval. fn() is called inside the timed interval to include
    materialization cost; out is held until AFTER perf_counter() stops.
    """
    out = None
    for _ in range(warmup):
        out = None
        gc.collect()
        out = fn()
    runs = []
    rows_last = None
    for _ in range(rounds):
        out = None
        gc.collect()
        t0 = time.perf_counter()
        out = fn()
        dt = time.perf_counter() - t0
        runs.append(dt)
        if hasattr(out, "num_rows"):
            rows_last = out.num_rows
    out = None
    s = _stats(runs)
    if rows_last is not None:
        s["rows_returned"] = rows_last
    return s


def open_lance(uri, storage_options=None):
    if storage_options:
        return lance.dataset(uri, storage_options=storage_options)
    return lance.dataset(uri)


def open_parquet_dataset(uri):
    return pa_ds.dataset(uri, format="parquet")


def lance_full_scan(ds):
    return ds.to_table()


def lance_col_scan(ds, col):
    return ds.to_table(columns=[col])


def lance_point_take(ds, n_rows):
    k = min(POINT_TAKE_N, n_rows)
    rng = np.random.default_rng(POINT_TAKE_SEED)
    indices = sorted(rng.choice(n_rows, size=k, replace=False).tolist())
    return ds.take(indices)


def lance_filter_scan(ds, filter_str, cols):
    return ds.to_table(columns=cols, filter=filter_str)


def lance_nested_read(ds):
    return ds.to_table(columns=["nested"])


def lance_blob_take_v22(ds, n_rows):
    k = min(BLOB_TAKE_N, n_rows)
    rng = np.random.default_rng(POINT_TAKE_SEED)
    indices = sorted(rng.choice(n_rows, size=k, replace=False).tolist())
    blobs = ds.take_blobs("payload", indices=indices)
    total = 0
    for b in blobs:
        with b as f:
            total += len(f.read())
    return {"num_rows": len(blobs), "total_bytes": total}


def lance_blob_take_legacy(ds, n_rows):
    """For v2.0/v2.1, 'payload' is a large_binary column, not a Blob V2 column.
    We simulate the same workload by taking the column for random rows."""
    k = min(BLOB_TAKE_N, n_rows)
    rng = np.random.default_rng(POINT_TAKE_SEED)
    indices = sorted(rng.choice(n_rows, size=k, replace=False).tolist())
    t = ds.take(indices, columns=["payload"])
    total_bytes = sum(len(v.as_py()) for v in t.column("payload"))
    return {"num_rows": t.num_rows, "total_bytes": total_bytes}


def parquet_full_scan(ds):
    return ds.to_table()


def parquet_col_scan(ds, col):
    return ds.to_table(columns=[col])


def parquet_point_take(ds, n_rows):
    """Parquet has no take-by-row-index primitive; this uses an id-based isin
    predicate as a functional proxy. Latency is NOT directly comparable to
    lance.take() — it measures full-scan-with-predicate-pushdown vs. Lance's
    direct index path. In the summary the ratio should be framed accordingly.
    """
    k = min(POINT_TAKE_N, n_rows)
    rng = np.random.default_rng(POINT_TAKE_SEED)
    indices = sorted(rng.choice(n_rows, size=k, replace=False).tolist())
    tbl = ds.to_table(columns=None, filter=pc.field("id").isin(indices))
    return tbl


def parquet_filter_scan(ds, filter_expr, cols):
    return ds.to_table(columns=cols, filter=filter_expr)


def parquet_nested_read(ds):
    return ds.to_table(columns=["nested"])


def parquet_blob_take(ds, n_rows):
    """Parquet has no 'take by row index' primitive. We use id-filter as a
    functional proxy. This is explicitly NOT comparable to lance.take()
    latency; it's the best Parquet can do for 'give me K rows by id'.
    """
    k = min(BLOB_TAKE_N, n_rows)
    rng = np.random.default_rng(POINT_TAKE_SEED)
    ids = sorted(rng.choice(n_rows, size=k, replace=False).tolist())
    t = ds.to_table(columns=["id", "payload"], filter=pc.field("id").isin(ids))
    total_bytes = sum(len(v.as_py()) for v in t.column("payload"))
    return {"num_rows": t.num_rows, "total_bytes": total_bytes}


def measure_lance(record):
    uri = record["uri"]
    wl = record["workload"]
    fmt = record["format"]
    n_rows = record["n_rows"]
    is_v22 = fmt == "lance_2.2"

    region = record.get("region")
    storage_options = {"region": region} if region else None
    ds = open_lance(uri, storage_options=storage_options)
    result = {
        "open_ok": True,
        "data_storage_version": ds.data_storage_version,
        "manifest_version": ds.version,
    }

    if wl in ("tab_flat", "tab_vec"):
        result["full_scan"] = timed(lambda: lance_full_scan(ds))
        result["col_scan_amount"] = timed(lambda: lance_col_scan(ds, "amount"))
        result["point_take"] = timed(lambda: lance_point_take(ds, n_rows))
        result["filter_category"] = timed(
            lambda: lance_filter_scan(ds, "category = 'cat_0'",
                                      ["id", "amount"]))
        if wl == "tab_vec":
            result["col_scan_vector"] = timed(lambda: lance_col_scan(ds, "vector"))

    elif wl == "tab_nested":
        result["full_scan"] = timed(lambda: lance_full_scan(ds))
        result["nested_subread"] = timed(lambda: lance_nested_read(ds))

    elif wl == "tab_blob":
        result["scan_non_blob"] = timed(
            lambda: lance_col_scan(ds, "id"))
        if is_v22:
            result["blob_take_v22"] = timed(
                lambda: lance_blob_take_v22(ds, n_rows))
        else:
            result["blob_take_legacy"] = timed(
                lambda: lance_blob_take_legacy(ds, n_rows))

    return result


def measure_parquet(record):
    uri = record["uri"]
    wl = record["workload"]
    n_rows = record["n_rows"]

    ds = open_parquet_dataset(uri)
    result = {"open_ok": True}

    if wl in ("tab_flat", "tab_vec"):
        result["full_scan"] = timed(lambda: parquet_full_scan(ds))
        result["col_scan_amount"] = timed(lambda: parquet_col_scan(ds, "amount"))
        result["point_take"] = timed(lambda: parquet_point_take(ds, n_rows))
        result["filter_category"] = timed(
            lambda: parquet_filter_scan(ds, pc.field("category") == "cat_0",
                                        ["id", "amount"]))
        if wl == "tab_vec":
            result["col_scan_vector"] = timed(lambda: parquet_col_scan(ds, "vector"))

    elif wl == "tab_nested":
        result["full_scan"] = timed(lambda: parquet_full_scan(ds))
        result["nested_subread"] = timed(lambda: parquet_nested_read(ds))

    elif wl == "tab_blob":
        result["scan_non_blob"] = timed(lambda: parquet_col_scan(ds, "id"))
        result["blob_take_legacy"] = timed(
            lambda: parquet_blob_take(ds, n_rows))

    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default="/home/hadoop/lance-extended-bench/"
                                          "results/L2_manifest.json")
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/"
                                     "results/L2_format_compare.json")
    ap.add_argument("--only-workloads", nargs="*", default=None,
                    help="restrict to these workloads (default: all in manifest)")
    ap.add_argument("--only-formats", nargs="*", default=None)
    args = ap.parse_args()

    with open(args.manifest) as f:
        manifest = json.load(f)
    records = manifest["records"]

    region = manifest.get("region")
    if region:
        os.environ.setdefault("AWS_REGION", region)

    print(f"[L2-measure] Manifest: {args.manifest}")
    print(f"[L2-measure] s3_root: {manifest.get('s3_root')}")
    print(f"[L2-measure] region: {region}")
    print(f"[L2-measure] lance_version(write): {manifest.get('lance_version')}")
    print(f"[L2-measure] lance_version(measure): {lance.__version__}")

    out_records = []
    for rec in records:
        if not rec.get("ok"):
            out_records.append({**rec, "measure_skipped": "write_failed"})
            continue
        if args.only_workloads and rec["workload"] not in args.only_workloads:
            continue
        if args.only_formats and rec["format"] not in args.only_formats:
            continue

        wl = rec["workload"]
        fmt = rec["format"]
        print(f"\n[L2-measure] {wl}/{fmt}  -> {rec['uri']}")
        out_rec = dict(rec)
        try:
            if fmt.startswith("lance_"):
                m = measure_lance(rec)
            elif fmt.startswith("parquet_"):
                m = measure_parquet(rec)
            else:
                raise ValueError(f"unknown format: {fmt}")
            out_rec["measurements"] = m
            for key in ("full_scan", "col_scan_amount", "col_scan_vector",
                        "point_take", "filter_category", "nested_subread",
                        "blob_take_v22", "blob_take_legacy", "scan_non_blob"):
                if key in m:
                    print(f"  {key:<20s}  p50={m[key]['median_ms']:9.2f} ms"
                          + (f"  rows={m[key]['rows_returned']}"
                             if "rows_returned" in m[key] else ""))
        except Exception as e:
            out_rec["measure_error"] = f"{type(e).__name__}: {e}"[:400]
            print(f"  measure FAILED: {out_rec['measure_error']}")
        out_records.append(out_rec)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "lance_version": lance.__version__,
            "pyarrow_version": pa.__version__,
            "warmup": WARMUP,
            "rounds": ROUNDS,
            "point_take_n": POINT_TAKE_N,
            "source_manifest": args.manifest,
            "s3_root": manifest.get("s3_root"),
            "records": out_records,
        }, f, indent=2)
    print(f"\n[L2-measure] Saved: {args.out}")

    print("\n=== L2 summary (p50 ms per op):")
    by_wl = {}
    for r in out_records:
        m = r.get("measurements")
        if not m:
            continue
        by_wl.setdefault(r["workload"], []).append(r)

    for wl, items in by_wl.items():
        print(f"\n-- {wl} --")
        ops = set()
        for it in items:
            ops.update(k for k in it["measurements"] if isinstance(
                it["measurements"][k], dict) and "median_ms" in it["measurements"][k])
        ops = sorted(ops)
        header = f"{'format':<16}" + "".join(f"{o[:14]:>16}" for o in ops)
        print(header)
        for it in items:
            row = f"{it['format']:<16}"
            for op in ops:
                v = it["measurements"].get(op, {}).get("median_ms")
                row += f"{(f'{v:.2f}' if v is not None else '--'):>16}"
            print(row)


if __name__ == "__main__":
    main()
