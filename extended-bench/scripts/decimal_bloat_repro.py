"""Minimal reproduction: Lance decimal columns bloat 5-6x vs Parquet.

Observed on real TPC-DS store_sales (M series): every Decimal(7,2) money
column (ss_net_paid, ss_ext_sales_price, etc.) is 5.44-5.79x larger in
Lance v2.2 than in zstd-level-3 Parquet, for the exact same data.

This script reproduces the effect from SYNTHETIC data on local disk so
that anyone can run it without AWS/EMR. It isolates the effect by
writing two datasets with IDENTICAL rows (Decimal(7,2) money column)
and NOTHING ELSE, then compares on-disk bytes.

Usage:
    python3 decimal_bloat_repro.py [--rows 1000000] [--out /tmp/lance_decimal_repro]

Environment (from observation run):
    pylance 4.0.1 (lance-core 0.39.0)
    pyarrow 20.0.0

Expected result (with defaults, n_rows=1M):
    Lance v2.2 decimal col: ~16 MB
    Parquet zstd-3 decimal col: ~3 MB
    Ratio: ~5x

Plus a control column (float64 random) that shows compression parity
(both formats ~8 MB) — proves the bloat is decimal-specific, not a
generic Lance-overhead issue.

Distribution of values mimics TPC-DS store_sales money columns:
heavy low-value tail (0.00 .. 200.00), matching what real retail
price data looks like. Parquet's dict encoding benefits hugely from
this skew; Lance apparently doesn't.
"""
import argparse
import decimal
import json
import os
import shutil
import time

import lance
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def build_table(n_rows, seed=42, mode="lognormal"):
    """Generate synthetic Decimal(7,2) column.

    modes:
      lognormal - iid log-normal(µ=3, σ=1.3), random order.
                  Tests pure distributional compression.
      tpcds_like - TPC-DS store_sales style: ss_net_paid ≈
                  quantity(int8) × base_price(Decimal) - discount, where
                  base_price is drawn from a small cluster (grouped by
                  product) and quantity has a heavy 1.0 mode. Introduces
                  the kind of within-row-group value clustering that
                  makes Parquet's dict + RLE encoding shine.
      sorted -   lognormal, then sorted. Maximum clustering -- upper
                  bound on what clustering alone can achieve.
    """
    rng = np.random.default_rng(seed)

    if mode == "lognormal":
        raw = rng.lognormal(mean=3.0, sigma=1.3, size=n_rows)
    elif mode == "sorted":
        raw = np.sort(rng.lognormal(mean=3.0, sigma=1.3, size=n_rows))
    elif mode == "tpcds_like":
        n_products = max(10, n_rows // 1000)
        base_prices = rng.lognormal(mean=3.0, sigma=1.0, size=n_products)
        product_id = rng.integers(0, n_products, n_rows)
        base = base_prices[product_id]
        quantity = rng.choice([1, 1, 1, 2, 3, 5, 10],
                              size=n_rows).astype(np.float64)
        discount_pct = rng.choice([0.0, 0.0, 0.0, 0.10, 0.25],
                                  size=n_rows)
        raw = base * quantity * (1.0 - discount_pct)
    else:
        raise ValueError(f"unknown mode: {mode}")

    raw = np.clip(raw, 0.0, 99999.99)
    cents = np.round(raw * 100).astype(np.int64)
    decimals = [decimal.Decimal(int(c)).scaleb(-2) for c in cents]
    decimal_arr = pa.array(decimals, type=pa.decimal128(7, 2))

    float_arr = pa.array(raw.astype(np.float64))
    int8_arr = pa.array(rng.integers(1, 101, n_rows, dtype=np.int8))

    return pa.table({
        "id": pa.array(np.arange(n_rows, dtype=np.int64)),
        "money_decimal": decimal_arr,
        "money_float64": float_arr,
        "quantity_int8": int8_arr,
    })


def write_lance(tbl, path, version):
    if os.path.exists(path):
        shutil.rmtree(path)
    t0 = time.perf_counter()
    lance.write_dataset(tbl, path, mode="overwrite",
                        data_storage_version=version)
    return round(time.perf_counter() - t0, 3)


def write_parquet(tbl, path, compression, level=None):
    if os.path.exists(path):
        os.remove(path)
    kw = dict(
        compression=compression,
        use_dictionary=True,
        write_statistics=True,
        data_page_version="2.0",
        row_group_size=1_048_576,
    )
    if level is not None and compression == "zstd":
        kw["compression_level"] = level
    t0 = time.perf_counter()
    pq.write_table(tbl, path, **kw)
    return round(time.perf_counter() - t0, 3)


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def lance_per_column_bytes(path):
    ds = lance.dataset(path)
    stats = ds.stats.data_stats()
    out = {}
    for field, fstats in zip(ds.schema, stats.fields):
        out[field.name] = {
            "bytes_on_disk": fstats.bytes_on_disk,
            "type": str(field.type),
        }
    return out


def parquet_per_column_bytes(path):
    pf = pq.ParquetFile(path)
    per_col_compressed = {}
    per_col_uncompressed = {}
    col_types = {}
    for c in range(pf.metadata.num_columns):
        col_types[pf.schema_arrow.field(c).name] = str(
            pf.schema_arrow.field(c).type)
    for rg_idx in range(pf.num_row_groups):
        rg = pf.metadata.row_group(rg_idx)
        for c_idx in range(rg.num_columns):
            col = rg.column(c_idx)
            name = col.path_in_schema
            per_col_compressed[name] = (per_col_compressed.get(name, 0)
                                        + col.total_compressed_size)
            per_col_uncompressed[name] = (per_col_uncompressed.get(name, 0)
                                          + col.total_uncompressed_size)
    out = {}
    for name, c in per_col_compressed.items():
        out[name] = {
            "bytes_on_disk": c,
            "bytes_uncompressed": per_col_uncompressed[name],
            "type": col_types.get(name),
        }
    return out


def fmt_mb(nbytes):
    return f"{nbytes / 1e6:.2f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--out", default="/tmp/lance_decimal_repro")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--mode", default="lognormal",
                    choices=["lognormal", "tpcds_like", "sorted"])
    ap.add_argument("--json-out", default=None,
                    help="write machine-readable JSON to this path")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    print(f"lance    {lance.__version__}")
    print(f"pyarrow  {pa.__version__}")
    print(f"rows     {args.rows:,}")
    print(f"mode     {args.mode}")
    print(f"out      {args.out}")
    print()

    print(f"[1/6] building synthetic table (mode={args.mode})")
    t0 = time.perf_counter()
    tbl = build_table(args.rows, seed=args.seed, mode=args.mode)
    print(f"      built in {time.perf_counter() - t0:.2f}s")
    print(f"      schema: {tbl.schema}")
    money = tbl.column("money_decimal")
    unique_count = len(pa.compute.unique(money))
    print(f"      money_decimal unique values: {unique_count:,} "
          f"({unique_count / args.rows * 100:.2f}% of rows)")
    print()

    targets = [
        ("lance_2.0", os.path.join(args.out, "v2_0.lance"), "lance", "2.0"),
        ("lance_2.1", os.path.join(args.out, "v2_1.lance"), "lance", "2.1"),
        ("lance_2.2", os.path.join(args.out, "v2_2.lance"), "lance", "2.2"),
        ("parquet_zstd3", os.path.join(args.out, "zstd3.parquet"),
         "parquet", ("zstd", 3)),
        ("parquet_snappy", os.path.join(args.out, "snappy.parquet"),
         "parquet", ("snappy", None)),
    ]

    results = []
    print(f"[2/6] writing {len(targets)} outputs")
    for label, path, kind, opt in targets:
        if kind == "lance":
            wsec = write_lance(tbl, path, opt)
        else:
            compression, level = opt
            wsec = write_parquet(tbl, path, compression, level)
        total = du(path)
        if kind == "lance":
            per_col = lance_per_column_bytes(path)
        else:
            per_col = parquet_per_column_bytes(path)
        results.append({
            "label": label,
            "kind": kind,
            "path": path,
            "write_seconds": wsec,
            "total_bytes": total,
            "per_column": per_col,
        })
        print(f"  {label:<16} write={wsec:>7}s total_mb={fmt_mb(total):>8}")

    print()
    print("[3/6] PER-COLUMN BYTES BREAKDOWN")
    col_names = ["id", "money_decimal", "money_float64", "quantity_int8"]
    hdr = f"  {'column':<18} " + "".join(f"{r['label']:>16}" for r in results)
    print(hdr)
    for col in col_names:
        row = f"  {col:<18} "
        for r in results:
            b = r["per_column"].get(col, {}).get("bytes_on_disk")
            row += f"{fmt_mb(b) + ' MB':>16}" if b is not None else f"{'--':>16}"
        print(row)

    print()
    print("[4/6] RATIOS vs parquet_zstd3 (per column)")
    parquet_zstd = next(r for r in results if r["label"] == "parquet_zstd3")
    hdr = f"  {'column':<18} " + "".join(f"{r['label']:>16}"
                                        for r in results if r["label"] != "parquet_zstd3")
    print(hdr)
    for col in col_names:
        pb = parquet_zstd["per_column"].get(col, {}).get("bytes_on_disk")
        if pb is None or pb == 0:
            continue
        row = f"  {col:<18} "
        for r in results:
            if r["label"] == "parquet_zstd3":
                continue
            b = r["per_column"].get(col, {}).get("bytes_on_disk")
            if b is None:
                row += f"{'--':>16}"
            else:
                ratio = b / pb
                marker = "  <<<" if ratio >= 3.0 else ""
                row += f"{ratio:>12.2f}x{marker:<4}"
        print(row)

    print()
    print("[5/6] INTERPRETATION")
    decimal_lance = next(
        r["per_column"]["money_decimal"]["bytes_on_disk"]
        for r in results if r["label"] == "lance_2.2")
    decimal_parquet = parquet_zstd["per_column"]["money_decimal"]["bytes_on_disk"]
    float_lance = next(
        r["per_column"]["money_float64"]["bytes_on_disk"]
        for r in results if r["label"] == "lance_2.2")
    float_parquet = parquet_zstd["per_column"]["money_float64"]["bytes_on_disk"]

    decimal_ratio = decimal_lance / decimal_parquet
    float_ratio = float_lance / float_parquet

    print(f"  money_decimal:  Lance {fmt_mb(decimal_lance)} MB  "
          f"Parquet {fmt_mb(decimal_parquet)} MB  "
          f"-> Lance / Parquet = {decimal_ratio:.2f}x")
    print(f"  money_float64:  Lance {fmt_mb(float_lance)} MB  "
          f"Parquet {fmt_mb(float_parquet)} MB  "
          f"-> Lance / Parquet = {float_ratio:.2f}x")
    print()
    if decimal_ratio >= 3.0 and float_ratio <= 1.5:
        print("  VERDICT: decimal-specific bloat reproduced.")
        print(f"     float control ratio = {float_ratio:.2f}x (normal),")
        print(f"     decimal ratio       = {decimal_ratio:.2f}x (> 3x -> bug).")
    elif decimal_ratio < 3.0:
        print("  decimal ratio is within normal range on this data size;")
        print("  try --rows 5000000 to amplify the dict-encoding benefit.")
    else:
        print("  both columns bloat -> not decimal-specific on this system.")

    print()
    print("[6/6] done")

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump({
                "lance_version": lance.__version__,
                "pyarrow_version": pa.__version__,
                "rows": args.rows,
                "seed": args.seed,
                "mode": args.mode,
                "results": results,
                "summary": {
                    "decimal_ratio_lance_over_parquet": decimal_ratio,
                    "float_ratio_lance_over_parquet": float_ratio,
                },
            }, f, indent=2, default=str)
        print(f"  JSON written: {args.json_out}")


if __name__ == "__main__":
    main()
