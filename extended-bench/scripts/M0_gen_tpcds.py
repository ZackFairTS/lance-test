"""M0: generate TPC-DS subset and stage as Parquet on S3 for M1 to consume.

Uses DuckDB's TPC-DS extension (`CALL dsdgen(sf=N)`) for authentic TPC-DS
data distributions without needing tpcds-kit / dsdgen toolchain.

Default table subset (store_sales + customer) is the M-series primary
workload: fact table with mixed int/dec/date/fk columns, plus a
wide-string customer dimension for filter-pushdown selectivity tests.

Output layout:
  s3://<base>/M/tpcds_sf<scale>/<table>/<table>.parquet
  results/M0_manifest_sf<scale>.json  -> consumed by M1_write_both.py
"""
import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time

import duckdb

TPCDS_TABLES = frozenset([
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item",
    "promotion", "reason", "ship_mode", "store", "store_returns",
    "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
])

DEFAULT_TABLES = ["store_sales", "customer"]


def parse_run_env(path="/home/hadoop/lance-extended-bench/run.env"):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.split("#", 1)[0].strip()
            out[k.strip()] = v
    return out


def dsdgen(con, scale):
    con.execute("INSTALL tpcds")
    con.execute("LOAD tpcds")
    t0 = time.perf_counter()
    con.execute(f"CALL dsdgen(sf={scale})")
    return time.perf_counter() - t0


def export_table(con, table, local_dir, row_group_size, compression,
                 compression_level):
    os.makedirs(local_dir, exist_ok=True)
    target = os.path.join(local_dir, f"{table}.parquet")
    t0 = time.perf_counter()
    con.execute(
        f"COPY (SELECT * FROM {table}) TO '{target}' "
        f"(FORMAT parquet, COMPRESSION {compression}, "
        f"COMPRESSION_LEVEL {compression_level}, "
        f"ROW_GROUP_SIZE {row_group_size})"
    )
    elapsed = time.perf_counter() - t0
    size = os.path.getsize(target)
    rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return {"rows": rows, "bytes": size, "seconds": round(elapsed, 3),
            "local_path": target}


def schema_for(con, table):
    rows = con.execute(f"DESCRIBE {table}").fetchall()
    return [{"name": r[0], "type": r[1], "nullable": r[2] == "YES"}
            for r in rows]


def s3_upload(local_path, s3_uri, region):
    cmd = ["aws", "s3", "cp", local_path, s3_uri,
           "--region", region, "--only-show-errors"]
    t0 = time.perf_counter()
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True,
                       timeout=3600)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"aws s3 cp failed (rc={e.returncode}): {e.stderr[:400]}") from e
    return time.perf_counter() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=1,
                    help="TPC-DS scale factor (sf=1 ~= 2.9M store_sales rows)")
    ap.add_argument("--tables", nargs="+", default=DEFAULT_TABLES,
                    help="TPC-DS tables to export")
    ap.add_argument("--s3-base", default=None,
                    help="default: read S3_BASE from run.env")
    ap.add_argument("--region", default=None,
                    help="default: read AWS_REGION from run.env")
    ap.add_argument("--row-group-size", type=int, default=1_048_576,
                    help="Parquet row group size (default 1M)")
    ap.add_argument("--compression", default="zstd",
                    help="Parquet compression codec (zstd/snappy/uncompressed)")
    ap.add_argument("--compression-level", type=int, default=3,
                    help="Parquet compression level (zstd: 1-22, default 3 "
                         "for fairness with Iceberg-Parquet side)")
    ap.add_argument("--local-dir", default=None,
                    help="local staging dir (default: tempfile.mkdtemp)")
    ap.add_argument("--keep-local", action="store_true",
                    help="do not delete local staging dir after upload")
    ap.add_argument("--manifest", default=None,
                    help="default: results/M0_manifest_sf<scale>.json")
    args = ap.parse_args()

    unknown = set(args.tables) - TPCDS_TABLES
    if unknown:
        raise SystemExit(
            f"Unknown TPC-DS tables: {sorted(unknown)}. "
            f"Valid tables: {sorted(TPCDS_TABLES)}")

    env = parse_run_env()
    if args.s3_base is None:
        args.s3_base = os.environ.get("S3_BASE") or env.get("S3_BASE")
        if not args.s3_base:
            raise SystemExit("S3 base not found. Pass --s3-base or set S3_BASE.")
    if args.region is None:
        args.region = os.environ.get("AWS_REGION") or env.get("AWS_REGION")
    if not args.region:
        raise SystemExit("AWS_REGION not found. Pass --region or set AWS_REGION.")
    os.environ["AWS_REGION"] = args.region

    if args.manifest is None:
        args.manifest = (f"/home/hadoop/lance-extended-bench/results/"
                         f"M0_manifest_sf{args.scale}.json")

    local_dir_auto = args.local_dir is None
    local_dir = args.local_dir or tempfile.mkdtemp(prefix=f"m0_sf{args.scale}_")
    s3_root = f"{args.s3_base.rstrip('/')}/M/tpcds_sf{args.scale}"

    print(f"[M0] scale=sf{args.scale}")
    print(f"[M0] tables={args.tables}")
    print(f"[M0] local_dir={local_dir} (auto={local_dir_auto})")
    print(f"[M0] s3_root={s3_root}")
    print(f"[M0] row_group_size={args.row_group_size} "
          f"compression={args.compression} level={args.compression_level}")

    print("\n[M0] DuckDB dsdgen ...")
    con = duckdb.connect()
    gen_seconds = dsdgen(con, args.scale)
    print(f"  dsdgen OK in {gen_seconds:.1f}s")

    os.makedirs(os.path.dirname(args.manifest), exist_ok=True)

    def write_manifest(records):
        with open(args.manifest, "w") as f:
            json.dump({
                "scale": args.scale,
                "region": args.region,
                "s3_root": s3_root,
                "row_group_size": args.row_group_size,
                "compression": args.compression,
                "compression_level": args.compression_level,
                "dsdgen_seconds": round(gen_seconds, 3),
                "duckdb_version": duckdb.__version__,
                "tables": records,
            }, f, indent=2, default=str)

    records = []
    try:
        for table in args.tables:
            print(f"\n[M0] Exporting {table} ...")
            local = export_table(con, table, local_dir,
                                 args.row_group_size, args.compression,
                                 args.compression_level)
            s3_uri = f"{s3_root}/{table}/{table}.parquet"
            print(f"  rows={local['rows']:,} bytes={local['bytes']:,} "
                  f"({local['bytes']/1e6:.1f} MB) export_s={local['seconds']}")
            print(f"  upload -> {s3_uri}")
            upload_s = s3_upload(local["local_path"], s3_uri, args.region)
            print(f"  upload OK in {upload_s:.2f}s")
            schema = schema_for(con, table)
            records.append({
                "table": table,
                "rows": local["rows"],
                "local_bytes": local["bytes"],
                "local_size_mb": round(local["bytes"] / 1e6, 2),
                "export_seconds": local["seconds"],
                "upload_seconds": round(upload_s, 3),
                "s3_uri": s3_uri,
                "schema": schema,
            })
            write_manifest(records)
    finally:
        con.close()
        if local_dir_auto and not args.keep_local:
            shutil.rmtree(local_dir, ignore_errors=True)
            print(f"[M0] Removed local staging dir: {local_dir}")

    print(f"\n[M0] Manifest: {args.manifest}")
    print("\n=== M0 summary (rows / size / upload):")
    print(f"{'table':<16} {'rows':>14} {'size_mb':>10} {'upload_s':>10}")
    for r in records:
        print(f"  {r['table']:<16} {r['rows']:>14,} "
              f"{r['local_size_mb']:>10} {r['upload_seconds']:>10}")


if __name__ == "__main__":
    main()
