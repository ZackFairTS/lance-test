"""M3: full-scan and single-column scan throughput for Lance vs Iceberg.

Reads both sides through Python + Arrow, NOT Spark:
  - Lance via `lance.dataset(uri).to_table(columns=[...])`  (pylance native).
  - Iceberg via `pyiceberg.table.StaticTable.from_metadata(metadata_uri)
    .scan(selected_fields=[...]).to_arrow()`  (pyiceberg native).

Why not Spark neutral-engine (B4's pattern): lance-spark 0.0.15 on
Spark 3.5.5-amzn-1 raises CatalogNotFoundException on
`spark.read.format("lance").option("path", uri).load()` because newer Spark
no longer falls back to DataSourceRegister SPI for the DSv2 catalog-style
format resolution, and 0.0.15's LanceNamespaceSparkCatalog requires a
lance-namespace jar not shipped with the bundle. Routing Iceberg through
Spark and Lance through pylance would make the comparison apples-to-oranges
(L2 bug class). Routing both through Python + Arrow keeps the engine
constant and the comparison honest.

Measurement contract:
  - 3 warmup + 7 rounds per operation.
  - Each round creates a FRESH dataset/table handle to exclude any
    file/metadata caching from inter-round timing.
  - full_scan reads all columns. col_scan reads a single designated column
    (mirrors M2's extreme size-ratio columns).

Output: results/M3_scan_sf<N>.json.
"""
import argparse
import gc
import json
import os
import statistics
import time

import lance
from pyiceberg.table import StaticTable


WARMUP = 3
ROUNDS = 7

SCAN_COLUMN_BY_TABLE = {
    "store_sales": "ss_ext_list_price",
    "customer": "c_customer_id",
}


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


def timed_materialized(action_builder, warmup=WARMUP, rounds=ROUNDS):
    """Run action_builder() -> callable that performs the read and returns
    a pyarrow.Table. Timer wraps ONLY the action call to capture
    materialization; the builder is called outside t0 so dataset/table open
    cost is warmed up separately.
    """
    for _ in range(warmup):
        action = action_builder()
        _ = action()
        gc.collect()
    runs = []
    last_rows = None
    out = None
    for _ in range(rounds):
        out = None
        gc.collect()
        action = action_builder()
        t0 = time.perf_counter()
        out = action()
        dt = time.perf_counter() - t0
        runs.append(dt)
        last_rows = out.num_rows if out is not None else None
    out = None
    stats = _stats(runs)
    stats["rows_returned"] = last_rows
    return stats


def lance_action(uri, storage_options, columns=None):
    def do_read():
        ds = lance.dataset(uri, storage_options=storage_options)
        if columns is None:
            return ds.to_table()
        return ds.to_table(columns=columns)
    return do_read


def iceberg_action(metadata_uri, columns=None):
    """`columns=None` means full scan; otherwise pyiceberg's
    `selected_fields` requires a tuple of column names.
    """
    def do_read():
        tbl = StaticTable.from_metadata(metadata_uri)
        scan = tbl.scan(selected_fields=tuple(columns)) if columns \
            else tbl.scan()
        return scan.to_arrow()
    return do_read


def find_iceberg_metadata_uri(region, data_uri, timeout_s=60):
    """Iceberg HadoopCatalog writes a `version-hint.text` file naming the
    current metadata version; the pointer is `metadata/v<N>.metadata.json`.
    We read the hint to avoid assuming v1 forever.
    """
    import subprocess
    rel = data_uri[len("s3://"):]
    bucket, _, key = rel.partition("/")
    hint_key = f"{key.rstrip('/')}/metadata/version-hint.text"
    r = subprocess.run(
        ["aws", "s3", "cp", f"s3://{bucket}/{hint_key}", "-",
         "--region", region],
        check=True, capture_output=True, text=True, timeout=timeout_s)
    version = r.stdout.strip()
    if not version.isdigit():
        raise RuntimeError(
            f"unexpected version-hint content at s3://{bucket}/{hint_key}: "
            f"{version!r}")
    return f"s3://{bucket}/{key.rstrip('/')}/metadata/v{version}.metadata.json"


def run_scans_for_table(fmt_name, action_factory, scan_col_name):
    """action_factory(columns=...) -> callable.
    Runs full_scan (columns=None) and col_scan (columns=[scan_col_name]).
    Cardinality is taken from the first timed run's output rows.
    """
    results = {}

    stats = timed_materialized(lambda: action_factory(columns=None))
    results["full_scan"] = stats
    print(f"  [{fmt_name}] full_scan p50={stats['median_ms']:>9.2f} ms  "
          f"rows={stats.get('rows_returned')}")

    stats = timed_materialized(
        lambda: action_factory(columns=[scan_col_name]))
    stats["column"] = scan_col_name
    results["col_scan"] = stats
    print(f"  [{fmt_name}] col_scan({scan_col_name}) "
          f"p50={stats['median_ms']:>9.2f} ms  "
          f"rows={stats.get('rows_returned')}")

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m1-manifest", required=True)
    ap.add_argument("--region", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--scan-column", default=None,
                    help="override single-column-scan target (default per table)")
    args = ap.parse_args()

    with open(args.m1_manifest) as f:
        m1 = json.load(f)
    env = parse_run_env()
    if args.region is None:
        args.region = (os.environ.get("AWS_REGION")
                       or m1.get("region") or env.get("AWS_REGION"))
    if not args.region:
        raise SystemExit("AWS_REGION not found.")
    os.environ["AWS_REGION"] = args.region

    scale = m1.get("scale")
    if scale is None:
        raise SystemExit("M1 manifest missing 'scale'")
    if args.out is None:
        args.out = (f"/home/hadoop/lance-extended-bench/results/"
                    f"M3_scan_sf{scale}.json")

    print(f"[M3] m1 manifest: {args.m1_manifest}")
    print(f"[M3] scale=sf{scale}  region={args.region}")

    by_table = {}
    for rec in m1["records"]:
        if not rec.get("ok"):
            continue
        by_table.setdefault(rec["table"], {})[rec["format"]] = rec

    storage_options = {"region": args.region}
    out_tables = []

    for table, fmts in by_table.items():
        if not table.isidentifier():
            print(f"[M3] skipping malformed table name: {table!r}")
            continue
        if "lance_2.2" not in fmts or "iceberg_v2" not in fmts:
            print(f"[M3] skipping {table}: needs both formats, "
                  f"have {list(fmts)}")
            continue
        scan_col = args.scan_column or SCAN_COLUMN_BY_TABLE.get(table)
        if scan_col is None:
            print(f"[M3] skipping {table}: no scan column configured "
                  f"(pass --scan-column)")
            continue
        print(f"\n[M3] === table: {table}  "
              f"(scan column: {scan_col}) ===")

        t_rec = {"table": table, "scan_column": scan_col, "formats": {}}

        lance_uri = fmts["lance_2.2"]["uri"]
        print(f"[M3] lance {lance_uri}")
        try:
            r = run_scans_for_table(
                "lance_2.2",
                lambda columns=None, u=lance_uri, so=storage_options:
                    lance_action(u, so, columns=columns),
                scan_col)
            t_rec["formats"]["lance_2.2"] = r
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            if isinstance(e, (SystemExit, GeneratorExit)):
                raise
            t_rec["formats"]["lance_2.2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  lance scans FAILED: "
                  f"{t_rec['formats']['lance_2.2']['error']}")

        iceberg_data_uri = fmts["iceberg_v2"]["data_uri"]
        try:
            meta_uri = find_iceberg_metadata_uri(args.region, iceberg_data_uri)
            print(f"[M3] iceberg {meta_uri}")
            r = run_scans_for_table(
                "iceberg_v2",
                lambda columns=None, m=meta_uri:
                    iceberg_action(m, columns=columns),
                scan_col)
            t_rec["formats"]["iceberg_v2"] = r
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            if isinstance(e, (SystemExit, GeneratorExit)):
                raise
            t_rec["formats"]["iceberg_v2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  iceberg scans FAILED: "
                  f"{t_rec['formats']['iceberg_v2']['error']}")

        out_tables.append(t_rec)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + ".tmp"
    with open(tmp, "w") as f:
        json.dump({
            "scale": scale,
            "region": args.region,
            "m1_manifest": os.path.abspath(args.m1_manifest),
            "warmup": WARMUP,
            "rounds": ROUNDS,
            "engine": "python+arrow (pylance + pyiceberg native)",
            "lance_version": lance.__version__,
            "tables": out_tables,
        }, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, args.out)
    print(f"\n[M3] Saved: {args.out}")

    print("\n=== M3 scan p50 summary (ms) ===")
    for t in out_tables:
        print(f"\n-- {t['table']} --")
        print(f"{'op':<20} {'lance_p50_ms':>14} "
              f"{'iceberg_p50_ms':>16} {'ratio':>8}")
        for op in ("full_scan", "col_scan"):
            l = t["formats"].get("lance_2.2", {}).get(op, {})
            i = t["formats"].get("iceberg_v2", {}).get(op, {})
            lm = l.get("median_ms")
            im = i.get("median_ms")
            lm_s = f"{lm:.2f}" if lm is not None else "--"
            im_s = f"{im:.2f}" if im is not None else "--"
            ratio = (f"{lm/im:.2f}x" if lm is not None and im
                     and im > 0 else "--")
            print(f"  {op:<18} {lm_s:>14} {im_s:>16} {ratio:>8}")


if __name__ == "__main__":
    main()
