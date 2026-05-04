"""M5: row-level DELETE + post-delete read amplification.

Tests the Iceberg v2 MoR (merge-on-read) pain point: after DELETE, the
reader must reconcile position-delete files against the original data
files, inflating scan latency. Lance has analogous deletion files per
fragment (always MoR).

Per delete-fraction (0.1%, 1%, 10%):
  1. Create FRESH copies of the source tables to isolate M5 mutations
     from M1 data (M2/M3/M4 must remain reproducible against M1):
       - Lance:   `aws s3 cp --recursive` the source .lance prefix.
                  Lance has no stored `location` field so the copy is
                  a valid independent dataset.
       - Iceberg: Spark `CREATE TABLE ... AS SELECT *` via the ice
                  HadoopCatalog. pyiceberg's StaticTable is read-only
                  (NoopCatalog.commit_table raises NotImplementedError);
                  pyiceberg 0.10 Table.delete() falls back to COPY-ON-
                  WRITE (ignoring write.delete.mode). Both invariants
                  make pyiceberg an unusable writer for this benchmark.
                  `aws s3 cp` of an Iceberg table would silently mutate
                  the source S3 prefix at write time because
                  metadata.json stores an absolute `location` field
                  that points back at the original URI.
  2. Issue DELETE on each copy:
       - Lance:   ds.delete(f"{col} <= {K}"). Writes deletion vector;
                  does not rewrite data files.
       - Iceberg: Spark SQL `DELETE FROM ice.tpcds.<copy> WHERE ...`
                  with `write.delete.mode=merge-on-read` set on the
                  copy at CREATE time, so Spark emits positional-delete
                  Parquet under `data/` rather than rewriting files.
     K is calibrated so the deleted-row count matches the target
     fraction (CDF-based pick; same technique as M4).
  3. Measure scan latency on the post-delete table using pyiceberg
     (read side) for engine symmetry with M3/M4.
  4. Record pre/post S3 footprint to show MoR overhead:
       - Lance:   `_deletions/*.arrow` under the dataset.
       - Iceberg: `data/*-deletes-*.parquet` (position-delete files).

Compaction is NOT run here; M6 covers it.

Output: results/M5_update_sf<N>.json.
"""
import argparse
import gc
import json
import os
import statistics
import subprocess
import time

import lance
import pyarrow.compute as pc
from pyiceberg.table import StaticTable


WARMUP = 3
ROUNDS = 7
DELETE_COLUMN = "ss_sold_date_sk"
DELETE_TABLE = "store_sales"
DELETE_FRACTIONS = {"0.1%": 0.001, "1%": 0.01, "10%": 0.10}
CALIBRATION_TOLERANCE = 0.3

PROJECTED_COLS = ["ss_item_sk", "ss_customer_sk", "ss_quantity",
                  "ss_sales_price"]
SCAN_COLUMN = "ss_ext_list_price"


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


def s3_cp_recursive(src_uri, dst_uri, region, timeout_s=3600):
    t0 = time.perf_counter()
    try:
        subprocess.run(
            ["aws", "s3", "cp", "--recursive", src_uri, dst_uri,
             "--region", region, "--only-show-errors"],
            check=True, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"aws s3 cp --recursive failed (rc={e.returncode}): "
            f"{e.stderr[:400]}") from e
    return round(time.perf_counter() - t0, 3)


def s3_rm_prefix(s3_uri, region, timeout_s=600):
    if not s3_uri.startswith("s3://"):
        return
    try:
        subprocess.run(
            ["aws", "s3", "rm", s3_uri, "--recursive",
             "--region", region, "--only-show-errors"],
            check=True, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.CalledProcessError as e:
        print(f"  s3 rm warn for {s3_uri}: rc={e.returncode} "
              f"stderr={e.stderr[:200]}")


def du_s3(s3_uri, region, timeout_s=600):
    try:
        rel = s3_uri[len("s3://"):]
        bucket, _, key = rel.partition("/")
        out = subprocess.run(
            ["aws", "s3", "ls", f"s3://{bucket}/{key}", "--recursive",
             "--summarize", "--region", region],
            check=True, capture_output=True, text=True, timeout=timeout_s,
        ).stdout
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("Total Size:"):
                return int(line.split(":")[1].strip())
        return None
    except Exception as e:
        print(f"  du failed for {s3_uri}: {e}")
        return None


def find_iceberg_metadata_uri(region, data_uri, timeout_s=60):
    rel = data_uri[len("s3://"):]
    bucket, _, key = rel.partition("/")
    hint_key = f"{key.rstrip('/')}/metadata/version-hint.text"
    try:
        r = subprocess.run(
            ["aws", "s3", "cp", f"s3://{bucket}/{hint_key}", "-",
             "--region", region],
            check=True, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"aws s3 cp version-hint failed (rc={e.returncode}): "
            f"{e.stderr[:400]}") from e
    version = r.stdout.strip()
    if not version.isdigit():
        raise RuntimeError(f"bad version-hint: {version!r}")
    return f"s3://{bucket}/{key.rstrip('/')}/metadata/v{version}.metadata.json"


def fetch_iceberg_metadata_location(metadata_uri, region, timeout_s=60):
    """Pull the Iceberg metadata.json and return its top-level `location`
    field. Used as a guard: `aws s3 cp` clones the metadata byte-for-byte,
    but Iceberg pyiceberg writes derive new file paths from this
    `location` field, NOT from the URI we loaded from (review B1).
    Mismatch between load URI and metadata.location means any write will
    silently corrupt the SOURCE location.
    """
    rel = metadata_uri[len("s3://"):]
    bucket, _, key = rel.partition("/")
    try:
        r = subprocess.run(
            ["aws", "s3", "cp", f"s3://{bucket}/{key}", "-",
             "--region", region],
            check=True, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"fetch metadata.json failed (rc={e.returncode}): "
            f"{e.stderr[:400]}") from e
    meta = json.loads(r.stdout)
    return meta.get("location")


def assert_iceberg_location_matches(metadata_uri, expected_location, region):
    """Abort before any pyiceberg write if the metadata's `location`
    points somewhere other than the expected working copy. Prevents the
    catastrophic B1 bug where `aws s3 cp` + pyiceberg write redirects
    writes into the M1 source prefix.
    """
    actual = fetch_iceberg_metadata_location(metadata_uri, region)
    expected = expected_location.rstrip("/")
    if actual is None:
        raise RuntimeError(
            f"metadata.json at {metadata_uri} has no 'location' field")
    if actual.rstrip("/") != expected:
        raise RuntimeError(
            f"Iceberg metadata location mismatch: metadata.json says "
            f"location={actual!r} but working copy is at "
            f"{expected_location!r}. ABORTING to prevent silent mutation "
            f"of the source location. See review B1 for details.")


def calibrate_delete_K(iceberg_metadata_uri, column, n_total, fractions,
                       tolerance=CALIBRATION_TOLERANCE):
    """Pick K per fraction such that `column <= K` deletes the target
    fraction of rows (same CDF calibration as M4, but the sign convention
    is applied to deletions).
    """
    tbl = StaticTable.from_metadata(iceberg_metadata_uri)
    col_tbl = tbl.scan(selected_fields=(column,)).to_arrow()
    values = col_tbl.column(column)
    counts = pc.value_counts(values).to_pylist()
    ordered = sorted(
        ((vc["values"], vc["counts"]) for vc in counts
         if vc["values"] is not None),
        key=lambda t: t[0])
    cum = []
    running = 0
    for v, c in ordered:
        running += c
        cum.append((v, running))
    picks = {}
    for label, target in fractions.items():
        target_count = max(1, int(round(target * n_total)))
        best_v, best_running = min(cum, key=lambda p: abs(p[1] - target_count))
        actual = best_running / n_total
        picks[label] = {
            "predicate_k": best_v,
            "delete_rows": int(best_running),
            "actual_fraction": actual,
            "target_fraction": target,
            "feasible": (abs(actual - target) / max(target, 1e-9) <= tolerance),
        }
    return picks


def lance_scan_action(uri, storage_options, columns=None):
    def do_read():
        ds = lance.dataset(uri, storage_options=storage_options)
        return ds.to_table(columns=columns) if columns else ds.to_table()
    return do_read


def iceberg_scan_action(metadata_uri, columns=None):
    def do_read():
        tbl = StaticTable.from_metadata(metadata_uri)
        scan = tbl.scan(selected_fields=tuple(columns)) if columns \
            else tbl.scan()
        return scan.to_arrow()
    return do_read


def lance_delete(uri, storage_options, column, k_value):
    ds = lance.dataset(uri, storage_options=storage_options)
    t0 = time.perf_counter()
    ds.delete(f"{column} <= {int(k_value)}")
    return round(time.perf_counter() - t0, 3)


def make_spark_session(iceberg_jar, warehouse_uri, region):
    from pyspark.sql import SparkSession
    if not os.path.exists(iceberg_jar):
        raise FileNotFoundError(f"Iceberg jar missing: {iceberg_jar}")
    return (
        SparkSession.builder
        .appName("M5-iceberg-mor-delete")
        .config("spark.jars", iceberg_jar)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ice",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", warehouse_uri)
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def iceberg_ctas_copy(spark, src_fqn, dst_fqn):
    """CTAS a fresh Iceberg table with write.delete.mode=merge-on-read so
    subsequent DELETE produces position-delete files (not CoW rewrites).
    Writes fresh metadata.json with the correct `location` pointing at
    the new table -- this is how M5 avoids the B1 silent-mutation trap.
    """
    spark.sql(f"DROP TABLE IF EXISTS {dst_fqn}")
    t0 = time.perf_counter()
    spark.sql(
        f"CREATE TABLE {dst_fqn} USING iceberg "
        f"TBLPROPERTIES ("
        f"'format-version'='2', "
        f"'write.format.default'='parquet', "
        f"'write.parquet.compression-codec'='zstd', "
        f"'write.parquet.compression-level'='3', "
        f"'write.delete.mode'='merge-on-read', "
        f"'write.update.mode'='merge-on-read', "
        f"'write.merge.mode'='merge-on-read' "
        f") AS SELECT * FROM {src_fqn}"
    )
    return round(time.perf_counter() - t0, 3)


def iceberg_spark_delete(spark, fqn, column, k_value):
    t0 = time.perf_counter()
    spark.sql(f"DELETE FROM {fqn} WHERE {column} <= {int(k_value)}")
    return round(time.perf_counter() - t0, 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m1-manifest", required=True)
    ap.add_argument("--region", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--work-suffix", default=None,
                    help="suffix under s3_base for M5 working copies "
                         "(default: M5_work_<run_id>)")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--keep-working-copies", action="store_true",
                    help="do not delete the M5 working S3 prefix after measure")
    ap.add_argument("--iceberg-jar",
                    default="/usr/share/aws/iceberg/lib/"
                            "iceberg-spark-runtime-3.5_2.12-1.8.1-amzn-0.jar")
    ap.add_argument("--iceberg-namespace", default="tpcds")
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

    run_id = args.run_id or os.environ.get("RUN_ID") or time.strftime(
        "%Y%m%d-%H%M%S")
    if args.out is None:
        args.out = (f"/home/hadoop/lance-extended-bench/results/"
                    f"M5_update_sf{scale}.json")

    s3_base = m1["s3_base"]
    work_suffix = args.work_suffix or f"M5_work_{run_id}"
    m5_root = f"{s3_base.rstrip('/')}/M5/{work_suffix}"
    iceberg_warehouse = m1["iceberg_warehouse"]

    print(f"[M5] m1 manifest: {args.m1_manifest}")
    print(f"[M5] scale=sf{scale}  region={args.region}")
    print(f"[M5] M5 working root (lance copies): {m5_root}")
    print(f"[M5] iceberg warehouse (CTAS dests): {iceberg_warehouse}")

    by_table = {}
    for rec in m1["records"]:
        if not rec.get("ok"):
            continue
        by_table.setdefault(rec["table"], {})[rec["format"]] = rec

    if DELETE_TABLE not in by_table:
        raise SystemExit(f"Table {DELETE_TABLE!r} missing from M1 manifest")
    fmts = by_table[DELETE_TABLE]
    if "lance_2.2" not in fmts or "iceberg_v2" not in fmts:
        raise SystemExit(
            f"Need both lance_2.2 and iceberg_v2 for {DELETE_TABLE}, "
            f"have {list(fmts)}")
    lance_src = fmts["lance_2.2"]["uri"]
    iceberg_src_fqn = fmts["iceberg_v2"]["fqn"]
    iceberg_src_data_uri = fmts["iceberg_v2"]["data_uri"]

    iceberg_meta_src = find_iceberg_metadata_uri(args.region,
                                                 iceberg_src_data_uri)
    storage_options = {"region": args.region}
    ds_probe = lance.dataset(lance_src, storage_options=storage_options)
    n_total = ds_probe.count_rows()
    print(f"[M5] n_total={n_total}")

    print("[M5] calibrating delete predicates ...")
    picks = calibrate_delete_K(iceberg_meta_src, DELETE_COLUMN,
                               n_total, DELETE_FRACTIONS)
    for label, p in picks.items():
        marker = "" if p["feasible"] else "  !! INFEASIBLE"
        print(f"  {label:>5}: {DELETE_COLUMN} <= {p['predicate_k']} "
              f"-> {p['delete_rows']:,} rows "
              f"(actual {p['actual_fraction']:.6f}){marker}")

    print("[M5] starting Spark session (for Iceberg CTAS + DELETE) ...")
    spark = make_spark_session(args.iceberg_jar, iceberg_warehouse,
                               args.region)
    spark.sparkContext.setLogLevel("WARN")
    print(f"[M5]   Spark version: {spark.version}")

    results = []
    ctas_tables = []
    try:
        for label, pick in picks.items():
            if not pick["feasible"]:
                print(f"\n[M5] skipping {label}: infeasible target")
                results.append({"label": label, "skipped": "infeasible_target",
                                **pick})
                continue

            k_value = pick["predicate_k"]
            print(f"\n[M5] === delete_fraction={label} "
                  f"(k={k_value}, {pick['delete_rows']:,} rows) ===")

            safe_label = label.replace("%", "pct").replace(".", "p")
            lance_work = f"{m5_root}/{label}/store_sales.lance"
            dst_fqn = f"ice.{args.iceberg_namespace}.store_sales_m5_{safe_label}"
            ctas_tables.append(dst_fqn)

            print(f"[M5] s3 rm+cp lance  {lance_src} -> {lance_work}")
            s3_rm_prefix(lance_work, args.region)
            cp_lance_s = s3_cp_recursive(lance_src, lance_work, args.region)
            print(f"  lance copy in {cp_lance_s}s")

            print(f"[M5] iceberg CTAS  {iceberg_src_fqn} -> {dst_fqn}")
            ctas_s = iceberg_ctas_copy(spark, iceberg_src_fqn, dst_fqn)
            print(f"  CTAS done in {ctas_s}s")

            dst_data_uri = (f"{iceberg_warehouse.rstrip('/')}/"
                            f"{args.iceberg_namespace}/store_sales_m5_{safe_label}")
            dst_meta = find_iceberg_metadata_uri(args.region, dst_data_uri)

            assert_iceberg_location_matches(dst_meta, dst_data_uri,
                                            args.region)
            print(f"  iceberg metadata location guard OK: {dst_data_uri}")

            sel_rec = {
                "label": label,
                "target_fraction": pick["target_fraction"],
                "actual_fraction": pick["actual_fraction"],
                "predicate_k": int(k_value),
                "delete_rows_expected": pick["delete_rows"],
                "copy_seconds": {"lance": cp_lance_s, "iceberg_ctas": ctas_s},
                "dst_fqn": dst_fqn,
                "dst_data_uri": dst_data_uri,
                "formats": {},
            }

            lance_stats = {}
            try:
                lance_stats["pre_size_bytes"] = du_s3(lance_work, args.region)
                delete_s = lance_delete(lance_work, storage_options,
                                        DELETE_COLUMN, k_value)
                lance_stats["delete_seconds"] = delete_s
                lance_stats["post_size_bytes"] = du_s3(lance_work, args.region)
                ds_after = lance.dataset(lance_work,
                                         storage_options=storage_options)
                lance_stats["rows_after_delete"] = ds_after.count_rows()
                print(f"  [lance_2.2] delete={delete_s}s  "
                      f"rows_after={lance_stats['rows_after_delete']}  "
                      f"post_size_mb="
                      f"{(lance_stats['post_size_bytes'] or 0) / 1e6:.1f}")
                scan_stats = timed_materialized(
                    lambda u=lance_work: lance_scan_action(
                        u, storage_options, PROJECTED_COLS))
                lance_stats["post_delete_scan"] = scan_stats
                if (scan_stats.get("rows_returned") is not None
                        and scan_stats["rows_returned"]
                            != lance_stats["rows_after_delete"]):
                    print(f"  !! lance rows_after_delete mismatch: "
                          f"count_rows={lance_stats['rows_after_delete']} "
                          f"scan={scan_stats['rows_returned']}")
                print(f"  [lance_2.2] post_scan p50={scan_stats['median_ms']:>9.2f} "
                      f"ms  rows={scan_stats.get('rows_returned')}")
            except KeyboardInterrupt:
                raise
            except BaseException as e:
                if isinstance(e, (SystemExit, GeneratorExit)):
                    raise
                lance_stats["error"] = f"{type(e).__name__}: {e}"[:400]
                print(f"  lance FAILED: {lance_stats['error']}")
            sel_rec["formats"]["lance_2.2"] = lance_stats

            iceberg_stats = {}
            try:
                iceberg_stats["pre_size_bytes"] = du_s3(dst_data_uri,
                                                       args.region)
                delete_s = iceberg_spark_delete(spark, dst_fqn,
                                                DELETE_COLUMN, k_value)
                iceberg_stats["delete_seconds"] = delete_s
                iceberg_stats["post_size_bytes"] = du_s3(dst_data_uri,
                                                         args.region)
                rows_after = spark.sql(
                    f"SELECT count(*) FROM {dst_fqn}").collect()[0][0]
                iceberg_stats["rows_after_delete"] = rows_after
                delete_files = spark.sql(
                    f"SELECT count(*) FROM {dst_fqn}.delete_files"
                ).collect()[0][0]
                iceberg_stats["delete_files"] = delete_files
                print(f"  [iceberg_v2] delete={delete_s}s  "
                      f"rows_after={rows_after}  "
                      f"delete_files={delete_files}  "
                      f"post_size_mb="
                      f"{(iceberg_stats['post_size_bytes'] or 0) / 1e6:.1f}")
                iceberg_meta_after = find_iceberg_metadata_uri(
                    args.region, dst_data_uri)
                scan_stats = timed_materialized(
                    lambda m=iceberg_meta_after: iceberg_scan_action(
                        m, PROJECTED_COLS))
                iceberg_stats["post_delete_scan"] = scan_stats
                if (scan_stats.get("rows_returned") is not None
                        and scan_stats["rows_returned"] != rows_after):
                    print(f"  !! iceberg rows_after_delete mismatch: "
                          f"count={rows_after} scan={scan_stats['rows_returned']}")
                print(f"  [iceberg_v2] post_scan p50={scan_stats['median_ms']:>9.2f} "
                      f"ms  rows={scan_stats.get('rows_returned')}")
            except KeyboardInterrupt:
                raise
            except BaseException as e:
                if isinstance(e, (SystemExit, GeneratorExit)):
                    raise
                iceberg_stats["error"] = f"{type(e).__name__}: {e}"[:400]
                print(f"  iceberg FAILED: {iceberg_stats['error']}")
            sel_rec["formats"]["iceberg_v2"] = iceberg_stats

            results.append(sel_rec)
    finally:
        if not args.keep_working_copies:
            print(f"\n[M5] dropping CTAS iceberg tables: {ctas_tables}")
            for fqn in ctas_tables:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
                except BaseException as e:
                    if isinstance(e, (KeyboardInterrupt, SystemExit,
                                      GeneratorExit)):
                        raise
                    print(f"  drop {fqn} warn: {e}")
            print(f"[M5] cleaning up lance working prefix {m5_root}")
            s3_rm_prefix(m5_root, args.region)
        spark.stop()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + ".tmp"
    with open(tmp, "w") as f:
        json.dump({
            "scale": scale,
            "region": args.region,
            "m1_manifest": os.path.abspath(args.m1_manifest),
            "m5_work_root": m5_root,
            "delete_column": DELETE_COLUMN,
            "delete_table": DELETE_TABLE,
            "delete_fractions": DELETE_FRACTIONS,
            "projected_columns": PROJECTED_COLS,
            "scan_column": SCAN_COLUMN,
            "warmup": WARMUP,
            "rounds": ROUNDS,
            "engine": "lance: pylance native; iceberg: Spark write (MoR) + "
                      "pyiceberg read",
            "n_total_source": n_total,
            "lance_version": lance.__version__,
            "results": results,
        }, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, args.out)
    print(f"\n[M5] Saved: {args.out}")

    print("\n=== M5 delete+read summary ===")
    print(f"{'frac':<8} {'delete_s_lance':>16} {'delete_s_ice':>14} "
          f"{'scan_p50_lance':>16} {'scan_p50_ice':>14} {'scan_ratio':>12}")
    for r in results:
        if r.get("skipped"):
            print(f"  {r['label']:<6} {'SKIPPED':>16}  {r['skipped']}")
            continue
        lf = r["formats"].get("lance_2.2", {})
        icef = r["formats"].get("iceberg_v2", {})
        ld = lf.get("delete_seconds")
        icd = icef.get("delete_seconds")
        ls = lf.get("post_delete_scan", {}).get("median_ms")
        iss = icef.get("post_delete_scan", {}).get("median_ms")
        ratio = (f"{ls/iss:.2f}x" if ls is not None and iss and iss > 0
                 else "--")
        print(f"  {r['label']:<6} "
              f"{str(ld):>16} {str(icd):>14} "
              f"{str(ls):>16} {str(iss):>14} {ratio:>12}")


if __name__ == "__main__":
    main()
