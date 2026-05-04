"""M6: small-files pathology + compaction cost.

Simulates the classic Iceberg small-files problem by appending many
small batches to a fresh table copy, then measures the effect on read
latency before and after compaction. Lance has an analogous problem at
the fragment level; `ds.optimize.compact_files()` is its equivalent of
Iceberg's `rewrite_data_files`.

Workflow:
  1. Create a small baseline table (same schema as M1 store_sales but
     only the first BASELINE_ROWS rows, via pyarrow.parquet slice).
       - Lance:   one fragment via pylance write_dataset.
       - Iceberg: one data file via Spark CTAS from a temp view.
  2. Append N_APPENDS small batches (default 50) of APPEND_ROWS rows
     each. Each append is committed independently so the manifest
     accumulates N_APPENDS snapshots (Iceberg) / manifest versions
     (Lance), and the data directory accumulates N_APPENDS + 1 files.
  3. Measure pre-compaction read latency (full_scan via neutral
     PyArrow reads: pylance + pyiceberg).
  4. Run compaction:
       - Lance:   ds.optimize.compact_files() with a target_rows_per_file
                  chosen to roll N_APPENDS + 1 files into ~1 file.
       - Iceberg: Spark SQL `CALL system.rewrite_data_files`.
     Record wall time + post-state file counts.
  5. Measure post-compaction read latency.

Why Spark write / pyiceberg read for Iceberg (same as M5 reasoning):
pyiceberg 0.10 cannot commit (NoopCatalog) and falls back to CoW;
lance-spark 0.0.15 DSv2 read is broken on Spark 3.5.5. The hybrid path
(Spark writes, pyiceberg reads) is what actually works end-to-end and
matches M5's locked-in approach.

Output: results/M6_compact_sf<N>.json.
"""
import argparse
import gc
import json
import os
import statistics
import subprocess
import time

import lance
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
from pyiceberg.table import StaticTable


WARMUP = 3
ROUNDS = 7
BASELINE_ROWS = 100_000
APPEND_ROWS = 10_000
N_APPENDS = 50
FILTER_TABLE = "store_sales"
PROJECTED_COLS = ["ss_item_sk", "ss_customer_sk", "ss_quantity",
                  "ss_sales_price"]


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


def load_source_batches(parquet_uri, total_rows_needed, batch_rows):
    """Stream rows from the M0 Parquet source, yielding
    `batch_rows`-sized RecordBatches until `total_rows_needed`.
    """
    filesystem, path = fs.FileSystem.from_uri(parquet_uri)
    pf = pq.ParquetFile(path, filesystem=filesystem)
    yielded = 0
    for batch in pf.iter_batches(batch_size=batch_rows):
        if yielded >= total_rows_needed:
            return
        if yielded + batch.num_rows > total_rows_needed:
            batch = batch.slice(0, total_rows_needed - yielded)
        yield batch
        yielded += batch.num_rows


def pyarrow_table_from_batches(batches, schema):
    return pa.Table.from_batches(list(batches), schema=schema)


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


def make_spark_session(iceberg_jar, warehouse_uri, region):
    from pyspark.sql import SparkSession
    if not os.path.exists(iceberg_jar):
        raise FileNotFoundError(f"Iceberg jar missing: {iceberg_jar}")
    return (
        SparkSession.builder
        .appName("M6-compaction")
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


def iceberg_create_empty(spark, fqn, src_fqn):
    """Create a fresh small-files target table with identical schema to
    src_fqn, MoR properties, and NO data -- we will INSERT in a loop.
    """
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")
    spark.sql(
        f"CREATE TABLE {fqn} USING iceberg "
        f"TBLPROPERTIES ("
        f"'format-version'='2', "
        f"'write.format.default'='parquet', "
        f"'write.parquet.compression-codec'='zstd', "
        f"'write.parquet.compression-level'='3' "
        f") AS SELECT * FROM {src_fqn} WHERE 1=0"
    )


def spark_table_from_arrow(spark, tbl, hdfs_staging_prefix, batch_name):
    """Hand a pyarrow.Table to Spark via a temp Parquet on HDFS. Going
    through `createDataFrame` + pandas silently promotes BIGINT NULLs to
    DOUBLE NaN and then Spark rejects the insert back into BIGINT
    columns with CAST_OVERFLOW (verified on EMR 7.10).

    HDFS staging (not local /tmp + file://) is required because EMR
    Spark runs tasks in YARN cluster/container workers that cannot see
    the driver's local /tmp. HDFS is visible cluster-wide by default.
    """
    local_tmp = f"/tmp/{batch_name}.parquet"
    pq.write_table(tbl, local_tmp, compression="snappy")
    hdfs_path = f"{hdfs_staging_prefix.rstrip('/')}/{batch_name}.parquet"
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_tmp, hdfs_path],
                   check=True, capture_output=True, text=True, timeout=600)
    os.remove(local_tmp)
    return spark.read.parquet(hdfs_path)


def hdfs_rm_prefix(hdfs_prefix, timeout_s=300):
    try:
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_prefix],
                       check=True, capture_output=True, text=True,
                       timeout=timeout_s)
    except subprocess.CalledProcessError as e:
        print(f"  hdfs rm warn for {hdfs_prefix}: rc={e.returncode} "
              f"stderr={e.stderr[:200]}")


def count_lance_fragments(uri, storage_options):
    ds = lance.dataset(uri, storage_options=storage_options)
    return len(ds.get_fragments())


def count_iceberg_data_files(spark, fqn):
    return spark.sql(
        f"SELECT count(*) FROM {fqn}.files "
        f"WHERE content = 0").collect()[0][0]


def count_iceberg_snapshots(spark, fqn):
    return spark.sql(f"SELECT count(*) FROM {fqn}.snapshots").collect()[0][0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m1-manifest", required=True)
    ap.add_argument("--m0-manifest", required=True,
                    help="needed to get the raw Parquet source for "
                         "append batches")
    ap.add_argument("--region", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--n-appends", type=int, default=N_APPENDS)
    ap.add_argument("--append-rows", type=int, default=APPEND_ROWS)
    ap.add_argument("--baseline-rows", type=int, default=BASELINE_ROWS)
    ap.add_argument("--keep-working-copies", action="store_true")
    ap.add_argument("--iceberg-jar",
                    default="/usr/share/aws/iceberg/lib/"
                            "iceberg-spark-runtime-3.5_2.12-1.8.1-amzn-0.jar")
    ap.add_argument("--iceberg-namespace", default="tpcds")
    args = ap.parse_args()

    with open(args.m1_manifest) as f:
        m1 = json.load(f)
    with open(args.m0_manifest) as f:
        m0 = json.load(f)
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
                    f"M6_compact_sf{scale}.json")

    s3_base = m1["s3_base"]
    iceberg_warehouse = m1["iceberg_warehouse"]
    m6_root = f"{s3_base.rstrip('/')}/M6/M6_work_{run_id}"
    storage_options = {"region": args.region}

    m0_table = next((t for t in m0["tables"] if t["table"] == FILTER_TABLE),
                    None)
    if m0_table is None:
        raise SystemExit(f"Table {FILTER_TABLE!r} not in M0 manifest")
    source_parquet_uri = m0_table["s3_uri"]

    n_required = args.baseline_rows + args.n_appends * args.append_rows
    print(f"[M6] m1={args.m1_manifest}  m0={args.m0_manifest}")
    print(f"[M6] scale=sf{scale}  region={args.region}")
    print(f"[M6] baseline_rows={args.baseline_rows}  "
          f"n_appends={args.n_appends}  append_rows={args.append_rows}")
    print(f"[M6] total rows needed from source: {n_required}")
    print(f"[M6] m6_root (lance): {m6_root}")
    print(f"[M6] iceberg ns: {args.iceberg_namespace}")

    filesystem, path = fs.FileSystem.from_uri(source_parquet_uri)
    pf = pq.ParquetFile(path, filesystem=filesystem)
    schema = pf.schema_arrow
    if pf.metadata.num_rows < n_required:
        raise SystemExit(
            f"source has {pf.metadata.num_rows} rows, need {n_required}")

    print(f"[M6] pre-loading {n_required} rows from {source_parquet_uri}")
    all_batches = list(load_source_batches(source_parquet_uri, n_required,
                                           min(args.append_rows, 100_000)))

    def take_rows(start, n):
        collected = []
        got = 0
        for batch in all_batches:
            if got + batch.num_rows <= start:
                got += batch.num_rows
                continue
            local_start = max(0, start - got)
            local_end = min(batch.num_rows, start + n - got)
            if local_end > local_start:
                collected.append(batch.slice(local_start,
                                             local_end - local_start))
            got_after = got + batch.num_rows
            if got_after >= start + n:
                break
            got = got_after
        return pa.Table.from_batches(collected, schema=schema)

    lance_uri = f"{m6_root}/store_sales.lance"
    iceberg_fqn = f"ice.{args.iceberg_namespace}.store_sales_m6"
    iceberg_data_uri = (f"{iceberg_warehouse.rstrip('/')}/"
                        f"{args.iceberg_namespace}/store_sales_m6")

    print(f"[M6] cleaning any prior M6 state at {lance_uri}")
    s3_rm_prefix(lance_uri, args.region)

    print("[M6] starting Spark session (Iceberg CTAS + INSERTs + OPTIMIZE)")
    spark = make_spark_session(args.iceberg_jar, iceberg_warehouse,
                               args.region)
    spark.sparkContext.setLogLevel("WARN")
    print(f"[M6]   Spark version: {spark.version}")

    src_fqn = None
    for rec in m1["records"]:
        if (rec.get("ok") and rec["table"] == FILTER_TABLE
                and rec["format"] == "iceberg_v2"):
            src_fqn = rec["fqn"]
            break
    if src_fqn is None:
        raise SystemExit(f"M1 manifest missing iceberg_v2 for {FILTER_TABLE}")

    out_rec = {
        "scale": scale,
        "region": args.region,
        "m1_manifest": os.path.abspath(args.m1_manifest),
        "m0_manifest": os.path.abspath(args.m0_manifest),
        "baseline_rows": args.baseline_rows,
        "n_appends": args.n_appends,
        "append_rows": args.append_rows,
        "warmup": WARMUP,
        "rounds": ROUNDS,
        "engine": ("lance: pylance native; iceberg: Spark write + "
                   "pyiceberg read"),
        "lance_version": lance.__version__,
    }

    hdfs_staging = f"/tmp/m6_stage_{run_id}"
    print(f"[M6] HDFS staging prefix: {hdfs_staging}")
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_staging],
                       check=True, capture_output=True, text=True,
                       timeout=60)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"failed to create HDFS staging dir {hdfs_staging}: "
            f"{e.stderr[:200]}") from e

    try:
        print("\n[M6] === PHASE 1: baseline table write ===")
        baseline_tbl = take_rows(0, args.baseline_rows)
        t0 = time.perf_counter()
        lance.write_dataset(baseline_tbl, lance_uri,
                            mode="overwrite",
                            data_storage_version="2.2",
                            storage_options=storage_options)
        lance_baseline_s = round(time.perf_counter() - t0, 3)
        print(f"  lance baseline write {lance_baseline_s}s")

        iceberg_create_empty(spark, iceberg_fqn, src_fqn)
        baseline_df = spark_table_from_arrow(spark, baseline_tbl,
                                             hdfs_staging, "baseline")
        t0 = time.perf_counter()
        baseline_df.repartition(1).writeTo(iceberg_fqn).append()
        iceberg_baseline_s = round(time.perf_counter() - t0, 3)
        print(f"  iceberg baseline append {iceberg_baseline_s}s")

        out_rec["baseline_write_seconds"] = {"lance": lance_baseline_s,
                                             "iceberg": iceberg_baseline_s}

        print(f"\n[M6] === PHASE 2: {args.n_appends} small appends ===")
        lance_append_total_s = 0.0
        iceberg_append_total_s = 0.0
        for i in range(args.n_appends):
            start = args.baseline_rows + i * args.append_rows
            batch_tbl = take_rows(start, args.append_rows)
            t0 = time.perf_counter()
            lance.write_dataset(batch_tbl, lance_uri,
                                mode="append",
                                data_storage_version="2.2",
                                storage_options=storage_options)
            lance_append_total_s += time.perf_counter() - t0

            df = spark_table_from_arrow(spark, batch_tbl, hdfs_staging,
                                        f"append_{i:04d}")
            t0 = time.perf_counter()
            df.repartition(1).writeTo(iceberg_fqn).append()
            iceberg_append_total_s += time.perf_counter() - t0

            if (i + 1) % 10 == 0 or i == args.n_appends - 1:
                print(f"  append {i+1}/{args.n_appends}  "
                      f"cumulative lance={lance_append_total_s:.1f}s  "
                      f"iceberg={iceberg_append_total_s:.1f}s")

        out_rec["append_total_seconds"] = {
            "lance": round(lance_append_total_s, 3),
            "iceberg": round(iceberg_append_total_s, 3)}

        lance_frag_pre = count_lance_fragments(lance_uri, storage_options)
        iceberg_files_pre = count_iceberg_data_files(spark, iceberg_fqn)
        iceberg_snaps_pre = count_iceberg_snapshots(spark, iceberg_fqn)
        lance_size_pre = du_s3(lance_uri, args.region)
        iceberg_size_pre = du_s3(iceberg_data_uri, args.region)
        print("\n[M6] pre-compact state:")
        print(f"  lance: fragments={lance_frag_pre}  "
              f"size_mb={(lance_size_pre or 0)/1e6:.1f}")
        print(f"  iceberg: data_files={iceberg_files_pre}  "
              f"snapshots={iceberg_snaps_pre}  "
              f"size_mb={(iceberg_size_pre or 0)/1e6:.1f}")
        out_rec["pre_compact"] = {
            "lance_fragments": lance_frag_pre,
            "iceberg_data_files": iceberg_files_pre,
            "iceberg_snapshots": iceberg_snaps_pre,
            "lance_size_bytes": lance_size_pre,
            "iceberg_size_bytes": iceberg_size_pre,
        }

        print("\n[M6] === PHASE 3: pre-compact read latency ===")
        out_rec["pre_compact_scan"] = {}
        try:
            stats = timed_materialized(
                lambda: lance_scan_action(lance_uri, storage_options,
                                          PROJECTED_COLS))
            out_rec["pre_compact_scan"]["lance_2.2"] = stats
            print(f"  lance pre-compact p50={stats['median_ms']:>9.2f} ms  "
                  f"rows={stats.get('rows_returned')}")
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            out_rec["pre_compact_scan"]["lance_2.2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  lance pre-compact FAILED: "
                  f"{out_rec['pre_compact_scan']['lance_2.2']['error']}")

        iceberg_meta_pre = find_iceberg_metadata_uri(args.region,
                                                    iceberg_data_uri)
        try:
            stats = timed_materialized(
                lambda m=iceberg_meta_pre: iceberg_scan_action(
                    m, PROJECTED_COLS))
            out_rec["pre_compact_scan"]["iceberg_v2"] = stats
            print(f"  iceberg pre-compact p50={stats['median_ms']:>9.2f} ms  "
                  f"rows={stats.get('rows_returned')}")
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            out_rec["pre_compact_scan"]["iceberg_v2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  iceberg pre-compact FAILED: "
                  f"{out_rec['pre_compact_scan']['iceberg_v2']['error']}")

        print("\n[M6] === PHASE 4: compaction ===")
        t0 = time.perf_counter()
        ds = lance.dataset(lance_uri, storage_options=storage_options)
        ds.optimize.compact_files()
        lance_compact_s = round(time.perf_counter() - t0, 3)
        print(f"  lance compact_files: {lance_compact_s}s")

        t0 = time.perf_counter()
        spark.sql(f"CALL ice.system.rewrite_data_files("
                  f"table => '{args.iceberg_namespace}.store_sales_m6')")
        iceberg_compact_s = round(time.perf_counter() - t0, 3)
        print(f"  iceberg rewrite_data_files: {iceberg_compact_s}s")

        out_rec["compact_seconds"] = {"lance": lance_compact_s,
                                      "iceberg": iceberg_compact_s}

        lance_frag_post = count_lance_fragments(lance_uri, storage_options)
        iceberg_files_post = count_iceberg_data_files(spark, iceberg_fqn)
        iceberg_snaps_post = count_iceberg_snapshots(spark, iceberg_fqn)
        lance_size_post = du_s3(lance_uri, args.region)
        iceberg_size_post = du_s3(iceberg_data_uri, args.region)
        print("\n[M6] post-compact state:")
        print(f"  lance: fragments={lance_frag_post}  "
              f"size_mb={(lance_size_post or 0)/1e6:.1f}")
        print(f"  iceberg: data_files={iceberg_files_post}  "
              f"snapshots={iceberg_snaps_post}  "
              f"size_mb={(iceberg_size_post or 0)/1e6:.1f}")
        out_rec["post_compact"] = {
            "lance_fragments": lance_frag_post,
            "iceberg_data_files": iceberg_files_post,
            "iceberg_snapshots": iceberg_snaps_post,
            "lance_size_bytes": lance_size_post,
            "iceberg_size_bytes": iceberg_size_post,
        }

        print("\n[M6] === PHASE 5: post-compact read latency ===")
        out_rec["post_compact_scan"] = {}
        try:
            stats = timed_materialized(
                lambda: lance_scan_action(lance_uri, storage_options,
                                          PROJECTED_COLS))
            out_rec["post_compact_scan"]["lance_2.2"] = stats
            print(f"  lance post-compact p50={stats['median_ms']:>9.2f} ms  "
                  f"rows={stats.get('rows_returned')}")
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            out_rec["post_compact_scan"]["lance_2.2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  lance post-compact FAILED: "
                  f"{out_rec['post_compact_scan']['lance_2.2']['error']}")

        iceberg_meta_post = find_iceberg_metadata_uri(args.region,
                                                     iceberg_data_uri)
        try:
            stats = timed_materialized(
                lambda m=iceberg_meta_post: iceberg_scan_action(
                    m, PROJECTED_COLS))
            out_rec["post_compact_scan"]["iceberg_v2"] = stats
            print(f"  iceberg post-compact p50={stats['median_ms']:>9.2f} ms  "
                  f"rows={stats.get('rows_returned')}")
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            out_rec["post_compact_scan"]["iceberg_v2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  iceberg post-compact FAILED: "
                  f"{out_rec['post_compact_scan']['iceberg_v2']['error']}")
    finally:
        if not args.keep_working_copies:
            print(f"\n[M6] cleanup: dropping {iceberg_fqn}")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {iceberg_fqn}")
            except BaseException as e:
                if isinstance(e, (KeyboardInterrupt, SystemExit,
                                  GeneratorExit)):
                    raise
                print(f"  drop warn: {e}")
            print(f"[M6] cleanup: removing {m6_root}")
            s3_rm_prefix(m6_root, args.region)
            print(f"[M6] cleanup: removing HDFS staging {hdfs_staging}")
            hdfs_rm_prefix(hdfs_staging)
        spark.stop()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + ".tmp"
    with open(tmp, "w") as f:
        json.dump(out_rec, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, args.out)
    print(f"\n[M6] Saved: {args.out}")

    print("\n=== M6 compaction summary ===")
    pre = out_rec.get("pre_compact", {})
    post = out_rec.get("post_compact", {})
    cs = out_rec.get("compact_seconds", {})
    print(f"  lance:   frags {pre.get('lance_fragments')} -> "
          f"{post.get('lance_fragments')}  "
          f"size_mb {(pre.get('lance_size_bytes') or 0)/1e6:.1f} -> "
          f"{(post.get('lance_size_bytes') or 0)/1e6:.1f}  "
          f"compact_s {cs.get('lance')}")
    print(f"  iceberg: files {pre.get('iceberg_data_files')} -> "
          f"{post.get('iceberg_data_files')}  "
          f"size_mb {(pre.get('iceberg_size_bytes') or 0)/1e6:.1f} -> "
          f"{(post.get('iceberg_size_bytes') or 0)/1e6:.1f}  "
          f"compact_s {cs.get('iceberg')}")
    for phase in ("pre_compact_scan", "post_compact_scan"):
        p = out_rec.get(phase, {})
        lp = p.get("lance_2.2", {}).get("median_ms")
        ip = p.get("iceberg_v2", {}).get("median_ms")
        lp_s = f"{lp:.2f}" if lp is not None else "--"
        ip_s = f"{ip:.2f}" if ip is not None else "--"
        ratio = (f"{lp/ip:.2f}x" if lp is not None and ip and ip > 0
                 else "--")
        print(f"  {phase}: lance={lp_s}  iceberg={ip_s}  ratio={ratio}")


if __name__ == "__main__":
    main()
