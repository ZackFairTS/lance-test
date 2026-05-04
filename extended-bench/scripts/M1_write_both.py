"""M1: write the M0 Parquet baseline into both Lance v2.2 and Iceberg v2 on S3.

Fairness contract (locked here; consumed by M2..M6):
  - Same source data (M0 Parquet on S3).
  - Same compression: zstd level 3 on both sides.
  - Same target file size: ~128 MiB (Iceberg via write-target-file-size-bytes,
    Lance via max_bytes_per_file; Lance also caps max_rows_per_file so
    small tables still split predictably).
  - Iceberg format-version=2 (MoR). HadoopCatalog, path-based on S3.
    single-writer only -- multi-writer S3+HadoopCatalog is not atomic.
  - Lance v2.2 written via pylance single-node. lance-spark 0.0.15 cannot
    request data_storage_version (L2 smoke confirmed). One fragment per
    ~128 MiB so fragment count ~= Iceberg file count.

Output layout:
  s3://<base>/M/tpcds_sf<N>/lance/<table>.lance
  s3://<base>/M/tpcds_sf<N>/iceberg/<namespace>/<table>/
    (Iceberg metadata at <warehouse>/<namespace>/<table>/metadata/)
  results/M1_manifest_sf<N>.json  -> consumed by M2_size / M3_scan / ...
"""
import argparse
import json
import os
import subprocess
import time

import lance
import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq

TARGET_FILE_BYTES = 128 * 1024 * 1024
LANCE_EXPECTED_STORAGE_VERSION = "2.2"


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
    """Recursively delete an S3 prefix before a write, to avoid orphan files
    from a prior failed run (pylance's overwrite does not GC old fragments;
    HadoopCatalog DROP TABLE does not delete data files on S3).
    """
    if not s3_uri.startswith("s3://"):
        return
    cmd = ["aws", "s3", "rm", s3_uri, "--recursive", "--region", region,
           "--only-show-errors"]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True,
                       timeout=timeout_s)
    except subprocess.CalledProcessError as e:
        print(f"  s3 rm warn for {s3_uri}: rc={e.returncode} "
              f"stderr={e.stderr[:200]}")


def write_lance_from_parquet(source_parquet_uri, target_uri, storage_options,
                             max_bytes_per_file, max_rows_per_file):
    """Stream Parquet -> Lance v2.2 without materializing the whole table.
    Uses fs.FileSystem.from_uri to resolve S3 URIs explicitly; pyarrow 20's
    implicit s3:// handling relies on fsspec being installed, which is
    not guaranteed on all EMR images.
    """
    filesystem, path = fs.FileSystem.from_uri(source_parquet_uri)
    pf = pq.ParquetFile(path, filesystem=filesystem)
    batches = pf.iter_batches(batch_size=131072)
    reader = iter(batches)
    try:
        first = next(reader)
    except StopIteration:
        raise RuntimeError(f"empty parquet source: {source_parquet_uri}")
    schema = first.schema

    def _rb_iter():
        yield first
        for b in reader:
            yield b

    rb_reader = pa.RecordBatchReader.from_batches(schema, _rb_iter())

    lance.write_dataset(
        rb_reader, target_uri,
        mode="overwrite",
        data_storage_version=LANCE_EXPECTED_STORAGE_VERSION,
        storage_options=storage_options,
        max_bytes_per_file=max_bytes_per_file,
        max_rows_per_file=max_rows_per_file,
    )


def verify_lance(uri, storage_options):
    ds = lance.dataset(uri, storage_options=storage_options)
    out = {
        "rows": ds.count_rows(),
        "storage_version": ds.data_storage_version,
        "manifest_version": ds.version,
        "num_fragments": len(ds.get_fragments()),
    }
    if out["storage_version"] != LANCE_EXPECTED_STORAGE_VERSION:
        raise RuntimeError(
            f"Lance storage_version mismatch: expected "
            f"{LANCE_EXPECTED_STORAGE_VERSION}, got {out['storage_version']}")
    return out


def make_spark_session(iceberg_jar, warehouse_uri, region):
    from pyspark.sql import SparkSession
    if not os.path.exists(iceberg_jar):
        raise FileNotFoundError(f"Iceberg jar missing: {iceberg_jar}")
    return (
        SparkSession.builder
        .appName("M1-write-iceberg")
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


ICEBERG_TBL_PROPS = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.parquet.compression-level": "3",
    "write.target-file-size-bytes": str(TARGET_FILE_BYTES),
}


def write_iceberg_from_parquet(spark, source_parquet_uri, namespace, table,
                               target_file_bytes, source_bytes):
    """Write an Iceberg v2 table from a Parquet source, applying the
    fairness-contract table properties at create time.

    `.using("iceberg")` is deliberately NOT used: with `ice.<ns>.<tbl>` already
    catalog-qualified, adding `.using(...)` has been observed to silently drop
    tableProperty() values on Spark 3.5 / Iceberg 1.8 (L2 bug class: conflicting
    DSv2 hints). We also repartition the input so write parallelism matches
    what target-file-size implies: a single Spark task can't emit files smaller
    than its own output.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS ice.{namespace}")
    fqn = f"ice.{namespace}.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {fqn}")

    props = dict(ICEBERG_TBL_PROPS)
    props["write.target-file-size-bytes"] = str(target_file_bytes)

    n_parts = max(1, (source_bytes + target_file_bytes - 1) // target_file_bytes)
    df = spark.read.parquet(source_parquet_uri).repartition(int(n_parts))

    builder = df.writeTo(fqn)
    for k, v in props.items():
        builder = builder.tableProperty(k, v)
    builder.create()
    return {"fqn": fqn, "applied_props": props, "n_write_partitions": int(n_parts)}


def verify_iceberg(spark, namespace, table):
    fqn = f"ice.{namespace}.{table}"
    rows = spark.sql(f"SELECT count(*) FROM {fqn}").collect()[0][0]
    snapshots = spark.sql(
        f"SELECT count(*) FROM {fqn}.snapshots").collect()[0][0]
    files = spark.sql(
        f"SELECT count(*) FROM {fqn}.files").collect()[0][0]
    props_rows = spark.sql(f"SHOW TBLPROPERTIES {fqn}").collect()
    persisted = {r["key"]: r["value"] for r in props_rows}

    required = {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "3",
    }
    for k, want in required.items():
        got = persisted.get(k)
        if got != want:
            raise RuntimeError(
                f"Iceberg table property {k}={got!r}, expected {want!r} "
                f"(all persisted: {persisted})")
    return {"rows": rows, "snapshots": snapshots, "data_files": files,
            "persisted_props": persisted}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m0-manifest", required=True,
                    help="path to M0_manifest_sf<N>.json")
    ap.add_argument("--s3-base", default=None,
                    help="default: derived from M0 manifest s3_root")
    ap.add_argument("--region", default=None,
                    help="default: read from M0 manifest or run.env")
    ap.add_argument("--iceberg-jar",
                    default="/usr/share/aws/iceberg/lib/"
                            "iceberg-spark-runtime-3.5_2.12-1.8.1-amzn-0.jar")
    ap.add_argument("--iceberg-namespace", default="tpcds",
                    help="Iceberg namespace under the hadoop warehouse")
    ap.add_argument("--target-file-bytes", type=int, default=TARGET_FILE_BYTES,
                    help="target file size for both Lance max_bytes_per_file "
                         "and Iceberg write.target-file-size-bytes")
    ap.add_argument("--max-rows-per-file", type=int, default=10_000_000,
                    help="Lance max_rows_per_file safety cap")
    ap.add_argument("--tables", nargs="*", default=None,
                    help="restrict to these tables (default: all in M0 manifest)")
    ap.add_argument("--formats", nargs="+",
                    default=["lance_2.2", "iceberg_v2"],
                    choices=["lance_2.2", "iceberg_v2"])
    ap.add_argument("--manifest", default=None,
                    help="default: derived from M0 manifest name (M1_... )")
    args = ap.parse_args()

    with open(args.m0_manifest) as f:
        m0 = json.load(f)

    env = parse_run_env()
    if args.region is None:
        args.region = (os.environ.get("AWS_REGION")
                       or m0.get("region") or env.get("AWS_REGION"))
    if not args.region:
        raise SystemExit("AWS_REGION not found.")
    os.environ["AWS_REGION"] = args.region

    m0_s3_root = m0["s3_root"].rstrip("/")
    if args.s3_base is None:
        args.s3_base = m0_s3_root
    s3_base = args.s3_base.rstrip("/")

    scale = m0.get("scale")
    iceberg_warehouse = f"{s3_base}/iceberg"
    lance_root = f"{s3_base}/lance"
    storage_options = {"region": args.region}

    if args.manifest is None:
        args.manifest = (f"/home/hadoop/lance-extended-bench/results/"
                         f"M1_manifest_sf{scale}.json")

    m0_tables = {t["table"]: t for t in m0["tables"]}
    if args.tables:
        missing = set(args.tables) - set(m0_tables)
        if missing:
            raise SystemExit(f"Tables not in M0 manifest: {sorted(missing)}")
        table_names = args.tables
    else:
        table_names = list(m0_tables.keys())

    print(f"[M1] M0 manifest: {args.m0_manifest}")
    print(f"[M1] scale=sf{scale}  region={args.region}")
    print(f"[M1] formats={args.formats}")
    print(f"[M1] tables={table_names}")
    print(f"[M1] target_file_bytes={args.target_file_bytes:,}")
    print(f"[M1] lance_root     = {lance_root}")
    print(f"[M1] iceberg_warehouse = {iceberg_warehouse}")

    spark = None
    if "iceberg_v2" in args.formats:
        print("\n[M1] Starting Spark session for Iceberg writes ...")
        spark = make_spark_session(args.iceberg_jar, iceberg_warehouse,
                                   args.region)
        spark.sparkContext.setLogLevel("WARN")
        print(f"[M1]   Spark version: {spark.version}")

    os.makedirs(os.path.dirname(args.manifest), exist_ok=True)
    records = []

    def write_manifest():
        tmp = args.manifest + ".tmp"
        with open(tmp, "w") as f:
            json.dump({
                "scale": scale,
                "region": args.region,
                "m0_manifest": os.path.abspath(args.m0_manifest),
                "s3_base": s3_base,
                "lance_root": lance_root,
                "iceberg_warehouse": iceberg_warehouse,
                "iceberg_namespace": args.iceberg_namespace,
                "target_file_bytes": args.target_file_bytes,
                "lance_version": lance.__version__,
                "lance_expected_storage_version": LANCE_EXPECTED_STORAGE_VERSION,
                "iceberg_jar": args.iceberg_jar,
                "records": records,
            }, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, args.manifest)

    try:
        for table in table_names:
            t_meta = m0_tables[table]
            src = t_meta["s3_uri"]
            print(f"\n[M1] === table: {table}  "
                  f"({t_meta['rows']:,} rows, {t_meta['local_size_mb']} MB Parquet) ===")

            if "lance_2.2" in args.formats:
                uri = f"{lance_root}/{table}.lance"
                print(f"[M1] lance_2.2 -> {uri}")
                rec = {"format": "lance_2.2", "table": table, "uri": uri,
                       "n_rows_source": t_meta["rows"]}
                s3_rm_prefix(uri, args.region)
                t0 = time.perf_counter()
                try:
                    write_lance_from_parquet(
                        src, uri, storage_options,
                        args.target_file_bytes, args.max_rows_per_file)
                    rec["write_seconds"] = round(time.perf_counter() - t0, 3)
                    rec["ok"] = True
                    rec.update(verify_lance(uri, storage_options))
                    if rec["rows"] != rec["n_rows_source"]:
                        raise RuntimeError(
                            f"row-count mismatch: source={rec['n_rows_source']} "
                            f"lance={rec['rows']}")
                    print(f"  write OK in {rec['write_seconds']}s  "
                          f"rows={rec['rows']}  frags={rec['num_fragments']}  "
                          f"storage_version={rec['storage_version']}")
                except KeyboardInterrupt:
                    raise
                except BaseException as e:
                    if isinstance(e, (SystemExit, GeneratorExit)):
                        raise
                    rec["write_seconds"] = None
                    rec["write_attempt_seconds"] = round(
                        time.perf_counter() - t0, 3)
                    rec["ok"] = False
                    rec["error"] = f"{type(e).__name__}: {e}"[:400]
                    print(f"  write FAILED: {rec['error']}")

                size = du_s3(uri, args.region) if rec.get("ok") else None
                if size is not None:
                    rec["size_bytes"] = size
                    rec["size_mb"] = round(size / 1e6, 2)
                    print(f"  size={rec['size_mb']} MB")
                records.append(rec)
                write_manifest()

            if "iceberg_v2" in args.formats:
                ns = args.iceberg_namespace
                tbl_uri = f"{iceberg_warehouse}/{ns}/{table}"
                print(f"[M1] iceberg_v2 -> ice.{ns}.{table}  (hadoop warehouse "
                      f"{iceberg_warehouse})")
                rec = {"format": "iceberg_v2", "table": table,
                       "fqn": f"ice.{ns}.{table}", "data_uri": tbl_uri,
                       "n_rows_source": t_meta["rows"]}
                s3_rm_prefix(tbl_uri, args.region)
                src_bytes = int(t_meta.get("local_bytes")
                                or (t_meta["local_size_mb"] * 1_000_000))
                t0 = time.perf_counter()
                try:
                    detail = write_iceberg_from_parquet(
                        spark, src, ns, table, args.target_file_bytes,
                        src_bytes)
                    rec["write_seconds"] = round(time.perf_counter() - t0, 3)
                    rec["ok"] = True
                    rec["applied_props"] = detail["applied_props"]
                    rec["n_write_partitions"] = detail["n_write_partitions"]
                    rec.update(verify_iceberg(spark, ns, table))
                    if rec["rows"] != rec["n_rows_source"]:
                        raise RuntimeError(
                            f"row-count mismatch: source={rec['n_rows_source']} "
                            f"iceberg={rec['rows']}")
                    print(f"  write OK in {rec['write_seconds']}s  "
                          f"rows={rec['rows']}  snapshots={rec['snapshots']}  "
                          f"data_files={rec['data_files']}")
                except KeyboardInterrupt:
                    raise
                except BaseException as e:
                    if isinstance(e, (SystemExit, GeneratorExit)):
                        raise
                    rec["write_seconds"] = None
                    rec["write_attempt_seconds"] = round(
                        time.perf_counter() - t0, 3)
                    rec["ok"] = False
                    rec["error"] = f"{type(e).__name__}: {e}"[:400]
                    print(f"  write FAILED: {rec['error']}")

                size = du_s3(tbl_uri, args.region) if rec.get("ok") else None
                if size is not None:
                    rec["size_bytes"] = size
                    rec["size_mb"] = round(size / 1e6, 2)
                    print(f"  size={rec['size_mb']} MB (incl. metadata)")
                records.append(rec)
                write_manifest()

    finally:
        if spark is not None:
            spark.stop()

    print(f"\n[M1] Manifest: {args.manifest}")
    print("\n=== M1 summary (write_seconds / size_mb):")
    print(f"{'table':<16} {'format':<14} "
          f"{'write_s':>10} {'size_mb':>12} {'rows':>12}  ok")
    for r in records:
        print(f"  {r['table']:<16} {r['format']:<14} "
              f"{str(r.get('write_seconds', '--')):>10} "
              f"{str(r.get('size_mb', '--')):>12} "
              f"{str(r.get('rows', '--')):>12}  {r.get('ok', False)}")


if __name__ == "__main__":
    main()
