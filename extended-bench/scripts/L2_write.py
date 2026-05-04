"""L2 write phase: 4 workloads x 5 formats to S3.

Writer strategy (determined empirically, no silent fallback):
  - Lance (all workloads):  pylance single-node. Reasons:
      * lance-spark 0.0.15 SparkOptions does NOT expose data_storage_version
        (only block_size/version/write_mode/max_row_per_file/...), so Spark
        cannot produce v2.2 output deterministically.
      * lance-spark 0.0.15 implements SupportsCatalogOptions, which causes
        .save() without a registered catalog to fail with
        CatalogNotFoundException on Spark 3.5+.
  - Parquet (tab_flat only):  Spark DSv2 - flat schemas survive
    spark.createDataFrame, so parallelism at 100M helps here.
  - Parquet (tab_vec / tab_nested / tab_blob):  pylance / pyarrow single-node.
    Reasons observed on smoke:
      * FixedSizeList<f32, dim> : Spark createDataFrame refuses to infer the
        pandas column ("Unable to infer the type of the field vector").
      * Nested struct/list/map: same "Unable to infer the type" error on
        tags/scores fields, even with standard pandas types.
      * Blob: row_group_size must be 1 KB row-scale to avoid exceeding
        Parquet per-column-chunk limits; Spark's writer doesn't expose
        fine row_group control uniformly.

Known Lance format limitations the script surfaces (not script bugs):
  - Lance v2.0 panics on map column type (unimplemented encoding).
  - Lance v2.1 rejects map with explicit "Map only supported in 2.2+".
  - Lance v2.0/2.1 have no Blob V2 (falls back to large_binary).
These failures produce ok=False records in the manifest so downstream
measurement skips them cleanly.
"""
import argparse
import json
import os
import subprocess
import sys
import time

import lance
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

LANCE_FORMATS = ["lance_2.0", "lance_2.1", "lance_2.2"]
PARQUET_FORMATS = ["parquet_snappy", "parquet_zstd"]
ALL_FORMATS = LANCE_FORMATS + PARQUET_FORMATS
ALL_WORKLOADS = ["tab_flat", "tab_vec", "tab_nested", "tab_blob"]

BLOB_TAKE_SAMPLE = 100


def writer_mode(workload, fmt):
    """Pick writer backend. See module docstring for rationale.
    tab_vec / tab_nested always single-node: FixedSizeList/nested types
    do not survive Spark's createDataFrame type inference.
    tab_blob always single-node: Parquet needs small custom row_group_size
    and Lance needs Blob V2 API (pylance-only).
    """
    if fmt.startswith("lance_"):
        return "single"
    if workload in ("tab_vec", "tab_nested", "tab_blob"):
        return "single"
    return "spark"


def build_tab_flat(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    return pa.table({
        "id":             pa.array(range(n_rows), type=pa.int64()),
        "user_id":        pa.array(rng.integers(0, 10_000_000, n_rows, dtype=np.int64)),
        "amount":         pa.array(rng.uniform(0, 10000, n_rows).astype(np.float64)),
        "score":          pa.array(rng.standard_normal(n_rows).astype(np.float32)),
        "category":       pa.array(rng.choice([f"cat_{i}" for i in range(50)], n_rows),
                                   type=pa.string()),
        "tag":            pa.array(rng.choice(["a", "b", "c", "d", "e"], n_rows),
                                   type=pa.string()),
        "flag_a":         pa.array(rng.integers(0, 2, n_rows, dtype=np.int8)),
        "flag_b":         pa.array(rng.integers(0, 2, n_rows, dtype=np.int8)),
        "ts_ms":          pa.array(rng.integers(1_700_000_000_000,
                                                1_750_000_000_000, n_rows,
                                                dtype=np.int64)),
        "ts_iso":         pa.array(
                              [f"2025-{1 + (i % 12):02d}-{1 + (i % 27):02d}"
                               for i in range(n_rows)], type=pa.string()),
    })


def build_tab_vec(n_rows, dim=128, seed=42):
    base = build_tab_flat(n_rows, seed=seed)
    rng = np.random.default_rng(seed + 1)
    flat = rng.standard_normal(n_rows * dim).astype(np.float32)
    vec = pa.FixedSizeListArray.from_arrays(pa.array(flat), list_size=dim)
    return base.append_column("vector", vec)


def build_tab_nested(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    inner_a = pa.array(rng.integers(0, 1000, n_rows, dtype=np.int32))
    inner_b = pa.array(rng.standard_normal(n_rows).astype(np.float32))
    nested_inner = pa.StructArray.from_arrays([inner_a, inner_b], names=["a", "b"])
    outer_c = pa.array(rng.choice(["x", "y", "z"], n_rows), type=pa.string())
    nested = pa.StructArray.from_arrays([nested_inner, outer_c],
                                        names=["inner", "c"])

    list_lengths = rng.integers(1, 9, n_rows)
    list_values = rng.integers(0, 1_000_000, int(list_lengths.sum()),
                               dtype=np.int64)
    list_offsets = np.concatenate([[0], np.cumsum(list_lengths)]).astype(np.int32)
    list_col = pa.ListArray.from_arrays(pa.array(list_offsets),
                                        pa.array(list_values))

    map_entries = rng.integers(2, 6, n_rows)
    total_entries = int(map_entries.sum())
    keys = [f"k{i % 20}" for i in range(total_entries)]
    vals = rng.uniform(0, 1, total_entries).astype(np.float32)
    map_offsets = np.concatenate([[0], np.cumsum(map_entries)]).astype(np.int32)
    map_col = pa.MapArray.from_arrays(pa.array(map_offsets),
                                      pa.array(keys, type=pa.string()),
                                      pa.array(vals, type=pa.float32()))

    return pa.table({
        "id":       pa.array(range(n_rows), type=pa.int64()),
        "nested":   nested,
        "tags":     list_col,
        "scores":   map_col,
    })


def build_tab_blob(n_rows, seed=42,
                   inline_max_kb=60, packed_max_kb=1024, dedicated_max_mb=6):
    """Size distribution lands in each Blob V2 mode; sizes are deliberately
    kept small so that the total payload stays bounded.

    Blob V2 boundaries in Lance source: Inline <= 64 KB, Packed <= 4 MB,
    Dedicated > 4 MB. We sample inside each band with capped upper sizes:
      - inline   1 KB .. inline_max_kb          (default 60 KB)
      - packed   65 KB .. packed_max_kb KB      (default 1 MB)
      - dedicated 4 MB+1 .. dedicated_max_mb MB (default 6 MB)

    With defaults and n_rows=N, total payload is roughly
    N * (0.60*30KB + 0.30*500KB + 0.10*5MB) ~= N * 670 KB.
    """
    base = build_tab_flat(n_rows, seed=seed)
    rng = np.random.default_rng(seed + 2)

    bucket = rng.choice(
        ["inline", "packed", "dedicated"],
        size=n_rows,
        p=[0.60, 0.30, 0.10],
    )
    payloads = []
    for b in bucket:
        if b == "inline":
            size = int(rng.integers(1 << 10, inline_max_kb * 1024 + 1))
        elif b == "packed":
            size = int(rng.integers((1 << 16) + 1, packed_max_kb * 1024 + 1))
        else:
            size = int(rng.integers((1 << 22) + 1, dedicated_max_mb * (1 << 20) + 1))
        payloads.append(rng.bytes(size))

    counts = {
        "inline":    int((bucket == "inline").sum()),
        "packed":    int((bucket == "packed").sum()),
        "dedicated": int((bucket == "dedicated").sum()),
    }
    total_bytes = sum(len(p) for p in payloads)
    return base, payloads, counts, total_bytes


def du_s3_or_local(uri, timeout_s=600):
    try:
        if uri.startswith("s3://"):
            rel = uri[len("s3://"):]
            bucket, _, key = rel.partition("/")
            out = subprocess.run(
                ["aws", "s3", "ls", f"s3://{bucket}/{key}", "--recursive",
                 "--summarize"],
                check=True, capture_output=True, text=True, timeout=timeout_s,
            ).stdout
            for line in out.splitlines():
                line = line.strip()
                if line.startswith("Total Size:"):
                    return int(line.split(":")[1].strip())
            return None
        if os.path.isfile(uri):
            return os.path.getsize(uri)
        total = 0
        for root, _, files in os.walk(uri):
            for f in files:
                total += os.path.getsize(os.path.join(root, f))
        return total
    except Exception as e:
        print(f"  du failed for {uri}: {e}", file=sys.stderr)
        return None


def write_lance_single_node(tbl, uri, version, storage_options):
    lance.write_dataset(
        tbl, uri,
        mode="overwrite",
        data_storage_version=version,
        storage_options=storage_options,
    )


def write_lance_blob_single_node(tbl_base, payloads, uri, version, storage_options):
    """v2.2 uses blob_field/blob_array (exercises Blob V2 mode routing).
    v2.0/v2.1 fall back to plain large_binary (no Blob V2 semantics).
    """
    if version == "2.2":
        blob_col = lance.blob_array(payloads)
        schema_with_blob = tbl_base.schema.append(lance.blob_field("payload"))
        tbl = pa.Table.from_arrays(
            [c.combine_chunks() if isinstance(c, pa.ChunkedArray) else c
             for c in tbl_base.columns] + [blob_col],
            schema=schema_with_blob,
        )
    else:
        bin_arr = pa.array(payloads, type=pa.large_binary())
        tbl = tbl_base.append_column("payload", bin_arr)

    lance.write_dataset(
        tbl, uri,
        mode="overwrite",
        data_storage_version=version,
        storage_options=storage_options,
    )


def write_parquet_single_node(tbl, uri, compression, row_group_size=1_048_576):
    pq.write_table(
        tbl, uri,
        compression=compression,
        row_group_size=row_group_size,
        data_page_size=1024 * 1024,
        write_statistics=True,
        use_dictionary=True,
        data_page_version="2.0",
    )


def write_parquet_blob_single_node(tbl, uri, compression):
    """Blob row groups must be small: each row is 1 KB - 6 MB, so a naive
    row_group_size=1M rows would produce multi-TB row groups that exceed
    Parquet per-column-chunk limits.
    """
    write_parquet_single_node(tbl, uri, compression, row_group_size=1024)


def write_via_spark(tbl, uri, kind, opt, spark, n_partitions):
    sdf = spark.createDataFrame(tbl.to_pandas())
    writer = sdf.repartition(n_partitions).write.mode("overwrite")
    if kind == "lance":
        (writer.format("lance")
               .option("path", uri)
               .option("data_storage_version", opt)
               .save())
    elif kind == "parquet":
        (writer.option("compression", opt)
               .parquet(uri))
    else:
        raise ValueError(f"unknown kind: {kind}")


def make_spark_session(jars_dir):
    from pyspark.sql import SparkSession
    jars = [
        os.path.join(jars_dir, "lance-spark-3.5_2.12-0.0.15.jar"),
        os.path.join(jars_dir, "lance-spark-base_2.12-0.0.15.jar"),
        os.path.join(jars_dir, "lance-core-0.39.0.jar"),
        os.path.join(jars_dir, "arrow-c-data-15.0.0.jar"),
        os.path.join(jars_dir, "arrow-dataset-15.0.0.jar"),
        os.path.join(jars_dir, "jar-jni.jar"),
    ]
    missing = [j for j in jars if not os.path.exists(j)]
    if missing:
        raise FileNotFoundError(f"lance-spark JARs missing: {missing}")

    return (
        SparkSession.builder
        .appName("L2-write")
        .config("spark.jars", ",".join(jars))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def plan_tasks(workloads, formats):
    tasks = []
    for w in workloads:
        for f in formats:
            tasks.append((w, f, writer_mode(w, f)))
    return tasks


def format_kind(fmt):
    if fmt.startswith("lance_"):
        return "lance", fmt.split("_", 1)[1]
    if fmt.startswith("parquet_"):
        return "parquet", fmt.split("_", 1)[1]
    raise ValueError(f"unknown format: {fmt}")


def read_run_env(path="/home/hadoop/lance-extended-bench/run.env"):
    if not os.path.exists(path):
        return {}
    out = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def verify_written(uri, kind):
    """Defense against E-class silent-overwrite bugs: confirm the dataset is
    readable at exactly the URI we passed.
    """
    try:
        if kind == "lance":
            ds = lance.dataset(uri)
            return True, {
                "verify_rows": ds.count_rows(),
                "verify_storage_version": ds.data_storage_version,
                "verify_manifest_version": ds.version,
            }
        elif kind == "parquet":
            import pyarrow.dataset as pa_ds
            d = pa_ds.dataset(uri, format="parquet")
            return True, {"verify_rows": d.count_rows()}
    except Exception as e:
        return False, {"verify_error": f"{type(e).__name__}: {e}"[:400]}
    return False, {"verify_error": "unknown"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=1_000_000,
                    help="rows for tab_flat/tab_vec")
    ap.add_argument("--n-rows-nested", type=int, default=None,
                    help="default: n_rows")
    ap.add_argument("--n-rows-blob", type=int, default=None,
                    help="default: min(n_rows, 100_000); blob payloads are "
                         "large so this is capped independently")
    ap.add_argument("--workloads", nargs="+",
                    default=ALL_WORKLOADS, choices=ALL_WORKLOADS)
    ap.add_argument("--formats", nargs="+",
                    default=ALL_FORMATS, choices=ALL_FORMATS)
    ap.add_argument("--s3-base", default=None,
                    help="default: read S3_BASE from run.env")
    ap.add_argument("--region", default=None,
                    help="default: read AWS_REGION from run.env")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--jars-dir", default="/home/hadoop/lance-read-bench/spark-libs")
    ap.add_argument("--spark-partitions", type=int, default=8)
    ap.add_argument("--vec-dim", type=int, default=128)
    ap.add_argument("--blob-inline-max-kb", type=int, default=60)
    ap.add_argument("--blob-packed-max-kb", type=int, default=1024)
    ap.add_argument("--blob-dedicated-max-mb", type=int, default=6)
    ap.add_argument("--manifest", default="/home/hadoop/lance-extended-bench/"
                                          "results/L2_manifest.json")
    args = ap.parse_args()

    env = read_run_env()
    if args.s3_base is None:
        args.s3_base = os.environ.get("S3_BASE") or env.get("S3_BASE")
        if not args.s3_base:
            raise SystemExit("S3 base not found. Pass --s3-base or set S3_BASE.")
    if args.region is None:
        args.region = os.environ.get("AWS_REGION") or env.get("AWS_REGION")
    if not args.region:
        raise SystemExit("AWS_REGION not found. Pass --region or set AWS_REGION.")
    os.environ.setdefault("AWS_REGION", args.region)
    storage_options = {"region": args.region}

    run_id = args.run_id or os.environ.get("RUN_ID") or time.strftime(
        "%Y%m%d-%H%M%S")
    s3_root = f"{args.s3_base.rstrip('/')}/L2/{run_id}"
    print(f"[L2-write] S3 output root: {s3_root}")
    print(f"[L2-write] Region: {args.region}")
    print(f"[L2-write] Run id: {run_id}")

    n_nested = args.n_rows_nested if args.n_rows_nested is not None else args.n_rows
    n_blob = (args.n_rows_blob if args.n_rows_blob is not None
              else min(args.n_rows, 100_000))

    tasks = plan_tasks(args.workloads, args.formats)
    print(f"[L2-write] Plan: {len(tasks)} tasks "
          f"({len(args.workloads)} workloads x {len(args.formats)} formats)")
    spark_needed = any(m == "spark" for _, _, m in tasks)

    spark = None
    if spark_needed:
        print("[L2-write] Starting Spark session ...")
        spark = make_spark_session(args.jars_dir)
        spark.sparkContext.setLogLevel("WARN")
        print(f"[L2-write]   Spark version: {spark.version}")

    print("[L2-write] Building source tables ...")
    sources = {}
    blob_metadata = None
    if "tab_flat" in args.workloads:
        t0 = time.perf_counter()
        sources["tab_flat"] = build_tab_flat(args.n_rows)
        print(f"  tab_flat   n_rows={args.n_rows:,} "
              f"(built in {time.perf_counter()-t0:.1f}s)")
    if "tab_vec" in args.workloads:
        t0 = time.perf_counter()
        sources["tab_vec"] = build_tab_vec(args.n_rows, dim=args.vec_dim)
        print(f"  tab_vec    n_rows={args.n_rows:,} dim={args.vec_dim} "
              f"(built in {time.perf_counter()-t0:.1f}s)")
    if "tab_nested" in args.workloads:
        t0 = time.perf_counter()
        sources["tab_nested"] = build_tab_nested(n_nested)
        print(f"  tab_nested n_rows={n_nested:,} "
              f"(built in {time.perf_counter()-t0:.1f}s)")
    if "tab_blob" in args.workloads:
        t0 = time.perf_counter()
        base, payloads, counts, total_bytes = build_tab_blob(
            n_blob,
            inline_max_kb=args.blob_inline_max_kb,
            packed_max_kb=args.blob_packed_max_kb,
            dedicated_max_mb=args.blob_dedicated_max_mb,
        )
        sources["tab_blob"] = (base, payloads)
        blob_metadata = {"bucket_counts": counts,
                         "total_payload_bytes": total_bytes}
        print(f"  tab_blob   n_rows={n_blob:,} "
              f"(built in {time.perf_counter()-t0:.1f}s, "
              f"total_payload={total_bytes/1e6:.0f}MB, "
              f"buckets={counts})")

    records = []
    for wl_name, fmt, mode in tasks:
        if wl_name not in sources:
            continue
        uri = f"{s3_root}/{wl_name}/{fmt}"
        if fmt.startswith("lance_"):
            uri += ".lance"

        kind, opt = format_kind(fmt)
        print(f"\n[L2-write] {wl_name}/{fmt}  mode={mode}  -> {uri}")

        rec = {
            "workload": wl_name,
            "format": fmt,
            "mode": mode,
            "uri": uri,
            "region": args.region,
            "lance_version": lance.__version__,
            "pyarrow_version": pa.__version__,
            "n_rows": (args.n_rows if wl_name in ("tab_flat", "tab_vec")
                       else n_nested if wl_name == "tab_nested"
                       else n_blob),
        }
        if wl_name == "tab_blob" and blob_metadata is not None:
            rec["blob_bucket_counts"] = blob_metadata["bucket_counts"]
            rec["blob_total_payload_bytes"] = blob_metadata["total_payload_bytes"]

        t0 = time.perf_counter()
        try:
            if mode == "single":
                payload = sources[wl_name]
                if wl_name == "tab_blob":
                    base, payloads = payload
                    if kind == "lance":
                        write_lance_blob_single_node(
                            base, payloads, uri, version=opt,
                            storage_options=storage_options)
                    elif kind == "parquet":
                        bin_arr = pa.array(payloads, type=pa.large_binary())
                        tbl = base.append_column("payload", bin_arr)
                        write_parquet_blob_single_node(tbl, uri, compression=opt)
                else:
                    tbl = payload
                    if kind == "lance":
                        write_lance_single_node(
                            tbl, uri, version=opt,
                            storage_options=storage_options)
                    elif kind == "parquet":
                        write_parquet_single_node(tbl, uri, compression=opt)
            elif mode == "spark":
                tbl = sources[wl_name]
                write_via_spark(tbl, uri, kind, opt, spark,
                                args.spark_partitions)
            else:
                raise ValueError(f"unknown mode: {mode}")
            elapsed = time.perf_counter() - t0
            rec["write_seconds"] = round(elapsed, 3)
            rec["ok"] = True
            print(f"  write OK in {elapsed:.2f}s")
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            if isinstance(e, (SystemExit, GeneratorExit)):
                raise
            elapsed = time.perf_counter() - t0
            rec["write_seconds"] = None
            rec["write_attempt_seconds"] = round(elapsed, 3)
            rec["ok"] = False
            rec["error"] = f"{type(e).__name__}: {e}"[:400]
            print(f"  write FAILED after {elapsed:.2f}s: {rec['error']}")

        if rec.get("ok"):
            ok, detail = verify_written(uri, kind)
            rec.update(detail)
            if not ok:
                rec["ok"] = False
                print(f"  verify FAILED: {detail.get('verify_error')}")
            else:
                msg = f"  verify OK  rows={detail.get('verify_rows')}"
                if "verify_storage_version" in detail:
                    msg += f" storage_version={detail.get('verify_storage_version')}"
                print(msg)

        size_bytes = du_s3_or_local(uri) if rec.get("ok") else None
        if size_bytes is not None:
            rec["size_bytes"] = size_bytes
            rec["size_mb"] = round(size_bytes / 1e6, 2)
            print(f"  size={rec['size_mb']} MB")
        records.append(rec)

    if spark is not None:
        spark.stop()

    os.makedirs(os.path.dirname(args.manifest), exist_ok=True)
    with open(args.manifest, "w") as f:
        json.dump({
            "run_id": run_id,
            "s3_root": s3_root,
            "region": args.region,
            "n_rows_flat_vec": args.n_rows,
            "n_rows_nested": n_nested,
            "n_rows_blob": n_blob,
            "vec_dim": args.vec_dim,
            "lance_version": lance.__version__,
            "records": records,
        }, f, indent=2)
    print(f"\n[L2-write] Manifest: {args.manifest}")

    print("\n=== L2 write summary (write_seconds / size_mb):")
    print(f"{'workload':<12} {'format':<16} {'mode':<7} "
          f"{'write_s':>10} {'size_mb':>10}  ok")
    for r in records:
        print(f"  {r['workload']:<12} {r['format']:<16} {r['mode']:<7} "
              f"{str(r.get('write_seconds', '--')):>10} "
              f"{str(r.get('size_mb', '--')):>10}  {r.get('ok', False)}")


if __name__ == "__main__":
    main()
