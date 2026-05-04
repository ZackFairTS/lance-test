"""M2: storage breakdown for the Lance vs Iceberg pair written by M1.

Reports three levels of detail per table:
  1. Top-level totals: data bytes, metadata bytes, manifest/transaction bytes.
  2. Per-file listing: every S3 object under the table prefix + its size,
     so Lance fragment balance and Iceberg file count are visible.
  3. Per-column bytes-on-disk: Lance via ds.stats.data_stats(), Parquet via
     pyarrow's row-group column metadata summed across row groups.
     The per-column comparison explains why Lance and Iceberg totals differ
     without hand-waving.

Output: results/M2_size_sf<N>.json + console table.
"""
import argparse
import json
import os
import subprocess
import sys

import lance
import pyarrow.parquet as pq
from pyarrow import fs

LANCE_DATA_DIRS = ("/data/",)
LANCE_META_MARKERS = ("/_versions/", "/_transactions/", "/_indices/")
ICEBERG_DATA_DIRS = ("/data/",)
ICEBERG_META_DIRS = ("/metadata/",)


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


def s3_list_recursive(s3_uri, region, timeout_s=600):
    rel = s3_uri[len("s3://"):]
    bucket, _, key = rel.partition("/")
    out = []
    token = None
    while True:
        cmd = ["aws", "s3api", "list-objects-v2",
               "--bucket", bucket, "--prefix", key,
               "--region", region, "--output", "json"]
        if token:
            cmd += ["--starting-token", token]
        r = subprocess.run(cmd, check=True, capture_output=True, text=True,
                           timeout=timeout_s)
        if not r.stdout.strip() or r.stdout.strip() == "null":
            break
        payload = json.loads(r.stdout)
        for row in payload.get("Contents", []) or []:
            out.append((row["Key"], int(row["Size"])))
        if not payload.get("IsTruncated"):
            break
        token = payload.get("NextContinuationToken")
        if not token:
            break
    return out


def classify_lance_file(key):
    for m in LANCE_META_MARKERS:
        if m in key:
            return "metadata"
    for d in LANCE_DATA_DIRS:
        if d in key:
            return "data"
    return "other"


def classify_iceberg_file(key):
    for d in ICEBERG_META_DIRS:
        if d in key:
            return "metadata"
    for d in ICEBERG_DATA_DIRS:
        if d in key:
            return "data"
    return "other"


def lance_column_bytes(uri, storage_options):
    ds = lance.dataset(uri, storage_options=storage_options)
    stats = ds.stats.data_stats()
    n_schema = len(ds.schema)
    n_stats = len(stats.fields)
    stats_ids = [fs_.id for fs_ in stats.fields]
    if n_stats != n_schema or stats_ids != list(range(n_schema)):
        raise RuntimeError(
            f"Lance field-id layout is not flat positional "
            f"(schema_fields={n_schema}, stats_fields={n_stats}, "
            f"stats_ids={stats_ids}). "
            f"M2 per-column mapping only handles flat schemas; add explicit "
            f"schema-id reconciliation before running on nested tables.")
    out = []
    for i, field in enumerate(ds.schema):
        fs_ = stats.fields[i]
        entry = {
            "name": field.name,
            "type": str(field.type),
            "field_id": fs_.id,
            "bytes_on_disk": fs_.bytes_on_disk,
        }
        for attr in ("num_rows", "null_count"):
            if hasattr(fs_, attr):
                entry[attr] = getattr(fs_, attr)
        out.append(entry)
    return out


def parquet_column_bytes(parquet_uris):
    """Sum total_compressed_size per column across all row groups of all files."""
    per_col_compressed = {}
    per_col_uncompressed = {}
    col_types = {}
    for uri in parquet_uris:
        filesystem, path = fs.FileSystem.from_uri(uri)
        pf = pq.ParquetFile(path, filesystem=filesystem)
        for c in range(pf.metadata.num_columns):
            col_types[pf.schema_arrow.field(c).name] = str(
                pf.schema_arrow.field(c).type)
        for rg_idx in range(pf.num_row_groups):
            rg = pf.metadata.row_group(rg_idx)
            for c_idx in range(rg.num_columns):
                col = rg.column(c_idx)
                name = col.path_in_schema
                per_col_compressed[name] = per_col_compressed.get(name, 0) + \
                    col.total_compressed_size
                per_col_uncompressed[name] = per_col_uncompressed.get(name, 0) + \
                    col.total_uncompressed_size
    out = []
    for name, c in per_col_compressed.items():
        out.append({
            "name": name,
            "type": col_types.get(name),
            "bytes_on_disk": c,
            "bytes_uncompressed": per_col_uncompressed[name],
        })
    return out


def size_breakdown(s3_uri, region, classifier):
    files = s3_list_recursive(s3_uri, region)
    totals = {"data": 0, "metadata": 0, "other": 0}
    counts = {"data": 0, "metadata": 0, "other": 0}
    listing = []
    for key, size in files:
        category = classifier(key)
        totals[category] += size
        counts[category] += 1
        listing.append({"key": key, "size": size, "category": category})
    listing.sort(key=lambda r: -r["size"])
    return {
        "total_bytes": sum(totals.values()),
        "bytes_by_category": totals,
        "files_by_category": counts,
        "listing": listing,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m1-manifest", required=True)
    ap.add_argument("--region", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--listing-top-n", type=int, default=20,
                    help="keep only the N largest files per table in the "
                         "JSON listing (full stats are still summarized)")
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
    if args.out is None:
        args.out = (f"/home/hadoop/lance-extended-bench/results/"
                    f"M2_size_sf{scale}.json")

    print(f"[M2] m1 manifest: {args.m1_manifest}")
    print(f"[M2] scale=sf{scale}  region={args.region}")

    by_table = {}
    for rec in m1["records"]:
        if not rec.get("ok"):
            continue
        table = rec["table"]
        fmt = rec["format"]
        by_table.setdefault(table, {})[fmt] = rec

    storage_options = {"region": args.region}
    out_tables = []

    for table, fmts in by_table.items():
        print(f"\n[M2] === table: {table} ===")
        t_rec = {"table": table, "formats": {}}

        lance_rec = fmts.get("lance_2.2")
        iceberg_rec = fmts.get("iceberg_v2")

        if lance_rec:
            uri = lance_rec["uri"]
            print(f"  [lance_2.2] breakdown {uri}")
            fmt_rec = {"uri": uri}
            try:
                b = size_breakdown(uri, args.region, classify_lance_file)
                per_col = lance_column_bytes(uri, storage_options)
                fmt_rec.update({
                    "total_bytes": b["total_bytes"],
                    "total_mb": round(b["total_bytes"] / 1e6, 2),
                    "bytes_by_category": b["bytes_by_category"],
                    "mb_by_category": {k: round(v / 1e6, 2)
                                       for k, v in b["bytes_by_category"].items()},
                    "files_by_category": b["files_by_category"],
                    "num_fragments": lance_rec.get("num_fragments"),
                    "per_column": per_col,
                    "top_files": b["listing"][:args.listing_top_n],
                })
                print(f"    total={b['total_bytes']/1e6:.1f} MB "
                      f"(data={b['bytes_by_category']['data']/1e6:.1f} MB, "
                      f"meta={b['bytes_by_category']['metadata']/1e6:.1f} MB)")
                print(f"    fragments={lance_rec.get('num_fragments')}")
            except KeyboardInterrupt:
                raise
            except BaseException as e:
                if isinstance(e, (SystemExit, GeneratorExit)):
                    raise
                fmt_rec["error"] = f"{type(e).__name__}: {e}"[:400]
                print(f"    lance breakdown FAILED: {fmt_rec['error']}",
                      file=sys.stderr)
            t_rec["formats"]["lance_2.2"] = fmt_rec

        if iceberg_rec:
            tbl_uri = iceberg_rec["data_uri"]
            print(f"  [iceberg_v2] breakdown {tbl_uri}")
            fmt_rec = {"data_uri": tbl_uri}
            try:
                b = size_breakdown(tbl_uri, args.region, classify_iceberg_file)
                bucket_prefix = f"s3://{tbl_uri[len('s3://'):].partition('/')[0]}/"
                data_files = [bucket_prefix + item["key"]
                              for item in b["listing"]
                              if item["category"] == "data"
                              and item["key"].endswith(".parquet")]
                try:
                    per_col = parquet_column_bytes(data_files)
                except Exception as e:
                    print(f"    parquet per-column read failed: {e}",
                          file=sys.stderr)
                    per_col = []
                fmt_rec.update({
                    "total_bytes": b["total_bytes"],
                    "total_mb": round(b["total_bytes"] / 1e6, 2),
                    "bytes_by_category": b["bytes_by_category"],
                    "mb_by_category": {k: round(v / 1e6, 2)
                                       for k, v in b["bytes_by_category"].items()},
                    "files_by_category": b["files_by_category"],
                    "num_data_files": iceberg_rec.get("data_files"),
                    "num_snapshots": iceberg_rec.get("snapshots"),
                    "per_column": per_col,
                    "top_files": b["listing"][:args.listing_top_n],
                })
                print(f"    total={b['total_bytes']/1e6:.1f} MB "
                      f"(data={b['bytes_by_category']['data']/1e6:.1f} MB, "
                      f"meta={b['bytes_by_category']['metadata']/1e6:.1f} MB)")
                print(f"    data_files={iceberg_rec.get('data_files')}")
            except KeyboardInterrupt:
                raise
            except BaseException as e:
                if isinstance(e, (SystemExit, GeneratorExit)):
                    raise
                fmt_rec["error"] = f"{type(e).__name__}: {e}"[:400]
                print(f"    iceberg breakdown FAILED: {fmt_rec['error']}",
                      file=sys.stderr)
            t_rec["formats"]["iceberg_v2"] = fmt_rec

        if lance_rec and iceberg_rec:
            lance_total = t_rec["formats"]["lance_2.2"].get("total_bytes")
            iceberg_total = t_rec["formats"]["iceberg_v2"].get("total_bytes")
            if (lance_total is not None and iceberg_total is not None
                    and iceberg_total > 0):
                t_rec["size_ratio_lance_over_iceberg"] = round(
                    lance_total / iceberg_total, 3)
                print(f"  ** ratio: Lance {lance_total/1e6:.1f} MB / "
                      f"Iceberg {iceberg_total/1e6:.1f} MB = "
                      f"{lance_total/iceberg_total:.2f}x **")

        out_tables.append(t_rec)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + ".tmp"
    with open(tmp, "w") as f:
        json.dump({
            "scale": scale,
            "region": args.region,
            "m1_manifest": os.path.abspath(args.m1_manifest),
            "lance_version": lance.__version__,
            "tables": out_tables,
        }, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, args.out)
    print(f"\n[M2] Saved: {args.out}")

    print("\n=== M2 per-column size comparison ===")
    for t in out_tables:
        fmts = t["formats"]
        if "lance_2.2" not in fmts or "iceberg_v2" not in fmts:
            continue
        print(f"\n-- {t['table']} --")
        lance_cols = {c["name"]: c for c in fmts["lance_2.2"].get("per_column", [])}
        iceberg_cols = {c["name"]: c for c in fmts["iceberg_v2"].get("per_column", [])}
        all_names = list(iceberg_cols.keys())
        for n in lance_cols:
            if n not in all_names:
                all_names.append(n)
        print(f"{'column':<25} {'lance_mb':>10} {'iceberg_mb':>12} "
              f"{'ratio':>8}  lance_type / iceberg_type")
        for n in all_names:
            lc = lance_cols.get(n, {})
            ic = iceberg_cols.get(n, {})
            lb = lc.get("bytes_on_disk")
            ib = ic.get("bytes_on_disk")
            lb_mb = f"{lb/1e6:.2f}" if lb is not None else "--"
            ib_mb = f"{ib/1e6:.2f}" if ib is not None else "--"
            ratio = (f"{lb/ib:.2f}" if lb is not None and ib and ib > 0
                     else "--")
            print(f"  {n:<23} {lb_mb:>10} {ib_mb:>12} {ratio:>8}  "
                  f"{lc.get('type', '--')} / {ic.get('type', '--')}")


if __name__ == "__main__":
    main()
