"""Correctly measure Lance and Iceberg table size, distinguishing:
  - Active size: bytes referenced by the current version (real storage cost
    if the user doesn't need time travel).
  - Total on-disk size: bytes in the whole directory/prefix, including old
    versions and transaction logs (MVCC state retained by design for time
    travel / audit / rollback).

The previous M6 and compact_gc scripts conflated these by summing the
whole directory / prefix, which made retained MVCC history look like
'bloat'. That's wrong framing: keeping old versions is a feature, not a
leak.

Active-size procedure:
  Lance:    ds.get_fragments() -> for each fragment, list its data files
            (via fragment.data_files()) and du those specific files in data/.
  Iceberg:  StaticTable.from_metadata(...) -> scan().plan_files() gives the
            manifest's active data files; sum their sizes from
            manifest entries (file_size_in_bytes).

For Iceberg, delete files (positional deletes) are referenced by the
active snapshot and also count as active.

Usage:
    python3 measure_active_size.py --lance-uri s3://... --region ap-northeast-1
    python3 measure_active_size.py --iceberg-data-uri s3://... --region ...
    python3 measure_active_size.py --local-path /tmp/xxx.lance
"""
import argparse
import json
import os
import subprocess

import lance
from pyiceberg.table import StaticTable


def s3_list(s3_uri, region):
    rel = s3_uri[len("s3://"):]
    bucket, _, key = rel.partition("/")
    out = subprocess.run(
        ["aws", "s3api", "list-objects-v2",
         "--bucket", bucket, "--prefix", key,
         "--region", region, "--output", "json"],
        check=True, capture_output=True, text=True, timeout=600,
    ).stdout
    if not out.strip() or out.strip() == "null":
        return []
    payload = json.loads(out)
    return [(row["Key"], int(row["Size"]))
            for row in payload.get("Contents", []) or []]


def local_list(path):
    out = []
    for root, _, files in os.walk(path):
        for f in files:
            full = os.path.join(root, f)
            rel = os.path.relpath(full, path)
            out.append((rel, os.path.getsize(full)))
    return out


def lance_active_fragment_paths(uri, storage_options=None):
    """Return the set of data filenames referenced by the CURRENT version.

    Each Fragment exposes .data_files() which is the list of data file
    objects that back this fragment. Their .path is relative to the
    dataset root.
    """
    ds = (lance.dataset(uri, storage_options=storage_options)
          if storage_options else lance.dataset(uri))
    active = set()
    for frag in ds.get_fragments():
        for df in frag.data_files():
            active.add(df.path)
    return active, ds.count_rows(), ds.version, len(ds.versions())


def iceberg_active_files(metadata_uri):
    """Return the (filename, size_bytes) list for the current snapshot.

    pyiceberg's Table.current_snapshot() + plan_files() yields the set
    of active data + delete files. We use the manifest-reported size in
    bytes; these are authoritative (Iceberg writers record them).
    """
    tbl = StaticTable.from_metadata(metadata_uri)
    plan = list(tbl.scan().plan_files())
    data_bytes = 0
    delete_bytes = 0
    data_paths = set()
    delete_paths = set()
    for task in plan:
        df = task.file
        data_bytes += df.file_size_in_bytes
        data_paths.add(df.file_path)
        for ddf in task.delete_files:
            delete_bytes += ddf.file_size_in_bytes
            delete_paths.add(ddf.file_path)
    return {
        "data_bytes": data_bytes,
        "delete_bytes": delete_bytes,
        "data_paths": data_paths,
        "delete_paths": delete_paths,
    }


def find_iceberg_metadata_uri(region, data_uri):
    rel = data_uri[len("s3://"):]
    bucket, _, key = rel.partition("/")
    hint = subprocess.run(
        ["aws", "s3", "cp",
         f"s3://{bucket}/{key.rstrip('/')}/metadata/version-hint.text", "-",
         "--region", region],
        check=True, capture_output=True, text=True, timeout=60).stdout.strip()
    return f"s3://{bucket}/{key.rstrip('/')}/metadata/v{hint}.metadata.json"


def report_lance(uri, region=None, local=False):
    storage_options = None
    if uri.startswith("s3://") and region:
        storage_options = {"region": region}

    active_paths, rows, ver, n_versions = lance_active_fragment_paths(
        uri, storage_options)

    if local:
        all_files = local_list(uri)
    else:
        all_files = s3_list(uri, region)

    active_path_basenames = {os.path.basename(p) for p in active_paths}
    active_total = 0
    total_all = 0
    data_total = 0
    meta_total = 0
    for key, size in all_files:
        total_all += size
        basename = os.path.basename(key)
        if "/data/" in key or key.startswith("data/"):
            data_total += size
            if basename in active_path_basenames:
                active_total += size
        elif ("/_versions/" in key or key.startswith("_versions/")
              or "/_transactions/" in key or key.startswith("_transactions/")
              or "/_indices/" in key or key.startswith("_indices/")):
            meta_total += size
    orphan_data = data_total - active_total
    return {
        "kind": "lance",
        "uri": uri,
        "rows": rows,
        "latest_version": ver,
        "n_versions": n_versions,
        "n_active_fragments": len(active_paths),
        "n_total_files": len(all_files),
        "active_data_bytes": active_total,
        "orphan_data_bytes": orphan_data,
        "metadata_bytes": meta_total,
        "total_bytes_on_disk": total_all,
        "active_mb": round(active_total / 1e6, 3),
        "orphan_mb": round(orphan_data / 1e6, 3),
        "total_mb": round(total_all / 1e6, 3),
        "pct_orphan": (round(100 * orphan_data / total_all, 2)
                       if total_all > 0 else 0.0),
    }


def report_iceberg(data_uri, region):
    meta_uri = find_iceberg_metadata_uri(region, data_uri)
    active = iceberg_active_files(meta_uri)
    all_files = s3_list(data_uri, region)
    active_basenames = ({os.path.basename(p) for p in active["data_paths"]}
                        | {os.path.basename(p) for p in active["delete_paths"]})
    data_total = 0
    meta_total = 0
    total_all = 0
    for key, size in all_files:
        total_all += size
        if "/data/" in key or key.startswith("data/"):
            data_total += size
        elif "/metadata/" in key or key.startswith("metadata/"):
            meta_total += size
    active_total = active["data_bytes"] + active["delete_bytes"]
    orphan_data = data_total - active_total
    return {
        "kind": "iceberg",
        "data_uri": data_uri,
        "metadata_uri": meta_uri,
        "n_active_data_files": len(active["data_paths"]),
        "n_active_delete_files": len(active["delete_paths"]),
        "n_total_files": len(all_files),
        "active_data_bytes": active["data_bytes"],
        "active_delete_bytes": active["delete_bytes"],
        "active_total_bytes": active_total,
        "orphan_data_bytes": orphan_data,
        "metadata_bytes": meta_total,
        "total_bytes_on_disk": total_all,
        "active_mb": round(active_total / 1e6, 3),
        "orphan_mb": round(orphan_data / 1e6, 3),
        "total_mb": round(total_all / 1e6, 3),
        "pct_orphan": (round(100 * orphan_data / total_all, 2)
                       if total_all > 0 else 0.0),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lance-uri", help="s3:// or local path to .lance")
    ap.add_argument("--iceberg-data-uri",
                    help="s3://.../namespace/table (Iceberg data URI)")
    ap.add_argument("--region", default=None)
    ap.add_argument("--local-path",
                    help="local .lance path (shortcut for --lance-uri)")
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    results = []
    if args.local_path:
        results.append(report_lance(args.local_path, local=True))
    if args.lance_uri:
        results.append(report_lance(args.lance_uri, region=args.region))
    if args.iceberg_data_uri:
        if not args.region:
            raise SystemExit("--region required for iceberg")
        results.append(report_iceberg(args.iceberg_data_uri, args.region))

    for r in results:
        print()
        print(f"=== {r['kind']} ===")
        for k in ("uri", "data_uri", "rows", "latest_version",
                  "n_versions", "n_active_fragments", "n_active_data_files",
                  "n_active_delete_files", "n_total_files",
                  "active_mb", "orphan_mb", "total_mb", "pct_orphan"):
            if k in r:
                print(f"  {k:<22} {r[k]}")

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nJSON: {args.json_out}")


if __name__ == "__main__":
    main()
