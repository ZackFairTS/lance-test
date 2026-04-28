#!/usr/bin/env python3
"""After the run, verify data correctness: count rows, check duplicate IDs, list versions."""
import os
import sys
import json
import lance

s3_path = sys.argv[1]
expected_rows = int(sys.argv[2]) if len(sys.argv) > 2 else -1
out_path = sys.argv[3] if len(sys.argv) > 3 else "/tmp/verify.json"

ds = lance.dataset(s3_path)
total = ds.count_rows()
versions = ds.versions()
fragments = ds.get_fragments()

print(f"Dataset: {s3_path}")
print(f"  Total rows: {total}")
print(f"  Expected: {expected_rows}")
print(f"  Versions: {len(versions)}")
print(f"  Latest version: {ds.version}")
print(f"  Fragments: {len(fragments)}")

duplicate_count = 0
duplicate_sample = []
try:
    import pyarrow.compute as pc
    tbl = ds.to_table(columns=["id"])
    ids = tbl["id"]
    unique_count = len(pc.unique(ids))
    duplicate_count = total - unique_count
    if duplicate_count > 0:
        counts = pc.value_counts(ids)
        dup_struct = counts.filter(pc.greater(counts.field("counts"), 1))
        if len(dup_struct) > 0:
            duplicate_sample = [
                {"id": dup_struct["values"][i].as_py(), "count": dup_struct["counts"][i].as_py()}
                for i in range(min(10, len(dup_struct)))
            ]
    print(f"  Unique IDs: {unique_count}")
    print(f"  Duplicates: {duplicate_count}")
    if duplicate_sample:
        print(f"  Sample duplicates: {duplicate_sample}")
except Exception as e:
    print(f"  Dup check failed: {e}")

result = {
    "path": s3_path,
    "total_rows": total,
    "expected_rows": expected_rows,
    "versions": len(versions),
    "latest_version": ds.version,
    "fragments": len(fragments),
    "duplicate_count": duplicate_count,
    "duplicate_sample": duplicate_sample,
}
with open(out_path, "w") as f:
    json.dump(result, f, indent=2)
print(f"Report: {out_path}")
