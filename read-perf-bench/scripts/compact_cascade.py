import sys
import time
import json
import lance

S3_PATH = sys.argv[1]
OUT_JSON = sys.argv[2]

ds = lance.dataset(S3_PATH)
total_rows = ds.count_rows()
initial_fragments = len(ds.get_fragments())
initial_version = ds.version

print(f"Starting: rows={total_rows} fragments={initial_fragments} version={initial_version}")

plan = [
    ("D", 10_000),
    ("C", 100_000),
    ("B", 1_000_000),
    ("A", 10_000_000),
]

results = {
    "base_path": S3_PATH,
    "E_version": initial_version,
    "E_fragments": initial_fragments,
    "E_rows": total_rows,
    "compactions": [],
}

for tag, target_rows in plan:
    print(f"\n=== Compacting to {tag} (target {target_rows} rows/fragment)")
    ds = lance.dataset(S3_PATH)
    t0 = time.time()
    metrics = ds.optimize.compact_files(
        target_rows_per_fragment=target_rows,
        max_rows_per_group=1024,
    )
    elapsed = time.time() - t0
    ds2 = lance.dataset(S3_PATH)
    info = {
        "tag": tag,
        "target_rows_per_fragment": target_rows,
        "fragments_after": len(ds2.get_fragments()),
        "version_after": ds2.version,
        "elapsed_s": elapsed,
        "fragments_removed": metrics.fragments_removed,
        "fragments_added": metrics.fragments_added,
        "files_removed": metrics.files_removed,
        "files_added": metrics.files_added,
    }
    print(f"  -> {info['fragments_after']} fragments, version {info['version_after']}, {elapsed:.1f}s")
    print(f"  -> removed {info['fragments_removed']} fragments, added {info['fragments_added']}")
    results["compactions"].append(info)

with open(OUT_JSON, "w") as f:
    json.dump(results, f, indent=2)
print(f"\nSaved plan to {OUT_JSON}")
print("\nVersion map:")
print(f"  E (no compact): version {results['E_version']}, {results['E_fragments']} fragments")
for c in results["compactions"]:
    print(f"  {c['tag']} (target {c['target_rows_per_fragment']}): version {c['version_after']}, {c['fragments_after']} fragments")
