"""Compact GC investigation: distinguish hypothesis A (compact does not GC
old fragments) from hypothesis B (compact itself bloats output).

Context (M6 finding): Lance `ds.optimize.compact_files()` inflates on-disk
size by 73-76% in both sf1 and sf10 runs. It is unclear whether:
  A. Old fragments remain on disk after compact, inflating total footprint
     -> size should drop after `cleanup_old_versions()`, per-fragment
        ratio should match pre-compact.
  B. New compacted fragments are themselves larger than the originals
     combined -> cleanup does nothing, new fragment is bloated.

Experiment protocol per replay:
  1. Build a baseline fragment, then append N small fragments.
  2. Snapshot: count fragments, total S3/disk bytes, count on-disk files
     under data/, count versions.
  3. Call ds.optimize.compact_files(). Record CompactionMetrics.
  4. Snapshot again.
  5. Call ds.cleanup_old_versions(older_than=timedelta(0), delete_unverified=True).
     Record CleanupStats.
  6. Snapshot again.
  7. Print a matrix: fragments, files, bytes, versions at each stage.

Hypothesis A is supported if:
  - Step-2 -> Step-4 grows in bytes and file count
  - Step-4 -> Step-6 shrinks in bytes (files cleaned up) close to pre-compact
  - Post-cleanup bytes <= pre-compact bytes

Hypothesis B is supported if:
  - Step-2 -> Step-4 grows in bytes
  - Step-4 -> Step-6 shrinks but still >> pre-compact
  - The surviving compacted fragment(s) are larger than the sum of the
    original fragments at Step 2.

We run this on local disk (not S3) so file-count is ground truth; the
M6 finding was on S3 via `aws s3 ls --summarize` which sums all prefixes
and cannot distinguish active from orphaned.
"""
import argparse
import datetime
import json
import os
import shutil
import subprocess
import time

import decimal

import lance
import numpy as np
import pyarrow as pa


def build_baseline(n_rows, seed=42):
    rng = np.random.default_rng(seed)
    raw = np.clip(rng.lognormal(3.0, 1.3, n_rows), 0, 99999.99)
    cents = np.round(raw * 100).astype(np.int64)
    decimals = [decimal.Decimal(int(c)).scaleb(-2) for c in cents]
    return pa.table({
        "id": pa.array(np.arange(n_rows, dtype=np.int64)),
        "money_decimal": pa.array(decimals, type=pa.decimal128(7, 2)),
        "value_f64": pa.array(rng.standard_normal(n_rows)),
        "category": pa.array(rng.choice(["a", "b", "c", "d"], n_rows)),
    })


def du_local(path):
    if not os.path.exists(path):
        return 0, 0
    total = 0
    n_files = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
            n_files += 1
    return total, n_files


def count_data_files(path):
    data_dir = os.path.join(path, "data")
    if not os.path.isdir(data_dir):
        return 0
    return sum(1 for _ in os.scandir(data_dir) if _.is_file())


def count_tx_files(path):
    tx_dir = os.path.join(path, "_transactions")
    if not os.path.isdir(tx_dir):
        return 0
    return sum(1 for _ in os.scandir(tx_dir) if _.is_file())


def snapshot(path, ds, label):
    total_b, n_files = du_local(path)
    data_files = count_data_files(path)
    tx_files = count_tx_files(path)
    active_frags = len(ds.get_fragments())
    versions = len(ds.versions())
    latest = ds.latest_version
    return {
        "stage": label,
        "total_bytes": total_b,
        "total_mb": round(total_b / 1e6, 3),
        "n_files": n_files,
        "data_files": data_files,
        "tx_files": tx_files,
        "active_fragments": active_frags,
        "versions": versions,
        "latest_version": latest,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline-rows", type=int, default=100_000)
    ap.add_argument("--append-rows", type=int, default=10_000)
    ap.add_argument("--n-appends", type=int, default=20)
    ap.add_argument("--out-dir", default="/tmp/lance_compact_gc")
    ap.add_argument("--json-out", default="/tmp/compact_gc_investigation.json")
    args = ap.parse_args()

    path = os.path.join(args.out_dir, "ds.lance")
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(args.out_dir, exist_ok=True)

    print(f"lance    {lance.__version__}")
    print(f"path     {path}")
    print(f"plan     baseline={args.baseline_rows} + "
          f"{args.n_appends} x {args.append_rows} appends")
    print()

    snapshots = []

    baseline = build_baseline(args.baseline_rows)
    lance.write_dataset(baseline, path, mode="overwrite",
                        data_storage_version="2.2")
    ds = lance.dataset(path)
    snapshots.append({**snapshot(path, ds, "1_baseline_written"),
                      "rows": ds.count_rows()})

    for i in range(args.n_appends):
        batch = build_baseline(args.append_rows, seed=43 + i)
        lance.write_dataset(batch, path, mode="append",
                            data_storage_version="2.2")
    ds = lance.dataset(path)
    snapshots.append({**snapshot(path, ds, "2_after_appends"),
                      "rows": ds.count_rows()})

    print("[pre-compact]")
    for k, v in snapshots[-1].items():
        print(f"  {k:<20} {v}")

    t0 = time.perf_counter()
    metrics = ds.optimize.compact_files()
    compact_seconds = time.perf_counter() - t0
    print(f"\n[compact] done in {compact_seconds:.2f}s")
    print(f"  metrics: {metrics}")

    ds = lance.dataset(path)
    snapshots.append({**snapshot(path, ds, "3_post_compact_precleanup"),
                      "rows": ds.count_rows(),
                      "compact_seconds": round(compact_seconds, 3),
                      "compact_metrics": str(metrics)})
    print("\n[post-compact, pre-cleanup]")
    for k, v in snapshots[-1].items():
        print(f"  {k:<20} {v}")

    t0 = time.perf_counter()
    cleanup_stats = ds.cleanup_old_versions(
        older_than=datetime.timedelta(0),
        delete_unverified=True,
    )
    cleanup_seconds = time.perf_counter() - t0
    print(f"\n[cleanup_old_versions(older_than=0)] done in "
          f"{cleanup_seconds:.2f}s")
    print(f"  cleanup_stats: {cleanup_stats}")

    ds = lance.dataset(path)
    snapshots.append({**snapshot(path, ds, "4_post_cleanup"),
                      "rows": ds.count_rows(),
                      "cleanup_seconds": round(cleanup_seconds, 3),
                      "cleanup_stats": str(cleanup_stats)})
    print("\n[post-cleanup]")
    for k, v in snapshots[-1].items():
        print(f"  {k:<20} {v}")

    print("\n=== SUMMARY MATRIX ===")
    headers = ["stage", "total_mb", "n_files", "data_files",
               "active_fragments", "versions"]
    print("  " + "  ".join(f"{h:>22}" for h in headers))
    for s in snapshots:
        row = []
        for h in headers:
            v = s.get(h)
            if isinstance(v, float):
                row.append(f"{v:>22.3f}")
            else:
                row.append(f"{str(v):>22}")
        print("  " + "  ".join(row))

    pre = snapshots[1]
    post_compact = snapshots[2]
    post_cleanup = snapshots[3]

    print("\n=== VERDICT ===")
    compact_growth_mb = post_compact["total_mb"] - pre["total_mb"]
    cleanup_shrink_mb = post_compact["total_mb"] - post_cleanup["total_mb"]
    compact_growth_pct = 100 * compact_growth_mb / max(pre["total_mb"], 1e-9)
    cleanup_recovery_pct = (100 * cleanup_shrink_mb /
                            max(compact_growth_mb, 1e-9))
    net_mb = post_cleanup["total_mb"] - pre["total_mb"]
    net_pct = 100 * net_mb / max(pre["total_mb"], 1e-9)

    print(f"  pre-compact total       : {pre['total_mb']:.2f} MB "
          f"({pre['data_files']} data files, "
          f"{pre['active_fragments']} active frags)")
    print(f"  post-compact total      : {post_compact['total_mb']:.2f} MB "
          f"({post_compact['data_files']} data files, "
          f"{post_compact['active_fragments']} active frags)  "
          f"(+{compact_growth_pct:.1f}%)")
    print(f"  post-cleanup total      : {post_cleanup['total_mb']:.2f} MB "
          f"({post_cleanup['data_files']} data files, "
          f"{post_cleanup['active_fragments']} active frags)  "
          f"(net {'+'if net_pct >= 0 else ''}{net_pct:.1f}% vs pre-compact)")
    print(f"  cleanup recovered       : {cleanup_shrink_mb:.2f} MB "
          f"= {cleanup_recovery_pct:.1f}% of compact growth")

    if cleanup_recovery_pct >= 80 and abs(net_pct) <= 15:
        verdict = ("HYPOTHESIS A supported: compact does NOT GC old "
                   "fragments. cleanup_old_versions recovers ~all the "
                   "inflated bytes, and net vs pre-compact is near zero.")
    elif cleanup_recovery_pct < 40:
        verdict = ("HYPOTHESIS B supported: the compacted fragment(s) "
                   "are themselves bloated. cleanup_old_versions "
                   "recovers little; the surviving data is larger than "
                   "the sum of the pre-compact fragments.")
    else:
        verdict = ("MIXED: cleanup recovered "
                   f"{cleanup_recovery_pct:.1f}% of the compact growth. "
                   "Both mechanisms contribute. Surviving bloat is "
                   f"{net_pct:.1f}% of pre-compact size.")
    print(f"\n  VERDICT: {verdict}")

    out = {
        "lance_version": lance.__version__,
        "baseline_rows": args.baseline_rows,
        "append_rows": args.append_rows,
        "n_appends": args.n_appends,
        "snapshots": snapshots,
        "deltas": {
            "compact_growth_mb": round(compact_growth_mb, 3),
            "compact_growth_pct": round(compact_growth_pct, 2),
            "cleanup_shrink_mb": round(cleanup_shrink_mb, 3),
            "cleanup_recovery_pct": round(cleanup_recovery_pct, 2),
            "net_mb_vs_pre_compact": round(net_mb, 3),
            "net_pct_vs_pre_compact": round(net_pct, 2),
        },
        "verdict": verdict,
    }
    with open(args.json_out, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n  JSON written: {args.json_out}")


if __name__ == "__main__":
    main()
