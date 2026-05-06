"""Attribute Lance compact's post-call directory growth: how much is
MVCC history (old fragments retained intentionally for time travel) vs
how much is compacted output that is itself larger than the original?

Context (M6 earlier finding, since corrected): Lance
`ds.optimize.compact_files()` grows the on-disk *directory* size by
73-76% in both sf1 and sf10 S3 runs. Naive `du` cannot tell apart:
  A. OLD FRAGMENTS RETAINED by design -- Lance/Iceberg are MVCC; old
     version fragments are what make `checkout_version(N)` / time
     travel / rollback work. They are features, not leaks.
     `cleanup_old_versions(older_than=...)` discards them when the
     user no longer needs the history.
  B. COMPACTED OUTPUT ITSELF LARGER than the sum of the originals --
     would be a real encoding-path regression, NOT recoverable by
     cleanup.

Experiment protocol:
  1. Write baseline + N small appends -> `ds` has N+1 versions, all
     referenced by the current version.
  2. Call `compact_files()`. Old fragments move from 'current-version
     referenced' to 'only referenced by historical versions'.
  3. Call `cleanup_old_versions(older_than=timedelta(0))`. Discards all
     versions that are not the latest. Equivalent to telling Lance
     'I do not need time travel, reclaim the space.'
  4. Compare active-version bytes at each stage.

Interpretation:
  - If post-cleanup bytes ~= pre-compact bytes -> hypothesis A, what
    looked like bloat was retained MVCC history.
  - If post-cleanup bytes >> pre-compact bytes -> hypothesis B, the
    compacted output itself is larger than the inputs combined.

We run locally so file-count is ground truth. S3 `aws s3 ls` sums
every subfolder and cannot distinguish active from historical files.
"""
import argparse
import datetime
import decimal
import json
import os
import shutil
import subprocess
import time

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
        verdict = ("HYPOTHESIS A confirmed. Post-compact directory growth "
                   "is MVCC history (old versions retained by design for "
                   "time travel). cleanup_old_versions() discards the "
                   "history and active size returns to near the pre-compact "
                   "baseline. This is not a bug -- Iceberg has identical "
                   "semantics via expire_snapshots.")
    elif cleanup_recovery_pct < 40:
        verdict = ("HYPOTHESIS B confirmed. The compacted output itself is "
                   "larger than the sum of the pre-compact fragments. "
                   "cleanup_old_versions cannot recover the difference. "
                   "This WOULD be a real encoding-path regression.")
    else:
        verdict = (f"MIXED: cleanup recovered {cleanup_recovery_pct:.1f}% "
                   "of the post-compact directory growth. Retained MVCC "
                   "history explains most of the increase, but a small "
                   f"fraction ({net_pct:.1f}% of pre-compact size) appears "
                   "to be compacted output that is marginally larger than "
                   "inputs. Worth re-running to check noise.")
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
