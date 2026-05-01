import argparse
import gc
import json
import os
import shutil
import tempfile
import time

import lance
import numpy as np
import pyarrow as pa


def build_table(n_rows, start_id=0, seed=42):
    rng = np.random.default_rng(seed + start_id)
    return pa.table({
        "id": pa.array(range(start_id, start_id + n_rows), type=pa.int64()),
        "value": pa.array(rng.standard_normal(n_rows).astype(np.float32)),
        "updated_at": pa.array(rng.integers(0, 10_000_000, n_rows, dtype=np.int64)),
    })


def build_upsert_batch(n_matches, n_new, base_n, seed=99):
    if n_matches > base_n:
        raise ValueError(f"n_matches={n_matches} exceeds base_n={base_n}")
    rng = np.random.default_rng(seed)
    match_ids = rng.choice(base_n, n_matches, replace=False)
    new_ids = np.arange(base_n, base_n + n_new)
    all_ids = np.concatenate([match_ids, new_ids])
    return pa.table({
        "id": pa.array(all_ids, type=pa.int64()),
        "value": pa.array(rng.standard_normal(len(all_ids)).astype(np.float32)),
        "updated_at": pa.array(np.full(len(all_ids), 99_999_999, dtype=np.int64)),
    })


def run_merge_insert_test(uri, base_n, n_matches, n_new, with_index):
    shutil.rmtree(uri, ignore_errors=True)
    base_tbl = build_table(base_n)
    lance.write_dataset(base_tbl, uri, mode="overwrite")
    ds = lance.dataset(uri)

    if with_index:
        ds.create_scalar_index("id", index_type="BTREE")
        ds = lance.dataset(uri)

    upsert_tbl = build_upsert_batch(n_matches, n_new, base_n)
    ds_for_merge = lance.dataset(uri)

    gc.collect()
    t0 = time.perf_counter()
    ds_for_merge.merge_insert("id") \
        .when_matched_update_all() \
        .when_not_matched_insert_all() \
        .execute(upsert_tbl)
    single_elapsed = time.perf_counter() - t0

    verify_ds = lance.dataset(uri)
    final_count = verify_ds.count_rows()
    expected = base_n + n_new

    return {
        "elapsed_s": round(single_elapsed, 3),
        "n_matches": n_matches,
        "n_new": n_new,
        "throughput_rows_s": round((n_matches + n_new) / single_elapsed, 0),
        "final_rowcount": final_count,
        "expected_rowcount": expected,
        "rowcount_ok": final_count == expected,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-rows", type=int, default=1_000_000)
    ap.add_argument("--work-dir", default=tempfile.mkdtemp(prefix="f_merge_"))
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/F_merge_insert.json")
    args = ap.parse_args()

    os.makedirs(args.work_dir, exist_ok=True)

    grid = [
        {"n_matches": 1_000, "n_new": 100},
        {"n_matches": 10_000, "n_new": 1_000},
        {"n_matches": 100_000, "n_new": 10_000},
        {"n_matches": 500_000, "n_new": 50_000},
    ]

    results = []
    for index_mode in [False, True]:
        print(f"\n=== with_index={index_mode}")
        for cfg in grid:
            uri = os.path.join(
                args.work_dir,
                f"merge_{'idx' if index_mode else 'noidx'}_{cfg['n_matches']}.lance",
            )
            print(f"  Running base={args.base_rows:,} matches={cfg['n_matches']:,} "
                  f"new={cfg['n_new']:,} with_index={index_mode}")
            try:
                r = run_merge_insert_test(
                    uri, args.base_rows,
                    cfg["n_matches"], cfg["n_new"], index_mode,
                )
                r["with_index"] = index_mode
                r["base_rows"] = args.base_rows
                results.append(r)
                print(f"    elapsed={r['elapsed_s']}s  "
                      f"throughput={r['throughput_rows_s']:.0f} rows/s")
            except Exception as e:
                print(f"    ERROR: {e}")
            finally:
                shutil.rmtree(uri, ignore_errors=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "lance_version": lance.__version__,
            "base_rows": args.base_rows,
            "timing_method": "single_shot",
            "results": results,
        }, f, indent=2)

    print(f"\nSaved to {args.out}")
    print("\n=== Summary: with_index vs no_index speedup")
    by_match = {}
    for r in results:
        key = r["n_matches"]
        by_match.setdefault(key, {})[r["with_index"]] = r
    for k in sorted(by_match.keys()):
        d = by_match[k]
        if True in d and False in d:
            noidx = d[False]["elapsed_s"]
            withidx = d[True]["elapsed_s"]
            print(f"  matches={k:>7}: no_index={noidx:6.2f}s  with_index={withidx:6.2f}s  "
                  f"speedup={noidx/withidx:.2f}x")


if __name__ == "__main__":
    main()
