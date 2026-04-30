import argparse
import gc
import json
import os
import shutil
import signal
import tempfile
import time

import lance
import pyarrow as pa


class TimeoutError(Exception):
    pass


def _timeout_handler(signum, frame):
    raise TimeoutError()


def make_table(n):
    return pa.table({
        "x": pa.array(range(n), type=pa.int32()),
        "v": pa.array([0] * n, type=pa.int32()),
    })


def bench_one(n_rows, rows_affected, stable, storage, max_rows_per_file, tmp_root,
              timeout_s):
    uri = os.path.join(tmp_root, f"update_bench_{os.getpid()}_{time.time_ns()}.lance")
    try:
        shutil.rmtree(uri, ignore_errors=True)
        t = make_table(n_rows)
        lance.write_dataset(
            t,
            uri,
            mode="create",
            data_storage_version=storage,
            enable_stable_row_ids=stable,
            max_rows_per_file=max_rows_per_file,
        )
        ds = lance.dataset(uri)
        where = f"x < {rows_affected}"

        gc.collect()
        gc.collect()

        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(timeout_s)
        t0 = time.perf_counter()
        try:
            res = ds.update({"v": "1"}, where=where)
        finally:
            signal.alarm(0)
        elapsed = time.perf_counter() - t0

        if isinstance(res, dict):
            n_updated = res.get("num_rows_updated", -1)
        else:
            n_updated = getattr(res, "num_rows_updated", -1)

        if n_updated != rows_affected:
            print(f"  WARN: expected {rows_affected} updated, got {n_updated}")

        return elapsed, n_updated
    finally:
        shutil.rmtree(uri, ignore_errors=True)


def build_grid():
    rows_affected_values = [1, 1_000, 10_000, 100_000, 500_000]
    stable_values = [False, True]
    storage_values = ["2.0", "2.1"]
    max_rows_per_file_values = [1_048_576, 100_000]

    grid = []
    for r in rows_affected_values:
        for s in stable_values:
            for sv in storage_values:
                for mrpf in max_rows_per_file_values:
                    grid.append({
                        "rows_affected": r,
                        "stable": s,
                        "storage": sv,
                        "max_rows_per_file": mrpf,
                    })
    return grid


def summarize_ratios(results):
    by_key = {}
    for row in results:
        key = (row["rows_affected"], row["storage"], row["max_rows_per_file"])
        by_key.setdefault(key, {})[row["stable"]] = row["seconds"]

    ratios = []
    for key, d in sorted(by_key.items()):
        if True in d and False in d and d[False] > 0:
            ratios.append({
                "rows_affected": key[0],
                "storage": key[1],
                "max_rows_per_file": key[2],
                "off_seconds": d[False],
                "on_seconds": d[True],
                "ratio": d[True] / d[False],
            })
    return ratios


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=1_000_000)
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/A_update_bug.json")
    ap.add_argument("--tmp-root", default=tempfile.gettempdir())
    ap.add_argument("--timeout-seconds", type=int, default=600)
    args = ap.parse_args()

    grid = build_grid()
    print(f"Running {len(grid)} configs on {args.n_rows} row tables, pylance={lance.__version__}")

    results = []
    skip_expensive = set()

    for i, cfg in enumerate(grid, 1):
        skip_key = (cfg["stable"], cfg["storage"], cfg["max_rows_per_file"])
        if cfg["stable"] and skip_key in skip_expensive:
            print(f"[{i}/{len(grid)}] SKIP (prior timeout/failure) cfg={cfg}", flush=True)
            continue

        try:
            elapsed, n_updated = bench_one(
                n_rows=args.n_rows,
                rows_affected=cfg["rows_affected"],
                stable=cfg["stable"],
                storage=cfg["storage"],
                max_rows_per_file=cfg["max_rows_per_file"],
                tmp_root=args.tmp_root,
                timeout_s=args.timeout_seconds,
            )
            row = dict(cfg, seconds=round(elapsed, 4), updated=n_updated)
            results.append(row)
            print(f"[{i}/{len(grid)}] {row}", flush=True)
        except TimeoutError:
            print(f"[{i}/{len(grid)}] TIMEOUT >{args.timeout_seconds}s cfg={cfg}", flush=True)
            if cfg["stable"]:
                skip_expensive.add(skip_key)
        except Exception as e:
            print(f"[{i}/{len(grid)}] ERROR cfg={cfg}: {e}", flush=True)

    ratios = summarize_ratios(results)
    payload = {
        "pylance_version": lance.__version__,
        "timestamp": time.time(),
        "n_rows": args.n_rows,
        "timeout_seconds": args.timeout_seconds,
        "results": results,
        "ratios": ratios,
    }

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"\nSaved {len(results)} rows to {args.out}")
    print(f"Computed {len(ratios)} stable_on/stable_off ratios")
    print("\nTop 10 worst slowdowns (stable_on / stable_off):")
    for r in sorted(ratios, key=lambda r: -r["ratio"])[:10]:
        print(f"  rows={r['rows_affected']:>8} storage={r['storage']} "
              f"mrpf={r['max_rows_per_file']:>7}: "
              f"off={r['off_seconds']:.4f}s on={r['on_seconds']:.4f}s "
              f"ratio={r['ratio']:.1f}x")


if __name__ == "__main__":
    main()
