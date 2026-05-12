"""
Q_concurrent_mutation_compact.py — concurrent mutation × compaction conflict benchmark

Question
--------
When `delete` / `update` / `merge_insert` run concurrently with
`optimize.compact_files()`, how often does a writer fail vs silently retry?

Source-code prior (lance-format/lance @ 443f2daab80d):
  * check_rewrite_txn (conflict_resolver.rs#L664-L684): Rewrite vs
    Delete/Update on overlapping fragments returns RetryableCommitConflict.
  * Delete/Update/MergeInsert go through execute_with_retry
    (write/retry.rs RetryConfig default 10 retries × 30s) → inner 20 retry.
  * compact_files calls apply_commit with default CommitConfig (20 retry)
    but NO outer wrapper → semantic RetryableCommitConflict bubbles to user.

This bench empirically measures:
  (a) end-user-visible failure rate per mutation kind under concurrency N
  (b) whether compact itself fails in the same workload
  (c) latency tails under conflict-driven retry-backoff

Scenarios (x 4 concurrency levels):
  S1_delete_noc             N concurrent deletes, no compaction baseline
  S2_delete_compact         N concurrent deletes + compactor loop
  S3_update_compact         N concurrent updates + compactor loop
  S4_merge_insert_compact   N concurrent merge_inserts + compactor loop

Writer loops for WINDOW_SECONDS; compactor loops with small sleep. Each
process appends {"op_id","started","finished","error_type","error_msg",
"duration_ms"} to a per-process JSONL. Main process aggregates into
results/Q_concurrent_mutation_compact.json.

Usage:
  python Q_concurrent_mutation_compact.py            # full grid
  python Q_concurrent_mutation_compact.py --smoke    # 1 concurrency × 30s
  python Q_concurrent_mutation_compact.py --skip S1_delete_noc  # skip
"""

import argparse
import json
import multiprocessing as mp
import os
import shutil
import signal
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import lance
import numpy as np
import pyarrow as pa


SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = SCRIPT_DIR.parent / "results"
BASE_PATH = "/tmp/Q_concurrent"

BASE_ROWS = 500_000
PAYLOAD_COL_BYTES = 32
CONCURRENCIES_DEFAULT = [1, 2, 4, 8]
WINDOW_SECONDS_DEFAULT = 60
COMPACTOR_SLEEP_S = 0.5


def build_base_table(n_rows: int, seed: int = 0) -> pa.Table:
    rng = np.random.default_rng(seed)
    payload = np.array(
        [f"{v:0{PAYLOAD_COL_BYTES}x}"[:PAYLOAD_COL_BYTES]
         for v in rng.integers(0, 2**63, n_rows, dtype=np.int64)],
        dtype=object,
    )
    return pa.table({
        "id": pa.array(range(n_rows), type=pa.int64()),
        "group_id": pa.array(rng.integers(0, 1000, n_rows, dtype=np.int64)),
        "value": pa.array(rng.standard_normal(n_rows).astype(np.float32)),
        "payload": pa.array(payload, type=pa.string()),
    })


def setup_dataset(path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)
    tbl = build_base_table(BASE_ROWS)
    lance.write_dataset(tbl, path, mode="overwrite", data_storage_version="2.1",
                        max_rows_per_file=50_000)


def classify_error(exc: BaseException) -> str:
    msg = str(exc).lower()
    cls = exc.__class__.__name__
    if "retryable" in msg or "preempted" in msg:
        return "RetryableCommitConflict"
    if "incompatible" in msg:
        return "IncompatibleTransaction"
    if "too many concurrent writers" in msg or "too much write contention" in msg:
        return "TooMuchWriteContention"
    if "commit conflict" in msg or "failed to commit" in msg:
        return "CommitConflict"
    if "timeout" in msg or "retry_timeout" in msg:
        return "RetryTimeout"
    return f"Other:{cls}"


def writer_loop(
    worker_id: int,
    mutation: str,
    dataset_path: str,
    window_s: float,
    out_path: str,
    start_event,
) -> None:
    start_event.wait()
    rng = np.random.default_rng(42 + worker_id * 997)
    out = open(out_path, "w", buffering=1)
    op_id = 0
    deadline = time.perf_counter() + window_s
    while time.perf_counter() < deadline:
        op_id += 1
        target = int(rng.integers(0, BASE_ROWS))
        t0 = time.perf_counter()
        err_type = None
        err_msg = None
        try:
            ds = lance.dataset(dataset_path)
            if mutation == "delete":
                ds.delete(f"id = {target}")
            elif mutation == "update":
                ds.update({"value": f"{float(rng.standard_normal()):.6f}"},
                          where=f"id = {target}")
            elif mutation == "merge_insert":
                upsert = pa.table({
                    "id": pa.array([target], type=pa.int64()),
                    "group_id": pa.array([int(rng.integers(0, 1000))], type=pa.int64()),
                    "value": pa.array([float(rng.standard_normal())],
                                      type=pa.float32()),
                    "payload": pa.array(["x" * PAYLOAD_COL_BYTES]),
                })
                ds.merge_insert("id") \
                  .when_matched_update_all() \
                  .when_not_matched_insert_all() \
                  .execute(upsert)
            else:
                raise ValueError(f"unknown mutation {mutation}")
        except BaseException as e:
            err_type = classify_error(e)
            err_msg = str(e)[:200]
        dur_ms = (time.perf_counter() - t0) * 1000
        rec = {"worker": worker_id, "op_id": op_id, "mutation": mutation,
               "target_id": target, "duration_ms": round(dur_ms, 3),
               "error_type": err_type, "error_msg": err_msg}
        out.write(json.dumps(rec) + "\n")
    out.close()


def compactor_loop(dataset_path: str, window_s: float, out_path: str,
                   start_event) -> None:
    start_event.wait()
    out = open(out_path, "w", buffering=1)
    iter_id = 0
    deadline = time.perf_counter() + window_s
    while time.perf_counter() < deadline:
        iter_id += 1
        t0 = time.perf_counter()
        err_type = None
        err_msg = None
        metrics: Dict[str, Any] = {}
        try:
            ds = lance.dataset(dataset_path)
            m = ds.optimize.compact_files(target_rows_per_fragment=50_000)
            metrics = {"fragments_removed": m.fragments_removed,
                       "fragments_added": m.fragments_added,
                       "files_removed": m.files_removed,
                       "files_added": m.files_added}
        except BaseException as e:
            err_type = classify_error(e)
            err_msg = str(e)[:200]
        dur_ms = (time.perf_counter() - t0) * 1000
        rec = {"iter": iter_id, "duration_ms": round(dur_ms, 3),
               "error_type": err_type, "error_msg": err_msg, **metrics}
        out.write(json.dumps(rec) + "\n")
        time.sleep(COMPACTOR_SLEEP_S)
    out.close()


def aggregate(path: str, is_compactor: bool = False) -> Dict[str, Any]:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    if not records:
        return {"total": 0, "success": 0, "errors_by_type": {}, "duration_ms": {}}
    ok = [r for r in records if r.get("error_type") is None]
    err_types: Dict[str, int] = {}
    for r in records:
        et = r.get("error_type")
        if et:
            err_types[et] = err_types.get(et, 0) + 1
    durs = [r["duration_ms"] for r in records]
    ok_durs = [r["duration_ms"] for r in ok]
    agg = {
        "total": len(records),
        "success": len(ok),
        "success_rate": round(len(ok) / len(records), 4),
        "errors_by_type": err_types,
        "duration_ms": {
            "p50_all": round(statistics.median(durs), 3),
            "p99_all": round(float(np.percentile(durs, 99)), 3) if durs else 0,
            "max_all": round(max(durs), 3),
            "p50_ok": round(statistics.median(ok_durs), 3) if ok_durs else None,
            "p99_ok": round(float(np.percentile(ok_durs, 99)), 3)
                      if ok_durs else None,
        },
    }
    if is_compactor:
        agg["fragments_removed_total"] = sum(
            r.get("fragments_removed", 0) or 0 for r in records)
        agg["fragments_added_total"] = sum(
            r.get("fragments_added", 0) or 0 for r in records)
    return agg


def scenario_mutation(scenario: str) -> str:
    if "merge_insert" in scenario:
        return "merge_insert"
    if "update" in scenario:
        return "update"
    if "delete" in scenario:
        return "delete"
    raise ValueError(f"cannot infer mutation from scenario {scenario!r}")


def run_scenario(scenario: str, concurrency: int, window_s: float,
                 workspace: Path) -> Dict[str, Any]:
    mutation = scenario_mutation(scenario)
    with_compact = "compact" in scenario

    dataset_path = str(workspace / f"{scenario}_N{concurrency}")
    setup_dataset(dataset_path)

    ctx = mp.get_context("spawn")
    start_event = ctx.Event()
    procs: List[mp.Process] = []
    writer_outs = []
    for i in range(concurrency):
        op = workspace / f"writer_{scenario}_N{concurrency}_w{i}.jsonl"
        writer_outs.append(str(op))
        p = ctx.Process(target=writer_loop,
                        args=(i, mutation, dataset_path, window_s,
                              str(op), start_event),
                        daemon=False)
        procs.append(p)

    comp_out = None
    if with_compact:
        comp_out = str(workspace / f"compactor_{scenario}_N{concurrency}.jsonl")
        p = ctx.Process(target=compactor_loop,
                        args=(dataset_path, window_s, comp_out, start_event),
                        daemon=False)
        procs.append(p)

    for p in procs:
        p.start()
    time.sleep(1.0)
    start_event.set()

    t_start = time.perf_counter()
    for p in procs:
        p.join(timeout=window_s + 60)
    wall_s = time.perf_counter() - t_start

    for p in procs:
        if p.is_alive():
            os.kill(p.pid, signal.SIGKILL)
            p.join(timeout=5)

    writer_aggs = [aggregate(op) for op in writer_outs]
    comp_agg = aggregate(comp_out, is_compactor=True) if comp_out else None

    total_ops = sum(a["total"] for a in writer_aggs)
    total_ok = sum(a["success"] for a in writer_aggs)
    all_err_types: Dict[str, int] = {}
    for a in writer_aggs:
        for k, v in a["errors_by_type"].items():
            all_err_types[k] = all_err_types.get(k, 0) + v

    all_durs: List[float] = []
    for op in writer_outs:
        with open(op) as f:
            for line in f:
                line = line.strip()
                if line:
                    all_durs.append(json.loads(line)["duration_ms"])

    return {
        "scenario": scenario,
        "concurrency": concurrency,
        "window_s": window_s,
        "wall_s": round(wall_s, 2),
        "writers": {
            "total_ops": total_ops,
            "success": total_ok,
            "success_rate": round(total_ok / total_ops, 4) if total_ops else 0,
            "errors_by_type": all_err_types,
            "qps": round(total_ops / wall_s, 2) if wall_s else 0,
            "duration_ms": {
                "p50": round(statistics.median(all_durs), 3) if all_durs else 0,
                "p99": round(float(np.percentile(all_durs, 99)), 3)
                        if all_durs else 0,
                "max": round(max(all_durs), 3) if all_durs else 0,
            },
        },
        "compactor": comp_agg,
        "per_writer": writer_aggs,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true",
                    help="quick smoke: 1 concurrency × 30s, one scenario only")
    ap.add_argument("--window", type=float, default=WINDOW_SECONDS_DEFAULT)
    ap.add_argument("--concurrencies", type=int, nargs="+",
                    default=CONCURRENCIES_DEFAULT)
    ap.add_argument("--scenarios", type=str, nargs="+",
                    default=["S1_delete_noc", "S2_delete_compact",
                             "S3_update_compact", "S4_merge_insert_compact"])
    ap.add_argument("--skip", type=str, nargs="*", default=[])
    ap.add_argument("--workspace", type=str, default=BASE_PATH)
    args = ap.parse_args()

    if args.smoke:
        args.concurrencies = [1]
        args.window = 30
        args.scenarios = args.scenarios[:1]

    workspace = Path(args.workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    mp.set_start_method("spawn", force=True)

    all_results: List[Dict[str, Any]] = []
    for scenario in args.scenarios:
        if scenario in args.skip:
            print(f"[Q] SKIP {scenario}")
            continue
        for c in args.concurrencies:
            print(f"[Q] === {scenario} N={c} window={args.window}s ===")
            r = run_scenario(scenario, c, args.window, workspace)
            all_results.append(r)
            w = r["writers"]
            print(f"[Q]   writers: {w['total_ops']} ops, "
                  f"success_rate={w['success_rate']*100:.2f}%, "
                  f"errors={w['errors_by_type']}, qps={w['qps']}")
            if r["compactor"]:
                c_agg = r["compactor"]
                print(f"[Q]   compactor: {c_agg['total']} iters, "
                      f"success={c_agg['success']}, "
                      f"errors={c_agg['errors_by_type']}, "
                      f"fragments_removed={c_agg.get('fragments_removed_total', 0)}")

    RESULTS_DIR.mkdir(exist_ok=True)
    out_file = RESULTS_DIR / ("Q_smoke.json" if args.smoke
                              else "Q_concurrent_mutation_compact.json")
    with open(out_file, "w") as f:
        json.dump({
            "run_id": time.strftime("%Y%m%d-%H%M%S"),
            "pylance_version": lance.__version__,
            "base_rows": BASE_ROWS,
            "args": vars(args),
            "results": all_results,
        }, f, indent=2)
    print(f"[Q] wrote {out_file}")


if __name__ == "__main__":
    main()
