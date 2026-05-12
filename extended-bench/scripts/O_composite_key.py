"""
O_composite_key.py — Composite-key filter benchmark on Lance
=============================================================
Question: For queries of the form `WHERE video_id = X AND frame_id = Y`,
what's the fastest approach on Lance (pylance 4.0.1, no native compound index)?

Variants tested (all on the SAME underlying table so indexes/layouts are the only delta):
  V0   baseline              no index, full scan
  V1   dual BTREE            separate BTREE on video_id and frame_id (maintainer-recommended)
  V2   bitmap+btree          BITMAP(video_id) + BTREE(frame_id)  (recommended when video_id low-card)
  V3   concat_btree          BTREE on a single concatenated "video_id|frame_id" string column
  V4   sort+btree_prefix     rows sorted by (video_id, frame_id) at write; BTREE only on video_id.
                              Queries on frame_id alone have NO index in this variant — Lance 2.1
                              does not auto-build zonemaps from row order, so Q_frame on V4 is
                              effectively equivalent to V0 baseline (documented in results JSON).

Workloads:
  Q_point   point lookup      video_id = X AND frame_id = Y             (the core question)
  Q_range   video range       video_id = X AND frame_id BETWEEN a AND b (sees who retains range ability)
  Q_vid    single col         video_id = X                              (all frames of one video)
  Q_frame  other col alone   frame_id = Y                               (stresses variants that "buried" frame_id)

Dataset sizing (tunable via --rows and --videos):
  default 10M rows, 10k videos → avg 1000 frames/video
  payload column ~40B hex so table has realistic row weight

Outputs JSON to results/O_composite_key.json in the same style as B3/B4.
"""

import argparse
import gc
import json
import os
import shutil
import statistics
import time

import lance
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


def build_table(n_rows: int, n_videos: int, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed)
    frames_per_video_cap = max(100, (n_rows // n_videos) * 2)

    video_ids = rng.integers(0, n_videos, n_rows, dtype=np.int64)
    frame_ids = rng.integers(0, frames_per_video_cap, n_rows, dtype=np.int64)
    payload = np.array(
        [f"{v:016x}" for v in rng.integers(0, 2**63, n_rows, dtype=np.int64)],
        dtype=object,
    )

    tbl = pa.table({
        "video_id":   pa.array(video_ids, type=pa.int64()),
        "frame_id":   pa.array(frame_ids, type=pa.int64()),
        # vf_key is pre-computed at write time so V3's Q_point timing measures
        # only the index lookup, not the string-concat cost (fair comparison).
        "vf_key":     pa.array(
            [f"{v}|{f}" for v, f in zip(video_ids.tolist(), frame_ids.tolist())],
            type=pa.string(),
        ),
        "payload":    pa.array(payload, type=pa.string()),
    })
    return tbl


def sorted_copy(tbl: pa.Table) -> pa.Table:
    indices = pc.sort_indices(
        tbl,
        sort_keys=[("video_id", "ascending"), ("frame_id", "ascending")],
    )
    return tbl.take(indices)


def timed(fn, warmup: int = 3, rounds: int = 10):
    # Warmup loads page cache + JIT caches; first runs otherwise show 10-100x outlier latency.
    for _ in range(warmup):
        fn()
        gc.collect()
    runs = []
    rows_returned = 0
    for _ in range(rounds):
        gc.collect()
        t0 = time.perf_counter()
        out = fn()
        runs.append(time.perf_counter() - t0)
        rows_returned = out.num_rows
    return {
        "median_ms": round(statistics.median(runs) * 1000, 3),
        "mean_ms":   round(statistics.mean(runs) * 1000, 3),
        "p90_ms":    round(float(np.percentile(runs, 90)) * 1000, 3),
        "min_ms":    round(min(runs) * 1000, 3),
        "max_ms":    round(max(runs) * 1000, 3),
        "stdev_ms":  round(statistics.stdev(runs) * 1000, 3) if len(runs) > 1 else 0.0,
        "rows_returned": rows_returned,
    }



def du(path: str) -> int:
    if not os.path.exists(path):
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def build_variant(tbl: pa.Table, root: str, name: str, index_spec, sort: bool):
    path = os.path.join(root, f"{name}.lance")
    if os.path.exists(path):
        shutil.rmtree(path)

    data = sorted_copy(tbl) if sort else tbl
    lance.write_dataset(data, path, mode="overwrite", data_storage_version="2.1")

    ds = lance.dataset(path)
    t0 = time.perf_counter()
    failures = []
    for col, idx_type in index_spec:
        try:
            ds.create_scalar_index(col, index_type=idx_type, replace=True)
        except Exception as e:
            failures.append({"col": col, "index_type": idx_type, "error": str(e)[:240]})
    build_s = time.perf_counter() - t0
    # Reopen required for scalar-index visibility; see B3_selectivity_sweep.py:93, F_merge_insert.py:45.
    ds = lance.dataset(path)
    return {
        "path": path,
        "size_mb": round(du(path) / 1e6, 2),
        "build_index_seconds": round(build_s, 3),
        "index_spec": [[c, t] for c, t in index_spec],
        "index_failures": failures,
    }


def make_workloads(n_videos: int, frames_per_video_cap: int, seed: int = 1337):
    """Stable seed ensures every variant is tested with identical (video_id, frame_id) targets.
    Without this, per-variant results would vary due to different target selectivity."""
    rng = np.random.default_rng(seed)
    target_videos = sorted(set(rng.integers(0, n_videos, 8).tolist()))[:5]
    target_frames = sorted(set(rng.integers(0, frames_per_video_cap, 8).tolist()))[:5]
    range_lo, range_hi = 100, 199
    return {
        "videos": target_videos,
        "frames": target_frames,
        "range_lo": range_lo,
        "range_hi": range_hi,
    }


def run_queries(ds, variant: str, wl: dict) -> list:
    results = []

    for v in wl["videos"]:
        for f in wl["frames"]:
            if variant == "V3_concat_btree":
                pred = f"vf_key = '{v}|{f}'"
            else:
                pred = f"video_id = {v} AND frame_id = {f}"
            def run(p=pred):
                return ds.to_table(columns=["video_id", "frame_id", "payload"], filter=p)
            try:
                r = timed(run)
                r.update(workload="Q_point", predicate=pred)
                results.append(r)
            except Exception as e:
                results.append({"workload": "Q_point", "predicate": pred, "error": str(e)[:240]})

    for v in wl["videos"]:
        if variant == "V3_concat_btree":
            # V3's vf_key is lexicographic ("100" < "9" < "99"), so string-range
            # on the concatenated key cannot express a numeric range on frame_id.
            # Record the structural limitation rather than run a misleading query.
            results.append({
                "workload": "Q_range",
                "predicate": f"video_id = {v} AND frame_id BETWEEN {wl['range_lo']} AND {wl['range_hi']}",
                "error": "concat-key cannot express numeric range query on frame_id (string lex ≠ numeric)",
            })
            continue
        pred = f"video_id = {v} AND frame_id BETWEEN {wl['range_lo']} AND {wl['range_hi']}"
        def run(p=pred):
            return ds.to_table(columns=["video_id", "frame_id", "payload"], filter=p)
        try:
            r = timed(run)
            r.update(workload="Q_range", predicate=pred)
            results.append(r)
        except Exception as e:
            results.append({"workload": "Q_range", "predicate": pred, "error": str(e)[:240]})

    for v in wl["videos"]:
        if variant == "V3_concat_btree":
            results.append({
                "workload": "Q_vid",
                "predicate": f"video_id = {v}",
                "error": "concat-key cannot accelerate single-column filter; would require full scan",
            })
            continue
        pred = f"video_id = {v}"
        def run(p=pred):
            return ds.to_table(columns=["video_id", "frame_id", "payload"], filter=p)
        try:
            r = timed(run)
            r.update(workload="Q_vid", predicate=pred)
            results.append(r)
        except Exception as e:
            results.append({"workload": "Q_vid", "predicate": pred, "error": str(e)[:240]})

    for f in wl["frames"]:
        if variant == "V3_concat_btree":
            results.append({
                "workload": "Q_frame",
                "predicate": f"frame_id = {f}",
                "error": "concat-key cannot accelerate single-column filter; would require full scan",
            })
            continue
        pred = f"frame_id = {f}"
        def run(p=pred):
            return ds.to_table(columns=["video_id", "frame_id", "payload"], filter=p)
        try:
            r = timed(run)
            r.update(workload="Q_frame", predicate=pred)
            results.append(r)
        except Exception as e:
            results.append({"workload": "Q_frame", "predicate": pred, "error": str(e)[:240]})

    return results


def summarize(qresults: list) -> dict:
    by_wl = {}
    for r in qresults:
        wl = r["workload"]
        by_wl.setdefault(wl, []).append(r)

    summary = {}
    for wl, rs in by_wl.items():
        ok = [r for r in rs if "median_ms" in r]
        if not ok:
            summary[wl] = {"n_runs": 0, "n_errors": len(rs), "error_sample": rs[0].get("error", "")}
            continue
        medians = [r["median_ms"] for r in ok]
        total_rows = sum(r["rows_returned"] for r in ok)
        summary[wl] = {
            "n_runs":         len(ok),
            "median_ms":      round(statistics.median(medians), 3),
            "p90_ms":         round(float(np.percentile(medians, 90)), 3) if len(medians) >= 2 else round(medians[0], 3),
            "min_ms":         round(min(medians), 3),
            "max_ms":         round(max(medians), 3),
            "avg_rows":       round(total_rows / len(ok), 1),
            "total_rows":     total_rows,
        }
    return summary



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows",    type=int, default=10_000_000, help="Total rows (default 10M)")
    ap.add_argument("--videos",  type=int, default=10_000,     help="Distinct video_id values (default 10k → avg 1000 frames/video)")
    ap.add_argument("--root",    type=str, default=os.environ.get("OUT_ROOT", "/tmp/lance_O"), help="Local working dir for datasets")
    ap.add_argument("--out",     type=str, default=None, help="Results JSON path (default: $REPO/results/O_composite_key.json)")
    ap.add_argument("--skip",    type=str, nargs="*", default=[], help="Skip variants by name (e.g. V4_sort_prefix)")
    args = ap.parse_args()

    os.makedirs(args.root, exist_ok=True)
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    out_path = args.out or os.path.join(repo_root, "results", "O_composite_key.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    print(f"[O] Building table: {args.rows:,} rows × {args.videos:,} videos → ~{args.rows // args.videos} frames/video")
    t0 = time.perf_counter()
    tbl = build_table(args.rows, args.videos)
    frames_per_video_cap = max(100, (args.rows // args.videos) * 2)
    print(f"[O] table built in {time.perf_counter() - t0:.1f}s (columns: {tbl.column_names})")

    workloads = make_workloads(args.videos, frames_per_video_cap)
    print(f"[O] workloads: videos={workloads['videos']}, frames={workloads['frames']}, range=[{workloads['range_lo']},{workloads['range_hi']}]")

    variants = [
        ("V0_baseline",     [],                                                 False),
        ("V1_dual_btree",   [("video_id", "BTREE"),  ("frame_id", "BTREE")],    False),
        ("V2_bitmap_btree", [("video_id", "BITMAP"), ("frame_id", "BTREE")],    False),
        ("V3_concat_btree", [("vf_key",   "BTREE")],                            False),
        ("V4_sort_prefix",  [("video_id", "BTREE")],                            True),
    ]

    all_results = {
        "run_id": os.environ.get("RUN_ID", time.strftime("%Y%m%d-%H%M%S")),
        "pylance_version": lance.__version__,
        "pyarrow_version": pa.__version__,
        "rows": args.rows,
        "videos": args.videos,
        "frames_per_video_avg": args.rows // args.videos,
        "workloads_meta": workloads,
        "variants": {},
    }

    for name, idx_spec, sort in variants:
        if name in args.skip:
            print(f"[O] {name}: skipped")
            continue
        print(f"\n[O] === {name} ===  (indexes={idx_spec}, sort={sort})")
        t0 = time.perf_counter()
        try:
            meta = build_variant(tbl, args.root, name, idx_spec, sort)
            print(f"[O] {name} built in {time.perf_counter()-t0:.1f}s  size={meta['size_mb']} MB  idx_build={meta['build_index_seconds']}s")
            if meta["index_failures"]:
                print(f"[O] {name} INDEX FAILURES: {meta['index_failures']}")
            ds = lance.dataset(meta["path"])
            qres = run_queries(ds, name, workloads)
            all_results["variants"][name] = {
                **meta,
                "summary": summarize(qres),
                "raw_queries": qres,
            }
            shutil.rmtree(meta["path"], ignore_errors=True)
        except Exception as e:
            print(f"[O] {name} FATAL: {e}")
            all_results["variants"][name] = {"fatal_error": str(e)[:500]}

    v4 = all_results["variants"].get("V4_sort_prefix", {})
    if "summary" in v4 and "Q_frame" in v4["summary"]:
        v4["summary"]["Q_frame"]["internal_verify_note"] = (
            "V4 has BTREE on video_id only; no index on frame_id. "
            "Lance 2.1 does not auto-build zonemaps from row-order, so Q_frame on V4 is a full scan "
            "(expect parity with V0 baseline, not speedup)."
        )

    row_mismatches = []
    ref_variant = "V0_baseline"
    ref_raw = all_results["variants"].get(ref_variant, {}).get("raw_queries", [])

    def _by_workload(raws):
        out = {}
        for r in raws:
            if "rows_returned" in r:
                out.setdefault(r["workload"], []).append(r["rows_returned"])
        return out

    ref_rows = _by_workload(ref_raw)
    for name, vdata in all_results["variants"].items():
        if name == ref_variant or "raw_queries" not in vdata:
            continue
        got_rows = _by_workload(vdata["raw_queries"])
        for wl, got_list in got_rows.items():
            ref_list = ref_rows.get(wl, [])
            if len(got_list) != len(ref_list):
                row_mismatches.append({
                    "variant": name, "workload": wl,
                    "reason": "length_mismatch",
                    "expected_n": len(ref_list), "got_n": len(got_list),
                })
                continue
            for i, (a, b) in enumerate(zip(ref_list, got_list)):
                if a != b:
                    row_mismatches.append({
                        "variant": name, "workload": wl, "index": i,
                        "expected": a, "got": b,
                    })
    all_results["internal_verify"] = {
        "cross_variant_row_count_mismatches": row_mismatches,
        "ok": len(row_mismatches) == 0,
    }
    if row_mismatches:
        print(f"[O] WARNING: {len(row_mismatches)} cross-variant row-count mismatches (see internal_verify)")

    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\n[O] results written to {out_path}")

    print("\n" + "=" * 72)
    print(f"  {'variant':<20} {'Q_point':>10} {'Q_range':>10} {'Q_vid':>10} {'Q_frame':>10}")
    print("=" * 72)
    for name, _, _ in variants:
        v = all_results["variants"].get(name, {})
        if "fatal_error" in v:
            print(f"  {name:<20}  FATAL: {v['fatal_error'][:50]}")
            continue
        s = v.get("summary", {})
        def fmt(wl):
            x = s.get(wl, {})
            if "median_ms" not in x:
                return "N/A"
            return f"{x['median_ms']:.1f}ms"
        print(f"  {name:<20} {fmt('Q_point'):>10} {fmt('Q_range'):>10} {fmt('Q_vid'):>10} {fmt('Q_frame'):>10}")
    print("=" * 72)


if __name__ == "__main__":
    main()
