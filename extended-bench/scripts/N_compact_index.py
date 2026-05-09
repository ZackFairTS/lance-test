"""N_compact_index.py — Compaction × Scalar/Vector Index Interaction Audit

Hypothesis under test (stated by user, 2026-05-09):
  "Compaction may invalidate existing scalar indexes and force rebuild."

Ground truth established from lance-format/lance source (pinned SHA
d8542b539a039550a8a4dba00a222988906f4cb5, 2026-05-08):

  Modern Lance does NOT invalidate indexes on compact_files. Two paths:
  * Default (defer_index_remap=False): IndexRemapper rewrites on-disk
    index files in the SAME transaction as the fragment rewrite. Index
    name preserved, UUID changes, fragment_bitmap updated.
  * Deferred (defer_index_remap=True): Index files on disk are NOT
    touched. A system index named __lance_frag_reuse (FRI) is appended
    and translates old->new row addresses at index-load time. UUID is
    preserved. Old files remain in _indices/.

  If stable row IDs are enabled (enable_stable_row_ids=True at write),
  needs_remapping becomes False automatically -- UUID stays, no FRI.

This script empirically validates those invariants for 9 index types
on pylance 4.0.1, and measures the latency/size tradeoffs of the two
compact paths.

Per-index-type behaviour under compaction has ZERO upstream CI coverage
for ZoneMap, BloomFilter, LabelList, NGram. This script is the first
public benchmark asserting they work.

Methodology (mirrors upstream tests in rust/lance/src/dataset/optimize.rs
lines 3485-4194):

  For each (index_type, compact_path) combo:
    1. Fresh dataset, K fragments via max_rows_per_file
    2. Ground-truth query results (brute-force scan) captured BEFORE index
    3. Build index -> capture {uuid, fragment_bitmap, on-disk-size, p50 latency}
    4. compact_files(defer_index_remap=<bool>)
    5. Capture post-compact state + query results
    6. Assert:
         a. post_counts == pre_counts                    (correctness)
         b. post.fragment_bitmap intersects new frag ids (bitmap rewritten)
         c. default path: post.uuid != pre.uuid          (index rewritten)
         d. defer   path: post.uuid == pre.uuid          (index NOT rewritten)
         e. defer   path: "__lance_frag_reuse" in list_indices()
    7. Cleanup old versions and re-snapshot size

MVCC size rule (METHODOLOGY_CORRECTION_size.md): every size number is
tagged active_bytes vs total_bytes. We report both. `_indices/` is
broken out as a third bucket.

Usage:
    python N_compact_index.py --work-dir /tmp/N_compact --smoke
    python N_compact_index.py --work-dir /tmp/N_compact --n-rows 1000000
    python N_compact_index.py \
        --work-dir s3://lance-benchmark-XXXX-ap-northeast-1/N_compact \
        --region ap-northeast-1 --n-rows 10000000

Output: /home/hadoop/lance-extended-bench/results/N_compact_index.json
"""
import argparse
import gc
import json
import os
import re
import shutil
import statistics
import time
import traceback
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, Optional

import lance
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


CAT_VOCAB = ["A", "B", "C", "D", "E"]
TAG_VOCAB = [f"tag_{i:02d}" for i in range(12)]
ADJECTIVES = ["quick", "lazy", "happy", "sad", "bright", "dark", "fast", "slow"]
NOUNS = ["fox", "dog", "cat", "bird", "mouse", "tree", "river", "mountain"]
VEC_DIM = 32


def make_batch(offset: int, n: int, *, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed + offset)
    ids = np.arange(offset, offset + n, dtype=np.int64)
    cats = rng.choice(CAT_VOCAB, n)
    prices = rng.uniform(0.0, 1000.0, n).astype(np.float64)
    texts = [
        f"the {rng.choice(ADJECTIVES)} {rng.choice(NOUNS)} jumps over the "
        f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS)} number {int(x) % 997}"
        for x in ids
    ]
    tags = [list(rng.choice(TAG_VOCAB, size=rng.integers(2, 5), replace=False))
            for _ in ids]
    vecs = rng.standard_normal((n, VEC_DIM)).astype(np.float32)
    return pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "cat": pa.array(cats, type=pa.string()),
        "price": pa.array(prices, type=pa.float64()),
        "text": pa.array(texts, type=pa.string()),
        "tags": pa.array(tags, type=pa.list_(pa.string())),
        "vec": pa.FixedSizeListArray.from_arrays(
            pa.array(vecs.flatten(), type=pa.float32()), VEC_DIM,
        ),
    })


def write_dataset(uri: str, n_rows: int, rows_per_fragment: int,
                  *, storage_options: Optional[dict] = None,
                  data_storage_version: str = "2.2",
                  stable_row_ids: bool = False) -> dict:
    """Write a multi-fragment dataset via repeated append.

    Gotcha: lance.write_dataset with max_rows_per_file on a single table
    does not reliably produce multiple fragments. Appending multiple
    batches does, because each append creates fresh fragment(s).
    """
    n_batches = (n_rows + rows_per_fragment - 1) // rows_per_fragment
    t0 = time.perf_counter()
    for i in range(n_batches):
        offset = i * rows_per_fragment
        size = min(rows_per_fragment, n_rows - offset)
        tbl = make_batch(offset, size)
        mode = "overwrite" if i == 0 else "append"
        kwargs = dict(mode=mode, data_storage_version=data_storage_version,
                      max_rows_per_file=rows_per_fragment)
        if i == 0 and stable_row_ids:
            kwargs["enable_stable_row_ids"] = True
        if storage_options:
            kwargs["storage_options"] = storage_options
        lance.write_dataset(tbl, uri, **kwargs)
    elapsed = time.perf_counter() - t0
    ds = lance.dataset(uri, storage_options=storage_options)
    return {
        "n_fragments": len(ds.get_fragments()),
        "n_rows": ds.count_rows(),
        "elapsed_s": round(elapsed, 3),
        "version": ds.version,
    }


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://") or uri.startswith("s3a://")


def _local_list(path: str) -> list[tuple[str, int]]:
    results = []
    if not os.path.exists(path):
        return results
    for root, _, files in os.walk(path):
        for name in files:
            abs_path = os.path.join(root, name)
            try:
                rel = os.path.relpath(abs_path, path)
                results.append((rel, os.path.getsize(abs_path)))
            except OSError:
                pass
    return results


def _s3_list(uri: str, region: Optional[str]) -> list[tuple[str, int]]:
    import boto3
    assert uri.startswith("s3://")
    bucket, _, prefix = uri[len("s3://"):].partition("/")
    prefix = prefix.rstrip("/") + "/" if prefix else ""
    s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    out = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            rel = obj["Key"][len(prefix):] if prefix else obj["Key"]
            out.append((rel, obj["Size"]))
    return out


def list_uri(uri: str, region: Optional[str]) -> list[tuple[str, int]]:
    if is_s3(uri):
        return _s3_list(uri, region)
    return _local_list(uri)


def rm_uri(uri: str, region: Optional[str]) -> None:
    if is_s3(uri):
        import boto3
        bucket, _, prefix = uri[len("s3://"):].partition("/")
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        s3 = boto3.client("s3", region_name=region) if region else boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        batch = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                batch.append({"Key": obj["Key"]})
                if len(batch) >= 1000:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                    batch = []
        if batch:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
    elif os.path.exists(uri):
        shutil.rmtree(uri)


def bucket_files(files: list[tuple[str, int]]) -> dict:
    buckets = {"data": [0, 0], "versions": [0, 0], "transactions": [0, 0],
               "indices": [0, 0], "other": [0, 0]}
    for rel, size in files:
        if rel.startswith("data/"):
            key = "data"
        elif rel.startswith("_versions/"):
            key = "versions"
        elif rel.startswith("_transactions/"):
            key = "transactions"
        elif rel.startswith("_indices/"):
            key = "indices"
        else:
            key = "other"
        buckets[key][0] += 1
        buckets[key][1] += size
    return {k: {"n_files": v[0], "total_bytes": v[1]} for k, v in buckets.items()}


def active_data_bytes(ds: "lance.LanceDataset", files: list[tuple[str, int]]) -> int:
    active_filenames = set()
    for frag in ds.get_fragments():
        for f in frag.metadata.files:
            active_filenames.add(os.path.basename(f.path))
    total = 0
    for rel, size in files:
        if rel.startswith("data/") and os.path.basename(rel) in active_filenames:
            total += size
    return total


def detect_stable_row_ids(ds: "lance.LanceDataset") -> bool:
    """Stable row IDs are present iff any fragment has non-None row_id_meta.
    4.0.1 does not expose `ds.has_stable_row_ids`; this is the portable probe.
    """
    for frag in ds.get_fragments():
        if frag.metadata.row_id_meta is not None:
            return True
    return False


def index_bytes_by_uuid(files: list[tuple[str, int]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for rel, size in files:
        if rel.startswith("_indices/"):
            rest = rel[len("_indices/"):]
            uuid = rest.split("/")[0].split(".")[0]
            out[uuid] = out.get(uuid, 0) + size
    return out


def timed(fn: Callable[[], Any], *, warmup: int = 2, rounds: int = 7) -> dict:
    last_result = None
    for _ in range(warmup):
        last_result = fn()
        gc.collect()
    runs = []
    for _ in range(rounds):
        gc.collect()
        t0 = time.perf_counter()
        last_result = fn()
        runs.append(time.perf_counter() - t0)
    runs_ms = [r * 1000 for r in runs]
    return {
        "median_ms": round(statistics.median(runs_ms), 3),
        "mean_ms": round(statistics.mean(runs_ms), 3),
        "min_ms": round(min(runs_ms), 3),
        "max_ms": round(max(runs_ms), 3),
        "stdev_ms": round(statistics.stdev(runs_ms) if len(runs_ms) > 1 else 0.0, 3),
        "runs_ms": [round(r, 3) for r in runs_ms],
        "rounds": rounds,
        "_last_result": last_result,
    }


@dataclass
class IndexSpec:
    label: str
    method: str
    column: str
    index_type: str
    extra_kwargs: dict = field(default_factory=dict)
    queries: list[dict] = field(default_factory=list)


def make_index_specs() -> list[IndexSpec]:
    return [
        IndexSpec(label="BTREE", method="scalar", column="id", index_type="BTREE",
                  queries=[{"name": "eq",    "filter": "id = 12345"},
                           {"name": "range", "filter": "id >= 5000 AND id < 15000"}]),
        IndexSpec(label="BITMAP", method="scalar", column="cat", index_type="BITMAP",
                  queries=[{"name": "eq", "filter": "cat = 'B'"},
                           {"name": "in", "filter": "cat IN ('A', 'C', 'E')"}]),
        IndexSpec(label="LABEL_LIST", method="scalar", column="tags", index_type="LABEL_LIST",
                  queries=[{"name": "has_any", "filter": "array_has_any(tags, ['tag_03', 'tag_07'])"},
                           {"name": "has",     "filter": "array_has(tags, 'tag_01')"}]),
        IndexSpec(label="NGRAM", method="scalar", column="text", index_type="NGRAM",
                  queries=[{"name": "contains",     "filter": "contains(text, 'quick')"},
                           {"name": "contains_num", "filter": "contains(text, 'number 42')"}]),
        IndexSpec(label="BLOOMFILTER", method="scalar", column="id", index_type="BLOOMFILTER",
                  queries=[{"name": "eq", "filter": "id = 12345"},
                           {"name": "in", "filter": "id IN (100, 5000, 99999)"}]),
        IndexSpec(label="ZONEMAP", method="scalar", column="price", index_type="ZONEMAP",
                  queries=[{"name": "lt",    "filter": "price < 50"},
                           {"name": "range", "filter": "price >= 100 AND price < 200"}]),
        IndexSpec(label="INVERTED", method="scalar", column="text", index_type="INVERTED",
                  queries=[{"name": "match",  "fts": "quick"},
                           {"name": "phrase", "fts": "lazy fox"}]),
        IndexSpec(label="IVF_HNSW_SQ", method="vector", column="vec", index_type="IVF_HNSW_SQ",
                  extra_kwargs={"num_partitions": 4},
                  queries=[{"name": "ann_k10", "nearest": True, "k": 10}]),
        IndexSpec(label="IVF_PQ", method="vector", column="vec", index_type="IVF_PQ",
                  extra_kwargs={"num_partitions": 4, "num_sub_vectors": 4},
                  queries=[{"name": "ann_k10", "nearest": True, "k": 10}]),
    ]


def make_query_callable(ds: "lance.LanceDataset", spec: IndexSpec,
                        q: dict) -> Callable[[], Any]:
    cols = [spec.column, "id"] if spec.column != "id" else ["id"]

    if "nearest" in q:
        query_vec = np.random.RandomState(42).standard_normal(VEC_DIM).astype(np.float32)
        k = q.get("k", 10)

        def run_vec():
            return ds.scanner(
                nearest={"column": spec.column, "q": query_vec, "k": k},
                columns=cols,
            ).to_table()
        return run_vec

    if "fts" in q:
        query_text = q["fts"]
        fts_cols = cols if "text" in cols else cols + ["text"]

        def run_fts():
            return ds.scanner(full_text_query=query_text, columns=fts_cols).to_table()
        return run_fts

    filter_str = q["filter"]

    def run_scalar():
        return ds.scanner(columns=cols, filter=filter_str).to_table()
    return run_scalar


def ground_truth_count(ds: "lance.LanceDataset", spec: IndexSpec, q: dict) -> int:
    """Compute expected result cardinality WITHOUT using any index.

    Strategy differs by query type because Lance offers no runtime
    "force full scan" flag:
      - scalar filters: ds.to_table() then evaluate in PyArrow compute
      - FTS: approximate via substring match on the first query term
      - vector KNN: ground truth == k (we only assert result count, not
        recall — recall verification would need a reference FLAT index)
    """
    if "nearest" in q:
        return q.get("k", 10)
    if "fts" in q:
        tbl = ds.to_table(columns=["text"])
        first_term = q["fts"].split()[0]
        matches = pc.match_substring(tbl["text"], first_term).to_numpy(zero_copy_only=False)
        return int(matches.sum())
    tbl = ds.to_table()
    return _pyarrow_count(tbl, q["filter"])


def _pyarrow_count(tbl: pa.Table, lance_filter: str) -> int:
    f = lance_filter.strip()
    try:
        if " AND " in f:
            parts = f.split(" AND ")
            masks = [_pyarrow_mask(tbl, p.strip()) for p in parts]
            m = masks[0]
            for m2 in masks[1:]:
                m = pc.and_(m, m2)
            return int(pc.sum(m).as_py() or 0)
        return int(pc.sum(_pyarrow_mask(tbl, f)).as_py() or 0)
    except Exception as e:
        print(f"    [WARN] ground-truth eval failed for '{lance_filter}': {e}")
        return -1


def _pyarrow_mask(tbl: pa.Table, filt: str) -> pa.Array:
    """Parse a subset of Lance filter grammar into a PyArrow boolean mask.

    Only supports the exact patterns used in make_index_specs():
      col = val | col >= val | col IN (...) | contains(col, 'x') |
      array_has(col, 'x') | array_has_any(col, ['x','y'])
    """
    filt = filt.strip()
    if filt.startswith("contains("):
        m = re.match(r"contains\((\w+),\s*'([^']+)'\)", filt)
        col, needle = m.group(1), m.group(2)
        return pc.match_substring(tbl[col], needle)
    if filt.startswith("array_has_any("):
        m = re.match(r"array_has_any\((\w+),\s*\[(.+)\]\)", filt)
        col = m.group(1)
        vals = [v.strip().strip("'") for v in m.group(2).split(",")]
        out = np.zeros(tbl.num_rows, dtype=bool)
        for i, row in enumerate(tbl[col].to_pylist()):
            if row is not None and any(v in row for v in vals):
                out[i] = True
        return pa.array(out)
    if filt.startswith("array_has("):
        m = re.match(r"array_has\((\w+),\s*'([^']+)'\)", filt)
        col, needle = m.group(1), m.group(2)
        out = np.zeros(tbl.num_rows, dtype=bool)
        for i, row in enumerate(tbl[col].to_pylist()):
            if row is not None and needle in row:
                out[i] = True
        return pa.array(out)
    m = re.match(r"(\w+)\s+IN\s+\(([^)]+)\)", filt)
    if m:
        col = m.group(1)
        raw = [v.strip().strip("'") for v in m.group(2).split(",")]
        target = tbl[col].type
        vals = [int(v) for v in raw] if pa.types.is_integer(target) else raw
        return pc.is_in(tbl[col], value_set=pa.array(vals, type=target))
    m = re.match(r"(\w+)\s*(=|!=|<=|>=|<|>)\s*(.+)", filt)
    if m:
        col, op, rhs = m.group(1), m.group(2), m.group(3).strip()
        if rhs.startswith("'"):
            v = rhs.strip("'")
        elif "." in rhs:
            v = float(rhs)
        else:
            v = int(rhs)
        op_map = {"=": pc.equal, "!=": pc.not_equal,
                  "<": pc.less, "<=": pc.less_equal,
                  ">": pc.greater, ">=": pc.greater_equal}
        return op_map[op](tbl[col], pa.scalar(v, type=tbl[col].type))
    raise ValueError(f"unparseable: {filt}")


def snapshot_dataset(ds: "lance.LanceDataset", uri: str,
                     region: Optional[str]) -> dict:
    files = list_uri(uri, region)
    buckets = bucket_files(files)
    idx_bytes_map = index_bytes_by_uuid(files)
    fragments = ds.get_fragments()
    indexes_clean = []
    for i in ds.list_indices():
        idx_copy = dict(i)
        if isinstance(idx_copy.get("fragment_ids"), set):
            idx_copy["fragment_ids"] = sorted(idx_copy["fragment_ids"])
        indexes_clean.append(idx_copy)
    return {
        "version": ds.version,
        "n_versions": len(ds.versions()),
        "n_fragments": len(fragments),
        "n_rows": ds.count_rows(),
        "active_data_bytes": active_data_bytes(ds, files),
        "buckets": buckets,
        "total_bytes_on_disk": sum(b["total_bytes"] for b in buckets.values()),
        "index_bytes_by_uuid": idx_bytes_map,
        "indexes": indexes_clean,
        "has_frag_reuse_index": any(i.get("name") == "__lance_frag_reuse" for i in indexes_clean),
        "has_stable_row_ids": detect_stable_row_ids(ds),
    }


def run_one_combo(*, index_spec: IndexSpec, compact_path: str,
                  work_dir: str, n_rows: int, rows_per_fragment: int,
                  region: Optional[str], storage_options: Optional[dict],
                  smoke: bool) -> dict:
    defer = (compact_path == "defer")
    combo_uri = f"{work_dir.rstrip('/')}/{index_spec.label.lower()}__{compact_path}"

    print(f"\n{'='*70}")
    print(f"[N] Combo: {index_spec.label} × {compact_path}")
    print(f"[N]  URI: {combo_uri}")
    print(f"[N]  n_rows={n_rows:,}  rows_per_fragment={rows_per_fragment:,}")
    print(f"{'='*70}")

    rm_uri(combo_uri, region)

    result: dict = {
        "index_spec": {
            "label": index_spec.label, "method": index_spec.method,
            "column": index_spec.column, "index_type": index_spec.index_type,
            "extra_kwargs": index_spec.extra_kwargs, "queries": index_spec.queries,
        },
        "compact_path": compact_path,
        "phases": {}, "assertions": {}, "errors": [],
    }

    try:
        print(f"\n[N] Phase 1: Writing dataset ({n_rows:,} rows × {rows_per_fragment:,}/frag)...")
        write_info = write_dataset(combo_uri, n_rows=n_rows,
                                   rows_per_fragment=rows_per_fragment,
                                   storage_options=storage_options)
        print(f"[N]   -> {write_info}")
        ds = lance.dataset(combo_uri, storage_options=storage_options)
        result["phases"]["1_after_write"] = {
            **snapshot_dataset(ds, combo_uri, region),
            "write_info": write_info,
        }

        print("\n[N] Phase 2: Ground-truth counts (no index)...")
        gt_counts = {}
        for q in index_spec.queries:
            try:
                c = ground_truth_count(ds, index_spec, q)
                gt_counts[q["name"]] = c
                print(f"[N]   {q['name']:<10} -> {c} rows")
            except Exception as e:
                gt_counts[q["name"]] = f"ERROR: {e}"
                print(f"[N]   {q['name']:<10} -> ERROR: {e}")
        result["phases"]["2_ground_truth_counts"] = gt_counts

        print(f"\n[N] Phase 3: Building {index_spec.label} on '{index_spec.column}'...")
        idx_name = f"test_{index_spec.label.lower()}_idx"
        t0 = time.perf_counter()
        if index_spec.method == "scalar":
            ds.create_scalar_index(index_spec.column, index_type=index_spec.index_type,
                                   name=idx_name, replace=True)
        else:
            ds.create_index([index_spec.column], index_type=index_spec.index_type,
                            name=idx_name, replace=True, **index_spec.extra_kwargs)
        build_elapsed = time.perf_counter() - t0
        print(f"[N]   build time: {build_elapsed:.2f}s")
        ds = lance.dataset(combo_uri, storage_options=storage_options)
        pre_snap = snapshot_dataset(ds, combo_uri, region)
        pre_idx = next((i for i in pre_snap["indexes"] if i["name"] == idx_name), None)
        if pre_idx is None:
            raise RuntimeError(f"index {idx_name} missing after create")
        pre_uuid = pre_idx["uuid"]
        pre_fragment_bitmap = pre_idx.get("fragment_ids", [])
        result["phases"]["3_after_index_build"] = {
            **pre_snap, "build_time_s": round(build_elapsed, 3),
            "pre_index_uuid": pre_uuid,
            "pre_index_fragment_ids": pre_fragment_bitmap,
        }

        print("\n[N] Phase 4: Pre-compact query latency (with index)...")
        pre_query_results = {}
        for q in index_spec.queries:
            try:
                cb = make_query_callable(ds, index_spec, q)
                t = timed(cb, warmup=2, rounds=5 if not smoke else 3)
                out: pa.Table = t.pop("_last_result")
                pre_query_results[q["name"]] = {
                    "latency": t, "rows_returned": out.num_rows,
                    "expected": gt_counts.get(q["name"]),
                }
                print(f"[N]   {q['name']:<10}  p50={t['median_ms']:>8.2f}ms  "
                      f"rows={out.num_rows}  (gt={gt_counts.get(q['name'])})")
            except Exception as e:
                pre_query_results[q["name"]] = {"error": str(e)}
                print(f"[N]   {q['name']:<10}  ERROR: {e}")
        result["phases"]["4_pre_compact_queries"] = pre_query_results

        print(f"\n[N] Phase 5: compact_files(defer_index_remap={defer})...")
        target_per_frag = max(n_rows // 4, 100_000)
        t0 = time.perf_counter()
        metrics = ds.optimize.compact_files(
            target_rows_per_fragment=target_per_frag,
            defer_index_remap=defer,
        )
        compact_elapsed = time.perf_counter() - t0
        print(f"[N]   compact time: {compact_elapsed:.2f}s")
        compact_metrics = {
            "fragments_removed": getattr(metrics, "fragments_removed", None),
            "fragments_added": getattr(metrics, "fragments_added", None),
            "files_removed": getattr(metrics, "files_removed", None),
            "files_added": getattr(metrics, "files_added", None),
        }
        print(f"[N]   metrics: {compact_metrics}")

        ds = lance.dataset(combo_uri, storage_options=storage_options)
        post_snap_preclean = snapshot_dataset(ds, combo_uri, region)
        post_idx = next((i for i in post_snap_preclean["indexes"] if i["name"] == idx_name), None)
        post_uuid = post_idx["uuid"] if post_idx else None
        post_fragment_bitmap = post_idx.get("fragment_ids", []) if post_idx else []
        result["phases"]["5_after_compact_preclean"] = {
            **post_snap_preclean, "compact_time_s": round(compact_elapsed, 3),
            "compact_metrics": compact_metrics,
            "post_index_uuid": post_uuid,
            "post_index_fragment_ids": post_fragment_bitmap,
        }

        print("\n[N] Phase 6: Post-compact query latency + correctness...")
        post_query_results = {}
        for q in index_spec.queries:
            try:
                cb = make_query_callable(ds, index_spec, q)
                t = timed(cb, warmup=2, rounds=5 if not smoke else 3)
                out: pa.Table = t.pop("_last_result")
                post_query_results[q["name"]] = {
                    "latency": t, "rows_returned": out.num_rows,
                    "expected": gt_counts.get(q["name"]),
                }
                pre = pre_query_results.get(q["name"], {})
                pre_ms = pre.get("latency", {}).get("median_ms") if isinstance(pre, dict) else None
                print(f"[N]   {q['name']:<10}  p50={t['median_ms']:>8.2f}ms  "
                      f"rows={out.num_rows}  (pre={pre_ms})")
            except Exception as e:
                post_query_results[q["name"]] = {"error": str(e)}
                print(f"[N]   {q['name']:<10}  ERROR: {e}")
        result["phases"]["6_post_compact_queries"] = post_query_results

        print("\n[N] Phase 7: cleanup_old_versions(0) + final snapshot...")
        try:
            cleanup_metrics = ds.cleanup_old_versions(
                older_than=timedelta(seconds=0), delete_unverified=True,
            )
            ds = lance.dataset(combo_uri, storage_options=storage_options)
            cleaned_snap = snapshot_dataset(ds, combo_uri, region)
            cleaned = {
                "bytes_removed": getattr(cleanup_metrics, "bytes_removed", None),
                "old_versions": getattr(cleanup_metrics, "old_versions", None),
            }
            result["phases"]["7_after_cleanup"] = {**cleaned_snap, "cleanup_metrics": cleaned}
            print(f"[N]   cleanup: {cleaned}")
            print(f"[N]   n_versions after: {cleaned_snap['n_versions']}")
        except Exception as e:
            result["phases"]["7_after_cleanup"] = {"error": str(e)}
            print(f"[N]   cleanup FAILED: {e}")

        print("\n[N] Phase 8: Assertions...")
        A = result["assertions"]

        correctness_ok = True
        any_query_errored_post_compact = False
        for q in index_spec.queries:
            pre = pre_query_results.get(q["name"], {})
            post = post_query_results.get(q["name"], {})
            if isinstance(post, dict) and "error" in post:
                any_query_errored_post_compact = True
                correctness_ok = False
                A[f"correctness_{q['name']}"] = {
                    "pass": False, "reason": "query errored post-compact",
                    "error": post["error"],
                }
                continue
            if isinstance(pre, dict) and "error" in pre:
                continue
            if pre.get("rows_returned") != post.get("rows_returned"):
                correctness_ok = False
                A[f"correctness_{q['name']}"] = {
                    "pass": False,
                    "pre_rows": pre.get("rows_returned"),
                    "post_rows": post.get("rows_returned"),
                }
            else:
                A[f"correctness_{q['name']}"] = {
                    "pass": True, "rows": pre.get("rows_returned"),
                }
        A["correctness_all_queries"] = correctness_ok
        A["any_query_errored_post_compact"] = any_query_errored_post_compact
        print(f"[N]   a. Correctness (row counts preserved): {'PASS' if correctness_ok else 'FAIL'}")

        metrics_ok = (compact_metrics["fragments_removed"] or 0) > 0
        A["compact_ran"] = {"pass": metrics_ok, **compact_metrics}
        print(f"[N]   b. Compact actually rewrote fragments: {'PASS' if metrics_ok else 'FAIL'} ({compact_metrics})")

        if post_idx:
            uuid_changed = (pre_uuid != post_uuid)
            stable_rids = detect_stable_row_ids(ds)
            if compact_path == "default":
                uuid_invariant_ok = uuid_changed or stable_rids
                reason = ("UUID changed (as expected)" if uuid_changed
                          else ("UUID preserved — allowed because stable_row_ids=True"
                                if stable_rids
                                else "UUID unchanged — UNEXPECTED for default path"))
            else:
                uuid_invariant_ok = not uuid_changed
                reason = ("UUID preserved (as expected for defer)" if not uuid_changed
                          else "UUID changed — UNEXPECTED for defer path")
            A["uuid_invariant"] = {
                "pass": uuid_invariant_ok, "compact_path": compact_path,
                "pre_uuid": pre_uuid, "post_uuid": post_uuid,
                "uuid_changed": uuid_changed, "reason": reason,
            }
            print(f"[N]   c/d. UUID invariant ({compact_path}): {'PASS' if uuid_invariant_ok else 'FAIL'} — {reason}")
        else:
            A["uuid_invariant"] = {"pass": False, "reason": "index missing post-compact"}
            print("[N]   c/d. UUID invariant: FAIL — index missing post-compact")

        has_fri = post_snap_preclean["has_frag_reuse_index"]
        stable_rids_post = detect_stable_row_ids(ds)
        if compact_path == "defer" and not stable_rids_post:
            fri_ok = has_fri
            reason_fri = "FRI present (expected for defer path)"
        elif compact_path == "default":
            fri_ok = not has_fri
            reason_fri = "FRI absent (expected for default path)"
        else:
            fri_ok = not has_fri
            reason_fri = "FRI absent — allowed because stable_row_ids=True"
        A["fri_invariant"] = {"pass": fri_ok, "has_fri": has_fri, "reason": reason_fri}
        print(f"[N]   e. FRI invariant: {'PASS' if fri_ok else 'FAIL'} — {reason_fri}")

        new_frag_ids = set(f.fragment_id for f in ds.get_fragments())
        if post_idx:
            bitmap_covers = bool(new_frag_ids.intersection(set(post_fragment_bitmap)))
            A["bitmap_updated"] = {
                "pass": bitmap_covers,
                "new_fragments": sorted(new_frag_ids),
                "index_bitmap": sorted(post_fragment_bitmap),
            }
            print(f"[N]   f. Fragment bitmap covers current fragments: {'PASS' if bitmap_covers else 'FAIL'}")

        for q in index_spec.queries:
            pre = pre_query_results.get(q["name"], {})
            post = post_query_results.get(q["name"], {})
            if (isinstance(pre, dict) and "latency" in pre
                    and isinstance(post, dict) and "latency" in post):
                pre_ms = pre["latency"]["median_ms"]
                post_ms = post["latency"]["median_ms"]
                ratio = post_ms / pre_ms if pre_ms else float("inf")
                A[f"latency_ratio_{q['name']}"] = {
                    "pre_ms": pre_ms, "post_ms": post_ms, "ratio": round(ratio, 3),
                    "verdict": ("improved" if ratio < 0.8
                                else "neutral" if ratio < 1.5
                                else "regressed"),
                }

        if post_idx:
            pre_idx_bytes = pre_snap["index_bytes_by_uuid"].get(pre_uuid, 0)
            post_idx_bytes = post_snap_preclean["index_bytes_by_uuid"].get(post_uuid, 0)
            A["index_bytes_delta"] = {
                "pre_uuid_bytes": pre_idx_bytes,
                "post_uuid_bytes": post_idx_bytes,
                "delta_bytes": post_idx_bytes - pre_idx_bytes,
            }

        print("[N]   Done.")

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[N] COMBO FAILED: {e}\n{tb}")
        result["errors"].append({"where": "combo top-level", "error": str(e), "traceback": tb})

    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work-dir", required=True,
                    help="Directory (local or s3://bucket/prefix) for test datasets")
    ap.add_argument("--region", default=None, help="AWS region if --work-dir is S3")
    ap.add_argument("--n-rows", type=int, default=1_000_000,
                    help="Total rows in each test dataset (default 1M)")
    ap.add_argument("--rows-per-fragment", type=int, default=None,
                    help="Rows per fragment; default = n-rows / 10")
    ap.add_argument("--smoke", action="store_true",
                    help="Smoke test: 100k rows, skip FTS/vector (fastest)")
    ap.add_argument("--index-types", default=None,
                    help="Comma-separated subset (e.g. BTREE,BITMAP,ZONEMAP)")
    ap.add_argument("--compact-paths", default="default,defer",
                    help="Comma-separated (default,defer)")
    ap.add_argument("--out",
                    default="/home/hadoop/lance-extended-bench/results/N_compact_index.json")
    ap.add_argument("--keep-data", action="store_true",
                    help="Don't remove datasets after run (for debugging)")
    args = ap.parse_args()

    if args.smoke:
        args.n_rows = 100_000
    if args.rows_per_fragment is None:
        args.rows_per_fragment = max(args.n_rows // 10, 10_000)

    storage_options = ({"region": args.region}
                       if args.region and is_s3(args.work_dir) else None)

    specs = make_index_specs()
    if args.index_types:
        wanted = {s.strip().upper() for s in args.index_types.split(",")}
        specs = [s for s in specs if s.label in wanted]
    if args.smoke:
        specs = [s for s in specs if s.label in {"BTREE", "BITMAP", "ZONEMAP", "BLOOMFILTER"}]

    compact_paths = [p.strip() for p in args.compact_paths.split(",")]
    for cp in compact_paths:
        if cp not in ("default", "defer"):
            raise ValueError(f"invalid compact path: {cp}")

    all_results = {
        "pylance_version": lance.__version__,
        "pyarrow_version": pa.__version__,
        "config": {
            "work_dir": args.work_dir, "region": args.region,
            "n_rows": args.n_rows, "rows_per_fragment": args.rows_per_fragment,
            "smoke": args.smoke, "compact_paths": compact_paths,
            "index_types": [s.label for s in specs],
        },
        "combos": [], "timestamp": time.time(),
    }

    print(f"\n[N] Starting N_compact_index benchmark")
    print(f"[N] pylance={lance.__version__}  pyarrow={pa.__version__}")
    print(f"[N] work_dir={args.work_dir}  n_rows={args.n_rows:,}  rows/frag={args.rows_per_fragment:,}")
    print(f"[N] Index types: {[s.label for s in specs]}")
    print(f"[N] Compact paths: {compact_paths}")

    overall_t0 = time.perf_counter()

    for spec in specs:
        for cp in compact_paths:
            r = run_one_combo(
                index_spec=spec, compact_path=cp,
                work_dir=args.work_dir,
                n_rows=args.n_rows, rows_per_fragment=args.rows_per_fragment,
                region=args.region, storage_options=storage_options,
                smoke=args.smoke,
            )
            all_results["combos"].append(r)

            os.makedirs(os.path.dirname(args.out), exist_ok=True)
            tmp = args.out + ".tmp"
            with open(tmp, "w") as f:
                json.dump(all_results, f, indent=2, default=str)
            os.replace(tmp, args.out)
            print(f"\n[N]   Saved checkpoint to {args.out}")

            if not args.keep_data:
                combo_uri = f"{args.work_dir.rstrip('/')}/{spec.label.lower()}__{cp}"
                try:
                    rm_uri(combo_uri, args.region)
                except Exception as e:
                    print(f"[N]   (could not remove {combo_uri}: {e})")

    all_results["total_elapsed_s"] = round(time.perf_counter() - overall_t0, 2)
    tmp = args.out + ".tmp"
    with open(tmp, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    os.replace(tmp, args.out)

    print(f"\n[N] === DONE in {all_results['total_elapsed_s']}s ===")
    print(f"[N] Results: {args.out}")

    print("\n[N] === Assertion Summary ===")
    print(f"{'Index':<14} {'Path':<8} {'Correct':<8} {'UUID':<8} {'FRI':<8} {'Bitmap':<8}")
    for c in all_results["combos"]:
        if c.get("errors"):
            err_msg = c['errors'][0]['error'][:40]
            print(f"{c['index_spec']['label']:<14} {c['compact_path']:<8} ERR      ({err_msg})")
            continue
        A = c.get("assertions", {})
        corr = "PASS" if A.get("correctness_all_queries") else "FAIL"
        uuid = "PASS" if A.get("uuid_invariant", {}).get("pass") else "FAIL"
        fri = "PASS" if A.get("fri_invariant", {}).get("pass") else "FAIL"
        bm = "PASS" if A.get("bitmap_updated", {}).get("pass") else "FAIL"
        print(f"{c['index_spec']['label']:<14} {c['compact_path']:<8} "
              f"{corr:<8} {uuid:<8} {fri:<8} {bm:<8}")


if __name__ == "__main__":
    main()
