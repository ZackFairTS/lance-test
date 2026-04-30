import argparse
import json
import os
import shutil
import time

import lance
import numpy as np
import pyarrow as pa


def read_fvecs(path):
    a = np.fromfile(path, dtype=np.float32)
    d = np.frombuffer(a[:1].tobytes(), dtype=np.int32)[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def read_ivecs(path):
    a = np.fromfile(path, dtype=np.int32)
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def build_lance_dataset(base_vectors, uri, dim, max_rows_per_file=1 << 20):
    if os.path.exists(uri):
        shutil.rmtree(uri)
    n = len(base_vectors)
    tbl = pa.Table.from_arrays(
        [
            pa.array(np.arange(n, dtype=np.uint32)),
            pa.FixedSizeListArray.from_arrays(
                pa.array(base_vectors.ravel(), type=pa.float32()),
                list_size=dim,
            ),
        ],
        names=["id", "vector"],
    )
    lance.write_dataset(tbl, uri, mode="overwrite", max_rows_per_file=max_rows_per_file)


def recall_at_k(pred, gt, k):
    if pred.shape[0] != gt.shape[0]:
        raise ValueError(f"pred {pred.shape} != gt {gt.shape}")
    hits = 0
    for i in range(pred.shape[0]):
        hits += len(set(pred[i]) & set(gt[i, :k]))
    return hits / (pred.shape[0] * k)


def run_query_grid(ds, index_name, queries, gt, k, nprobes_list, refine_list, ef_list,
                   warmup=5):
    rows = []
    is_hnsw = "HNSW" in index_name
    is_pq_or_rq = index_name in ("IVF_PQ", "IVF_RQ", "IVF_HNSW_PQ")

    sample_qs = queries[:min(10, len(queries))]
    for q in sample_qs:
        ds.to_table(nearest={
            "column": "vector", "q": q, "k": k,
            "minimum_nprobes": nprobes_list[len(nprobes_list) // 2],
            "maximum_nprobes": nprobes_list[len(nprobes_list) // 2],
            "refine_factor": refine_list[0] if is_pq_or_rq else None,
            "ef": ef_list[len(ef_list) // 2] if is_hnsw else None,
        })

    actual_refine = refine_list if is_pq_or_rq else [None]
    actual_ef = ef_list if is_hnsw else [None]

    for nprobes in nprobes_list:
        for rf in actual_refine:
            for ef in actual_ef:
                preds = np.empty((len(queries), k), dtype=np.int64)
                t0 = time.perf_counter()
                for qi, q in enumerate(queries):
                    nearest = {
                        "column": "vector", "q": q, "k": k,
                        "minimum_nprobes": nprobes,
                        "maximum_nprobes": nprobes,
                    }
                    if rf is not None:
                        nearest["refine_factor"] = rf
                    if ef is not None:
                        nearest["ef"] = ef
                    r = ds.to_table(nearest=nearest)
                    preds[qi] = r["id"].to_numpy()
                elapsed = time.perf_counter() - t0
                qps = len(queries) / elapsed
                rec = recall_at_k(preds, gt, k)
                rows.append({
                    "index": index_name,
                    "nprobes": nprobes,
                    "refine": rf,
                    "ef": ef,
                    "qps": round(qps, 2),
                    "mean_latency_ms": round(1000 * elapsed / len(queries), 3),
                    "recall_at_k": round(rec, 4),
                })
                print(f"  {index_name:13s} nprobes={nprobes:3d} rf={str(rf):4s} ef={str(ef):4s}  "
                      f"recall@{k}={rec:.3f}  qps={qps:7.1f}", flush=True)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sift-dir", default="/home/hadoop/lance-extended-bench/data/sift")
    ap.add_argument("--work-dir", default="/home/hadoop/lance-extended-bench/data/lance_sift")
    ap.add_argument("--n-queries", type=int, default=500)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/D_vector_search.json")
    ap.add_argument("--indexes", default="IVF_PQ,IVF_HNSW_SQ",
                    help="comma-separated subset of IVF_PQ,IVF_HNSW_SQ,IVF_RQ")
    args = ap.parse_args()

    print("Loading SIFT-1M...")
    base = read_fvecs(os.path.join(args.sift_dir, "sift_base.fvecs"))
    queries = read_fvecs(os.path.join(args.sift_dir, "sift_query.fvecs"))[:args.n_queries]
    gt = read_ivecs(os.path.join(args.sift_dir, "sift_groundtruth.ivecs"))[:args.n_queries]
    print(f"  base={base.shape}  queries={queries.shape}  gt={gt.shape}")

    os.makedirs(args.work_dir, exist_ok=True)
    base_uri = os.path.join(args.work_dir, "base.lance")
    if not os.path.exists(base_uri):
        print("Building base Lance dataset...")
        t0 = time.time()
        build_lance_dataset(base, base_uri, dim=base.shape[1])
        print(f"  base write: {time.time() - t0:.1f}s")
    else:
        print(f"Reusing existing {base_uri}")

    configs = {
        "IVF_PQ": {
            "build": dict(index_type="IVF_PQ", metric="L2",
                          num_partitions=1024, num_sub_vectors=16),
            "nprobes": [1, 5, 10, 25, 50, 100],
            "refine": [None, 1, 10, 50],
            "ef": [None],
        },
        "IVF_HNSW_SQ": {
            "build": dict(index_type="IVF_HNSW_SQ", metric="L2",
                          num_partitions=256, m=20, ef_construction=300),
            "nprobes": [1, 5, 10, 25, 50],
            "refine": [None],
            "ef": [16, 32, 64, 128],
        },
        "IVF_RQ": {
            "build": dict(index_type="IVF_RQ", metric="L2",
                          num_partitions=1024),
            "nprobes": [1, 5, 10, 25, 50, 100],
            "refine": [None, 1, 10, 50],
            "ef": [None],
        },
    }

    want = [x.strip() for x in args.indexes.split(",") if x.strip()]
    all_rows = []
    build_times = {}
    index_sizes = {}

    for name in want:
        if name not in configs:
            print(f"Skipping unknown index {name}")
            continue
        cfg = configs[name]
        uri = os.path.join(args.work_dir, f"idx_{name}.lance")
        if not os.path.exists(uri):
            shutil.copytree(base_uri, uri)

        ds = lance.dataset(uri)
        print(f"\n=== Building {name}  params={cfg['build']}")
        t0 = time.time()
        try:
            ds.create_index("vector", replace=True, **cfg["build"])
        except Exception as e:
            print(f"  FAILED to build {name}: {e}")
            continue
        build_times[name] = time.time() - t0
        print(f"  build time: {build_times[name]:.1f}s")

        size_bytes = 0
        indices_dir = os.path.join(uri, "_indices")
        if os.path.isdir(indices_dir):
            for root, _, files in os.walk(indices_dir):
                for f in files:
                    size_bytes += os.path.getsize(os.path.join(root, f))
        index_sizes[name] = size_bytes

        ds = lance.dataset(uri)
        print(f"  running query grid...")
        rows = run_query_grid(
            ds, name, queries, gt, args.k,
            nprobes_list=cfg["nprobes"],
            refine_list=cfg["refine"],
            ef_list=cfg["ef"],
        )
        all_rows.extend(rows)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "pylance_version": lance.__version__,
            "dataset": "SIFT-1M",
            "n_queries": len(queries),
            "k": args.k,
            "build_seconds": build_times,
            "index_bytes": index_sizes,
            "rows": all_rows,
        }, f, indent=2)
    print(f"\nSaved {len(all_rows)} query configs to {args.out}")

    print("\n=== Pareto summary (best QPS at each recall bucket):")
    buckets = [0.8, 0.9, 0.95, 0.98, 0.99]
    for name in want:
        sub = [r for r in all_rows if r["index"] == name]
        if not sub:
            continue
        for b in buckets:
            above = [r for r in sub if r["recall_at_k"] >= b]
            if above:
                best = max(above, key=lambda r: r["qps"])
                print(f"  {name:13s}  recall>={b:.2f}  qps={best['qps']:7.1f}  "
                      f"cfg=nprobes={best['nprobes']} rf={best['refine']} ef={best['ef']}")


if __name__ == "__main__":
    main()
