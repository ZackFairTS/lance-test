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


def build_dataset_with_metadata(base_vectors, uri, dim, seed=42):
    if os.path.exists(uri):
        shutil.rmtree(uri)
    n = len(base_vectors)
    rng = np.random.default_rng(seed)
    price = rng.uniform(0, 1000, n)
    tbl = pa.Table.from_arrays(
        [
            pa.array(np.arange(n, dtype=np.uint32)),
            pa.FixedSizeListArray.from_arrays(
                pa.array(base_vectors.ravel(), type=pa.float32()),
                list_size=dim,
            ),
            pa.array(price, type=pa.float64()),
        ],
        names=["id", "vector", "price"],
    )
    lance.write_dataset(tbl, uri, mode="overwrite", max_rows_per_file=1 << 20)


def selectivity_to_predicate(ds, target):
    n = ds.count_rows()
    threshold = target * 1000.0
    expr = f"price < {threshold}"
    actual = ds.count_rows(filter=expr)
    return expr, actual / n


def make_nearest_base(vec_index, k, nprobes=20, ef=128, refine_factor=10):
    base = {
        "column": "vector",
        "k": k,
        "minimum_nprobes": nprobes,
        "maximum_nprobes": nprobes,
        "use_index": True,
    }
    if "HNSW" in vec_index:
        base["ef"] = ef
    else:
        base["refine_factor"] = refine_factor
    return base


def run_cell(ds, queries, expr, nearest_base, prefilter, n_queries, warmup):
    for q in queries[:warmup]:
        nearest = dict(nearest_base)
        nearest["q"] = q
        ds.to_table(nearest=nearest, filter=expr, prefilter=prefilter)

    latencies = []
    returned_sizes = []
    for q in queries[:n_queries]:
        nearest = dict(nearest_base)
        nearest["q"] = q
        t0 = time.perf_counter()
        r = ds.to_table(nearest=nearest, filter=expr, prefilter=prefilter)
        latencies.append(time.perf_counter() - t0)
        returned_sizes.append(r.num_rows)
    return latencies, returned_sizes


def summarize_latencies(latencies):
    arr = np.array(latencies) * 1000
    return {
        "n": len(latencies),
        "p50_ms": float(np.percentile(arr, 50)),
        "p90_ms": float(np.percentile(arr, 90)),
        "p99_ms": float(np.percentile(arr, 99)),
        "mean_ms": float(arr.mean()),
        "stdev_ms": float(arr.std()),
        "cov": float(arr.std() / arr.mean()) if arr.mean() > 0 else 0.0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sift-dir", default="/home/hadoop/lance-extended-bench/data/sift")
    ap.add_argument("--work-dir", default="/home/hadoop/lance-extended-bench/data/lance_sift_meta")
    ap.add_argument("--n-queries", type=int, default=500)
    ap.add_argument("--warmup", type=int, default=20)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/E_prefilter.json")
    args = ap.parse_args()

    print("Loading SIFT-1M base + queries...")
    base = read_fvecs(os.path.join(args.sift_dir, "sift_base.fvecs"))
    queries = read_fvecs(os.path.join(args.sift_dir, "sift_query.fvecs"))[:args.n_queries + args.warmup]
    dim = base.shape[1]

    os.makedirs(args.work_dir, exist_ok=True)
    base_uri = os.path.join(args.work_dir, "base.lance")
    if not os.path.exists(base_uri):
        print("Building base dataset with metadata...")
        build_dataset_with_metadata(base, base_uri, dim)

    build_seconds = {}
    datasets = {}

    for vec_index, build_params in [
        ("IVF_PQ", dict(index_type="IVF_PQ", metric="L2",
                        num_partitions=1024, num_sub_vectors=16)),
        ("IVF_HNSW_SQ", dict(index_type="IVF_HNSW_SQ", metric="L2",
                             num_partitions=256, m=20, ef_construction=300)),
    ]:
        idx_uri = os.path.join(args.work_dir, f"idx_{vec_index}.lance")
        if not os.path.exists(idx_uri):
            shutil.copytree(base_uri, idx_uri)
        ds = lance.dataset(idx_uri)
        existing = [ix.get("name") for ix in (ds.list_indices() or [])]
        if not any(vec_index.lower() in (n or "").lower() for n in existing):
            print(f"Building {vec_index}...")
            t0 = time.time()
            ds.create_index("vector", replace=True, **build_params)
            build_seconds[vec_index] = time.time() - t0
            print(f"  {vec_index} build: {build_seconds[vec_index]:.1f}s")
        print(f"Building BTREE on price for {vec_index} dataset...")
        t0 = time.time()
        try:
            ds.create_scalar_index("price", index_type="BTREE", replace=True)
            build_seconds[f"BTREE_price_{vec_index}"] = time.time() - t0
        except Exception as e:
            print(f"  BTREE build failed: {e}")
        datasets[vec_index] = lance.dataset(idx_uri)

    selectivities = [0.001, 0.01, 0.05, 0.1, 0.2, 0.5, 0.9]
    rows = []
    for vec_index, ds in datasets.items():
        print(f"\n=== Global warmup for {vec_index} ...")
        nearest_base = make_nearest_base(vec_index, args.k)
        for q in queries[:50]:
            nearest = dict(nearest_base)
            nearest["q"] = q
            ds.to_table(nearest=nearest)

        for sel_target in selectivities:
            expr, actual_sel = selectivity_to_predicate(ds, sel_target)
            for prefilter in [True, False]:
                try:
                    latencies, returned = run_cell(
                        ds, queries, expr, nearest_base, prefilter,
                        args.n_queries, args.warmup,
                    )
                    stats = summarize_latencies(latencies)
                    avg_returned = float(np.mean(returned))
                    valid = avg_returned >= args.k * 0.5
                    row = {
                        "selectivity_target": sel_target,
                        "selectivity_actual": round(actual_sel, 5),
                        "vec_index": vec_index,
                        "prefilter": prefilter,
                        **stats,
                        "avg_rows_returned": avg_returned,
                        "valid_result": valid,
                    }
                    rows.append(row)
                    status = "" if valid else "  [INVALID: too few rows]"
                    print(f"  sel={sel_target:.3f} ({actual_sel:.3f})  {vec_index:13s} "
                          f"prefilter={str(prefilter):5s}  p50={stats['p50_ms']:6.1f}ms  "
                          f"p99={stats['p99_ms']:6.1f}ms  cov={stats['cov']:.2f}  "
                          f"ret_avg={avg_returned:.1f}{status}")
                except Exception as e:
                    print(f"  ERROR vec={vec_index} pf={prefilter} sel={sel_target}: {e}")

    no_filter_rows = []
    for vec_index, ds in datasets.items():
        nearest_base = make_nearest_base(vec_index, args.k)
        latencies, returned = run_cell(ds, queries, None, nearest_base, True,
                                        args.n_queries, args.warmup)
        stats = summarize_latencies(latencies)
        no_filter_rows.append({"vec_index": vec_index, "no_filter": True, **stats})
        print(f"\nBaseline (no filter): {vec_index}  p50={stats['p50_ms']:.1f}ms  cov={stats['cov']:.2f}")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "pylance_version": lance.__version__,
            "dataset": "SIFT-1M + synthetic price col",
            "n_queries": args.n_queries,
            "warmup_per_cell": args.warmup,
            "global_warmup": 50,
            "build_seconds": build_seconds,
            "baseline_no_filter": no_filter_rows,
            "rows": rows,
        }, f, indent=2)
    print(f"\nSaved to {args.out}")

    print("\n=== HNSW_SQ variance across selectivities (10% boundary check):")
    for r in rows:
        if r["vec_index"] == "IVF_HNSW_SQ" and r["prefilter"]:
            marker = " <-- 10% boundary" if 0.08 <= r["selectivity_target"] <= 0.12 else ""
            print(f"  sel={r['selectivity_target']:.3f} ({r['selectivity_actual']:.3f})  "
                  f"p50={r['p50_ms']:6.1f}ms  p99={r['p99_ms']:6.1f}ms  "
                  f"cov={r['cov']:.2f}{marker}")


if __name__ == "__main__":
    main()
