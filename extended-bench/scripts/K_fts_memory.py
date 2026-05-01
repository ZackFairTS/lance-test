import argparse
import gc
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time

import lance
import numpy as np
import pyarrow as pa


def generate_text(n_rows, words_per_doc, seed=42):
    rng = np.random.default_rng(seed)
    vocab = [
        "lance", "parquet", "column", "database", "storage", "vector", "search",
        "arrow", "benchmark", "python", "rust", "index", "query", "filter",
        "dataset", "fragment", "manifest", "s3", "cloud", "random", "access",
        "the", "a", "of", "and", "for", "with", "in", "on", "at", "by",
        "analytics", "machine", "learning", "training", "deep", "neural",
        "network", "tensor", "embedding", "retrieval", "augmented", "generation",
        "language", "model", "transformer", "attention", "prompt",
        "apple", "banana", "cherry", "dog", "elephant", "forest", "garden",
        "house", "igloo", "jungle", "kangaroo", "lemon", "mountain",
    ]
    docs = []
    for _ in range(n_rows):
        chosen = rng.choice(vocab, words_per_doc)
        docs.append(" ".join(chosen))
    return docs


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def child_build(n_rows, words_per_doc, work_dir):
    uri = os.path.join(work_dir, f"fts_{n_rows}_{words_per_doc}_{os.getpid()}.lance")
    shutil.rmtree(uri, ignore_errors=True)
    try:
        t0 = time.time()
        docs = generate_text(n_rows, words_per_doc)
        gen_seconds = time.time() - t0

        tbl = pa.table({
            "id": pa.array(range(n_rows), type=pa.int64()),
            "text": pa.array(docs, type=pa.string()),
        })
        input_bytes = tbl.nbytes

        gc.collect()
        t0 = time.time()
        ds = lance.write_dataset(tbl, uri, mode="overwrite")
        write_seconds = time.time() - t0

        t0 = time.time()
        ds.create_scalar_index("text", index_type="INVERTED")
        build_seconds = time.time() - t0

        peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_rss_mb = peak_rss_kb / 1024
        dataset_size_mb = du(uri) / 1e6

        return {
            "n_rows": n_rows,
            "words_per_doc": words_per_doc,
            "input_mb": round(input_bytes / 1e6, 2),
            "gen_seconds": round(gen_seconds, 2),
            "write_seconds": round(write_seconds, 2),
            "build_seconds": round(build_seconds, 2),
            "peak_rss_mb": round(peak_rss_mb, 1),
            "dataset_mb": round(dataset_size_mb, 2),
            "ok": True,
        }
    except Exception as e:
        return {
            "n_rows": n_rows,
            "words_per_doc": words_per_doc,
            "error": repr(e)[:800],
            "ok": False,
        }
    finally:
        shutil.rmtree(uri, ignore_errors=True)


def run_in_subprocess(n_rows, wpd, work_dir):
    cmd = [
        sys.executable, "-u", __file__,
        "--child-mode",
        "--n-rows", str(n_rows),
        "--words-per-doc", str(wpd),
        "--work-dir", work_dir,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=1800,
        )
        if result.returncode != 0:
            return {
                "n_rows": n_rows, "words_per_doc": wpd,
                "ok": False,
                "error": f"subprocess rc={result.returncode}: {result.stderr[-500:]}",
            }
        for line in result.stdout.splitlines():
            if line.startswith("RESULT_JSON:"):
                return json.loads(line[len("RESULT_JSON:"):])
        return {
            "n_rows": n_rows, "words_per_doc": wpd,
            "ok": False, "error": "no RESULT_JSON line in child output",
        }
    except subprocess.TimeoutExpired:
        return {
            "n_rows": n_rows, "words_per_doc": wpd,
            "ok": False, "error": "subprocess timeout after 1800s",
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", default=None)
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/K_fts_memory.json")
    ap.add_argument("--child-mode", action="store_true")
    ap.add_argument("--n-rows", type=int)
    ap.add_argument("--words-per-doc", type=int)
    args = ap.parse_args()

    if args.child_mode:
        result = child_build(args.n_rows, args.words_per_doc, args.work_dir)
        print(f"RESULT_JSON:{json.dumps(result)}")
        return

    work_dir = args.work_dir or tempfile.mkdtemp(prefix="k_fts_")
    os.makedirs(work_dir, exist_ok=True)

    test_configs = [
        (10_000, 50),
        (100_000, 50),
        (100_000, 200),
        (500_000, 50),
        (500_000, 200),
        (1_000_000, 50),
    ]

    results = []
    try:
        for n_rows, wpd in test_configs:
            print(f"\n=== FTS build: {n_rows:,} docs × {wpd} words (in subprocess)")
            t0 = time.time()
            r = run_in_subprocess(n_rows, wpd, work_dir)
            elapsed = time.time() - t0
            results.append(r)
            if r.get("ok"):
                print(f"  input={r['input_mb']}MB  build={r['build_seconds']}s  "
                      f"peak_rss={r['peak_rss_mb']}MB  dataset={r['dataset_mb']}MB  "
                      f"[wall={elapsed:.0f}s]")
            else:
                print(f"  FAILED ({elapsed:.0f}s wall): {r.get('error', '?')[:200]}")
    finally:
        if args.work_dir is None:
            shutil.rmtree(work_dir, ignore_errors=True)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "lance_version": lance.__version__,
            "measurement_method": "subprocess-per-config",
            "results": results,
        }, f, indent=2)
    print(f"\nSaved to {args.out}")

    print("\n=== Memory envelope (peak_rss / input_mb ratio):")
    for r in results:
        if not r.get("ok"):
            continue
        ratio = r["peak_rss_mb"] / r["input_mb"] if r["input_mb"] > 0 else 0
        print(f"  {r['n_rows']:>8} × {r['words_per_doc']:>3} wpd  "
              f"input={r['input_mb']:>7.1f} MB  "
              f"peak_rss={r['peak_rss_mb']:>7.1f} MB  "
              f"ratio={ratio:.1f}x  "
              f"build={r['build_seconds']}s")


if __name__ == "__main__":
    main()
