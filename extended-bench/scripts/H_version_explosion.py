import argparse
import gc
import json
import os
import shutil
import statistics
import tempfile
import time

import lance
import pyarrow as pa


def build_small_batch(offset, n=10):
    return pa.table({
        "id": pa.array(range(offset, offset + n), type=pa.int64()),
        "v": pa.array([0] * n, type=pa.int32()),
    })


def build_dataset_with_versions(uri, target_versions):
    if os.path.exists(uri):
        shutil.rmtree(uri)
    lance.write_dataset(build_small_batch(0, 10), uri, mode="overwrite")
    for i in range(1, target_versions):
        lance.write_dataset(build_small_batch(i * 10, 10), uri, mode="append")


def _make_offset_generator():
    counter = [0]
    def _next():
        counter[0] += 1
        return 10_000_000 + counter[0] * 100
    return _next


def timed(fn, warmup=2, rounds=10):
    for _ in range(warmup):
        fn()
        gc.collect()
    runs = []
    for _ in range(rounds):
        gc.collect()
        t0 = time.perf_counter()
        fn()
        runs.append(time.perf_counter() - t0)
    return {
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms": statistics.mean(runs) * 1000,
        "min_ms": min(runs) * 1000,
        "max_ms": max(runs) * 1000,
        "stdev_ms": statistics.stdev(runs) * 1000 if len(runs) > 1 else 0.0,
    }


def count_manifest_files(uri):
    versions_dir = os.path.join(uri, "_versions")
    if not os.path.isdir(versions_dir):
        return 0
    return len(os.listdir(versions_dir))


def measure_reads(uri):
    results = {"manifest_files": count_manifest_files(uri)}

    def do_open():
        return lance.dataset(uri)

    results["open"] = timed(do_open)

    def do_count():
        return lance.dataset(uri).count_rows()

    results["open_and_count"] = timed(do_count)

    def do_list_versions():
        return lance.dataset(uri).versions()

    results["list_versions"] = timed(do_list_versions)
    return results


def measure_append(source_uri, tmp_root):
    clone_uri = os.path.join(tmp_root, f"clone_{time.time_ns()}.lance")
    shutil.copytree(source_uri, clone_uri)
    try:
        next_offset = _make_offset_generator()

        def do_append():
            lance.write_dataset(
                build_small_batch(next_offset(), 5),
                clone_uri,
                mode="append",
            )

        return timed(do_append, warmup=1, rounds=5)
    finally:
        shutil.rmtree(clone_uri, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work-dir", default=tempfile.mkdtemp(prefix="h_ver_"))
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/H_version_explosion.json")
    args = ap.parse_args()

    version_counts = [10, 100, 500, 1000, 3000]
    results = {}

    for n_versions in version_counts:
        uri = os.path.join(args.work_dir, f"dataset_v{n_versions}.lance")
        print(f"\n=== Building dataset with {n_versions} versions...")
        t0 = time.time()
        build_dataset_with_versions(uri, n_versions)
        build_elapsed = time.time() - t0
        print(f"  build: {build_elapsed:.1f}s")

        metrics = measure_reads(uri)
        metrics["append"] = measure_append(uri, args.work_dir)
        metrics["build_seconds"] = round(build_elapsed, 2)
        results[str(n_versions)] = metrics

        print(f"  manifest files: {metrics['manifest_files']}")
        print(f"  open:            p50={metrics['open']['median_ms']:7.1f} ms")
        print(f"  open+count_rows: p50={metrics['open_and_count']['median_ms']:7.1f} ms")
        print(f"  list_versions:   p50={metrics['list_versions']['median_ms']:7.1f} ms")
        print(f"  append:          p50={metrics['append']['median_ms']:7.1f} ms")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "pylance_version": lance.__version__,
            "version_counts": version_counts,
            "results": results,
        }, f, indent=2)
    print(f"\nSaved to {args.out}")

    print("\n=== open() latency vs version count:")
    for n in version_counts:
        r = results[str(n)]
        print(f"  n={n:>5}  manifest_files={r['manifest_files']:>5}  "
              f"open_p50={r['open']['median_ms']:7.1f}ms  "
              f"append_p50={r['append']['median_ms']:7.1f}ms")


if __name__ == "__main__":
    main()
