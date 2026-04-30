import os
import sys
import time
import json
import random
import statistics
import argparse
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import lance
import lance.blob
import boto3

def bench_take_blobs_serial(uri, n_batches, batch_size, n_total):
    ds = lance.dataset(uri)
    random.seed(42)
    times = []
    total_bytes = 0
    t0 = time.perf_counter()
    for b in range(n_batches):
        indices = sorted(random.sample(range(n_total), batch_size))
        bs = time.perf_counter()
        blobs = ds.take_blobs("image", indices=indices)
        for bf in blobs:
            with bf as h:
                data = h.read()
                total_bytes += len(data)
        times.append(time.perf_counter() - bs)
    total = time.perf_counter() - t0
    return {
        "mode": "serial",
        "n_batches": n_batches,
        "batch_size": batch_size,
        "total_time": total,
        "total_rows": n_batches * batch_size,
        "throughput_rows_s": (n_batches * batch_size) / total,
        "total_mb": total_bytes / 1e6,
        "mb_per_s": (total_bytes / 1e6) / total,
        "p50_batch_ms": statistics.median(times) * 1000,
        "p99_batch_ms": sorted(times)[min(len(times)-1, int(len(times)*0.99))] * 1000,
    }


def bench_take_blobs_threads(uri, n_batches, batch_size, n_total, n_threads):
    ds = lance.dataset(uri)
    random.seed(42)
    batches = []
    for b in range(n_batches):
        indices = sorted(random.sample(range(n_total), batch_size))
        batches.append(indices)

    def fetch(indices):
        blobs = ds.take_blobs("image", indices=indices)
        n = 0
        for bf in blobs:
            with bf as h:
                n += len(h.read())
        return n, len(indices)

    total_bytes = 0
    total_rows = 0
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        for b, r in ex.map(fetch, batches):
            total_bytes += b
            total_rows += r
    total = time.perf_counter() - t0
    return {
        "mode": f"threads_{n_threads}",
        "n_batches": n_batches,
        "batch_size": batch_size,
        "total_time": total,
        "total_rows": total_rows,
        "throughput_rows_s": total_rows / total,
        "total_mb": total_bytes / 1e6,
        "mb_per_s": (total_bytes / 1e6) / total,
    }


def bench_take_meta_only(uri, n_batches, batch_size, n_total):
    ds = lance.dataset(uri)
    random.seed(42)
    times = []
    total_rows = 0
    t0 = time.perf_counter()
    for b in range(n_batches):
        indices = sorted(random.sample(range(n_total), batch_size))
        bs = time.perf_counter()
        tbl = ds.take(indices, columns=["id", "label"])
        total_rows += tbl.num_rows
        times.append(time.perf_counter() - bs)
    total = time.perf_counter() - t0
    return {
        "mode": "take_meta_only",
        "n_batches": n_batches,
        "batch_size": batch_size,
        "total_time": total,
        "total_rows": total_rows,
        "throughput_rows_s": total_rows / total,
        "p50_batch_ms": statistics.median(times) * 1000,
        "p99_batch_ms": sorted(times)[min(len(times)-1, int(len(times)*0.99))] * 1000,
    }


def bench_raw_s3_serial(bucket, prefix, n_batches, batch_size, n_total):
    s3 = boto3.client('s3', region_name='ap-northeast-1')
    random.seed(42)
    times = []
    total_bytes = 0
    t0 = time.perf_counter()
    for b in range(n_batches):
        indices = random.sample(range(n_total), batch_size)
        bs = time.perf_counter()
        for idx in indices:
            key = f"{prefix}/img_{idx:06d}.jpg"
            resp = s3.get_object(Bucket=bucket, Key=key)
            data = resp['Body'].read()
            total_bytes += len(data)
        times.append(time.perf_counter() - bs)
    total = time.perf_counter() - t0
    return {
        "mode": "raw_s3_serial",
        "n_batches": n_batches,
        "batch_size": batch_size,
        "total_time": total,
        "total_rows": n_batches * batch_size,
        "throughput_rows_s": (n_batches * batch_size) / total,
        "total_mb": total_bytes / 1e6,
        "mb_per_s": (total_bytes / 1e6) / total,
        "p50_batch_ms": statistics.median(times) * 1000,
    }


def bench_raw_s3_threads(bucket, prefix, n_batches, batch_size, n_total, n_threads):
    s3 = boto3.client('s3', region_name='ap-northeast-1')
    random.seed(42)
    batches = []
    for b in range(n_batches):
        batches.append(random.sample(range(n_total), batch_size))

    def fetch_one(idx):
        key = f"{prefix}/img_{idx:06d}.jpg"
        resp = s3.get_object(Bucket=bucket, Key=key)
        return len(resp['Body'].read())

    def fetch_batch(indices):
        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            sizes = list(ex.map(fetch_one, indices))
        return sum(sizes), len(sizes)

    total_bytes = 0
    total_rows = 0
    t0 = time.perf_counter()
    for indices in batches:
        b, r = fetch_batch(indices)
        total_bytes += b
        total_rows += r
    total = time.perf_counter() - t0
    return {
        "mode": f"raw_s3_threads_{n_threads}",
        "n_batches": n_batches,
        "batch_size": batch_size,
        "total_time": total,
        "total_rows": total_rows,
        "throughput_rows_s": total_rows / total,
        "total_mb": total_bytes / 1e6,
        "mb_per_s": (total_bytes / 1e6) / total,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file-list-json", default="/home/hadoop/lance-ml-bench/data/file_list.json")
    ap.add_argument("--n-batches", type=int, default=50)
    ap.add_argument("--batch-size", type=int, default=256)
    ap.add_argument("--out", default="/home/hadoop/lance-ml-bench/results/official_claim.json")
    args = ap.parse_args()

    with open(args.file_list_json) as f:
        meta = json.load(f)

    n_total = meta["n_images"]
    uri = meta["lance_v22_path"]
    bucket = meta["bucket"]
    prefix = meta["prefix"]
    n_batches = args.n_batches
    batch_size = args.batch_size

    print(f"Dataset: {uri}")
    print(f"Total images: {n_total}, batches={n_batches}, batch_size={batch_size}")
    print(f"Total rows to fetch: {n_batches * batch_size}")
    print()

    results = {
        "config": {
            "n_total": n_total,
            "n_batches": n_batches,
            "batch_size": batch_size,
            "uri": uri,
            "env_lance_io_threads": os.environ.get("LANCE_IO_THREADS", "default(64)"),
        },
        "runs": {}
    }

    print("[0] Warmup: read 20 random blobs to prime connections")
    ds = lance.dataset(uri)
    indices = sorted(random.sample(range(n_total), 20))
    blobs = ds.take_blobs("image", indices=indices)
    for bf in blobs:
        with bf as h:
            h.read()
    print("   warmup done\n")

    print("[1] take() metadata only (id, label)")
    r = bench_take_meta_only(uri, n_batches, batch_size, n_total)
    results["runs"]["take_meta_only"] = r
    print(f"   rows/s={r['throughput_rows_s']:.0f}  p50_batch={r['p50_batch_ms']:.0f}ms  p99={r['p99_batch_ms']:.0f}ms\n")

    print("[2] Lance take_blobs serial (single thread, read fully)")
    r = bench_take_blobs_serial(uri, n_batches, batch_size, n_total)
    results["runs"]["lance_take_blobs_serial"] = r
    print(f"   rows/s={r['throughput_rows_s']:.0f}  MB/s={r['mb_per_s']:.0f}  p50_batch={r['p50_batch_ms']:.0f}ms\n")

    for nt in [8, 16, 32]:
        print(f"[3-{nt}] Lance take_blobs threaded ({nt} workers)")
        r = bench_take_blobs_threads(uri, n_batches, batch_size, n_total, nt)
        results["runs"][f"lance_take_blobs_threads_{nt}"] = r
        print(f"   rows/s={r['throughput_rows_s']:.0f}  MB/s={r['mb_per_s']:.0f}\n")

    print("[4] Raw S3 files serial (one GET per row)")
    r = bench_raw_s3_serial(bucket, prefix, min(n_batches, 20), batch_size, n_total)
    results["runs"]["raw_s3_serial"] = r
    print(f"   rows/s={r['throughput_rows_s']:.0f}  MB/s={r['mb_per_s']:.0f}  p50_batch={r['p50_batch_ms']:.0f}ms\n")

    print("[5] Raw S3 files threaded (32 workers per batch)")
    r = bench_raw_s3_threads(bucket, prefix, n_batches, batch_size, n_total, 32)
    results["runs"]["raw_s3_threads_32"] = r
    print(f"   rows/s={r['throughput_rows_s']:.0f}  MB/s={r['mb_per_s']:.0f}\n")

    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved: {args.out}")


if __name__ == "__main__":
    main()
