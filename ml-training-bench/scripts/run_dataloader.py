import os
import sys
import time
import json
import io
import argparse
import numpy as np
import torch
from torch.utils.data import DataLoader, Dataset
from PIL import Image
import boto3
import lance
import lance.blob
import pyarrow as pa
import pyarrow.compute as pc


def decode_jpeg(img_bytes, size=224):
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB").resize((size, size))
    arr = np.asarray(img, dtype=np.uint8).copy()
    return torch.from_numpy(arr).permute(2, 0, 1)


class RawS3Dataset(Dataset):
    def __init__(self, bucket, prefix, n_images):
        self.bucket = bucket
        self.prefix = prefix
        self.n = n_images
        self._s3 = None

    def _client(self):
        if self._s3 is None:
            self._s3 = boto3.client('s3', region_name='ap-northeast-1')
        return self._s3

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        key = f"{self.prefix}/img_{idx:06d}.jpg"
        resp = self._client().get_object(Bucket=self.bucket, Key=key)
        img_bytes = resp['Body'].read()
        return decode_jpeg(img_bytes, 224), idx % 1000


class LanceBlobDataset(Dataset):
    def __init__(self, uri, n_images):
        self.uri = uri
        self.n = n_images
        self._ds = None

    def _dataset(self):
        if self._ds is None:
            self._ds = lance.dataset(self.uri)
        return self._ds

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        return self.__getitems__([idx])[0]

    def __getitems__(self, indices):
        ds = self._dataset()
        indices = list(indices)
        meta_tbl = ds.take(indices, columns=["id", "label"])
        labels = meta_tbl["label"].to_pylist()
        sorted_pairs = sorted(enumerate(indices), key=lambda p: p[1])
        sorted_indices = [p[1] for p in sorted_pairs]
        order_map = [p[0] for p in sorted_pairs]
        blob_files = ds.take_blobs("image", indices=sorted_indices)
        restored = [None] * len(indices)
        for i_sorted, bf in enumerate(blob_files):
            restored[order_map[i_sorted]] = bf
        results = []
        for bf, label in zip(restored, labels):
            with bf as handle:
                img_bytes = handle.read()
            results.append((decode_jpeg(img_bytes, 224), label))
        return results


class ParquetS3Dataset(Dataset):
    def __init__(self, s3_uri, n_images):
        self.uri = s3_uri
        self.n = n_images
        self._dataset = None

    def _ds(self):
        if self._dataset is None:
            import pyarrow.dataset as pads
            self._dataset = pads.dataset(self.uri, format="parquet")
        return self._dataset

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        return self.__getitems__([idx])[0]

    def __getitems__(self, indices):
        ds = self._ds()
        indices_arr = pa.array(sorted(indices), type=pa.int64())
        tbl = ds.to_table(filter=pc.field("id").isin(indices_arr))
        idx_map = {row['id']: row for row in tbl.to_pylist()}
        results = []
        for idx in indices:
            row = idx_map[idx]
            img_bytes = row["image"]
            label = row["label"]
            results.append((decode_jpeg(img_bytes, 224), label))
        return results


def tuple_collate(items):
    tensors = torch.stack([x[0] for x in items])
    labels = torch.tensor([x[1] for x in items])
    return tensors, labels


def run_epoch(loader, name, epoch_i):
    batch_times = []
    t0 = time.perf_counter()
    first_batch_t = None
    n_images = 0
    for batch_idx, batch in enumerate(loader):
        if first_batch_t is None:
            first_batch_t = time.perf_counter() - t0
        batch_start = time.perf_counter()
        imgs, labels = batch
        batch_n = imgs.shape[0]
        n_images += batch_n
        time.sleep(batch_n * 0.0001)
        batch_times.append(time.perf_counter() - batch_start)
    total = time.perf_counter() - t0
    p50 = sorted(batch_times)[len(batch_times)//2] * 1000 if batch_times else 0
    p99 = sorted(batch_times)[int(len(batch_times)*0.99)] * 1000 if batch_times else 0
    print(f"  [{name} epoch {epoch_i}] total={total:.2f}s TTFB={first_batch_t:.2f}s imgs={n_images} "
          f"throughput={n_images/total:.1f} img/s batches={len(batch_times)} p50={p50:.0f}ms p99={p99:.0f}ms",
          flush=True)
    return {
        "name": name,
        "epoch": epoch_i,
        "total_s": total,
        "ttfb_s": first_batch_t,
        "n_images": n_images,
        "throughput": n_images / total,
        "batch_p50_ms": p50,
        "batch_p99_ms": p99,
        "batch_count": len(batch_times),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--method", choices=["raw_s3", "lance_take_blobs", "parquet"], required=True)
    ap.add_argument("--file-list-json", default="/home/hadoop/lance-ml-bench/data/file_list.json")
    ap.add_argument("--batch-size", type=int, default=256)
    ap.add_argument("--num-workers", type=int, default=8)
    ap.add_argument("--prefetch", type=int, default=2)
    ap.add_argument("--epochs", type=int, default=2)
    ap.add_argument("--out", default="/home/hadoop/lance-ml-bench/results/result.json")
    args = ap.parse_args()

    with open(args.file_list_json) as f:
        meta = json.load(f)

    print(f"=== Method: {args.method}")
    print(f"  N images: {meta['n_images']}, batch_size: {args.batch_size}, workers: {args.num_workers}")

    if args.method == "raw_s3":
        dataset = RawS3Dataset(meta["bucket"], meta["prefix"], meta["n_images"])
    elif args.method == "lance_take_blobs":
        dataset = LanceBlobDataset(meta["lance_v22_path"], meta["n_images"])
    elif args.method == "parquet":
        parquet_uri = meta.get("parquet_path") or f"{meta['lance_v22_path'].rsplit('/', 1)[0]}/parquet"
        dataset = ParquetS3Dataset(parquet_uri, meta["n_images"])

    loader_kwargs = dict(
        batch_size=args.batch_size,
        shuffle=True,
        num_workers=args.num_workers,
        collate_fn=tuple_collate,
    )
    if args.num_workers > 0:
        loader_kwargs.update(
            prefetch_factor=args.prefetch,
            persistent_workers=True,
            multiprocessing_context="spawn",
        )
    loader = DataLoader(dataset, **loader_kwargs)

    epoch_results = []
    for epoch in range(args.epochs):
        er = run_epoch(loader, args.method, epoch)
        epoch_results.append(er)

    with open(args.out, "w") as f:
        json.dump({
            "method": args.method,
            "config": {
                "batch_size": args.batch_size,
                "num_workers": args.num_workers,
                "prefetch": args.prefetch,
                "epochs": args.epochs,
                "n_images": meta["n_images"],
                "image_sample_bytes": meta["sample_actual_bytes"],
            },
            "epochs": epoch_results,
        }, f, indent=2)
    print(f"Saved: {args.out}")


if __name__ == "__main__":
    main()
