import argparse
import json
import os
import shutil
import tempfile

import lance
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def make_column(kind, n_rows, seed=42):
    rng = np.random.default_rng(seed)
    if kind == "int64_sequential":
        return "int64", pa.array(range(n_rows), type=pa.int64())
    if kind == "int64_random":
        return "int64", pa.array(rng.integers(0, 10 ** 18, n_rows, dtype=np.int64))
    if kind == "float32_vector_128d":
        return "vector_128d", pa.FixedSizeListArray.from_arrays(
            pa.array(rng.standard_normal(n_rows * 128).astype(np.float32)),
            list_size=128,
        )
    if kind == "float32_vector_1536d":
        return "vector_1536d", pa.FixedSizeListArray.from_arrays(
            pa.array(rng.standard_normal(n_rows * 1536).astype(np.float32)),
            list_size=1536,
        )
    if kind == "uint8_embeddings_1024d":
        return "embedding_1024d", pa.FixedSizeListArray.from_arrays(
            pa.array(rng.integers(0, 256, n_rows * 1024, dtype=np.uint8)),
            list_size=1024,
        )
    if kind == "long_text":
        words = ["the", "quick", "brown", "fox", "jumped", "over", "lazy", "dog",
                 "lorem", "ipsum", "data", "bench", "lance", "parquet", "analytics"]
        strings = [" ".join(rng.choice(words, 30)) for _ in range(n_rows)]
        return "long_text", pa.array(strings, type=pa.string())
    if kind == "short_categorical":
        cats = [f"CAT_{i}" for i in range(50)]
        return "short_categorical", pa.array(rng.choice(cats, n_rows), type=pa.string())
    if kind == "jpeg_blob_small":
        from io import BytesIO
        from PIL import Image
        images = []
        for i in range(n_rows):
            arr = rng.integers(0, 256, (64, 64, 3), dtype=np.uint8)
            img = Image.fromarray(arr)
            buf = BytesIO()
            img.save(buf, format="JPEG", quality=85)
            images.append(buf.getvalue())
        return "jpeg_small", pa.array(images, type=pa.binary())
    raise ValueError(f"unknown kind: {kind}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-rows", type=int, default=100_000)
    ap.add_argument("--work-dir", default=tempfile.mkdtemp(prefix="i_compress_"))
    ap.add_argument("--out", default="/home/hadoop/lance-extended-bench/results/I_compression.json")
    args = ap.parse_args()

    kinds = [
        "int64_sequential",
        "int64_random",
        "float32_vector_128d",
        "float32_vector_1536d",
        "uint8_embeddings_1024d",
        "long_text",
        "short_categorical",
        "jpeg_blob_small",
    ]

    results = {}
    for kind in kinds:
        print(f"\n=== Column type: {kind}")
        try:
            col_name, arr = make_column(kind, args.n_rows)
        except ImportError as e:
            print(f"  SKIP (missing dependency): {e}")
            results[kind] = {"error": f"missing dep: {e}"}
            continue
        except Exception as e:
            print(f"  SKIP (generation failed): {e}")
            results[kind] = {"error": f"generation failed: {e}"}
            continue
        tbl = pa.table([arr], names=[col_name])

        variants = [
            ("lance_v2_0", "lance", {"data_storage_version": "2.0"}),
            ("lance_v2_1", "lance", {"data_storage_version": "2.1"}),
            ("parquet_snappy", "parquet", {"compression": "snappy"}),
            ("parquet_zstd", "parquet", {"compression": "zstd"}),
            ("parquet_uncompressed", "parquet", {"compression": "none"}),
        ]

        kind_results = {}
        for name, kind_w, kwargs in variants:
            path = os.path.join(args.work_dir, f"{kind}_{name}" + (".parquet" if kind_w == "parquet" else ".lance"))
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            try:
                if kind_w == "lance":
                    lance.write_dataset(tbl, path, mode="overwrite", **kwargs)
                else:
                    pq.write_table(tbl, path,
                                   row_group_size=1 << 20,
                                   use_dictionary=True,
                                   write_statistics=True,
                                   data_page_version="2.0",
                                   **kwargs)
                size_bytes = du(path)
                kind_results[name] = {"size_bytes": size_bytes, "size_mb": round(size_bytes / 1e6, 3)}
                print(f"  {name:22s}: {size_bytes/1e6:7.2f} MB")
            except Exception as e:
                kind_results[name] = {"error": str(e)[:200]}
                print(f"  {name:22s}: ERROR {str(e)[:100]}")

        raw_bytes = tbl.nbytes
        kind_results["_arrow_in_memory_mb"] = round(raw_bytes / 1e6, 3)
        results[kind] = kind_results
        print(f"  (arrow in-memory: {raw_bytes/1e6:.2f} MB)")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({
            "lance_version": lance.__version__,
            "n_rows": args.n_rows,
            "results": results,
        }, f, indent=2)
    print(f"\nSaved to {args.out}")

    print("\n=== Size vs Parquet(snappy) baseline AND vs raw Arrow in-memory:")
    for kind, kr in results.items():
        if "error" in kr:
            continue
        baseline = kr.get("parquet_snappy", {}).get("size_mb")
        raw = kr.get("_arrow_in_memory_mb")
        if not baseline or not raw:
            continue
        print(f"\n  {kind} (raw arrow={raw:.2f} MB):")
        for name in ["lance_v2_0", "lance_v2_1", "parquet_snappy", "parquet_zstd", "parquet_uncompressed"]:
            v = kr.get(name, {}).get("size_mb")
            if v is None:
                continue
            vs_parquet = v / baseline
            vs_raw = v / raw
            print(f"    {name:22s}  {v:7.2f} MB  ({vs_parquet:5.2f}x pq, {vs_raw:5.2f}x raw)")


if __name__ == "__main__":
    main()
