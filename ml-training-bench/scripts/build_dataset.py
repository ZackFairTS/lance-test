import os
import sys
import io
import time
import numpy as np
import pyarrow as pa
import lance
import lance.blob
import boto3
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

S3_BASE = sys.argv[1]
N_IMAGES = int(sys.argv[2]) if len(sys.argv) > 2 else 20_000
IMAGE_PX = int(sys.argv[3]) if len(sys.argv) > 3 else 512

LANCE_V22 = f"{S3_BASE}/lance-v22"
RAW_S3_PREFIX_FULL = f"{S3_BASE}/raw-files"
print(f"N_IMAGES={N_IMAGES}, size={IMAGE_PX}px")

bucket = S3_BASE.split("/")[2]
prefix = "/".join(S3_BASE.split("/")[3:]) + "/raw-files"

def make_image_bytes(i):
    arr = np.random.default_rng(seed=i).integers(0, 256, size=(IMAGE_PX, IMAGE_PX, 3), dtype=np.uint8)
    img = Image.fromarray(arr)
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=85)
    return buf.getvalue()

sample = make_image_bytes(0)
print(f"  Sample image size: {len(sample)} bytes = {len(sample)/1024:.1f} KB")

print("\nPhase 1: Building Lance v2.2 dataset with blob v2 extension")
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("label", pa.int32(), nullable=False),
    lance.blob.blob_field("image", nullable=False),
])

BATCH = 500
t0 = time.time()
is_first = True
for batch_start in range(0, N_IMAGES, BATCH):
    batch_end = min(batch_start + BATCH, N_IMAGES)
    ids = list(range(batch_start, batch_end))
    labels = [i % 1000 for i in ids]
    images = [make_image_bytes(i) for i in ids]
    batch_tbl = pa.Table.from_arrays([
        pa.array(ids, type=pa.int64()),
        pa.array(labels, type=pa.int32()),
        lance.blob.blob_array(images),
    ], schema=schema)
    mode = "overwrite" if is_first else "append"
    lance.write_dataset(batch_tbl, LANCE_V22, mode=mode, schema=schema,
                        data_storage_version="2.2",
                        max_rows_per_file=BATCH * 20)
    is_first = False
    elapsed = time.time() - t0
    if batch_start % (BATCH * 10) == 0 or batch_end == N_IMAGES:
        print(f"  [{batch_end}/{N_IMAGES}] elapsed={elapsed:.1f}s rate={batch_end/elapsed:.0f} img/s", flush=True)

ds = lance.dataset(LANCE_V22)
print(f"  Final: rows={ds.count_rows()} fragments={len(ds.get_fragments())} version={ds.version}")

print("\nPhase 2: Uploading raw JPEG files to S3")
s3 = boto3.client('s3', region_name='ap-northeast-1')

def upload_one(i):
    key = f"{prefix}/img_{i:06d}.jpg"
    img_bytes = make_image_bytes(i)
    s3.put_object(Bucket=bucket, Key=key, Body=img_bytes, ContentType='image/jpeg')
    return key

t0 = time.time()
with ThreadPoolExecutor(max_workers=32) as ex:
    for i, _ in enumerate(ex.map(upload_one, range(N_IMAGES))):
        if (i+1) % 2000 == 0 or (i+1) == N_IMAGES:
            elapsed = time.time() - t0
            print(f"  [{i+1}/{N_IMAGES}] elapsed={elapsed:.1f}s rate={(i+1)/elapsed:.0f} img/s", flush=True)

import json
with open("/home/hadoop/lance-ml-bench/data/file_list.json", "w") as f:
    json.dump({
        "bucket": bucket,
        "prefix": prefix,
        "n_images": N_IMAGES,
        "lance_v22_path": LANCE_V22,
        "image_px": IMAGE_PX,
        "sample_actual_bytes": len(sample),
    }, f, indent=2)

print(f"\nDone. Lance: {LANCE_V22}")
print(f"Raw S3: s3://{bucket}/{prefix}/ ({N_IMAGES} objects)")
