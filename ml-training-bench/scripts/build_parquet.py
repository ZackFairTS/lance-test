import io
import sys
import time
import json
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

S3_BASE = sys.argv[1]
N_IMAGES = int(sys.argv[2])
IMAGE_PX = int(sys.argv[3]) if len(sys.argv) > 3 else 512
PARQUET_PATH = f"{S3_BASE}/parquet"

print(f"Writing {N_IMAGES} images to Parquet at {PARQUET_PATH}")

def make_image_bytes(i):
    arr = np.random.default_rng(seed=i).integers(0, 256, size=(IMAGE_PX, IMAGE_PX, 3), dtype=np.uint8)
    img = Image.fromarray(arr)
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=85)
    return buf.getvalue()

import pyarrow.fs as pafs
bucket = S3_BASE.split("/")[2]
key_prefix = "/".join(S3_BASE.split("/")[3:]) + "/parquet"
fs = pafs.S3FileSystem(region="ap-northeast-1")

schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("label", pa.int32()),
    pa.field("image", pa.binary()),
])

BATCH = 2000
t0 = time.time()
file_counter = 0
for batch_start in range(0, N_IMAGES, BATCH):
    batch_end = min(batch_start + BATCH, N_IMAGES)
    ids = list(range(batch_start, batch_end))
    labels = [i % 1000 for i in ids]
    images = [make_image_bytes(i) for i in ids]
    tbl = pa.table([
        pa.array(ids, type=pa.int64()),
        pa.array(labels, type=pa.int32()),
        pa.array(images, type=pa.binary()),
    ], schema=schema)
    file_key = f"{key_prefix}/part_{file_counter:04d}.parquet"
    with fs.open_output_stream(f"{bucket}/{file_key}") as f:
        pq.write_table(tbl, f, compression='snappy', row_group_size=500)
    file_counter += 1
    elapsed = time.time() - t0
    print(f"  [{batch_end}/{N_IMAGES}] {file_counter} files, elapsed={elapsed:.1f}s rate={batch_end/elapsed:.0f} img/s", flush=True)

print(f"Done. {file_counter} parquet files at s3://{bucket}/{key_prefix}/")

with open("/home/hadoop/lance-ml-bench/data/file_list.json") as f:
    meta = json.load(f)
meta["parquet_path"] = PARQUET_PATH
with open("/home/hadoop/lance-ml-bench/data/file_list.json", "w") as f:
    json.dump(meta, f, indent=2)
print("Updated file_list.json with parquet_path")
