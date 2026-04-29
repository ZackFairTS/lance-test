import os
import sys
import time
import json
import pyarrow as pa
import lance
import random
import string

S3_PATH = sys.argv[1]
TOTAL_ROWS = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000_000
TARGET_FRAGMENTS = int(sys.argv[3]) if len(sys.argv) > 3 else 10000

ROWS_PER_APPEND = TOTAL_ROWS // TARGET_FRAGMENTS
print(f"Creating dataset at {S3_PATH}")
print(f"Total rows: {TOTAL_ROWS}, target fragments: {TARGET_FRAGMENTS}, rows/append: {ROWS_PER_APPEND}")

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("ts", pa.int64(), nullable=False),
    pa.field("payload", pa.string(), nullable=True),
])

empty = pa.table({
    "id": pa.array([], type=pa.int64()),
    "ts": pa.array([], type=pa.int64()),
    "payload": pa.array([], type=pa.string()),
})
lance.write_dataset(empty, S3_PATH, mode="overwrite", schema=schema)
print(f"Empty dataset initialized")

def make_batch(start_id, n):
    ids = pa.array(list(range(start_id, start_id + n)), type=pa.int64())
    ts = pa.array([int(time.time() * 1000)] * n, type=pa.int64())
    payload = pa.array(
        [''.join(random.choices(string.hexdigits, k=80)) for _ in range(n)],
        type=pa.string()
    )
    return pa.table({"id": ids, "ts": ts, "payload": payload})

t0 = time.time()
batch = make_batch(0, ROWS_PER_APPEND)
del batch

base_ts = int(time.time() * 1000)
cur_id = 0
start = time.time()
last_print = start
for i in range(TARGET_FRAGMENTS):
    ids = pa.array(list(range(cur_id, cur_id + ROWS_PER_APPEND)), type=pa.int64())
    ts = pa.array([base_ts + i] * ROWS_PER_APPEND, type=pa.int64())
    payload = pa.array(
        [''.join(random.choices(string.hexdigits, k=80)) for _ in range(ROWS_PER_APPEND)],
        type=pa.string()
    )
    tbl = pa.table({"id": ids, "ts": ts, "payload": payload})
    lance.write_dataset(
        tbl, S3_PATH, mode="append",
        max_rows_per_file=ROWS_PER_APPEND + 1,
    )
    cur_id += ROWS_PER_APPEND
    now = time.time()
    if now - last_print > 10:
        elapsed = now - start
        rate = (i + 1) / elapsed
        eta = (TARGET_FRAGMENTS - i - 1) / rate
        print(f"  [{i+1}/{TARGET_FRAGMENTS}] elapsed={elapsed:.1f}s rate={rate:.1f} appends/s eta={eta:.0f}s", flush=True)
        last_print = now

total_elapsed = time.time() - start
print(f"Done. Total elapsed: {total_elapsed:.1f}s")
ds = lance.dataset(S3_PATH)
print(f"Final: rows={ds.count_rows()} fragments={len(ds.get_fragments())} version={ds.version}")

output_path = sys.argv[4] if len(sys.argv) > 4 else "/home/hadoop/lance-read-bench/results/E_dataset_info.json"
with open(output_path, "w") as f:
    json.dump({
        "path": S3_PATH,
        "total_rows": ds.count_rows(),
        "fragments": len(ds.get_fragments()),
        "version": ds.version,
        "build_elapsed_s": total_elapsed,
        "target_fragments": TARGET_FRAGMENTS,
    }, f, indent=2)
