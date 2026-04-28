#!/usr/bin/env python3
"""Pre-create an empty Lance dataset on S3 so the Flink job appends into an existing table."""
import os
import sys
import pyarrow as pa
import lance

s3_path = sys.argv[1]
print(f"Creating empty Lance dataset at {s3_path}")

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

ds = lance.write_dataset(empty, s3_path, mode="overwrite", schema=schema)
print(f"Dataset created. Version: {ds.version}")
print(f"Fragments: {len(ds.get_fragments())}")
