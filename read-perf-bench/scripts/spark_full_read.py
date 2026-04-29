import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import length as _len, sum as _sum, avg, max as _max

S3_PATH = sys.argv[1]
VERSION = sys.argv[2]
TAG = sys.argv[3]
OUT_JSON = sys.argv[4]

spark = (SparkSession.builder
    .appName(f"lance-full-read-{TAG}")
    .getOrCreate())

reader = spark.read.format("lance").option("path", S3_PATH).option("version", VERSION)
df = reader.load()

results = {"tag": TAG, "path": S3_PATH, "version": VERSION}

durs = []
for i in range(3):
    t0 = time.time()
    row = df.select(_sum(_len("payload")).alias("total_bytes")).collect()[0]
    elapsed = time.time() - t0
    durs.append(elapsed)
    print(f"  full-read iter {i}: {elapsed*1000:.0f}ms total_bytes={row.total_bytes}")
results["full_read"] = {
    "mean_ms": sum(durs)/len(durs)*1000,
    "p50_ms": sorted(durs)[len(durs)//2]*1000,
    "samples_ms": [d*1000 for d in durs],
    "total_payload_bytes": row.total_bytes,
}

durs = []
for i in range(3):
    t0 = time.time()
    row = df.filter("id >= 1000000 AND id < 2000000").select(_sum("ts").alias("s")).collect()[0]
    durs.append(time.time() - t0)
    print(f"  range iter {i}: {durs[-1]*1000:.0f}ms sum={row.s}")
results["range_filter"] = {
    "mean_ms": sum(durs)/len(durs)*1000,
    "p50_ms": sorted(durs)[len(durs)//2]*1000,
    "samples_ms": [d*1000 for d in durs],
}

with open(OUT_JSON, "w") as f:
    json.dump(results, f, indent=2)
print(f"Saved: {OUT_JSON}")
spark.stop()
