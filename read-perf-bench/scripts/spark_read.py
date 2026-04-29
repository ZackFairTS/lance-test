import sys
import time
import json
from pyspark.sql import SparkSession

S3_PATH = sys.argv[1]
VERSION = sys.argv[2] if len(sys.argv) > 2 else "latest"
TAG = sys.argv[3]
OUT_JSON = sys.argv[4]

spark = (SparkSession.builder
    .appName(f"lance-read-bench-{TAG}")
    .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceCatalog")
    .config("spark.jars.packages", "com.lancedb:lance-spark-3.5_2.12:0.0.15")
    .config("spark.sql.extensions", "com.lancedb.lance.spark.extensions.LanceSparkExtensions")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate())

reader = (spark.read.format("lance")
    .option("path", S3_PATH))
if VERSION != "latest":
    reader = reader.option("version", VERSION)

print(f"Reading {S3_PATH} @ v{VERSION}")

results = {"tag": TAG, "path": S3_PATH, "version": VERSION}

import time
t0 = time.time()
df = reader.load()
load_time = time.time() - t0
print(f"load() took {load_time*1000:.0f}ms")
results["load_ms"] = load_time * 1000

durs = []
for i in range(3):
    t0 = time.time()
    count = df.count()
    durs.append(time.time() - t0)
    print(f"  count iter {i}: {durs[-1]*1000:.0f}ms -> {count}")
results["count"] = {
    "total_rows": count,
    "iterations_ms": [d*1000 for d in durs],
    "mean_ms": sum(durs)/len(durs)*1000,
    "p50_ms": sorted(durs)[len(durs)//2]*1000,
}

from pyspark.sql.functions import sum as _sum, avg
durs = []
for i in range(3):
    t0 = time.time()
    agg = df.agg(_sum("id").alias("sum_id"), avg("ts").alias("avg_ts")).collect()
    durs.append(time.time() - t0)
    print(f"  agg iter {i}: {durs[-1]*1000:.0f}ms")
results["aggregate"] = {
    "iterations_ms": [d*1000 for d in durs],
    "mean_ms": sum(durs)/len(durs)*1000,
    "p50_ms": sorted(durs)[len(durs)//2]*1000,
}

with open(OUT_JSON, "w") as f:
    json.dump(results, f, indent=2)
print(f"Saved: {OUT_JSON}")
spark.stop()
