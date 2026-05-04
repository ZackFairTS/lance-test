# Lance v2.2 decimal column ignores sorted-order locality (36x worse than Parquet)

**Target repo**: https://github.com/lance-format/lance
**Environment**: pylance 4.0.1, lance-core 0.39.0, pyarrow 20.0.0, Amazon Linux 2023, Graviton r8g.2xlarge
**Category**: encoding / compression regression

---

## Summary

Lance v2.2's Decimal(7,2) column encoding does not take advantage of sorted-order locality. On a monotonically sorted column, Parquet zstd-3 writes **0.40 MB** where Lance v2.2 writes **14.57 MB** (3M rows) — a **36.7x** gap. On the same data as iid-random, both are within **1.07x** of each other. So the bug is not "Lance decimal encoding is generically bad"; it is specifically "Lance does not use delta / RLE / dict encoding when the data is clustered".

This matches what I observed on real TPC-DS store_sales at sf10: **every `$$$` column (ss_net_paid, ss_ext_sales_price, ...) is 5.4-5.8x larger in Lance v2.2 than Iceberg-Parquet**, even though the rows are identical and both writers use zstd level 3. TPC-DS columns are naturally clustered by sale date / store, which exposes the same missing-locality path this synthetic test isolates.

## Minimal reproduction

Script: [`decimal_bloat_repro.py`](../extended-bench/scripts/decimal_bloat_repro.py) — single file, no S3, no AWS.

```bash
python3 decimal_bloat_repro.py --rows 3000000 --mode sorted --out /tmp/lance_decimal_sorted
```

### Results (3,000,000 rows, Decimal(7,2), 62,871 unique values)

Per-column on-disk bytes for the sorted `money_decimal` column only:

| Writer                 | money_decimal MB | ratio vs parquet_zstd3 |
|------------------------|------------------|------------------------|
| **parquet_zstd (level 3)** | **0.40** ⭐       | 1.00x                  |
| parquet_snappy         | 0.48             | 1.21x                  |
| lance_2.0              | 48.00            | 120.99x 🔴             |
| lance_2.1              | 14.60            | 36.82x 🔴              |
| **lance_2.2**          | 14.57            | **36.73x** 🔴           |

### Control columns on the same dataset

Float64 column in the **same file**, from the **same random sequence**, rules out "Lance compresses worse in general":

| column          | Lance 2.2 | Parquet zstd3 | Lance / Parquet |
|-----------------|-----------|---------------|-----------------|
| money_decimal   | 14.57 MB  | 0.40 MB       | **36.73x** 🔴   |
| money_float64   | 23.67 MB  | 22.10 MB      | 1.07x ✅         |
| quantity_int8   | 2.68 MB   | 2.63 MB       | 1.02x ✅         |

Only decimal explodes.

### Comparison across input distributions (3M rows, same Lance v2.2 vs Parquet zstd3)

| distribution | Lance v2.2 MB | Parquet zstd3 MB | Lance / Parquet |
|---|---|---|---|
| iid lognormal                | 6.39  | 5.94 | 1.07x ✅ |
| TPC-DS-like (product-grouped)| 5.90  | 5.79 | 1.02x ✅ |
| **sorted (monotonic)**       | **14.57** | **0.40** | **36.73x** 🔴 |

**Lance v2.2 actually produces a *larger* file when the column is sorted than when it is random** (14.57 > 6.39). That is the smoking gun: sorted data is strictly more compressible than random, so the encoding path must be falling back to a no-compression layout as soon as it sees clustering it doesn't recognize.

## Impact

This is not a synthetic-only effect. On real TPC-DS sf10 store_sales (2.88M rows), I measured Lance v2.2 total size **3568 MB vs Iceberg-Parquet 1476 MB (2.4x)**, driven entirely by the money columns (all 5.4-5.8x larger individually). Full numbers: [REPORT_M_lance_vs_iceberg.md](../extended-bench/REPORT_M_lance_vs_iceberg.md).

For production TPC-DS-shaped workloads this is a **2-3x storage cost multiplier** for choosing Lance over Iceberg on the same S3 bucket.

## What I think is happening (speculation)

Decimal values are stored via `decimal128`, which is 16 bytes per value (raw). Parquet zstd3 on sorted decimals benefits from two things:

1. **Delta encoding** collapses monotonic sequences to a few bits per row.
2. **Dict encoding** + RLE amplifies clustering when nearby rows repeat the same value.

My reading of the v2.2 encoding picker (I haven't dug deep) is that the decimal physical type does not dispatch to either path. On floats Lance v2.2 hits a generic entropy path that works; on decimals it looks like there is no locality-aware specialization, so it falls back to raw 16-byte-per-row + zstd on mostly-random bytes.

Suggestive datapoint: on `sorted` mode, `money_float64` is 23.67 MB (Lance) vs 22.10 MB (Parquet) — **1.07x**. So Parquet is also not using delta on float64 (makes sense: IEEE754 bit patterns don't delta-encode well). Parquet's edge on sorted decimals comes from a decimal-specific path that Lance doesn't have.

## Requested fix

Either of these would close the gap to within ~2x of Parquet zstd3 on sorted decimal columns:
1. Teach the v2.2 decimal encoder to try delta encoding on monotonic subsequences.
2. Make the dictionary/RLE path available to decimal128 when cardinality or run length triggers the heuristic (currently triggered on strings per observation; may be missing for decimal).

## Related existing issues

- #3705 — "Lance has no compression for FixedSizeList vector embeddings" (similar "forgot to specialize encoding" issue on another type)
- The `B_filter_vs_parquet` tests in my repo ([REPORT.md](../extended-bench/REPORT.md)) already showed Lance filter queries suffer 2-3x because the underlying scan is heavier; this bug is one of the root causes of that scan overhead.

## Environment (full)

```
pylance: 4.0.1
lance-core: 0.39.0 (compiled in pylance 4.0.1)
pyarrow: 20.0.0
OS: Amazon Linux 2023 (GLIBC 2.34)
Arch: aarch64 (AWS Graviton r8g.2xlarge)
```

## Data files

See the `/tmp/lance_decimal_sorted/` layout produced by the script:
- `v2_2.lance/` — 14.57 MB for money_decimal column
- `zstd3.parquet` — 0.40 MB for money_decimal column
- Raw per-column byte breakdown JSON: [decimal_bloat_sorted.json](../extended-bench/data/decimal_bloat_sorted.json)

---

## Attached: complete repro run output

```
$ python3 decimal_bloat_repro.py --rows 3000000 --mode sorted

lance    4.0.1
pyarrow  20.0.0
rows     3,000,000
mode     sorted

[1/6] building synthetic table (mode=sorted)
      money_decimal unique values: 62,871 (2.10% of rows)

[2/6] writing 5 outputs
  lance_2.0        write=  0.038s total_mb=   99.00
  lance_2.1        write=  0.066s total_mb=   48.73
  lance_2.2        write=  0.061s total_mb=   48.72
  parquet_zstd3    write=  0.288s total_mb=   29.02
  parquet_snappy   write=  0.216s total_mb=   40.80

[3/6] PER-COLUMN BYTES BREAKDOWN
  column                    lance_2.0       lance_2.1       lance_2.2   parquet_zstd3  parquet_snappy
  id                         24.00 MB         7.78 MB         7.79 MB         3.90 MB        12.84 MB
  money_decimal              48.00 MB        14.60 MB        14.57 MB         0.40 MB         0.48 MB
  money_float64              24.00 MB        23.67 MB        23.67 MB        22.10 MB        24.84 MB
  quantity_int8               3.00 MB         2.68 MB         2.68 MB         2.63 MB         2.63 MB

[4/6] RATIOS vs parquet_zstd3 (per column)
  money_decimal           120.99x          36.82x          36.73x           1.21x

[5/6] VERDICT: decimal-specific bloat reproduced.
     float control ratio = 1.07x (normal),
     decimal ratio       = 36.73x (> 3x -> bug).
```
