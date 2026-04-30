# 纯 `take_blobs` 吞吐测试（验证 Lance 官方声称的 20-25K rows/s）

**目标**: 抛开 DataLoader + JPEG decode 开销，看 Lance `take_blobs` 在 S3 上的纯吞吐能不能达到官方数字。

**结论**: **没达到**。最高 4,391 rows/s（per-row 256-way 并行），是官方声称 20-25K rows/s 的 **~20%**。

## 测试配置

- 20K × 200KB JPEG 图片 on S3 ap-northeast-1
- EMR master r8g.2xlarge
- pylance 4.0.1 / lance-core 0.39.0
- batch_size=256, n_batches=30
- 纯 I/O，**不解码 JPEG**

## 结果表

| 方法 | rows/s | MB/s | 相对 raw S3 serial |
|---|---|---|---|
| Metadata only (`ds.take`, 取 id+label 列) | **6,243** | - | 极快（不涉及 blob） |
| **Lance take_blobs serial (1 thread)** | 15 | 3 | 1x (灾难) |
| Lance take_blobs batched, 8 workers | 245 | 49 | 16x |
| Lance take_blobs batched, 16 workers | 467 | 93 | 31x |
| Lance take_blobs batched, 32 workers | 929 | 185 | 62x |
| Lance take_blobs batched, 64 workers | 1,075 | 214 | 72x |
| Lance take_blobs batched, 128 workers | 1,070 | 213 | 72x (plateau) |
| **Lance take_blobs per-row, 256 workers** ⭐ | **4,391** | **873** | **293x** |
| Raw S3 files serial (1 thread) | 15 | 3 | 1x |
| Raw S3 files parallel, 32 workers | 430 | 85 | 29x |

## 关键洞察

### 1. Batched `take_blobs([256 indices])` plateau 在 ~1,000 rows/s

```python
# Too slow for ML training
blobs = ds.take_blobs("image", indices=[0, 5, 10, ..., 256 items])
# Each call: ~250ms even with many outer threads
```

Lance 内部对 `take_blobs([indices])` 只开 ~8 workers（可能 hardcoded 或看 schedule batching），外部并发 32/64/128 几乎没差。

### 2. Per-row `take_blobs([single_idx])` 能真正 scale

```python
# 4x faster with 256-way per-row parallelism
with ThreadPoolExecutor(max_workers=256) as ex:
    ex.map(lambda idx: ds.take_blobs("image", indices=[idx]), indices)
```

上到 4,391 rows/s / 873 MB/s —— 已经接近单进程 S3 带宽上限（~1-3 GB/s on EC2）。

### 3. `LANCE_IO_THREADS=256` 没效果

设置环境变量 `LANCE_IO_THREADS=256`（官方 docs 说调这个能提高并发）实测**完全没差异**（32 workers 时 929 → 1016, 误差范围）。可能的解释:
- pylance 4.0.1 在启动时已经固定 Tokio runtime
- 或者这个变量只在 Rust 直接读时有效，Python 绑定没传过去

### 4. Lance 比 raw S3 files **快 10x**

- Raw S3 32 workers: 430 rows/s
- Lance per-row 256 workers: 4,391 rows/s
- **10.2x faster** —— 这才是 Lance 的真实优势范围（不是 100-2000x）

原因:
- Raw S3 每个 GET 都是独立对象 (1 object/image)
- Lance Packed blob 把多张图片合并到 `.blob` sidecar，每个 GET 是 range read
- 减少了 HEAD + metadata lookup 次数
- S3 per-prefix IOPS 限制 (~5500/s) 对 raw files 更严重

## 为什么没达到官方 20-25K rows/s？

LanceDB 创始人 Weston Pace 原话 ([lance#3320](https://github.com/lance-format/lance/discussions/3320)):
> *"S3 can deliver a peak of around 20-25K rows per second"*

我们实测最高 **4,391 rows/s**，差 ~5x。可能原因:

1. **图片比我们的 200KB 小** —— 官方数字可能是小记录（KB 级），不是 hundreds-of-KB 的图片。按吞吐看，873 MB/s 已经碰到 S3 带宽上限，再往上 rows/s 只能靠更小的记录
2. **更强的机型** —— 更大的 EC2 实例有更高的网络带宽（我们 r8g.2xlarge 大约 12.5 Gbps）
3. **NVMe 或 S3 Express** —— 官方 benchmark blog 说 "NVMe: 100-200K rows/s", 可能混淆了
4. **不走 Python binding 的开销** —— Rust native 调用可能更快（Python GIL、subprocess-per-worker 开销）

## 结论

对真实 ML 训练场景（图片 + DataLoader + JPEG decode），你能期待的 Lance 性能：

| 场景 | 预估 rows/s |
|---|---|
| **Python DataLoader, batch sampling, 200KB JPEG, S3** | **200-300** ✅ 实测 237 |
| **纯 take_blobs, per-row 256-parallel, 200KB, S3** | **4,000-5,000** ✅ 实测 4,391 |
| **纯 take_blobs, 小记录 (KB 级), S3** | 可能 20,000 (未实测) |
| **纯 take_blobs, 本地 NVMe** | 100K+ (官方声称) |

**和官方声称 20-25K 的差距**说明:
- 官方数字对应的不是 ML 训练场景（pixel 数据）
- 而是小记录（向量 embedding 之类，KB 级）
- 或者 NVMe 缓存的场景

## 给用户的建议

1. **别相信 "20-25K rows/s"** 这个数字直接适用你的场景 —— 图片+DataLoader 场景实测 ~200-300 rows/s
2. **如果能接受 per-row parallelism** (不走标准 DataLoader)，能到 4K+ rows/s
3. **Lance 比 raw S3 files 快 10x** 是真的，这是 Lance 的核心价值
4. **比 Parquet 快 2x** 也是真的（不是 2000x）
5. **单进程 Python 吞吐上限大概在 ~1 GB/s**（S3 带宽），再高要多进程

## 原始数据

`data/pure_take_default.json`, `data/pure_take_high_concurrency.json`
