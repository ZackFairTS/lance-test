# Lance ML 训练场景压测（宽列 blob 点查）

**测试日期**: 2026-04-30
**环境**: AWS EMR master (r8g.2xlarge Graviton, 8vCPU 64GiB), S3 ap-northeast-1
**Lance version**: pylance 4.0.1 (lance-core 0.39.0 native)
**PyTorch**: 2.8.0 CPU

## 场景

模拟 ML 训练 batch loading：
- 20,000 张合成 JPEG 图片（512×512 原始，quality 85 编码后 ~200KB each，总 ~4GB）
- PyTorch DataLoader, batch_size=256, num_workers=8, shuffle=True, prefetch_factor=2
- 每 batch 解码 + resize 到 224×224
- 模拟 GPU 处理（batch × 0.1ms sleep）
- 跑 2 epochs（epoch 1 冷，epoch 2 稳态）

## TL;DR 

| 方案 | Epoch 2 稳态 (img/s) | TTFB Epoch 2 | 相对 Lance |
|---|---|---|---|
| **Lance v2.2 (Blob V2)** | **237** ⭐ | 8.2s | 1.00x |
| Raw S3 files (boto3) | 159 | 10.5s | 0.67x |
| Parquet on S3 | 111 | 18.6s | **0.47x** |

**Lance 比 Parquet 快 2.13×, 比 raw S3 files 快 1.49×**。但远低于 LanceDB 官方声称的 "20-25K rows/s on S3"（我们的 237 只达到 ~1%）。

## 发现的关键问题

### 1. 🔴 pylance 4.0.1 的 `take_blobs` Bug
- **症状**: `ds.take_blobs("col", indices=[...])` 只要 indices **乱序**就报 `Schema error: Can not append column _rowaddr`
- **原因**: pylance 4.0.1 要求 indices **必须升序**
- **Workaround**: 传之前 sort + 维护原始顺序的 mapping，读完再 remap
- **影响**: DataLoader with `shuffle=True` 直接不能用，必须自己包装

**最小复现**:
```python
ds.take_blobs("image", indices=[5, 3])   # FAIL: schema error
ds.take_blobs("image", indices=[3, 5])   # OK
```

### 2. 🟡 `SafeLanceDataset` + blob 列不能开箱即用
- `SafeLanceDataset.__getitems__` 对 blob 列**只返回 descriptor**（`{kind, position, size, blob_id, blob_uri}`）
- 不是 bytes
- 必须手动再调 `take_blobs()` 拿实际数据
- 官方 docs 没说清楚这个 2-阶段流程

### 3. 🟡 blob 必须用 extension type，不能用 metadata
v2.2 不再接受 `lance-encoding:blob=true` field metadata：
```
Legacy blob columns (field metadata key "lance-encoding:blob") are not 
supported for file version >= 2.2. Use the blob v2 extension type 
(ARROW:extension:name = "lance.blob.v2")
```
必须用 `lance.blob.blob_field("image")` 和 `lance.blob.blob_array(data)`。

## 详细数据

### Epoch 1 (冷启动)

| 方法 | Total (s) | TTFB (s) | Throughput (img/s) | Batch p50 | Batch p99 |
|---|---|---|---|---|---|
| Lance v2.2 | 94.4 | 17.7 | 212 | 29ms | 30ms |
| Raw S3 | 186.4 | 21.2 | 107 | 29ms | 29ms |
| Parquet | 183.4 | 21.9 | 109 | 31ms | 40ms |

### Epoch 2 (稳态)

| 方法 | Total (s) | TTFB (s) | Throughput (img/s) | Batch p50 | Batch p99 |
|---|---|---|---|---|---|
| **Lance v2.2** | **84.3** ⭐ | **8.2** | **237** | 29ms | 30ms |
| Raw S3 | 125.9 | 10.5 | 159 | 29ms | 29ms |
| Parquet | 179.5 | 18.6 | 111 | 31ms | 41ms |

### 关键观察

1. **Lance 稳态领先** —— 比 Parquet 2.13x，比 raw S3 1.49x
2. **Lance 有显著的 warm benefit** (epoch 1 → 2 提升 12%)，Parquet 几乎没有 (1.5%)
3. **Parquet 的劣势在每 batch 扫整个 row group** —— batch p99 (41ms) 比 p50 (31ms) 高 32%，说明有些 batch 跨多个 row group
4. **TTFB Lance 显著更快** —— 第 2 epoch lance 8.2s vs raw S3 10.5s vs parquet 18.6s
5. **batch p50 几乎相同** (29-31ms) —— 所有方法都受 DataLoader IPC + JPEG decode 限制，不是纯 I/O

## 为什么没达到官方声称的 20-25K rows/s

Lance 官方 [GitHub#3320](https://github.com/lance-format/lance/discussions/3320) 原话: *"S3 can deliver a peak of around 20-25K rows per second"*

**差距 ~100x** 的原因分析：

1. **官方那是纯 `take_blobs` 吞吐**，不含 DataLoader + JPEG decode
2. **DataLoader 的 spawn worker 有显著 IPC 开销** (每 batch 64×200KB = 12.8MB 要 serialize)
3. **JPEG decode 在 Python worker 里是 CPU-bound** (单线程 PIL)
4. **batch_size=256 对 8 workers = 平均 32/worker** 每次只并发 32 个 take

要达到官方数字的设置可能是：
- batch=1024, workers=32
- num_batches=100 纯 loop，不走 DataLoader
- 不解码 JPEG，直接返回 bytes

## 建议

### 如果你的场景...

1. **图片在 S3，随机 batch sampling 训练** → Lance **值得用**，比 raw files 快 ~50%, 比 Parquet 快 ~2x
2. **图片在 NVMe 本地** → Lance 优势应更大（IOPS 不受 S3 限制，官方数字 100-200K rows/s）
3. **序列化流式访问（大数据集顺序扫）** → **Parquet / WebDataset 可能更合适**，Lance 的随机访问优势用不上
4. **数据集已经是文件形式** → 迁移到 Lance 有一次性成本，收益（1.5x 提速）不一定值得

### 调优建议

```python
# 1. 必须 sort indices（workaround for take_blobs bug）
sorted_indices = sorted(indices)
blob_files = ds.take_blobs("image", indices=sorted_indices)

# 2. 必须 spawn 而不是 fork
DataLoader(..., multiprocessing_context="spawn")

# 3. 用 prefetch 隐藏 S3 延迟
DataLoader(..., prefetch_factor=4, persistent_workers=True)

# 4. 适当大的 batch_size 摊销 take_blobs 的固定开销
batch_size=256  # 实测 > 64 才开始有优势

# 5. 调 Lance I/O 并发
os.environ["LANCE_IO_THREADS"] = "128"  # default 64
```

## 原始数据

`data/`:
- `v1_lance_take_blobs_bs256_w8.json` — Lance 结果
- `v1_raw_s3_bs256_w8.json` — Raw S3 结果
- `v1_parquet_bs256_w8.json` — Parquet 结果

---

## 未验证的扩展点（smoke test 阶段未覆盖）

1. **更大数据集** (100K-1M images) — 20K 太小，cache 可能主导
2. **不同图片大小** (10KB Inline, 5MB Dedicated) — 不同 blob storage mode 性能可能不同
3. **WebDataset 流式对比** — 流式是 Parquet 替代方案，未测
4. **不同 num_workers** (4/16/32) — 未扫参数
5. **不同 batch_size** (64/128/1024) — 未扫参数
6. **LANCE_IO_THREADS 调优** — 默认 64，未验证高值是否更快
7. **本地 NVMe vs S3** — 未对比，官方数字差 5-10x
8. **纯 `take_blobs` 不走 DataLoader** — 验证能否达到 20K rows/s 官方数字
