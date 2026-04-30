# Lance ML 训练场景压测

验证 Lance 作为 ML 训练数据源（图片 blob + 随机 batch sampling）的真实性能。

## 两份报告

### [REPORT.md](REPORT.md) — 完整 PyTorch DataLoader pipeline 对比

模拟真实训练场景: 20K × 200KB JPEG, batch=256, num_workers=8, 2 epochs。

| 方案 | Epoch 2 稳态 | 相对 Lance |
|---|---|---|
| Lance v2.2 (Blob V2) | **237 img/s** ⭐ | 1.00x |
| Raw S3 files | 159 | 0.67x |
| Parquet | 111 | 0.47x |

### [REPORT_pure_take.md](REPORT_pure_take.md) — 纯 `take_blobs` 吞吐（验证官方声称数字）

排除 DataLoader + JPEG decode 开销，纯 I/O 吞吐。

| 方法 | rows/s | MB/s |
|---|---|---|
| Lance batched (64 workers) | 1,075 | 214 |
| **Lance per-row (256 workers)** ⭐ | **4,391** | **873** |
| Raw S3 (32 workers) | 430 | 85 |

**对官方声称的 "20-25K rows/s on S3" 未能达到**（最高 4,391 = 20%）。差距原因分析见报告。

## 核心结论

1. **真实 ML 训练场景下 Lance vs Parquet 快 2.13x**（不是官方的 100-2000x）
2. **纯 take_blobs 极限吞吐比 raw S3 files 快 10x**（这才是 Lance 真正的价值范围）
3. **官方"20-25K rows/s"对应的应该是小记录（KB 级 embedding），不是图片 blob**

## 发现的 Bug

1. **pylance 4.0.1 `take_blobs` 不接受乱序 indices** — DataLoader(shuffle=True) 直接坏
2. **`SafeLanceDataset` 对 blob 列只返回 descriptor** 不返回 bytes
3. **v2.2 不再支持 `lance-encoding:blob=true` metadata**

## 复现

`scripts/` 下有完整脚本，数据在 `data/`。
