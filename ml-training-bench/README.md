# Lance ML 训练场景压测

验证 Lance 作为 ML 训练数据源（图片 blob + 随机 batch sampling）是否真的是一个好选择。

## 对比对象

| 方案 | 代表 |
|---|---|
| Lance v2.2 with Blob V2 | 被测主角 |
| Raw S3 files + boto3 + DataLoader | 生产最常见 baseline |
| Parquet on S3 | Lance 官方比较对象（宣称快 100-2000x）|

## 实测结果（稳态 img/s）

| 方案 | img/s | 相对 |
|---|---|---|
| **Lance v2.2** | **237** ⭐ | 1.00x |
| Raw S3 | 159 | 0.67x |
| Parquet | 111 | 0.47x |

**Lance 比 Parquet 快 2.13x, 比 raw files 快 1.49x** — 真实的 ML 训练 pipeline 里，和 Lance 官方声称的 100-2000x 有显著差距。

## 发现的 Bug

1. **pylance 4.0.1 `take_blobs` 不接受乱序 indices** — 必须 sort
2. **`SafeLanceDataset` 对 blob 列只返回 descriptor** — 不能直接用于训练
3. **v2.2 不接受旧的 `lance-encoding:blob=true` metadata** — 必须用 `lance.blob.blob_field()`

详见 [REPORT.md](REPORT.md)。

## 快速查看

- [REPORT.md](REPORT.md) — 完整报告
- `scripts/` — 复现脚本
- `data/` — 原始 JSON 结果
