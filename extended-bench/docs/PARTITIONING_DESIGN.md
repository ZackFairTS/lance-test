# `video_id` 分区方案设计（Pattern D）

> **Review 状态（2026-05-11 ai-slop-remover pass + 作者后续修订）**：
> 2 hallucinated APIs removed（`lance.schema.get_field_id`、错误 `write_dataset` 异常假设）；
> 1 missing import 修复（`timedelta`）；section 3 JSON 与官方 spec 结构不一致已按实际 spec 重写；
> section 4 Iceberg 哈希互操作性 **已验证 bit-identical**（int64(34) → 2017239379 与 Iceberg Java 一致）；
> N 选择表、Netflix/ByteDance 声称、S3 paginator 片段已降级为未验证 / 推测；
> untested code blocks 已标注；**ACID 范围警告已上移到 §1**（原本埋在 §11）；
> §10 决策树规模门限从 "< 1 亿" 降级为 "≤ 10M 实测 + >10M 需自行验证"，与 `O_composite_key.py` 实测范围严格对齐。
> 详细见文末 Review Log。

基于 Lance 2025-12 新特性 [Lance Partitioning Spec](https://lance.org/format/namespace/partitioning-spec/)，
为 `(video_id, frame_id)` 工作负载设计的分区方案。

**适用场景**：视频/图像帧存储，`video_id` 基数中到高（10³–10⁷），
查询几乎总是携带 `video_id` 谓词，需要亚秒级响应。

> ⚠️ **关键前提（读这份文档前请先知道）**：Lance 官方 Partitioning Spec 定义的是 Directory Namespace
> 层面的分区（由 `__manifest` Lance 表 + partition spec JSON metadata 组成），
> **不是**单一 Lance 表的原生分区。本文档描述的 Pattern D 是**应用层手动分区**（每个 `video_id` 一个独立 Lance dataset），
> 其目录布局和 spec 的 physical layout **不完全一致**。如需要 spec 兼容的 Partitioned Namespace，
> 请使用 Rust 层 API 或等 pylance 高层 API 发布；本文档的 Python 示例是当前可立即落地的变通方案。
>
> ⚠️ **ACID 范围限制**：Lance 的 ACID 语义是 per-dataset（OCC）。跨 dataset（即跨 `video_id`）的原子写入
> 本方案**不支持**。若业务需要"多个视频帧必须一起成功或一起失败"，需要应用层做 staging + rename，
> 或等待 spec 的 Multi-Partition Transaction 实现落地。详见 §11。

---

## 1. 为什么分区

对比其他方案（来自本项目 `O_composite_key.py` 实测 + PR #5480 bench）：

| 方案 | 适用规模 | 小 video 范围查询 | 跨 video 扫描 | 运维成本 |
|---|---|---|---|---|
| V1 双 BTREE | 单表 ≤ 1 亿行 | 500µs–数 ms | 全表扫 | 低 |
| V4 排序 + BTREE 前缀 | 单表 ≤ 10 亿行 | 依赖 zonemap 剪枝 | 全表扫 | 中（需维持有序） |
| **D 按 video_id 分区** | **10 亿–PB** | **读单个子表** | **读多个子表** | **中-高** |

分区的核心价值：**每次 `video_id = X` 的查询只需打开一个子 dataset**——
manifest 加载和元数据扫描的代价**只与该视频自身的版本/fragment 数有关，不随总视频数或总行数增长**。
（严格讲，打开 dataset 是 O(manifest-chain-length)，不是真正的 O(1)；此处指"不随 video 总量 scale"。）

对 PB 级视频数据这是**量级差别**。分区 + 每租户/每实体独立 dataset 的模式
与 Netflix Media Data Lake、字节跳动 LAS 等公开描述的 Lance 生产部署**思路一致**，
但本文档**没有**它们按 `video_id` 分区的一手证据，请当作行业常见模式而非官方背书。

---

## 2. 分区键选择

### 原则
1. **必须**按查询的最高频谓词分区 → `video_id`
2. **必须**保证每个分区的数据量可控（建议 10⁶–10⁸ 行 / 10 GB 以内 / 10³ fragment 以内）
3. **不要**直接按 `video_id` identity 分区（如果视频数 > 10⁴）—— 会产生**海量小分区**，S3 listing 和 namespace 元数据会崩

### 推荐：两层分区 `bucket(video_id, N) / video_id`

借鉴 Iceberg 的 `bucket()` 变换 + ClickHouse `Distributed` 表的 shard 模式：

```
s3://bucket/frames/
├── _namespace.json                 # Lance namespace 元数据
├── bucket=000/
│   ├── video_id=17/
│   │   ├── _lance/manifests/...
│   │   └── data/...
│   ├── video_id=213/
│   └── ...
├── bucket=001/
│   ├── video_id=42/
│   └── ...
└── bucket=N-1/
```

- **一层 bucket**：`bucket(video_id, N)` 做粗粒度哈希分片 —— 防止顶层目录下 video_id=* 数过多导致 S3 list 慢
- **二层 video_id**：identity 分区 —— 单视频查询直接落到一个 dataset

### N 的选择

> ⚠️ **以下数值是经验推测（rule of thumb），未经基准测试验证**。
> S3 listing 和目录深度的实际性能拐点取决于 region、S3 Express vs 标准、并发 list 度。
> **请在你的环境中实测**目录 listing 延迟后再决定 N。

| 视频总数 | 建议 N（推测）| 单 bucket 下 video 数（推测）|
|---|---|---|
| < 10⁴ | N = 1（跳过 bucket 层，直接 video_id 分区）| N/A |
| 10⁴–10⁵ | N = 64 | ~1500 |
| 10⁵–10⁶ | N = 256 | ~4000 |
| 10⁶–10⁷ | N = 1024 | ~10000 |
| > 10⁷ | N = 4096 + 需要分层 namespace | — |

目标：任何单个目录下的 `ls` 返回 ≤ 几千项。具体数字以自测为准。

---

## 3. Lance Namespace Partitioning 规范映射

> ⚠️ 本节 JSON 按 [Lance Partitioning Spec](https://lance.org/format/namespace/partitioning-spec/)
> 的字段名重写；仍为**示意**，字段细节（尤其 `transform` 内部配置键名、`result_type` 具体值）
> 以官方 spec 当前版本为准。spec 仍在演进，**建议以 Rust 实现的单元测试作为 ground truth**。

spec 要求将 partition 元数据存储在 **Directory Namespace 的 `__manifest` Lance 表的
表级 metadata** 中，键为 `partition_spec_v<N>`，值为 JSON 字符串。一个 spec 对象的骨架：

```json
{
  "id": 1,
  "fields": [
    {
      "field_id": "bucket_video_id",
      "source_ids": [<field_id_of_video_id>],
      "transform": { "type": "bucket", "num_buckets": 256 },
      "result_type": { "type": "int32" }
    },
    {
      "field_id": "video_id",
      "source_ids": [<field_id_of_video_id>],
      "transform": { "type": "identity" },
      "result_type": { "type": "int64" }
    }
  ]
}
```

> ⚠️ 字段名 `id`/`fields`/`field_id`/`source_ids`/`transform`/`result_type` 已对齐官方 spec。
> 但 `transform` 内部键名（如上例中的 `"num_buckets"`）本文档**未在官方 spec 文本中找到明确确认**，
> 落地前请核对最新 spec 的 "Partition Transform → Transform Schema" 章节。
>
> ⚠️ `source_ids` 引用的是 Lance schema 中的 **field ID**（不是列名）。spec 定义字段 ID 存放在
> Arrow field metadata 的 `lance:field_id` 键下。**没有**公开的 `lance.schema.get_field_id()` Python 辅助函数
> （2026-05 pylance 4.0.1 验证），需要自行从 `dataset.schema.field("video_id").metadata[b"lance:field_id"]`
> 读取并解码。

---

## 4. 写入路径（参考实现）

由于 Lance Partitioning Spec 的 Python 高层 API 截至 pylance 4.0.1 **尚无明显公开符号**
（`import lance` 顶层仅有 `LanceNamespace`、`LanceNamespaceStorageOptionsProvider`，
未见 partition spec 构造/读取函数；主要通过 Rust / Geneva feature-eng 平台使用），
当前生产落地推荐**应用层手动分区**：

```python
# UNTESTED — illustrative; verify in your environment
import lance
import pyarrow as pa
import pyarrow.compute as pc

NAMESPACE_ROOT = "s3://my-bucket/frames"
N_BUCKETS = 256

def bucket_id(video_id: int) -> int:
    """Iceberg-compatible bucket hash for int64: Murmur3 -> mod N.

    验证：mmh3.hash(int64(34).to_bytes(8, "little"), signed=False)
    == 2017239379, 与 Iceberg Java TestBucketing 参考值一致（2026-05 实测）。
    因此本函数输出与 Iceberg `bucket[N]` transform on long **bit-identical**。
    """
    import mmh3  # pip install mmh3
    h = mmh3.hash(video_id.to_bytes(8, "little", signed=True), signed=False)
    return (h & 0x7fffffff) % N_BUCKETS

def path_for(video_id: int) -> str:
    b = bucket_id(video_id)
    return f"{NAMESPACE_ROOT}/bucket={b:03d}/video_id={video_id}"


def write_frames(frames: pa.Table):
    """frames 必须包含 video_id 列；按 video_id 分组后每组写入对应的 Lance dataset。

    注：pylance 4.0.1 实测 `lance.write_dataset(..., mode="append")` 在
    目标不存在时会**自动创建**（日志中 "No existing dataset ... it will be created"），
    不会抛异常；因此无需 try/except 退化到 overwrite。
    """
    video_ids = frames["video_id"].unique().to_pylist()
    for vid in video_ids:
        mask = pc.equal(frames["video_id"], vid)
        chunk = frames.filter(mask)
        lance.write_dataset(
            chunk, path_for(vid), mode="append", data_storage_version="2.1"
        )


def open_video(video_id: int) -> lance.LanceDataset:
    """读取单个视频的所有帧。打开成本 = O(该 video 自身 manifest 链长度)，
    不随 video 总数 / 总行数 scale。"""
    return lance.dataset(path_for(video_id))


def query_frame(video_id: int, frame_id: int) -> pa.Table:
    """WHERE video_id=X AND frame_id=Y → 分区路由后只查一个子表 + frame_id 标量索引。"""
    ds = open_video(video_id)
    return ds.to_table(filter=f"frame_id = {frame_id}")
```

---

## 5. 子表内的索引策略

在每个 `video_id={v}` 子 dataset 内，`video_id` 已成常量，**真正起作用的是 `frame_id` 索引**：

```python
from datetime import timedelta
import lance

def ensure_indexes(video_id: int):
    ds = lance.dataset(path_for(video_id))
    # BTREE 支持 frame_id 的范围查询（WHERE frame_id BETWEEN a AND b）
    ds.create_scalar_index("frame_id", "BTREE", replace=True)
```

如果子表内还有其他常见过滤列（如 `timestamp`、`object_class`），每列独立建索引，
靠 Lance 查询引擎自动 AND 相交（本项目 `O_composite_key.py` V1 已验证这是最优路径）。

---

## 6. 跨 video 查询（反模式 + 补救）

### 反模式：`WHERE video_id IN (v1, v2, ..., v100)`

原生分区方案必须：
```python
results = []
for vid in [v1, v2, ...]:
    results.append(open_video(vid).to_table(filter=...))
return pa.concat_tables(results)
```

这是 O(K) 的 dataset open 成本（每次 open ≈ 一个 video 的 manifest 链加载）。当 K > 几百时会很慢。

### 补救方案：Materialized "hot-set" 视图

对高频跨视频查询（如"最近 7 天所有视频的关键帧"），**额外维护一份按时间或类别分区的聚合表**：

```
s3://bucket/frames_by_day/
└── day=2026-05-10/
    ├── _lance/...
    └── data/...    # 这一天所有 video 的 keyframe，带 video_id 列
```

这份是冗余数据，通过离线 ETL 定期从主表重算。

---

## 7. Compaction 策略

每个子 dataset 独立运行 compaction：

```python
from datetime import timedelta
import lance

def compact_video(video_id: int):
    ds = lance.dataset(path_for(video_id))
    ds.optimize.compact_files(target_rows_per_fragment=1_000_000)
    ds.cleanup_old_versions(older_than=timedelta(days=7))
```

**好处**：compaction 并发安全（单视频内仍是 Lance OCC 并发模型，但视频之间完全独立）——
这也是 Pattern D 相对 Pattern A/C 的附加红利。

---

## 8. 迁移现有单表到分区布局

```python
# UNTESTED — illustrative; verify in your environment
# 尤其：下方 S3 paginator 片段使用 Delimiter="/_lance/" 来枚举"每个 _lance 目录所在的
# dataset root"。S3 ListObjectsV2 支持任意字符串作为 Delimiter，但该模式的
# CommonPrefixes/Contents 行为作者未实测；可靠替代方案是直接枚举 Contents 并按路径
# 解析 "bucket=XXX/video_id=YYY" 前缀（见注释）。
def migrate(source_path: str, batch_size: int = 100_000):
    src = lance.dataset(source_path)
    scanner = src.scanner(batch_size=batch_size)
    for batch in scanner.to_batches():
        write_frames(pa.Table.from_batches([batch]))
    print("migration complete — now run compaction on each partition")

    # Compact all partitions. 下方枚举子 dataset 的策略未经 S3 实测，
    # 建议改为直接从 write_frames() 里累积 video_id 集合。
    import re
    import boto3
    s3 = boto3.client("s3")
    bucket, prefix = NAMESPACE_ROOT.replace("s3://", "").split("/", 1)
    paginator = s3.get_paginator("list_objects_v2")
    seen_datasets = set()
    pat = re.compile(r"(bucket=\d+/video_id=\d+)")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            m = pat.search(obj["Key"])
            if m:
                seen_datasets.add(f"s3://{bucket}/{prefix}/{m.group(1)}")
    for p in seen_datasets:
        lance.dataset(p).optimize.compact_files()
```

---

## 9. 运维注意事项

| 项 | 建议 |
|---|---|
| **元数据列表性能** | 在 EMR/K8s 侧预先缓存 `ls` 结果（TTL 1 小时），避免每查询都 S3 list |
| **"新视频"冷启动** | 第一次 `write_frames` 必须用 `mode="overwrite"` 或预建表；参考 `lance-stress/scripts/create_dataset.py` |
| **监控** | 每个子 dataset 的 `fragment_count`、`version_count`；超过阈值自动触发 compact |
| **GC** | `cleanup_old_versions` 必须每个子表单独跑；跨表并行 |
| **安全性** | IAM 按 `bucket=*/video_id=*` 前缀授权；单视频隔离天然友好 |
| **Spark 读** | lance-spark 0.0.15 实测语法为 `spark.read.format("lance").option("path", path_for(vid)).load()`（非 `.load(path)` 形式；见 `lance-read-bench/scripts/spark_full_read.py`）。跨视频查询建议注册 Lance Catalog 后走 SQL，或在 Python 侧 union 多个 DataFrame |

---

## 10. 跟本项目的 5 种方案对比的选择树

```
需要 video_id+frame_id 查询吗？
│
├─ 数据量 ≤ 10M 行 → 选 V1（双独立 BTREE） ——— O_composite_key.py @ 10M/10k-videos 实测
│
├─ 数据量 10M–10 亿行、查询永远有 video_id 谓词
│  └─ 选 V4（写入时按 (video_id, frame_id) 排序 + BTREE(video_id)）
│     ⚠ V4 在 10M/10k-videos 实测 Q_range/Q_vid 均 ≤ 2.2ms（优于 V1 3-6×），
│       但 1 亿+规模外推需自行验证 —— 关键风险：维持排序的写入侧成本 +
│       每个 video 行数膨胀对 zonemap 剪枝效果的影响
│
├─ 数据量 > 10 亿行、或需要单视频独立 compaction/retention 策略
│  └─ 选 **Pattern D (本文档)**
│     ⚠ 本文档的设计基于 Lance Partitioning Spec + 应用层实现，尚未在本项目跑端到端
│
└─ 真的需要复合索引能力（非 equality 点查的复杂复合谓词）
   └─ 等 Lance PR #5480 合并 / 用 Catalyzed fork / 考虑 TileDB
```

---

## 11. 未决问题（需要进一步实验确认）

1. **pylance 4.0.1 对 Partitioning Spec 的 Python 支持** —— 2026-05 本地检查 `dir(lance)`
   顶层**没有** partition spec 构造/解析相关公开符号（只有 `LanceNamespace` /
   `LanceNamespaceStorageOptionsProvider` 这类通用 namespace 入口），
   `lance.LanceDataset` 也仅暴露 `partition_expression`。Rust 层实现走在前面，
   Python 高层封装预计会在后续 pylance 版本中补齐。当前生产建议**应用层手动分区**（本文档 §4）。
2. **bucket transform 与 Iceberg 的哈希兼容性** —— 本文档 §4 的 `bucket_id()`
   已用已知测试向量 `int64(34) → 2017239379` **实测与 Iceberg Java 参考实现 bit-identical**。
   其它类型（string、decimal、date/timestamp）的 Iceberg 兼容性本文档**未覆盖**，
   如需跨 catalog 互操作请按 Iceberg spec 各类型独立验证。
3. **超大 video（> 10⁸ 帧）的子表分裂策略** —— 需要进一步引入时间或帧号的二级分区。
   这种罕见 case 可以用 `video_id={v}/shard={f // 10^7}` 的第三层分区应对（未实测）。
4. **Namespace-level transaction / atomic multi-partition write** —— 详见 §1 顶部警告，
   以及 spec 中 "Multi-Partition Transaction" 章节（目前作为规范描述存在，各实现完成度需单独评估）。

---

## 参考文献

- [Lance Namespace Partitioning Spec](https://lance.org/format/namespace/partitioning-spec/) — Lance 官方分区规范（2025-12，仍在演进）
- [Lance Transactions spec](https://lance.org/format/table/transaction/) — 子表内的 MVCC/OCC 模型
- [Iceberg bucket transform](https://iceberg.apache.org/spec/#bucket-transform-details) — 哈希算法参考（本文档 §4 已对 int64 路径做 bit-level 兼容性验证）
- [Netflix Media Data Lake（LanceDB blog）](https://www.lancedb.com/blog/case-study-netflix) — Lance 在视频工作负载的生产描述（**未**明确按 `video_id` 分区；作为行业模式参考）
- [BDD100K × LanceDB](https://www.lancedb.com/blog/unifying-the-av-ml-stack-lancedb) — 帧级数据的物化视图模式
- 本项目 `O_composite_key.py` — 单表方案（V0–V4）实测对比

---

## Review Log（2026-05-11 ai-slop-remover）

| # | Concern | Verdict | Evidence | Action |
|---|---|---|---|---|
| 1 | `write_dataset(mode="append")` 异常假设 | **FALSE_ALARM / hallucinated exception** | 本地 pylance 4.0.1 测试：append 到不存在目标**自动创建**，仅 WARN 日志 | §4 移除 try/except，改为直接 append |
| 1 | `timedelta` 未 import | **REAL** | 源码 §5/§7 直接使用 `timedelta(days=7)` 无 import | §5/§7 加 `from datetime import timedelta` |
| 1 | `lance.schema.get_field_id()` 存在性 | **REAL — hallucinated API** | `dir(lance.schema)` 只有 `LanceSchema/json_to_schema/schema_to_json`，无 `get_field_id` | §3 移除引用，改为从 `field.metadata[b"lance:field_id"]` 手取 |
| 1 | S3 `Delimiter="/_lance/"` paginator 片段 | **CANNOT_VERIFY（低信心 → 降级）** | S3 支持任意字符串 Delimiter，但与 `Contents` 迭代的交互未实测 | §8 加 UNTESTED 注释，提供改用路径正则的替代方案 |
| 2 | Partitioning Spec JSON 字段 | **REAL — 字段名错误** | 官方 spec 字段为 `id/fields/field_id/source_ids/transform/result_type`；doc 原写 `version/partitioning` | §3 按 spec 重写 JSON；`transform` 内部子字段（`num_buckets`）加"待核对"注释 |
| 3 | Iceberg bucket 哈希兼容性 | **REAL — 已验证 bit-identical（int64 路径）** | `mmh3.hash(int64(34).to_bytes(8,"little"), signed=False) == 2017239379`，与 Iceberg Java TestBucketing 参考值一致 | §4 docstring 升级为"verified for int64"；提醒其他类型未覆盖 |
| 4 | "O(1) metadata load" 口径 | **REAL — 技术性不准确** | 打开 dataset 是 O(manifest-chain-length)，不是真正 O(1) | §1/§6 改为"不随 video 总数 scale"，保留原意 |
| 5 | N=64/256/1024/4096 选择表 | **CANNOT_VERIFY — 无基准** | 本项目无对应 benchmark | 加显著"rule of thumb，自行实测"警告，保留数字但降级 |
| 6 | Spark read 语法 | **REAL — 语法错误** | `lance-read-bench/scripts/spark_full_read.py` 用 `.option("path", ...).load()` 形式，非 `.load(path)` | §9 表格项改写为实际 working 语法 |
| 7 | Netflix/ByteDance "都走这条路" | **CANNOT_VERIFY** | 公开资料只说 Lance 用于大规模视频/多模态，未明确按 video_id 分区 | §1 降级为"模式一致，非官方背书" |
| 8 | pylance 4.0.1 Python 高层 API FUD | **REAL — 有具体证据** | `dir(lance)` 无 partition 相关符号；`LanceDataset` 仅 `partition_expression` | §11 从泛泛 FUD 改为具体缺失符号证据 |
| 9 | ACID 跨 dataset 警告位置 | **REAL — 埋藏过深** | 原在 §11 末尾，是架构级限制 | 上移到 §1 顶部警告框 |
| 10 | §10 决策树 vs 实测覆盖 | **RESOLVED — 降级规模门限** | `O_composite_key.py` 实测规模是 10M 行 / 10k video；1 亿+ 规模属外推，不是实测 | §10 决策树节点阈值从 "< 1 亿" 降级为 "≤ 10M"（实测覆盖），10M–10 亿档位新增 ⚠ 警告提示"需自行验证" + 列出两条关键风险（写入侧排序成本、大视频对 zonemap 剪枝的影响） |

### Hallucinated APIs 汇总

| Hallucinated API | Actual API | Where claimed |
|---|---|---|
| `lance.schema.get_field_id()` | 无；需读 `field.metadata[b"lance:field_id"]` | §3 note（已移除） |
| `lance.write_dataset(mode="append")` 对不存在目标抛 `FileNotFoundError/ValueError` | 实际**自动创建**（WARN 日志），不抛异常 | §4 `write_frames` 的 try/except（已简化） |
| `spark.read.format("lance").load(path)` | `spark.read.format("lance").option("path", path).load()` | §9 表格（已修正） |

### Overclaimed Citations

| Overclaimed | Actual evidence | Action |
|---|---|---|
| "Netflix Media Data Lake、字节跳动 LAS 都走这条路"（按 video_id 分区） | LanceDB blog 只描述用 Lance 存视频/多模态，未明确分区键 | §1 降级为"思路一致；非一手证据" |
| "O(1) metadata load" | 实际 O(manifest-chain-length)；但不随 video 总数 scale | §1/§6 改写为精确口径 |
| "与 Iceberg bucket transform 一致的哈希" | 已 bit-identical 验证 int64；其他类型未测 | §4 docstring 补充 scope 限定 |
| §3 对 Lance Partitioning Spec JSON 的描述 | 字段名与官方 spec 不同 | §3 按 spec 重写；`transform` 内部配置键标注"待核对" |

### 结果

**ACTION: needs-author-attention**（非 ready-to-ship）

- 4 项 hallucinated / 错误 API 已修复
- 2 项规范级错误（§3 JSON、§9 Spark）已修复
- 3 项过度声称已降级（Netflix、O(1)、N 选择表）
- 1 项结构性改动（ACID 警告上移）
- §10 决策树已降级：规模门限对齐 `O_composite_key.py` 实测范围（10M 行 / 10k videos），
  10M–10 亿档位标注为推测并列出关键风险

落地前建议：作者亲自跑通 §4/§8 的代码块（目前标 UNTESTED）、
在目标 S3 region 实测 §2 的 N 选择、以及验证 §10 决策树中 >10M 规模档位的 V4/Pattern-D 行为。
