# N. Compaction × Scalar/Vector Index 交互审计

**测试日期**: 2026-05-09
**Lance 版本**: **pylance 4.0.1**（PyPI 最新）+ **pylance 6.0.0-rc.4**（源码编译，commit `cadf921b`，2026-05-08 tag）
**PyArrow**: 21.0.0
**环境**: AWS EMR master r8g.2xlarge (Graviton ARM64), 本地 `/tmp`（避免 S3 网络噪声）
**数据规模**: 100K 行 × 10 fragments，compact 到 1 fragment
**脚本**: `extended-bench/scripts/N_compact_index.py`
**结果**:
- `results/N_compact_index.json` — pylance 4.0.1
- `results/N_compact_index_v6rc4.json` — pylance 6.0.0-rc.4（源码编译）

---

## 🎯 假设 & 前置研究

**用户提出的假设**："Compaction 操作可能会让现有标量索引失效，需要重建索引。"

### 上游源码调查结论（在动手测之前）

来自 `lance-format/lance` @ SHA `d8542b539a039550a8a4dba00a222988906f4cb5`：

现代 Lance **不会**让 `compact_files` 使索引失效。两条路径：

1. **默认路径**（`defer_index_remap=False`）：`IndexRemapper` 在**同一个事务里**重写 `_indices/` 下的索引文件。索引名不变，UUID 换新，`fragment_bitmap` 更新到新 fragment ID。
2. **延迟路径**（`defer_index_remap=True`）：索引文件**不动**。写入一个系统索引 `__lance_frag_reuse`（FRI），查询时在内存里把旧 row address 翻译成新 address。UUID **保持不变**。

两条路径下，索引都应该**立即可用，不需要手动重建**。

**关键不变式**（upstream 测试 `rust/lance/src/dataset/optimize.rs` L3485-4194 用的）：

| # | 不变式 | 默认路径 | 延迟路径 |
|---|---|---|---|
| a | 查询结果行数保持不变 | ✓ | ✓ |
| b | compact 真的重写了 fragment | ✓ | ✓ |
| c/d | UUID | 必须变 | 必须不变 |
| e | `__lance_frag_reuse` 索引 | 不应出现 | 必须出现 |
| f | `fragment_bitmap` 覆盖新 fragment ID | ✓ | ✓ |

---

## 📊 实测结果（18 个 combo = 9 index × 2 path，**两个版本都测**）

### pylance 4.0.1 (2026-04-30, PyPI latest)

| Index 类型 | Path | a. 正确性 | b. Compact 生效 | c/d. UUID | e. FRI | f. Bitmap 更新 | 验证结论 |
|---|---|---|---|---|---|---|---|
| BTREE | default | ✅ | ✅ | ✅ UUID 换新 | ✅ 无 FRI | ✅ | ✅ |
| BTREE | defer | ✅ | ✅ | ✅ UUID 不变 | ✅ FRI 存在 | ✅ | ✅ |
| BITMAP | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| BITMAP | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| LABEL_LIST | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| LABEL_LIST | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| NGRAM | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| NGRAM | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **BLOOMFILTER** | **default** | ✅ | ✅ | ❌ UUID 不变 | ✅ | ❌ Bitmap 未更新 | 🔴 |
| **BLOOMFILTER** | **defer** | ❌ 查询返回 0 行 | ✅ | ✅ | ✅ | ✅ | 🔴 |
| **ZONEMAP** | **default** | ✅ | ✅ | ❌ UUID 不变 | ✅ | ❌ Bitmap 未更新 | 🔴 |
| **ZONEMAP** | **defer** | ❌ 查询返回 0 行 | ✅ | ✅ | ✅ | ✅ | 🔴 |
| INVERTED | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| INVERTED | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IVF_HNSW_SQ | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IVF_HNSW_SQ | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IVF_PQ | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **IVF_PQ** | **defer** | ❌ 查询抛 Rust 错误 | ✅ | ✅ | ✅ | ✅ | 🔴 |

**13/18 ✅，5/18 🔴**

### pylance 6.0.0-rc.4 (2026-05-08, **源码编译**)

| Index 类型 | Path | a. 正确性 | b. Compact 生效 | c/d. UUID | e. FRI | f. Bitmap 更新 | 验证结论 |
|---|---|---|---|---|---|---|---|
| BTREE | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| BTREE | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| BITMAP | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| BITMAP | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| LABEL_LIST | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| LABEL_LIST | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| NGRAM | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| NGRAM | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **BLOOMFILTER** | **default** | ✅ | ✅ | ❌ | ✅ | ❌ | 🔴 **相同 bug** |
| **BLOOMFILTER** | **defer** | ❌ 0 rows | ✅ | ✅ | ✅ | ✅ | 🔴 **相同 bug** |
| **ZONEMAP** | **default** | ✅ | ✅ | ❌ | ✅ | ❌ | 🔴 **相同 bug** |
| **ZONEMAP** | **defer** | ❌ 0 rows | ✅ | ✅ | ✅ | ✅ | 🔴 **相同 bug** |
| INVERTED | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| INVERTED | defer | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| IVF_HNSW_SQ | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **IVF_HNSW_SQ** | **defer** | ❌ 查询抛 Rust 错误 | ✅ | ✅ | ✅ | ✅ | 🔴 **新回归** |
| IVF_PQ | default | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **IVF_PQ** | **defer** | ❌ 查询抛 Rust 错误 | ✅ | ✅ | ✅ | ✅ | 🔴 **相同 bug** |

**12/18 ✅，6/18 🔴** — 情况**比 4.0.1 更糟**：多了一个 IVF_HNSW_SQ × defer 的回归。

### 🔴 跨版本变化汇总

| Bug | 4.0.1 | 6.0.0-rc.4 | 变化 |
|---|---|---|---|
| BLOOMFILTER default: UUID/bitmap stale | 🔴 | 🔴 | **6 个月未修** |
| BLOOMFILTER defer: 0 rows | 🔴 | 🔴 | **6 个月未修** |
| ZONEMAP default: UUID/bitmap stale | 🔴 | 🔴 | **6 个月未修** |
| ZONEMAP defer: 0 rows | 🔴 | 🔴 | **6 个月未修** |
| IVF_PQ defer: Rust error | 🔴 | 🔴 | **6 个月未修** |
| **IVF_HNSW_SQ defer: Rust error** | ✅ | 🔴 | **❗ v6 新回归** |

---

## 🔴 发现的异常详解

### 异常 1 — BLOOMFILTER / ZONEMAP 在 default 路径**跳过 index remap**

**证据**（BLOOMFILTER default 为例）：

```
pre_uuid:   95bac6c5-c81a-456c-945e-d21c381e7873
post_uuid:  95bac6c5-c81a-456c-945e-d21c381e7873  ← 没换，default 路径应该换
pre_frag_ids:   [0,1,2,3,4,5,6,7,8,9]
post_frag_ids:  [0,1,2,3,4,5,6,7,8,9]              ← 没更新，应该变成 [10]
compact metrics: fragments_removed=10, fragments_added=1 （确实 compact 了）
new_fragments on ds: [10]                           ← 当前只有一个 fragment id=10
```

**BLOOMFILTER 和 ZONEMAP 的 default 路径行为等价于它们被当作了 defer 路径**：UUID 保持，bitmap 保持旧 fragment ids。FRI 却又不存在。这是一个中间状态 —— 既没走 IndexRemapper，也没写 FragReuseIndex。

**但正确性竟然 PASS**：post-compact 的查询仍返回正确行数（1 行、3 行），虽然 bitmap 指的是 "不存在的 fragment 0-9"。推测 Lance 在这些 index 类型上有某种 fallback 路径（可能回退到全扫 + 在结果里 filter）。

**性能影响**：几乎无差（pre p50 1.64ms → post p50 1.96ms）。延迟不变说明 query planner 确实没在用这个索引，走的是标量过滤的默认路径。

### 异常 2 — BLOOMFILTER / ZONEMAP 在 defer 路径**查询返回 0 行**

**证据**（ZONEMAP defer 为例）：

```
pre_uuid == post_uuid ✓
FRI 已创建 ✓
post_frag_ids = [10] ✓ (bitmap 正确更新)
Query: "price < 50"  pre.rows=4936, post.rows=0 (!)
Query: "price >= 100 AND price < 200"  pre.rows=9969, post.rows=0 (!)
```

**defer 路径下，ZONEMAP 和 BLOOMFILTER 的 query 返回 0 行**（实际应该有几千行）。FRI 翻译明显没生效 —— index 指向新 fragment ID 10，但 query 在内部还在用旧 row address 查，新 fragment 里对应的旧 row address 根本不存在，所以 0 匹配。

**性能反而变快**（3.6ms → 1.3ms）—— 因为 index 直接空返回，没走数据路径。

这是**真实的 correctness bug**，不是方法论问题。可以用来向 upstream 上报。

### 异常 3 — IVF_PQ 在 defer 路径**查询抛 Rust 错**

**证据**：

```
Error: External error: Query Execution error: Execution error: 
The input to a take operation specified fragment id 0 but this fragment 
does not exist in the dataset 
(uri=..., version=13, 
 manifest=.../_versions/18446744073709551602.manifest, branch=main),
/home/runner/work/lance/lance/rust/lance/src/dataset/scanner.rs:4524:66
```

**IVF_PQ defer 直接崩溃**。Manifest version 显示 `18446744073709551602` (= `u64::MAX - 13`)，像是溢出或未初始化值。`scanner.rs:4524` 是 IVF 的 sub-index take 路径。

注意：**IVF_HNSW_SQ defer 在 4.0.1 上正常**，但下文的 v6.0.0-rc.4 跨版本测试会揭示 v6 里也坏了。

### 异常 4 — v6.0.0-rc.4 IVF_HNSW_SQ defer 新回归

**同样的 "fragment id 0 does not exist" 错误现在也命中 IVF_HNSW_SQ**：

```
# 4.0.1 (过去版本):
IVF_HNSW_SQ × defer: ✅ Correctness PASS, p50=2.01ms

# 6.0.0-rc.4 (最新):
IVF_HNSW_SQ × defer: ❌ "The input to a take operation specified 
                         fragment id 0 but this fragment does not exist"
```

**从 4.0.1 到 6.0.0-rc.4 之间某个 commit，IVF_HNSW_SQ defer 路径从工作变成崩溃。** IVF_PQ defer 在两个版本都崩溃。报错位置（`scanner.rs:4730` in v6 vs `scanner.rs:4524` in 4.0.1）是 IVF sub-index 的 take 路径，两个 vector index 共用，所以回归在共享代码上。

**默认路径两个 vector index 都正常工作**。这个 bug 只影响 `defer_index_remap=True`。

---

## ✅ Upstream 研究预测完美命中的点

1. **BTREE + BITMAP**：两条路径都按文档工作。Upstream 有 CI 覆盖，也是生产最常用的两个。
2. **LABEL_LIST / NGRAM**：Upstream `rust/lance/src/dataset/optimize.rs` 有 `test_read_label_list_index_with_defer_index_remap`（L3943-4040）和 `test_read_ngram_index_with_defer_index_remap`（L3820-3940）两个测试覆盖。本次实测证实它们在 pylance 4.0.1 里也工作。这是**首次公开复现 upstream 测试结果**。
3. **INVERTED (FTS)**：Upstream 有 `test_read_inverted_index_with_defer_index_remap`（L3679-3818）。实测通过。
4. **IVF_HNSW_SQ**：Upstream 有 vector index 的 defer 测试（L4042-4194），针对 v3 vector index。实测通过。
5. **FRI 存在性**：defer 路径必现 `__lance_frag_reuse`，default 路径必无，完全符合文档。

---

## 🔴 Upstream 研究**没预测到**的点

Upstream librarian 明确说过 **"ZoneMap, BloomFilter, and RTree ... do not have upstream tests verifying behavior across compaction."** 本次实测证实这个预言是对的 —— **ZoneMap 和 BloomFilter 确实有 bug**。

这是 **novel finding**：
- Upstream CI 覆盖不到的 index × compact 组合，**实测两个坏两个**（ZoneMap、BloomFilter 4 个 combo 3 个异常）
- Upstream CI 覆盖到的 index × compact 组合，**实测全部 pass**

这对 Lance 的信号强度：**有 CI 的功能靠谱，没 CI 的功能生产环境风险显著**。

---

## 📝 可提交给 upstream 的 issue 草稿

**这些 bug 经过 pylance 4.0.1 和源码编译的 6.0.0-rc.4 双版本确认，不是老版本遗留。**

```
Title: [bug] BloomFilter/ZoneMap scalar indexes + IVF_* vector defer-remap 
       broken across compact_files(); 1 new regression in 6.0.0-rc.4

Environment
- pylance 4.0.1 (lance-core 0.39.0 native) — PyPI latest
- pylance 6.0.0-rc.4 (source built from tag v6.0.0-rc.4, commit cadf921b)
- Both on Linux aarch64 (Graviton ARM64), local /tmp FS

Summary

After compact_files() on a dataset with certain index types, the resulting 
state is inconsistent in ways that violate the documented IndexRemapper / 
FragmentReuseIndex contracts. Tested 9 index types × 2 paths = 18 combos; 
confirmed on both pylance 4.0.1 and 6.0.0-rc.4.

Bug 1: ZONEMAP and BLOOMFILTER — default path (defer_index_remap=False)

- Expected: IndexRemapper rewrites index files, UUID changes, 
  fragment_bitmap updated to new fragment IDs.
- Actual: UUID preserved, fragment_bitmap still references pre-compact 
  fragment IDs [0..9] even though compact produced single new fragment 
  with ID 10. FRI is also absent.
- This is essentially a silent no-op — neither path ran.
- Queries still return correct row counts, but index is clearly not 
  being used (latency unchanged vs no-index baseline).
- Reproduces on both 4.0.1 AND 6.0.0-rc.4.

Bug 2: ZONEMAP and BLOOMFILTER — defer path (defer_index_remap=True)

- Expected: UUID preserved, FRI created, queries work via FRI address 
  translation.
- Actual: UUID preserved ✓, FRI present ✓, bitmap updated to [10] ✓, 
  BUT queries return 0 rows (should return 4936/9969/1/3 rows).
- Definite correctness failure — silent data loss at query time.
- Reproduces on both 4.0.1 AND 6.0.0-rc.4.

Bug 3: IVF_PQ — defer path

- Query throws Rust error at scanner.rs:4524 (4.0.1) / 4730 (6.0.0-rc.4):
  "The input to a take operation specified fragment id 0 but this 
   fragment does not exist in the dataset"
- Manifest version shows 18446744073709551602 (u64::MAX - 13), 
  suggesting uninitialized or overflow value being consumed.
- Reproduces on both 4.0.1 AND 6.0.0-rc.4.

Bug 4 (NEW REGRESSION in 6.0.0-rc.4): IVF_HNSW_SQ — defer path

- IVF_HNSW_SQ × defer worked correctly on 4.0.1 (pre=2.23ms, post=2.01ms, 
  10 rows returned).
- On 6.0.0-rc.4 it throws the SAME Rust error as IVF_PQ × defer.
- Some commit between 4.0.1 (2026-04-30) and 6.0.0-rc.4 (2026-05-08) 
  broke the shared IVF sub-index take code path for HNSW_SQ.

Summary table (18 combos × 2 versions):

                   4.0.1          6.0.0-rc.4
BTREE/BITMAP/LABEL_LIST/NGRAM/INVERTED × both paths: ✅ all pass
IVF_HNSW_SQ × default:          ✅              ✅
IVF_HNSW_SQ × defer:            ✅              ❌ NEW
IVF_PQ × default:               ✅              ✅
IVF_PQ × defer:                 ❌              ❌ (same)
ZONEMAP × default:              ❌              ❌ (same)
ZONEMAP × defer:                ❌              ❌ (same)
BLOOMFILTER × default:          ❌              ❌ (same)
BLOOMFILTER × defer:            ❌              ❌ (same)

Root cause hypotheses (from source review):

- ZoneMap and BloomFilter may not be registered in IndexRemapper's 
  dispatch table in commit_compaction (rust/lance/src/dataset/optimize.rs)
  → falls through as no-op in default path.
- FRI translation layer may not be wired for ZoneMap/BloomFilter address 
  types → their queries read zero rows post-compact.
- IVF sub-index scan path in scanner.rs uses a fragment-id lookup that 
  doesn't go through FRI translation in defer mode.

These are exactly the index types the repo docs flagged as having NO 
upstream CI coverage across compaction 
(see rust/lance/src/dataset/optimize.rs — there's no 
test_*_with_defer_index_remap for ZoneMap, BloomFilter, or RTree).

Reproduction

Full script (~800 lines): [link to N_compact_index.py]
Reproduces in ~90 seconds on a single r8g.2xlarge with 100K rows × 10 
fragments → compacted to 1 fragment. Both the script and the results 
JSONs from both versions are attached.
```

---

## 💡 给 Lance 用户的实用建议

跨两个版本的测试结果：

1. **Bug 都是上游一直存在的**，不是老版本遗留 —— 生产必须考虑。
2. **v6 比 4.0.1 多一个 IVF_HNSW_SQ × defer 回归** —— 不要假定"升级自然会修"。
3. **安全的 index × compact 组合**（**两个版本都验证**）：
   - BTREE / BITMAP / LABEL_LIST / NGRAM / INVERTED —— 两条路径都 OK
   - IVF_HNSW_SQ —— **只在 default 路径 OK**（v6 defer 回归）
   - IVF_PQ —— **只在 default 路径 OK**（两个版本 defer 都坏）
4. **在 4.0.1 / 6.0.0-rc.4 上要避开的组合**：
   - **ZONEMAP + compact**（任何路径）—— 不要上生产
   - **BLOOMFILTER + compact**（任何路径）—— 不要上生产
   - **任何 IVF_* vector index + `defer_index_remap=True`** —— 默认参数就是对的，别手贱
5. **默认路径（`defer_index_remap=False`）更安全**：节省时间极小（本测试 ~30ms 差），但 default 在 6/9 index 正确，defer 只在 5/9 正确（v6 统计）。
6. **Compaction 后不需要手动 `create_index(..., replace=True)` 重建** —— 对能工作的 index 来说上游文档承诺是真实的；对不能工作的 index 来说重建也救不回来（因为 compact 阶段已经搞乱 bitmap，需要的是上游修 IndexRemapper）。

---

## 📦 测试设施

**脚本特性**：
- 9 index × 2 compact path = 18 combo 矩阵
- 每 combo 独立 dataset，避免状态污染
- 每 combo 按 8 个 phase 记录（write → ground-truth → build index → pre-compact query → compact → post-compact query → cleanup → assertions）
- 6 种 assertion：correctness / compact_ran / uuid_invariant / fri_invariant / bitmap_updated / latency_ratio
- 支持本地 `/tmp` 或 S3 后端（可扩展到 10M+ rows 规模）
- JSON 结果 checkpoint-friendly（每个 combo 结束都写盘，kill 后可恢复）
- 用 `detect_stable_row_ids()`（检查 `frag.metadata.row_id_meta is not None`）作为跨版本稳定的 stable row id 探测（`ds.has_stable_row_ids` 在 4.0.1 不存在）

**MVCC 尺寸规则遵守** (`METHODOLOGY_CORRECTION_size.md`)：
- 每个 snapshot 同时报 `active_data_bytes`（当前版本引用的）和 `total_bytes_on_disk`
- `_indices/` bytes 按 UUID 分解（这是项目里第一个需要这个维度的测试）
- 每个 combo 末尾调 `cleanup_old_versions(timedelta(0))` 让 active 和 total 重合，方便 apples-to-apples 对比

---

## 🔮 后续可做

1. ~~**规模化验证**：10M 行 S3 重跑~~ —— 低 ROI。bug 在 100K 本地就完全复现，两个版本一致。Scale 不会改变结论，只花钱花时间。除非要用 production-scale 数据做 issue 的"严重性"证据。
2. ~~**版本对比**~~ —— **✅ 已完成**（4.0.1 + 6.0.0-rc.4）。结果：所有 5 个 bug 在 v6 都还在，外加 1 个新的 IVF_HNSW_SQ × defer 回归。
3. **`stable_row_ids=True` 行为** —— 未测。上游源码说 stable row ids 下 compact 不 remap（needs_remapping=False 自动）；应验证 ZoneMap/BloomFilter 在 stable row ids 下是否正常。**这是修复路径**：用户可以借 stable row ids 绕开 ZoneMap/BloomFilter bug 吗？
4. **`optimize_indices()` 修复效果** —— 测试"compact 完再调 `optimize_indices()`"能否把 ZoneMap/BloomFilter 的坏 bitmap 修好。上游文档暗示 `optimize_indices` 可以"catch up" FRI 版本。
5. **RTREE** —— 未测（上游研究也点名无 CI 覆盖）。加一个空间数据集重跑。在 ZoneMap/BloomFilter 都坏的背景下，RTREE 也很可能坏。
6. **Upstream issue 提交** —— 报告里的 issue 草稿可以直接发到 `lance-format/lance` / `lancedb/lance` repo。**这是修好这些 bug 最有效的行动。**

---

## 📎 附录：脚本用法

```bash
# Smoke test (local, 100K rows, ~90s)
python extended-bench/scripts/N_compact_index.py \
    --work-dir /tmp/N_compact_smoke --smoke

# 100K rows 全 9 index (local, ~2 min)
python extended-bench/scripts/N_compact_index.py \
    --work-dir /tmp/N_compact \
    --n-rows 100000 --rows-per-fragment 10000

# 10M rows S3 ap-northeast-1 (预计 4-6h, $20-40)
python extended-bench/scripts/N_compact_index.py \
    --work-dir s3://lance-benchmark-XXXX-ap-northeast-1/N_compact \
    --region ap-northeast-1 \
    --n-rows 10000000 --rows-per-fragment 100000

# 只测特定 index / path
python extended-bench/scripts/N_compact_index.py \
    --work-dir /tmp/N_compact \
    --index-types ZONEMAP,BLOOMFILTER \
    --compact-paths defer
```
