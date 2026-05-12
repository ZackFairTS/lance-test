# Lance Table 组合字段查询方案设计

> **研究目标**：针对 `WHERE video_id = X AND frame_id = Y` 这类多字段组合查询，在 Lance Table 上如何最高效实现？是否应通过"拼接字段成单列"的方式绕过 Lance 缺乏原生联合索引的限制？
>
> **结论先行**：**不应该拼接。** Lance 维护者明确推荐"每列各建独立索引 + 查询引擎 AND 相交"，本项目实测 `O_composite_key.py` 证实该方案在 10M 行数据上达到 1.80 ms p50，优于拼接方案的 1.65 ms p50（仅快 8%）且能保留所有单列/范围查询能力。拼接方案在 ClickHouse/Delta/Iceberg 生态均被视作反模式。

---

## 1. 背景

生产工作负载需要组合字段查询，典型如：

```sql
SELECT * FROM frames WHERE video_id = 12345 AND frame_id = 678;
```

在关系型数据库（PostgreSQL、MySQL）和部分列式数据库（ClickHouse）中，这类查询可以通过**联合索引（composite index）** 或 **复合主键（compound primary key）** 高效实现。本文档回答三个问题：

1. Lance 是否支持原生联合索引？
2. 如果不支持，"拼接字段成单列"是否是合理的工程替代？
3. 标准工程答案是什么？

本文档采用三层证据链：**源码 → 官方 issue/PR → 实测数据**。

---

## 2. 事实基础：Lance 目前不支持原生联合索引

### 2.1 源码级铁证

Lance 在 Rust 层显式拒绝多列索引。

**文件**：[`rust/lance/src/index/create.rs` 第 139–145 行](https://github.com/lance-format/lance/blob/main/rust/lance/src/index/create.rs#L139-L146)

```rust
#[instrument(skip_all)]
pub async fn execute_uncommitted(&mut self) -> Result<IndexMetadata> {
    if self.columns.len() != 1 {
        return Err(Error::index(
            "Only support building index on 1 column at the moment".to_string(),
        ));
    }
    let column_input = &self.columns[0];
```

这条校验在**所有标量索引类型**执行前生效：BTREE / BITMAP / LABEL_LIST / NGRAM / INVERTED / ZONEMAP / BLOOMFILTER / RTREE。`git blame` 显示该校验 2026-03-03 仍被维护者修改以完善错误消息，不是遗留代码而是当前设计。

Python 绑定层做了二次校验：

**文件**：[`python/python/lance/dataset.py` 第 3030–3036 行](https://github.com/lance-format/lance/blob/main/python/python/lance/dataset.py#L3030)

```python
if isinstance(column, str):
    column = [column]

if len(column) > 1:
    raise NotImplementedError(
        "Scalar indices currently only support a single column"
    )
```

`create_scalar_index()` 的 `column` 参数签名直接就是 `str`，API 表面就不提供多列形式。

### 2.2 TypeScript 客户端的官方原话

**文件**：[`lancedb.github.io/lancedb/js/interfaces/IndexConfig/`](https://lancedb.github.io/lancedb/js/interfaces/IndexConfig/)

> `columns: string[]` — "The columns in the index. **Currently this is always an array of size 1. In the future there may be more columns to represent composite indices.**"

这是全部官方文档中**最直白的声明**：schema 保留了 list 形式是为将来支持 composite indices 留口子，**但今天始终是 size 1**。

### 2.3 "标量索引"并非同质类别 —— 按谓词形态三分

Lance 内核的 [`IndexType::is_scalar()`](https://github.com/lance-format/lance/blob/main/rust/lance-index/src/lib.rs#L226) 把 7 种索引都归为"标量族"，但它们**加速的谓词形态完全不同**。对本文档讨论的 `WHERE video_id = X AND frame_id = Y` 这类组合等值查询，真正相关的只有其中 4 种。按**谓词形态**三分如下。

#### 2.3.1 等值/范围索引 —— 本文档的核心候选

对 `col = X` / `col IN (...)` / `col BETWEEN a AND b` 等经典 SQL 谓词有效。这组索引是 §4-§5 实测的主角。

| 索引 | 加速的谓词 | 精确性 | 适用 |
|---|---|---|---|
| `BTREE` | `=` `!=` `<` `>` `<=` `>=` `BETWEEN` `IN` `IS NULL` `LIKE 'prefix%'` | 精确 | 高基数数值/字符串/时间 |
| `BITMAP` | `=` `!=` `IN` `IS NULL` | 精确 | 低基数（< ~1,000 distinct） |
| `BLOOMFILTER` | `=` `!=` `IN` `IS NULL` `IS <bool>` | **不精确**（有假阳性，需回表 recheck）| 等值过滤，索引体积小 |
| `ZONEMAP` | 范围 / 比较 / `LIKE 'prefix%'` | 精确（剪枝层面） | 近似有序列 |

**这 4 种都通过 Lance 查询引擎的 [`ScalarIndexExpr::And`](https://github.com/lance-format/lance/blob/main/rust/lance-index/src/scalar/expression.rs#L1210) 自动 AND 相交**，是 §4 "维护者推荐方案" 的实现基础。§2.3.2 / §2.3.3 的索引也能参与同一条相交路径，汇总对照见 §2.3.4。

#### 2.3.2 函数谓词索引 —— 参与 AND 相交，但仅对特定函数

这组索引也走同一条 `ScalarIndexExpr::And` 相交路径，能与上面 4 种 AND 组合，**但只响应特定的标量函数调用谓词**——不响应 `col = X`。

| 索引 | 适用列类型 | 加速的**唯一**谓词形态 | 精确性 | 源码依据 |
|---|---|---|---|---|
| `LABEL_LIST` | `List<T>`（多标签/数组列，如 `tags: List<String>`）| `array_has(col, v)` / `array_has_all(col, vs)` / `array_has_any(col, vs)` | 精确 | [`LabelListQueryParser` (`#L678-L790` @ `443f2da`)](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L678-L790) 的 `visit_between/in_list/is_bool/is_null/comparison` **全部返回 `None`**，仅 `visit_scalar_function` 对 `array_has` / `array_has_all` / `array_has_any` 这三个 UDF 返回 `Some`（L723–L789） |
| `NGRAM` | 字符串 | `contains(col, 'substr')` / `LIKE '%x%'` | 不精确（需回表） | 同上模式 |

**正确用法示例**：视频帧表如果有 `scene_tags: List<String>` 列（如 `["outdoor", "daytime", "crowd"]`），则 `WHERE array_has_all(scene_tags, ['outdoor', 'crowd']) AND video_id = 123` 这类混合查询里，LABEL_LIST 加速前半段、BTREE 加速后半段，两者通过 `ScalarIndexExpr::And` 相交。

**不要把它当等值索引用**：LABEL_LIST 只响应 `array_has*` 函数调用，不响应 `= / IN / BETWEEN`。对主键/高基数标识列（`video_id`、`frame_id`、`user_id` 等）请用 §2.3.1 的 `BTREE`；对低基数枚举列（`status`、`region`）用 `BITMAP`。

#### 2.3.3 全文索引 —— 名义上是标量，实际语义独立

| 索引 | 加速的谓词 | 精确性 | 备注 |
|---|---|---|---|
| `INVERTED` / `FTS` | **仅** `contains_tokens(col, 'word')` / `MatchQuery` / `PhraseQuery` | 取决于查询类型 | 不响应 `=`/`!=`/`IN`/`BETWEEN`/任何比较运算 |

**源码依据**：[`FtsQueryParser` (`expression.rs#L873-L943` @ `443f2da`)](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L873-L943) 的 `visit_between` / `visit_in_list` / `visit_is_bool` / `visit_is_null` / `visit_comparison` **全部返回 `None`**（L890–L918），仅 `visit_scalar_function` 在 `func.name() == "contains_tokens"` 这一个分支（L932）返回 `Some(TokenQuery::TokensContains(...))`，任何其它 UDF 落入最后一行 `None`（L942）。所有 `visit_*` 签名由父 trait [`ScalarQueryParser` (`#L72-L170`)](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L72-L170) 定义。

**关键事实**：虽然 Lance 内核把 `INVERTED` 分到 `is_scalar() == true`，但——

- LanceDB 包装层的类型别名 **`ScalarIndexType = Literal["BTREE", "BITMAP", "LABEL_LIST"]`** ([`lancedb/python/python/lancedb/types.py#L30`](https://github.com/lancedb/lancedb/blob/main/python/python/lancedb/types.py#L30)) **排除了 FTS**。
- LanceDB 暴露独立的 `create_fts_index()` 方法，不走 `create_scalar_index()`。
- LanceDB **不暴露** `NGRAM` / `ZONEMAP` / `BLOOMFILTER` / `RTREE` 于公开 Python API —— 这些仅 Lance-core 可用。

**也就是说：Lance 官方 API 和 LanceDB 公开 API 对"标量索引"这个词的定义本身就不一致。** 本文档在谈组合字段过滤时，如无特别说明，"标量索引"指 §2.3.1 的四种；`INVERTED`/`FTS` 可以与这些索引在同一查询里 AND 相交，但**只能承担 `contains_tokens(col, 'word')` 那一侧的谓词**，不能替代 `col = X`。

#### 2.3.4 快速对照：对"组合字段等值查询"有用吗？

| 索引 | 归类（Lance 内核）| 归类（LanceDB 公开 API）| 能加速 `video_id = X`？ | 能与其它标量索引 AND 相交？ | AND 时该索引侧的谓词约束 |
|---|---|---|---|---|---|
| `BTREE` | scalar | scalar | ✅ | ✅ | — |
| `BITMAP` | scalar | scalar | ✅ | ✅ | — |
| `BLOOMFILTER` | scalar | 不暴露 | ✅（带 recheck） | ✅ | 仅等值/IN/IsNull/IsBool（不支持范围） |
| `ZONEMAP` | scalar | 不暴露 | ⚠️（仅对有序列有效） | ✅ | — |
| `LABEL_LIST` | scalar | scalar | ❌ | ✅ | 仅 `array_has` / `array_has_all` / `array_has_any` |
| `NGRAM` | scalar | 不暴露 | ❌ | ✅ | 仅 `contains(col, 'substr')` / `LIKE '%x%'` |
| `INVERTED` / `FTS` | scalar | **独立 FTS 类别** | ❌ | ✅ | 仅 `contains_tokens` / `MatchQuery` / `PhraseQuery` |

**AND 相交机制是对称的**：第 5 列所有行都是 ✅ 不是笔误 —— Lance 的 [`ScalarIndexExpr::And(Box<Self>, Box<Self>)` (`expression.rs#L1257-L1267` @ `443f2da`)](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L1257-L1267) 是递归组合器，左右子树是任意 `ScalarIndexExpr`，不分索引类型。实际运行时用[`BitAnd::bitand` 对 row-mask 做 `&` 操作（L1328-L1350）](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L1328-L1350)，对 "exact×exact / exact×inexact / inexact×inexact" 三种精确度组合都有对应的 match 分支。测试证据：[`test_null_handling` (`test_scalar_index.py#L2034-L2063`)](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/python/python/tests/test_scalar_index.py#L2034-L2063) 里 `filter="x > 0 AND (y != 'a')"` 的两列分别是 BITMAP 和 BTREE —— 证明跨类型 AND 是官方测试覆盖的路径。

**当 AND 含"不精确"索引时自动 recheck**：BLOOMFILTER / ZONEMAP / NGRAM 三类索引对应的 `needs_recheck = true`（参数在每个索引自己的 `new_query_parser` 构造时传入）。[`ScalarIndexExpr::needs_recheck() (#L1534-L1541)`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L1534-L1541) 对 `And`/`Or` 子树做 `||` 递归传播，只要**任一叶子**不精确，整棵 AND 树就被 flagged。[`Scanner::scalar_indexed_scan` (`scanner.rs#L3867-L3919`)](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance/src/dataset/scanner.rs#L3867-L3919) 看到这个 flag 后，会在 take 阶段把原始 filter 列也物化，然后把**完整原 filter 表达式**作为 `post_take_filter` 重新跑一遍 —— 所以 `BLOOMFILTER(col_a) AND NGRAM(col_b)` 虽然都不精确，Lance 会自动 recheck，**最终结果依然正确**。

---

**本文档 §4–§5 以 BTREE 为主角**只是因为 benchmark 方案（V1 双 BTREE / V2 BITMAP+BTREE / V4 排序+BTREE 前缀）都围绕 BTREE 展开；机制上并没有"必须有一侧是 BTREE"的约束。

---

## 3. 功能缺口被官方承认，但进展停滞

### 3.1 跟踪 issue 状态

**[Issue #3125 "Composite scalar indices"](https://github.com/lance-format/lance/issues/3125)** — 开启

| 字段 | 值 |
|---|---|
| 开启者 | **@westonpace**（Lance 技术负责人、LanceDB 联合创始人） |
| 开启日期 | 2024-11-14 |
| 正文全文 | *"We should support creating composite scalar indices (i.e. multiple columns)"* |
| 关联 PR / 分支 | **无** |
| Assignee / Milestone | **均为空** |

时间线显示该 issue **18 个月零实质进展**：

| 日期 | 事件 |
|---|---|
| 2024-11-14 | 核心维护者 @westonpace 开启 |
| 2025-07-03 | 用户 @pavanramkumar 询问 roadmap，**维护者无回复** |
| 2025-11-05 | 机器人标 `Stale` |
| 2025-12-06 | 机器人自动关闭 |
| 2025-12-07 | 另一位维护者 @Xuanwo 重新打开 |
| 至今 | 开启，无人认领，**未列入 2025 年 Roadmap**（[Issue #3730](https://github.com/lance-format/lance/issues/3730) 明确列出的 5 项均不涉及联合索引）|

### 3.2 社区 PR 被撤回

**[PR #5480 "feat: add compound (multi-column) scalar index"](https://github.com/lance-format/lance/pull/5480)** — **Closed, not merged**

- 作者：@tomsanbear（[Catalyzed.ai](https://catalyzed.ai) 外部贡献者）
- 规模：**~11,000 行代码**，实现完整：leftmost-prefix B-tree、2–8 列、支持前缀/前缀+range/IN-list/IS NULL
- 时间：2025-12-15 提交 → **2026-01-15 作者自行关闭**
- 撤回原因（维护者 @wjones127 原话）：

> *"I think this would be the first index that covers multiple columns, and I think it needs careful design. ... we should first outline the API changes needed to support multi-column indices (how are queries routed to them and run, for example)."*

维护者 @westonpace 列出了必须先完成的前置重构：

1. 表格式支持多个索引
2. Compound sargable query + parser
3. Scanner 支持多索引
4. Compound btree index search + 简单训练
5. 分布式训练

**这些都没发生。** 作者接受意见后撤回。

### 3.3 另一个被内部撤回的相关 PR

**[PR #4639 "feat: support to create FTS index on multiple columns"](https://github.com/lance-format/lance/pull/4639)** — **Closed, not merged**

- 作者：@BubbleCal（LanceDB **内部**维护者）
- 撤回原因（作者原话）：

> *"after discussed, we think this API may confuse users because we may use the same API to create compound index in the future, so close it."*

这进一步确认：维护团队希望等 "compound index" 的总体 API 设计出来再统一发布，而那个设计工作尚未启动。

---

## 4. 维护者明确推荐的替代方案

### 4.1 原话

**@wjones127（核心维护者）在 PR #5480 评论中**：

> *"Our query engine can combine the results of multiple index lookups, so I'd be curious how that compared to a compound index."*

**@westonpace（技术负责人）在同一 PR**：

> *"I might wonder if a bitmap index on tenant plus a btree index on the range column would perform similarly to this compound case."*

即：**每列各建独立的单列索引，查询引擎自动 AND 相交。**

### 4.2 源码中的 AND 相交实现

Lance 查询引擎把一个 AND 过滤条件拆成**两份**送入 scan：一份是走索引的 `index_query`，另一份是走内存过滤的 `refine_expr`。文件 [`rust/lance-index/src/scalar/expression.rs`](https://github.com/lance-format/lance/blob/main/rust/lance-index/src/scalar/expression.rs) 的 module-level 注释给了一个两列过滤场景下的直观描述：

```rust
/// If the user asked for "type = 'dog' && z = 3" and we had a scalar index on the
/// "type" column then we could convert this to an indexed scan for "type='dog'"
/// followed by an in-memory filter for z=3.
```

即：`type` 列有索引 → 索引查 `type='dog'` 得到候选行号；`z` 列没索引 → 把候选行**物化**回来，在内存里对每行算 `z = 3`。

这套切分逻辑在 planner 里落地为 [`IndexedExpression`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L1135-L1161) 结构，其中 `scalar_query: Option<ScalarIndexExpr>` 是能下推到索引的那部分，`refine_expr: Option<Expr>` 是必须在内存里重算的那部分。最终被 scanner 消费为 [`FilterPlan`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance/src/dataset/scanner.rs#L3867-L3919)。核心分派在 [`visit_and` (`#L1858-L1882`)](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L1858-L1882)：

```rust
Ok(match (left, right) {
    (Some(left), Some(right)) => Some(left.and(right)),          // 两侧都有索引 → 完全由索引满足
    (Some(left), None)        => Some(left.refine((*expr.right).clone())),  // 只一侧有 → 部分
    (None, Some(right))       => Some(right.refine((*expr.left).clone())),  // 同上
    (None, None)              => None,                            // 两侧都没 → 放弃索引
})
```

用这个结构回看 [PR #1543](https://github.com/lance-format/lance/pull/1543) 引入该能力时给的两个例子（**前提：`x` / `y` 两列都建了标量索引，`z` 没有**）：

| 过滤条件 | planner 产物 | 含义 |
|---|---|---|
| `x = 7 AND y < 20` | `index_query = And(Query(x=7), Query(y<20))`，`refine_expr = None` | **完全由索引满足**：两侧都下推到索引，相交后直接得到最终行号 |
| `x = 7 AND z > 30` | `index_query = Query(x=7)`，`refine_expr = Some(z > 30)` | **部分由索引满足**：只有 `x=7` 走索引，缩小范围后的候选行必须物化回内存，在 CPU 上过 `z > 30` —— 这就是上面 Rust 注释里 *"an indexed scan ... followed by an in-memory filter"* 的实际形态 |

**这就是本文档反复强调"每个 filter 列都应该建索引"的底层原因**：部分由索引满足不仅 CPU 多跑一步，更决定了**实际回表的 I/O 量** —— scanner 按**有索引那一侧**的选择率 take 行，没索引那一侧的选择率再高也省不下 I/O。例如 `x=7` 覆盖 10% 的行、最终 `x=7 AND z>30` 只剩 1%，"部分"方案要读 10% 行的全量数据；若 `z` 也有索引则只读 1%，**I/O 差 10 倍**。

### 4.3 官方测试用例

**文件**：[`python/python/tests/test_scalar_index.py` `test_use_multi_index`](https://github.com/lance-format/lance/blob/119f87b3/python/python/tests/test_scalar_index.py)

```python
def test_use_multi_index(tmp_path):
    dataset = lance.write_dataset(pa.table({"ints": range(1024)}), tmp_path, ...)
    dataset.create_scalar_index("ints", index_type="BTREE")
    dataset.create_scalar_index("ints", index_type="BITMAP", name="ints_bitmap_idx")
    # Multiple indices can be applied here. One of them will be chosen
```

### 4.4 LanceDB Cloud FAQ 的用户级建议

来源：[`lancedb.com/docs/faq/faq-cloud`](https://lancedb.com/docs/faq/faq-cloud/)

> *"It is strongly recommended to create scalar indices on **the filter columns**. Scalar indices will reduce the amount of data that needs to be scanned and thus speed up the filter."*

注意 "filter columns" 是**复数**——官方期望是"每个过滤列各一个索引"。

---

## 5. 实测对比（`O_composite_key.py`）

### 5.1 测试环境

- **数据集**：10,000,000 行 × 10,000 distinct video_id × 平均 1,000 frame/video
- **机器**：r8g.2xlarge（Graviton ARM64，8 vCPU / 64 GiB），本地 NVMe（单机、无分布式）
- **软件**：pylance 4.0.1（lance-core 0.39.0）+ pyarrow 20.0.0
- **Schema**：`video_id int64, frame_id int64, vf_key string, payload string`
- **方法**：每查询 warmup=3, rounds=10，中位数用于排序

### 5.2 五种方案：设计矩阵

本实验是**单变量对照**：全部 5 个方案在同一份底层数据（10M 行相同分布）、同一套查询、同一台硬件上跑，**唯一变量是"索引策略 + 写入布局"**。任意两个方案的性能差都能单独归因到这个变量。脚本实现见 [`scripts/O_composite_key.py`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/O_composite_key.py)（docstring L1–L28 列出了每个变体的原始设计意图）。

每个变体对应一个**可证伪的假设**（"这个方案到底行不行？代价多大？"），而不是"随便挑 5 种组合来跑"：

| 代号 | 研究假设 | 索引策略 | 权威出处 |
|---|---|---|---|
| **V0** | *"不建索引、靠列式全扫，延迟能不能接受？"* —— 给所有方案提供绝对性能基线 | 无索引 | — |
| **V1** | *"维护者明确推荐的'每列独立索引 + 引擎 AND 相交'方案，实测效果如何？"* —— 直接验证 §4 的核心结论 | BTREE(video_id) + BTREE(frame_id) | ⭐ @wjones127 原话（[PR #5480 评论](https://github.com/lance-format/lance/pull/5480)） |
| **V2** | *"@westonpace 提到'低基数列用 BITMAP' —— 在本数据集 10k 的 video_id 基数下有没有优势？"* —— 测 BITMAP 的基数临界点 | BITMAP(video_id) + BTREE(frame_id) | ⭐ @westonpace 原话（[PR #5480 评论](https://github.com/lance-format/lance/pull/5480)） |
| **V3** | *"把两列拼成一列再建 BTREE —— 读者最容易想到的'民间方案'，真能绕开 Lance 无联合索引的限制吗？"* —— 钉死反模式 | 拼接列 `vf_key = "video\|frame"` + BTREE(vf_key) | ❌ 无权威推荐（见 §6 反模式论证） |
| **V4** | *"借鉴 ClickHouse leftmost-prefix —— 写入时按 (video_id, frame_id) 物理排序，只对 video_id 建 BTREE，利用 row-order locality，行不行？"* —— 测物理布局这个正交维度 | 写入按 (video_id, frame_id) 排序 + BTREE(video_id)，**`frame_id` 不建任何索引**（见下方"V4 工作原理"） | 类 ClickHouse leftmost-prefix（非 Lance 原生推荐） |

#### V4 工作原理（为什么"排序 + 单列 BTREE"能模拟联合索引）

V4 名字里的"前缀"容易让人以为是"联合前缀索引"—— 但 Lance 没这种东西。V4 其实是**两件独立的事叠在一起**：

1. **物理排序**：写入时对表整体按 `(video_id, frame_id)` 排序（脚本 [`O_composite_key.py#L69-L74`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/O_composite_key.py) 的 `sorted_copy()`），再 `lance.write_dataset(...)`。磁盘上的实际效果是：**同一个 `video_id` 的所有行物理连续**，且这些连续段内部又按 `frame_id` 升序。
2. **仅对主键列建 BTREE**：只 `create_scalar_index("video_id", "BTREE")`，`frame_id` **不建任何索引**。

查询 `WHERE video_id = X AND frame_id BETWEEN a AND b` 时真正发生的事：

- BTREE 查 `video_id = X` → 得到候选行号集
- 因为物理排序，这个行号集对应磁盘上**一段连续区间**（而非散落的行号）
- Lance 读这段区间走**顺序 I/O**，吞吐率远高于 V1 的随机 take
- `frame_id ∈ [a, b]` 走 `refine_expr`（§4.2 的"部分由索引满足"），但因为区间内 `frame_id` 本身有序，DataFusion 只扫区间的头尾一小段就停

**对比 V1**：V1 的 `BTREE(video_id) + BTREE(frame_id)` 做 AND 相交能精确拿到命中行号集，但这些行号**在磁盘上是散的**（V1 没排序），必须随机 take。对 10M 行的点查，随机 take 的 I/O overhead 反而比 V4 的顺序扫慢。

**V4 的致命盲区（Q_frame 崩溃的源头）**：`frame_id` 在每个 video 段内**物理上有序**，但 Lance 2.1 **不会从 row-order 自动构建 zonemap** —— 要 zonemap 剪枝必须显式 `create_scalar_index(col, "ZONEMAP")`。V4 没建，所以 `WHERE frame_id = Y` 对 Lance 来说等价于"这列没有任何元数据"，只能**全扫**。这就是 §5.3 里 V4 的 Q_frame = 56.56 ms（与 V0 无索引的 60.13 ms 基本一致）的根本原因，也是 V4 变体存在的**独家教学价值** —— 它钉死了 Lance 2.1 在物理布局优化上的这个非显而易见盲区。

#### V4 的运维陷阱：compaction / update 会不会打乱排序？

V4 的所有优势都建立在"磁盘上行物理有序"之上 —— 一旦排序被打乱，Q_vid / Q_range 就退化到 V1 水平（甚至更差，因为 V4 没有 frame_id 索引）。实际运维中有三种常见操作会触发这个风险，下面按源码结论给出判定（证据均在 [`lance-format/lance`](https://github.com/lance-format/lance) 的 `rust/lance/src/dataset/optimize.rs` 和 `rust/lance/src/dataset/write/update.rs`）：

| 操作 | V4 排序是否保留？ | 源码依据 |
|---|---|---|
| `optimize.compact_files()` | ✅ **保留**（前提见下） | [`optimize.rs#L732`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance/src/dataset/optimize.rs#L732) 文档字符串明示 *"Compacts the files in the dataset without reordering them... This method tries to preserve the insertion order of rows in the dataset."*；[`#L904`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance/src/dataset/optimize.rs#L904) 强制 `scan_in_order(true)` 读源 fragment；bins 按**相邻 fragment id** 连续分组后**拼接输出**，不做 merge-sort 也不 reshuffle |
| `delete()` + compact | ✅ **保留**（tombstone 是软删除） | 删除只写 deletion bitmap；compact 时 `scan_in_order` 跳过 tombstoned 行但保留剩余行的相对顺序 |
| `update()` | 🔴 **破坏**（无法恢复） | [`update.rs#L299`](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance/src/dataset/write/update.rs#L299) 显示 update = "tombstone 原位 + 把修改后的行作为**新 fragment 追加到尾部**"。新 fragment 的 id 最大，compaction 的 bin 只组合**相邻** id 的 fragment，**无法把它们归位到有序的中段** |
| 非单调 append（两批数据的 video_id 区间有重叠） | 🔴 **破坏** | compact 按 fragment id 顺序拼接而非按键归并，两批交叉键的数据会被穿插 |

Lance 的 `CompactionOptions` **没有** `sort_by` / `order_by` / `clustering_key` 任何参数（pylance 4.0.1 的 `ds.optimize.compact_files(...)` 签名同样没有），也就是说 Lance **不会替你把乱序数据重新排序**；compaction 的 "preserve insertion order" 只是忠实保留你写入时的顺序，不会做任何二次排序。schema 层面虽然有一个 `unenforced_clustering_key` metadata 字段，但**没有任何写入、扫描、compaction 代码读取它** —— 顾名思义是"unenforced"。

#### V4 的生产落地规则（若决定采用此方案）

1. **每批写入前必须自己先排序**（`pyarrow.compute.sort_indices` + `take`，见 [`O_composite_key.py#L69-L74`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/O_composite_key.py)）—— Lance `WriteParams` 没有 sort 选项
2. **批与批之间 `video_id` 区间必须单调**：batch N 的 max ≤ batch N+1 的 min，否则 compaction 会按 fragment id 拼接穿插
3. **禁用 `Dataset.update()`**：它会把修改的行作为尾部新 fragment 追加，全局有序性**一去不返**；需要改数据请改用"`delete(predicate)` + 一次把受影响邻域整段按顺序 append"
4. **`delete()` + `compact_files()` 是安全的**，可以放心在 V4 上跑
5. **非单调 append 发生了？只能整表 overwrite** —— `lance.write_dataset(ds.to_table().sort_by([...]), path, mode="overwrite")`，然后重建 BTREE

这套规则的严苛程度本身就是 V4 的实际成本：**V4 在 10M 行实测上 Q_range / Q_vid 比 V1 快 3–6 倍，但写入路径的约束比 V1（无任何要求）重得多**。决定选 V4 前要先评估业务的写入模式是否天然单调（例如按时间顺序到达的监控帧、视频编码流水线的帧输出）—— 如果是，V4 的优势可以稳定持有；如果不是，V4 的优势会在第一次乱序写入后消失，且只能靠 overwrite 恢复。

#### 四种查询负载的设计动机

4 种 query **不是** "随便选 4 个" —— 是刻意设计成一个 **矩阵**，每一列都针对某一类方案的潜在弱点：

| 查询 | 谓词 | 设计动机（要把哪个方案逼到弱点？） |
|---|---|---|
| **Q_point** | `video=X ∧ frame=Y` | 文档核心问题本身 —— 最窄的复合点查。几乎所有方案都应表现好，作为"baseline sanity" |
| **Q_range** | `video=X ∧ frame∈[100,199]` | **V3 的陷阱题**：拼接列后字符串字典序 ≠ 数值序，范围谓词直接破产。也考察 V4 在前缀 + 后续 range locality 上的优势 |
| **Q_vid** | `video=X` | **V3 的第二道陷阱**：拼接列无法对"某视频所有帧"做前缀检索。同时考察 V4 的排序布局能否带来 row-order locality 加速 |
| **Q_frame** | `frame=Y` | **V3 和 V4 的致命陷阱**：拼接后的第 2 段 / 排序后的非主键列，是否仍能被索引加速？这一列会暴露出**Lance 2.1 不从 row-order 自动构建 zonemap** 这个非显而易见的限制 |

所以 §5.3 的结果表不是看"哪个数字最小"，而是看**反对角线上的崩溃格子**：V3 在 Q_range / Q_vid / Q_frame 三格全 ❌，V4 在 Q_frame 一格崩溃（56.56 ms，**退化到与 V0 无索引相当**）。V1 / V2 的特点是**没有任何格子崩溃** —— 这就是它们被称作"全能型"的实测定义。

### 5.3 关键数据（中位数 ms）

下表每一行是一个方案、每一列是一个 query。**关注点不是"哪个数字最小"，而是反对角线 —— 方案在哪一格崩溃**：V3 的三个 ❌（Q_range / Q_vid / Q_frame）和 V4 的 56.56 ms（Q_frame 退化到全扫）就是钉住 §5.4 两条核心结论的实测证据。


| 变体 | 建索引耗时 | 存储 | **Q_point**<br>`video=X ∧ frame=Y` | Q_range<br>`video=X ∧ frame∈[100,199]` | Q_vid<br>`video=X` | Q_frame<br>`frame=Y` |
|---|---:|---:|---:|---:|---:|---:|
| V0 无索引 | 0 s | 241.5 MB | 36.25 | 37.86 | 41.17 | 60.13 |
| **V1 双 BTREE** ⭐ | 1.8 s | 331.8 MB | **1.80** | 8.33 | 10.04 | **21.79** |
| V2 BITMAP+BTREE | 3.4 s | 320.2 MB | 1.79 | 8.33 | 9.99 | 23.89 |
| V3 拼接列 BTREE | 2.7 s | 366.1 MB | **1.65** 🏆 | ❌ | ❌ | ❌ |
| **V4 排序+前缀 BTREE** ⭐ | 0.6 s | 264.7 MB | 2.02 | **2.15** 🏆 | **1.72** 🏆 | 56.56 |

### 5.4 数据解读

**1. V3 拼接列仅在 Q_point 快 0.15 ms（8%），代价是失去所有其他查询能力**

- Q_range 不可行：拼接后字符串字典序 ≠ 数值序（`"100" < "9" < "99"`），范围谓词意义完全改变
- Q_vid 不可行：想查"某视频所有帧"必须全扫
- Q_frame 不可行：想按 frame_id 单列过滤必须全扫
- 额外存储成本：366 MB vs 332 MB（**多 10%**）
- 索引构建成本：2.65 s vs 1.79 s

**2. V1 双 BTREE 是全能型冠军**

- Q_point 相对无索引提升 **20×**（36.25 → 1.80 ms）
- Q_frame 提升 **2.8×**（60.13 → 21.79 ms）
- 支持所有四种查询形态
- 索引开销最低（1.8 s 构建）
- **与维护者明确推荐路径一致**

**3. V4 排序+前缀 BTREE 在"某视频内"类查询上碾压**

- Q_range **快于 V1 的 3.9×**（8.33 → 2.15 ms）
- Q_vid **快于 V1 的 5.8×**（10.04 → 1.72 ms）
- Q_point 与 V1 基本持平（2.02 ms）
- 代价：Q_frame **退化到与无索引相当**（56.56 ms）——因为 Lance 2.1 **不会**从 row-order 自动构建 zonemap，排序对单列 `frame_id` 查询无剪枝效果

**4. V2 BITMAP 在 10k video_id 基数下与 V1 BTREE 几乎相同**

当 video_id 基数更低（< 1000）时 BITMAP 优势才会显现，本数据集基数偏高未体现。

### 5.5 实测结果与上游 PR #5480 的交叉验证

PR #5480 作者在被维护者质疑"为什么不用双独立索引"后，**添加了 "Dual BTree" 基线的对照 benchmark**，多 fragment 真实生产场景结果：

| 场景 | Tenant-only BTree | **Dual BTree** | Compound (PR #5480) |
|---|---:|---:|---:|
| Tenant + 窄范围点查 ← 对应本文 Q_point | 2.08 ms | **472 µs** 🏆 | 693 µs |
| Tenant + 中等范围 | 2.31 ms | 2.77 ms | **2.07 ms** 🏆 |
| Tenant + 宽范围 | 1.89 ms | 767 µs | 818 µs |

**PR #5480 作者自己的 benchmark 确认：对窄选择率的点查场景，"Dual BTree"（即本文 V1 方案）比所谓的 compound 索引还要更快。** 这是作者随后决定撤回 PR 的依据之一。

---

## 6. 为什么不能拼接字段（反模式警告）

拼接字段成单列在 OLTP 和 OLAP 领域均被视为反模式。权威论据：

### 6.1 标准 SQL 性能文献

**[Use The Index Luke — concatenated keys](https://use-the-index-luke.com/sql/where-clause/the-equals-operator/concatenated-keys)**：

明确反对拼接字段，建议使用原生复合索引并谨慎选择列顺序；数据结构本身就支持前缀查询。

**Stack Overflow 热门讨论 [*Database Modeling: avoid using primary keys based on multiple columns merging them*](https://stackoverflow.com/questions/31501853)**：

> *"concatenating fields makes things far less understandable… it violates even the first normal form of database design! Either live with the fact your PK is made up from multiple columns, or… introduce a single INT IDENTITY as a surrogate key — DO NOT merge columns!"*

### 6.2 列式格式生态一致反对

| 系统 | 推荐做法 | 是否建议拼接？ |
|---|---|---|
| **ClickHouse** | `ORDER BY (col_a, col_b)` 稀疏主索引 | **明确反对** |
| **Databricks Delta** | **Liquid Clustering**（取代 Z-order），最多 4 列，直接在原列上聚类 | 明确反对 |
| **Apache Iceberg** | Hidden partitioning + `bucket(N, col)` transforms | 不建议 |
| **Apache Parquet** | 多列 sort order + Page Index | 不建议 |
| **TileDB** | 原生多维坐标 + Hilbert / R-Tree | 模型即多维 |

相关权威资料：

- ClickHouse 稀疏主索引指南：[`clickhouse.com/docs/en/guides/improving-query-performance/sparse-primary-indexes/sparse-primary-indexes-multiple/`](https://clickhouse.com/docs/en/guides/improving-query-performance/sparse-primary-indexes/sparse-primary-indexes-multiple/)
- Databricks Liquid Clustering：[`docs.databricks.com/en/delta/clustering/index.html`](https://docs.databricks.com/en/delta/clustering/index.html)
- Iceberg Partitioning：[`iceberg.apache.org/spec/#partition-transforms`](https://iceberg.apache.org/spec/#partition-transforms)

### 6.3 拼接列的具体技术缺陷

1. **破坏类型信息**：`int + int → string`，min/max 统计作废，zonemap 剪枝失效
2. **破坏优化器下推**：DataFusion 无法将 `concat(a, '-', b) = 'x-y'` 重写回 `a = x AND b = y`
3. **丢失范围能力**：`BETWEEN` 无法工作（字典序 ≠ 数值序）
4. **丢失单列过滤**：按单列过滤必须全扫
5. **存储成本增加**：实测多 10% 存储（V3 366 MB vs V1 332 MB）
6. **索引维护翻倍**：源列更新后需重算拼接列并重建索引

### 6.4 唯一的 Lance 商业用户公开讨论

**[Catalyzed.ai — "Bringing Multi-Column B-Trees to Columnar Storage"](https://catalyzed.ai/blog/bringing-multi-column-btrees-to-columnar-storage/)**

该博客是**唯一公开讨论 Lance 联合索引问题的商业用户**（付费客户，使用自己的 fork 版）。他们明确枚举了可选方案并给出判断：

> *"Option 1: Post-filtering — Index the most selective column, use it to get candidate rows, then filter the rest in memory… you're doing 2000× more I/O than necessary"*
>
> *"Option 2: Row ID intersection (multiple indexes intersected) — theoretically efficient… In practice, it's complex to implement correctly, requires multiple index lookups with their own I/O costs"*

他们没有把"拼接字段"列入候选，而是选择 **fork Lance 自建 `CompoundBTreeIndex`**——这反向证明拼接方案在严肃生产环境不被认可。

---

## 7. 生产落地清单

### 7.1 API 调用模板（推荐 V1）

```python
import lance

ds = lance.dataset("s3://bucket/frames")

ds.create_scalar_index("video_id", "BTREE", replace=True)
ds.create_scalar_index("frame_id", "BTREE", replace=True)

ds = lance.dataset("s3://bucket/frames")

result = ds.to_table(
    columns=["video_id", "frame_id", "payload"],
    filter="video_id = 12345 AND frame_id = 678",
)
```

**关键注意事项**：

1. **必须在 `create_scalar_index` 后重新 `lance.dataset(path)` 打开**—— pylance 4.0.1 查询规划器只在 dataset 重新打开时加载新索引。项目其他脚本（[`B2_filter_fair.py:142`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/B2_filter_fair.py)、[`B3_selectivity_sweep.py:93`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/B3_selectivity_sweep.py)、[`F_merge_insert.py:45`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/F_merge_insert.py)）均遵循此约定。
2. **向 `create_scalar_index` 传 list 会抛 `NotImplementedError`**——必须调用两次。
3. `replace=True` 建议保留，便于在 schema 变更或数据重建后幂等重建索引。

### 7.2 选择 BITMAP 还是 BTREE 的经验法则

| video_id distinct 数 | 推荐 | 理由 |
|---|---|---|
| < 1,000 | BITMAP | BITMAP 在低基数上更紧凑、查询更快 |
| 1,000 – 100,000 | BTREE 或 BITMAP 皆可 | 本测 10,000 时两者几乎持平 |
| > 100,000 | BTREE | BITMAP 存储成本线性增长 |

### 7.3 监控指标

使用 [`docs.lancedb.com/indexing`](https://docs.lancedb.com/indexing) 推荐的做法：

- 追踪每列索引的 size（`ds.list_indices()`）
- 追踪 fragment 数量；超过 1000 时及时 `optimize.compact_files()`
- 重大 append 后调用 `ds.optimize.optimize_indices()` 增量刷新

---

## 8. 未决问题 —— 上游进展追踪

| Issue / PR | 当前状态 | 监控意义 |
|---|---|---|
| [lance#3125](https://github.com/lance-format/lance/issues/3125) | 开启，18 个月无进展 | 一旦有进展 → 本文 §2 失效，V1 方案可升级为原生联合索引 |
| [lance#5480](https://github.com/lance-format/lance/pull/5480) | 已关闭 | 若被重开或 Catalyzed.ai fork 成为事实标准 → 评估其生产风险 |
| [lance#3868](https://github.com/lance-format/lance/issues/3868) "Generalize scalar indices" | 开启 | 是 #3125 的前置架构重构 |
| [lance#4805](https://github.com/lance-format/lance/issues/4805) "Multiple indexes per column" | 开启 | 相关的多索引规划工作 |

---

## 9. 外部证据关键信源

### 9.1 Lance 源码与 API

> **仓库身份说明**：Lance 核心 Rust 仓库早期位于 `github.com/lancedb/lance`，现已**重命名**为 `github.com/lance-format/lance`（GitHub API 返回同一 `id: 511691380`、`fork: false`；旧 URL 通过透明重定向仍可访问）。本节统一使用 canonical 名称 `lance-format/lance`。`lancedb/lancedb` 是另一个独立仓库（LanceDB 的 Python/Node 客户端层），**不要混淆**。

- [Lance `create_index` 单列强制校验](https://github.com/lance-format/lance/blob/main/rust/lance/src/index/create.rs#L139-L146)
- [Python `create_scalar_index` 签名](https://github.com/lance-format/lance/blob/main/python/python/lance/dataset.py#L3030)
- [TypeScript `IndexConfig.columns` 注释](https://lancedb.github.io/lancedb/js/interfaces/IndexConfig/)
- [Lance `expression.rs` 多索引 AND 相交实现](https://github.com/lance-format/lance/blob/main/rust/lance-index/src/scalar/expression.rs)
- [`IndexType::is_scalar()` 标量族定义](https://github.com/lance-format/lance/blob/main/rust/lance-index/src/lib.rs#L226) —— 决定哪些索引被归为"标量"
- [`FtsQueryParser` 源码（证明 FTS 仅响应 `contains_tokens`）](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L873-L943) —— 5 个 `visit_*` 返回 `None`（L890–L918），`visit_scalar_function` 仅对 `contains_tokens` 返回 `Some(TokenQuery::TokensContains)`（L932），其它 UDF 落 `None`（L942）
- [`LabelListQueryParser` 源码（证明 LabelList 仅响应 `array_has*`）](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L678-L790) —— 5 个 `visit_*` 返回 `None`；`visit_scalar_function` 仅对 `array_has` / `array_has_all` / `array_has_any` 这三个 UDF 返回 `Some`（L723–L789）
- [`BloomFilterQueryParser` 源码（证明其为不精确索引，需 recheck）](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L582-L677) —— 支持 `Eq`/`NotEq` / `IS IN` / `IS NULL` / `IS <bool>`；`visit_between` 与 `visit_scalar_function` 显式返回 `None`（源码注释 *"Bloom filters don't support range queries / scalar functions"*，L606–L613、L668–L676）
- [父 trait `ScalarQueryParser` 定义](https://github.com/lance-format/lance/blob/443f2daab80d10a35c9d6444ad8daa9cba37c6ba/rust/lance-index/src/scalar/expression.rs#L72-L170) —— 规定 `visit_between` / `visit_in_list` / `visit_is_bool` / `visit_is_null` / `visit_comparison` / `visit_scalar_function` / `visit_like` / `is_valid_reference` 八个 hook 的签名与默认返回
- [LanceDB `ScalarIndexType` 类型别名（仅含 BTREE/BITMAP/LABEL_LIST）](https://github.com/lancedb/lancedb/blob/main/python/python/lancedb/types.py#L30)
- [LanceDB `create_fts_index` 独立方法](https://github.com/lancedb/lancedb/blob/main/python/python/lancedb/table.py#L968)
- [官方测试 `test_use_multi_index`](https://github.com/lance-format/lance/blob/119f87b3/python/python/tests/test_scalar_index.py)

### 9.2 Lance 跟踪 issue 与 PR

- [Issue #3125 Composite scalar indices（核心跟踪）](https://github.com/lance-format/lance/issues/3125)
- [PR #5480 compound scalar index 完整讨论（含维护者反馈 + 作者基准数据）](https://github.com/lance-format/lance/pull/5480)
- [PR #4639 FTS 多列支持（内部撤回说明）](https://github.com/lance-format/lance/pull/4639)
- [Issue #3730 2025 Roadmap（明确不含联合索引）](https://github.com/lance-format/lance/issues/3730)
- [PR #1543 多索引 AND 相交能力引入](https://github.com/lance-format/lance/pull/1543)
- [Issue #3868 Generalize scalar indices（前置重构）](https://github.com/lance-format/lance/issues/3868)
- [Issue #4805 Multiple indexes per column（相关规划）](https://github.com/lance-format/lance/issues/4805)

### 9.3 Lance / LanceDB 官方文档

- [LanceDB Scalar Index 文档](https://docs.lancedb.com/indexing/scalar-index)
- [LanceDB Cloud FAQ（"filter columns" 复数建议）](https://lancedb.com/docs/faq/faq-cloud/)
- [Lance 用户指南 Skill](https://github.com/lance-format/lance/blob/main/skills/lance-user-guide/SKILL.md)
- [Rust docs `lancedb::index::scalar`](https://docs.rs/lancedb/latest/lancedb/index/scalar/)

### 9.4 Lance 商业用户的公开讨论

- **[Catalyzed.ai: Bringing Multi-Column B-Trees to Columnar Storage](https://catalyzed.ai/blog/bringing-multi-column-btrees-to-columnar-storage/)** ——唯一公开讨论此问题的商业 Lance 用户

### 9.5 业界反模式证据

- [Use The Index Luke — concatenated keys](https://use-the-index-luke.com/sql/where-clause/the-equals-operator/concatenated-keys)
- [Stack Overflow — avoid composite-column primary keys](https://stackoverflow.com/questions/31501853/database-modeling-avoid-using-primary-keys-based-on-multiple-columns-merging-th)
- [ClickHouse Sparse Primary Indexes (compound)](https://clickhouse.com/docs/en/guides/improving-query-performance/sparse-primary-indexes/sparse-primary-indexes-multiple/)
- [ClickHouse 24.6: optimize_row_order + hilbertEncode() 空间复合键](https://clickhouse.com/blog/clickhouse-release-24-06)
- [Databricks Liquid Clustering（Z-order 替代品）](https://docs.databricks.com/en/delta/clustering/index.html)
- [Databricks Z-order 9-列硬限制](https://databricks.helpjuice.com/en_US/dbsql/zorder-results-in-hilbert-indexing-can-only-be-used-on-9-or-fewer-columns-error)
- [Apache Iceberg Partitioning Transforms](https://iceberg.apache.org/spec/#partition-transforms)
- [Parquet Page Index（多列 sort order）](https://parquet.apache.org/docs/file-format/pageindex/)
- [TileDB 101: Arrays（原生多维坐标）](https://tiledb.com/blog/tiledb-101-arrays/)

### 9.6 本项目实测与脚本

- [`scripts/O_composite_key.py`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/O_composite_key.py) —— 5 变体 benchmark 脚本（349 行）
- [`results/O_composite_key.json`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/results/O_composite_key.json) —— 完整实测数据（`run_id=20260511-145554`，`internal_verify.ok=true`）
- [`docs/PARTITIONING_DESIGN.md`](PARTITIONING_DESIGN.md) —— PB 级分区方案设计
- [`scripts/B3_selectivity_sweep.py`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/B3_selectivity_sweep.py) —— 多单列索引 + AND 过滤的前置实验
- [`scripts/B4_spark_neutral.py`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/B4_spark_neutral.py) —— 同上，Spark 中立引擎下的复刻

---

## 10. 修订记录

| 日期 | 变更 | 依据 |
|---|---|---|
| 2026-05-11 | 初版发布 | 本次会话调研（5-agent 并行搜索 + `O_composite_key.py` 实测） |
| 2026-05-12 | §2.3 重构：将 7 种索引按谓词形态拆为三小节（等值/范围 · 函数谓词 · 全文），并明确 FTS/INVERTED、LABEL_LIST、NGRAM 对 `col = X` 无效 | 读者反馈 + librarian 对 `IndexType::is_scalar()` / `FtsQueryParser` / `LanceDB ScalarIndexType` 的源码交叉验证 |
| 2026-05-12 | §2.3.2 修正：移除不合逻辑的 "video_id 建 LABEL_LIST" 反例（主键本就不是 `List<T>` 类型），改用 `scene_tags: List<String>` 的真实混合查询场景演示 LABEL_LIST 与 BTREE 如何 AND 相交 | 读者指出举例不符合工程实践 |
| 2026-05-12 | §2.3.3 / §2.3.2 / §9.1 源码引用升级为 pinned commit `443f2da` + 精确行区间（FtsQueryParser `#L873-L943`、LabelListQueryParser `#L678-L790`、BloomFilterQueryParser `#L582-L677`、父 trait `ScalarQueryParser` `#L72-L170`），并点出关键分支行号（contains_tokens 匹配在 L932、非命中落 L942）| librarian 对 `lance-format/lance@443f2da` HEAD 的源码交叉验证（原引用的 `blob/main/...#L850` 因文件增长到 3087 行已过期） |
| 2026-05-12 | §9.1 顶部新增「仓库身份说明」：澄清 `lancedb/lance` 已重命名为 `lance-format/lance`（同一 repo id `511691380`，非 fork），`lancedb/lancedb` 是独立的客户端仓库；全文 `lancedb/lance/*` URL 统一迁移到 `lance-format/lance/*`（4 处）| librarian 对两个 GitHub API endpoint 返回 `id` 一致的验证 |
| 2026-05-12 | §3.1 时间线由 bullet list 改为表格，规避 CloudFront 渲染管线把相邻 list items 塌陷为单行的问题 | CDN 发布版 `composite-key-index.html` 的 `<ul>` 折行异常 |
| 2026-05-12 | §2.3.4 表头「能与 BTREE AND 相交？」→「能与其它标量索引 AND 相交？」；新增「AND 时该索引侧的谓词约束」列，把原来混在同一格的"非等值语义 / 仅 contains_tokens"等副本移到此列，语义维度正交化。表下新增 2 段脚注引 `ScalarIndexExpr::And` 递归对称结构 + `needs_recheck` 自动 recheck 机制的源码证据 | 读者指出原表头把相交机制锚定在 BTREE，与 §2.3.1 L87 / §2.3.2 L91 / §2.3.3 L116 自身声明的对称性矛盾；librarian 对 `expression.rs#L1257-L1350`（enum + BitAnd）+ `#L1534-L1541`（needs_recheck）+ `scanner.rs#L3867-L3919`（post_take recheck）+ `test_null_handling`（BITMAP×BTREE 测试）的源码交叉验证 |
| 2026-05-12 | §4.2 重写：把孤立的 Rust 注释和 PR #1543 例子合并为连贯叙述，补齐"前提 `x`/`y` 有索引、`z` 没有"的关键前提，新增 planner 产物列（`index_query` / `refine_expr` 的实际形态），并点出"完全由索引满足"vs."部分由索引满足"本质是 `refine_expr` 是否为 `None`，进一步说明这决定了回表 I/O 的放大系数 | 读者指出原表 2 行（`x=7 AND y<20` vs `x=7 AND z>30`）没讲清"完全/部分"的定义、前提、和后果；librarian 对 `IndexedExpression` (`#L1135-L1161`) / `visit_and` (`#L1858-L1882`) / `FilterPlan` (`scanner.rs#L3867-L3919`) 的源码交叉验证 |
| 2026-05-12 | §5.2 扩写：从 3 列（代号/策略/维护者推荐）扩为 4 列并加「研究假设」列，让每个 V0–V4 变体对应一个可证伪的问题；新增 §5.2 子节「四种查询负载的设计动机」把 Q_point/Q_range/Q_vid/Q_frame 设计为方案弱点矩阵；§5.3 表前加一句导览指引读者关注"反对角线崩溃格"而非最小值 | 读者指出原 §5.2 只列索引策略、没说每个变体在测什么假设，读者看大结果表时找不到设计意图；设计意图其实已在 `O_composite_key.py` docstring L1–L28 中，但未迁移到文档 |
| 2026-05-12 | §5.1 精简：删除"AWS EMR"字样（测试为单机非分布式）及"已通过 ai-slop-remover review"一行（内部工具信息不应暴露在对外文档） | 读者指出平台描述误导（EMR 暗示分布式）且 review 工具链属内部流程 |
| 2026-05-12 | §5.2 V4 行扩写 + 新增子节「V4 工作原理（为什么"排序 + 单列 BTREE"能模拟联合索引）」：点明 V4 = 物理排序 + 主键列 BTREE 两件事叠加（非"联合前缀索引"，Lance 无此概念），解释查询时如何利用连续区间 + 顺序 I/O，以及为什么 Q_frame 会崩溃（Lance 2.1 不从 row-order 自动构建 zonemap）| 读者指出 V4 描述过于简略、"前缀"一词易误读为"联合前缀索引" |
| 2026-05-12 | 删除原 §7「按数据规模的决策树」章节；后续章节号下移一位（原 §8→§7，§9→§8，§10→§9，§11→§10）；§10 修订记录内 2 处引用的 §10.1 同步更新为 §9.1 | 读者要求删除该章节（规模外推建议信心不足，维护成本高于价值） |
| 2026-05-12 | §5.2 新增子节「V4 的运维陷阱：compaction / update 会不会打乱排序？」+「V4 的生产落地规则」：给出 `compact_files()` / `delete()+compact` / `update()` / 非单调 append 四种操作对 V4 物理有序性的影响判定表，并基于源码确认 `CompactionOptions` 无 `sort_by` 参数、`unenforced_clustering_key` metadata 未被任何代码路径读取；结论是 `compact_files()` 在"单调 append + 不用 update"前提下保留 V4 有序性，否则需整表 overwrite 恢复 | 读者提问"compact 会不会打乱 V4 的排序"；librarian 对 `rust/lance/src/dataset/optimize.rs`（`compact_files` docstring L732 + `scan_in_order(true)` L904 + 无 `sort_by` 参数）、`write/update.rs#L299`（update = 尾部 append 新 fragment + 原位 tombstone）、`field.rs#L54`（`unenforced_clustering_key` 确为 unenforced）的源码交叉验证 |
| 2026-05-12 | 发布管线 `publish.py` 新增 `--strip-from TEXT` flag：发布时按 heading 文本前缀剥除章节（到下一个等级或更高级 heading，或 EOF）。源 md 保留完整修订记录（内部 audit trail），CDN HTML 不显示 §10 和「文档状态」元信息段 | 读者要求对外文档隐藏修订记录，但源文件需保留 audit 用途 |
| 2026-05-12 | 删除原 §8.1「本项目未覆盖的场景」子节；§8 由"§8.1 / §8.2 两子节"合并为单层结构，标题改为「§8. 未决问题 —— 上游进展追踪」，原 §8.2 表格上提为章节正文 | §8.1 列举的"未覆盖场景"对读者价值低（列出"还没做什么"偏内部规划思路）；删除后只保留 §8.2 的"跟踪上游 issue/PR" 对外部读者才有实操价值 |
| 2026-05-12 | §5.2 / §7.1 / §9.6 的 8 处相对路径（`../scripts/*.py`、`../results/*.json`）全部替换为 GitHub 绝对 URL（`https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/...`）；项目源代码首次推送到 `github.com/ZackFairTS/lance-test` repo 的 `extended-bench/` 目录下（含今日新增的 `O_composite_key.py` + `docs/` + `results/O_*.json`），`publish.py` 管线本身也一并入库 | 对外发布的 HTML 里相对 path 无法工作（CDN 根本没这些文件）；必须改绝对 URL 且源代码须可公开访问 |

---

**文档状态**：所有外部引用链接均为调研时可访问；若失效，源码/issue ID 可通过 GitHub 内部搜索定位。
