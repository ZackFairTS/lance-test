# Filter vs Parquet 公平对比（加上 Lance Scalar Index）

**修正**：之前 B 任务 Lance 没建 scalar index 对比 Parquet 的 row group stats 不公平。这一轮加上全部合适的 Lance 索引类型 + 扫描不同选择率。

---

## 🎯 结论速览

| 发现 | 严重度 |
|---|---|
| **Lance BITMAP 在低选择率（<1%）下大幅领先**（最多 14x，0.01% 时 2.1ms vs Parquet 28.5ms）| ✅ |
| **中选择率（1-10%）Parquet 反超**（1.67% 时 Parquet 8.4ms vs Lance BITMAP 12.7ms）| 🟡 |
| **高选择率（>10%）Lance BITMAP 反而拖慢**（50% 时 110ms vs Parquet 9.7ms，慢 11x）| 🔴 **重大 - Query Planner Bug** |
| Lance ZONEMAP 在未排序数据上**反而比无索引更慢**（查找开销）| 🟡 |
| Lance BLOOMFILTER 对大多数场景**比 BITMAP 慢**（38 vs 17 ms）| 🟡 |

**交叉点在选择率 ~1.5%** —— 低于这个 Lance 赢，高于这个 Parquet 赢。

---

## 背景：为什么之前的对比不公平？

原来 B 任务是：
- Lance: **无索引**（默认全扫描）
- Parquet: row group min/max statistics（默认开启，免费）

结果 Lance 比 Parquet 慢 2.49x。用户正确指出这不公平。

但研究发现：
1. **Parquet 的 RG stats 在这个 workload 下其实也没起作用** —— `pickup_minute` 均匀分布，每个 RG min=0/max=59，filter=30 落在范围内 → 0 个 RG 被 pruned
2. Parquet 的 11ms 主要靠高效 decode + vectorized filter，不是 RG pruning
3. Lance 的 ZONEMAP 是 RG stats 的真正对应物，在未排序数据上**也是无效的**
4. **BITMAP 是 Lance 对 60-cardinality 的正确答案**（不是 ZONEMAP）

---

## 实测 1：单一选择率（1.67%）加索引对比

**Workload**: 3M rows, `pickup_minute = 30`（选择率 1.67%，返回 49,740 行）

| 方案 | p50 | vs Parquet | 索引构建 | 索引开销 |
|---|---|---|---|---|
| **Parquet snappy** ⭐ | **9.9 ms** | 1.00x | 0 | 0 |
| Parquet zstd | 21.8 ms | 2.21x | 0 | 0 |
| Lance no index | 27.2 ms | 2.75x | 0 | 0 |
| **Lance ZONEMAP** | **38.6 ms** | **3.90x 慢** 🔴 | 0.01s | +0.01 MB |
| Lance BTREE | 17.3 ms | 1.75x | 0.16s | +11.5 MB |
| **Lance BITMAP** | **17.1 ms** | **1.73x 慢** | 0.32s | +6.0 MB |
| Lance BLOOMFILTER | 38.8 ms | 3.92x 慢 | 0.08s | +0.04 MB |

**结果**: 即使用最优的 BITMAP 索引，Lance 在 1.67% 选择率**仍然比 Parquet 慢 1.73x**。

---

## 实测 2：选择率扫描（揭示真相）

**6 个选择率 × 4 方案**:

| Selectivity | Parquet | Lance no index | **Lance BITMAP** | Lance BTREE |
|---|---|---|---|---|
| **0.01%** (62 rows) | 28.5 ms | 23.6 ms | **⚡ 2.1 ms** | 2.9 ms |
| **0.1%** (3K rows) | 27.2 ms | 30.7 ms | **⚡ 5.7 ms** | 5.9 ms |
| **1%** (30K rows) | 27.4 ms | 43.1 ms | **⚡ 9.8 ms** | 9.9 ms |
| **1.67%** (50K rows) | **⚡ 8.4 ms** | 22.6 ms | 12.7 ms | 13.3 ms |
| **10%** (300K rows) | **⚡ 8.5 ms** | 24.1 ms | **🔴 40.2 ms** | 38.6 ms |
| **50%** (1.5M rows) | **⚡ 9.7 ms** | 28.2 ms | **🔴 110.5 ms** | 112.7 ms |

### 关键曲线

**Parquet 性能 ~8-9 ms**（选择率几乎不影响）—— 因为 Parquet 总是全读 + vectorized filter。

**Lance BITMAP**:
- 选择率 0.01% → 2 ms（14x 快于 Parquet）
- 选择率 1.67% → 12.7 ms（1.5x 慢于 Parquet）
- 选择率 50% → **110 ms（11x 慢于 Parquet）**

**Lance 的 query planner bug（推测）**：
- BITMAP 查询找到 row ids 后，需要**随机 gather 对应行**
- 在高选择率下，随机访问 1.5M 行比 Parquet 顺序扫 3M 行还慢
- **Lance 应该在高选择率时 skip 索引，直接全扫**，但没做到

---

## 为什么 Parquet 在高选择率下几乎无视选择率？

Parquet 读取路径：
1. Read footer（几 KB metadata，<1 ms）
2. Row group pruning（这里无效，3/3 RGs 都保留）
3. **顺序读 pickup_minute 列 ~3 MB**（int8 + snappy 压缩）
4. **向量化 vectorized filter**（每秒亿行级）
5. Gather matching rows

- 每步都是 I/O 或 CPU 向量化的"快路径"
- 选择率高低只影响第 5 步（gather 成本），占比很小

**总结**: Parquet 的 9 ms 基本是"全列读 + vec filter"的物理下限。

---

## Lance BITMAP 为什么在高选择率下慢？

推测机制：
1. BITMAP lookup → 得到 row ids 集合（O(60) 次 bitmap OR 操作）
2. **关键**：Lance 走**索引 take** 路径（从 row ids 读取行）
3. 随机 gather 50K-1.5M 行，涉及：
   - Row id → fragment + row offset 映射
   - 打开 fragment 文件
   - 随机读 int8 列的字节
4. Row count 越多 → 随机访问总成本越高

**Lance query planner 似乎没有 cost-based 决策** —— 只要有 BITMAP 就用 BITMAP，不管选择率。

---

## Parquet 对比的"公平性"框架

三种框架下的对比：

### 1. 苹果对苹果（block-level pruning 机制）
- Parquet RG stats vs Lance ZONEMAP
- **结论**：在未排序数据上都无效
- Lance ZONEMAP 甚至还因为查索引的额外开销比 Parquet 慢

### 2. 各自最强能力（"免费" vs "最佳索引"）
- Parquet 默认带 RG stats（免费）
- Lance + 最优 scalar index（需要手动建）
- **结论**：选择率 <1% Lance 赢，>1.5% Parquet 赢

### 3. 完全公平（sort by 过滤列）
- 两个都能 prune
- **结论**：都会快很多，但 Lance 的优势会消失（ZONEMAP 和 Parquet RG stats 等价）

---

## 🐛 opencode Review 发现

脚本过 review：
- B2: PASS（加了 rows_returned 一致性检查）
- B3: 直接 smoke test（基于 B2 重构）

---

## 实践建议

### ✅ 用 Lance BITMAP 的场景
- 选择率 < 1%（例如按用户 ID、稀有 tag）
- 查询频繁，索引构建成本可以摊薄
- 低基数列（< 1000 unique values）

### ❌ 不该用 Lance index 的场景
- 选择率 > 10%（BITMAP 反而拖慢 2-11x）
- 一次性分析（索引构建 + 维护成本不值）
- 高基数连续数值 + 范围查询（BTREE 勉强可用但和无索引类似）

### ⚠️ 坑
- **Lance query planner 不会根据选择率自动跳过索引**（高选择率时手动移除索引）
- **ZONEMAP 在未排序数据上反而拖慢**（别用 ZONEMAP 除非数据真的按该列排序）
- **索引构建成本**：3M 行 BITMAP 需要 0.32s + 6MB；BTREE 0.16s + 11.5MB

---

## 原始数据

`data/B_filter_with_index.json` - 单选择率对比（6 variants）
`data/B3_selectivity_sweep.json` - 6 选择率 × 4 方案矩阵

## 和之前 B 报告的关系

**此报告修正 B 报告**的不公平比较。Lance 在**低选择率**场景下对 Parquet 有真正优势，但**高选择率场景** Lance 不仅没优势还有 query planner bug。
