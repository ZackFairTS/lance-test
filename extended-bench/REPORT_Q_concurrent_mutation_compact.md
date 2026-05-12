# Q — 并发 mutation × compaction 冲突实测

**研究问题**：当 `delete` / `update` / `merge_insert` 和 `optimize.compact_files()` 并发执行时，是否会出现用户可见的任务失败？

**简短回答**：在本实测规模下（500k 行 × 4 scenarios × 4 并发度 × 60s = 16 次运行，63,338 总操作数），**writer 端从不失败** —— Delete/Update/MergeInsert 的 10 × 30s 外层 retry 机制完全吸收了语义冲突。但 **compactor 本身在并发 update/merge_insert 场景下有 1–3% 的失败率**（同 fragment 重叠时），这和源码预测完全一致：`compact_files()` 没有外层 retry 包装，`RetryableCommitConflict` 会直接抛到用户。

---

## 实验环境

- **机器**：r8g.2xlarge（Graviton ARM64，8 vCPU / 64 GiB），本地 NVMe（单机、无分布式）
- **软件**：pylance 4.0.1（lance-core 0.39.0）
- **底表**：500,000 行，4 列（`id int64`、`group_id int64`、`value float32`、`payload string ~32 字节`），10 个 fragment 写入（`max_rows_per_file=50000`）
- **每轮时长**：60 秒（每个 scenario × 并发度组合）
- **Writer 架构**：`multiprocessing` spawn 出的独立 process，通过 barrier 同步启动
- **Compactor**：独立 process 跑 `ds.optimize.compact_files(target_rows_per_fragment=50000)` 循环，每轮间隔 0.5s sleep
- **脚本**：[`scripts/Q_concurrent_mutation_compact.py`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/Q_concurrent_mutation_compact.py)
- **原始数据**：[`results/Q_concurrent_mutation_compact.json`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/results/Q_concurrent_mutation_compact.json)

### Scenario 矩阵

| Scenario | Mutation 操作 | 并发 compact？ | 测试意图 |
|---|---|---|---|
| **S1_delete_noc** | `ds.delete("id=X")` | 否 | 基线 —— 只测 delete 自身的 contention，不与 compact 交互 |
| **S2_delete_compact** | `ds.delete("id=X")` | 是 | delete 与 compact 在同 fragment 上的竞争 |
| **S3_update_compact** | `ds.update({"value": v}, where="id=X")` | 是 | update 与 compact 的竞争（code path 与 S2 不同） |
| **S4_merge_insert_compact** | `ds.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(tbl)` | 是 | upsert 与 compact 的竞争 —— 生产最常见场景 |

并发度 **N ∈ {1, 2, 4, 8}**，共 16 次运行。

---

## 结果

### Writer 成功率 —— 全部 100%

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S1 delete（基线） | 100% | 100% | 100% | 100% |
| S2 delete + compact | 100% | 100% | 100% | 100% |
| S3 update + compact | 100% | 100% | 100% | 100% |
| S4 merge_insert + compact | 100% | 100% | 100% | 100% |

**63,338 次 writer 调用，零失败。** 这与源码设计完全吻合：Delete/Update/MergeInsert 都走 `execute_with_retry`，默认 `RetryConfig { max_retries: 10, retry_timeout: 30s }` 包裹着内层 20-retry 的 manifest-race 循环。每次语义冲突都会以 `RetryableCommitConflict` 形式触发 rebase + 重试，最终成功。

### Compactor 失败率 —— 真正的风险点

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S2 delete + compact | 0/120 | 0/120 | 0/120 | 0/120 |
| S3 update + compact | **1/111 (0.9%)** | 0/103 | **2/100 (2.0%)** | **3/100 (3.0%)** |
| S4 merge_insert + compact | 0/114 | 0/108 | **3/102 (2.9%)** | **2/96 (2.1%)** |

所有失败都是 `RetryableCommitConflict`（错误消息含 "preempted by concurrent transaction ... Please retry"）。`compact_files()` 没有外层 retry 包装 —— 内层 20-retry 只处理 object-store CAS 竞争，**不处理** fragment 重叠的语义冲突。

**为什么 S2 从不失败**：`delete` 写入极小（只写 deletion vector），双方同时持有同 fragment 旧视图的竞争窗口太短，在这个 workload 节奏下碰不上。源码层面**应该**可以竞争，但实测跑不出来。

**为什么 S3/S4 在 N≥4 开始失败**：`update` 会重写整个 fragment（重新物化被修改的行），竞争窗口大幅拉长。`merge_insert` 命中 matched 行时也走相同路径。N=4 时 update 频率 ~70/s，compactor 的 20 次内层重试在这个强度下被耗尽。

### 冲突下的延迟

Writer p99 延迟（毫秒），注意尾部增长：

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S1 delete（基线） | 31.9 | 115.5 | 175.4 | 275.2 |
| S2 delete + compact | 64.3（**+100%**） | 152.1（+32%） | 265.5（+51%） | 418.7（+52%） |
| S3 update + compact | 86.0 | 138.0 | 274.0 | 388.8 |
| S4 merge_insert + compact | 81.7 | 144.9 | 224.7 | 392.2 |

吞吐量 QPS（writer 总和）：

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S1 delete（基线） | 46.3 | 71.0 | 56.4 | 32.9 |
| S2 delete + compact | 46.1 | 68.1 | 85.9 | 49.9 |
| S3 update + compact | 39.8 | 62.0 | 57.9 | 44.8 |
| S4 merge_insert + compact | 32.1 | 53.6 | 59.1 | 52.0 |

**并行度在 N=2 到 N=4 之间饱和。** 从 N=4 到 N=8 反而多个 scenario 吞吐**下降** —— retry-backoff 的尾部增长快于并行度带来的收益。这是典型的**中心化 manifest-CAS 瓶颈**特征。

**最大单次延迟尾部异常**：N≥4 下部分 writer 单次操作看到 40–150 **秒** 的延迟。这些是 retry 挂起，几乎跑满整个 `retry_timeout=30s`。10 次 retry 预算足够（没有 writer 触及上限），但尾部单次操作在成功提交前确实在 retry 循环里待满了整个 timeout 窗口。

---

## 结论

1. **纯高并发写入本身不会造成任务失败** —— Delete/Update/MergeInsert 的 10 × 30s 外层 retry 预算在这个 workload 下足以吸收所有语义冲突。实测一直到 N=8 并发 + 持续 compactor 都成立。

2. **Compact 才是风险点，不是 writer。** `compact_files()` 没有外层 retry，在并发 update/merge_insert 下 **1–3% 失败率**，失败以 Python `RuntimeError` 形式抛出。生产环境的 compaction scheduler 必须 try/except 并按需 re-plan。这与 lance 仓库的 issue #2977 / #3068（维护者原话："目前最佳 workaround 是串行执行更新"）相符。

3. **并发度超过 N≈4 时吞吐收益为负**。在并发 compact 的情况下，继续增加 writer 反而让尾延增长、总 QPS 下降。生产建议：根据对象存储 CAS 延迟和 Lance 能稳定承受的 manifest-commit 速率来调整 writer 池大小。

4. **单次操作最大延迟可达 40–150 秒**。如果你的 p99.9 SLA 严于 60 秒，默认 `retry_timeout=30s` + backoff 仍可能造成用户可见的超时，即使操作最终成功提交。敏感场景建议调小 `conflict_retries=` 和 `retry_timeout=`。

5. **未观察到静默数据损坏**。每次 compact 失败都抛出了清晰的异常；没有已删除行复活、没有数据丢失。这与 2026-05-07 合入的 PR #6653（修复分布式 compaction stale read_version 导致的行复活）一致。

### 本 benchmark 不涵盖的场景

- **S3 / 对象存储**：本测全部在本地 NVMe。生产 S3 CAS 失败（lancedb #2426）、腾讯云 COS 原子性问题（lance #6595）等不在范围内。
- **分布式 compaction**：跨多 Python process 调用 `plan_compaction` + `commit_compaction`（Spark/Ray 模式）—— PR #6653 的 stale read_version 修复较新，需要独立验证。
- **>10 分钟持续压力下的 retry 长尾**：`retry_timeout` 在真正极端竞争下仍可能被耗尽。
- **N>8 的并发度**：本 EMR 机器只有 8 vCPU；进一步扩展需要更大实例或分布式方案。
- **`Overwrite` 操作**：故意跳过，因为源码层面 Overwrite 与任意 mutation 的组合都是 `IncompatibleTransaction`（不可重试）。生产场景也不应在有并发读写的表上用 Overwrite。

### 生产落地建议

回答原始风险问题"并发高吞吐写入 + compaction 是否会造成任务失败"：

- **Writer 端：不用加冲突处理** —— retry 预算在此 workload 下足够。但要评估单次操作的尾延是否可接受（最大可达数十秒）。
- **Compaction 端：必须处理 `RuntimeError`**（错误消息含 "preempted" / "retryable" / "conflict"）—— 实现应用层重试 + 指数退避。更简单的替代方案：只在低写入时段跑 compact。
- **`merge_insert` 当 upsert 用**（常见场景）：建议走**单线程串行** writer，不要多 process 并发；冲突率随并发度增长，即使有 10-retry 预算，N≥8 时预算也显得吃紧。
