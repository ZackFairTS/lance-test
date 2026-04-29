# Lance 小文件对读性能影响 - 压测报告目录

## 本目录包含什么

这是对 **"频繁 commit 导致大量小 fragment 时，读性能如何受影响"** 的完整实证测试。

### 文件

- **[REPORT.md](REPORT.md)** ⭐ — 主报告，所有结论和数据
- **[plots/performance_plot.png](plots/performance_plot.png)** — 6 图合一的对比图（log-log scale）
- `scripts/` — 复现脚本
  - `build_E.py` — 构建 5000 fragments 的基线数据集
  - `compact_cascade.py` — 逐步 compact 出 D/C/B/A 四个版本
  - `read_bench.py` — Python 单进程读测试（6 种读场景 × 5 版本）
  - `spark_read.py`, `spark_full_read.py` — Spark 分布式读对照
  - `generate_report.py` — 从 JSON 生成 Markdown 报告
- `data/` — raw 测试数据
  - `E_dataset_info.json` — 基线数据集构建统计
  - `compact_plan.json` — 4 次 compaction 详情
  - `read_{A,B,C,D,E}.json` — 5 个版本的读测试完整结果
  - `spark_{A,E}.json`, `spark_full_{A,E}.json` — Spark 读结果
  - `build_E.log.gz` — 36 分钟构建过程日志（含速率退化证据）

## 一句话结论

**小 fragment 对读性能的影响强烈依赖于读取方式**：

| 读方式 | 5000 frag vs 1 frag |
|---|---|
| Python 单进程扫描/range | **10-18x 慢** 🔴 |
| Dataset.open() | 1.7x 慢 🟡 |
| Python 点查 | 几乎无差 🟢 |
| **Spark 分布式读** | **几乎无差** 🟢 |

**小文件 = Python 单线程 I/O 串行化问题**。Spark 的并行读用 executor 扇出同时读 fragment，这个问题反而成了优势。

## 方法论

- **单一数据集 + version checkout** 技巧：只写一次 10M 行数据（5000 fragments），然后 compact 出 1000/100/10/1 fragment 的 4 个 version。测试时 `lance.dataset(path, version=N)` 切换版本 → 保证对比公平（同样数据、同样 S3 bucket、同样进程）。
- 读指标全：open、全表扫描、单列扫描、点查、范围查询、count
- 环境: AWS EMR master (ARM64 Graviton)，S3 ap-northeast-1，pylance 4.0.1 (lance-core 0.39.0)

## 如何复现

见 [REPORT.md](REPORT.md) 末尾的 "原始数据位置"，以及 `scripts/` 下的脚本。
