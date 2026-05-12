# Q — Concurrent Mutation × Compaction Conflict Benchmark

**Question**: When `delete` / `update` / `merge_insert` run concurrently with `optimize.compact_files()`, does it cause user-visible task failures?

**Short answer**: At this workload size (500k rows, 4 scenarios × 4 concurrency levels, 60s each), **writers never fail end-to-end** — Delete/Update/MergeInsert's 10×30s outer retry wrapper absorbs all semantic conflicts. **Compactor itself fails 1–3% of the time under concurrent Update/MergeInsert** on overlapping fragments, consistent with the source-code prior that `compact_files()` has no outer retry wrapper.

---

## Experimental setup

- **Machine**: r8g.2xlarge (Graviton ARM64, 8 vCPU / 64 GiB), local NVMe (single-machine, non-distributed)
- **Software**: pylance 4.0.1 (lance-core 0.39.0)
- **Base table**: 500,000 rows, 4 columns (`id int64`, `group_id int64`, `value float32`, `payload string` ~32 bytes), written in 10 fragments (`max_rows_per_file=50000`)
- **Run duration**: 60 seconds per (scenario, concurrency)
- **Workers**: `multiprocessing` spawned processes, barrier-synchronised to start simultaneously
- **Compactor**: single dedicated process running `ds.optimize.compact_files(target_rows_per_fragment=50000)` in a loop with 0.5s sleep
- **Script**: [`scripts/Q_concurrent_mutation_compact.py`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/scripts/Q_concurrent_mutation_compact.py)
- **Raw results**: [`results/Q_concurrent_mutation_compact.json`](https://github.com/ZackFairTS/lance-test/blob/main/extended-bench/results/Q_concurrent_mutation_compact.json)

### Scenario matrix

| Scenario | Mutation | With compactor? | What it tests |
|---|---|---|---|
| **S1_delete_noc** | `ds.delete("id=X")` | no | baseline — delete contention only, no compact races |
| **S2_delete_compact** | `ds.delete("id=X")` | yes | delete vs compact on overlapping fragments |
| **S3_update_compact** | `ds.update({"value": v}, where="id=X")` | yes | update vs compact (semantically similar to S2 but different code path) |
| **S4_merge_insert_compact** | `ds.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(tbl)` | yes | upsert vs compact — the path most users hit |

Concurrency levels **N ∈ {1, 2, 4, 8}** for each scenario, 16 runs total.

---

## Results

### Writer success rate — 100% across the board

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S1 delete (baseline) | 100% | 100% | 100% | 100% |
| S2 delete + compact | 100% | 100% | 100% | 100% |
| S3 update + compact | 100% | 100% | 100% | 100% |
| S4 merge_insert + compact | 100% | 100% | 100% | 100% |

**No single writer operation failed across 63,338 total writer calls.** This is the expected behavior per source code: Delete/Update/MergeInsert go through `execute_with_retry` with default `RetryConfig { max_retries: 10, retry_timeout: 30s }` wrapping the inner 20-retry manifest-race loop. Every semantic conflict surfaces as `RetryableCommitConflict` → rebase → retry → eventually wins.

### Compactor failure rate — this is where conflicts become user-visible

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S2 delete + compact | 0/120 | 0/120 | 0/120 | 0/120 |
| S3 update + compact | **1/111 (0.9%)** | 0/103 | **2/100 (2.0%)** | **3/100 (3.0%)** |
| S4 merge_insert + compact | 0/114 | 0/108 | **3/102 (2.9%)** | **2/96 (2.1%)** |

All failures are `RetryableCommitConflict` ("preempted by concurrent transaction ... Please retry"). `compact_files()` has no outer retry wrapper — the inner 20-retry loop only covers object-store CAS races, **not** semantic fragment-overlap conflicts.

**Why S2 never fails**: `delete` writes very small deletion-vector updates. The critical window where both parties hold the same fragment's old view is too short to race often in this workload. Source code says it *should* be racable; just not under this specific tempo.

**Why S3/S4 fail at N≥4**: `update` rewrites entire fragments (re-materializing modified rows), widening the race window. `merge_insert` does the same when a matched row triggers an update. By N=4 the compactor's 20 inner retries get exhausted against an update rate of ~70/s.

### Latency under contention

Writer p99 latency (ms) — note the tail growth:

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S1 delete (baseline) | 31.9 | 115.5 | 175.4 | 275.2 |
| S2 delete + compact | 64.3 (**+100%**) | 152.1 (+32%) | 265.5 (+51%) | 418.7 (+52%) |
| S3 update + compact | 86.0 | 138.0 | 274.0 | 388.8 |
| S4 merge_insert + compact | 81.7 | 144.9 | 224.7 | 392.2 |

Throughput QPS (writer-aggregate):

| Scenario | N=1 | N=2 | N=4 | N=8 |
|---|---:|---:|---:|---:|
| S1 delete (baseline) | 46.3 | 71.0 | 56.4 | 32.9 |
| S2 delete + compact | 46.1 | 68.1 | 85.9 | 49.9 |
| S3 update + compact | 39.8 | 62.0 | 57.9 | 44.8 |
| S4 merge_insert + compact | 32.1 | 53.6 | 59.1 | 52.0 |

**Parallelism caps out between N=2 and N=4.** Going from N=4 to N=8 actually *reduces* throughput in several scenarios — the retry-backoff tail grows faster than parallelism helps. This is the classic signature of a centralised manifest-CAS bottleneck.

**Max latency outliers** (single-operation worst case): some writers saw 40–150 *seconds* at N≥4. These are retries consuming near-full `retry_timeout=30s`. The 10-retry budget was enough (no writer reached it), but individual operations in the tail did sit in the retry loop for their full timeout window before eventually committing.

---

## Conclusions

1. **High-concurrency writes alone do NOT cause task failures** — the 10×30s outer retry budget for `delete`/`update`/`merge_insert` successfully absorbs all semantic conflicts at this workload. This holds empirically up to N=8 against a continuous compactor.

2. **Compact is the risk point, not the writers.** `compact_files()` has no outer retry and raised `RetryableCommitConflict` as Python `RuntimeError` **1–3% of the time** under concurrent update/merge_insert. A production scheduler running compact on a timer must wrap the call in try/except and re-plan on failure. This confirms lance issues #2397 (open 2 years) and #3068 (maintainer-acknowledged: "the best workaround for now is to do updates serially").

3. **Throughput scaling beyond N≈4 is negative** under concurrent compact. Adding writers past this point increases tail latency and reduces aggregate QPS. For production: size your writer pool to the *steady* manifest-commit rate Lance can sustain, which depends on object store CAS latency.

4. **Max-latency outliers hit 40–150 seconds** for individual writer operations. If your p99.9 SLA is tighter than 60 seconds, the default `retry_timeout=30s` + backoff may still cause user-visible timeouts even though the operation eventually commits. Consider tuning `conflict_retries=` and `retry_timeout=` on sensitive call sites.

5. **No evidence of silent data corruption.** Every failed compact iteration raised a clear exception; no row resurrection, no data loss. This matches the test-regression PR #6653 which fixed the last known row-resurrection path on 2026-05-07 (distributed compaction stale read_version).

### What this benchmark does NOT cover

- **S3 / object-store backed datasets** — all results are from local NVMe. Production S3 CAS failures (lancedb #2426) and Tencent COS atomicity bugs (lance #6595) are out of scope.
- **Distributed compaction** across multiple Python processes running `plan_compaction` then `commit_compaction` (Spark/Ray pattern) — PR #6653 stale read_version fix is recent, needs its own validation.
- **Very long retry tails under sustained pressure > 10 minutes** — `retry_timeout` may still exhaust under truly extreme contention.
- **Beyond N=8** — this EMR machine has 8 vCPU; scaling further would need a bigger instance or distributed setup.
- **`Overwrite` operations** — deliberately skipped because source says all Overwrite × mutation pairs are `IncompatibleTransaction` (non-retryable). Production should not concurrently Overwrite a table being read/written anyway.

### Practical recommendation

For the original risk question "does concurrent high-throughput write + compaction cause task failures":

- **Writer side: No, your app-level writer code does not need conflict handling** — the retry budget covers this workload. Do check if your individual operation tail latency is acceptable (max can hit tens of seconds).
- **Compaction side: Yes, your compaction scheduler MUST handle `RuntimeError` with "preempted" / "retryable" / "conflict" in the message** — implement application-level retry with exponential backoff. Simpler alternative: run compact only during low-write windows.
- **If you use `merge_insert` as an upsert** (the common case): consider routing it through a single serial writer rather than multiple parallel processes; conflict rate scales with concurrency and even the 10-retry budget gets thinner at N≥8.
