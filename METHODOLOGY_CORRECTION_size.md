# Methodology Correction: Size Measurement and MVCC

**Date**: 2026-05-06
**Trigger**: User feedback on `compact_gc_investigation.py`
**Applies to**: All reports with "storage size" claims

---

## The mistake

Early M6 and `compact_gc_investigation.py` wording called Lance's post-compact directory growth "bloat" / "does not GC". Both are wrong framing.

Lance and Iceberg are both MVCC systems. Every commit (insert, delete, compact) produces a new version/snapshot. **Old versions are retained by design** because they make time travel (`checkout_version(N)`), audit logs, and rollback possible. Calling these "orphans" or "bloat" conflates:

- **Active size**: bytes referenced by the current version. This is the real "table size" if the user does not need history.
- **Total on-disk size**: bytes in the whole prefix, including MVCC history. This is the real billing cost.

`aws s3 ls --recursive --summarize` and `os.walk()` sum every file and return Total. They do not compute Active. Using Total where Active is the right answer makes retained history look like a leak.

---

## What we actually measured, stage by stage

Using the new `measure_active_size.py` on the sf10 M1-written store_sales tables:

| Format | Active MB | Total MB | Ratio |
|---|---|---|---|
| Lance v2.2 | **3568.4** | 3626.4 | active/total = 98.4% (58 MB is `_indices/` BITMAP from M4) |
| Iceberg v2 | **1475.8** | 1475.8 | active == total (no history at M1 yet) |

**Lance active / Iceberg active = 2.42x** — the M2 conclusion stands. This was always apples-to-apples because both sides were fresh single-version writes.

### The only test that needs reframing: M6

M6 wrote baseline + 20 / 50 small appends + compact. Each stage:

| Stage | Active MB | Total MB | Active/Total | What's the extra Total? |
|---|---|---|---|---|
| 1. Baseline | 3.50 | 3.50 | 100% | (no history) |
| 2. After 50 appends | 21.72 | 21.86 | 99% | 137 KB of version logs |
| 3. Post-compact | **20.64** | 42.51 | 48.6% | 51 historical fragments from v1..v51 (required for time travel) |
| 4. Post `cleanup_old_versions(0)` | 20.64 | 20.64 | 100% | (user discarded history) |

**The right conclusion**: Lance compact actively **reduces** active size from 21.72 → 20.64 MB (5% win). The Total doubling at stage 3 is **intentional MVCC retention** — Iceberg behaves identically via snapshot history.

---

## What needs re-running

### Nothing that was an Active-size claim

These all used single-version writes (1 version == 0 history) so Total == Active:

| Report | Claim affected | Status |
|---|---|---|
| M2 size (Lance 2.4x vs Iceberg) | Active size comparison, both 1 version | ✅ **Valid as-is** |
| M2 per-column (decimal 5.79x, string 27-35x) | Used `ds.stats.data_stats()` / Parquet row_group metadata, no history in either | ✅ **Valid as-is** |
| L2 format matrix (Blob V2 20x slower, v2.1 54% regression) | 1-shot writes, 1 version each | ✅ **Valid as-is** |
| Decimal bloat repro (36x on sorted) | 1-shot writes | ✅ **Valid as-is** |
| I_compression, J_format_versions, B filter series | 1-shot writes | ✅ **Valid as-is** |
| Flink 40-68% duplicate rate | Not a size claim (row-count on dedup) | ✅ **Valid as-is** |
| read-perf-bench 10-18x slowdown | Not a size claim (latency) | ✅ **Valid as-is** |
| ml-training-bench throughput | Not a size claim | ✅ **Valid as-is** |
| M3 scan latency, M4 filter latency, M5 delete latency + post-scan latency | Not size claims | ✅ **Valid as-is** |

### Things that need re-wording (no re-run needed)

| File | Claim | Correction |
|---|---|---|
| REPORT_M_lance_vs_iceberg.md M6 section | "Lance compact inflates size 76%" | "Post-compact Total grows 76% as MVCC history; Active shrinks 5%" |
| REPORT_M_lance_vs_iceberg.md TL;DR | "compact 后还膨胀 73-76%" | Removed; M6 is MVCC not bloat |
| REPORT_M_lance_vs_iceberg.md M5 note | "Storage 变化几乎不可测" | Same numbers, added note that the small deltas are new deletion file + MVCC retention |
| compact_gc_investigation.py VERDICT | "HYPOTHESIS A supported: compact does NOT GC" | "HYPOTHESIS A confirmed. Post-compact growth is MVCC history retained by design" |
| META_ANALYSIS.md open question #6 | Framed as "bug solved via cleanup" | Reframed as "both MVCC systems require periodic history cleanup; Lance cleanup_old_versions corresponds to Iceberg expire_snapshots" |
| META_ANALYSIS.md structural defect #1 | "Manifest-per-commit → 高频写拖垮读" | Same root cause (manifest chain read on open) but clarified this is MVCC architecture both formats share, not Lance-specific |

### Optional re-runs that would add rigor (not required for correctness)

| Re-run | Cost | Value |
|---|---|---|
| M6 sf10 with `measure_active_size.py` snapshots at each phase | 5 min Python on existing S3 data | Exact active vs total numbers in the report table |
| M5 sf10 post-delete `active_mb` measurement | 3 min Python on M5 S3 state (if still present) + CTAS re-do if cleaned | Confirms deletion-file size is ~same active/total |
| Flink compaction run with measure_active_size tracking | 15-30 min on EMR | Shows Lance+Flink's 2000-version accumulation in MVCC-aware terms |

---

## Permanent addition to the methodology playbook

Every future size claim must answer:
1. How many versions/snapshots does this dataset have?
2. Is the number cited `active_size` or `total_on_disk`?
3. If `total_on_disk`: is the comparison target also a multi-version dataset under the same pressure?

The `measure_active_size.py` script is the canonical tool going forward.

---

## Credits

User identified the issue:
> Lance 的机制是：每次 compact 都会生成新的版本文件，但是旧的版本不会删，因为是历史版本。如果你把历史版本都算进去，肯定是大啊。Lance 提供了版本回溯的功能，所以保留历史版本是正确的。你不能这么算。

Correct. The framing in the first version of the M6 writeup did exactly that mistake.
