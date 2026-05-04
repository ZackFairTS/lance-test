"""M4: filter-pushdown (selective scan) throughput, Lance scalar index vs
Iceberg Parquet min/max statistics.

This benchmark is deliberately an "each format uses its native pushdown
primitive" comparison:
  - Lance: BITMAP scalar index on the filter column (built in-place).
  - Iceberg: row-group min/max statistics (Parquet default, written by M1).
    Bloom filters are NOT enabled here because M1 did not set
    `write.parquet.bloom-filter-enabled.column.*`; turning them on would
    require rewriting the Iceberg table. The honest production-default
    comparison (no bloom) is more informative for most users.

Fairness note on row-group min/max effectiveness: for UNCLUSTERED columns
(e.g., TPC-DS ss_quantity, uniform in [1..100]), every Parquet row group
has [min=1, max=100] so min/max provides ZERO pruning -- Iceberg's native
pushdown reduces to "read all row groups, filter in memory". Lance's
BITMAP is still doing real skipping. Results will reflect this reality,
not "Parquet stats are slow" vs "Lance is fast". To exercise min/max
pruning properly, run with --filter-column on a clustered/sorted column.

Selectivity sweep: 0.01% / 0.1% / 1% / 10% / 50%. Predicate form is RANGE
(`ss_quantity <= K`) not equality. For uniform 1..100 ss_quantity, every
equality predicate hits ~1% -- claimed "0.01%" and "50%" labels would be
lies. A range on an integer column is supported identically by both
engines: Lance filter string `"ss_quantity <= K"`, pyiceberg
`LessThanOrEqual("ss_quantity", K)`.

Why Python + Arrow for both sides (same reasoning as M3): lance-spark
0.0.15 DSv2 read path is broken on Spark 3.5.5.

Index strategy: for each Lance dataset, create an in-place BITMAP index
the first time we benchmark a new filter column; skip if it exists. The
index lives in `_indices/` under the Lance dataset; it persists across
M4 runs and is visible to M5/M6.

Output: results/M4_filter_sf<N>.json.
"""
import argparse
import gc
import json
import os
import statistics
import time

import lance
import pyarrow.compute as pc
from pyiceberg.expressions import LessThanOrEqual
from pyiceberg.table import StaticTable


WARMUP = 3
ROUNDS = 7
FILTER_COLUMN = "ss_quantity"
FILTER_TABLE = "store_sales"

SELECTIVITY_TARGETS = {
    "0.01%": 0.0001,
    "0.1%":  0.001,
    "1%":    0.01,
    "10%":   0.10,
    "50%":   0.50,
}
CALIBRATION_TOLERANCE = 0.3


def parse_run_env(path="/home/hadoop/lance-extended-bench/run.env"):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.split("#", 1)[0].strip()
            out[k.strip()] = v
    return out


def _stats(runs):
    out = {
        "median_ms": statistics.median(runs) * 1000,
        "mean_ms":   statistics.mean(runs) * 1000,
        "min_ms":    min(runs) * 1000,
        "max_ms":    max(runs) * 1000,
        "stdev_ms": (statistics.stdev(runs) * 1000
                     if len(runs) > 1 else 0.0),
        "runs_ms":  [round(r * 1000, 2) for r in runs],
    }
    return {k: (round(v, 3) if isinstance(v, float) else v)
            for k, v in out.items()}


def timed_materialized(action_builder, warmup=WARMUP, rounds=ROUNDS):
    for _ in range(warmup):
        action = action_builder()
        _ = action()
        gc.collect()
    runs = []
    last_rows = None
    out = None
    for _ in range(rounds):
        out = None
        gc.collect()
        action = action_builder()
        t0 = time.perf_counter()
        out = action()
        dt = time.perf_counter() - t0
        runs.append(dt)
        last_rows = out.num_rows if out is not None else None
    out = None
    stats = _stats(runs)
    stats["rows_returned"] = last_rows
    return stats


def probe_lance_index(uri, storage_options, column):
    ds = lance.dataset(uri, storage_options=storage_options)
    existing = []
    for info in ds.list_indices():
        if column in info.get("fields", []):
            t = info.get("type")
            if t:
                existing.append(t)
    return sorted(set(existing))


def ensure_lance_bitmap_index(uri, storage_options, column):
    """Build a BITMAP scalar index on `column` if not already present.
    Idempotent across M4 runs. If a non-Bitmap scalar index already exists
    on the column, leave it alone and report -- don't silently replace
    (E-class silent-overwrite defense) and don't raise (M5/M6 may have
    built BTREE/ZONEMAP for other experiments).
    """
    existing = probe_lance_index(uri, storage_options, column)
    if "Bitmap" in existing:
        return {"built": False, "existing_types": existing}
    if existing:
        return {"built": False, "existing_types": existing,
                "skipped_reason": "non-bitmap index present; not replacing"}
    ds = lance.dataset(uri, storage_options=storage_options)
    t0 = time.perf_counter()
    ds.create_scalar_index(column, index_type="BITMAP", replace=False)
    built_sec = round(time.perf_counter() - t0, 3)
    return {"built": True, "build_seconds": built_sec,
            "existing_types": probe_lance_index(uri, storage_options, column)}


def calibrate_predicates(iceberg_metadata_uri, column, n_total, targets,
                         tolerance=CALIBRATION_TOLERANCE):
    """Scan the column, compute CUMULATIVE frequency per value, and pick
    a range predicate `column <= K` per target selectivity so claimed
    selectivities match actual selectivity within ``tolerance``.

    Range-not-equality is required because uniform distributions like
    TPC-DS `ss_quantity` give every equality predicate ~1% selectivity,
    which would silently falsify 0.01% / 10% / 50% labels (review B1).
    """
    tbl = StaticTable.from_metadata(iceberg_metadata_uri)
    col_tbl = tbl.scan(selected_fields=(column,)).to_arrow()
    values = col_tbl.column(column)
    counts = pc.value_counts(values).to_pylist()
    ordered = sorted(
        ((vc["values"], vc["counts"]) for vc in counts
         if vc["values"] is not None),
        key=lambda t: t[0])
    cum = []
    running = 0
    for v, c in ordered:
        running += c
        cum.append((v, running))
    if not cum:
        raise RuntimeError(f"column {column!r} has no non-null values")

    picks = {}
    for label, target in targets.items():
        target_count = max(1, int(round(target * n_total)))
        best_v, best_running = min(cum, key=lambda p: abs(p[1] - target_count))
        actual = best_running / n_total
        feasible = abs(actual - target) / max(target, 1e-9) <= tolerance
        picks[label] = {
            "predicate_k": best_v,
            "actual_rows": int(best_running),
            "actual_selectivity": actual,
            "target_selectivity": target,
            "feasible": feasible,
        }
    return picks


def lance_filter_action(uri, storage_options, column, k_value, projected):
    def do_read():
        ds = lance.dataset(uri, storage_options=storage_options)
        return ds.to_table(columns=projected,
                           filter=f"{column} <= {int(k_value)}")
    return do_read


def iceberg_filter_action(metadata_uri, column, k_value, projected):
    def do_read():
        tbl = StaticTable.from_metadata(metadata_uri)
        return (tbl.scan(selected_fields=tuple(projected),
                         row_filter=LessThanOrEqual(column, int(k_value)))
                   .to_arrow())
    return do_read


def find_iceberg_metadata_uri(region, data_uri, timeout_s=60):
    import subprocess
    rel = data_uri[len("s3://"):]
    bucket, _, key = rel.partition("/")
    hint_key = f"{key.rstrip('/')}/metadata/version-hint.text"
    try:
        r = subprocess.run(
            ["aws", "s3", "cp", f"s3://{bucket}/{hint_key}", "-",
             "--region", region],
            check=True, capture_output=True, text=True, timeout=timeout_s)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"aws s3 cp version-hint failed (rc={e.returncode}): "
            f"{e.stderr[:400]}") from e
    version = r.stdout.strip()
    if not version.isdigit():
        raise RuntimeError(f"bad version-hint: {version!r}")
    return f"s3://{bucket}/{key.rstrip('/')}/metadata/v{version}.metadata.json"


PROJECTED_COLS = ["ss_item_sk", "ss_customer_sk", "ss_quantity",
                  "ss_sales_price"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m1-manifest", required=True)
    ap.add_argument("--region", default=None)
    ap.add_argument("--out", default=None)
    ap.add_argument("--skip-index-build", action="store_true",
                    help="do not create Lance BITMAP index even if missing")
    ap.add_argument("--filter-column", default=FILTER_COLUMN)
    ap.add_argument("--filter-table", default=FILTER_TABLE)
    args = ap.parse_args()

    with open(args.m1_manifest) as f:
        m1 = json.load(f)
    env = parse_run_env()
    if args.region is None:
        args.region = (os.environ.get("AWS_REGION")
                       or m1.get("region") or env.get("AWS_REGION"))
    if not args.region:
        raise SystemExit("AWS_REGION not found.")
    os.environ["AWS_REGION"] = args.region

    scale = m1.get("scale")
    if scale is None:
        raise SystemExit("M1 manifest missing 'scale'")
    if args.out is None:
        args.out = (f"/home/hadoop/lance-extended-bench/results/"
                    f"M4_filter_sf{scale}.json")

    print(f"[M4] m1 manifest: {args.m1_manifest}")
    print(f"[M4] scale=sf{scale}  region={args.region}")
    print(f"[M4] filter: {args.filter_table}.{args.filter_column} = <value>")

    by_table = {}
    for rec in m1["records"]:
        if not rec.get("ok"):
            continue
        by_table.setdefault(rec["table"], {})[rec["format"]] = rec

    if args.filter_table not in by_table:
        raise SystemExit(f"Filter table {args.filter_table!r} not in M1 manifest")
    fmts = by_table[args.filter_table]
    if "lance_2.2" not in fmts or "iceberg_v2" not in fmts:
        raise SystemExit(
            f"Need both lance_2.2 and iceberg_v2 for {args.filter_table}, "
            f"have {list(fmts)}")

    lance_uri = fmts["lance_2.2"]["uri"]
    iceberg_data_uri = fmts["iceberg_v2"]["data_uri"]
    storage_options = {"region": args.region}

    meta_uri = find_iceberg_metadata_uri(args.region, iceberg_data_uri)
    print(f"[M4] iceberg metadata: {meta_uri}")

    ds_probe = lance.dataset(lance_uri, storage_options=storage_options)
    n_total = ds_probe.count_rows()
    print(f"[M4] n_total={n_total}")

    index_info = {"requested_build": not args.skip_index_build,
                  "column": args.filter_column}
    if not args.skip_index_build:
        print(f"[M4] Ensuring Lance BITMAP index on {args.filter_column} ...")
        idx = ensure_lance_bitmap_index(lance_uri, storage_options,
                                        args.filter_column)
        index_info.update(idx)
        if idx.get("built"):
            print(f"  built in {idx['build_seconds']}s")
        elif idx.get("skipped_reason"):
            print(f"  skipped: {idx['skipped_reason']} "
                  f"(existing {idx['existing_types']})")
        else:
            print(f"  already exists: {idx['existing_types']}")
    else:
        existing = probe_lance_index(lance_uri, storage_options,
                                     args.filter_column)
        index_info["existing_types"] = existing
        index_info["built"] = False
        print(f"[M4] --skip-index-build: Lance index state on "
              f"{args.filter_column}: {existing or 'NONE'}")
    index_info["bitmap_present_at_runtime"] = \
        "Bitmap" in index_info.get("existing_types", [])

    print("[M4] calibrating predicates for target selectivities ...")
    picks = calibrate_predicates(meta_uri, args.filter_column,
                                 n_total, SELECTIVITY_TARGETS)
    for label, pick in picks.items():
        marker = "" if pick["feasible"] else "  !! INFEASIBLE (tolerance exceeded)"
        print(f"  {label:>6}: {args.filter_column} <= {pick['predicate_k']} "
              f"-> {pick['actual_rows']:,} rows "
              f"(actual {pick['actual_selectivity']:.6f} vs target "
              f"{pick['target_selectivity']:.6f}){marker}")

    results = []
    for label, pick in picks.items():
        if not pick["feasible"]:
            print(f"\n[M4] skipping {label}: infeasible target on this data")
            results.append({
                "label": label,
                "target_selectivity": pick["target_selectivity"],
                "actual_selectivity": pick["actual_selectivity"],
                "predicate_k": pick["predicate_k"],
                "expected_rows": pick["actual_rows"],
                "skipped": "infeasible_target",
            })
            continue
        k_value = pick["predicate_k"]
        print(f"\n[M4] === selectivity={label} "
              f"(k={k_value}, {pick['actual_rows']:,} rows target) ===")
        sel_rec = {
            "label": label,
            "target_selectivity": pick["target_selectivity"],
            "actual_selectivity": pick["actual_selectivity"],
            "predicate_k": int(k_value),
            "expected_rows": pick["actual_rows"],
            "formats": {},
        }

        try:
            stats = timed_materialized(lambda k=k_value: lance_filter_action(
                lance_uri, storage_options, args.filter_column,
                k, PROJECTED_COLS))
            sel_rec["formats"]["lance_2.2"] = stats
            print(f"  [lance_2.2]  p50={stats['median_ms']:>9.2f} ms  "
                  f"rows={stats.get('rows_returned')}")
            if stats.get("rows_returned") != pick["actual_rows"]:
                print(f"  !! lance row-count mismatch: "
                      f"got {stats.get('rows_returned')} "
                      f"expected {pick['actual_rows']}")
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            if isinstance(e, (SystemExit, GeneratorExit)):
                raise
            sel_rec["formats"]["lance_2.2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  lance FAILED: {sel_rec['formats']['lance_2.2']['error']}")

        try:
            stats = timed_materialized(lambda k=k_value: iceberg_filter_action(
                meta_uri, args.filter_column, k, PROJECTED_COLS))
            sel_rec["formats"]["iceberg_v2"] = stats
            print(f"  [iceberg_v2] p50={stats['median_ms']:>9.2f} ms  "
                  f"rows={stats.get('rows_returned')}")
            if stats.get("rows_returned") != pick["actual_rows"]:
                print(f"  !! iceberg row-count mismatch: "
                      f"got {stats.get('rows_returned')} "
                      f"expected {pick['actual_rows']}")
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            if isinstance(e, (SystemExit, GeneratorExit)):
                raise
            sel_rec["formats"]["iceberg_v2"] = {
                "error": f"{type(e).__name__}: {e}"[:400]}
            print(f"  iceberg FAILED: {sel_rec['formats']['iceberg_v2']['error']}")

        results.append(sel_rec)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tmp = args.out + ".tmp"
    with open(tmp, "w") as f:
        json.dump({
            "scale": scale,
            "region": args.region,
            "m1_manifest": os.path.abspath(args.m1_manifest),
            "filter_table": args.filter_table,
            "filter_column": args.filter_column,
            "projected_columns": PROJECTED_COLS,
            "selectivity_targets": SELECTIVITY_TARGETS,
            "warmup": WARMUP,
            "rounds": ROUNDS,
            "engine": "python+arrow (pylance + pyiceberg native)",
            "lance_index": index_info,
            "n_total": n_total,
            "lance_version": lance.__version__,
            "results": results,
        }, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, args.out)
    print(f"\n[M4] Saved: {args.out}")

    print("\n=== M4 filter p50 ms by selectivity ===")
    print(f"{'selectivity':<12} {'lance_p50':>12} {'iceberg_p50':>14} "
          f"{'ratio':>8}  expected_rows")
    for r in results:
        if r.get("skipped"):
            print(f"  {r['label']:<10} {'SKIPPED':>12} {r['skipped']:>14}")
            continue
        l = r["formats"].get("lance_2.2", {})
        i = r["formats"].get("iceberg_v2", {})
        lm = l.get("median_ms")
        im = i.get("median_ms")
        lm_s = f"{lm:.2f}" if lm is not None else "--"
        im_s = f"{im:.2f}" if im is not None else "--"
        ratio = (f"{lm/im:.2f}x" if lm is not None and im and im > 0
                 else "--")
        print(f"  {r['label']:<10} {lm_s:>12} {im_s:>14} {ratio:>8}  "
              f"{r['expected_rows']:,}")


if __name__ == "__main__":
    main()
