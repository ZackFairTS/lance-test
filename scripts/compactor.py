#!/usr/bin/env python3
"""Aggressively compact the Lance table in a tight loop."""
import os
import sys
import time
import json
import lance

s3_path = sys.argv[1]
log_path = sys.argv[2]
sleep_between = float(sys.argv[3]) if len(sys.argv) > 3 else 5.0

print(f"Compactor targeting {s3_path}, sleep {sleep_between}s between iters")
with open(log_path, "w") as f:
    f.write("iter,start_ts,end_ts,duration_s,fragments_removed,fragments_added,files_removed,error\n")
    iter_num = 0
    while True:
        iter_num += 1
        start = time.time()
        err = ""
        removed = added = files_removed = 0
        try:
            ds = lance.dataset(s3_path)
            metrics = ds.optimize.compact_files()
            removed = metrics.fragments_removed
            added = metrics.fragments_added
            files_removed = metrics.files_removed
        except Exception as e:
            err = repr(e).replace(",", ";")[:200]
        end = time.time()
        duration = end - start
        print(f"[{iter_num}] {duration:.2f}s removed={removed} added={added} err={err}")
        f.write(f"{iter_num},{start},{end},{duration:.3f},{removed},{added},{files_removed},{err}\n")
        f.flush()
        time.sleep(sleep_between)
