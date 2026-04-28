import sys, time, json, urllib.request

JM = "http://localhost:18081"
out_path = sys.argv[1]
poll_secs = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0
target_jid = sys.argv[3] if len(sys.argv) > 3 else None

def fetch(p):
    try:
        with urllib.request.urlopen(JM + p, timeout=5) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"_error": str(e)}

print(f"Writing metrics to {out_path}, poll every {poll_secs}s, target={target_jid}")
f = open(out_path, "w")
f.write("ts,job_id,state,duration_ms,running_count,cp_completed,cp_failed,cp_inprogress,cp_last_dur_ms,cp_last_size,cp_total_count,num_cp_history\n")

restart_count = 0
prev_state = None
prev_running_ts = None
while True:
    ts = time.time()
    jobs = fetch("/jobs")
    if "_error" in jobs:
        f.write(f"{ts},,_ERROR,,,,,,,,,\n"); f.flush(); time.sleep(poll_secs); continue
    jlist = jobs.get("jobs", [])
    active = [j for j in jlist if target_jid is None or j["id"] == target_jid]
    if not active:
        f.write(f"{ts},,NO_JOB,,,,,,,,,\n"); f.flush(); time.sleep(poll_secs); continue
    for j in active:
        jid = j["id"]
        detail = fetch(f"/jobs/{jid}")
        if "_error" in detail:
            continue
        state = detail.get("state", "?")
        duration = detail.get("duration", 0)
        running_ts = detail.get("timestamps", {}).get("RUNNING", 0)
        if prev_running_ts is not None and running_ts > prev_running_ts:
            restart_count += 1
        prev_running_ts = running_ts

        cp = fetch(f"/jobs/{jid}/checkpoints")
        counts = cp.get("counts", {})
        completed = (cp.get("latest", {}) or {}).get("completed", {}) or {}
        history = cp.get("history", [])

        cp_completed = counts.get("completed", 0)
        cp_failed = counts.get("failed", 0)
        cp_inprogress = counts.get("in_progress", 0)
        cp_total = counts.get("total", 0)
        cp_last_dur = completed.get("end_to_end_duration", 0)
        cp_last_size = completed.get("state_size", 0)

        f.write(f"{ts},{jid},{state},{duration},{restart_count},{cp_completed},{cp_failed},{cp_inprogress},{cp_last_dur},{cp_last_size},{cp_total},{len(history)}\n")
        f.flush()
    time.sleep(poll_secs)
