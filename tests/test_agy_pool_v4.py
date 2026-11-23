"""Tests for the activity-file layer + CORS (needed by the DSH sidebar panel)."""
import json
import os
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

TMP = Path(tempfile.mkdtemp(prefix="agy-v4-"))
os.environ["AGY_POOL_STATE"] = str(TMP)
os.environ["AGY_POOL_MIN_INTERVAL_SEC"] = "0"
os.environ["AGY_POOL_JITTER_SEC"] = "0"
_HERE = Path(__file__).resolve()
for _cand in (_HERE.parent.parent, Path(r"C:\Users\b\.config\opencode\mcp")):
    if (_cand / "agy_pool.py").exists():
        sys.path.insert(0, str(_cand)); break
import agy_pool as m  # noqa: E402

fails = []
def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (" | " + detail if detail else ""))
    if not ok:
        fails.append(name)

# 1. activity file is written for this process
cid = m.ACTIVITY.begin("a@x", "acc1", "high")
path = TMP / f"activity-{os.getpid()}.json"
check("activity file written", path.exists(), str(path))
if path.exists():
    saved = json.loads(path.read_text(encoding="utf-8"))
    check("file has active call", len(saved.get("active") or []) == 1)
    check("id is process scoped", str(saved["active"][0]["id"]).startswith(f"{os.getpid()}-"))
m.ACTIVITY.end(cid, 200)

# 2. another LIVE process's file is merged in (dead pids are pruned on purpose)
import subprocess
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
other = TMP / f"activity-{child.pid}.json"
other.write_text(json.dumps({
    "updated_at": time.time(), "started_at": time.time() - 100,
    "version": "x", "stats": {"ok": 5, "fail": 1, "calls": 6, "inflight": 0},
    "accounts": [{"email": "b@y", "state": "cooling", "cooldown_s": 42,
                  "requests": 3, "active": 0}],
    "active": [{"id": "999999-1", "email": "b@y", "model": "m", "step": "sending",
                "started_at": time.time() - 2, "elapsed_s": 2}],
    "recent": [{"at": time.time() - 5, "email": "b@y", "model": "m", "ms": 900,
                "status": "OK", "detail": ""}],
}), encoding="utf-8")
snap = m._activity_snapshot()
emails = {a["email"] for a in snap["accounts"]}
check("merges other process accounts", {"a@x", "b@y"} <= emails, str(emails))
check("process count", snap.get("processes") == 2, str(snap.get("processes")))
check("inflight merged", snap["stats"]["inflight"] >= 1, str(snap["stats"]))
check("recent merged", any(r["email"] == "b@y" for r in snap["recent"]))
check("cooldown visible", any(a["state"] == "cooling" and a["cooldown_s"] > 0
                              for a in snap["accounts"]))

# 3. CORS headers + OPTIONS
check("dashboard starts", m._start_dashboard(0) is True)
port = m._DASHBOARD_SERVER.server_address[1]
req = urllib.request.Request(f"http://127.0.0.1:{port}/activity")
with urllib.request.urlopen(req, timeout=5) as r:
    check("CORS allow-origin", r.headers.get("Access-Control-Allow-Origin") == "*",
          str(r.headers.get("Access-Control-Allow-Origin")))
    body = json.loads(r.read())
check("activity json parse", "accounts" in body)

req = urllib.request.Request(f"http://127.0.0.1:{port}/activity", method="OPTIONS")
with urllib.request.urlopen(req, timeout=5) as r:
    check("OPTIONS 204 + cors", r.status == 204
          and r.headers.get("Access-Control-Allow-Origin") == "*")

m._DASHBOARD_SERVER.shutdown()
child.kill()
print("\nRESULT:", "ALL PASS" if not fails else f"{len(fails)} FAILED -> {fails}")
sys.exit(1 if fails else 0)
