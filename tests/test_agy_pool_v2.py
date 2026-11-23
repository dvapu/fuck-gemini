"""Tests for the v2 additions: chunking, map/reduce, activity, dashboard."""
import asyncio
import json
import os
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

os.environ["AGY_POOL_STATE"] = tempfile.mkdtemp(prefix="agy-v2-")
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


text = ("\n\n".join("Paragraph %d. " % i + "word " * 60 for i in range(400)))
chunks = m._chunk_text(text, 5000)
check("chunking loses nothing", "".join(chunks) == text, f"{len(chunks)} chunks")
check("chunk size respected", all(len(c) <= 5000 for c in chunks),
      f"max={max(len(c) for c in chunks)}")
check("no empty chunks", all(c.strip() for c in chunks))
check("boundary aware", all(c.endswith(("\n\n", "\n", " ")) or len(c) == 5000 for c in chunks[:-1]))

check("json trailing comma repaired", m._safe_json('{"a": 1, "b": [2,3,],}') == {"a": 1, "b": [2, 3]})

calls = {"n": 0}


async def fake_chat(prompt, model, **kw):
    calls["n"] += 1
    return {"status": "OK", "text": "PART" + str(calls["n"]), "model": model}


async def main():
    m._chat_once = fake_chat
    small = "x" * 100
    big = "y" * 15000
    res = await m._map_reduce([small, big], "summarize:", "high", "combine:", chunk_chars=6000)
    check("map_reduce status OK", res["status"] == "OK")
    check("map_reduce split the big item", calls["n"] >= 4, f"calls={calls['n']}")
    check("map_reduce reports parts", len(res["parts"]) == 2, str(res["parts"]))
    check("map_reduce reduce ran", res["text"].startswith("PART"), str(res["text"])[:20])

    # activity lifecycle
    cid = m.ACTIVITY.begin("a@x", "acc1", "high")
    snap = m.ACTIVITY.snapshot()
    check("activity shows active", len(snap["active"]) == 1
          and snap["active"][0]["step"] == "pacing")
    m.ACTIVITY.step(cid, "sending")
    snap = m.ACTIVITY.snapshot()
    check("activity step updates", snap["accounts"][0]["state"] == "sending",
          str(snap["accounts"]))
    m.ACTIVITY.end(cid, 200)
    snap = m.ACTIVITY.snapshot()
    check("activity moves to recent", not snap["active"]
          and snap["recent"][-1]["status"] == "OK")
    check("stats carry inflight", "inflight" in snap["stats"])

    # dashboard server + routes
    ok = m._start_dashboard(0)
    check("dashboard starts", ok is True)
    port = m._DASHBOARD_SERVER.server_address[1]
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/activity", timeout=5) as r:
        data = json.loads(r.read())
    check("dashboard /activity json", data.get("version") and "accounts" in data)
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=5) as r:
        html = r.read().decode()
    check("dashboard / html", "Gemini pool" in html and "/activity" in html)
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=5) as r:
        check("dashboard healthz", r.read() == b"ok")
    check("dashboard restart is idempotent", m._start_dashboard(0) is True)


asyncio.run(main())
m._DASHBOARD_SERVER.shutdown()
print("\nRESULT:", "ALL PASS" if not fails else f"{len(fails)} FAILED -> {fails}")
sys.exit(1 if fails else 0)
