"""Regression tests for the ergonomics fixes: argument aliases, disk paths inside
files[], a retry when the model answers outside the JSON contract, and the code
stamp that says whether the running process has the patch.
"""

import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

os.environ["AGY_POOL_STATE"] = tempfile.mkdtemp(prefix="agy-ergo-")
sys.path.insert(0, r"C:\Users\b\.config\opencode\mcp")
import agy_pool as m  # noqa: E402

fails = []


def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (" | " + detail if detail else ""))
    if not ok:
        fails.append(name)


def run(coro):
    return asyncio.run(coro)


GOOD = json.dumps({"analysis": "a", "changes": [{"path": "x.py", "operation": "create",
                                                 "content": "print(1)\n"}],
                   "verification": [], "risks": [], "done": True})


def stub(replies):
    """Fake _chat_continue returning the given texts in order."""
    state = {"i": 0}

    async def fake(prompt_or_contents, model, system="", **kw):
        text = replies[min(state["i"], len(replies) - 1)]
        state["i"] += 1
        return {"status": "OK", "text": text, "model": model, "account": "stub",
                "truncated": False, "finish_reason": "STOP", "continuations": 0,
                "latency_ms": 1, "tokens": 5}

    # Some tools call _chat_once directly, others go through the continuing variant:
    # stub both so no test ever reaches the network.
    m._chat_continue = fake
    m._chat_once = fake
    return fake, state


root = Path(tempfile.mkdtemp(prefix="agy-ergo-files-"))
sample = root / "sample.py"
sample.write_text("def f():\n    return 'SAMPLE_MARKER'\n", encoding="utf-8")

# 1. work_order alias, files as a bare path string
fake, st = stub([GOOD])
m._chat_continue = fake
out = json.loads(run(m.code_task(work_order="do the thing", files=[str(sample)])))
check("work_order alias accepted", out.get("status") == "OK", str(out.get("status")))
check("bare path read from disk", st["i"] >= 1)

# 2. a {path} object with no content also means "read it"
fake, _ = stub([GOOD])
m._chat_continue = fake
out = json.loads(run(m.code_task(goal="g", files=[{"path": str(sample)}])))
check("{path} without content accepted", out.get("status") == "OK", str(out.get("status")))

# 3. unparsable first reply -> one cold retry -> OK
fake, st = stub(["Sure! Here is the code you asked for.", GOOD])
m._chat_continue = fake
out = json.loads(run(m.code_task(goal="g", files=[])))
check("retried once after prose", st["i"] == 2, "calls=" + str(st["i"]))
check("retry produced edits", out.get("status") == "OK" and len(out.get("changes") or []) == 1,
      str(out.get("status")))

# 4. unparsable twice -> UNPARSABLE with the raw tail, never an empty silence
fake, st = stub(["not json", "still not json"])
m._chat_continue = fake
out = json.loads(run(m.code_task(goal="g", files=[])))
check("unparsable reported", out.get("status") == "UNPARSABLE", str(out.get("status")))
check("raw tail present", "still not json" in (out.get("raw_text") or ""), out.get("raw_text") or "")

# 5. the model answers with a bare array of changes: wrap, do not fail
bare = json.dumps([{"path": "a.py", "operation": "create", "content": "x"}])
fake, _ = stub([bare])
m._chat_continue = fake
out = json.loads(run(m.code_task(goal="g", files=[])))
check("bare array wrapped", out.get("status") in ("OK", "PARTIAL")
      and len(out.get("changes") or []) == 1, str(out.get("status")))

# 6. review accepts the same alias and a disk path
fake, st = stub([json.dumps({"verdict": "PASS", "confidence": 0.9, "problems": [],
                             "next_steps": [], "done": True})])
m._chat_continue = fake
out = json.loads(run(m.review(work_order="check it", paths=[str(sample)])))
check("review takes work_order", out.get("verdict") == "PASS", str(out)[:120])

# 7. status carries the code stamp so "is my patch live?" is answerable
st_json = json.loads(m.status())
check("status has server_version", bool(st_json.get("server_version")), str(st_json.get("server_version")))
code = st_json.get("code") or {}
check("status has code stamp", bool(code.get("sha256_12")), str(code))
check("restart_needed is a bool", isinstance(code.get("restart_needed"), bool), str(code.get("restart_needed")))

print()
if fails:
    print("FAILED: " + ", ".join(fails))
    sys.exit(1)
print("ALL ERGONOMICS TESTS PASS")
