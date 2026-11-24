"""Regression tests for code_task output-truncation handling.

A truncated reply used to come back as {"status": "TRUNCATED", "changes": []} -
a paid-for work order with nothing in it. This pins the replacement behaviour:
continue the reply, then keep every complete change that was written.
"""

import asyncio
import json
import os
import sys
import tempfile

os.environ["AGY_POOL_STATE"] = tempfile.mkdtemp(prefix="agy-trunc-")
sys.path.insert(0, r"C:\Users\b\.config\opencode\mcp")
import agy_pool as m  # noqa: E402

fails = []


def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (" | " + detail if detail else ""))
    if not ok:
        fails.append(name)


def run(coro):
    return asyncio.run(coro)


TRUNCATED_TWO_OK = (
    '{"analysis":"add the check",'
    '"changes":[{"path":"a.py","operation":"patch","search":"x","replace":"y"},'
    '{"path":"b.py","operation":"create","content":"print(1)\\n"}],'
    '"verification":["pytest"],"risks":[],"done":false'
)

CLEAN = json.dumps({"analysis": "a", "changes": [{"path": "c.py", "operation": "create",
                                                  "content": "pass\n"}],
                    "verification": [], "risks": [], "done": True})


def stub(return_text, truncated, continuations=1):
    calls = {"n": 0, "kwargs": []}

    async def fake(prompt_or_contents, model, system="", **kw):
        calls["n"] += 1
        calls["kwargs"].append(kw)
        return {"status": "OK", "text": return_text, "model": model,
                "account": "stub", "truncated": truncated,
                "finish_reason": "MAX_TOKENS" if truncated else "STOP",
                "continuations": continuations, "latency_ms": 1, "tokens": 10}

    return fake, calls


# 1. truncation with complete edits -> PARTIAL, edits kept
fake, calls = stub(TRUNCATED_TWO_OK, True)
m._chat_continue = fake
out = json.loads(run(m.code_task(goal="g", files=[])))
check("uses the continuing call", calls["n"] == 1)
check("continuation budget requested", calls["kwargs"][0].get("max_continuations") == 2,
      str(calls["kwargs"][0]))
check("partial status", out.get("status") == "PARTIAL", str(out.get("status")))
check("complete edits kept", len(out.get("changes") or []) == 2, str(len(out.get("changes") or [])))
check("note explains the cut", "max_tokens" in (out.get("note") or ""), out.get("note") or "")
check("done stays false", out.get("done") is False)

# 2. truncation before any complete edit -> TRUNCATED but with the raw tail
fake, _ = stub('{"analysis":"thinking","changes":[{"path":"a.py","operation":"create","content":"hal',
               True)
m._chat_continue = fake
out = json.loads(run(m.code_task(goal="g", files=[])))
check("truncated status", out.get("status") == "TRUNCATED", str(out.get("status")))
check("raw tail returned", bool(out.get("raw_text")), str(out.get("raw_text"))[:60])
check("hint kept", "smaller" in (out.get("hint") or ""), out.get("hint") or "")

# 3. a clean reply is untouched
fake, _ = stub(CLEAN, False, continuations=0)
m._chat_continue = fake
out = json.loads(run(m.code_task(goal="g", files=[])))
check("clean reply stays OK", out.get("status") == "OK", str(out.get("status")))
check("clean reply edit kept", len(out.get("changes") or []) == 1)
check("no note on a clean reply", "note" not in out)

# 4. the salvage scanner itself
sal = m._salvage_changes(TRUNCATED_TWO_OK + ',{"path":"c.py","operation":"crea')
check("salvage keeps 2 complete", len(sal["changes"]) == 2, str(len(sal["changes"])))
check("salvage reads analysis", sal["analysis"] == "add the check", sal["analysis"])
check("salvage on garbage is empty", m._salvage_changes("nope")["changes"] == [])

print()
if fails:
    print("FAILED: " + ", ".join(fails))
    sys.exit(1)
print("ALL TRUNCATION TESTS PASS")
