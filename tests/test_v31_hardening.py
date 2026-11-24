"""v3.1 hardening: tolerant JSON, thread-safe failover, safe truncation, progress."""
import asyncio
import concurrent.futures
import importlib.util
import json
import os
import sys
import tempfile
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("AGY_POOL_STATE", tempfile.mkdtemp(prefix="agy-v31-"))
import agy_pool as m  # noqa: E402

fails = []
def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (" | " + detail if detail else ""))
    if not ok:
        fails.append(name)


def test_lenient_parser():
    raw = '''{
 "analysis": "create file",
 "changes": [{
   "path": "C:\\new\\project\\demo.py",
   "operation": "create",
   "content": "def f(value):
    \"\"\"doc\"\"\"
    pattern = r\"[\\d\\s]+\"
    return f\"{value}\""
 }],
 "verification": ["python demo.py"], "risks": [], "done": true
}'''
    parsed = m._parse_code_task_lenient(raw)
    check("lenient parser recovers dirty JSON", bool(parsed and parsed.get("changes")))
    if parsed and parsed.get("changes"):
        item = parsed["changes"][0]
        check("Windows path preserved", item["path"] == r"C:\new\project\demo.py", item["path"])
        content = item["content"]
        check("triple quotes preserved", '"""doc"""' in content)
        check("f-string preserved", 'f"{value}"' in content)
        check("regex escapes preserved", r"[\d\s]+" in content)
        check("complete dirty object keeps done", parsed.get("done") is True)


def test_lenient_truncation():
    first = '{"path":"a.py","operation":"create","content":"print(1)"}'
    raw = '{"analysis":"x","changes":[' + first + ',{"path":"b.py","operation":"create","content":"def broken():'
    parsed = m._parse_code_task_lenient(raw)
    check("truncated second change salvages first", bool(parsed and len(parsed.get("changes", [])) == 1))
    check("salvage forces done false", parsed is not None and parsed.get("done") is False)


def test_endpoint_thread_safety():
    manager = m.EndpointFailoverManager()
    for _ in range(3):
        manager.note_failure(manager.DAILY_URL, 503)
    manager._cooldown_until = time.time() - 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        endpoints = list(pool.map(lambda _: manager.acquire_endpoint(), range(20)))
    check("half-open concurrent acquire has one daily probe",
          endpoints.count(manager.DAILY_URL) == 1 and endpoints.count(manager.PROD_URL) == 19,
          str({e: endpoints.count(e) for e in set(endpoints)}))
    for _ in range(3):
        manager.note_success(manager.DAILY_URL)
        if manager.snapshot()["state"] == "HALF_OPEN":
            manager.acquire_endpoint()
    check("three sequential probes close circuit", manager.snapshot()["state"] == "CLOSED")


def test_synthetic_no_concat():
    calls = {"n": 0}
    async def fake(*args, **kwargs):
        calls["n"] += 1
        return {"status": "OK", "text": '{"changes":[', "truncated": True,
                "finish_reason": "MAX_TOKENS", "function_call": {"name": "apply_code_edits", "args": {}},
                "account": "a", "model": "m"}
    original = m._chat_once
    m._chat_once = fake
    try:
        out = asyncio.run(m._chat_continue("x", "lite", synthetic_code_tool=True))
    finally:
        m._chat_once = original
    check("truncated functionCall is not concatenated", calls["n"] == 1 and out.get("continuations") == 0)
    check("truncated functionCall flagged", out.get("synthetic_tool_truncated") is True)


def test_structured_json_no_concat():
    calls = {"n": 0}
    async def fake(*args, **kwargs):
        calls["n"] += 1
        return {"status": "OK", "text": '{"changes":[', "truncated": True,
                "finish_reason": "MAX_TOKENS", "account": "a", "model": "m"}
    original = m._chat_once
    m._chat_once = fake
    try:
        out = asyncio.run(m._chat_continue("x", "lite", json_mode=True))
    finally:
        m._chat_once = original
    check("truncated structured JSON is not concatenated", calls["n"] == 1 and out.get("continuations") == 0)
    check("structured truncation flagged", out.get("structured_output_truncated") is True)


def test_code_task_truncated_status():
    async def fake(*args, **kwargs):
        return {"status": "OK", "text": '{"changes":[{"path":"a.py","content":"cut',
                "truncated": True, "finish_reason": "MAX_TOKENS", "account": "a", "model": "lite"}
    original = m._chat_continue
    m._chat_continue = fake
    try:
        out = json.loads(asyncio.run(m.code_task(goal="x", model="lite", max_tokens=128)))
    finally:
        m._chat_continue = original
    check("code_task truncation is structured", out.get("status") == "TRUNCATED", out.get("status"))
    check("truncation never returns partial change", not out.get("changes"))


def test_code_task_lenient_integration():
    dirty = '''{"analysis":"x","changes":[{"path":"a.py","operation":"create","content":"def f():
    \\\"\\\"\\\"doc\\\"\\\"\\\"
    return 1"}],"verification":[],"risks":[],"done":true}'''
    async def fake(*args, **kwargs):
        return {"status": "OK", "text": dirty, "truncated": False,
                "finish_reason": "STOP", "account": "a", "model": "lite"}
    original = m._chat_continue
    m._chat_continue = fake
    try:
        out = json.loads(asyncio.run(m.code_task(goal="x", model="lite")))
    finally:
        m._chat_continue = original
    check("code_task accepts repaired complete JSON", out.get("status") in ("OK", "PARTIAL") and len(out.get("changes", [])) == 1, out.get("status"))


def test_progress_heartbeat():
    class FakeCtx:
        def __init__(self): self.calls = []
        async def report_progress(self, progress, total=None, message=None):
            self.calls.append((progress, message))
    async def run():
        ctx = FakeCtx()
        async with m._progress_heartbeat(ctx, "working", interval=0.01):
            await asyncio.sleep(0.035)
        await asyncio.sleep(0.02)
        return ctx.calls
    calls = asyncio.run(run())
    check("progress heartbeat emits and stops", len(calls) >= 2, str(len(calls)))
    async def no_ctx():
        async with m._progress_heartbeat(None, "working", interval=0.01):
            await asyncio.sleep(0.01)
    asyncio.run(no_ctx())
    check("missing context is safe", True)


def test_sticky_handover():
    sid = "handover-test"
    m._SESSIONS[sid] = {
        "contents": [{"role": "user", "parts": [{"text": "hi"}]},
                     {"role": "model", "parts": [{"text": "hello"}]}],
        "system": "", "model": "gemini-3.5-flash-lite", "created": time.time(),
        "last_used": time.time(), "turns": 1, "chars_dropped": 0,
        "pinned_account": "account-A",
    }
    seen = []
    async def fake(contents, model, system="", **kwargs):
        seen.append(kwargs.get("preferred_account"))
        return {"status": "OK", "text": "ok", "account": "account-B", "model": model}
    original = m._chat_once
    m._chat_once = fake
    try:
        asyncio.run(m.session_send(sid, "next"))
        check("session handover updates pin", m._SESSIONS[sid]["pinned_account"] == "account-B")
        asyncio.run(m.session_send(sid, "again"))
        check("next turn prefers new account", seen[-1] == "account-B", str(seen))
    finally:
        m._chat_once = original
        m._SESSIONS.pop(sid, None)
        m._SESSION_LOCKS.pop(sid, None)


def test_dsh_safe_output():
    original = m.DSH_SAFE
    m.DSH_SAFE = True
    try:
        payload = {"status": "OK", "text": "tiếng Việt\nline2", "parts": [{"thoughtSignature": "x"}],
                   "raw_text": "x" * 5000}
        out = m._ok(payload)
        parsed = json.loads(out)
        check("DSH-safe output is ASCII", out.isascii())
        check("DSH-safe output has no raw controls", all(ch not in out for ch in "\n\r\t"))
        check("DSH-safe output drops protocol metadata", "parts" not in parsed)
        check("DSH-safe raw_text capped", len(parsed["raw_text"]) <= 1200)
    finally:
        m.DSH_SAFE = original


test_lenient_parser()
test_lenient_truncation()
test_endpoint_thread_safety()
test_synthetic_no_concat()
test_structured_json_no_concat()
test_code_task_truncated_status()
test_code_task_lenient_integration()
test_progress_heartbeat()
test_sticky_handover()
test_dsh_safe_output()

print("\nRESULT:", "ALL PASS" if not fails else f"FAILED {fails}")
sys.exit(1 if fails else 0)
