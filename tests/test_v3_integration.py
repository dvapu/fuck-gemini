"""Direct integration tests for agy_pool v3 enhancements."""
import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

# Ensure E:\linhtinh\fuck-gemini is at the top of sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

os.environ["AGY_POOL_STATE"] = tempfile.mkdtemp(prefix="agy-v3-integ-")
import agy_pool as m

fails = []

def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (" | " + detail if detail else ""))
    if not ok:
        fails.append(name)

def run(coro):
    return asyncio.run(coro)

print("=== RUNNING V3 INTEGRATION TESTS ===")

# 1. Waterfilling Quota Router in Pool._pick_order
# a1 has 80% quota, a2 has 20% quota, a3 has 2% quota (< 5% emergency reserve)
acc1 = m.Account({"id": "a1", "quota": {"quota_groups": [{"buckets": [{"bucket_id": "gemini-5h", "remaining_fraction": 0.8}]}]}}, Path("a1.json"))
acc2 = m.Account({"id": "a2", "quota": {"quota_groups": [{"buckets": [{"bucket_id": "gemini-5h", "remaining_fraction": 0.2}]}]}}, Path("a2.json"))
acc3 = m.Account({"id": "a3", "quota": {"quota_groups": [{"buckets": [{"bucket_id": "gemini-5h", "remaining_fraction": 0.02}]}]}}, Path("a3.json"))

order = m.POOL._pick_order([acc2, acc3, acc1], model="gemini-3.8-flash-high")
check("Waterfilling prioritizes higher quota (a1 before a2)", order[0].id == "a1" and order[1].id == "a2")
check("Waterfilling reserves < 5% quota for emergency (a3 last)", order[2].id == "a3")

# Sticky session overrides order if preferred account is ready (by id or by email)
sticky_order = m.POOL._pick_order([acc2, acc3, acc1], model="gemini-3.8-flash-high", preferred_acc_id="a2")
check("Sticky session affinity pins preferred account first", sticky_order[0].id == "a2")

# Real-world sticky session test: preferred_acc_id is an email address while account has UUID id
acc_uuid = m.Account({"id": "uuid-999", "email": "dev@corp.com"}, Path("uuid-999.json"))
order_email = m.POOL._pick_order([acc1, acc_uuid], model="gemini-3.8-flash-high", preferred_acc_id="dev@corp.com")
check("Sticky session affinity pins by email address when id is UUID", order_email[0].id == "uuid-999")

# If all normal accounts are cooling, emergency accounts are utilized
m.POOL._penalize(acc1, 600, "gemini-3.8-flash-high")
m.POOL._penalize(acc2, 600, "gemini-3.8-flash-high")
fallback_order = m.POOL._pick_order([acc1, acc2, acc3], model="gemini-3.8-flash-high")
check("Emergency account serves when normal accounts are cooling", fallback_order[0].id == "a3")

# Clean up cooldowns
m.POOL.note_success(acc1, "gemini-3.8-flash-high")
m.POOL.note_success(acc2, "gemini-3.8-flash-high")

# 2. Sticky Session in session_new and session_send
async def test_session_lifecycle():
    state = {"called_accounts": []}
    
    async def fake_chat_once(contents, model, system="", preferred_account=None, **kw):
        acct = preferred_account if preferred_account else "acc_pinned_1"
        state["called_accounts"].append(acct)
        return {
            "status": "OK",
            "text": "Hello turn",
            "account": acct,
            "parts": [{"thought": True, "text": "Reasoning...", "thoughtSignature": "SIG123"},
                      {"text": "Hello turn"}],
            "model": model,
            "latency_ms": 10
        }
    
    orig_chat_once = m._chat_once
    m._chat_once = fake_chat_once
    try:
        # session_new
        new_res = json.loads(await m.session_new(first_message="Hi turn 1"))
        sess_id = new_res["session_id"]
        sess = m._session_get(sess_id)
        check("session_new pins account", sess.get("pinned_account") == "acc_pinned_1")
        check("session_new preserves thoughtSignature in parts", 
              sess["contents"][1]["parts"][0].get("thoughtSignature") == "SIG123")
        
        # session_send turn 2
        send_res = json.loads(await m.session_send(session_id=sess_id, message="Hi turn 2"))
        check("session_send used preferred_account", state["called_accounts"][-1] == "acc_pinned_1")
        check("session_send preserves model parts in context",
              sess["contents"][3]["parts"][0].get("thoughtSignature") == "SIG123")
    finally:
        m._chat_once = orig_chat_once

run(test_session_lifecycle())

# 3. Synthetic Tool Calling in code_task
async def test_code_task_synthetic_tool():
    passed_kwargs = {}

    async def fake_chat_continue(payload_contents, model, system="", **kw):
        passed_kwargs.update(kw)
        return {
            "status": "OK",
            "text": "",
            "function_call": {
                "name": "apply_code_edits",
                "args": {
                    "analysis": "Tool call refactor",
                    "changes": [{"path": "main.py", "operation": "create", "content": "print('ok')\n"}],
                    "done": True
                }
            },
            "parts": [
                {
                    "functionCall": {
                        "name": "apply_code_edits",
                        "args": {
                            "analysis": "Tool call refactor",
                            "changes": [{"path": "main.py", "operation": "create", "content": "print('ok')\n"}],
                            "done": True
                        }
                    }
                }
            ],
            "model": model,
            "finish_reason": "STOP"
        }
    
    orig_chat_cont = m._chat_continue
    m._chat_continue = fake_chat_continue
    try:
        out = json.loads(await m.code_task(goal="test tool call", files=[], synthetic_tool=True))
        check("code_task passes synthetic_code_tool=True", passed_kwargs.get("synthetic_code_tool") is True)
        check("code_task extracts synthetic tool functionCall args", out.get("status") == "OK")
        check("code_task sets done flag from tool args", out.get("done") is True)
        check("code_task has valid changes from tool args", len(out.get("changes") or []) == 1)
    finally:
        m._chat_continue = orig_chat_cont

run(test_code_task_synthetic_tool())

# 4. Telemetry and Circuit Breaker in status()
st = json.loads(m.status())
check("status reports active_endpoint", "active_endpoint" in st and "cloudcode-pa" in st["active_endpoint"])
check("status reports circuit_breaker state", isinstance(st.get("circuit_breaker"), dict) and st["circuit_breaker"].get("state") in ("CLOSED", "OPEN", "HALF_OPEN"))
check("status reports cached_tokens_total", "cached_tokens_total" in st)

print()
if fails:
    print("FAILED: " + ", ".join(fails))
    sys.exit(1)
print("ALL V3 INTEGRATION TESTS PASS!")
