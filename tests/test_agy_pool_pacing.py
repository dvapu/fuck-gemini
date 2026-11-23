"""Smoke test for the new traffic-shaping layer in agy_pool.py.

Runs offline: no HTTP, no OAuth. Uses a temp state dir so the real pool state
is untouched.
"""
import asyncio
import json
import os
import sys
import tempfile
import time
import types
from pathlib import Path

TMP = Path(tempfile.mkdtemp(prefix="agy-pace-"))
os.environ["AGY_POOL_STATE"] = str(TMP)
os.environ["AGY_POOL_MIN_INTERVAL_SEC"] = "0.4"
os.environ["AGY_POOL_JITTER_SEC"] = "0"
os.environ["AGY_POOL_MAX_INFLIGHT"] = "2"
os.environ["AGY_POOL_ACCOUNT_RPM"] = "50"

sys.path.insert(0, r"C:\Users\b\.config\opencode\mcp")
import agy_pool as m  # noqa: E402

fails = []


def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + ((" | " + detail) if detail else ""))
    if not ok:
        fails.append(name)


def acc(aid):
    return types.SimpleNamespace(id=aid, email=aid + "@x")


async def main():
    check("config parsed", (m.PACE_MIN_INTERVAL, m.MAX_INFLIGHT) == (0.4, 2),
          str((m.PACE_MIN_INTERVAL, m.MAX_INFLIGHT)))

    # 1. same account is paced
    a = acc("a1")
    stamps = []
    for _ in range(3):
        async with m.POOL.gate(a):
            stamps.append(time.time())
    gaps = [round(stamps[i + 1] - stamps[i], 2) for i in range(len(stamps) - 1)]
    check("per-account pacing >=0.4s", all(g >= 0.35 for g in gaps), str(gaps))

    # 2. different accounts are not serialized by the pacing rule
    b, c = acc("b1"), acc("c1")
    t0 = time.time()
    async with m.POOL.gate(b):
        pass
    async with m.POOL.gate(c):
        pass
    check("distinct accounts not paced against each other", time.time() - t0 < 0.3,
          f"{time.time() - t0:.2f}s")

    # 3. global in-flight ceiling of 2
    order = []

    async def worker(name, hold):
        async with m.POOL.gate(acc(name)):
            order.append(("enter", name, round(time.time() - t0b, 2)))
            await asyncio.sleep(hold)
            order.append(("exit", name, round(time.time() - t0b, 2)))

    t0b = time.time()
    await asyncio.gather(worker("w1", 0.4), worker("w2", 0.4), worker("w3", 0.05))
    concurrent = 0
    peak = 0
    for kind, _, _ in order:
        concurrent += 1 if kind == "enter" else -1
        peak = max(peak, concurrent)
    check("MAX_INFLIGHT respected", peak <= 2, f"peak={peak}")

    # 4. strike escalation doubles and caps
    s1 = m.POOL.strike_rate(acc("d1"), "m1", None)
    s2 = m.POOL.strike_rate(acc("d1"), "m1", None)
    s3 = m.POOL.strike_rate(acc("d1"), "m1", None)
    check("strike escalates 300->600->1200", s1 < s2 < s3 and 300 <= s1 < 330,
          f"{s1},{s2},{s3}")
    s4 = m.POOL.strike_rate(acc("d1"), "m1", 9999)
    check("Retry-After honoured + capped at 3600", s4 <= 3630, str(s4))

    # 5. success clears strikes and cooldown
    m.POOL.note_success(acc("d1"), "m1")
    check("note_success resets", "d1:m1" not in m.POOL._strikes
          and "d1:m1" not in m.POOL._cooldown)

    # 6. state persists pacing + strikes
    m._save_state(m.POOL.snapshot())
    disk = json.loads((TMP / "state.json").read_text())
    check("state has last_call/strikes", "last_call" in disk and "strikes" in disk,
          str(sorted(disk)))
    restored = m.Pool.__new__(m.Pool)
    restored._cooldown, restored._stats = {}, {"ok": 0, "fail": 0, "calls": 0}
    restored._last_call, restored._bucket, restored._strikes = {}, {}, {}
    restored._acct_locks = {}
    restored._gate_sem = None
    restored._state_saved_at = 0.0
    restored._cursor = 0
    restored._refresh_locks = {}
    restored._last_errors = {}
    restored._restore(disk)
    check("restore keeps pace markers", restored._last_call.get("a1", 0) > 0,
          str(restored._last_call))

    # 7. a paced account must not stall the others (global slot is taken after
    #    the per-account wait, not before it)
    seq = []

    async def hit(a, tag):
        async with m.POOL.gate(a):
            seq.append(tag)
            await asyncio.sleep(0.02)

    await hit(acc("slow1"), "first")
    await asyncio.gather(hit(acc("slow1"), "slow-second"), hit(acc("free1"), "free"))
    check("paced account does not block another account",
          seq.index("free") < seq.index("slow-second"), str(seq))

    # 8. same account never runs two requests at once
    active = {"n": 0, "peak": 0}

    async def overlap():
        async with m.POOL.gate(acc("solo")):
            active["n"] += 1
            active["peak"] = max(active["peak"], active["n"])
            await asyncio.sleep(0.15)
            active["n"] -= 1

    await asyncio.gather(*(overlap() for _ in range(3)))
    check("one request in flight per account", active["peak"] == 1,
          f"peak={active['peak']}")

    return 0


rc = asyncio.run(main())
print("\nRESULT:", "ALL PASS" if not fails else f"{len(fails)} FAILED -> {fails}")
sys.exit(1 if fails else 0)
