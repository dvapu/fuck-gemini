#!/usr/bin/env python3
"""Standalone sanity check for agy_pool.py — no MCP client needed.

Run:  python check.py
Exits 0 when the pool is usable, 1 otherwise. Never prints token material.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("agy_pool", HERE / "agy_pool.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


async def main() -> int:
    print("== agy_pool check ==\n")

    accounts, err = mod._load_accounts()
    if err:
        print(f"[FAIL] {err}")
        return 1
    print(f"[OK]   accounts loaded: {len(accounts)}")

    print(f"[{'OK' if mod.CLIENT_ID and mod.CLIENT_SECRET else 'WARN'}] "
          f"oauth refresh credentials: {'found' if mod.CLIENT_ID and mod.CLIENT_SECRET else 'missing (auto-refresh disabled)'}")

    status = json.loads(mod.agy_pool_status())
    ready = status["accounts_ready"]
    print(f"[{'OK' if ready else 'FAIL'}]  accounts ready: {ready}/{status['accounts_total']}")
    if not ready:
        print("       all accounts are disabled, blocked, or cooling down")
        return 1

    print("\n[ping] calling model 'flash' ...")
    result = json.loads(await mod.agy_pool_chat("Reply with exactly: POOL_OK", model="flash"))
    if result.get("status") == "OK":
        print(f"[OK]   response: {result.get('text')!r}")
        print(f"       account: {result.get('account')}  latency: {result.get('latency_ms')} ms")
        print("\nALL GOOD - the pool is usable.")
        return 0

    print(f"[FAIL] status={result.get('status')} error={result.get('error')}")
    for a in result.get("attempts", [])[:5]:
        print("       attempt:", a)
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
