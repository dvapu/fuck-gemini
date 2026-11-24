#!/usr/bin/env python3
"""Run a list of code_task work orders concurrently through the local pool.

Usage: python poolbatch.py tasks.json [--outdir pool_out] [--max-tokens 12000]

tasks.json = [{"name": "unique", "goal": "...", "paths": ["..."], "model": "high",
               "max_tokens": 12000, "context": "", "constraints": ""}]

Each task writes <outdir>/<name>.json and the script prints one summary line per
task plus the paths of anything that produced changes, so the caller can read
only the files it needs. Uses the pool's own account rotation, so N tasks run
concurrently across N accounts.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

POOL = Path(r"C:\Users\b\.config\opencode\mcp")
sys.path.insert(0, str(POOL))
import agy_pool as pool  # noqa: E402


STRICT = ("Your previous reply could not be parsed. Reply with ONE JSON object and nothing "
          "else: no markdown fence, no prose before or after, first character {, last }.")


async def run_one(task, outdir, default_tokens, attempts=2):
    name = task["name"]
    started = time.time()
    payload = {}
    for attempt in range(1, attempts + 1):
        constraints = task.get("constraints") or ""
        if attempt > 1:
            constraints = (constraints + "\n" + STRICT).strip()
        try:
            raw = await pool.code_task(
                goal=task["goal"],
                files=[],
                paths=task.get("paths") or [],
                context=task.get("context") or "",
                constraints=constraints,
                model=task.get("model") or "high",
                max_tokens=int(task.get("max_tokens") or default_tokens),
            )
            payload = json.loads(raw)
        except Exception as exc:  # never let one task kill the batch
            payload = {"status": "EXCEPTION", "error": f"{type(exc).__name__}: {exc}"}
        payload["attempt"] = attempt
        if payload.get("status") in ("OK", "PARTIAL"):
            break
        if payload.get("status") not in ("UNPARSABLE", "TRUNCATED"):
            break
    payload["task"] = name
    payload["wall_ms"] = int((time.time() - started) * 1000)
    (outdir / f"{name}.json").write_text(json.dumps(payload, indent=1), encoding="utf-8")
    return payload


def summarise(p):
    changes = p.get("changes") or []
    head = f"{p['task']:<22} {p.get('status'):<10} {p.get('wall_ms', 0) // 1000:>4}s changes={len(changes)}"
    if changes:
        head += " " + " ".join(f"{c.get('path')}({len(c.get('content') or '')}B)" for c in changes)
    if p.get("error"):
        head += " err=" + str(p["error"])[:80]
    if p.get("note"):
        head += " note=" + str(p["note"])[:60]
    return head


async def main_async(args):
    tasks = json.loads(Path(args.tasks).read_text(encoding="utf-8"))
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    results = await asyncio.gather(*(run_one(t, outdir, args.max_tokens) for t in tasks))
    print("\n".join(summarise(p) for p in results))
    ok = sum(1 for p in results if p.get("status") in ("OK", "PARTIAL"))
    print(f"\n{ok}/{len(results)} usable, results in {outdir}")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tasks")
    ap.add_argument("--outdir", default="pool_out")
    ap.add_argument("--max-tokens", type=int, default=16000)
    sys.exit(asyncio.run(main_async(ap.parse_args())))


if __name__ == "__main__":
    main()
