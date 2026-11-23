"""Tests for chunking, JSON repair and map_reduce."""
import asyncio
import json
import os
import sys
import tempfile
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
check("boundary aware", all(c.endswith(("\n\n", "\n", " ")) or len(c) == 5000
                            for c in chunks[:-1]))

check("json trailing comma repaired",
      m._safe_json('{"a": 1, "b": [2,3,],}') == {"a": 1, "b": [2, 3]})

calls = {"n": 0}


async def fake_chat(prompt, model, **kw):
    calls["n"] += 1
    return {"status": "OK", "text": "PART" + str(calls["n"]), "model": model}


async def main():
    m._chat_once = fake_chat
    res = await m._map_reduce(["x" * 100, "y" * 15000], "summarize:", "high",
                              "combine:", chunk_chars=6000)
    check("map_reduce status OK", res["status"] == "OK")
    check("map_reduce split the big item", calls["n"] >= 4, f"calls={calls['n']}")
    check("map_reduce reports parts", len(res["parts"]) == 2, str(res["parts"]))
    check("map_reduce reduce ran", res["text"].startswith("PART"), str(res["text"])[:16])

    # a failing combine call must be reported, never disguised as a synthesis
    async def flaky(prompt, model, **kw):
        if "combine" in prompt:
            return {"status": "MODEL_UNAVAILABLE", "error": "no capacity (HTTP 503)"}
        return {"status": "OK", "text": "41", "model": model}

    m._chat_once = flaky
    res = await m._map_reduce(["a", "b"], "extract", "high", "combine", max_parallel=1)
    check("failed combine reports PARTIAL", res["status"] in ("PARTIAL", "FAILED"),
          res["status"])
    check("failed combine keeps partials", "41" in (res.get("text") or ""))


asyncio.run(main())
print("\nRESULT:", "ALL PASS" if not fails else f"{len(fails)} FAILED -> {fails}")
sys.exit(1 if fails else 0)
