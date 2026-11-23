"""Tests for v3+: server-side file reading with skip semantics + continuation."""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

os.environ["AGY_POOL_STATE"] = tempfile.mkdtemp(prefix="agy-v3-")
sys.path.insert(0, r"C:\Users\b\.config\opencode\mcp")
import agy_pool as m  # noqa: E402

fails = []
def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (" | " + detail if detail else ""))
    if not ok:
        fails.append(name)

root = Path(tempfile.mkdtemp(prefix="agy-files-"))
a = root / "a.py";   a.write_text("def hello():\n    return 'A_MARKER'\n", encoding="utf-8")
b = root / "b.md";   b.write_text("# doc\nB_MARKER\n", encoding="utf-8")
bin_txt = root / "fake.txt"; bin_txt.write_bytes(b"PK\x03\x04\x00binary\x00")
exe = root / "c.exe"; exe.write_bytes(b"MZ\x00")
sub = root / "sub";  sub.mkdir()
missing = root / "nope.py"

files, skipped, err = m._read_files_by_path([str(a), str(b), str(a)])
check("reads files", len(files) == 2 and not err, err)
check("dedupes", len(skipped) == 0)
check("content correct", "A_MARKER" in files[0]["content"] and "B_MARKER" in files[1]["content"])
check("path kept as given", files[0]["path"] == str(a))

files, skipped, err = m._read_files_by_path([str(a), str(exe), str(bin_txt), str(sub),
                                             str(missing)])
check("mixed batch keeps good files", len(files) == 1 and files[0]["path"] == str(a))
reasons = {s["reason"] for s in skipped}
check("skips unsupported suffix", "unsupported suffix" in reasons, str(reasons))
check("skips binary", "binary" in reasons, str(reasons))
check("skips directory", "is a directory" in reasons, str(reasons))
check("skips missing", "not found" in reasons, str(reasons))
check("mixed batch has no error", err == "", err)

files, skipped, err = m._read_files_by_path([str(exe)])
check("all-skipped reports error", not files and "nothing could be read" in err, err)
check("all-skipped lists reason", "unsupported suffix" in err, err)

files, skipped, err = m._read_files_by_path([str(a)], max_total_chars=5)
check("total cap skips", not files and any("total limit" in s["reason"] for s in skipped))

captured = {}
async def fake_chat(prompt, model, system="", **kw):
    captured["prompt"] = prompt if isinstance(prompt, str) else json.dumps(prompt)
    return {"status": "OK", "text": json.dumps({"analysis": "a", "changes": [],
                                                "verification": [], "risks": [],
                                                "done": True}),
            "model": model, "account": "x", "truncated": False}

async def main():
    m._chat_once = fake_chat
    out = await m.code_task(goal="g", files=[], paths=[str(a), str(bin_txt)])
    check("code_task reads paths", "A_MARKER" in captured["prompt"])
    check("code_task reports skipped input", "Inputs not read" in captured["prompt"]
          and "binary" in captured["prompt"], captured["prompt"][-120:])
    check("code_task returns parsed json", json.loads(out).get("done") is True)

    captured.clear()
    await m.review(goal="g", files=[], paths=[str(b)])
    check("review reads paths", "B_MARKER" in captured["prompt"])

    seq = [{"status": "OK", "text": "first half ", "truncated": True,
            "model": "m", "account": "x"},
           {"status": "OK", "text": "second half", "truncated": False,
            "model": "m", "account": "x"}]
    calls = {"n": 0}
    async def seq_chat(*args, **kw):
        i = min(calls["n"], len(seq) - 1); calls["n"] += 1
        return dict(seq[i])
    m._chat_once = seq_chat
    res = await m._chat_continue("write a long thing", "high", max_tokens=100)
    check("continuation concatenates", res["text"] == "first half second half", res["text"])
    check("continuation count", res.get("continuations") == 1)

    calls["n"] = 0
    async def fail_second(*args, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"status": "OK", "text": "partial", "truncated": True, "model": "m"}
        return {"status": "ERROR", "error": "boom"}
    m._chat_once = fail_second
    res = await m._chat_continue("x", "high", max_tokens=10)
    check("failed continuation keeps partial", res["text"] == "partial"
          and res["status"] == "OK", f"{res.get('status')} {res.get('text')}")

    async def fail_first(*a, **k):
        return {"status": "EXHAUSTED", "error": "nope"}
    m._chat_once = fail_first
    res = await m._chat_continue("x", "high")
    check("failed first call unchanged", res["status"] == "EXHAUSTED")

asyncio.run(main())
print("\nRESULT:", "ALL PASS" if not fails else f"{len(fails)} FAILED -> {fails}")
sys.exit(1 if fails else 0)
