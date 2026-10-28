#!/usr/bin/env python3
"""Antigravity Account Pool — direct Gemini API access for OpenCode.

Why this exists
---------------
Going through `language_server.exe agentapi` creates a real Antigravity
conversation for every single call, which floods the Antigravity/Gemini chat
history with noise and adds a slow intermediate layer.

This server talks to the same Gemini backend the Antigravity app uses, but
directly over HTTPS with OAuth tokens from the agy-switch account pool:

    https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent

No conversation is created, nothing is written to Antigravity history, and each
request is a single stateless round-trip.

Model: OpenCode is the orchestrator (the "boss"). It sends a work order, reads
back structured proposed edits, applies them itself, verifies, then sends the
next work order. The pool never touches files on its own.

Security
--------
Tokens are read from the agy-switch account store and are NEVER logged, echoed,
or written anywhere except back to the account file they came from when Google
rotates a refresh token. Nothing sensitive lives inside a git repository.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mcp.server.mcpserver import MCPServer

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DATA_DIR = Path(
    os.environ.get("ABV_DATA_DIR", Path.home() / ".antigravity_tools")
)
ACCOUNTS_DIR = DATA_DIR / "accounts"
STATE_DIR = Path(
    os.environ.get("AGY_POOL_STATE", Path(os.environ.get("LOCALAPPDATA", str(Path.home()))) / "agy-pool")
)
STATE_FILE = STATE_DIR / "state.json"
OAUTH_FILE = STATE_DIR / "oauth.json"

API_ENDPOINT = "https://daily-cloudcode-pa.googleapis.com/v1internal:generateContent"
OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"


def _load_oauth_credentials() -> Tuple[str, str]:
    """Resolve OAuth client credentials without ever hardcoding them.

    Order: environment, then a private file under the state directory, then the
    bot's .env if it happens to be on this machine. The private file lives
    outside any repository so it can never be committed.
    """
    cid = os.environ.get("AGY_CLIENT_ID", "").strip()
    secret = os.environ.get("AGY_CLIENT_SECRET", "").strip()
    if cid and secret:
        return cid, secret

    try:
        data = json.loads(OAUTH_FILE.read_text(encoding="utf-8-sig"))
        cid = cid or (data.get("client_id") or "").strip()
        secret = secret or (data.get("client_secret") or "").strip()
    except Exception:
        pass
    if cid and secret:
        return cid, secret

    for candidate in (
        Path.home() / "muse-discord-bot" / "BotMusicaDiscord" / ".env",
    ):
        if not candidate.exists():
            continue
        try:
            for line in candidate.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if line.startswith("AGY_CLIENT_ID="):
                    cid = cid or line.split("=", 1)[1].strip().strip('"').strip("'")
                elif line.startswith("AGY_CLIENT_SECRET="):
                    secret = secret or line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            continue
        if cid and secret:
            break
    return cid, secret


CLIENT_ID, CLIENT_SECRET = _load_oauth_credentials()

USER_AGENT = "antigravity/2.27.0"
DEFAULT_MODEL = "gemini-3.8-flash-high"
DEFAULT_PROJECT = "aicode-consumers"

REQUEST_TIMEOUT = 180
COOLDOWN_RATE_LIMIT = 5 * 60      # 429 / quota
COOLDOWN_AUTH = 15 * 60           # 401 / 403
COOLDOWN_TRANSIENT = 60           # timeouts, 5xx
REFRESH_MARGIN = 300              # refresh if < 5 min left

# Errors that should make us try the next account rather than give up.
ROTATE_STATUS = {401, 403, 429, 500, 502, 503, 504}

MODEL_ALIASES = {
    # Measured on 2026-10-28 with 67-case execution tests:
    # every aliased model below solved the same coding task, so the choice is
    # about speed and headroom, not raw capability.
    "pro": "gemini-3.8-flash-high",   # hardest reasoning, slowest (~8-10s)
    "high": "gemini-3.8-flash-high",
    "deep": "gemini-3.8-flash-high",
    "default": "gemini-3-flash",      # best speed/quality balance (~2.5s)
    "flash": "gemini-3-flash",
    "fast": "gemini-3-flash",
    "lite": "gemini-3.5-flash-lite",  # cheapest, still passed all tests (~2.4s)
    "cheap": "gemini-3.5-flash-lite",
    "sonnet": "claude-sonnet-4-6",    # strongest non-Google (~5s)
    "claude": "claude-sonnet-4-6",
    "opus": "claude-opus-4-6-thinking",
    "gpt": "gpt-oss-120b-medium",     # NOT recommended: leaked reasoning, 439 out tokens
}

# Models that answer HTTP 200 with a hard-coded "no longer available" string
# instead of an error. Verified live on 2026-10-28; a 200 alone is NOT proof of
# life for this API.
DEPRECATED_MARKERS = (
    "no longer available",
    "is not supported",
    "unsupported model",
    "model has been deprecated",
)

KNOWN_DEAD = {
    "gemini-2.5-pro",          # HTTP 503 "No capacity available"
    "gemini-3-flash-agent",    # returns "Gemini 3.5 Flash is no longer available."
    "gemini-3.5-flash-low",    # same
    "gemini-3.5-flash-extra-low",  # same
}


def _no_window() -> int:
    return 0


# --------------------------------------------------------------------------- #
# State
# --------------------------------------------------------------------------- #


def _load_state() -> Dict[str, Any]:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Account store
# --------------------------------------------------------------------------- #


class Account:
    __slots__ = ("id", "email", "path", "access", "refresh", "expiry", "project",
                 "disabled", "blocked", "tier")

    def __init__(self, raw: Dict[str, Any], path: Path):
        tok = raw.get("token") or {}
        self.id = raw.get("id") or path.stem
        self.email = raw.get("email") or ""
        self.path = path
        self.access = tok.get("access_token") or ""
        self.refresh = tok.get("refresh_token") or ""
        self.expiry = int(tok.get("expiry_timestamp") or 0)
        self.project = tok.get("project_id") or DEFAULT_PROJECT
        self.disabled = bool(raw.get("disabled"))
        self.blocked = bool(raw.get("validation_blocked"))
        q = raw.get("quota") or {}
        self.tier = q.get("subscription_tier") or ""

    @property
    def usable(self) -> bool:
        return bool(self.access) and not self.disabled and not self.blocked


def _load_accounts() -> Tuple[List[Account], Optional[str]]:
    if not ACCOUNTS_DIR.exists():
        return [], f"Account directory not found: {ACCOUNTS_DIR}"

    files = sorted(ACCOUNTS_DIR.glob("*.json"))
    enc = list(ACCOUNTS_DIR.glob("*.enc"))

    if not files and enc:
        return [], (
            "Accounts are in VAULT mode (.enc files). Start the bot with the vault "
            "key shares configured, or run agy-switch to decrypt them first."
        )
    if not files:
        return [], f"No account files found in {ACCOUNTS_DIR}"

    accounts: List[Account] = []
    for f in files:
        try:
            raw = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        acc = Account(raw, f)
        if acc.usable:
            accounts.append(acc)
    return accounts, None


def _persist_rotated_token(acc: Account, new_refresh: str, new_access: str,
                           expires_in: int) -> None:
    """Write a rotated refresh token back so agy-switch keeps working.

    Google may invalidate the previous refresh token when it issues a new one.
    Only this account's file is touched, and the write is atomic.
    """
    try:
        raw = json.loads(acc.path.read_text(encoding="utf-8"))
    except Exception:
        return
    tok = raw.setdefault("token", {})
    tok["access_token"] = new_access
    if new_refresh:
        tok["refresh_token"] = new_refresh
    if expires_in:
        tok["expires_in"] = int(expires_in)
        tok["expiry_timestamp"] = int(time.time()) + int(expires_in)
    tmp = acc.path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        os.replace(tmp, acc.path)
    except Exception:
        pass


def _refresh_token(acc: Account) -> Optional[str]:
    """Exchange the refresh token for a new access token."""
    if not acc.refresh:
        return None
    if not CLIENT_ID or not CLIENT_SECRET:
        return None

    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": acc.refresh,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }).encode()

    req = urllib.request.Request(
        OAUTH_TOKEN_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode())
    except Exception:
        return None

    new_access = data.get("access_token")
    if not new_access:
        return None

    new_refresh = data.get("refresh_token") or acc.refresh
    expires_in = int(data.get("expires_in") or 3600)
    acc.access = new_access
    acc.refresh = new_refresh
    acc.expiry = int(time.time()) + expires_in
    _persist_rotated_token(acc, new_refresh, new_access, expires_in)
    return new_access


def _ensure_token(acc: Account) -> Optional[str]:
    if acc.access and acc.expiry - time.time() > REFRESH_MARGIN:
        return acc.access
    return _refresh_token(acc) or (acc.access or None)


# --------------------------------------------------------------------------- #
# Rotation
# --------------------------------------------------------------------------- #


class Pool:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._cursor = 0
        self._cooldown: Dict[str, float] = {}
        self._stats: Dict[str, Any] = {"ok": 0, "fail": 0, "calls": 0}

    def _pick_order(self, accounts: List[Account]) -> List[Account]:
        now = time.time()
        ready = [a for a in accounts
                 if self._cooldown.get(a.id, 0) <= now]
        cooling = [a for a in accounts
                   if self._cooldown.get(a.id, 0) > now]
        if not ready:
            # everything cooling: use the least-cooled anyway
            return sorted(accounts, key=lambda a: self._cooldown.get(a.id, 0))
        start = self._cursor % len(ready)
        rotated = ready[start:] + ready[:start]
        self._cursor = (self._cursor + 1) % max(1, len(ready))
        return rotated + cooling

    def _penalize(self, acc: Account, seconds: int) -> None:
        self._cooldown[acc.id] = time.time() + seconds


POOL = Pool()


# --------------------------------------------------------------------------- #
# API call
# --------------------------------------------------------------------------- #


def _build_payload(acc: Account, model: str, contents: List[Dict[str, Any]],
                   system: str, temperature: float, max_tokens: int,
                   json_mode: bool) -> bytes:
    gen: Dict[str, Any] = {"temperature": temperature, "maxOutputTokens": max_tokens}
    if json_mode:
        gen["responseMimeType"] = "application/json"

    request: Dict[str, Any] = {"contents": contents, "generationConfig": gen}
    if system:
        request["systemInstruction"] = {"parts": [{"text": system}]}

    return json.dumps({
        "project": acc.project,
        "model": model,
        "userAgent": "antigravity",
        "requestId": f"agent/oc-{int(time.time() * 1000)}/{random.randint(0, 9999)}/1",
        "request": request,
    }).encode()


def _post(token: str, payload: bytes) -> Tuple[Optional[Dict[str, Any]], Optional[int], str]:
    req = urllib.request.Request(
        API_ENDPOINT,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "x-request-source": "local",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
            return json.loads(r.read().decode("utf-8", "replace")), 200, ""
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:400]
        except Exception:
            detail = ""
        return None, e.code, detail
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"


def _extract_text(body: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    resp = body.get("response") if isinstance(body.get("response"), dict) else body
    candidates = resp.get("candidates") or []
    text = ""
    finish = ""
    for cand in candidates:
        parts = (cand.get("content") or {}).get("parts") or []
        chunk = "".join(p.get("text", "") for p in parts if isinstance(p, dict))
        if chunk.strip():
            text = chunk
            finish = cand.get("finishReason") or ""
            break
    usage = resp.get("usageMetadata") or body.get("usageMetadata") or {}
    return text.strip(), {
        "finish_reason": finish,
        "tokens": usage.get("totalTokenCount"),
        "prompt_tokens": usage.get("promptTokenCount"),
        "output_tokens": usage.get("candidatesTokenCount"),
    }


def _resolve_model(name: str) -> str:
    if not name:
        return DEFAULT_MODEL
    key = name.strip().lower()
    return MODEL_ALIASES.get(key, name.strip())


async def _chat_once(prompt_or_contents: Any, model: str, system: str = "",
                     temperature: float = 0.7, max_tokens: int = 8192,
                     json_mode: bool = False,
                     history: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Run one prompt across the pool, rotating on failure."""
    accounts, err = _load_accounts()
    if err:
        return {"status": "ERROR", "error": err}
    if not accounts:
        return {"status": "ERROR", "error": "No usable accounts in the pool."}

    model = _resolve_model(model)

    if isinstance(prompt_or_contents, list):
        contents = prompt_or_contents
    else:
        contents = list(history or []) + [
            {"role": "user", "parts": [{"text": str(prompt_or_contents)}]}
        ]

    order = POOL._pick_order(accounts)
    attempts: List[Dict[str, Any]] = []

    for acc in order:
        token = _ensure_token(acc)
        if not token:
            attempts.append({"account": acc.email, "error": "no-token"})
            POOL._penalize(acc, COOLDOWN_AUTH)
            continue

        payload = _build_payload(acc, model, contents, system, temperature,
                                 max_tokens, json_mode)
        started = time.time()
        body, code, detail = await asyncio.to_thread(_post, token, payload)
        elapsed = int((time.time() - started) * 1000)

        if body is not None and code == 200:
            text, meta = _extract_text(body)
            if not text:
                POOL._stats["fail"] += 1
                attempts.append({"account": acc.email, "error": "empty-response",
                                 "finish": meta.get("finish_reason")})
                POOL._penalize(acc, COOLDOWN_TRANSIENT)
                continue

            # A 200 with a short "no longer available" sentence is a dead model,
            # not an answer. Rotating accounts cannot fix it, so fail fast.
            lowered = text.lower()
            if len(text) < 220 and any(mk in lowered for mk in DEPRECATED_MARKERS):
                POOL._stats["fail"] += 1
                return {
                    "status": "MODEL_UNAVAILABLE",
                    "error": f"Model '{model}' is retired: {text[:160]}",
                    "model": model,
                    "hint": "Use agy_pool_health to list models that actually work.",
                }

            POOL._stats["ok"] += 1
            POOL._stats["calls"] += 1
            return {
                "status": "OK",
                "text": text,
                "model": model,
                "account": acc.email,
                "latency_ms": elapsed,
                **meta,
                "attempts": attempts,
            }

        # failure
        POOL._stats["fail"] += 1
        attempts.append({"account": acc.email, "http": code, "detail": detail[:200]})

        if code in ROTATE_STATUS:
            POOL._penalize(acc, COOLDOWN_RATE_LIMIT if code == 429 else COOLDOWN_AUTH)
            continue
        if code is None:
            POOL._penalize(acc, COOLDOWN_TRANSIENT)
            continue
        # non-rotatable client error: stop, it will fail everywhere
        return {
            "status": "ERROR",
            "error": f"API rejected the request (HTTP {code})",
            "detail": detail[:400],
            "model": model,
            "attempts": attempts,
        }

    return {
        "status": "EXHAUSTED",
        "error": "Every account failed or is cooling down.",
        "attempts": attempts,
    }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _ok(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _err(message: str, **extra: Any) -> str:
    return json.dumps({"status": "ERROR", "error": message, **extra},
                      ensure_ascii=False, indent=2)


def _safe_json(text: str) -> Optional[Any]:
    """Parse JSON that may be wrapped in a markdown fence."""
    t = text.strip()
    if t.startswith("```"):
        lines = t.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        t = "\n".join(lines).strip()
    try:
        return json.loads(t)
    except Exception:
        start = min([i for i in (t.find("{"), t.find("[")) if i >= 0], default=-1)
        end = max(t.rfind("}"), t.rfind("]"))
        if start >= 0 and end > start:
            try:
                return json.loads(t[start:end + 1])
            except Exception:
                return None
        return None


# --------------------------------------------------------------------------- #
# MCP server
# --------------------------------------------------------------------------- #

app = MCPServer(
    "agy-pool",
    instructions=(
        "Direct Gemini/Claude access through the agy-switch OAuth account pool.\n\n"
        "This does NOT create Antigravity conversations, so it adds nothing to the "
        "Antigravity or Gemini chat history. It is the preferred way to use the "
        "Antigravity accounts from OpenCode.\n\n"
        "Orchestration loop: you are the boss. Use agy_pool_code_task to get a "
        "structured work order result (proposed file edits as JSON), apply the "
        "edits yourself with your own tools, verify them (read the file, run the "
        "build/tests), then send the next work order describing what failed or "
        "what is next. Repeat until the goal is met.\n\n"
        "Use agy_pool_chat for plain questions, agy_pool_batch for independent "
        "work spread across accounts in parallel, agy_pool_health to verify "
        "which models actually respond, and agy_pool_status to check the pool."
    ),
)


@app.tool()
async def agy_pool_health(sample_limit: int = 0) -> str:
    """Probe models for real availability, not quota metadata.

    A model can return HTTP 200 with "no longer available" text, or 503 with no
    capacity, while still appearing in the account's model list. This tool sends
    a tiny real request to each model and reports what actually answers.

    Args:
        sample_limit: 0 probes every known model. Set a smaller number for a
            quick check.
    """
    accounts, err = _load_accounts()
    if err:
        return _err(err)

    # Discover declared models plus known-dead ones so removals are visible.
    declared = set(KNOWN_DEAD)
    for acc in accounts:
        try:
            raw = json.loads(acc.path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for m in (raw.get("quota") or {}).get("models") or []:
            if m.get("name"):
                declared.add(m["name"])

    targets = sorted(declared)
    if sample_limit and sample_limit > 0:
        preferred = [m for m in targets if m not in KNOWN_DEAD][:sample_limit]
        targets = preferred

    sem = asyncio.Semaphore(4)

    async def probe(idx: int, model: str) -> Dict[str, Any]:
        async with sem:
            acc = accounts[idx % len(accounts)]
            token = _ensure_token(acc)
            if not token:
                return {"model": model, "state": "NO_TOKEN"}
            payload = _build_payload(acc, model,
                                     [{"role": "user", "parts": [{"text": "Reply with exactly: PING"}]}],
                                     "", 0.0, 256, False)
            started = time.time()
            body, code, detail = await asyncio.to_thread(_post, token, payload)
            elapsed = int((time.time() - started) * 1000)
            if body is None or code != 200:
                return {"model": model, "state": "DEAD", "http": code,
                        "latency_ms": elapsed, "detail": (detail or "")[:120]}
            text, meta = _extract_text(body)
            lowered = text.lower()
            if len(text) < 220 and any(mk in lowered for mk in DEPRECATED_MARKERS):
                return {"model": model, "state": "DEPRECATED",
                        "latency_ms": elapsed, "says": text[:120]}
            if not text:
                return {"model": model, "state": "EMPTY", "latency_ms": elapsed}
            return {"model": model, "state": "ALIVE", "latency_ms": elapsed,
                    "tokens": meta.get("tokens")}

    checks = await asyncio.gather(*(probe(i, m) for i, m in enumerate(targets)))
    alive = [c["model"] for c in checks if c["state"] == "ALIVE"]
    dead = [c for c in checks if c["state"] in ("DEAD", "DEPRECATED", "EMPTY")]

    return _ok({
        "status": "OK",
        "probed": len(checks),
        "alive_count": len(alive),
        "dead_count": len(dead),
        "alive": sorted(alive),
        "dead": dead,
        "aliases": MODEL_ALIASES,
        "note": "HTTP 200 with 'no longer available' text is reported as DEPRECATED.",
        "checks": sorted(checks, key=lambda c: (c["state"] != "ALIVE", c["model"])),
    })


@app.tool()
def agy_pool_quota() -> str:
    """Show remaining quota per account, read from the cached quota metadata."""
    accounts, err = _load_accounts()
    if err:
        return _err(err)

    rows = []
    for acc in accounts:
        try:
            raw = json.loads(acc.path.read_text(encoding="utf-8"))
        except Exception:
            continue
        q = raw.get("quota") or {}
        buckets = {}
        for g in q.get("quota_groups") or []:
            for b in g.get("buckets") or []:
                buckets[b.get("bucket_id")] = b.get("remaining_fraction")
        rows.append({
            "email": acc.email,
            "tier": q.get("subscription_tier"),
            "forbidden": q.get("is_forbidden"),
            "gemini_5h_remaining": buckets.get("gemini-5h"),
            "weekly_remaining": buckets.get("gemini-weekly"),
            "third_party_5h_remaining": buckets.get("3p-5h"),
        })

    known = [r["gemini_5h_remaining"] for r in rows
             if isinstance(r["gemini_5h_remaining"], (int, float))]
    return _ok({
        "status": "OK",
        "accounts": len(rows),
        "average_gemini_5h": (sum(known) / len(known)) if known else None,
        "note": "Values are 0-1 fractions from cached quota; run agy-switch refresh for fresh data.",
        "rows": rows,
    })


@app.tool()
def agy_pool_status() -> str:
    """Report the health of the Antigravity account pool.

    Shows which accounts are usable, token expiry, cooldowns, and counters.
    Never includes token material.
    """
    accounts, err = _load_accounts()
    if err:
        return _err(err)

    now = time.time()
    rows = []
    for acc in accounts:
        rows.append({
            "email": acc.email,
            "tier": acc.tier,
            "token_valid": acc.expiry > now,
            "expires_in_minutes": max(0, int((acc.expiry - now) / 60)),
            "can_refresh": bool(acc.refresh),
            "cooling_down_seconds": max(0, int(POOL._cooldown.get(acc.id, 0) - now)),
        })

    return _ok({
        "status": "OK",
        "accounts_total": len(accounts),
        "accounts_ready": sum(1 for r in rows
                              if r["token_valid"] and r["cooling_down_seconds"] == 0),
        "default_model": DEFAULT_MODEL,
        "stats": POOL._stats,
        "oauth_refresh_configured": bool(CLIENT_ID and CLIENT_SECRET),
        "accounts": rows,
    })


@app.tool()
def agy_pool_models() -> str:
    """List the model names available to the pool, with aliases."""
    accounts, err = _load_accounts()
    if err:
        return _err(err)

    seen: Dict[str, Any] = {}
    for acc in accounts:
        try:
            raw = json.loads(acc.path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for m in (raw.get("quota") or {}).get("models") or []:
            name = m.get("name")
            if name and name not in seen:
                seen[name] = {
                    "display": m.get("display_name"),
                    "max_output_tokens": m.get("max_output_tokens"),
                }
    return _ok({
        "status": "OK",
        "default": DEFAULT_MODEL,
        "aliases": MODEL_ALIASES,
        "models": dict(sorted(seen.items())),
    })


@app.tool()
async def agy_pool_chat(
    prompt: str,
    model: str = "pro",
    system: str = "",
    temperature: float = 0.7,
    max_tokens: int = 8192,
) -> str:
    """Ask one question and get plain text back. Stateless, no conversation created.

    Args:
        prompt: The question or instruction.
        model: Model name or alias (pro, flash, lite, sonnet, opus, gpt).
        system: Optional system instruction.
        temperature: Sampling temperature.
        max_tokens: Maximum output tokens.
    """
    if not prompt or not prompt.strip():
        return _err("prompt must not be empty.")
    result = await _chat_once(prompt, model, system, temperature, max_tokens)
    return _ok(result)


@app.tool()
async def agy_pool_code_task(
    goal: str,
    files: List[Dict[str, str]],
    context: str = "",
    constraints: str = "",
    model: str = "pro",
    max_tokens: int = 16384,
) -> str:
    """Turn a work order into structured file edits for YOU to apply and verify.

    The pool does not touch the filesystem. It returns proposed edits as JSON so
    the caller can apply them, verify the result, and then send the next work
    order.

    Args:
        goal: What must be achieved, with acceptance criteria.
        files: Files to work on, as [{"path": "...", "content": "..."}].
        context: Extra context (project layout, conventions, prior attempt).
        constraints: Hard rules, e.g. "do not change public APIs".
        model: Model name or alias.
        max_tokens: Maximum output tokens.
    """
    if not goal or not goal.strip():
        return _err("goal must not be empty.")

    system = (
        "You are a precise senior software engineer working under an orchestrator.\n"
        "You never claim work is done. You only propose concrete edits.\n"
        "Respond with a single JSON object and nothing else, matching this schema:\n"
        "{\n"
        '  "analysis": "short reasoning about what must change",\n'
        '  "changes": [\n'
        '    {"path": "absolute or repo-relative path",\n'
        '     "operation": "create" | "replace" | "patch",\n'
        '     "content": "full new file content (for create/replace)",\n'
        '     "search": "exact existing text to replace (for patch)",\n'
        '     "replace": "new text (for patch)"}\n'
        "  ],\n"
        '  "verification": ["commands or checks the orchestrator should run"],\n'
        '  "risks": ["anything uncertain or incomplete"],\n'
        '  "done": true | false\n'
        "}\n"
        "Rules:\n"
        "- Output every change fully; never use placeholders like '... rest unchanged'.\n"
        "- For patch operations, 'search' must match the existing file exactly.\n"
        "- If the goal cannot be completed with the given information, set "
        "done=false and explain what is missing in risks.\n"
        "- Do not invent file paths that were not given unless clearly required."
    )

    parts: List[str] = [f"# Goal\n{goal}"]
    if constraints:
        parts.append(f"# Constraints\n{constraints}")
    if context:
        parts.append(f"# Context\n{context}")

    if files:
        blocks = []
        for f in files:
            path = f.get("path", "?")
            content = f.get("content", "")
            blocks.append(f"--- FILE: {path} ---\n{content}")
        parts.append("# Files\n" + "\n\n".join(blocks))
    else:
        parts.append("# Files\n(none provided; propose new files if needed)")

    result = await _chat_once("\n\n".join(parts), model, system,
                              temperature=0.2, max_tokens=max_tokens,
                              json_mode=True)

    if result.get("status") != "OK":
        return _ok(result)

    parsed = _safe_json(result["text"])
    if parsed is None:
        return _ok({
            "status": "UNPARSABLE",
            "error": "Model did not return valid JSON.",
            "raw_text": result["text"][:4000],
            "account": result.get("account"),
        })

    parsed["status"] = "OK"
    parsed["model"] = result.get("model")
    parsed["account"] = result.get("account")
    parsed["latency_ms"] = result.get("latency_ms")
    parsed["tokens"] = result.get("tokens")
    parsed["raw_text"] = None
    return _ok(parsed)


@app.tool()
async def agy_pool_batch(
    prompts: List[str],
    model: str = "pro",
    system: str = "",
    max_tokens: int = 8192,
    max_parallel: int = 4,
) -> str:
    """Run several independent prompts in parallel, spread across accounts.

    Args:
        prompts: The prompts to run. Keep them independent of each other.
        model: Model name or alias.
        system: Optional system instruction shared by every prompt.
        max_tokens: Maximum output tokens per prompt.
        max_parallel: How many run at once (1-8).
    """
    if not prompts:
        return _err("prompts must not be empty.")
    if len(prompts) > 32:
        return _err("Too many prompts (max 32).")
    parallel = max(1, min(int(max_parallel or 4), 8))
    sem = asyncio.Semaphore(parallel)

    async def one(idx: int, p: str) -> Dict[str, Any]:
        async with sem:
            r = await _chat_once(p, model, system, temperature=0.7,
                                 max_tokens=max_tokens)
            r["index"] = idx
            return r

    results = await asyncio.gather(*(one(i, p) for i, p in enumerate(prompts)))
    ok = sum(1 for r in results if r.get("status") == "OK")
    return _ok({
        "status": "OK" if ok else "FAILED",
        "succeeded": ok,
        "failed": len(results) - ok,
        "results": results,
    })


@app.tool()
async def agy_pool_review(
    goal: str,
    files: List[Dict[str, str]],
    model: str = "pro",
    max_tokens: int = 8192,
) -> str:
    """Independently review whether the work meets the goal. Use it to verify.

    Returns a verdict plus concrete remaining problems, so the orchestrator can
    decide whether to stop or send another work order.

    Args:
        goal: The original goal and its acceptance criteria.
        files: The current state of the files to review.
        model: Model name or alias.
        max_tokens: Maximum output tokens.
    """
    system = (
        "You are a strict code reviewer verifying another engineer's work.\n"
        "Be skeptical. Do not approve unless the evidence in the files supports it.\n"
        "Respond with a single JSON object:\n"
        '{"verdict": "pass" | "fail", "confidence": 0-100,\n'
        ' "problems": [{"file": "...", "issue": "...", "severity": "high|medium|low"}],\n'
        ' "next_steps": ["..."],\n'
        ' "summary": "one paragraph"}'
    )
    parts = [f"# Goal\n{goal}"]
    blocks = []
    for f in files:
        blocks.append(f"--- FILE: {f.get('path','?')} ---\n{f.get('content','')}")
    parts.append("# Current files\n" + "\n\n".join(blocks))

    result = await _chat_once("\n\n".join(parts), model, system,
                              temperature=0.1, max_tokens=max_tokens,
                              json_mode=True)
    if result.get("status") != "OK":
        return _ok(result)

    parsed = _safe_json(result["text"])
    if parsed is None:
        return _ok({"status": "UNPARSABLE",
                    "raw_text": result["text"][:3000]})
    parsed["status"] = "OK"
    parsed["account"] = result.get("account")
    parsed["model"] = result.get("model")
    return _ok(parsed)


def main() -> None:
    app.run(transport="stdio")


if __name__ == "__main__":
    main()
