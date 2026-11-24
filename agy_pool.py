#!/usr/bin/env python3
"""Antigravity Account Pool — direct Gemini API access for MCP clients.

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

Model: the calling agent is the orchestrator (the "boss"). It sends a work
order, reads back structured proposed edits, applies them itself, verifies, then
sends the next work order. The pool never touches files on its own.

Deployment knobs (environment)
------------------------------
    AGY_POOL_TOOL_PREFIX  Tool-name prefix, default "agy_pool_". Set it to an
                          empty value when the client already namespaces tools by
                          server, so names do not collide or repeat themselves.
    AGY_POOL_STATE        Directory for cooldown/session state, default
                          %LOCALAPPDATA%/agy-pool.
    AGY_CLIENT_ID, AGY_CLIENT_SECRET
                          OAuth client credentials used for token refresh.

    Traffic shaping (raise these if the pool starts getting 429s):
    AGY_POOL_MIN_INTERVAL_SEC
                          Minimum seconds between two calls on one account,
                          default 4. Jitter of 0-2 s is added on top.
    AGY_POOL_JITTER_SEC   Upper bound of that random extra delay, default 2.
    AGY_POOL_ACCOUNT_RPM  Rolling-60s request budget per account, default 15.
    AGY_POOL_MAX_INFLIGHT Process-wide concurrent request ceiling, default 2.
                          fanout/batch still accept 8 items; they queue here.


Security
--------
Tokens are read from the agy-switch account store and are NEVER logged, echoed,
or written anywhere except back to the account file they came from when Google
rotates a refresh token. Nothing sensitive lives inside a git repository.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import email.utils
import html as html_lib
import json
import logging
import mimetypes
import os
import random
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple

from mcp.server.mcpserver import MCPServer
from pydantic import Field

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

    Order: environment, then a private file under the state directory. The
    private file lives outside any repository so it can never be committed.
    No foreign project path is scanned: a missing fallback is better than reading
    secrets from an unrelated app's .env on the same machine.
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
    return cid, secret


CLIENT_ID, CLIENT_SECRET = _load_oauth_credentials()

# Diagnostics only. Tokens, prompts and account credentials are never logged.
LOG = logging.getLogger("agy_pool")

USER_AGENT = "antigravity/2.27.0"
DEFAULT_MODEL = "gemini-3.8-flash-high"
DEFAULT_PROJECT = "aicode-consumers"

REQUEST_TIMEOUT = 180
COOLDOWN_RATE_LIMIT = 5 * 60      # 429 / quota
COOLDOWN_AUTH = 15 * 60           # 401 / 403
COOLDOWN_TRANSIENT = 60           # timeouts, 5xx
REFRESH_MARGIN = 300              # refresh if < 5 min left

# Reasoning models spend budget on thinking before emitting any text. A
# max_tokens below the thinking budget yields finishReason=MAX_TOKENS with zero
# output tokens. That is a bad caller parameter, not a broken account: rotating
# accounts can never fix it, so we must never penalize the pool for it.
MIN_OUTPUT_TOKENS = 512           # retry once at this floor if truncated empty
MAX_OUTPUT_TOKENS = 65536         # hard ceiling accepted by the backend
MAX_ATTEMPTS = 4                  # do not burn every account on one request
# Total wall-clock budget per call. The DSH MCP client allows 600 s and OpenCode
# 900 s; one HTTP attempt can take up to REQUEST_TIMEOUT=180 s, so stay well
# below the smaller client timeout instead of being killed from the outside.
CALL_DEADLINE_SECONDS = 540

# Errors that should make us try the next account rather than give up.
ROTATE_STATUS = {401, 403, 408, 425, 429, 500, 502, 503, 504, 529}

# --------------------------------------------------------------------------- #
# Traffic shaping (account safety)
# --------------------------------------------------------------------------- #
# The backend this talks to is built for one human driving one desktop client.
# What gets an account suspended is not a single request but a *shape*: bursts,
# machine-regular spacing, eight parallel calls, and instant retries after a
# 429. Everything below exists to make the pool look like a slow human and to
# back off harder the more the upstream complains.

PACE_MIN_INTERVAL = float(os.environ.get("AGY_POOL_MIN_INTERVAL_SEC", "4"))
PACE_JITTER = float(os.environ.get("AGY_POOL_JITTER_SEC", "2"))
# Rolling 60s budget per account. Above this the gate sleeps until the oldest
# call in the window falls out.
ACCOUNT_RPM = max(1, int(os.environ.get("AGY_POOL_ACCOUNT_RPM", "15")))
# Process-wide in-flight ceiling. fanout/batch still accept up to 8 items, they
# just queue behind this instead of firing at once.
MAX_INFLIGHT = max(1, int(os.environ.get("AGY_POOL_MAX_INFLIGHT", "2")))
RETRY_AFTER_CAP = 3600            # never honour a Retry-After beyond one hour
STRIKE_CAP_SECONDS = 3600         # escalating 429 cooldown ceiling
GATE_MAX_WAIT = 90                # a single call never waits longer than this

MODEL_ALIASES = {
    # Measured on 2026-10-28 with 67-case execution tests:
    # every aliased model below solved the same coding task, so the choice is
    # about speed and headroom, not raw capability.
    "pro": "gemini-3.8-flash-high",   # hardest reasoning, slowest (~8-10s)
    "high": "gemini-3.8-flash-high",
    "deep": "gemini-3.8-flash-high",
    "default": "gemini-3.8-flash-high",  # strongest reasoning (~9.5s, user choice)
    "flash": "gemini-3.8-flash-high",
    "fast": "gemini-3.8-flash-high",
    "gemini": "gemini-3.8-flash-high",
    "lite": "gemini-3.5-flash-lite",  # cheapest, still passed all tests (~2.4s)
    "cheap": "gemini-3.5-flash-lite",
    # sonnet/claude aliases removed on purpose (2026-10-28): Claude models draw
    # from the scarce 3p-5h quota bucket (separate from gemini-5h), so ensembles
    # should stay all-Gemini. The raw model name still works if ever needed.
    "gpt": "gpt-oss-120b-medium",     # NOT recommended: leaked reasoning, 439 out tokens
}

# Models that answer HTTP 200 with a hard-coded "no longer available" string
# instead of an error. Verified live on 2026-10-28; a 200 alone is NOT proof of
# life for this API. Only retirement-specific phrases belong here: a generic
# "is not supported" appears in ordinary answers too, and misclassifying a real
# answer as a retired model fails the whole call.
DEPRECATED_MARKERS = (
    "no longer available",
    "unsupported model",
    "model has been deprecated",
    "model is deprecated",
)

KNOWN_DEAD = {
    "gemini-2.5-pro",          # HTTP 503 "No capacity available"
    "gemini-3-flash-agent",    # returns "Gemini 3.5 Flash is no longer available."
    "gemini-3.5-flash-low",    # same
    "gemini-3.5-flash-extra-low",  # same
}

SERVER_VERSION = "2.1.0"

# Tool names are prefixed so several clients can share one server process, but a
# client that already namespaces by server name (DSH renders
# mcp__agy-pool__<tool>) should set this to an empty value: repeating the server
# name inside every tool name costs tokens and reads badly.
TOOL_PREFIX = os.environ.get("AGY_POOL_TOOL_PREFIX", "agy_pool_").strip()


def _t(suffix: str) -> str:
    """Full MCP tool name for this deployment."""
    return f"{TOOL_PREFIX}{suffix}" if TOOL_PREFIX else suffix


# Fan-out answers come back pasted whole. N model answers is N times the context
# for no extra signal, so bound both each answer and the whole result.
MAX_PER_RESULT_CHARS = 12000
MAX_TOTAL_RESULT_CHARS = 60000

# health makes real (tiny) model requests, so it is neither free nor
# instant: keep the default scope small, cap wall-clock time, and reuse a recent
# answer instead of re-probing on every call.
HEALTH_TTL_SECONDS = 300
HEALTH_DEADLINE_SECONDS = 240
HEALTH_CONCURRENCY = 4

# A prompt that cannot possibly fit is better rejected locally with one clear
# message than sent and refused by the API with an opaque HTTP 400.
MAX_PROMPT_CHARS = 500_000

# --------------------------------------------------------------------------- #
# State: cooldowns and counters survive a client/server restart
# --------------------------------------------------------------------------- #


def _load_state() -> Dict[str, Any]:
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    """Best-effort atomic write.

    OpenCode and DSH can each run a server process against the same state file,
    so cooldowns are merged (latest expiry wins) instead of blindly replaced.
    A failed write must never break a model call.
    """
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        disk = _load_state()
        merged = dict(disk.get("cooldowns") or {})
        for key, until in (state.get("cooldowns") or {}).items():
            merged[key] = max(float(until), float(merged.get(key) or 0))
        now = time.time()
        merged = {k: v for k, v in merged.items() if float(v) > now}
        merged_last = dict(disk.get("last_call") or {})
        for key, stamp in (state.get("last_call") or {}).items():
            try:
                merged_last[key] = max(float(stamp), float(merged_last.get(key) or 0))
            except (TypeError, ValueError):
                continue
        merged_strikes = dict(disk.get("strikes") or {})
        for key, count in (state.get("strikes") or {}).items():
            try:
                merged_strikes[key] = max(int(count),
                                          int(merged_strikes.get(key) or 0))
            except (TypeError, ValueError):
                continue
        payload = {
            "version": 1,
            "saved_at": now,
            "cooldowns": merged,
            "stats": state.get("stats") or {},
            "last_call": merged_last,
            "strikes": merged_strikes,
        }
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Account store
# --------------------------------------------------------------------------- #


class Account:
    __slots__ = ("id", "email", "path", "access", "refresh", "expiry", "project",
                 "disabled", "blocked", "tier", "last_error")

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
        self.last_error = ""
        q = raw.get("quota") or {}
        self.tier = q.get("subscription_tier") or ""

    @property
    def usable(self) -> bool:
        # A refresh-only account is usable: the first call mints an access token.
        return (bool(self.access) or bool(self.refresh)) \
            and not self.disabled and not self.blocked


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
            LOG.debug("skipping unreadable account file %s", f.name)
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


def _refresh_token(acc: Account) -> Tuple[Optional[str], str]:
    """Exchange the refresh token for a new access token.

    Returns (access_token, error_kind). The kind matters: an invalid_grant means
    the credential is dead (penalize the account), while a network blip or a
    missing client id must not bench an otherwise healthy account for 15 minutes.
    """
    if not acc.refresh:
        return None, "no_refresh_token"
    if not CLIENT_ID or not CLIENT_SECRET:
        return None, "no_credentials"

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
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", "replace").lower()
        except Exception:
            detail = ""
        if exc.code in (400, 401) and ("invalid_grant" in detail or exc.code == 401):
            return None, "invalid_grant"
        return None, "network"
    except Exception:
        return None, "network"

    new_access = data.get("access_token")
    if not new_access:
        return None, "network"

    new_refresh = data.get("refresh_token") or acc.refresh
    expires_in = int(data.get("expires_in") or 3600)
    acc.access = new_access
    acc.refresh = new_refresh
    acc.expiry = int(time.time()) + expires_in
    _persist_rotated_token(acc, new_refresh, new_access, expires_in)
    return new_access, ""


async def _ensure_token(acc: Account) -> Optional[str]:
    """Return a valid access token, refreshing under a per-account lock.

    Google invalidates the previous refresh token when it rotates one. Two
    concurrent refreshes for the same account therefore race, and the loser
    persists a token that is already dead. Serializing per account prevents that.
    """
    if acc.access and acc.expiry - time.time() > REFRESH_MARGIN:
        POOL._last_errors.pop(acc.id, None)
        return acc.access
    async with POOL.refresh_lock(acc):
        # Another task may have refreshed while we waited for the lock.
        if acc.access and acc.expiry - time.time() > REFRESH_MARGIN:
            POOL._last_errors.pop(acc.id, None)
            return acc.access
        refreshed, error_kind = await asyncio.to_thread(_refresh_token, acc)
        acc.last_error = error_kind
        if refreshed:
            acc.last_error = ""
            POOL._last_errors.pop(acc.id, None)
            return refreshed
        if error_kind:
            POOL._last_errors[acc.id] = error_kind
        if error_kind not in ("", "no_refresh_token"):
            LOG.warning("token refresh failed for account %s: %s", acc.id, error_kind)
        # Refresh failed. A token with no recorded expiry may still be accepted,
        # but one known to be expired can only produce a 401: returning it would
        # burn an attempt and misattribute the failure to the account's quota.
        if acc.access and acc.expiry <= 0:
            return acc.access
        return None


# --------------------------------------------------------------------------- #
# Rotation
# --------------------------------------------------------------------------- #


class Pool:
    def __init__(self) -> None:
        self._cursor = 0
        self._cooldown: Dict[str, float] = {}
        self._refresh_locks: Dict[str, asyncio.Lock] = {}
        self._stats: Dict[str, Any] = {"ok": 0, "fail": 0, "calls": 0}
        # keyed by account id: survives the fresh Account objects created by
        # every _load_accounts() call, so status() can report the last reason
        self._last_errors: Dict[str, str] = {}
        # Traffic-shaping state. Kept on the pool (not on Account) because new
        # Account objects are built on every _load_accounts() call.
        self._last_call: Dict[str, float] = {}
        self._bucket: Dict[str, List[float]] = {}
        self._strikes: Dict[str, int] = {}
        self._acct_locks: Dict[str, asyncio.Lock] = {}
        self._gate_sem: Optional[asyncio.Semaphore] = None
        self._state_saved_at = 0.0
        self._restore(_load_state())

    def _restore(self, state: Dict[str, Any]) -> None:
        """Re-apply cooldowns from a previous process, dropping expired ones.

        Without this a restart forgets that an account was rate-limited minutes
        ago and sends the very first request straight back into a 429.
        """
        now = time.time()
        for key, until in (state.get("cooldowns") or {}).items():
            try:
                until_f = float(until)
            except (TypeError, ValueError):
                continue
            if until_f > now:
                self._cooldown[str(key)] = until_f
        stats = state.get("stats")
        if isinstance(stats, dict):
            for key in ("ok", "fail", "calls"):
                value = stats.get(key)
                if isinstance(value, int) and value >= 0:
                    self._stats[key] = value
        # Pacing and strikes survive a restart too: a fresh process that
        # forgets the last call time opens with the exact burst it meant to
        # avoid.
        for key, stamp in (state.get("last_call") or {}).items():
            try:
                self._last_call[str(key)] = float(stamp)
            except (TypeError, ValueError):
                continue
        for key, count in (state.get("strikes") or {}).items():
            try:
                value = int(count)
            except (TypeError, ValueError):
                continue
            if value > 0:
                self._strikes[str(key)] = value

    def snapshot(self) -> Dict[str, Any]:
        return {
            "cooldowns": dict(self._cooldown),
            "stats": dict(self._stats),
            "last_call": dict(self._last_call),
            "strikes": dict(self._strikes),
        }

    @staticmethod
    def _key(acc: Account, model: str) -> str:
        """Cooldown is per (account, model).

        A model that is dead or rate-limited must not take the account out of
        service for every other model. The empty-model key is a whole-account
        penalty, used only for auth failures where the credential itself is bad.
        """
        return f"{acc.id}:{model}" if model else acc.id

    def _cool_left(self, acc: Account, model: str) -> float:
        now = time.time()
        return max(self._cooldown.get(self._key(acc, model), 0),
                   self._cooldown.get(self._key(acc, ""), 0)) - now

    def _pick_order(self, accounts: List[Account],
                    model: str = "") -> List[Account]:
        ready = [a for a in accounts
                 if self._cool_left(a, model) <= 0]
        cooling = [a for a in accounts
                   if self._cool_left(a, model) > 0]
        if not ready:
            # everything cooling: use the least-cooled anyway
            return sorted(accounts, key=lambda a: self._cool_left(a, model))
        start = self._cursor % len(ready)
        rotated = ready[start:] + ready[:start]
        self._cursor = (self._cursor + 1) % max(1, len(ready))
        return rotated + cooling

    def refresh_lock(self, acc: Account) -> asyncio.Lock:
        """Serialize token refresh per account.

        Concurrent refreshes for the same account can invalidate the rotated
        refresh token Google just issued, so only one refresh may run at a time.
        """
        lock = self._refresh_locks.get(acc.id)
        if lock is None:
            lock = asyncio.Lock()
            self._refresh_locks[acc.id] = lock
        return lock

    def _penalize(self, acc: Account, seconds: int, model: str = "") -> None:
        self._cooldown[self._key(acc, model)] = time.time() + seconds
        # Persist sparingly: a 429 storm must not rewrite the state file on every
        # single request.
        now = time.time()
        if now - self._state_saved_at >= 5:
            self._state_saved_at = now
            _save_state(self.snapshot())

    @contextlib.asynccontextmanager
    async def gate(self, acc: Account):
        """Hold this account's turn, then a global in-flight slot.

        Two independent brakes: spacing per account (with jitter, so the gaps
        are not machine-regular) and a rolling 60s budget. Both are per account,
        never global, so the other eight accounts stay usable.

        Lock order is always account lock -> global semaphore. Waiting for an
        account's turn happens BEFORE taking a global slot: doing it the other
        way let one rate-limited account hold every slot while it slept, which
        stalled the whole pool behind a single account.
        """
        if self._gate_sem is None:
            self._gate_sem = asyncio.Semaphore(MAX_INFLIGHT)
        lock = self._acct_locks.get(acc.id)
        if lock is None:
            lock = asyncio.Lock()
            self._acct_locks[acc.id] = lock
        async with lock:
            # Also caps this account at one request in flight, which is what a
            # human client would do anyway.
            gap = PACE_MIN_INTERVAL + random.uniform(0, PACE_JITTER)
            waited = 0.0
            while True:
                now = time.time()
                wait = 0.0
                last = self._last_call.get(acc.id, 0.0)
                if last:
                    wait = max(wait, last + gap - now)
                window = [t for t in self._bucket.get(acc.id, []) if now - t < 60.0]
                self._bucket[acc.id] = window
                if len(window) >= ACCOUNT_RPM:
                    wait = max(wait, window[0] + 60.0 - now)
                # Sleep in short hops instead of one long sleep. After
                # GATE_MAX_WAIT the call goes out anyway rather than dying past
                # the client's own timeout.
                if wait <= 0 or waited >= GATE_MAX_WAIT:
                    break
                step = min(wait, 2.0)
                await asyncio.sleep(step)
                waited += step
            async with self._gate_sem:
                stamp = time.time()
                self._last_call[acc.id] = stamp
                self._bucket.setdefault(acc.id, []).append(stamp)
                yield

    def note_success(self, acc: Account, model: str = "") -> None:
        """A good answer relaxes the backoff again."""
        self._strikes.pop(self._key(acc, model), None)
        self._strikes.pop(acc.id, None)
        self._cooldown.pop(self._key(acc, model), None)

    def strike_rate(self, acc: Account, model: str,
                    retry_after: Optional[float] = None) -> int:
        """Escalating cooldown for 429s, honouring the upstream Retry-After.

        A flat 5-minute freeze regardless of how often the account was throttled
        makes the client look like it will keep hammering. Doubling each time is
        the behaviour a human client would show.
        """
        key = self._key(acc, model)
        n = self._strikes.get(key, 0) + 1
        self._strikes[key] = n
        seconds = float(COOLDOWN_RATE_LIMIT * (2 ** min(n - 1, 5)))
        if retry_after:
            seconds = max(seconds, float(retry_after))
        seconds = int(min(seconds, float(STRIKE_CAP_SECONDS))
                      + random.uniform(0, 30))
        self._penalize(acc, seconds, model)
        return seconds


POOL = Pool()


def _token_penalty(acc: Account) -> int:
    """Cooldown to apply after a token could not be obtained."""
    return (COOLDOWN_TRANSIENT
            if acc.last_error in ("network", "no_credentials", "no_refresh_token")
            else COOLDOWN_AUTH)


def _http_penalty(code: Optional[int], detail: str, model: str) -> Tuple[int, str]:
    """Cooldown seconds and scope ("account" or "model") for an HTTP failure.

    Account-wide punishment is reserved for failures of the credential itself.
    A 429, a 5xx or a model-scoped 403 says nothing about the account's other
    models, so penalizing the whole account over one of them strands the pool.
    """
    detail_l = (detail or "").lower()
    if code is None:
        return COOLDOWN_TRANSIENT, "model"
    if code == 401:
        return COOLDOWN_AUTH, "account"
    if code == 403:
        if any(word in detail_l for word in ("credential", "invalid_grant",
                                             "unauthenticated", "invalid_token")):
            return COOLDOWN_AUTH, "account"
        return COOLDOWN_AUTH, "model"
    if code == 429:
        return COOLDOWN_RATE_LIMIT, "model"
    return COOLDOWN_TRANSIENT, "model"


# --------------------------------------------------------------------------- #
# API call
# --------------------------------------------------------------------------- #


def _build_payload(acc: Account, model: str, contents: List[Dict[str, Any]],
                   system: str, temperature: float, max_tokens: int,
                   json_mode: bool,
                   thinking_budget: Optional[int] = None,
                   tools: Optional[List[Dict[str, Any]]] = None) -> bytes:
    gen: Dict[str, Any] = {"temperature": temperature, "maxOutputTokens": max_tokens}
    if json_mode:
        gen["responseMimeType"] = "application/json"
    # Reasoning models happily burn the entire output budget on hidden thoughts
    # and then return an empty candidate with finishReason=MAX_TOKENS. Capping
    # the thinking budget is the only reliable way to get text back from a
    # tool-grounded call (google_search especially).
    if thinking_budget is not None:
        gen["thinkingConfig"] = {"thinkingBudget": int(thinking_budget)}

    request: Dict[str, Any] = {"contents": contents, "generationConfig": gen}
    if tools:
        request["tools"] = tools
    if system:
        request["systemInstruction"] = {"parts": [{"text": system}]}

    return json.dumps({
        "project": acc.project,
        "model": model,
        "userAgent": "antigravity",
        "requestId": f"agent/oc-{int(time.time() * 1000)}/{random.randint(0, 9999)}/1",
        "request": request,
    }).encode()


def _parse_retry_after(raw: Any) -> Optional[float]:
    """Retry-After as seconds, from either delta-seconds or an HTTP-date."""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return max(0.0, min(float(text), float(RETRY_AFTER_CAP)))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(text)
        return max(0.0, min(when.timestamp() - time.time(),
                            float(RETRY_AFTER_CAP)))
    except Exception:
        return None


def _post_ex(token: str, payload: bytes,
             timeout: int = REQUEST_TIMEOUT
             ) -> Tuple[Optional[Dict[str, Any]], Optional[int], str,
                        Optional[float]]:
    """POST, returning (body, status, detail, retry_after).

    retry_after is the upstream's own backoff request. Ignoring it and retrying
    on the fixed cooldown is exactly the behaviour that turns a soft throttle
    into an account review.
    """
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
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace")), 200, "", None
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:400]
        except Exception:
            detail = ""
        retry_after = _parse_retry_after(e.headers.get("Retry-After")
                                         if e.headers else None)
        return None, e.code, detail, retry_after
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}", None


def _post(token: str, payload: bytes,
          timeout: int = REQUEST_TIMEOUT) -> Tuple[Optional[Dict[str, Any]], Optional[int], str]:
    body, code, detail, _ = _post_ex(token, payload, timeout)
    return body, code, detail


def _extract_text(body: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    resp = body.get("response") if isinstance(body.get("response"), dict) else body
    candidates = resp.get("candidates") or []
    text = ""
    finish = ""
    # finishReason must be captured even when no text was produced. A thinking
    # model that burns its whole budget returns MAX_TOKENS with an empty part
    # list; if we only recorded `finish` alongside text, that case looked like a
    # blank "empty-response" and got misdiagnosed as a broken account.
    for cand in candidates:
        if not finish:
            finish = cand.get("finishReason") or ""
        parts = (cand.get("content") or {}).get("parts") or []
        chunk = "".join(p.get("text", "") for p in parts if isinstance(p, dict))
        if chunk.strip():
            text = chunk
            finish = cand.get("finishReason") or finish
            break
    usage = resp.get("usageMetadata") or body.get("usageMetadata") or {}
    grounding = None
    for cand in candidates:
        gm = cand.get("groundingMetadata")
        if not gm:
            continue
        chunks = gm.get("groundingChunks") or []
        sources = []
        for ch in chunks:
            web = (ch or {}).get("web") or {}
            if web.get("uri"):
                sources.append({"title": web.get("title") or "", "uri": web.get("uri")})
        grounding = {
            "queries": gm.get("webSearchQueries") or [],
            "sources": sources,
        }
        break
    meta: Dict[str, Any] = {
        "finish_reason": finish,
        "tokens": usage.get("totalTokenCount"),
        "prompt_tokens": usage.get("promptTokenCount"),
        "output_tokens": usage.get("candidatesTokenCount"),
        "thought_tokens": usage.get("thoughtsTokenCount"),
    }
    # A safety refusal produces an empty candidate, exactly like a burned
    # thinking budget. Telling them apart matters: retrying accounts cannot fix
    # a blocked prompt, and penalizing accounts for it hides the real cause.
    feedback = resp.get("promptFeedback") or {}
    blocked = feedback.get("blockReason") or ""
    if not blocked and str(finish).upper() in ("SAFETY", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII"):
        blocked = str(finish).upper()
    if blocked:
        meta["blocked"] = blocked
    if grounding:
        meta["grounding"] = grounding
    return text.strip(), meta


def _resolve_model(name: str) -> str:
    if not name:
        return DEFAULT_MODEL
    key = name.strip().lower()
    # Model ids are lowercase on this backend; normalizing them makes a
    # case-variant spelling dedupe instead of being probed twice.
    return MODEL_ALIASES.get(key, key)


def _clamp_tokens(value: Any, default: int = 8192) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return max(MIN_OUTPUT_TOKENS, min(MAX_OUTPUT_TOKENS, n))


def _clamp_temperature(value: Any, default: float = 0.7) -> float:
    try:
        t = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(2.0, t))


def _clamp_thinking(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    try:
        return max(0, min(MAX_OUTPUT_TOKENS, int(value)))
    except (TypeError, ValueError):
        return None


def _looks_retired(text: str) -> bool:
    """True when a 200 response is really a one-line retirement notice.

    This API answers HTTP 200 with a hard-coded "no longer available" sentence
    for retired models. Only short responses are treated as notices, so a real
    answer that happens to contain one of the markers is not discarded.
    """
    stripped = text.strip()
    lowered = stripped.lower()
    if not lowered or len(lowered) > 200:
        return False
    # A retirement notice is one sentence that names the model/platform
    # situation. "That flag is not supported in v2." is a real answer.
    if stripped.count("\n") > 1 or lowered.count(". ") > 1:
        return False
    return any(marker in lowered for marker in DEPRECATED_MARKERS)


# Gemini accepts images inline as base64. The hard ceiling is the request size,
# not cost, so keep the guard generous: base64 inflates payloads by ~33%, so we
# cap raw bytes at 12 MB per image and 14 MB per call (≈19 MB on the wire).
MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_TOTAL_IMAGE_BYTES = 14 * 1024 * 1024
IMAGE_SUFFIX_MIME = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".heic": "image/heic",
    ".heif": "image/heif",
}


def _sniff_image_mime(blob: bytes) -> str:
    if blob[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if blob[:2] == b"\xff\xd8":
        return "image/jpeg"
    if blob[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if blob[:4] == b"RIFF" and blob[8:12] == b"WEBP":
        return "image/webp"
    if blob[:2] == b"BM":
        return "image/bmp"
    return ""


def _image_part(spec: str) -> Dict[str, Any]:
    """Turn a path, data URI or http(s) URL into a Gemini inlineData part."""
    if not spec or not str(spec).strip():
        raise ValueError("empty image reference")
    spec = str(spec).strip()

    if spec.startswith("data:"):
        header, _, payload = spec.partition(",")
        if ";base64" not in header:
            raise ValueError("data URI must be base64 encoded")
        mime = header[5:].split(";")[0] or "image/png"
        try:
            blob = base64.b64decode(payload, validate=True)
        except Exception as exc:
            raise ValueError(f"invalid base64 in data URI: {exc}") from exc
        return {"inlineData": {"mimeType": mime,
                               "data": base64.b64encode(blob).decode()}}

    if spec.lower().startswith(("http://", "https://")):
        req = urllib.request.Request(spec, headers={"User-Agent": USER_AGENT})
        suffix = ""
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                blob = r.read(MAX_IMAGE_BYTES + 1)
                mime = (r.headers.get("Content-Type") or "").split(";")[0].strip()
        except Exception as exc:
            raise ValueError(f"cannot download image: {exc}") from exc
    else:
        path = Path(os.path.expandvars(os.path.expanduser(spec)))
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.is_file():
            raise ValueError(f"image not found: {path}")
        blob = path.read_bytes()
        mime = ""
        suffix = path.suffix.lower()

    if len(blob) > MAX_IMAGE_BYTES:
        raise ValueError(
            f"image is {len(blob) / 1048576:.1f} MB, over the "
            f"{MAX_IMAGE_BYTES / 1048576:.0f} MB inline limit. Resize it first.")
    if not mime.startswith("image/"):
        mime = (IMAGE_SUFFIX_MIME.get(suffix, "") or _sniff_image_mime(blob)
                or mimetypes.guess_type(spec)[0] or "image/png")
    if not mime.startswith("image/"):
        raise ValueError(f"not an image: {spec}")

    return {"inlineData": {"mimeType": mime,
                           "data": base64.b64encode(blob).decode()}}


def _image_parts(specs: List[str]) -> List[Dict[str, Any]]:
    """Build inline image parts and enforce the per-call payload budget.

    Cost is not a concern here, but request size is: the API rejects oversized
    bodies outright. Track the running total so a folder of full-res screenshots
    fails with one clear message instead of a bare HTTP 400.
    """
    parts: List[Dict[str, Any]] = []
    total = 0
    for spec in specs:
        part = _image_part(spec)
        blob_len = len(part["inlineData"]["data"]) * 3 // 4
        total += blob_len
        if total > MAX_TOTAL_IMAGE_BYTES:
            raise ValueError(
                f"images total {total / 1048576:.1f} MB, over the "
                f"{MAX_TOTAL_IMAGE_BYTES / 1048576:.0f} MB per-call limit. "
                "Send fewer at once, or downscale them.")
        parts.append(part)
    return parts




# --------------------------------------------------------------------------- #
# Web fetch (local, so url does not depend on an upstream tool)
# --------------------------------------------------------------------------- #

WEB_USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# Pasting a whole site into the prompt burns the context window for no gain.
MAX_FETCH_CHARS = 20000
MAX_FETCH_BYTES = 6_000_000


def _html_to_text(raw: str) -> str:
    """Strip markup down to readable text, keeping paragraph breaks."""
    raw = re.sub(r"(?is)<(script|style|noscript|svg|template)[^>]*>.*?</\1>", " ", raw)
    raw = re.sub(r"(?is)<!--.*?-->", " ", raw)
    raw = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    raw = re.sub(r"(?i)</(p|div|li|tr|h[1-6]|section|article|header|footer|pre|blockquote|table)>",
                 "\n", raw)
    text = re.sub(r"(?s)<[^>]+>", " ", raw)
    return html_lib.unescape(text)


def _ssl_context() -> "ssl.SSLContext":
    """CA bundle for public https. This Python build ships none of its own."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


_FETCH_CACHE: Dict[str, Tuple[float, str, int]] = {}
FETCH_CACHE_TTL = 600
FETCH_CACHE_MAX = 64


def _fetch_readable(url: str, limit: int = MAX_FETCH_CHARS) -> Tuple[str, int]:
    """Download one URL and return (readable_text, untruncated_length)."""
    if not url.lower().startswith(("http://", "https://")):
        raise ValueError("only http(s) URLs are allowed")

    cache_key = f"{limit}|{url}"
    hit = _FETCH_CACHE.get(cache_key)
    if hit and time.time() - hit[0] < FETCH_CACHE_TTL:
        return hit[1], hit[2]

    req = urllib.request.Request(url, headers={
        "User-Agent": WEB_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.5",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    })
    # This Python build ships no default CA store, so urllib fails every public
    # https site with CERTIFICATE_VERIFY_FAILED. certifi is installed; use it.
    context = _ssl_context()
    with urllib.request.urlopen(req, timeout=45, context=context) as r:
        ctype = (r.headers.get("Content-Type") or "").lower()
        charset = r.headers.get_content_charset() or "utf-8"
        raw = r.read(MAX_FETCH_BYTES)

    body = raw.decode(charset, "replace")
    head = body.lstrip()[:200].lower()
    text = _html_to_text(body) if ("html" in ctype or head.startswith(("<!doctype", "<html"))) else body

    text = re.sub(r"[ \t\xa0]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    total = len(text)
    if total > limit:
        text = text[:limit] + f"\n\n[... truncated, {total} chars total]"

    # Cache the page for a short while: an agent often reads the same URL twice
    # (search -> fetch -> verify), and refetching wastes 10-20 seconds each time.
    if len(_FETCH_CACHE) >= FETCH_CACHE_MAX:
        oldest = min(_FETCH_CACHE, key=lambda k: _FETCH_CACHE[k][0])
        _FETCH_CACHE.pop(oldest, None)
    _FETCH_CACHE[cache_key] = (time.time(), text, total)
    return text, total


# --------------------------------------------------------------------------- #
# Agent loop: the model calls local tools through function calling
# --------------------------------------------------------------------------- #

MAX_AGENT_STEPS = 8
MAX_AGENT_TOOL_CHARS = 12000

# Commands that are never worth letting a model run unattended.
SHELL_DENY = re.compile(
    r"(?i)\b(rm\s+-rf\s+/|rm\s+-rf\s+~|mkfs|diskpart|format\s+[a-z]:|"
    r"shutdown|logout|restart-computer|stop-computer|bcdedit|"
    r"Remove-Item\s+[^|;]*-Recurse[^|;]*(?:[A-Za-z]:\\?(?:\s|$)|~)|"
    r"reg\s+delete|takeown|icacls\s+[^|;]*/reset|"
    r"del\s+(?:/[a-z]+\s+)*[a-z]:\\|rmdir\s+/s|"
    r"(?:curl|wget|iwr|invoke-webrequest)[^|;]*\|\s*(?:sh|bash|iex|powershell)|"
    r"powershell(?:\s+-\w+)*\s+-e(?:nc|ncodedcommand))")
SHELL_DEFAULT_CWD = Path(os.environ.get("TEMP", ".")) / "opencode" / "agent-shell"


def _clip(text: str, limit: int = MAX_AGENT_TOOL_CHARS) -> str:
    text = str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n[... {len(text)} chars total, truncated]"


def _web_search_local(query: str, limit: int = 6) -> List[Dict[str, str]]:
    """Search via DuckDuckGo lite, which tolerates plain HTTP clients."""
    url = "https://lite.duckduckgo.com/lite/?q=" + urllib.parse.quote_plus(query)
    req = urllib.request.Request(url, headers={"User-Agent": WEB_USER_AGENT})
    with urllib.request.urlopen(req, timeout=45, context=_ssl_context()) as r:
        raw = r.read(MAX_FETCH_BYTES).decode("utf-8", "replace")

    hits: List[Dict[str, str]] = []
    for tag, body in re.findall(r"(?is)(<a\b[^>]*class=['\"]result-link['\"][^>]*>)(.*?)</a>", raw):
        href = re.search(r'href="([^"]+)"', tag)
        link = html_lib.unescape(href.group(1)) if href else ""
        redirect = re.search(r"[?&]uddg=([^&]+)", link)
        if redirect:
            link = urllib.parse.unquote(redirect.group(1))
        if link.startswith("//"):
            link = "https:" + link
        hits.append({"title": _html_to_text(body).strip()[:200], "url": link})
        if len(hits) >= limit:
            break

    snippets = [_html_to_text(s).strip()[:300]
                for s in re.findall(r"(?is)class=['\"]result-snippet['\"][^>]*>(.*?)</td>", raw)]
    for i, snip in enumerate(snippets[:len(hits)]):
        if snip:
            hits[i]["snippet"] = snip
    return hits


def _tool_declarations(allow_write: bool, allow_shell: bool) -> List[Dict[str, Any]]:
    decls = [
        {"name": "web_search",
         "description": "Search the live web. Returns titles, URLs and snippets. Use it to find pages before web_fetch.",
         "parameters": {"type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"]}},
        {"name": "web_fetch",
         "description": "Download one http(s) URL and return its readable text.",
         "parameters": {"type": "object",
                        "properties": {"url": {"type": "string"}},
                        "required": ["url"]}},
        {"name": "read_file",
         "description": ("Read a text file. For big files pass start_line (1-based) and "
                         "max_lines to page through it; the reply reports total_lines."),
         "parameters": {"type": "object",
                        "properties": {"path": {"type": "string"},
                                       "start_line": {"type": "integer"},
                                       "max_lines": {"type": "integer"}},
                        "required": ["path"]}},
        {"name": "list_dir",
         "description": "List entries of a directory.",
         "parameters": {"type": "object",
                        "properties": {"path": {"type": "string"}},
                        "required": ["path"]}},
    ]
    if allow_write:
        decls.append({"name": "write_file",
                      "description": "Write text to a file, creating it if needed.",
                      "parameters": {"type": "object",
                                     "properties": {"path": {"type": "string"},
                                                    "content": {"type": "string"}},
                                     "required": ["path", "content"]}})
    if allow_shell:
        decls.append({"name": "run_shell",
                      "description": "Run a shell command and return stdout/stderr.",
                      "parameters": {"type": "object",
                                     "properties": {"command": {"type": "string"}},
                                     "required": ["command"]}})
    return [{"functionDeclarations": decls}]


async def _execute_tool(name: str, args: Dict[str, Any],
                        allow_write: bool, allow_shell: bool) -> Dict[str, Any]:
    """Run one local tool for the agent loop.

    Every blocking call is pushed to a worker thread. This loop shares the MCP
    event loop with every other in-flight request, so a synchronous 45 s download
    here would freeze fan-out subtasks and health probes as well.
    """
    args = args or {}
    try:
        if name == "web_search":
            query = str(args.get("query") or "").strip()
            if not query:
                return {"error": "query is required"}
            results = await asyncio.to_thread(_web_search_local, query)
            return {"query": query, "results": results}
        if name == "web_fetch":
            url = str(args.get("url") or "").strip()
            text, total = await asyncio.to_thread(_fetch_readable, url)
            return {"url": url, "chars": total, "text": _clip(text)}
        if name == "read_file":
            raw_path = str(args.get("path") or "")
            start = max(1, int(args.get("start_line") or 1))
            want = max(1, min(int(args.get("max_lines") or 400), 2000))

            def _read() -> Dict[str, Any]:
                path = Path(os.path.expanduser(raw_path))
                if not path.is_file():
                    return {"error": f"file not found: {path}"}
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
                chunk = lines[start - 1: start - 1 + want]
                return {"path": str(path), "total_lines": len(lines),
                        "start_line": start, "returned_lines": len(chunk),
                        "text": _clip("\n".join(chunk))}

            return await asyncio.to_thread(_read)
        if name == "list_dir":
            raw_path = str(args.get("path") or ".")

            def _list() -> Dict[str, Any]:
                path = Path(os.path.expanduser(raw_path))
                if not path.is_dir():
                    return {"error": f"not a directory: {path}"}
                entries = sorted(p.name for p in path.iterdir())[:200]
                return {"path": str(path), "entries": entries}

            return await asyncio.to_thread(_list)
        if name == "write_file" and allow_write:
            raw_path = str(args.get("path") or "")
            content = str(args.get("content") or "")

            def _write() -> Dict[str, Any]:
                path = Path(os.path.expanduser(raw_path))
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
                return {"path": str(path), "bytes": path.stat().st_size}

            return await asyncio.to_thread(_write)
        if name == "run_shell" and allow_shell:
            cmd = str(args.get("command") or "").strip()
            if not cmd:
                return {"error": "command is required"}
            if SHELL_DENY.search(cmd):
                return {"error": "command blocked by the safety guard "
                                 "(destructive pattern detected)"}
            # Guardrail, not a sandbox: commands run as this user. The working
            # directory is fixed to the temp folder; honoring a model-supplied
            # cwd would let a relative path escape it.
            workdir = Path(SHELL_DEFAULT_CWD)
            workdir.mkdir(parents=True, exist_ok=True)
            proc = await asyncio.create_subprocess_shell(
                cmd, cwd=str(workdir),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                out, err = await asyncio.wait_for(proc.communicate(), timeout=120)
            except asyncio.TimeoutError:
                proc.kill()
                return {"error": "command timed out after 120s"}
            return {"exit_code": proc.returncode, "cwd": str(workdir),
                    "stdout": _clip(out.decode("utf-8", "replace"), 4000),
                    "stderr": _clip(err.decode("utf-8", "replace"), 2000)}
        return {"error": f"unknown or disallowed tool: {name}"}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def _response_parts(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    resp = body.get("response") if isinstance(body.get("response"), dict) else body
    cands = resp.get("candidates") or []
    if not cands:
        return []
    return ((cands[0].get("content") or {}).get("parts") or [])


def _parts_text(parts: List[Dict[str, Any]]) -> str:
    return "".join(p.get("text", "") for p in parts if isinstance(p, dict)).strip()


async def _agent_run(task: str, model: str, system: str, max_steps: int,
                     max_tokens: int, allow_write: bool,
                     allow_shell: bool) -> Dict[str, Any]:
    """Run a bounded tool-calling loop, then force a final answer.

    Each step asks the pool for the best account right now instead of assuming
    account N is healthy: a rate-limited account is skipped rather than retried
    into a second 429. Two consecutive steps that produce nothing usable end the
    run instead of burning the whole step budget.
    """
    accounts, err = _load_accounts()
    if err:
        return {"status": "ERROR", "error": err}
    if not accounts:
        return {"status": "ERROR", "error": "No usable accounts in the pool."}

    model = _resolve_model(model)
    max_tokens = _clamp_tokens(max_tokens)
    tools = _tool_declarations(allow_write, allow_shell)
    contents: List[Dict[str, Any]] = [
        {"role": "user", "parts": [{"text": task}]}]
    trace: List[Dict[str, Any]] = []
    started_at = time.time()
    steps = max(1, min(int(max_steps or MAX_AGENT_STEPS), 20))

    def _remaining() -> float:
        return CALL_DEADLINE_SECONDS - (time.time() - started_at)

    def _finish(text: str, acc: Account, step: int, **extra: Any) -> Dict[str, Any]:
        return {"status": "OK", "text": text, "model": model, "account": acc.email,
                "steps": step + 1,
                "latency_ms": int((time.time() - started_at) * 1000),
                "trace": trace, **extra}

    consecutive_failures = 0
    for step in range(steps):
        last_http_error: Optional[Dict[str, Any]] = None
        if _remaining() <= 0:
            return {"status": "TIMEOUT", "error": "agent deadline reached",
                    "steps": step, "trace": trace}

        forced = step == steps - 1
        if forced:
            contents.append({"role": "user", "parts": [{"text": (
                "Tool budget exhausted. Do not call any more tools. Answer now "
                "using only what you already gathered, and say what is uncertain.")}]})

        order = POOL._pick_order(accounts, model)[:MAX_ATTEMPTS]
        advanced = False
        for acc in order:
            if _remaining() <= 0:
                break
            token = await _ensure_token(acc)
            if not token:
                trace.append({"step": step, "error": "no-token",
                              "why": acc.last_error or "expired",
                              "account": acc.email})
                POOL._penalize(acc, _token_penalty(acc))
                continue

            payload = _build_payload(acc, model, contents, system, 0.2, max_tokens,
                                     False, None, None if forced else tools)
            async with POOL.gate(acc):
                body, code, detail = await asyncio.to_thread(_post, token, payload)
            if body is None or code != 200:
                trace.append({"step": step, "http": code,
                              "detail": _clip(detail, 300), "account": acc.email})
                seconds, scope = _http_penalty(code, detail or "", model)
                POOL._penalize(acc, seconds, "" if scope == "account" else model)
                last_http_error = {"http": code, "detail": _clip(detail, 200)}
                continue

            parts = _response_parts(body)
            text = _parts_text(parts)
            calls = [p["functionCall"] for p in parts
                     if isinstance(p, dict) and "functionCall" in p]

            if forced:
                if text:
                    return _finish(text, acc, step, forced_answer=True)
                trace.append({"step": step, "error": "empty final answer",
                              "account": acc.email})
                POOL._penalize(acc, COOLDOWN_TRANSIENT, model)
                continue

            if not calls:
                if text:
                    return _finish(text, acc, step)
                trace.append({"step": step, "error": "empty-response",
                              "account": acc.email})
                POOL._penalize(acc, COOLDOWN_TRANSIENT, model)
                continue

            # Echo the model turn back verbatim: thinking models attach a
            # thoughtSignature that the next request must carry.
            contents.append({"role": "model", "parts": parts})
            responses = []
            for call in calls:
                tool_name = str(call.get("name") or "")
                tool_args = call.get("args") or {}
                result = await _execute_tool(tool_name, tool_args,
                                             allow_write, allow_shell)
                trace.append({"step": step, "tool": tool_name, "args": tool_args,
                              "result": _clip(json.dumps(result, ensure_ascii=False), 400)})
                responses.append({"functionResponse": {"name": tool_name,
                                                       "response": result}})
            contents.append({"role": "user", "parts": responses})
            advanced = True
            break

        if forced:
            if last_http_error:
                return {"status": "ERROR",
                        "error": (f"API rejected the final-answer request "
                                  f"(HTTP {last_http_error['http']})"),
                        "detail": last_http_error["detail"],
                        "model": model, "steps": step + 1, "trace": trace}
            break
        if advanced:
            consecutive_failures = 0
            continue
        consecutive_failures += 1
        if consecutive_failures >= 2:
            return {"status": "EXHAUSTED",
                    "error": "two consecutive steps produced no usable response",
                    "model": model, "steps": step + 1, "trace": trace}

    return {"status": "STEP_LIMIT",
            "error": f"stopped after {steps} steps without a final answer",
            "model": model, "trace": trace}


async def _chat_once(prompt_or_contents: Any, model: str, system: str = "",
                     temperature: float = 0.7, max_tokens: int = 8192,
                     json_mode: bool = False,
                     history: Optional[List[Dict[str, Any]]] = None,
                     thinking_budget: Optional[int] = None,
                     tools: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Run one prompt across the pool, rotating on failure.

    Rotation rules: only account-scoped failures (401/403, 429, timeouts, 5xx)
    move to the next account. Model-scoped answers -- retired model, burned
    thinking budget, safety block -- fail immediately with the real cause,
    because every account will answer identically and rotating only burns quota.
    """
    accounts, err = _load_accounts()
    if err:
        return {"status": "ERROR", "error": err}
    if not accounts:
        return {"status": "ERROR", "error": "No usable accounts in the pool."}

    model = _resolve_model(model)
    temperature = _clamp_temperature(temperature)
    max_tokens = _clamp_tokens(max_tokens)
    thinking_budget = _clamp_thinking(thinking_budget)

    if isinstance(prompt_or_contents, list):
        contents = prompt_or_contents
    else:
        contents = list(history or []) + [
            {"role": "user", "parts": [{"text": str(prompt_or_contents)}]}
        ]

    order = POOL._pick_order(accounts, model)
    attempts: List[Dict[str, Any]] = []
    tried = 0
    bumped = False
    started_at = time.time()

    def _remaining() -> float:
        return CALL_DEADLINE_SECONDS - (time.time() - started_at)

    def _attempt_timeout() -> int:
        # Never hand the client a request that could outlive its own timeout.
        return int(max(30, min(REQUEST_TIMEOUT, _remaining())))

    def _truncated(account: str, why: str) -> Dict[str, Any]:
        POOL._stats["fail"] += 1
        return {
            "status": "TRUNCATED",
            "error": (f"Model '{model}' produced no text at max_tokens={max_tokens} "
                      f"({why})."),
            "model": model,
            "account": account,
            "hint": ("Raise max_tokens or lower thinking_budget; the reasoning "
                     "budget consumed the whole output. Not an account failure."),
            "attempts": attempts,
        }

    for acc in order:
        if _remaining() <= 0:
            attempts.append({"stopped": f"CALL_DEADLINE_SECONDS={CALL_DEADLINE_SECONDS} reached"})
            break
        if tried >= MAX_ATTEMPTS:
            attempts.append({"stopped": f"MAX_ATTEMPTS={MAX_ATTEMPTS} reached"})
            break
        tried += 1

        token = await _ensure_token(acc)
        if not token:
            attempts.append({"account": acc.email, "error": "no-token",
                             "why": acc.last_error or "expired"})
            POOL._penalize(acc, _token_penalty(acc))
            continue

        payload = _build_payload(acc, model, contents, system, temperature,
                                 max_tokens, json_mode, thinking_budget, tools)
        started = time.time()
        async with POOL.gate(acc):
            body, code, detail, retry_after = await asyncio.to_thread(
                _post_ex, token, payload, _attempt_timeout())
        elapsed = int((time.time() - started) * 1000)

        if body is not None and code == 200:
            text, meta = _extract_text(body)

            if not text:
                finish = str(meta.get("finish_reason") or "").upper()

                if meta.get("blocked"):
                    POOL._stats["fail"] += 1
                    return {
                        "status": "BLOCKED",
                        "error": f"Prompt blocked upstream ({meta['blocked']}).",
                        "model": model,
                        "account": acc.email,
                        "hint": ("Rephrase the prompt; a safety block is not an "
                                 "account failure and rotating accounts cannot fix it."),
                        "attempts": attempts,
                    }

                # Empty output with MAX_TOKENS means hidden reasoning consumed the
                # whole budget. Every account behaves the same way, so rotating is
                # pointless and penalizing is wrong: retry once with a bigger floor.
                if finish == "MAX_TOKENS":
                    if bumped:
                        return _truncated(acc.email, "finishReason=MAX_TOKENS")
                    new_max = min(MAX_OUTPUT_TOKENS,
                                  max(MIN_OUTPUT_TOKENS, max_tokens * 4))
                    if new_max <= max_tokens or _remaining() <= 30:
                        return _truncated(acc.email, "no room left to retry bigger")
                    bumped = True
                    attempts.append({
                        "account": acc.email,
                        "error": "truncated-before-output",
                        "finish": finish,
                        "retry_max_tokens": new_max,
                    })
                    max_tokens = new_max
                    payload = _build_payload(acc, model, contents, system, temperature,
                                             max_tokens, json_mode, thinking_budget, tools)
                    async with POOL.gate(acc):
                        body, code, detail, retry_after = await asyncio.to_thread(
                            _post_ex, token, payload, _attempt_timeout())
                    elapsed = int((time.time() - started) * 1000)
                    if body is None or code != 200:
                        attempts.append({"account": acc.email, "http": code,
                                         "detail": (detail or "")[:200],
                                         "after_bump": True})
                        if code is None or code in ROTATE_STATUS:
                            if code == 429:
                                POOL.strike_rate(acc, model, retry_after)
                            else:
                                seconds, scope = _http_penalty(code, detail or "", model)
                                if code == 503 and retry_after:
                                    seconds = max(seconds, int(retry_after))
                                POOL._penalize(acc, seconds,
                                               "" if scope == "account" else model)
                        continue
                    text, meta = _extract_text(body)
                    if not text:
                        return _truncated(acc.email, "still no text after raising max_tokens")
                    if _looks_retired(text):
                        POOL._stats["fail"] += 1
                        return {
                            "status": "MODEL_UNAVAILABLE",
                            "error": f"Model '{model}' is retired: {text[:160]}",
                            "model": model,
                            "hint": f"Use {_t('health')} to list models that actually work.",
                            "attempts": attempts,
                        }
                    POOL.note_success(acc, model)
                    POOL._stats["ok"] += 1
                    POOL._stats["calls"] += 1
                    return {
                        "status": "OK",
                        "text": text,
                        "model": model,
                        "account": acc.email,
                        "latency_ms": elapsed,
                        **meta,
                        "note": f"max_tokens auto-raised to {max_tokens}",
                        "attempts": attempts,
                    }

                # Genuine empty response (transient upstream blip): another
                # account may do better.
                POOL._stats["fail"] += 1
                attempts.append({"account": acc.email, "error": "empty-response",
                                 "finish": finish})
                POOL._penalize(acc, COOLDOWN_TRANSIENT, model)
                continue

            if _looks_retired(text):
                POOL._stats["fail"] += 1
                return {
                    "status": "MODEL_UNAVAILABLE",
                    "error": f"Model '{model}' is retired: {text[:160]}",
                    "model": model,
                    "hint": f"Use {_t('health')} to list models that actually work.",
                    "attempts": attempts,
                }

            POOL.note_success(acc, model)
            POOL._stats["ok"] += 1
            POOL._stats["calls"] += 1
            return {
                "status": "OK",
                "text": text,
                "model": model,
                "account": acc.email,
                "latency_ms": elapsed,
                **meta,
                "truncated": str(meta.get("finish_reason") or "").upper() == "MAX_TOKENS",
                "attempts": attempts,
            }

        # Failure. Rotate only when the failure is about this account.
        POOL._stats["fail"] += 1
        attempts.append({"account": acc.email, "http": code, "detail": (detail or "")[:200]})

        if code in ROTATE_STATUS:
            if code == 429:
                # The strongest signal there is: this account is already being
                # throttled. Escalate instead of the flat 5-minute freeze.
                POOL.strike_rate(acc, model, retry_after)
                continue
            seconds, scope = _http_penalty(code, detail or "", model)
            if code == 503 and retry_after:
                seconds = max(seconds, int(retry_after))
            POOL._penalize(acc, seconds, "" if scope == "account" else model)
            if code == 503 and "no capacity" in (detail or "").lower():
                # A per-model backend shortage fails identically everywhere;
                # penalizing the account for it took a whole pool offline before.
                break
            continue
        if code is None:
            POOL._penalize(acc, COOLDOWN_TRANSIENT, model)
            continue
        # A non-rotatable client error will fail everywhere: stop with the cause.
        hint = ""
        if code == 400:
            hint = ("Request rejected as malformed: check that contents alternate "
                    "user/model and that the prompt is not oversized.")
        return {
            "status": "ERROR",
            "error": f"API rejected the request (HTTP {code})",
            "detail": (detail or "")[:400],
            "model": model,
            "hint": hint,
            "attempts": attempts,
        }

    # Every attempt failed the same way on the same model: report the real cause
    # instead of a generic EXHAUSTED that reads like "the pool is down".
    if attempts and all(a.get("http") == 503 and "no capacity" in (a.get("detail") or "").lower()
                        for a in attempts if "account" in a) and any("account" in a for a in attempts):
        POOL._stats["fail"] += 1
        return {
            "status": "MODEL_UNAVAILABLE",
            "error": f"Model '{model}' has no backend capacity right now (HTTP 503).",
            "model": model,
            "hint": "This is a model-side shortage, not an account failure. Use another model.",
            "attempts": attempts,
        }

    return {
        "status": "EXHAUSTED",
        "error": (f"No account produced an answer after {tried} attempt(s). "
                  f"Check {_t('status')} for cooldowns."),
        "model": model,
        "attempts": attempts,
    }


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _ok(payload: Dict[str, Any]) -> str:
    # Compact separators: every byte here is context the calling model pays for.
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _err(message: str, **extra: Any) -> str:
    return json.dumps({"status": "ERROR", "error": message, **extra},
                      ensure_ascii=False, separators=(",", ":"))


def _repair_json(text: str) -> Optional[Any]:
    """Last resort before declaring a model answer unparsable."""
    cleaned = re.sub(r",\s*([}\]])", r"\1", text)          # trailing commas
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", cleaned)
    try:
        return json.loads(cleaned)
    except Exception:
        return None


def _safe_json(text: str) -> Optional[Any]:
    """Parse JSON that may be wrapped in a markdown fence or surrounded by prose."""
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
        pass

    start = min((i for i in (t.find("{"), t.find("[")) if i >= 0), default=-1)
    if start < 0:
        return None
    opening = t[start]
    closing = "}" if opening == "{" else "]"
    depth = 0
    in_str = False
    escaped = False
    for i in range(start, len(t)):
        ch = t[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == opening:
            depth += 1
        elif ch == closing:
            depth -= 1
            if depth == 0:
                candidate = t[start:i + 1]
                try:
                    return json.loads(candidate)
                except Exception:
                    # Trailing commas and stray control characters are the two
                    # failures worth repairing; anything else is a real miss.
                    return _repair_json(candidate)
    return None

def _salvage_changes(text: str) -> Dict[str, Any]:
    """Recover the complete edits from a reply that was cut short.

    A truncated answer is expensive: the work order was paid for and part of the
    edit set is already written. Instead of discarding it, keep every change
    object that is complete and say so in the result.
    """
    salvaged: Dict[str, Any] = {"changes": [], "verification": [], "risks": [],
                                "done": False}
    if "{" not in text:
        return salvaged
    match = re.search(r'"analysis"\s*:\s*("(?:[^"\\]|\\.)*")', text)
    if match:
        try:
            salvaged["analysis"] = json.loads(match.group(1))
        except Exception:
            pass
    key = text.find('"changes"')
    if key < 0:
        return salvaged
    opening = text.find("[", key)
    if opening < 0:
        return salvaged
    depth = 0
    in_str = False
    escaped = False
    obj_start = -1
    for pos in range(opening, len(text)):
        ch = text[pos]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            if depth == 0:
                obj_start = pos
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
            if depth == 0 and obj_start >= 0:
                blob = text[obj_start:pos + 1]
                try:
                    item = json.loads(blob)
                except Exception:
                    item = _repair_json(blob)
                if isinstance(item, dict) and isinstance(item.get("path"), str):
                    salvaged["changes"].append(item)
                obj_start = -1
        elif ch == "]" and depth == 0:
            break
    return salvaged


# --------------------------------------------------------------------------- #
# Long-input handling
# --------------------------------------------------------------------------- #


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4) if text else 0


def _chunk_text(text: str, chunk_chars: int) -> List[str]:
    """Split on paragraph, then line, then word boundaries; keep every char."""
    if not text:
        return []
    chunk_chars = max(1000, chunk_chars)
    if len(text) <= chunk_chars:
        return [text]
    chunks: List[str] = []
    pos, total = 0, len(text)
    while pos < total:
        if total - pos <= chunk_chars:
            chunks.append(text[pos:])
            break
        window = text[pos:pos + chunk_chars]
        cut = 0
        for sep in ("\n\n", "\n", " "):
            at = window.rfind(sep)
            if at > chunk_chars // 2:
                cut = at + len(sep)
                break
        if cut <= 0:
            cut = chunk_chars
        chunks.append(text[pos:pos + cut])
        pos += cut
    return [c for c in chunks if c]


async def _map_reduce(items: List[str], instruction: str, model: str,
                      reduce_instruction: str, max_parallel: int = 3,
                      chunk_chars: int = 60000) -> Dict[str, Any]:
    """One call per chunk, then one call to combine: the way to survive 1M-token input.

    Chunks of the same item run sequentially on purpose: one item is one account's
    problem, and spreading it across accounts in parallel is what makes the pool
    look like a bot.
    """
    sem = asyncio.Semaphore(max(1, min(int(max_parallel), 6)))
    started = time.time()
    budget = 480.0
    results: List[Optional[Dict[str, Any]]] = [None] * len(items)

    async def run_item(index: int, item: str) -> None:
        if time.time() - started >= budget:
            results[index] = {"status": "TIMEOUT", "text": ""}
            return
        async with sem:
            parts: List[str] = []
            for chunk in _chunk_text(item, chunk_chars):
                if time.time() - started >= budget:
                    results[index] = {"status": "TIMEOUT", "text": "\n\n".join(parts)}
                    return
                res = await _chat_once(f"{instruction}\n\n---\n\n{chunk}", model)
                if res.get("status") != "OK":
                    results[index] = {"status": "ERROR", "text": "\n\n".join(parts),
                                      "error": res.get("error", "")}
                    return
                parts.append(res.get("text", ""))
            results[index] = {"status": "OK", "text": "\n\n".join(parts)}

    await asyncio.gather(*(run_item(i, item) for i, item in enumerate(items)))

    meta, parts_text, failed = [], [], 0
    for index, res in enumerate(results):
        if res is None:
            res = {"status": "TIMEOUT", "text": ""}
        meta.append({"index": index, "status": res["status"],
                     "chars": len(res.get("text") or "")})
        if res["status"] == "OK":
            parts_text.append(res.get("text") or "")
        else:
            failed += 1

    if not parts_text:
        return {"status": "FAILED", "text": "", "parts": meta, "failed": failed}

    joined = "\n\n".join(parts_text)
    limit = 120_000
    if len(joined) > limit:
        per = max(200, limit // len(parts_text))
        trimmed = []
        for text in parts_text:
            if len(text) > per:
                half = max(0, (per - 20) // 2)
                trimmed.append(f"{text[:half]} [... trimmed ...] {text[-half:]}")
            else:
                trimmed.append(text)
        joined = "\n\n".join(trimmed)

    reduced = await _chat_once(f"{reduce_instruction}\n\n{joined}", model)
    if reduced.get("status") != "OK":
        # A model-side 503 ends the pool's rotation on purpose. One pause and a
        # second attempt is cheaper than handing back unreduced chunks silently.
        await asyncio.sleep(1.5)
        reduced = await _chat_once(f"{reduce_instruction}\n\n{joined}", model)
    ok = reduced.get("status") == "OK"
    out: Dict[str, Any] = {
        "status": "OK" if ok else "PARTIAL",
        "text": (reduced.get("text") if ok else "") or joined,
        "parts": meta,
        "failed": failed,
        "reduce_status": reduced.get("status"),
    }
    if not ok:
        # Never pretend joined chunks are a synthesis.
        out["reduce_error"] = (reduced.get("error") or "")[:300]
        out["hint"] = ("Every chunk answered, but the combine call failed; the "
                       "text is the joined partials, not a synthesis.")
    return out


def _read_files_by_path(paths: List[str], max_total_chars: int = 500_000,
                        max_file_chars: int = 200_000
                        ) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], str]:
    """Read local files so the client never ships contents through tool-call JSON.

    A 300 KB source file becomes ~400 KB of JSON escaping on the wire, and the
    client truncates the argument list long before the model's own context limit
    is anywhere near. The server is on the same machine; it can just read.

    One unusable entry must not sink the batch: binaries, oversized files and
    unknown suffixes are skipped and reported, never decoded into noise.
    """
    suffixes = {".py", ".pyw", ".js", ".jsx", ".ts", ".tsx", ".json", ".jsonc",
                ".md", ".txt", ".toml", ".yaml", ".yml", ".ini", ".cfg", ".sql",
                ".html", ".css", ".scss", ".sh", ".ps1", ".bat", ".cmd", ".c",
                ".h", ".cpp", ".hpp", ".cs", ".java", ".kt", ".go", ".rs", ".rb",
                ".php", ".lua", ".vue", ".svelte"}
    files: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []
    seen = set()
    total = 0
    attempts = 0

    for raw in paths:
        if not isinstance(raw, str) or not raw.strip():
            continue
        attempts += 1
        candidate = os.path.expanduser(os.path.expandvars(raw.strip().strip("'\"")))
        if not candidate:
            skipped.append({"path": raw, "reason": "empty path"})
            continue
        try:
            key = os.path.normcase(os.path.abspath(candidate))
        except Exception:
            key = candidate
        if key in seen:
            continue
        seen.add(key)

        path = Path(candidate)
        try:
            if not path.exists():
                skipped.append({"path": raw, "reason": "not found"})
                continue
            if path.is_dir():
                skipped.append({"path": raw, "reason": "is a directory"})
                continue
            if not path.is_file():
                skipped.append({"path": raw, "reason": "not a regular file"})
                continue
            if path.suffix and path.suffix.lower() not in suffixes:
                skipped.append({"path": raw, "reason": "unsupported suffix"})
                continue
            size = path.stat().st_size
            if size > max_file_chars:
                skipped.append({"path": raw, "reason": f"too large ({size} bytes)"})
                continue
            blob = path.read_bytes()[:max_file_chars]
            if b"\x00" in blob[:4096]:
                skipped.append({"path": raw, "reason": "binary"})
                continue
            text = blob.decode("utf-8", errors="replace")
            if text.startswith("\ufeff"):
                text = text[1:]
            if total + len(text) > max_total_chars:
                skipped.append({"path": raw, "reason": "total limit reached"})
                continue
            files.append({"path": raw, "content": text})
            total += len(text)
        except Exception as exc:
            skipped.append({"path": raw,
                            "reason": f"{type(exc).__name__}: {exc}"[:120]})

    error = ""
    if not attempts:
        error = "no usable paths were given"
    elif not files:
        reasons = "; ".join(sorted({s["reason"] for s in skipped})[:3])
        error = f"nothing could be read ({reasons})" if reasons else "nothing could be read"
    return files, skipped, error


MAX_CONTINUATIONS = 3


async def _chat_continue(prompt_or_contents: Any, model: str, system: str = "",
                         temperature: float = 0.7, max_tokens: int = 8192,
                         json_mode: bool = False,
                         thinking_budget: Optional[int] = None,
                         tools: Optional[List[Dict[str, Any]]] = None,
                         max_continuations: int = MAX_CONTINUATIONS,
                         total_budget_seconds: int = 900) -> Dict[str, Any]:
    """Keep writing when the model stops at its output cap.

    A reasoning model that hits max_tokens still returns a partial answer; asking
    it to continue is one call, while raising max_tokens blindly can cost the
    whole request. Stops on an empty reply, an error, or the wall-clock budget.
    """
    started = time.time()
    try:
        last = await _chat_once(prompt_or_contents, model, system,
                                temperature=temperature, max_tokens=max_tokens,
                                json_mode=json_mode,
                                thinking_budget=thinking_budget, tools=tools)
        if not isinstance(last, dict) or last.get("status") != "OK":
            return last
        if isinstance(prompt_or_contents, str):
            base = [{"role": "user", "parts": [{"text": prompt_or_contents}]}]
        else:
            base = list(prompt_or_contents)
        text = last.get("text") or ""
        rounds = 0
        log: List[str] = []
        while (rounds < max_continuations and last.get("truncated")
               and (last.get("text") or "")
               # Bound the total spend: four 8k rounds is 32k output tokens the
               # caller never asked for.
               and _estimate_tokens(text) < max_tokens * 3):
            if time.time() - started >= total_budget_seconds:
                log.append("stop: budget")
                break
            follow = base + [
                {"role": "model", "parts": [{"text": text}]},
                {"role": "user", "parts": [{"text": (
                    "Continue exactly where you stopped. Do not repeat text "
                    "already written. Output only the continuation.")}]},
            ]
            try:
                nxt = await _chat_once(follow, model, system,
                                       temperature=temperature,
                                       max_tokens=max_tokens,
                                       json_mode=json_mode,
                                       thinking_budget=thinking_budget, tools=tools)
            except Exception:
                # Keep what was already written: a broken follow-up must not
                # throw away a usable partial answer.
                log.append("stop: error")
                break
            rounds += 1
            if not isinstance(nxt, dict) or nxt.get("status") != "OK":
                log.append("stop: error")
                break
            part = nxt.get("text") or ""
            if not part.strip():
                log.append("stop: empty")
                break
            text += part
            last = nxt
            log.append("continued")
        out = dict(last)
        out["text"] = text
        out["continuations"] = rounds
        out["truncated"] = bool(last.get("truncated"))
        if rounds:
            out["continuation_log"] = log
        return out
    except Exception as exc:
        return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}


# --------------------------------------------------------------------------- #
# MCP server
# --------------------------------------------------------------------------- #

app = MCPServer(
    "agy-pool",
    title="Gemini account pool",
    version=SERVER_VERSION,
    instructions=(
        "Direct Gemini access through a pool of Google Antigravity OAuth accounts. "
        "Each call is stateless; no Antigravity conversation is created.\n\n"
        "You are the orchestrator. For real work, send a work order with "
        f"{_t('code_task')}, apply the returned file edits yourself, verify by "
        "reading the file back or running the tests, then send the next order "
        "describing what failed. Verify anything the pool returns before acting on "
        "it.\n\n"
        "Model aliases: pro, flash, fast, deep, high and gemini are the strongest "
        "tier; lite and cheap the fastest; exact model ids also work."
    ),
)


# Probe results are cached because every health check costs real model requests.
_HEALTH_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _declared_models(accounts: List[Account]) -> Dict[str, Dict[str, Any]]:
    """Model catalogue declared by the accounts' quota metadata."""
    seen: Dict[str, Dict[str, Any]] = {}
    for acc in accounts:
        try:
            raw = json.loads(acc.path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for item in (raw.get("quota") or {}).get("models") or []:
            name = item.get("name")
            if name and name not in seen:
                seen[name] = {"display": item.get("display_name"),
                              "max_output_tokens": item.get("max_output_tokens")}
    return seen


def _quota_buckets(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Remaining quota fractions keyed by bucket id (gemini-5h, 3p-5h, ...)."""
    quota = raw.get("quota") or {}
    buckets: Dict[str, Any] = {}
    for group in quota.get("quota_groups") or []:
        for bucket in group.get("buckets") or []:
            buckets[bucket.get("bucket_id")] = bucket.get("remaining_fraction")
    return buckets


@app.tool(name=_t("health"))
async def health(
    models: Annotated[List[str], Field(
        description=('Probe these aliases or ids. Empty means the usual tiers; '
                     '["all"] means every model the accounts declare (slow: one '
                     "real request each)."))] = [],
    refresh: Annotated[bool, Field(
        description="Ignore cached probe results and probe again.")] = False,
) -> str:
    """Check which models actually answer, not merely which ones are listed.

    A listed model can answer HTTP 200 with a "no longer available" sentence, or
    HTTP 503 with no capacity. This sends one tiny request per model and reports
    what really replied, so a dead model id is diagnosed as a dead model instead
    of looking like a broken pool. Results are cached briefly; probes cost quota.
    """
    accounts, err = _load_accounts()
    if err:
        return _err(err)
    if not accounts:
        return _err("No usable accounts in the pool.")

    declared = sorted(_declared_models(accounts))
    wanted = [str(entry).strip() for entry in models if str(entry).strip()]
    targets: List[str] = []
    if any(entry.lower() == "all" for entry in wanted):
        targets = sorted(set(declared) | set(MODEL_ALIASES.values()) | KNOWN_DEAD)
        wanted = [entry for entry in wanted if entry.lower() != "all"]
    for raw_name in wanted:
        resolved = _resolve_model(raw_name)
        if resolved not in targets:
            targets.append(resolved)
    if not targets:
        # Canonical scope: the aliased tiers people actually use. One probe each
        # is enough to tell whether the pool is healthy.
        targets = sorted(set(MODEL_ALIASES.values()))

    now = time.time()
    results: List[Dict[str, Any]] = []
    pending: List[str] = []
    for name in targets:
        hit = _HEALTH_CACHE.get(name)
        if hit and not refresh and now - hit[0] < HEALTH_TTL_SECONDS:
            results.append({**hit[1], "cached": True})
        else:
            pending.append(name)

    deadline = time.time() + HEALTH_DEADLINE_SECONDS
    semaphore = asyncio.Semaphore(HEALTH_CONCURRENCY)

    async def probe(index: int, model_name: str) -> Dict[str, Any]:
        async with semaphore:
            if time.time() >= deadline:
                return {"model": model_name, "state": "SKIPPED",
                        "detail": "health deadline reached before probing"}
            acc = accounts[index % len(accounts)]
            token = await _ensure_token(acc)
            if not token:
                return {"model": model_name, "state": "NO_TOKEN",
                        "account": acc.email}
            payload = _build_payload(
                acc, model_name,
                [{"role": "user", "parts": [{"text": "Reply with exactly: PING"}]}],
                "", 0.0, 64, False,
                # A liveness probe with the reasoning budget unset can burn all
                # 64 tokens thinking and return an empty candidate, which used to
                # be reported as a dead model. Disable thinking for the probe.
                thinking_budget=0)
            started = time.time()
            async with POOL.gate(acc):
                body, code, detail, retry_after = await asyncio.to_thread(
                    _post_ex, token, payload,
                    int(max(15, min(90, deadline - time.time()))))
            elapsed = int((time.time() - started) * 1000)
            if body is None or code != 200:
                state = "COOLING" if code == 429 else (
                    "AUTH" if code in (401, 403) else "DEAD")
                return {"model": model_name, "state": state, "http": code,
                        "retry_after_s": retry_after,
                        "latency_ms": elapsed, "detail": (detail or "")[:120]}
            text, meta = _extract_text(body)
            if _looks_retired(text):
                return {"model": model_name, "state": "DEPRECATED",
                        "latency_ms": elapsed, "says": text[:120]}
            if not text:
                finish = str(meta.get("finish_reason") or "").upper()
                if finish == "MAX_TOKENS":
                    return {"model": model_name, "state": "ALIVE",
                            "latency_ms": elapsed, "note": "reply truncated"}
                return {"model": model_name, "state": "EMPTY",
                        "latency_ms": elapsed, "finish": finish}
            return {"model": model_name, "state": "ALIVE", "latency_ms": elapsed,
                    "tokens": meta.get("tokens")}

    if pending:
        fresh = await asyncio.gather(*(probe(i, m) for i, m in enumerate(pending)))
        for check in fresh:
            if check["state"] != "SKIPPED":
                _HEALTH_CACHE[check["model"]] = (time.time(), check)
        results.extend(fresh)

    alive = sorted(c["model"] for c in results if c["state"] == "ALIVE")
    degraded = [c for c in results
                if c["state"] in ("DEAD", "DEPRECATED", "EMPTY", "AUTH", "COOLING",
                                  "NO_TOKEN", "SKIPPED")]
    return _ok({
        "status": "OK" if alive else "FAILED",
        "probed": len(pending),
        "cached_count": len(results) - len(pending),
        "alive": alive,
        "degraded": degraded,
        "aliases": MODEL_ALIASES,
        "checks": sorted(results, key=lambda c: (c["state"] != "ALIVE", c["model"])),
        "note": ("COOLING/AUTH describe the account, not the model. A dead model id "
                 "answers the same way on every account."),
    })


@app.tool(name=_t("status"))
def status() -> str:
    """Report pool health: accounts, token expiry, cooldowns, quota and counters.

    Never includes token material. An expired token is not an outage when the
    account can refresh it on demand, so `ready` means "can serve a request now".
    """
    accounts, err = _load_accounts()
    if err:
        return _err(err)

    now = time.time()
    rows = []
    for acc in accounts:
        raw: Dict[str, Any] = {}
        try:
            raw = json.loads(acc.path.read_text(encoding="utf-8"))
        except Exception:
            pass
        buckets = _quota_buckets(raw)
        quota = raw.get("quota") or {}
        # Account-wide cooldowns only. Per-model cooldowns are reported
        # separately so one sick model never looks like a sick account.
        account_cd = max(0, int(POOL._cooldown.get(POOL._key(acc, ""), 0) - now))
        model_cd = {k.split(":", 1)[1]: max(0, int(v - now))
                    for k, v in POOL._cooldown.items()
                    if k.startswith(f"{acc.id}:") and k != f"{acc.id}:" and v > now}
        rows.append({
            "email": acc.email,
            "tier": acc.tier,
            "token_valid": acc.expiry > now,
            "expires_in_minutes": max(0, int((acc.expiry - now) / 60)),
            "can_refresh": bool(acc.refresh),
            "last_error": POOL._last_errors.get(acc.id) or acc.last_error or None,
            "cooling_down_seconds": account_cd,
            "model_cooldowns": model_cd,
            "ready": (acc.expiry > now or bool(acc.refresh)) and account_cd == 0,
            "gemini_5h_remaining": buckets.get("gemini-5h"),
            "weekly_remaining": buckets.get("gemini-weekly"),
            "third_party_5h_remaining": buckets.get("3p-5h"),
            "forbidden": quota.get("is_forbidden"),
        })

    known = [r["gemini_5h_remaining"] for r in rows
             if isinstance(r["gemini_5h_remaining"], (int, float))]
    return _ok({
        "status": "OK",
        "accounts_total": len(accounts),
        "accounts_ready": sum(1 for r in rows if r["ready"]),
        "average_gemini_5h": (sum(known) / len(known)) if known else None,
        "default_model": DEFAULT_MODEL,
        "stats": POOL._stats,
        "oauth_refresh_configured": bool(CLIENT_ID and CLIENT_SECRET),
        "state_dir": str(STATE_DIR),
        "note": ("ready counts accounts whose token can be refreshed on demand; an "
                 "expired token is not an outage. Quota fractions come from cached "
                 "account metadata."),
        "accounts": rows,
    })


@app.tool(name=_t("models"))
def models() -> str:
    """List the exact model ids the accounts declare, with aliases and output limits."""
    accounts, err = _load_accounts()
    if err:
        return _err(err)
    catalog = _declared_models(accounts)
    return _ok({
        "status": "OK",
        "default": DEFAULT_MODEL,
        "aliases": MODEL_ALIASES,
        "models": dict(sorted(catalog.items())),
        "note": "A listed model can still be retired or out of capacity; health checks that.",
    })


@app.tool(name=_t("chat"))
async def chat(
    prompt: Annotated[str, Field(description="The question or instruction.")],
    model: Annotated[str, Field(
        description="Alias or model id.")] = "pro",
    system: Annotated[str, Field(
        description="Optional system instruction.")] = "",
    temperature: Annotated[float, Field(
        ge=0, le=2, description="Sampling temperature (0-2). Lower is more literal.")] = 0.7,
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Output token budget.")] = 8192,
    images: Annotated[List[str], Field(
        description="Images: local paths, URLs or data URIs.")] = [],
    web: Annotated[bool, Field(
        description="Ground with Google Search; the reply then cites its sources.")] = False,
    thinking_budget: Annotated[Optional[int], Field(
        ge=0, le=65536,
        description=("Hidden reasoning cap. web=true defaults it to 512, because a "
                     "grounded call can otherwise burn the whole budget thinking."))] = None,
) -> str:
    """Ask one question and get plain text back. Stateless, no conversation created.

    Use web=true for anything the model cannot know from training: current
    events, prices, releases, version numbers.
    """
    if not prompt or not prompt.strip():
        return _err("prompt must not be empty.")

    contents: Any = prompt
    if images:
        parts: List[Dict[str, Any]] = [{"text": prompt}]
        try:
            parts.extend(await asyncio.to_thread(_image_parts, images))
        except ValueError as exc:
            return _err(f"image input rejected: {exc}")
        contents = [{"role": "user", "parts": parts}]

    if web and thinking_budget is None:
        thinking_budget = 512
    result = await _chat_continue(
        contents, model, system, temperature, max_tokens,
        thinking_budget=thinking_budget,
        tools=[{"google_search": {}}] if web else None,
    )
    return _ok(result)


@app.tool(name=_t("url"))
async def url(
    urls: Annotated[str, Field(
        description="One or more http(s) URLs, separated by commas or newlines (max 5).")],
    question: Annotated[str, Field(
        description="What to extract from those pages. Defaults to a short summary.")] = "",
    model: Annotated[str, Field(
        description="Alias or model id.")] = "flash",
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Output token budget.")] = 4096,
    thinking_budget: Annotated[int, Field(
        ge=0, le=65536, description="Hidden reasoning cap. Keep it small.")] = 512,
) -> str:
    """Read web pages locally and answer a question about them, citing the URLs.

    The pages are downloaded here and pasted as text, so this works even though
    the upstream endpoint has no URL-reading tool. Only the readable text is
    sent, and the total is bounded so a long page cannot flood the model.
    """
    cleaned = [u.strip() for u in urls.replace("\n", ",").split(",") if u.strip()]
    cleaned = [u for u in cleaned if u.startswith(("http://", "https://"))][:5]
    if not cleaned:
        return _err("no http(s) URL found in `urls`.")

    per_url = max(4000, MAX_FETCH_CHARS // len(cleaned))
    fetched: List[Dict[str, Any]] = []
    blocks: List[str] = []

    async def fetch(url: str) -> Optional[str]:
        try:
            text, total = await asyncio.to_thread(_fetch_readable, url, per_url)
        except Exception as exc:
            fetched.append({"url": url, "chars": 0,
                            "error": f"{type(exc).__name__}: {exc}"})
            return None
        fetched.append({"url": url, "chars": total})
        return "=== SOURCE %s ===\n%s" % (url, text)

    pages = await asyncio.gather(*(fetch(u) for u in cleaned))
    blocks = [page for page in pages if page]
    if not blocks:
        return _err("every URL failed to fetch.", fetched=fetched)

    prompt = (question.strip() or "Summarise these pages, keeping concrete facts.")
    prompt += "\n\n" + "\n\n".join(blocks)
    prompt += "\n\nAnswer using only the sources above and cite the URL you used."
    result = await _chat_continue(
        prompt, model, "", 0.3, max_tokens,
        thinking_budget=thinking_budget,
    )
    result["fetched"] = fetched
    return _ok(result)


@app.tool(name=_t("agent"))
async def agent(
    task: Annotated[str, Field(
        description="What to accomplish, with the expected answer format.")],
    model: Annotated[str, Field(
        description="Alias or model id.")] = "pro",
    system: Annotated[str, Field(
        description="Optional system instruction.")] = "",
    max_steps: Annotated[int, Field(
        ge=1, le=20,
        description="Tool-calling rounds before the agent must answer.")] = 8,
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Output budget per round.")] = 8192,
    allow_write: Annotated[bool, Field(
        description="Also expose write_file.")] = False,
    allow_shell: Annotated[bool, Field(
        description="Also expose run_shell (powerful and risky).")] = False,
) -> str:
    """Let the model run its own tool loop (web search, page fetch, file reads), then answer.

    The model decides which tools to call and when it has enough; you get one
    answer plus a trace. Costs one model request per step, so use chat for a
    single-shot question and this for research that needs several lookups.
    """
    if not task or not task.strip():
        return _err("task must not be empty.")
    result = await _agent_run(task, model, system, max_steps, max_tokens,
                              allow_write, allow_shell)
    return _ok(result)


# --------------------------------------------------------------------------- #
# Sessions: keep a conversation alive so files are not resent every turn
# --------------------------------------------------------------------------- #

_SESSIONS: Dict[str, Dict[str, Any]] = {}
_SESSION_LOCKS: Dict[str, asyncio.Lock] = {}
MAX_SESSION_CHARS = 400_000
SESSION_TTL_SECONDS = 6 * 3600
SESSIONS_FILE = DATA_DIR / "sessions.json"
SESSIONS_FILE_MAX_BYTES = 20 * 1024 * 1024
_SESSIONS_SAVED_AT = 0.0


def _session_lock(session_id: str) -> asyncio.Lock:
    lock = _SESSION_LOCKS.get(session_id)
    if lock is None:
        lock = asyncio.Lock()
        _SESSION_LOCKS[session_id] = lock
    return lock


def _sessions_save(force: bool = False) -> None:
    """Persist live sessions so an MCP restart does not lose the context.

    Throttled by default: a session can approach 20 MB and a long conversation
    would otherwise rewrite the whole file after every single turn. Best effort:
    a failed write must never break a chat turn.
    """
    global _SESSIONS_SAVED_AT
    now = time.time()
    if not force and now - _SESSIONS_SAVED_AT < 5:
        return
    try:
        SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": 1, "saved_at": now, "sessions": _SESSIONS}
        blob = json.dumps(payload, ensure_ascii=False)
        if len(blob.encode("utf-8")) > SESSIONS_FILE_MAX_BYTES:
            # Keep the most recently used sessions only.
            ordered = sorted(_SESSIONS.items(),
                             key=lambda kv: kv[1].get("last_used", 0),
                             reverse=True)
            kept: Dict[str, Any] = {}
            size = 0
            for sid, sess in ordered:
                chunk = len(json.dumps({sid: sess}, ensure_ascii=False).encode("utf-8"))
                if size + chunk > SESSIONS_FILE_MAX_BYTES // 2:
                    break
                kept[sid] = sess
                size += chunk
            _SESSIONS.clear()
            _SESSIONS.update(kept)
            blob = json.dumps({"version": 1, "saved_at": now,
                               "sessions": _SESSIONS}, ensure_ascii=False)
        tmp = SESSIONS_FILE.with_suffix(".json.tmp")
        tmp.write_text(blob, encoding="utf-8")
        os.replace(tmp, SESSIONS_FILE)
        _SESSIONS_SAVED_AT = now
    except Exception:
        pass


def _sessions_load() -> None:
    """Restore sessions written by a previous process."""
    try:
        if not SESSIONS_FILE.is_file():
            return
        data = json.loads(SESSIONS_FILE.read_text(encoding="utf-8"))
        raw = data.get("sessions") if isinstance(data, dict) else None
        if not isinstance(raw, dict):
            return
        now = time.time()
        for sid, sess in raw.items():
            if not isinstance(sess, dict) or not isinstance(sess.get("contents"), list):
                continue
            if now - float(sess.get("last_used") or 0) > SESSION_TTL_SECONDS:
                continue
            sess.setdefault("system", "")
            sess.setdefault("model", DEFAULT_MODEL)
            sess.setdefault("turns", 0)
            sess.setdefault("chars_dropped", 0)
            sess.setdefault("created", now)
            _SESSIONS[sid] = sess
    except Exception:
        pass


_sessions_load()


def _session_text_chars(contents: List[Dict[str, Any]]) -> int:
    return sum(len(p.get("text", "")) for c in contents
               for p in (c.get("parts") or []) if isinstance(p, dict))


def _session_trim(sess: Dict[str, Any]) -> None:
    """Drop the oldest exchanges until the context fits the budget.

    Gemini rejects contents that do not start with a user turn, so the opening
    user task stays as the anchor and pairs are dropped from just after it. A
    bare pop(0) could leave a model turn first and break every later request.
    """
    contents = sess["contents"]
    while len(contents) > 2 and _session_text_chars(contents) > MAX_SESSION_CHARS:
        before = _session_text_chars(contents)
        if len(contents) >= 4:
            del contents[1:3]
        else:
            contents.pop(1)
        sess["chars_dropped"] = (int(sess.get("chars_dropped") or 0)
                                 + before - _session_text_chars(contents))


def _session_get(session_id: str) -> Dict[str, Any]:
    sess = _SESSIONS.get(session_id)
    if not sess:
        raise ValueError(f"unknown session: {session_id}")
    if time.time() - sess["last_used"] > SESSION_TTL_SECONDS:
        _SESSION_LOCKS.pop(session_id, None)
        _SESSIONS.pop(session_id, None)
        raise ValueError(f"session expired after {SESSION_TTL_SECONDS // 3600}h: {session_id}")
    return sess


@app.tool(name=_t("session_new"))
async def session_new(
    system: Annotated[str, Field(
        description="System instruction kept for the whole session.")] = "",
    model: Annotated[str, Field(
        description="Alias or model id; fixed for the session.")] = "pro",
    first_message: Annotated[str, Field(
        description="Optional opening message, sent immediately.")] = "",
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Output budget for the first message.")] = 8192,
) -> str:
    """Start a stateful conversation and return its session id.

    Use it when a task spans several turns over the same material: send the files
    once, then keep asking follow-ups by id instead of resending everything.
    Sessions live for six hours and survive a server restart.
    """
    session_id = uuid.uuid4().hex[:12]
    session = {
        "contents": [], "system": system, "model": _resolve_model(model),
        "created": time.time(), "last_used": time.time(), "turns": 0,
        "chars_dropped": 0,
    }
    _SESSIONS[session_id] = session
    out: Dict[str, Any] = {"status": "OK", "session_id": session_id,
                           "model": session["model"]}
    if first_message.strip():
        session["contents"].append({"role": "user",
                                    "parts": [{"text": first_message}]})
        result = await _chat_once(list(session["contents"]), session["model"],
                                  system, temperature=0.4, max_tokens=max_tokens)
        if result.get("status") != "OK":
            # Do not leave a half-open session holding a prompt that never got
            # an answer: the next send would silently resend it as history.
            _SESSIONS.pop(session_id, None)
            _SESSION_LOCKS.pop(session_id, None)
            await asyncio.to_thread(_sessions_save, True)
            return _ok({"status": result.get("status"),
                        "error": result.get("error"), "session_id": session_id})
        session["contents"].append({"role": "model",
                                    "parts": [{"text": result["text"]}]})
        session["turns"] += 1
        session["last_used"] = time.time()
        _session_trim(session)
        out.update({"text": result["text"], "account": result.get("account"),
                    "latency_ms": result.get("latency_ms"),
                    "turns": session["turns"]})
    await asyncio.to_thread(_sessions_save, True)
    return _ok(out)


@app.tool(name=_t("session_send"))
async def session_send(
    session_id: Annotated[str, Field(
        description="Session id returned by session_new.")],
    message: Annotated[str, Field(description="The next message.")],
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Output token budget.")] = 8192,
    temperature: Annotated[float, Field(
        ge=0, le=2, description="Sampling temperature.")] = 0.4,
) -> str:
    """Continue a session; earlier turns stay in context.

    Concurrent follow-ups on the same session are serialized, so their history
    cannot interleave.
    """
    if not message.strip():
        return _err("message must not be empty.")
    try:
        _session_get(session_id)
    except ValueError as exc:
        return _err(str(exc))

    async with _session_lock(session_id):
        try:
            sess = _session_get(session_id)
        except ValueError as exc:
            return _err(str(exc))
        if not sess["contents"]:
            return _err("session has no messages yet; start it with a first_message.")

        sess["contents"].append({"role": "user", "parts": [{"text": message}]})
        result = await _chat_once(list(sess["contents"]), sess["model"],
                                  sess["system"], temperature=temperature,
                                  max_tokens=max_tokens)
        if result.get("status") != "OK":
            sess["contents"].pop()
            return _ok({"status": result.get("status"),
                        "error": result.get("error"), "session_id": session_id})

        sess["contents"].append({"role": "model",
                                 "parts": [{"text": result["text"]}]})
        sess["turns"] += 1
        sess["last_used"] = time.time()
        _session_trim(sess)
        await asyncio.to_thread(_sessions_save)
        return _ok({"status": "OK", "session_id": session_id,
                    "text": result["text"], "model": result.get("model"),
                    "account": result.get("account"),
                    "latency_ms": result.get("latency_ms"),
                    "turns": sess["turns"],
                    "context_chars": _session_text_chars(sess["contents"])})


@app.tool(name=_t("session_list"))
async def session_list() -> str:
    """List live sessions with model, turn count and context size."""
    now = time.time()
    rows = []
    for session_id, sess in list(_SESSIONS.items()):
        if now - sess["last_used"] > SESSION_TTL_SECONDS:
            _SESSIONS.pop(session_id, None)
            _SESSION_LOCKS.pop(session_id, None)
            continue
        rows.append({"session_id": session_id, "model": sess["model"],
                     "turns": sess["turns"],
                     "context_chars": _session_text_chars(sess["contents"]),
                     "chars_dropped": int(sess.get("chars_dropped") or 0),
                     "idle_seconds": int(now - sess["last_used"])})
    return _ok({"status": "OK", "sessions": rows})


@app.tool(name=_t("session_drop"))
async def session_drop(
    session_id: Annotated[str, Field(description="Session id to forget.")],
) -> str:
    """Forget one session and free its context."""
    existed = _SESSIONS.pop(session_id, None) is not None
    _SESSION_LOCKS.pop(session_id, None)
    if existed:
        await asyncio.to_thread(_sessions_save, True)
    return _ok({"status": "OK", "dropped": existed, "session_id": session_id})


def _validate_files(files: Any) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """Normalize the caller's file list, or explain exactly what is wrong."""
    if files in (None, []):
        return [], None
    if not isinstance(files, list):
        return [], "files must be a list of {path, content} objects"
    cleaned: List[Dict[str, str]] = []
    for index, item in enumerate(files):
        if not isinstance(item, dict):
            return [], f"files[{index}] must be an object with path and content"
        path = item.get("path")
        content = item.get("content")
        if not isinstance(path, str) or not path.strip():
            return [], f"files[{index}].path must be a non-empty string"
        if not isinstance(content, str):
            return [], f"files[{index}].content must be a string"
        cleaned.append({"path": path.strip(), "content": content})
    return cleaned, None


def _files_block(files: List[Dict[str, str]]) -> str:
    return "\n\n".join(f"--- FILE: {f.get('path', '?')} ---\n{f.get('content', '')}"
                       for f in files)


def _cap_results(results: List[Dict[str, Any]],
                 per: int = MAX_PER_RESULT_CHARS,
                 total: int = MAX_TOTAL_RESULT_CHARS) -> None:
    """Bound parallel answers in place: N model answers is N contexts."""
    used = 0
    for item in results:
        text = item.get("text")
        if not isinstance(text, str) or not text:
            continue
        if used >= total:
            item["text"] = f"[omitted: {len(text)} chars; result budget reached]"
            item["truncated"] = True
            continue
        allowance = min(per, total - used)
        if len(text) > allowance:
            item["text"] = text[:allowance] + f"\n[... {len(text)} chars total, truncated]"
            item["truncated"] = True
        used += len(item["text"])


@app.tool(name=_t("code_task"))
async def code_task(
    goal: Annotated[str, Field(
        description="What must be achieved, with acceptance criteria.")],
    files: Annotated[List[Dict[str, str]], Field(
        description='Files to work on, as [{"path": "...", "content": "..."}]. '
                    'For files already on disk prefer `paths` instead.')] = [],
    paths: Annotated[List[str], Field(
        description="Local file paths the server reads itself. Cheaper than "
                    "sending content through the tool-call JSON.")] = [],
    context: Annotated[str, Field(
        description="Prior context: conventions, or what an earlier attempt missed.")] = "",
    constraints: Annotated[str, Field(
        description='Hard rules, e.g. "do not change public APIs".')] = "",
    model: Annotated[str, Field(
        description="Alias or model id.")] = "pro",
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Output budget for the whole edit set; the reply "
                   "continues automatically when it runs out.")] = 32768,
    images: Annotated[List[str], Field(
        description="Screenshots or mockups to look at while editing.")] = [],
) -> str:
    """Turn a work order into structured file edits for you to apply and verify.

    The pool never touches the filesystem. It replies with JSON containing
    changes[] (create/replace/patch), verification[] and risks[]. Apply the
    changes yourself, then verify for real: its `done` flag is an opinion.
    """
    if not goal or not goal.strip():
        return _err("goal must not be empty.")
    cleaned_files, files_err = _validate_files(files)
    if files_err:
        return _err(files_err)
    skipped_note = ""
    if paths:
        read_files, skipped, read_err = await asyncio.to_thread(
            _read_files_by_path, paths)
        if read_err:
            return _err(read_err)
        known = {(item.get("path") or "") for item in cleaned_files}
        for item in read_files:
            key = item.get("path") or ""
            if key not in known:
                cleaned_files.append(item)
                known.add(key)
        if skipped:
            skipped_note = "; ".join(f"{s['path']} ({s['reason']})"
                                     for s in skipped[:5])

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
        "- Do not invent file paths that were not given unless clearly required.\n"
        "- Keep the reply compact: one sentence of analysis, no repetition of the "
        "input files, no markdown outside the JSON.\n"
        "- Prefer a 'patch' with a short exact 'search' over 'replace' for long "
        "files; rewriting a very long file is what gets cut off.\n"
        "- If the work does not fit the output budget, return the most important "
        "edit alone and list the rest under risks instead of a partial file."
    )

    parts: List[str] = [f"# Goal\n{goal}"]
    if constraints:
        parts.append(f"# Constraints\n{constraints}")
    if context:
        parts.append(f"# Context\n{context}")
    if cleaned_files:
        parts.append("# Files\n" + _files_block(cleaned_files))
    else:
        parts.append("# Files\n(none provided; propose new files if needed)")
    if skipped_note:
        parts.append(f"# Inputs not read\n{skipped_note}")

    prompt_text = "\n\n".join(parts)
    if len(prompt_text) > MAX_PROMPT_CHARS:
        return _err(f"prompt is {len(prompt_text)} chars, over the {MAX_PROMPT_CHARS} "
                    "limit. Split the work order, or send only the files this step needs.")

    prompt_parts: List[Dict[str, Any]] = [{"text": prompt_text}]
    if images:
        try:
            prompt_parts.extend(await asyncio.to_thread(_image_parts, images))
        except ValueError as exc:
            return _err(f"image input rejected: {exc}")

    payload_contents: Any = (prompt_text if not images
                             else [{"role": "user", "parts": prompt_parts}])

    result = await _chat_continue(payload_contents, model, system,
                                  temperature=0.2, max_tokens=max_tokens,
                                  json_mode=True, max_continuations=2)
    if result.get("status") != "OK":
        return _ok(result)

    finish_reason = result.get("finish_reason")
    reply = result.get("text") or ""
    parsed = _safe_json(reply)
    cut_short = (bool(result.get("truncated"))
                 or str(finish_reason or "").upper() == "MAX_TOKENS")
    if parsed is None and cut_short:
        # Continuing did not finish the JSON: keep every edit that is complete
        # instead of losing a paid-for work order.
        salvaged = _salvage_changes(reply)
        if salvaged["changes"]:
            salvaged["_salvage_note"] = (
                f"the reply ran out at max_tokens={max_tokens} and was continued "
                f"{result.get('continuations', 0)} time(s); only the complete edits "
                "are returned. Send the remaining work as a smaller order.")
            parsed = salvaged
    if parsed is None:
        raw_tail = reply[-2000:]
        if cut_short:
            return _ok({
                "status": "TRUNCATED",
                "error": (f"Output was cut at max_tokens={max_tokens} before any "
                          "complete JSON object was produced."),
                "hint": "Split the work order into smaller tasks or raise max_tokens.",
                "finish_reason": finish_reason,
                "account": result.get("account"),
                "raw_text": raw_tail,
            })
        return _ok({
            "status": "UNPARSABLE",
            "error": "Model did not return valid JSON.",
            "raw_text": raw_tail,
            "finish_reason": finish_reason,
            "account": result.get("account"),
        })

    if not isinstance(parsed, dict):
        return _ok({"status": "UNPARSABLE",
                    "error": "Model returned JSON that is not an object.",
                    "raw_text": result["text"][:4000]})

    raw = parsed.get("changes")
    if not isinstance(raw, list):
        parsed["changes"] = []
        parsed["invalid_changes"] = []
    else:
        valid = []
        invalid = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                invalid.append({"index": i, "reason": "change must be a dict"})
                continue
            path = item.get("path")
            if not isinstance(path, str) or not path:
                invalid.append({"index": i, "reason": "path must be a non-empty string"})
                continue
            op = item.get("operation")
            if op not in ("create", "replace", "patch"):
                invalid.append({"index": i,
                                "reason": "operation must be 'create', 'replace', or 'patch'"})
                continue
            if op in ("create", "replace"):
                if not isinstance(item.get("content"), str):
                    invalid.append({"index": i, "reason": "content must be a string"})
                    continue
            elif op == "patch":
                search_val = item.get("search")
                if not isinstance(search_val, str) or not search_val:
                    invalid.append({"index": i, "reason": "search must be a non-empty string"})
                    continue
                if not isinstance(item.get("replace"), str):
                    invalid.append({"index": i, "reason": "replace must be a string"})
                    continue
            valid.append(item)
        parsed["changes"] = valid
        parsed["invalid_changes"] = invalid

    salvage_note = parsed.pop("_salvage_note", None)
    parsed["status"] = "PARTIAL" if salvage_note else "OK"
    if salvage_note:
        parsed["note"] = salvage_note
        parsed["continued"] = result.get("continuations", 0)
    parsed["model"] = result.get("model")
    parsed["account"] = result.get("account")
    parsed["latency_ms"] = result.get("latency_ms")
    parsed["tokens"] = result.get("tokens")
    parsed["finish_reason"] = finish_reason
    parsed["raw_text"] = None
    return _ok(parsed)


@app.tool(name=_t("batch"))
async def batch(
    prompts: Annotated[List[str], Field(
        description="Independent prompts to run, one result each.")],
    model: Annotated[str, Field(
        description="Alias or model id.")] = "pro",
    system: Annotated[str, Field(
        description="System instruction shared by every prompt.")] = "",
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Output budget per prompt.")] = 8192,
    max_parallel: Annotated[int, Field(
        ge=1, le=8, description="How many run at once.")] = 4,
) -> str:
    """Run several independent prompts in parallel, spread across accounts.

    One failing prompt never cancels the others: every result carries its own
    status. Use it for genuinely independent work, not for steps that depend on
    each other's answers.
    """
    if not prompts:
        return _err("prompts must not be empty.")
    if len(prompts) > 32:
        return _err("Too many prompts (max 32).")
    parallel = max(1, min(int(max_parallel or 4), 8))
    semaphore = asyncio.Semaphore(parallel)
    deadline = time.time() + CALL_DEADLINE_SECONDS

    async def one(index: int, prompt: str) -> Dict[str, Any]:
        async with semaphore:
            remaining = deadline - time.time()
            if remaining <= 5:
                return {"index": index, "status": "TIMEOUT",
                        "error": "batch deadline reached before this prompt started"}
            try:
                result = await asyncio.wait_for(
                    _chat_once(prompt, model, system, temperature=0.7,
                               max_tokens=max_tokens),
                    timeout=remaining)
            except asyncio.TimeoutError:
                result = {"status": "TIMEOUT", "error": "batch deadline reached"}
            except Exception as exc:
                result = {"status": "ERROR",
                          "error": f"{type(exc).__name__}: {exc}"}
            result["index"] = index
            return result

    results = await asyncio.gather(*(one(i, p) for i, p in enumerate(prompts)))
    _cap_results(results)
    ok = sum(1 for r in results if r.get("status") == "OK")
    return _ok({
        "status": "OK" if ok else "FAILED",
        "succeeded": ok,
        "failed": len(results) - ok,
        "results": results,
    })


@app.tool(name=_t("fanout"))
async def fanout(
    plan: Annotated[List[Dict[str, Any]], Field(
        description=('Subtasks, one per agent: [{"task": "...", "context": "optional", '
                     '"model": "optional alias", "agent": false}]. Independent; each '
                     'agent sees only its own entry.'))],
    strategy: Annotated[Literal["all", "vote"], Field(
        description=("all = return every answer verbatim; vote = answers are short "
                     "factual replies, return the majority answer and the distribution."))] = "all",
    max_parallel: Annotated[int, Field(
        ge=1, le=8, description="Concurrency cap.")] = 6,
) -> str:
    """Fan a work order out to several agents in parallel, then collect every result.

    The agent-as-boss pattern: split a problem into independent subtasks, fire
    them at multiple accounts/models at once, and get every answer back in one
    call. Subtasks must be independent - they run concurrently and cannot see
    each other. Merging and verifying the outputs is your job.
    """
    if not plan:
        return _err("plan must not be empty.")
    if len(plan) > 8:
        return _err("plan too large (max 8 agents). Keep subtasks meaningful.")

    parallel = max(1, min(int(max_parallel or 6), 8))
    semaphore = asyncio.Semaphore(parallel)
    deadline = time.time() + CALL_DEADLINE_SECONDS

    async def one(index: int, item: Any) -> Dict[str, Any]:
        async with semaphore:
            if not isinstance(item, dict):
                return {"index": index, "status": "ERROR",
                        "error": "each plan entry must be an object"}
            task = str(item.get("task") or "").strip()
            if not task:
                return {"index": index, "status": "ERROR", "error": "empty task"}
            context = str(item.get("context") or "").strip()
            requested = str(item.get("model") or "default").strip()
            prompt = task if not context else f"{task}\n\nContext:\n{context}"
            remaining = deadline - time.time()
            if remaining <= 5:
                return {"index": index, "status": "TIMEOUT", "requested_model": requested,
                        "error": "fan-out deadline reached before this agent started"}
            try:
                if item.get("agent"):
                    coro = _agent_run(prompt, requested, "",
                                      int(item.get("max_steps") or 6),
                                      int(item.get("max_tokens") or 8192),
                                      bool(item.get("allow_write")),
                                      bool(item.get("allow_shell")))
                else:
                    coro = _chat_once(prompt, requested, temperature=0.2,
                                      max_tokens=int(item.get("max_tokens") or 32768))
                result = await asyncio.wait_for(coro, timeout=remaining)
            except asyncio.TimeoutError:
                result = {"status": "TIMEOUT", "error": "fan-out deadline reached"}
            except Exception as exc:
                result = {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}
            result["index"] = index
            result["requested_model"] = requested
            return result

    started = time.time()
    results = await asyncio.gather(*(one(i, item) for i, item in enumerate(plan)))
    wall_ms = int((time.time() - started) * 1000)
    _cap_results(results)

    oks = [r for r in results if r.get("status") == "OK"]
    payload: Dict[str, Any] = {
        "status": "OK" if oks else "FAILED",
        "agents": len(plan),
        "succeeded": len(oks),
        "failed": len(results) - len(oks),
        "wall_ms": wall_ms,
        "note": ("Subtasks ran in parallel on separate accounts. Merge and verify "
                 "the outputs yourself before acting on them."),
        "results": results,
    }

    if strategy == "vote":
        votes: Dict[str, List[str]] = {}
        for result in oks:
            text = (result.get("text") or "").strip()
            key = " ".join(text.split()).lower()
            votes.setdefault(key, []).append(text)
        if votes:
            winner_key = max(votes, key=lambda k: len(votes[k]))
            winner_votes = votes[winner_key]
            # Group by the whole normalized answer: two different answers can
            # share their first 120 characters and would otherwise merge.
            distribution = [{"answer": answers[0][:200], "count": len(answers)}
                            for answers in sorted(votes.values(),
                                                  key=lambda v: len(v), reverse=True)]
            payload["vote"] = {
                "winner": winner_votes[0][:2000],
                "count": len(winner_votes),
                "of": len(oks),
                "distribution": distribution,
                "unanimous": len(votes) == 1,
            }
        else:
            payload["vote"] = {"winner": None, "count": 0, "of": 0}

    return _ok(payload)


@app.tool(name=_t("review"))
async def review(
    goal: Annotated[str, Field(
        description="The original goal and its acceptance criteria.")],
    files: Annotated[List[Dict[str, str]], Field(
        description='Current state of the files to review, as [{"path", "content"}]. '
                    'Prefer `paths` for files already on disk.')],
    paths: Annotated[List[str], Field(
        description="Local file paths the server reads itself.")] = [],
    model: Annotated[str, Field(
        description="Alias or model id.")] = "pro",
    max_tokens: Annotated[int, Field(
        ge=1, le=65536, description="Maximum output tokens.")] = 8192,
    images: Annotated[List[str], Field(
        description="Screenshots of the result.")] = [],
) -> str:
    """Get an independent pass/fail verdict on finished work.

    Returns a verdict, confidence, concrete problems and next steps, so you can
    decide whether to stop or send another work order. Use a different model than
    the one that produced the work when possible.
    """
    if not goal or not goal.strip():
        return _err("goal must not be empty.")
    cleaned_files, files_err = _validate_files(files)
    if files_err:
        return _err(files_err)
    skipped_note = ""
    if paths:
        read_files, skipped, read_err = await asyncio.to_thread(
            _read_files_by_path, paths)
        if read_err:
            return _err(read_err)
        known = {(item.get("path") or "") for item in cleaned_files}
        for item in read_files:
            key = item.get("path") or ""
            if key not in known:
                cleaned_files.append(item)
                known.add(key)
        if skipped:
            skipped_note = "; ".join(f"{s['path']} ({s['reason']})"
                                     for s in skipped[:5])

    system = (
        "You are a strict code reviewer verifying another engineer's work.\n"
        "Be skeptical. Do not approve unless the evidence in the files supports it.\n"
        "Respond with a single JSON object:\n"
        '{"verdict": "pass" | "fail", "confidence": 0-100,\n'
        ' "problems": [{"file": "...", "issue": "...", "severity": "high|medium|low"}],\n'
        ' "next_steps": ["..."],\n'
        ' "summary": "one paragraph"}'
    )
    parts = [f"# Goal\n{goal}",
             "# Current files\n" + (_files_block(cleaned_files) or "(none provided)")]
    if skipped_note:
        parts.append(f"# Inputs not read\n{skipped_note}")
    prompt_text = "\n\n".join(parts)
    if len(prompt_text) > MAX_PROMPT_CHARS:
        return _err(f"prompt is {len(prompt_text)} chars, over the {MAX_PROMPT_CHARS} "
                    "limit. Split the review into smaller pieces.")

    prompt_parts: List[Dict[str, Any]] = [{"text": prompt_text}]
    if images:
        try:
            prompt_parts.extend(await asyncio.to_thread(_image_parts, images))
        except ValueError as exc:
            return _err(f"image input rejected: {exc}")

    payload_contents: Any = (prompt_text if not images
                             else [{"role": "user", "parts": prompt_parts}])

    result = await _chat_once(payload_contents, model, system,
                              temperature=0.1, max_tokens=max_tokens,
                              json_mode=True)
    if result.get("status") != "OK":
        return _ok(result)

    parsed = _safe_json(result["text"])
    if parsed is None:
        return _ok({"status": "UNPARSABLE",
                    "raw_text": result["text"][:3000]})
    if not isinstance(parsed, dict):
        return _ok({"status": "UNPARSABLE",
                    "raw_text": result["text"][:3000]})
    parsed["status"] = "OK"
    parsed["account"] = result.get("account")
    parsed["model"] = result.get("model")
    return _ok(parsed)


@app.tool(name=_t("map_reduce"))
async def map_reduce(
    items: Annotated[List[str], Field(
        description="Independent texts to process; each may be far larger than one request should carry.")],
    instruction: Annotated[str, Field(
        description="What to do with each chunk on its own.")],
    reduce_instruction: Annotated[str, Field(
        description="How to combine the per-chunk answers.")] = "Combine the partial answers into one coherent result.",
    model: Annotated[str, Field(
        description="Alias or model id.")] = "pro",
    max_parallel: Annotated[int, Field(
        ge=1, le=6, description="How many items run at once.")] = 3,
    chunk_chars: Annotated[int, Field(
        ge=1000, le=400000, description="Chunk size in characters.")] = 60000,
) -> str:
    """Chunk long input, one call per chunk, then one reduce call.

    The path for work that does not fit one request: oversized work orders, whole
    repositories, scraped pages. Each chunk is a normal pooled call, so pacing and
    cooldowns still apply; chunks of one item run in sequence, items in parallel.
    """
    if not items:
        return _err("items must not be empty.")
    if any(not isinstance(item, str) for item in items):
        return _err("every item must be a string.")
    if len(items) > 32:
        return _err("too many items (max 32).")
    total = sum(len(item) for item in items)
    if total > 4_000_000:
        return _err(f"input is {total} chars (~{total // 4} tokens). "
                    "Split it into several map_reduce calls.")
    result = await _map_reduce(items, instruction, model, reduce_instruction,
                               max_parallel=max_parallel, chunk_chars=chunk_chars)
    result["chars_in"] = total
    result["chars_out"] = len(result.get("text") or "")
    return _ok(result)


def main() -> None:
    app.run(transport="stdio")


if __name__ == "__main__":
    main()


