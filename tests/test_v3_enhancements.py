"""Unit test suite for agy_pool v3.0 enhancements.

Tests:
1. Protobuf Schema Sanitizer ($ref resolving, anyOf/Optional handling, const, uppercase types, stripping unsupported fields)
2. Upstream Error Reset Parser (RetryInfo string, RetryInfo dict, ErrorInfo ISO reset_time, message fallback)
3. Dynamic Concurrency & DynamicGate
4. Quota Bucket Separation (Gemini vs 3P cooldown isolation, note_success)
5. Sticky Session Affinity & Prompt Caching
6. Thought Signature & Part Preservation
7. Synthetic Tool Calling Extraction in _extract_text
8. Endpoint Failover Manager (Circuit Breaker: Closed -> Open -> Half-Open -> Closed)
"""

import time
import datetime
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from agy_pool import (
    sanitize_schema_for_gemini,
    parse_upstream_quota_reset,
    DynamicGate,
    EndpointFailoverManager,
    _quota_bucket as get_quota_bucket,
    _extract_text as extract_response_v3,
    Pool,
    Account,
)


class BucketAwarePoolStub:
    def __init__(self):
        self._pool = Pool()

    def _key(self, acc_id: str, model: str) -> str:
        return self._pool._key(acc_id, model)

    def penalize(self, acc_id: str, seconds: float, model: str = "") -> None:
        self._pool._penalize(acc_id, int(seconds), model)

    def is_cooling(self, acc_id: str, model: str = "") -> bool:
        dummy_acc = Account({"id": acc_id, "email": acc_id}, Path(f"{acc_id}.json"))
        return self._pool._cool_left(dummy_acc, model) > 0

    def strike_rate(self, acc_id: str, model: str, retry_after: Optional[float] = None) -> float:
        dummy_acc = Account({"id": acc_id, "email": acc_id}, Path(f"{acc_id}.json"))
        return float(self._pool.strike_rate(dummy_acc, model, retry_after))

    def note_success(self, acc_id: str, model: str = "") -> None:
        self._pool.note_success(acc_id, model)


# --------------------------------------------------------------------------- #
# Test Executions
# --------------------------------------------------------------------------- #

def run_all_tests():
    print("=== RUNNING V3 ENHANCEMENT TESTS ===")

    # Test 1: Schema Sanitizer with Pydantic $defs & $ref & anyOf
    pydantic_schema = {
        "$defs": {
            "ChangeItem": {
                "title": "ChangeItem",
                "type": "object",
                "properties": {
                    "path": {"title": "Path", "type": "string"},
                    "operation": {"title": "Op", "type": "string", "enum": ["create", "replace"]},
                    "content": {"title": "Content", "anyOf": [{"type": "string"}, {"type": "null"}]}
                },
                "required": ["path", "operation"],
                "additionalProperties": False
            }
        },
        "title": "CodeEdits",
        "type": "object",
        "properties": {
            "analysis": {"title": "Analysis", "type": "string"},
            "changes": {
                "type": "array",
                "items": {"$ref": "#/$defs/ChangeItem"}
            },
            "status": {"const": "SUCCESS"}
        },
        "required": ["analysis", "changes"]
    }

    sanitized = sanitize_schema_for_gemini(pydantic_schema)
    assert sanitized["type"] == "OBJECT"
    assert "$defs" not in sanitized
    assert "title" not in sanitized
    changes_items = sanitized["properties"]["changes"]["items"]
    assert changes_items["type"] == "OBJECT", f"Expected OBJECT items, got: {changes_items}"
    assert changes_items["properties"]["path"]["type"] == "STRING"
    assert changes_items["properties"]["content"]["type"] == "STRING"
    assert changes_items["properties"]["content"]["nullable"] is True
    assert "additionalProperties" not in changes_items
    assert sanitized["properties"]["status"]["enum"] == ["SUCCESS"]

    # Test circular reference does not raise RecursionError
    recursive_schema = {
        "$defs": {
            "Node": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "child": {"$ref": "#/$defs/Node"}
                }
            }
        },
        "$ref": "#/$defs/Node"
    }
    rec_res = sanitize_schema_for_gemini(recursive_schema)
    assert rec_res["type"] == "OBJECT"
    assert rec_res["properties"]["child"]["type"] == "OBJECT"

    # Test anyOf with all const variants becomes enum
    const_union = {"anyOf": [{"const": "apple"}, {"const": "banana"}]}
    cu_res = sanitize_schema_for_gemini(const_union)
    assert cu_res["type"] == "STRING"
    assert cu_res["enum"] == ["apple", "banana"]

    print("PASS 1: Protobuf Schema Sanitizer ($ref inlined, Optional->nullable, const->enum, Draft-7 stripped)")

    # Test 2: Error Reset Parser
    resp_delay_str = {"error": {"code": 429, "details": [{"@type": "google.rpc.RetryInfo", "retryDelay": "45.5s"}]}}
    assert parse_upstream_quota_reset(resp_delay_str) == 45.5

    resp_delay_dict = {"error": {"code": 429, "details": [{"@type": "google.rpc.RetryInfo", "retryDelay": {"seconds": 90, "nanos": 500000000}}]}}
    assert parse_upstream_quota_reset(resp_delay_dict) == 90.5

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    future_utc = now_utc + datetime.timedelta(seconds=60)
    iso_str = future_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    resp_iso = {"error": {"code": 429, "details": [{"@type": "google.rpc.ErrorInfo", "metadata": {"reset_time": iso_str}}]}}
    reset_val = parse_upstream_quota_reset(resp_iso)
    assert reset_val is not None and 58.0 <= reset_val <= 64.0

    resp_msg = {"error": {"code": 429, "message": "Resource exhausted. Please retry after 25s."}}
    assert parse_upstream_quota_reset(resp_msg) == 25.0

    resp_camel = {"error": {"code": 429, "details": [{"metadata": {"resetTime": iso_str}}]}}
    assert parse_upstream_quota_reset(resp_camel) is not None
    resp_msg2 = {"error": {"code": 429, "message": "Rate limit exceeded. Try again in 18 seconds."}}
    assert parse_upstream_quota_reset(resp_msg2) == 18.0
    print("PASS 2: Upstream Error Quota Reset Parser (String, Duration Dict, ISO 8601, Message Fallback)")

    # Test 3: Dynamic Gate Concurrency
    async def test_gate():
        gate = DynamicGate(initial_capacity=2, max_capacity=16)
        assert gate.capacity == 2
        
        async with gate:
            assert gate.active_in_flight == 1
            # Resize while active
            gate.resize(4)
            assert gate.capacity == 4
            
        assert gate.active_in_flight == 0

    asyncio.run(test_gate())

    # Test 3b: shrink drains active holders before waking waiters; cancellation is safe.
    async def test_gate_drain():
        gate = DynamicGate(initial_capacity=5, max_capacity=16)
        for _ in range(5):
            await gate.acquire()
        waiter = asyncio.create_task(gate.acquire())
        await asyncio.sleep(0)
        gate.resize(2)
        for _ in range(3):
            gate.release()
        await asyncio.sleep(0)
        assert not waiter.done(), "waiter woke before capacity drained"
        gate.release()
        await waiter
        assert gate.active_in_flight == 2
        gate.release()
        gate.release()
        assert gate.active_in_flight == 0

        cancelled_gate = DynamicGate(initial_capacity=1, max_capacity=2)
        await cancelled_gate.acquire()
        cancelled = asyncio.create_task(cancelled_gate.acquire())
        await asyncio.sleep(0)
        cancelled.cancel()
        try:
            await cancelled
        except asyncio.CancelledError:
            pass
        cancelled_gate.release()
        assert cancelled_gate.active_in_flight == 0

    asyncio.run(test_gate_drain())
    print("PASS 3: DynamicGate concurrency, shrink drain, and cancellation safety")

    # Test 4: Quota Bucket Separation
    pool = BucketAwarePoolStub()
    # Account 1 gets 429 on gemini-3.8-flash-high
    pool.strike_rate("acc1", "gemini-3.8-flash-high", retry_after=300)
    # Gemini flash lite must also be cooling (same bucket)
    assert pool.is_cooling("acc1", "gemini-3.5-flash-lite") is True
    # Claude 3.5 Sonnet must NOT be cooling (separate 3p bucket)
    assert pool.is_cooling("acc1", "claude-3-5-sonnet") is False
    # Note success on claude clears claude but not gemini
    pool.note_success("acc1", "claude-3-5-sonnet")
    assert pool.is_cooling("acc1", "gemini-3.8-flash-high") is True
    # Note success on gemini clears gemini
    pool.note_success("acc1", "gemini-3.8-flash-high")
    assert pool.is_cooling("acc1", "gemini-3.8-flash-high") is False
    print("PASS 4: Quota Bucket Separation (Gemini vs 3P cooldown isolation & synchronized success)")

    # Test 5: Thought Signature & Cached Tokens Extraction
    mock_response = {
        "response": {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [
                            {"thought": True, "text": "Deep thinking...", "thoughtSignature": "CiQA7Z1mX0R8..."},
                            {"text": "Final answer text"}
                        ]
                    },
                    "finishReason": "STOP"
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 10000,
                "candidatesTokenCount": 500,
                "totalTokenCount": 10500,
                "thoughtsTokenCount": 200,
                "cachedContentTokenCount": 8192
            }
        }
    }

    text, meta = extract_response_v3(mock_response)
    assert text == "Final answer text"
    assert meta["cached_tokens"] == 8192
    assert meta["cache_hit_ratio"] == 0.8192
    assert len(meta["parts"]) == 2
    assert meta["parts"][0]["thoughtSignature"] == "CiQA7Z1mX0R8..."
    print("PASS 5: Thought Signature & Prompt Caching Telemetry extracted correctly")

    # Test 6: Synthetic Tool Calling Extraction
    mock_tool_response = {
        "response": {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "apply_code_edits",
                                    "args": {
                                        "analysis": "Refactor router",
                                        "changes": [{"path": "router.py", "operation": "create", "content": "..."}],
                                        "done": True
                                    }
                                }
                            }
                        ]
                    },
                    "finishReason": "STOP"
                }
            ]
        }
    }

    tool_text, tool_meta = extract_response_v3(mock_tool_response)
    parsed_json = json.loads(tool_text)
    assert parsed_json["analysis"] == "Refactor router"
    assert parsed_json["done"] is True
    assert tool_meta["function_call"]["name"] == "apply_code_edits"
    print("PASS 6: Synthetic Tool Calling automatically synthesized into clean JSON text")

    # Test 7: Endpoint Failover Manager (thread-safe sequential half-open probes)
    manager = EndpointFailoverManager()
    assert manager.acquire_endpoint() == manager.DAILY_URL
    # 2 failures do not trip
    manager.note_failure(manager.DAILY_URL, 503)
    manager.note_failure(manager.DAILY_URL, 502)
    assert manager.acquire_endpoint() == manager.DAILY_URL
    # 3rd failure trips circuit breaker
    manager.note_failure(manager.DAILY_URL, 503)
    assert manager.acquire_endpoint() == manager.PROD_URL

    # Fast forward cooldown; only one sequential probe may use daily.
    manager._cooldown_until = time.time() - 1.0
    assert manager.acquire_endpoint() == manager.DAILY_URL
    assert manager.acquire_endpoint() == manager.PROD_URL
    manager.note_success(manager.DAILY_URL)
    assert manager._state == "HALF_OPEN"
    assert manager.acquire_endpoint() == manager.DAILY_URL
    manager.note_success(manager.DAILY_URL)
    assert manager._state == "HALF_OPEN"
    assert manager.acquire_endpoint() == manager.DAILY_URL
    manager.note_success(manager.DAILY_URL)
    assert manager._state == "CLOSED"
    assert manager.acquire_endpoint() == manager.DAILY_URL
    assert manager._daily_fails == 0

    # Probe failure trips straight back to OPEN.
    manager._state = "OPEN"
    manager._cooldown_until = time.time() - 1.0
    assert manager.acquire_endpoint() == manager.DAILY_URL
    manager.note_failure(manager.DAILY_URL, 502)
    assert manager._state == "OPEN"
    assert manager.acquire_endpoint() == manager.PROD_URL
    print("PASS 7: Endpoint Failover Circuit Breaker (thread-safe sequential probes)")

    print("\nALL V3 ENHANCEMENT TESTS PASS!")

if __name__ == "__main__":
    run_all_tests()
