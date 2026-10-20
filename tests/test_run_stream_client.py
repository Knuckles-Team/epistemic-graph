"""Facade/generated transport tests; native durability is tested in Rust."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any, cast

import pytest
from _client_send_stub import CapturingEngine

from epistemic_graph.client import EpistemicGraphClient, WorkItemClient

pytestmark = pytest.mark.no_engine


class Engine(CapturingEngine):
    supports = EpistemicGraphClient.supports

    def __init__(self, answer: object, ops: list[str] | None = None) -> None:
        super().__init__(answer)
        self.ops = ["AppendRunEvent", "GetRunEvents"] if ops is None else ops
        self.health_calls = 0
        self.keys: list[object] = []

    async def health(self) -> dict[str, Any]:
        self.health_calls += 1
        return {"ops": self.ops}

    async def _send(
        self, method: str, params: object, graph: object, *, idempotency_key: object
    ) -> object:
        self.keys.append(idempotency_key)
        return await super()._send(
            method, params, graph, idempotency_key=idempotency_key
        )


def items(engine: Engine) -> WorkItemClient:
    return WorkItemClient(cast(EpistemicGraphClient, engine))


def test_health_negotiates_both_methods_once_and_old_engine_refuses_before_write() -> (
    None
):
    engine = Engine(None, ["GetRunEvents"])
    assert asyncio.run(items(engine).run_events_supported()) is False
    assert engine.health_calls == 1
    with pytest.raises(RuntimeError, match="unavailable"):
        asyncio.run(
            items(engine).append_event(
                tenant="t",
                work_item_id="wi",
                run_id="run",
                worker_id="worker",
                lease_epoch=1,
                fencing_token=2,
                expected_index=0,
                event={},
                idempotency_key="key",
            )
        )
    assert engine.sent == []
    assert engine.health_calls == 1


def test_get_events_is_read_only_and_preserves_caller_cursor_binding() -> None:
    engine = Engine(None)
    cursor = "eg-run-v1:1:" + "a" * 64
    assert (
        asyncio.run(
            items(engine).get_events(
                tenant="tenant",
                work_item_id="wi",
                run_id="run",
                after_cursor=cursor,
                limit=2,
            )
        )
        is None
    )
    assert engine.sent == [
        (
            "GetRunEvents",
            {
                "request": {
                    "tenant": "tenant",
                    "work_item_id": "wi",
                    "run_id": "run",
                    "after_cursor": cursor,
                    "limit": 2,
                }
            },
        )
    ]


def test_append_preserves_exact_native_retry_request_and_replayed_receipt() -> None:
    content = '{"text":"chunk"}'
    event = {
        "kind": "text_delta",
        "content": content,
        "payload_digest": hashlib.sha256(content.encode()).hexdigest(),
    }
    receipt = {
        "work_item_id": "wi",
        "run_id": "run",
        "event_index": 1,
        "payload_digest": event["payload_digest"],
        "replayed": True,
        "changed_work_item_ids": ["wi"],
    }
    engine = Engine(receipt)
    request: dict[str, Any] = {
        "tenant": "t",
        "work_item_id": "wi",
        "run_id": "run",
        "worker_id": "worker",
        "lease_epoch": 2,
        "fencing_token": 3,
        "expected_index": 0,
        "event": event,
        "idempotency_key": "exact-retry-key",
    }
    assert asyncio.run(items(engine).append_event(**request)) == receipt
    assert asyncio.run(items(engine).append_event(**request)) == receipt
    assert engine.sent == [("AppendRunEvent", {"request": request})] * 2
    assert engine.keys == ["exact-retry-key"] * 2


def test_commit_terminal_stream_is_atomic_request_extension() -> None:
    receipt = {
        "status": "succeeded",
        "work_item_id": "wi",
        "lease_epoch": 2,
        "fencing_token": 3,
        "changed_work_item_ids": ["wi"],
    }
    engine = Engine(receipt)
    asyncio.run(
        items(engine).commit_result(
            tenant="t",
            work_item_id="wi",
            worker_id="worker",
            lease_epoch=2,
            fencing_token=3,
            idempotency_key="terminal",
            outcome="succeeded",
            now_ms=4,
            terminal_stream={"run_id": "run", "expected_index": 2},
        )
    )
    assert [name for name, _ in engine.sent] == ["CommitWorkItemResult"]
    assert engine.sent[0][1]["terminal_stream"] == {
        "run_id": "run",
        "expected_index": 2,
    }
