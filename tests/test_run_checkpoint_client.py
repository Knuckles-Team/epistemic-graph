"""Checkpoint facade request/receipt wire contract; native isolation is Rust-tested."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from _client_send_stub import CapturingEngine

from epistemic_graph.client import EpistemicGraphClient, WorkItemClient

pytestmark = pytest.mark.no_engine


def client(answer: object) -> tuple[WorkItemClient, CapturingEngine]:
    engine = CapturingEngine(answer)
    return WorkItemClient(cast(EpistemicGraphClient, engine)), engine


def test_checkpoint_read_does_not_dispatch_or_write() -> None:
    items, engine = client(None)
    assert (
        asyncio.run(items.get_checkpoint(tenant="t", work_item_id="wi", run_id="run"))
        is None
    )
    assert engine.sent == [
        (
            "GetRunCheckpoint",
            {
                "request": {
                    "tenant": "t",
                    "work_item_id": "wi",
                    "run_id": "run",
                    "current_claim": None,
                }
            },
        )
    ]


def test_checkpoint_publish_preserves_native_replayed_acknowledgement() -> None:
    receipt: dict[str, Any] = {
        "work_item_id": "wi",
        "run_id": "run",
        "revision": 1,
        "payload_digest": "a" * 64,
        "changed_work_item_ids": ["wi"],
        "replayed": True,
    }
    items, engine = client(receipt)
    snapshot = {
        "content": "{}",
        "payload_digest": "a" * 64,
        "short_term": [],
        "long_term": [],
    }
    result = asyncio.run(
        items.publish_checkpoint(
            tenant="t",
            work_item_id="wi",
            run_id="run",
            worker_id="worker",
            lease_epoch=2,
            fencing_token=3,
            expected_revision=None,
            snapshot=snapshot,
            idempotency_key="request-key",
        )
    )
    assert result == receipt
    method, params = engine.sent[0]
    assert method == "PublishRunCheckpoint"
    assert isinstance(params, dict)
    assert params["request"]["snapshot"] == snapshot
    assert params["request"]["expected_revision"] is None
