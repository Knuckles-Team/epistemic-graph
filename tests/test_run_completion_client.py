"""Public WorkItem adapter forwards ordinary receipts without inventing usage."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest
from _client_send_stub import CapturingEngine

from epistemic_graph.client import EpistemicGraphClient, WorkItemClient

pytestmark = pytest.mark.no_engine


@pytest.mark.parametrize(
    "completion",
    [
        None,
        {"schema_version": 1, "run_id": "run", "usage": {}},
        {
            "schema_version": 1,
            "run_id": "run",
            "usage": {
                "requests": 1,
                "input_tokens": 3,
                "output_tokens": 2,
                "total_tokens": 5,
            },
        },
    ],
)
def test_public_commit_preserves_optional_completion(completion):
    transport = CapturingEngine({"status": "failed"})
    client = WorkItemClient(cast(EpistemicGraphClient, transport))
    result = asyncio.run(
        client.commit_result(
            tenant="tenant",
            work_item_id="work",
            worker_id="worker",
            lease_epoch=1,
            fencing_token=2,
            idempotency_key="commit",
            outcome="failed",
            now_ms=3,
            run_completion=completion,
        )
    )
    assert result == {"status": "failed"}
    assert len(transport.sent) == 1
    method, raw_params = transport.sent[0]
    params = cast(dict[str, object], raw_params)
    assert method == "CommitWorkItemResult"
    if completion is None:
        assert "run_completion" not in params
    else:
        assert params["run_completion"] == completion
