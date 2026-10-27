"""Explicit history negotiation preserves strict legacy claim decoding."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from _client_send_stub import CapturingEngine
from test_work_item_retry_identity import _claim_empty_result, _claim_request

from epistemic_graph.client import EpistemicGraphClient, WorkItemClient

pytestmark = pytest.mark.no_engine


def test_legacy_claim_result_rejects_new_witness() -> None:
    result = _claim_empty_result()
    result["fresh_execution_grant"] = False
    with pytest.raises(ValueError):
        WorkItemClient._claim_result(result)
    assert WorkItemClient._claim_result(result, completed_text_history=True) == result
    result["fresh_execution_grant"] = True
    with pytest.raises(ValueError, match="first claimed attempt"):
        WorkItemClient._claim_result(result, completed_text_history=True)


@pytest.mark.parametrize("value", ["true", 1, None])
def test_claim_opt_in_rejects_non_boolean(value: Any) -> None:
    engine = CapturingEngine(_claim_empty_result())
    client = WorkItemClient(cast(EpistemicGraphClient, engine))
    with pytest.raises(TypeError):
        asyncio.run(client.claim(_claim_request(), completed_text_history=value))
    assert engine.sent == []


def test_claim_opt_in_uses_wrapper_not_request() -> None:
    result = _claim_empty_result()
    engine = CapturingEngine(result)
    client = WorkItemClient(cast(EpistemicGraphClient, engine))
    request = _claim_request()
    assert asyncio.run(client.claim(request, completed_text_history=True)) == result
    assert engine.sent == [
        ("ClaimWorkItem", {"request": request, "completed_text_history": True})
    ]


def test_legacy_claim_omits_opt_in_wrapper() -> None:
    result = _claim_empty_result()
    engine = CapturingEngine(result)
    client = WorkItemClient(cast(EpistemicGraphClient, engine))
    request = _claim_request()
    assert asyncio.run(client.claim(request)) == result
    assert engine.sent == [("ClaimWorkItem", {"request": request})]
