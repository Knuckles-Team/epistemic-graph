"""EH-219 typed WorkItem reads through ``EpistemicGraphClient.work_items``."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from epistemic_graph.client import EpistemicGraphClient, WorkItemClient

pytestmark = pytest.mark.no_engine

VIEW: dict[str, Any] = {
    "work_item_id": "wi-1",
    "kind": "au.task",
    "status": "ready",
    "input_ref": "au-task:sha256:abc",
    "metadata": {"au:description": "summarize"},
    "version": 2,
    "updated_at_ms": 1_000,
}


class _Engine:
    def __init__(self, answer: object) -> None:
        self.answer = answer
        self.sent: list[tuple[str, object]] = []

    async def _send(
        self,
        method: str,
        params: object,
        graph: object,
        *,
        idempotency_key: object,
    ) -> object:
        self.sent.append((method, params))
        return self.answer


def _work_items(engine: _Engine) -> WorkItemClient:
    return WorkItemClient(cast(EpistemicGraphClient, engine))


def test_get_sends_the_typed_request_and_returns_the_view() -> None:
    engine = _Engine(VIEW)
    view = asyncio.run(_work_items(engine).get(tenant="tenant-a", work_item_id="wi-1"))

    assert view == VIEW
    assert engine.sent == [
        ("GetWorkItem", {"tenant": "tenant-a", "work_item_id": "wi-1"})
    ]


def test_get_answers_none_for_an_invisible_item() -> None:
    engine = _Engine(None)
    assert (
        asyncio.run(_work_items(engine).get(tenant="tenant-a", work_item_id="wi-9"))
        is None
    )


def test_list_forwards_paging_and_returns_the_page() -> None:
    engine = _Engine({"items": [VIEW], "next_cursor": "opaque"})
    page = asyncio.run(
        _work_items(engine).list(
            tenant="tenant-a", cursor="c1", limit=5, kind="au.task"
        )
    )

    assert page == {"items": [VIEW], "next_cursor": "opaque"}
    assert engine.sent == [
        (
            "ListWorkItems",
            {"tenant": "tenant-a", "cursor": "c1", "limit": 5, "kind": "au.task"},
        )
    ]


def test_a_view_carrying_lease_authority_never_reaches_the_caller() -> None:
    leaky = {**VIEW, "fencing_token": 7}
    with pytest.raises(RuntimeError):
        asyncio.run(
            _work_items(_Engine(leaky)).get(tenant="tenant-a", work_item_id="wi-1")
        )
    with pytest.raises(RuntimeError):
        asyncio.run(
            _work_items(_Engine({"items": [leaky], "next_cursor": None})).list(
                tenant="tenant-a"
            )
        )


@pytest.mark.parametrize("limit", [0, 101])
def test_list_refuses_a_limit_outside_the_engine_bound(limit: int) -> None:
    engine = _Engine({"items": [], "next_cursor": None})
    with pytest.raises(ValueError):
        asyncio.run(_work_items(engine).list(tenant="tenant-a", limit=limit))
    assert engine.sent == []
