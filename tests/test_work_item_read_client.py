"""EH-219 typed WorkItem reads through ``EpistemicGraphClient.work_items``."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from epistemic_graph.client import (
    ControlLeaseClient,
    EpistemicGraphClient,
    WorkItemClient,
)

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


# -- graph-os EG-2 native control leases -------------------------------------

LEASE: dict[str, Any] = {
    "lease_id": "browserlease_1",
    "kind": "browser.control",
    "status": "active",
    "grant": {"tool_ids": ["click"]},
    "issued_at_ms": 1_000,
    "expires_at_ms": 301_000,
    "hard_expires_at_ms": 901_000,
    "revision": 1,
}


def _leases(engine: _Engine) -> ControlLeaseClient:
    return ControlLeaseClient(cast(EpistemicGraphClient, engine))


def test_issue_sends_the_typed_request_and_validates_the_answer() -> None:
    engine = _Engine(
        {"outcome": "issued", "lease": LEASE, "changed_work_item_ids": ["x"]}
    )
    answer = asyncio.run(
        _leases(engine).issue(
            tenant="tenant-a",
            lease_id="browserlease_1",
            kind="browser.control",
            grant={"tool_ids": ["click"]},
            issued_at_ms=1_000,
            expires_at_ms=301_000,
            hard_expires_at_ms=901_000,
            idempotency_key="issue-1",
        )
    )
    assert answer == {"outcome": "issued", "lease": LEASE, "changed_ids": ["x"]}
    method, params = engine.sent[0]
    assert method == "IssueControlLease"
    assert isinstance(params, dict)
    assert params["request"]["idempotency_key"] == "issue-1"


def test_transition_and_get_round_trip_the_lease_view() -> None:
    ended = {**LEASE, "status": "revoked", "revision": 2}
    engine = _Engine(
        {"outcome": "applied", "lease": ended, "changed_work_item_ids": []}
    )
    answer = asyncio.run(
        _leases(engine).transition(
            tenant="tenant-a",
            lease_id="browserlease_1",
            expected_revision=1,
            to="revoked",
            idempotency_key="end-1",
        )
    )
    assert answer["lease"] == ended
    assert asyncio.run(_leases(_Engine(None)).get(tenant="t", lease_id="l")) is None
    assert asyncio.run(_leases(_Engine(LEASE)).get(tenant="t", lease_id="l")) == LEASE


def test_a_lease_answer_outside_the_contract_is_refused() -> None:
    with pytest.raises(RuntimeError):
        asyncio.run(
            _leases(_Engine({**LEASE, "extra": 1})).get(tenant="t", lease_id="l")
        )
    with pytest.raises(ValueError):
        asyncio.run(
            _leases(_Engine(None)).transition(
                tenant="t",
                lease_id="l",
                expected_revision=1,
                to=cast(Any, "active"),
                idempotency_key="k",
            )
        )


# -- graph-os EG-3 committed provenance ---------------------------------------


def test_get_outcome_returns_the_verified_provenance_view() -> None:
    outcome = {
        "work_item": VIEW,
        "trace_ref": "rt-1",
        "tool_call_refs": ["tc-1"],
        "outcome_ref": "oe-1",
        "outcome": {"status": "succeeded"},
    }
    engine = _Engine(outcome)
    items = _work_items(engine)
    assert asyncio.run(items.get_outcome(tenant="t", work_item_id="wi-1")) == outcome
    assert engine.sent == [
        ("GetWorkItemOutcome", {"tenant": "t", "work_item_id": "wi-1"})
    ]
    leaky = {**outcome, "work_item": {**VIEW, "lease_epoch": 3}}
    with pytest.raises(RuntimeError):
        asyncio.run(
            _work_items(_Engine(leaky)).get_outcome(tenant="t", work_item_id="wi-1")
        )


def test_commit_result_carries_the_outcome_extension_only_when_given() -> None:
    engine = _Engine({"status": "succeeded"})
    common: dict[str, Any] = {
        "tenant": "t",
        "work_item_id": "wi-1",
        "worker_id": "w",
        "lease_epoch": 1,
        "fencing_token": 1,
        "idempotency_key": "k",
        "outcome": "succeeded",
        "now_ms": 5,
    }
    asyncio.run(_work_items(engine).commit_result(**common))
    asyncio.run(
        _work_items(engine).commit_result(**common, outcome_extension={"bundle": 1})
    )
    first, second = (params for _, params in engine.sent)
    assert isinstance(first, dict) and "outcome_extension" not in first
    assert isinstance(second, dict) and second["outcome_extension"] == {"bundle": 1}
