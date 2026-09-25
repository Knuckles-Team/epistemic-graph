"""EH-348 work market through ``EpistemicGraphClient.gaps`` / ``.work_market``."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.work_market import GapClient, WorkMarketClient

from _client_send_stub import CapturingEngine as _Engine

pytestmark = pytest.mark.no_engine

DIGEST = "sha256:" + "a" * 64
GAP: dict[str, Any] = {
    "gap_id": "gap:failure:timeout",
    "source": "failure",
    "signature": "timeout",
    "statement": "tool calls time out",
    "domain": "",
    "severity_ppm": 900_000,
    "priority_bucket": 0,
    "status": "open",
    "generation": 1,
    "work_item_id": "work-item:gap:1",
    "work": {"kind": "gap_remediation", "max_attempts": 3},
    "evidence": [],
    "evidence_count": 1,
    "concept_ids": [],
    "spec_refs": [],
    "offer": None,
    "offer_version": 0,
    "created_at_ms": 1,
    "updated_at_ms": 1,
    "revision": 1,
}



def _gaps(engine: _Engine) -> GapClient:
    return GapClient(cast(EpistemicGraphClient, engine))


def test_upsert_sends_the_typed_request_and_validates_the_answer() -> None:
    engine = _Engine(
        {
            "outcome": "created",
            "gap": GAP,
            "work_item_created": True,
            "changed_work_item_ids": ["gap-row:x", "work-item:gap:1"],
        }
    )
    answer = asyncio.run(
        _gaps(engine).upsert(
            tenant="tenant-a",
            gap_id="gap:failure:timeout",
            source="failure",
            signature="timeout",
            statement="tool calls time out",
            evidence=[
                {"digest": DIGEST, "kind": "failure_cluster", "reference": "c:1"}
            ],
            work_kind="gap_remediation",
            max_attempts=3,
            idempotency_key="upsert-1",
            severity_ppm=900_000,
        )
    )
    assert answer["outcome"] == "created"
    assert answer["work_item_created"] is True
    assert answer["gap"] == GAP
    method, params = engine.sent[0]
    assert method == "GapUpsert"
    assert isinstance(params, dict)
    assert params["request"]["work"] == {"kind": "gap_remediation", "max_attempts": 3}


def test_a_view_outside_the_contract_is_refused() -> None:
    engine = _Engine({**GAP, "lease_owner": "worker-1"})
    with pytest.raises(RuntimeError):
        asyncio.run(_gaps(engine).get(tenant="tenant-a", gap_id="gap:failure:timeout"))


def test_get_answers_none_for_an_invisible_gap_and_list_pages() -> None:
    assert asyncio.run(_gaps(_Engine(None)).get(tenant="t", gap_id="gap:a:b")) is None
    engine = _Engine({"gaps": [GAP], "next_cursor": None})
    page = asyncio.run(_gaps(engine).list(tenant="t", status="open", limit=10))
    assert page == {"gaps": [GAP], "next_cursor": None}
    assert engine.sent[0][0] == "GapList"


def test_settle_and_transition_carry_only_the_gap_identity() -> None:
    engine = _Engine({"outcome": "pending", "gap": GAP, "changed_work_item_ids": []})
    answer = asyncio.run(
        _gaps(engine).settle(tenant="t", gap_id="gap:a:b", idempotency_key="s")
    )
    assert answer["outcome"] == "pending"
    assert engine.sent[0] == (
        "GapSettle",
        {"request": {"tenant": "t", "gap_id": "gap:a:b", "idempotency_key": "s"}},
    )
    with pytest.raises(ValueError):
        asyncio.run(
            _gaps(engine).transition(
                tenant="t",
                gap_id="gap:a:b",
                expected_revision=1,
                to=cast(Any, "open"),
                reference="r",
                idempotency_key="k",
            )
        )


def test_put_offer_refuses_fields_outside_the_contract() -> None:
    market = WorkMarketClient(cast(EpistemicGraphClient, _Engine(None)))
    with pytest.raises(ValueError):
        asyncio.run(
            market.put_offer(
                tenant="t",
                gap_id="gap:a:b",
                expected_offer_version=0,
                offer={"score": 1},
                idempotency_key="k",
            )
        )
