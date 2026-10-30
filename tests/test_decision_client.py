"""The typed decision client builds what the wire models accept (EH-065)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from epistemic_graph import decision_client as dc
from epistemic_graph.generated import models

pytestmark = pytest.mark.no_engine

PIN = {"component_id": "s", "kind": "feature_schema", "definition_digest": "sha256:x"}


def valid(model: Any, value: Any) -> bool:
    """Whether ``value`` validates as the wire model (extra fields forbidden)."""
    try:
        TypeAdapter(model).validate_python(value)
    except ValidationError:
        return False
    return True


def _options() -> list[dc.DeclaredOption]:
    return [
        dc.DeclaredOption("plan-hyde", {"threshold": dc.q32_of(0.38)}),
        dc.DeclaredOption(
            "plan-deep", {"threshold": dc.q32_of(0.28)}, {"label": "deep"}
        ),
    ]


def test_a_declared_decide_request_matches_the_contract() -> None:
    request = dc.decide_request(
        "tenant-a",
        ("au.retrieval.plan", "retrieval_plan", "ordinary"),
        dc.declared(_options()),
        PIN,
        params=[dc.param("query", "text", "who owns billing")],
    )
    assert valid(models.DecideRequest, request)
    assert [o["option_id"] for o in request["candidates"]["options"]] == [
        "plan-deep",
        "plan-hyde",
    ]
    broken = dict(request, candidates={"source": "declared", "options": [{"id": "x"}]})
    assert not valid(models.DecideRequest, broken), "the checker catches a bad shape"


def test_a_named_evaluator_is_a_principal_or_a_policy_role() -> None:
    by_role = dc.named_evaluator(1, role="decide-evaluator")
    by_principal = dc.named_evaluator(1, principal="principal:sha256:" + "0" * 64)
    for named in (by_role, by_principal):
        assert valid(models.NamedEvaluator, named), named


def test_every_log_op_matches_the_contract() -> None:
    ops = [
        dc.get_op("tenant-a", "decision:abc"),
        dc.resolve_op("tenant-a", "decision:abc", "r-1", "plan-deep"),
        dc.resolve_op("tenant-a", "decision:abc", "r-2", "plan-deep", producer="llm"),
        dc.aggregate_op("tenant-a", (0, 10), "au.retrieval.plan"),
        dc.record_outcome_op(
            "tenant-a", "decision:abc", [("n1", "prose"), ("p1", None)], ["p1"]
        ),
        dc.learn_op(
            "tenant-a",
            "move_pointer",
            pointer={"pointer": "adapter", "graph": "kg"},
            movement={"movement": "rollback"},
        ),
    ]
    for op in ops:
        assert valid(models.DecisionLogOp, op), op


def test_duplicate_declared_ids_are_refused_before_the_wire() -> None:
    with pytest.raises(ValueError):
        dc.declared([dc.DeclaredOption("a"), dc.DeclaredOption("a")])


def test_the_client_sends_through_the_generated_senders() -> None:
    sent: list[tuple[str, Any]] = []

    class _Client:
        async def _send(self, method: str, params: Any, graph: Any, **_: Any) -> Any:
            sent.append((method, params))
            return {"records": []}

    client = dc.DecisionClient(_Client(), graph="tenant-a")
    request = dc.decide_request(
        "tenant-a", ("q", "route", "ordinary"), dc.declared(_options()), PIN
    )
    assert asyncio.run(client.decide(request)) == {"records": []}
    asyncio.run(client.log(dc.get_op("tenant-a", "decision:abc")))
    assert [m for m, _ in sent] == ["Decide", "DecisionLog"]


@pytest.mark.spec("EG-DECISION-ENGINE-R076")
def test_decide_can_opt_into_the_nested_pydantic_decision_batch_model() -> None:
    """EG-DECISION-ENGINE-R076: as_model=True yields the nested DecisionBatch
    model; the default (as_model=False, exercised above) stays the raw
    payload, so other callers of this client are unaffected."""
    from epistemic_graph.generated.models import DecisionBatch

    class _Client:
        async def _send(self, method: str, params: Any, graph: Any, **_: Any) -> Any:
            return {
                "schema_version": 2,
                "inputs_digest": "sha256:" + "0" * 64,
                "records": [],
            }

    client = dc.DecisionClient(_Client(), graph="tenant-a")
    request = dc.decide_request(
        "tenant-a", ("q", "route", "ordinary"), dc.declared(_options()), PIN
    )
    model = asyncio.run(client.decide(request, as_model=True))
    assert isinstance(model, DecisionBatch)
    assert model.schema_version == 2
    assert model.records == []
