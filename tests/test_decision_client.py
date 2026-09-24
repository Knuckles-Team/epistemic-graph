"""The typed decision client builds what the contract schema accepts (EH-065)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from epistemic_graph import decision_client as dc

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "contract/schemas/method.request.json").read_text())
DEFS = SCHEMA["$defs"]
PIN = {"component_id": "s", "kind": "feature_schema", "definition_digest": "sha256:x"}


def _resolve(node: dict[str, Any]) -> dict[str, Any]:
    while "$ref" in node:
        node = DEFS[node["$ref"].rsplit("/", 1)[-1]]
    return node


def _type_ok(node: dict[str, Any], value: Any) -> bool:
    kinds = node.get("type")
    if kinds is None:
        return True
    kinds = kinds if isinstance(kinds, list) else [kinds]
    checks = {
        "object": lambda v: isinstance(v, dict),
        "array": lambda v: isinstance(v, list),
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "boolean": lambda v: isinstance(v, bool),
        "null": lambda v: v is None,
        "number": lambda v: isinstance(v, int | float) and not isinstance(v, bool),
    }
    return any(checks[k](value) for k in kinds)


def _object_ok(node: dict[str, Any], value: Any) -> bool:
    props = node.get("properties", {})
    if any(key not in value for key in node.get("required", [])):
        return False
    if node.get("additionalProperties") is False and set(value) - set(props):
        return False
    return all(valid(props[k], v) for k, v in value.items() if k in props)


def valid(node: dict[str, Any], value: Any) -> bool:
    """A small JSON-Schema subset: $ref, oneOf/anyOf, const, type, objects, arrays."""
    node = _resolve(node)
    branches = node.get("oneOf") or node.get("anyOf")
    if branches is not None:
        return any(valid(branch, value) for branch in branches)
    if "const" in node and value != node["const"]:
        return False
    if not _type_ok(node, value):
        return False
    if isinstance(value, dict):
        return _object_ok(node, value)
    if isinstance(value, list) and "items" in node:
        return all(valid(node["items"], item) for item in value)
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
    assert valid(DEFS["DecideRequest"], request)
    assert [o["option_id"] for o in request["candidates"]["options"]] == [
        "plan-deep",
        "plan-hyde",
    ]
    broken = dict(request, candidates={"source": "declared", "options": [{"id": "x"}]})
    assert not valid(DEFS["DecideRequest"], broken), "the checker catches a bad shape"


def test_every_log_op_matches_the_contract() -> None:
    ops = [
        dc.get_op("tenant-a", "decision:abc"),
        dc.resolve_op("tenant-a", "decision:abc", "r-1", "plan-deep"),
        dc.resolve_op("tenant-a", "decision:abc", "r-2", "plan-deep", producer="llm"),
        dc.aggregate_op("tenant-a", (0, 10), "au.retrieval.plan"),
        dc.query_op("tenant-a", "SELECT count(*) FROM decisions"),
        dc.record_outcome_op(
            "tenant-a", "decision:abc", [("n1", "doc"), ("p1", None)], ["p1"]
        ),
        dc.hard_negatives_op("tenant-a", (0, 10), 50),
        dc.retrieval_op("tenant-a", "usage", window=dc.window_of((0, 10))),
        dc.retrieval_op("tenant-a", "adapter_status", space_digest="sha256:s"),
    ]
    for op in ops:
        assert valid(DEFS["DecisionLogOp"], op), op


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
