"""Typed client for the statistical decision surface (EH-065).

``epistemic_graph.decision_stat`` carries the body codec and feature builders;
this module is the request side: typed builders for ``Decide`` requests
(including ``CandidateSource::Declared`` options) and every ``DecisionLog`` op,
and :class:`DecisionClient`, which sends them through the generated contract
senders (``send_decide``, ``send_decision_log``) so the generated request
validation runs on every call.

Every dict lists its keys in Rust field order and uses the serde wire names
(snake_case enums, ``tag`` fields), so what is built here is exactly what the
engine decodes. Numbers on declared options are ``Q32`` integers -- never a
float on the wire.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

Q32_ONE = 1 << 32


def q32_of(value: float) -> int:
    """``value`` on the ``Q32`` scale (rounded to the nearest representable)."""
    return round(float(value) * Q32_ONE)


@dataclass(frozen=True, slots=True)
class DeclaredOption:
    """One caller-declared option: every fact is the caller's claim."""

    option_id: str
    numbers: Mapping[str, int] = field(default_factory=dict)
    texts: Mapping[str, str] = field(default_factory=dict)
    classification: tuple[str, ...] = ()

    def wire(self) -> dict[str, Any]:
        return {
            "option_id": self.option_id,
            "classification": list(self.classification),
            "numbers": [
                {"key": k, "q32": int(self.numbers[k])} for k in sorted(self.numbers)
            ],
            "texts": [{"key": k, "text": self.texts[k]} for k in sorted(self.texts)],
        }


def declared(options: Iterable[DeclaredOption]) -> dict[str, Any]:
    """``CandidateSource::Declared``; options sorted by id (the matrix order)."""
    ordered = sorted(options, key=lambda o: o.option_id)
    ids = [o.option_id for o in ordered]
    if len(set(ids)) != len(ids):
        raise ValueError("declared option ids must be unique")
    return {"source": "declared", "options": [o.wire() for o in ordered]}


def library(
    kinds: Sequence[str], classification_under: str | None = None
) -> dict[str, Any]:
    """``CandidateSource::AgentLibrary`` over component kinds (wire names)."""
    return {
        "source": "agent_library",
        "scope": {"kinds": list(kinds), "classification_under": classification_under},
    }


def param(name: str, kind: str, value: Any) -> dict[str, Any]:
    """One ``TypedParam``: ``kind`` is ``bool|int|text|iri|iri_list|rational``."""
    return {"name": name, "value": {"type": kind, "value": value}}


def decide_request(
    tenant_id: str,
    question: tuple[str, str, str],
    candidates: Mapping[str, Any],
    feature_schema: Mapping[str, Any],
    *,
    head: Mapping[str, Any] | None = None,
    policy: Mapping[str, Any] | None = None,
    params: Iterable[Mapping[str, Any]] = (),
    max_records: int | None = None,
) -> dict[str, Any]:
    """A ``DecideRequest``; ``question`` is ``(question_id, kind, safety)``."""
    question_id, kind, safety = question
    return {
        "tenant_id": tenant_id,
        "question": {"question_id": question_id, "kind": kind, "safety": safety},
        "candidates": dict(candidates),
        "feature_schema": dict(feature_schema),
        "head": None if head is None else dict(head),
        "policy": dict(policy or {"policy": "default"}),
        "params": sorted((dict(p) for p in params), key=lambda p: str(p["name"])),
        "max_records": max_records,
    }


def commit_op(record: Mapping[str, Any]) -> dict[str, Any]:
    return {"op": "commit", "record": dict(record)}


def get_op(tenant_id: str, record_id: str) -> dict[str, Any]:
    return {"op": "get", "tenant_id": tenant_id, "record_id": record_id}


def resolve_op(
    tenant_id: str,
    record_id: str,
    resolution_id: str,
    option_id: str,
    *,
    producer: str | None = None,
    prompt_digest: str | None = None,
) -> dict[str, Any]:
    """Resolve a logged abstention: by the verified human caller when
    ``producer`` is ``None`` (an observation), else by that model (a claim)."""
    resolver: dict[str, Any] = (
        {"resolver": "human"}
        if producer is None
        else {"resolver": "model", "producer": producer, "prompt_digest": prompt_digest}
    )
    return {
        "op": "resolve",
        "tenant_id": tenant_id,
        "resolution": {
            "record_id": record_id,
            "resolution_id": resolution_id,
            "option_id": option_id,
            "resolver": resolver,
        },
    }


def evaluate_op(tenant_id: str, evaluation: Mapping[str, Any]) -> dict[str, Any]:
    return {"op": "evaluate", "tenant_id": tenant_id, "evaluation": dict(evaluation)}


def aggregate_op(
    tenant_id: str, window: tuple[int, int], question_id: str | None = None
) -> dict[str, Any]:
    return {
        "op": "aggregate",
        "request": {
            "tenant_id": tenant_id,
            "question_id": question_id,
            "window": {"from_ms": window[0], "to_ms": window[1]},
        },
    }


def query_op(tenant_id: str, sql: str) -> dict[str, Any]:
    """One read-only SQL statement over the caller's visible log (EH-066):
    relations ``decisions``, ``evaluations`` and ``resolutions``."""
    return {"op": "query", "tenant_id": tenant_id, "sql": sql}


def _payload(result: Any) -> Any:
    return getattr(result, "payload", result)


@dataclass(frozen=True, slots=True)
class DecisionClient:
    """``Decide`` and ``DecisionLog`` over the generated senders."""

    client: Any
    graph: str | None = None

    async def decide(self, request: Mapping[str, Any]) -> Any:
        from epistemic_graph.generated.query import send_decide

        return _payload(
            await send_decide(self.client, {"request": dict(request)}, self.graph)
        )

    async def log(
        self, op: Mapping[str, Any], *, idempotency_key: str | None = None
    ) -> Any:
        from epistemic_graph.generated.coordination import send_decision_log

        result = await send_decision_log(
            self.client, {"op": dict(op)}, self.graph, idempotency_key=idempotency_key
        )
        return _payload(result)


__all__ = [
    "Q32_ONE",
    "DecisionClient",
    "DeclaredOption",
    "aggregate_op",
    "commit_op",
    "decide_request",
    "declared",
    "evaluate_op",
    "get_op",
    "library",
    "param",
    "q32_of",
    "query_op",
    "resolve_op",
]
