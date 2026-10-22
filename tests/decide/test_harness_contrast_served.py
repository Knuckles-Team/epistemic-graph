"""EG-DECISION-ENGINE-R803.3/.6 served receipts: the contrastive harness
composition decision over the wire.

The entry point is the served ``Decide`` method with the
``harness_contrast`` question kind: the caller declares each candidate
component version as an option (option id = the version's node id, the four
measured deltas as declared Q32 numbers, the probe trace id as a declared
text) and names the task, the task class, the baseline trace, the identical
probe conditions and the evidence graph as typed request params. The engine
reads the held versions, the probe runs and the stored contrasts out of the
named graph, adopts the improving candidate (positive ``eval_score_delta``
no other candidate beats, cost the tie-break) or abstains with
``no_improvement`` when nothing improves, and seals the answer as one record
whose ``inputs`` carry the decision. R803.3 commits that record through
``DecisionLog`` -- the commit runs verify-replay over the contrast's own
deterministic core -- and reads it back by record id.

The graph evidence (published component versions, probe runs, one stored
contrast) is written through the generated client write
(``ApplyChangeEnvelopes``; ``changes.apply_batch`` compiles to it), the same
entry point as the R801/R802 served tests.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from typing import Any

import msgpack
import pytest
from _decision_resident_fixture import publish
from conftest import TEST_TENANT, request_context

from epistemic_graph import decision_client as dc
from epistemic_graph import decision_stat as ds
from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.decision import digest_text

Q32 = 1 << 32
#: The two components whose versions the contrast candidates are: one
#: version per component, so each publishes as its component's first
#: version without a supersession of its own.
COMPONENT_A = "comp:contrast-a"
COMPONENT_B = "comp:contrast-b"
CONTENT_DIGEST_DOMAIN = "eg/harness-component-content/v1"
TASK_REF = "task:h2-r803"
TASK_CLASS = "eg:taskclass/contrast"
CONDITIONS_DIGEST = "sha256:conditions"
BASELINE = "trace:baseline"
#: The four contrast delta features, in the sorted order the decision
#: record's feature matrix names them.
CONTRAST_FEATURES = [
    "cost_delta",
    "eval_score_delta",
    "retry_delta",
    "tool_selection_delta",
]


def content_digest(content_ref: str) -> str:
    """The R801 vocabulary's content digest -- the engine's one digest scheme."""
    return digest_text(CONTENT_DIGEST_DOMAIN, content_ref)


def version_node_id(component: str, content_ref: str) -> str:
    return f"harness-component:{component}:{content_digest(content_ref)}"


def version_props(
    component: str, content_ref: str, valid_from: int, tx_from: int
) -> dict[str, Any]:
    """One published version as the writer composes it: the record's fields
    plus the engine-owned write marker and the client's transaction stamp."""
    return {
        "_harness_component": True,
        "component_id": component,
        "kind": "system_prompt",
        "content_ref": content_ref,
        "content_digest": content_digest(content_ref),
        "valid_from": valid_from,
        "confidence": 0.8,
        "evidence_run_id": f"run:{component}:{content_ref}",
        "applied_to_id": "agent:h2-r803",
        "tx_from": tx_from,
    }


def probe_run_node_id(trace_id: str) -> str:
    return f"harness-probe-run:{trace_id}"


def probe_run_props(trace_id: str, tx_from: int) -> dict[str, Any]:
    return {
        "_harness_probe_run": True,
        "trace_id": trace_id,
        "conditions_digest": CONDITIONS_DIGEST,
        "tx_from": tx_from,
    }


def contrast_node_id(run_a: str, run_b: str) -> str:
    return f"harness-contrast:{CONDITIONS_DIGEST}:{run_a}:{run_b}"


def contrast_props(
    run_a: str,
    run_b: str,
    retry_delta: int,
    tool_selection_delta: int,
    eval_score_delta: int,
    cost_delta: int,
    tx_from: int,
) -> dict[str, Any]:
    """One measured contrast: B minus A on the Q32 scale, run against the
    baseline under the identical probe conditions."""
    return {
        "_harness_contrast": True,
        "task_ref": TASK_REF,
        "conditions_digest": CONDITIONS_DIGEST,
        "run_a_trace_id": run_a,
        "run_b_trace_id": run_b,
        "retry_delta": retry_delta,
        "tool_selection_delta": tool_selection_delta,
        "eval_score_delta": eval_score_delta,
        "cost_delta": cost_delta,
        "tx_from": tx_from,
    }


def op(ordinal: int, method: str, params: dict[str, Any]) -> dict[str, Any]:
    return {
        "ordinal": ordinal,
        "surface": "graph",
        "domain": "graph_rows",
        "method": {"method": method, "params": params},
    }


def _envelope(
    graph: str,
    node_id: str,
    key: str,
    sequence: int,
    operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """One draft envelope for the governed write (see
    ``tests/orchestration/test_harness_lessons_served.py`` for the shape)."""
    body = repr(operations)
    return {
        "schema_version": 1,
        "envelope_id": f"envelope:{node_id}:{sequence}",
        "mutation": {
            "batch_id": f"batch:{node_id}:{sequence}",
            "graph": graph,
            "idempotency_key": key,
            "operations": operations,
            "outbox": [],
        },
        "content_version": {
            "object_id": node_id,
            "digest_algorithm": "sha256",
            "digest": hashlib.sha256(body.encode()).hexdigest(),
            "source_version": {"kind": "sequence", "value": sequence},
        },
        "policies": [
            {
                "policy_id": f"policy:{node_id}",
                "operation": "upsert",
                "object_id": node_id,
                "classification": "internal",
                "policy_version": "policy-v1",
                "subject_set_digest": "b" * 64,
            }
        ],
        "privacy": {
            "policy_version": "privacy-v1",
            "sanitizer_version": "sanitizer-v1",
            "sanitized_payload_digest": "c" * 64,
        },
    }


def _graph_name(client: Any) -> str:
    inner = getattr(client, "_client", client)
    return str(inner._graph_name)


def _apply_evidence(
    clean_graph: Any, key: str, nodes: list[tuple[str, dict[str, Any]]]
) -> None:
    """One envelope, in application order, writing the evidence nodes.

    The batch id derives from the caller's key: the shared session graph
    persists batch receipts across tests, so two tests may never claim the
    same batch id under different idempotency keys."""
    envelope = _envelope(
        _graph_name(clean_graph),
        key,
        key,
        1,
        [
            op(
                ordinal,
                "AddNode",
                {"node_id": node_id, "properties_msgpack": msgpack.packb(props)},
            )
            for ordinal, (node_id, props) in enumerate(nodes)
        ],
    )
    results = clean_graph.changes.apply_batch([envelope])
    assert [result["status"] for result in results] == ["applied"], results


def contrast_schema_body() -> dict[str, Any]:
    """The pinned feature schema: exactly the four contrast deltas, declared
    in the sorted order the record's matrix names them."""
    return ds.feature_schema_body(
        [ds.feature(name, ds.number(name)) for name in CONTRAST_FEATURES]
    )


def candidate(
    version_ref: str,
    probe_trace_id: str,
    retry_delta: int,
    tool_selection_delta: int,
    eval_score_delta: int,
    cost_delta: int,
) -> dc.DeclaredOption:
    """One declared candidate option: the version ref as option id, the four
    measured deltas as declared Q32 numbers, the probe trace as declared text."""
    return dc.DeclaredOption(
        version_ref,
        numbers={
            "retry_delta": retry_delta,
            "tool_selection_delta": tool_selection_delta,
            "eval_score_delta": eval_score_delta,
            "cost_delta": cost_delta,
        },
        texts={"probe_trace_id": probe_trace_id},
    )


def _contrast_params(graph: str) -> list[dict[str, Any]]:
    return [
        dc.param("baseline_trace_id", "text", BASELINE),
        dc.param("conditions_digest", "text", CONDITIONS_DIGEST),
        dc.param("graph", "text", graph),
        dc.param("task_class", "iri", TASK_CLASS),
        dc.param("task_ref", "text", TASK_REF),
    ]


async def _contrast_journey(
    schema: dict[str, Any],
    options: list[dc.DeclaredOption],
    question_id: str,
    graph: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Decide, commit through the decision log and read the entry back."""
    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
    client = await EpistemicGraphClient.connect(
        socket_path=socket_path,
        verified_context=request_context(),
    )
    try:
        decision = dc.DecisionClient(client=client)
        request = dc.decide_request(
            TEST_TENANT,
            (question_id, "harness_contrast", "ordinary"),
            dc.declared(options),
            schema,
            params=_contrast_params(graph),
        )
        payload = await decision.decide(request)
        record = payload["records"][0]
        committed = await decision.log(dc.commit_op(record))
        entry = await decision.log(dc.get_op(TEST_TENANT, record["record_id"]))
        return payload, record, committed, entry
    finally:
        await client.close()


def _expected_matrix(
    ordered_refs: list[str],
    traces: dict[str, str],
    deltas: dict[str, dict[str, int]],
) -> dict[str, Any]:
    """The inline matrix the engine seals: candidate order outer, the four
    delta features (sorted) inner, Q32."""
    values: list[int] = []
    for ref in ordered_refs:
        delta = deltas[traces[ref]]
        values.extend(
            [
                delta["cost_delta"],
                delta["eval_score_delta"],
                delta["retry_delta"],
                delta["tool_selection_delta"],
            ]
        )
    return {
        "matrix": "inline",
        "candidate_ids": ordered_refs,
        "feature_names": list(CONTRAST_FEATURES),
        "scale": "q32",
        "values": values,
    }


# spec: EG-DECISION-ENGINE-R803.3
@pytest.mark.spec("EG-DECISION-ENGINE-R803.3")
def test_contrast_decide_stores_record(clean_graph):
    # The evidence: the two held candidate versions (one per component),
    # the baseline probe run and the two candidate probe runs under the
    # identical conditions, and the stored contrast of the improving
    # candidate against the baseline -- every one written through the
    # generated client write.
    improving_ref = version_node_id(COMPONENT_A, "blob:improving")
    neutral_ref = version_node_id(COMPONENT_B, "blob:neutral")
    improving_trace = "trace:improving"
    neutral_trace = "trace:neutral"
    improving = {
        "retry_delta": -Q32,
        "tool_selection_delta": 0,
        "eval_score_delta": 2 * Q32,
        "cost_delta": -Q32,
    }
    neutral = {
        "retry_delta": 0,
        "tool_selection_delta": 0,
        "eval_score_delta": 0,
        "cost_delta": -Q32,
    }
    _apply_evidence(
        clean_graph,
        "h2-r803:evidence:adopt",
        [
            (improving_ref, version_props(COMPONENT_A, "blob:improving", 10, 100)),
            (neutral_ref, version_props(COMPONENT_B, "blob:neutral", 10, 101)),
            (probe_run_node_id(BASELINE), probe_run_props(BASELINE, 102)),
            (probe_run_node_id(improving_trace), probe_run_props(improving_trace, 103)),
            (probe_run_node_id(neutral_trace), probe_run_props(neutral_trace, 104)),
            (
                contrast_node_id(BASELINE, improving_trace),
                contrast_props(
                    BASELINE,
                    improving_trace,
                    improving["retry_delta"],
                    improving["tool_selection_delta"],
                    improving["eval_score_delta"],
                    improving["cost_delta"],
                    105,
                ),
            ),
        ],
    )

    schema = publish(
        clean_graph, "h2-r803-schema", "feature_schema", contrast_schema_body()
    )
    graph = _graph_name(clean_graph)
    options = [
        candidate(improving_ref, improving_trace, **improving),
        candidate(neutral_ref, neutral_trace, **neutral),
    ]
    payload, record, committed, entry = asyncio.run(
        _contrast_journey(schema, options, "q:h2-r803:adopt", graph)
    )

    # The answer is one sealed record; the decision is stored in its inputs
    # with the adopted candidate and the per-candidate deltas pinned.
    ordered_refs = sorted([improving_ref, neutral_ref])
    traces = {improving_ref: improving_trace, neutral_ref: neutral_trace}
    deltas = {improving_trace: improving, neutral_trace: neutral}
    assert payload["schema_version"] == record["schema_version"]
    assert payload["inputs_digest"] == record["inputs_digest"]
    assert len(payload["records"]) == 1
    assert record["question"] == {
        "question_id": "q:h2-r803:adopt",
        "kind": "harness_contrast",
        "safety": "ordinary",
    }
    assert record["candidate_source"]["source"] == "declared"
    assert record["outcome"] == {
        "outcome": "explored",
        "option_id": improving_ref,
        "propensity": {"numerator": 1, "denominator": 1},
    }
    assert record["resolution_kind"] == "statistical"
    decision = record["inputs"]["harness_contrast"]
    assert decision["task_ref"] == TASK_REF
    assert decision["task_class"] == TASK_CLASS
    assert decision["candidate_version_refs"] == ordered_refs
    assert decision["conditions_digest"] == CONDITIONS_DIGEST
    assert decision["probe_trace_ids"] == [traces[ref] for ref in ordered_refs]
    assert decision["deltas"] == [
        {
            "task_ref": TASK_REF,
            "conditions_digest": CONDITIONS_DIGEST,
            "run_a_trace_id": BASELINE,
            "run_b_trace_id": traces[ref],
            **deltas[traces[ref]],
        }
        for ref in ordered_refs
    ]
    assert decision["adopted_version_ref"] == improving_ref
    assert decision.get("abstain_reason") is None
    assert record["inputs"]["feature_matrix"] == _expected_matrix(
        ordered_refs, traces, deltas
    )
    assert record["inputs"]["head"] is None
    assert "harness_evaluation" not in record["inputs"]
    assert record["inputs"]["policy"] == {"policy": "default"}

    # The record commits through the decision log (the commit runs the
    # contrast's verify-replay) and reads back under its record id.
    assert committed["record_id"] == record["record_id"]
    assert committed["replayed"] is True
    assert committed["record_digest"] == record["record_digest"]
    assert entry["record"]["record_id"] == record["record_id"]
    assert entry["record"]["outcome"] == record["outcome"]
    assert entry["record"]["inputs"]["harness_contrast"] == decision
    assert entry["record"]["inputs"]["feature_matrix"] == _expected_matrix(
        ordered_refs, traces, deltas
    )
    assert entry["record"]["record_digest"] == record["record_digest"]
    assert entry["visibility"]["visibility"] == "principal"


# spec: EG-DECISION-ENGINE-R803.6
@pytest.mark.spec("EG-DECISION-ENGINE-R803.6")
def test_contrast_hot_path_served(clean_graph):
    # One improving and one neutral candidate adopt the improving one; two
    # non-improving candidates abstain with no_improvement and the
    # abstention stores through the same commit path.
    a_ref = version_node_id(COMPONENT_A, "blob:hot-a")
    b_ref = version_node_id(COMPONENT_B, "blob:hot-b")
    _apply_evidence(
        clean_graph,
        "h2-r803:evidence:hot",
        [
            (a_ref, version_props(COMPONENT_A, "blob:hot-a", 10, 100)),
            (b_ref, version_props(COMPONENT_B, "blob:hot-b", 10, 101)),
            (probe_run_node_id(BASELINE), probe_run_props(BASELINE, 102)),
            (probe_run_node_id("trace:hot-a"), probe_run_props("trace:hot-a", 103)),
            (probe_run_node_id("trace:hot-b"), probe_run_props("trace:hot-b", 104)),
            (probe_run_node_id("trace:hot-c"), probe_run_props("trace:hot-c", 105)),
            (probe_run_node_id("trace:hot-d"), probe_run_props("trace:hot-d", 106)),
        ],
    )

    schema = publish(
        clean_graph, "h2-r803-hot-schema", "feature_schema", contrast_schema_body()
    )
    graph = _graph_name(clean_graph)

    improving = candidate(a_ref, "trace:hot-a", 0, 0, 2 * Q32, -Q32)
    neutral = candidate(b_ref, "trace:hot-b", 0, 0, 0, -Q32)
    _, record, committed, _ = asyncio.run(
        _contrast_journey(schema, [improving, neutral], "q:h2-r803:hot-adopt", graph)
    )
    assert record["outcome"] == {
        "outcome": "explored",
        "option_id": a_ref,
        "propensity": {"numerator": 1, "denominator": 1},
    }
    assert record["inputs"]["harness_contrast"]["abstain_reason"] is None
    assert committed["replayed"] is True

    flat = candidate(a_ref, "trace:hot-c", 0, 0, 0, -Q32)
    worse = candidate(b_ref, "trace:hot-d", Q32, 0, -Q32, Q32)
    _, record, committed, entry = asyncio.run(
        _contrast_journey(schema, [flat, worse], "q:h2-r803:hot-abstain", graph)
    )
    assert record["outcome"] == {
        "outcome": "abstained",
        "reasons": [{"reason": "insufficient_confidence"}],
    }
    assert record["resolution_kind"] == "abstention"
    decision = record["inputs"]["harness_contrast"]
    assert decision["abstain_reason"] == "no_improvement"
    assert decision.get("adopted_version_ref") is None
    # The abstention commits and reads back with its closed reason.
    assert committed["record_id"] == record["record_id"]
    assert committed["replayed"] is True
    assert entry["record"]["inputs"]["harness_contrast"]["abstain_reason"] == (
        "no_improvement"
    )
