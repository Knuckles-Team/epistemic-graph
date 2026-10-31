"""MEDULLA-ORCH-VOCAB-R802.6 served receipt: contrast, adoption, lesson, and refusals.

The entry point is the generated client write through
``ApplyChangeEnvelopes`` (``changes.apply_batch`` compiles to it): one
envelope writes the two probe runs, the behavioral contrast of them, the
adopted harness reading that contrast, and the lesson learned from it — in
application order, so the contrast's runs and the lesson's evidence are the
earlier ops of the same write. The records are then read back through the
node property reads, keyed by their own (deficiency- and content-derived)
identities. Each of the three R802.5 negative cases refuses with its own
served code and stores nothing.
"""

from __future__ import annotations

from typing import Any

import msgpack
from _harness_envelope import draft_envelope as _envelope
from _harness_envelope import graph_name as _graph_name
from _harness_envelope import op

from medulla.client import EngineResponseError
from medulla.decision import digest_text

TASK_CLASS = "task:served"
DEFICIENCY = "deficiency:retries"
AGENT_REF = "agent:served"
CONDITIONS_DIGEST = "sha256:conditions"
TRACE_A = "trace-a"
TRACE_B = "trace-b"
LESSON_DIGEST_DOMAIN = "eg/harness-lesson/v1"
ADOPTION_DIGEST_DOMAIN = "eg/adopted-harness-refs/v1"


def contrast_node_id(conditions_digest: str, run_a: str, run_b: str) -> str:
    return f"harness-contrast:{conditions_digest}:{run_a}:{run_b}"


def adoption_node_id(component_version_refs: list[str]) -> str:
    digest = digest_text(ADOPTION_DIGEST_DOMAIN, list(component_version_refs))
    return f"adopted-harness:{AGENT_REF}:{TASK_CLASS}:{digest}"


def lesson_node_id(
    situation: str,
    what_changed: str,
    applicability: list[str],
    confidence: float,
    evidence_run_ids: list[str],
) -> str:
    digest = digest_text(
        LESSON_DIGEST_DOMAIN,
        (
            situation,
            "system_prompt",
            what_changed,
            list(applicability),
            confidence,
            list(evidence_run_ids),
        ),
    )
    return f"harness-lesson:{TASK_CLASS}:{DEFICIENCY}:{digest}"


def probe_run_props(trace_id: str, tx_from: int) -> dict[str, Any]:
    return {
        "_harness_probe_run": True,
        "trace_id": trace_id,
        "conditions_digest": CONDITIONS_DIGEST,
        "tx_from": tx_from,
    }


def contrast_props(tx_from: int) -> dict[str, Any]:
    """One measured contrast: B minus A, the retry count improved."""
    return {
        "_harness_contrast": True,
        "task_ref": TASK_CLASS,
        "conditions_digest": CONDITIONS_DIGEST,
        "run_a_trace_id": TRACE_A,
        "run_b_trace_id": TRACE_B,
        "retry_delta": -1,
        "tool_selection_delta": 0,
        "eval_score_delta": 0,
        "cost_delta": 0,
        "tx_from": tx_from,
    }


def adoption_props(
    component_version_refs: list[str], contrast_ref: str, tx_from: int
) -> dict[str, Any]:
    return {
        "_adopted_harness": True,
        "agent_ref": AGENT_REF,
        "task_class": TASK_CLASS,
        "component_version_refs": list(component_version_refs),
        "decision_ref": "decision:served:h2-r802",
        "contrast_ref": contrast_ref,
        "tx_from": tx_from,
    }


def lesson_props(
    situation: str,
    what_changed: str,
    applicability: list[str],
    confidence: float,
    evidence_run_ids: list[str],
    tx_from: int,
) -> dict[str, Any]:
    return {
        "_harness_lesson": True,
        "task_class": TASK_CLASS,
        "deficiency": DEFICIENCY,
        "situation": situation,
        "changed_component_kind": "system_prompt",
        "what_changed": what_changed,
        "applicability": list(applicability),
        "confidence": confidence,
        "evidence_run_ids": list(evidence_run_ids),
        "tx_from": tx_from,
    }


def _single_op_apply(
    clean_graph: Any,
    graph: str,
    node_id: str,
    key: str,
    sequence: int,
    properties: dict[str, Any],
) -> None:
    envelope = _envelope(
        graph,
        node_id,
        key,
        sequence,
        [
            op(
                0,
                "AddNode",
                {
                    "node_id": node_id,
                    "properties_msgpack": msgpack.packb(properties),
                },
            )
        ],
    )
    clean_graph.changes.apply(envelope)


# spec: MEDULLA-ORCH-VOCAB-R802.6
def test_contrast_lesson_and_retrieval_served(clean_graph):
    graph = _graph_name(clean_graph)
    contrast_id = contrast_node_id(CONDITIONS_DIGEST, TRACE_A, TRACE_B)
    version_ref = (
        "harness-component:comp:served-prompt:"
        "sha256:2447aaa8f76dcc91df6588294cb693e9c5164c97e460587b798111dfb88f7ac8"
    )
    adoption_id = adoption_node_id([version_ref])
    situation = "the run retried the failing tool"
    what_changed = "tightened the retry policy"
    applicability = ["feature:long-context"]
    confidence = 0.85
    evidence = [TRACE_A, TRACE_B]
    lesson_id = lesson_node_id(
        situation, what_changed, applicability, confidence, evidence
    )

    # One write, in application order: the two probe runs, the contrast of
    # them, the adoption reading that contrast, and the lesson citing the
    # runs as evidence. Each record carries its client-stamped transaction
    # time (the engine verifies instead of stamps on this path).
    envelope = _envelope(
        graph,
        lesson_id,
        "h2-r802:library",
        1,
        [
            op(
                0,
                "AddNode",
                {
                    "node_id": f"harness-probe-run:{TRACE_A}",
                    "properties_msgpack": msgpack.packb(probe_run_props(TRACE_A, 100)),
                },
            ),
            op(
                1,
                "AddNode",
                {
                    "node_id": f"harness-probe-run:{TRACE_B}",
                    "properties_msgpack": msgpack.packb(probe_run_props(TRACE_B, 101)),
                },
            ),
            op(
                2,
                "AddNode",
                {
                    "node_id": contrast_id,
                    "properties_msgpack": msgpack.packb(contrast_props(102)),
                },
            ),
            op(
                3,
                "AddNode",
                {
                    "node_id": adoption_id,
                    "properties_msgpack": msgpack.packb(
                        adoption_props([version_ref], contrast_id, 103)
                    ),
                },
            ),
            op(
                4,
                "AddNode",
                {
                    "node_id": lesson_id,
                    "properties_msgpack": msgpack.packb(
                        lesson_props(
                            situation,
                            what_changed,
                            applicability,
                            confidence,
                            evidence,
                            104,
                        )
                    ),
                },
            ),
        ],
    )
    results = clean_graph.changes.apply_batch([envelope])
    assert [result["status"] for result in results] == ["applied"], results

    # The query by deficiency reads the lesson back through its own
    # identity — the deficiency is part of that identity, and the stored
    # record carries the writer's fields and its transaction stamp.
    props = clean_graph.nodes.properties(lesson_id)
    assert props is not None, (
        "the lesson is stored under its deficiency-derived identity"
    )
    assert props["deficiency"] == DEFICIENCY
    assert props["task_class"] == TASK_CLASS
    assert props["what_changed"] == what_changed
    assert props["changed_component_kind"] == "system_prompt"
    assert props["applicability"] == applicability
    assert props["confidence"] == confidence
    assert props["evidence_run_ids"] == evidence
    assert props["tx_from"] == 104
    assert props["_harness_lesson"] is True

    # The contrast and the adoption read back with their fields; the
    # adoption's content address recomputes from its version refs.
    contrast = clean_graph.nodes.properties(contrast_id)
    assert contrast is not None
    assert contrast["run_a_trace_id"] == TRACE_A
    assert contrast["run_b_trace_id"] == TRACE_B
    assert contrast["conditions_digest"] == CONDITIONS_DIGEST
    assert contrast["retry_delta"] == -1
    adoption = clean_graph.nodes.properties(adoption_id)
    assert adoption is not None
    assert adoption["component_version_refs"] == [version_ref]
    assert adoption["decision_ref"] == "decision:served:h2-r802"
    assert adoption["contrast_ref"] == contrast_id

    # R802.5 negative one: a lesson without evidence runs refuses, and
    # nothing is stored.
    no_evidence_props = lesson_props("no evidence", "a change", [], 0.5, [], 200)
    no_evidence_id = lesson_node_id("no evidence", "a change", [], 0.5, [])
    try:
        _single_op_apply(
            clean_graph,
            graph,
            no_evidence_id,
            "h2-r802:no-evidence",
            2,
            no_evidence_props,
        )
    except EngineResponseError as error:
        assert error.code == "LessonWithoutEvidence", error.code
    else:
        raise AssertionError("a lesson without evidence runs must refuse")
    assert clean_graph.nodes.properties(no_evidence_id) is None

    # R802.5 negative two: a contrast whose conditions digest differs from
    # its runs refuses, and nothing is stored.
    mismatch_id = contrast_node_id("sha256:other-conditions", TRACE_A, TRACE_B)
    mismatch_props = contrast_props(201)
    mismatch_props["conditions_digest"] = "sha256:other-conditions"
    try:
        _single_op_apply(
            clean_graph, graph, mismatch_id, "h2-r802:mismatch", 3, mismatch_props
        )
    except EngineResponseError as error:
        assert error.code == "ContrastConditionsMismatch", error.code
    else:
        raise AssertionError("a contrast disagreeing with its runs must refuse")
    assert clean_graph.nodes.properties(mismatch_id) is None

    # R802.5 negative three: an adopted harness without a decision reference
    # refuses, and nothing is stored.
    no_decision_ref = (
        "harness-component:comp:served-memory:"
        "sha256:2447aaa8f76dcc91df6588294cb693e9c5164c97e460587b798111dfb88f7ac8"
    )
    no_decision_id = adoption_node_id([no_decision_ref])
    no_decision_props = adoption_props([no_decision_ref], contrast_id, 202)
    no_decision_props["decision_ref"] = ""
    try:
        _single_op_apply(
            clean_graph,
            graph,
            no_decision_id,
            "h2-r802:no-decision",
            4,
            no_decision_props,
        )
    except EngineResponseError as error:
        assert error.code == "AdoptedHarnessNoDecision", error.code
    else:
        raise AssertionError("an adoption without a decision reference must refuse")
    assert clean_graph.nodes.properties(no_decision_id) is None
