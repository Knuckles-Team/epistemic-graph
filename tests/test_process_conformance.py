"""Discovery and conformance separation proofs.

CONCEPT:AU-KG.mining.process-conformance-checking.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from epistemic_graph.ingestion.process_conformance import (
    ConformanceRun,
    Deviation,
    check_directly_follows_conformance,
    conformance_run_graph_slice,
    run_conformance_check,
)
from epistemic_graph.ingestion.semantic_event_model import (
    ProcessPerspective,
)


def _perspective(**overrides: object) -> ProcessPerspective:
    fields = {
        "perspective_id": "case:order-view",
        "object_types": ("Order",),
        "derivation_version": "v1",
    }
    fields.update(overrides)
    return ProcessPerspective(**fields)  # type: ignore[arg-type]


def _run(**overrides: object) -> ConformanceRun:
    fields: dict[str, object] = {
        "run_id": "run-1",
        "perspective": _perspective(),
        "graph_as_of": datetime(2026, 1, 1, tzinfo=UTC),
        "mapping_version": "ocel-json-2.0",
        "model_ref": "process-model:checkout-flow",
        "export_digest": "a" * 64,
    }
    fields.update(overrides)
    return ConformanceRun(**fields)  # type: ignore[arg-type]


# ── ConformanceRun frozen boundary ──────────────────────────────────────────
def test_run_digest_is_stable_across_run_id_worker_and_created_at() -> None:
    """The reproducibility guarantee: only the FIVE frozen data/model fields
    determine the digest — run identity/worker/timestamp never do."""
    first = _run(run_id="run-a", worker="native-directly-follows")
    second = _run(
        run_id="run-b",
        worker="pm4py-alignment",
        created_at=datetime(2030, 6, 1, tzinfo=UTC),
    )
    assert first.run_digest() == second.run_digest()


@pytest.mark.parametrize(
    "overrides",
    [
        {"mapping_version": "ocel-json-2.1"},
        {"model_ref": "process-model:other-flow"},
        {"export_digest": "b" * 64},
        {"graph_as_of": datetime(2026, 6, 1, tzinfo=UTC)},
    ],
)
def test_run_digest_changes_when_any_frozen_dimension_changes(
    overrides: dict[str, object],
) -> None:
    baseline = _run()
    changed = _run(**overrides)
    assert baseline.run_digest() != changed.run_digest()


def test_run_digest_is_insensitive_to_perspective_declaration_order() -> None:
    a = _run(perspective=_perspective(object_types=("Order", "Item")))
    b = _run(perspective=_perspective(object_types=("Item", "Order")))
    assert a.run_digest() == b.run_digest()


def test_conformance_run_is_frozen() -> None:
    run = _run()
    with pytest.raises(ValidationError):
        run.run_id = "mutated"  # type: ignore[misc]


# ── Deviation ────────────────────────────────────────────────────────────────
def test_deviation_requires_observed_or_expected_activity() -> None:
    with pytest.raises(ValidationError, match="observed.*expected"):
        Deviation(
            deviation_id="deviation:1",
            object_id="o1",
            position=0,
            kind="unexpected_transition",
        )


# ── native directly-follows conformance worker ──────────────────────────────
def test_fully_conformant_traces_report_no_deviations() -> None:
    run = _run()
    allowed_edges = {("create", "approve"), ("approve", "ship")}
    deviations = check_directly_follows_conformance(
        [("create", "approve", "ship")], ["o1"], allowed_edges, run=run
    )
    assert deviations == ()


def test_unexpected_transition_is_reported_with_the_allowed_alternatives() -> None:
    run = _run()
    allowed_edges = {("create", "approve"), ("create", "cancel"), ("approve", "ship")}
    deviations = check_directly_follows_conformance(
        [("create", "reject")], ["o1"], allowed_edges, run=run
    )
    assert len(deviations) == 1
    deviation = deviations[0]
    assert deviation.kind == "unexpected_transition"
    assert deviation.object_id == "o1"
    assert deviation.position == 0
    assert deviation.observed_activity == "reject"
    assert deviation.expected_activities == ("approve", "cancel")


def test_unexpected_start_activity_is_reported_when_start_set_is_declared() -> None:
    run = _run()
    allowed_edges = {("create", "approve"), ("approve", "ship")}
    deviations = check_directly_follows_conformance(
        [("approve", "ship")],
        ["o1"],
        allowed_edges,
        run=run,
        start_activities={"create"},
    )
    kinds = {deviation.kind for deviation in deviations}
    assert kinds == {"unexpected_start"}


def test_start_end_conformance_is_not_checked_when_no_start_end_set_is_declared() -> (
    None
):
    """A directly-follows edge set alone cannot distinguish "illegal start"
    from "legal mid-process activity that opens this trace" — omitting the
    declared sets means start/end conformance is simply not asserted."""
    run = _run()
    allowed_edges = {("create", "approve"), ("approve", "ship")}
    deviations = check_directly_follows_conformance(
        [("approve", "ship")], ["o1"], allowed_edges, run=run
    )
    assert deviations == ()


def test_deviation_identity_is_deterministic_and_replay_stable() -> None:
    run = _run()
    allowed_edges = {("create", "approve")}
    first = check_directly_follows_conformance(
        [("create", "reject")], ["o1"], allowed_edges, run=run
    )
    second = check_directly_follows_conformance(
        [("create", "reject")], ["o1"], allowed_edges, run=run
    )
    assert first == second


def test_traces_and_object_ids_length_mismatch_is_refused() -> None:
    with pytest.raises(ValueError, match="same length"):
        check_directly_follows_conformance(
            [("create",), ("approve",)], ["o1"], set(), run=_run()
        )


# ── discovery/conformance separation + pluggable optional worker ───────────
def test_run_conformance_check_never_mutates_or_re_derives_the_run() -> None:
    """The run's model_ref/export_digest were fixed by its CALLER before this
    executes — conformance checking must never re-derive them from the
    traces it is given (that would silently turn conformance into discovery)."""
    run = _run()
    allowed_edges = {("create", "approve")}
    returned_run, _ = run_conformance_check(
        [("create", "approve")], ["o1"], allowed_edges, run=run
    )
    assert returned_run is run
    assert returned_run.model_ref == "process-model:checkout-flow"


def test_an_alternative_worker_can_be_swapped_in_without_a_new_dependency() -> None:
    """CONCEPT:AU-KG.mining.process-conformance-checking — PM4Py or any other
    analytics library is an OPTIONAL worker (this fake stands in for one) —
    the frozen run identity is untouched by which worker executed."""

    def _stub_alignment_worker(
        traces,
        object_ids,
        allowed_edges,
        *,
        run,
        start_activities=None,
        end_activities=None,
    ):
        return (
            Deviation(
                deviation_id="deviation:stub",
                object_id=object_ids[0],
                position=0,
                kind="unexpected_end",
                observed_activity="stub",
            ),
        )

    run = _run(worker="stub-alignment")
    returned_run, deviations = run_conformance_check(
        [("create",)], ["o1"], set(), run=run, worker=_stub_alignment_worker
    )
    assert returned_run is run
    assert deviations[0].deviation_id == "deviation:stub"


# ── graph writeback (D-61-1: querying past conformance runs/deviations) ───────
def test_conformance_run_graph_slice_shape_with_a_deviation() -> None:
    run = _run()
    allowed_edges = {("create", "approve")}
    _, deviations = run_conformance_check(
        [("create", "reject")], ["o1"], allowed_edges, run=run
    )
    entities, links = conformance_run_graph_slice(
        run, deviations, source_ref="src:test"
    )

    run_entities = [e for e in entities if e["node_type"] == "ConformanceRun"]
    deviation_entities = [e for e in entities if e["node_type"] == "Deviation"]
    assert len(run_entities) == 1
    assert len(deviation_entities) == 1
    assert run_entities[0]["model_ref"] == "process-model:checkout-flow"
    assert run_entities[0]["run_digest"] == run.run_digest()
    assert deviation_entities[0]["object_id"] == "o1"

    relationship_kinds = {link["relationship"] for link in links}
    assert relationship_kinds == {"CHECKED_UNDER_PERSPECTIVE", "HAS_DEVIATION"}
    run_id = run_entities[0]["id"]
    perspective_links = [
        link for link in links if link["relationship"] == "CHECKED_UNDER_PERSPECTIVE"
    ]
    assert perspective_links[0]["source"] == run_id
    deviation_links = [
        link for link in links if link["relationship"] == "HAS_DEVIATION"
    ]
    assert deviation_links[0] == {
        "source": run_id,
        "target": deviation_entities[0]["id"],
        "relationship": "HAS_DEVIATION",
    }


def test_conformance_graph_slice_uses_ocel_perspective_node_id() -> None:
    """The CHECKED_UNDER_PERSPECTIVE target id must be computed the SAME way
    ``ObjectCentricGraphSlice.to_graph_slice`` computes a ProcessPerspective
    node id — otherwise a conformance run committed after an OCEL commit under
    the same ``source_ref``/perspective would silently create a DUPLICATE,
    disconnected perspective node instead of joining the existing one."""
    from epistemic_graph.ingestion.semantic_event_model import (
        _stable_id,
    )

    run = _run()
    entities, links = conformance_run_graph_slice(run, (), source_ref="src:test")
    perspective_link = next(
        link for link in links if link["relationship"] == "CHECKED_UNDER_PERSPECTIVE"
    )
    expected_id = _stable_id(
        "process-perspective", "src:test", run.perspective.perspective_id
    )
    assert perspective_link["target"] == expected_id


def test_conformance_run_graph_slice_with_no_deviations_has_no_run_node() -> None:
    run = _run()
    entities, links = conformance_run_graph_slice(run, (), source_ref="src:test")
    assert {e["node_type"] for e in entities} == {"ConformanceRun"}
    assert {link["relationship"] for link in links} == {"CHECKED_UNDER_PERSPECTIVE"}
