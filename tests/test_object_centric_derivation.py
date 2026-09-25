"""Incremental object-centric derivation proofs.

CONCEPT:AU-KG.mining.incremental-object-centric-derivation
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from epistemic_graph.ingestion.object_centric_derivation import (
    IncrementalObjectCentricDeriver,
    ObjectTimeline,
    Watermark,
)
from epistemic_graph.ingestion.semantic_event_model import (
    EventObjectParticipation,
    ProcessEvent,
    TemporalAttributeValue,
)


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def _event(
    event_id: str, activity: str, at: str, object_id: str = "o1"
) -> ProcessEvent:
    return ProcessEvent(
        event_id=event_id,
        activity=activity,
        occurred_at=_dt(at),
        objects=(EventObjectParticipation(object_id=object_id, object_type="Order"),),
        source_ref="src:1",
    )


# ── Watermark ──────────────────────────────────────────────────────────────
def test_watermark_advances_and_flags_lateness_outside_the_grace_window() -> None:
    watermark = Watermark(allowed_lateness=timedelta(minutes=5))
    assert watermark.observe(_dt("2026-01-01T00:10:00")) is False
    assert watermark.current == _dt("2026-01-01T00:05:00")
    # Within the grace window: not late.
    assert watermark.observe(_dt("2026-01-01T00:06:00")) is False
    # Older than max_seen - allowed_lateness: late.
    assert watermark.observe(_dt("2026-01-01T00:00:00")) is True
    # The watermark itself does not regress on a late/out-of-order arrival.
    assert watermark.current == _dt("2026-01-01T00:05:00")


def test_watermark_rejects_a_negative_lateness_bound() -> None:
    with pytest.raises(ValueError, match="negative"):
        Watermark(allowed_lateness=timedelta(seconds=-1))


# ── ObjectTimeline ───────────────────────────────────────────────────────────
def test_object_timeline_keeps_events_ordered_by_time_then_tiebreaker_then_id() -> None:
    timeline = ObjectTimeline()
    timeline.insert(_event("e3", "ship", "2026-01-03T00:00:00"))
    timeline.insert(_event("e1", "create", "2026-01-01T00:00:00"))
    predecessor, successor = timeline.insert(
        _event("e2", "approve", "2026-01-02T00:00:00")
    )

    assert [event.event_id for event in timeline] == ["e1", "e2", "e3"]
    assert predecessor is not None and predecessor.event_id == "e1"
    assert successor is not None and successor.event_id == "e3"


def test_object_timeline_remove_reports_its_old_neighbors() -> None:
    timeline = ObjectTimeline()
    for event in (
        _event("e1", "create", "2026-01-01T00:00:00"),
        _event("e2", "approve", "2026-01-02T00:00:00"),
        _event("e3", "ship", "2026-01-03T00:00:00"),
    ):
        timeline.insert(event)

    removed, predecessor, successor = timeline.remove("e2")
    assert removed.event_id == "e2"
    assert predecessor is not None and predecessor.event_id == "e1"
    assert successor is not None and successor.event_id == "e3"
    assert [event.event_id for event in timeline] == ["e1", "e3"]


def test_object_timeline_remove_of_unknown_event_raises() -> None:
    with pytest.raises(KeyError):
        ObjectTimeline().remove("missing")


# ── bounded directly-follows update ─────────────────────────────────────────
def test_in_order_arrivals_only_add_the_one_new_adjacent_edge() -> None:
    deriver = IncrementalObjectCentricDeriver()
    first = deriver.ingest_event(
        _event("e1", "create", "2026-01-01T00:00:00"),
        object_id="o1",
        state_id="s1",
        observed_at=_dt("2026-01-01T00:00:01"),
    )
    assert first.dfg_delta.removed == ()
    assert first.dfg_delta.added == ()  # no predecessor yet

    second = deriver.ingest_event(
        _event("e2", "ship", "2026-01-02T00:00:00"),
        object_id="o1",
        state_id="s2",
        observed_at=_dt("2026-01-02T00:00:01"),
    )
    assert second.dfg_delta.removed == ()
    assert second.dfg_delta.added == (("create", "ship"),)
    assert deriver.dfg_snapshot() == {("create", "ship"): 1}


def test_a_late_insertion_only_touches_the_one_split_segment() -> None:
    """The core 6.4 bounded-update proof: inserting an event BETWEEN two
    already-known events for the same object updates exactly the one
    predecessor/successor pair it splits — never a full DFG rebuild."""
    deriver = IncrementalObjectCentricDeriver()
    deriver.ingest_event(
        _event("e1", "create", "2026-01-01T00:00:00"),
        object_id="o1",
        state_id="s1",
        observed_at=_dt("2026-01-01T00:00:01"),
    )
    deriver.ingest_event(
        _event("e3", "ship", "2026-01-03T00:00:00"),
        object_id="o1",
        state_id="s3",
        observed_at=_dt("2026-01-03T00:00:01"),
    )
    assert deriver.dfg_snapshot() == {("create", "ship"): 1}

    delta = deriver.ingest_event(
        _event("e2", "approve", "2026-01-02T00:00:00"),
        object_id="o1",
        state_id="s2",
        observed_at=_dt("2026-01-02T00:00:01"),
    )

    assert delta.dfg_delta.removed == (("create", "ship"),)
    assert set(delta.dfg_delta.added) == {("create", "approve"), ("approve", "ship")}
    assert deriver.dfg_snapshot() == {
        ("create", "approve"): 1,
        ("approve", "ship"): 1,
    }
    assert [event.event_id for event in deriver.timeline("o1")] == ["e1", "e2", "e3"]


def test_independent_objects_contribute_independent_edges() -> None:
    deriver = IncrementalObjectCentricDeriver()
    for object_id in ("o1", "o2"):
        deriver.ingest_event(
            _event(f"{object_id}-e1", "create", "2026-01-01T00:00:00", object_id),
            object_id=object_id,
            state_id=f"{object_id}-s1",
            observed_at=_dt("2026-01-01T00:00:01"),
        )
        deriver.ingest_event(
            _event(f"{object_id}-e2", "ship", "2026-01-02T00:00:00", object_id),
            object_id=object_id,
            state_id=f"{object_id}-s2",
            observed_at=_dt("2026-01-02T00:00:01"),
        )
    # Two independent objects both contributing (create, ship) sum into one
    # aggregate edge count of 2 — a per-object-local update, aggregated.
    assert deriver.dfg_snapshot() == {("create", "ship"): 2}


# ── ObjectState materialization, never inventing values ────────────────────
def test_object_state_omits_attributes_with_no_known_revision_yet() -> None:
    deriver = IncrementalObjectCentricDeriver()
    delta = deriver.ingest_event(
        _event("e1", "create", "2026-01-01T00:00:00"),
        object_id="o1",
        state_id="s1",
        observed_at=_dt("2026-01-01T00:00:01"),
    )
    (state,) = delta.revised_states
    assert state.attributes == ()  # nothing invented for an unobserved attribute


def test_object_state_uses_the_most_recent_known_value_as_of_the_event() -> None:
    deriver = IncrementalObjectCentricDeriver()
    deriver.observe_object_attributes(
        "o1",
        [
            TemporalAttributeValue(
                name="status", value="new", valid_from=_dt("2026-01-01T00:00:00")
            ),
            TemporalAttributeValue(
                name="status", value="approved", valid_from=_dt("2026-01-02T00:00:00")
            ),
        ],
    )
    early = deriver.ingest_event(
        _event("e1", "create", "2026-01-01T12:00:00"),
        object_id="o1",
        state_id="s1",
        observed_at=_dt("2026-01-01T12:00:01"),
    )
    late = deriver.ingest_event(
        _event("e2", "ship", "2026-01-03T00:00:00"),
        object_id="o1",
        state_id="s2",
        observed_at=_dt("2026-01-03T00:00:01"),
    )
    assert [a.value for a in early.revised_states[0].attributes] == ["new"]
    assert [a.value for a in late.revised_states[-1].attributes] == ["approved"]


def test_a_correction_revises_only_the_bounded_suffix_it_could_affect() -> None:
    """A correction to an early event must not touch states materialized for
    events strictly BEFORE it in the same object's timeline."""
    deriver = IncrementalObjectCentricDeriver(allowed_lateness=timedelta(minutes=5))
    deriver.observe_object_attributes(
        "o1",
        [
            TemporalAttributeValue(
                name="status", value="new", valid_from=_dt("2026-01-01T00:00:00")
            )
        ],
    )
    deriver.ingest_event(
        _event("e1", "create", "2026-01-01T00:00:00"),
        object_id="o1",
        state_id="s1",
        observed_at=_dt("2026-01-01T00:00:01"),
    )
    deriver.ingest_event(
        _event("e2", "approve", "2026-01-02T00:00:00"),
        object_id="o1",
        state_id="s2",
        observed_at=_dt("2026-01-02T00:00:01"),
    )
    deriver.ingest_event(
        _event("e3", "ship", "2026-01-03T00:00:00"),
        object_id="o1",
        state_id="s3",
        observed_at=_dt("2026-01-03T00:00:01"),
    )

    delta = deriver.correct_event(
        _event("e2", "approve", "2026-01-02T00:00:00"),
        object_id="o1",
        state_id="s2-corrected",
        observed_at=_dt("2026-01-02T00:10:00"),
    )

    # Only e2 and its successor e3 are in the revised suffix — e1 is untouched.
    revised_event_valid_froms = {state.valid_from for state in delta.revised_states}
    assert revised_event_valid_froms == {
        _dt("2026-01-02T00:00:00"),
        _dt("2026-01-03T00:00:00"),
    }
    assert _dt("2026-01-01T00:00:00") not in revised_event_valid_froms


# ── watermark-driven derivation generation ──────────────────────────────────
def test_generation_only_advances_on_a_late_arrival_or_a_correction() -> None:
    deriver = IncrementalObjectCentricDeriver(allowed_lateness=timedelta(minutes=5))
    assert deriver.generation == 0
    deriver.ingest_event(
        _event("e1", "create", "2026-01-01T00:00:00"),
        object_id="o1",
        state_id="s1",
        observed_at=_dt("2026-01-01T00:00:01"),
    )
    assert deriver.generation == 0  # first-ever arrival is never "late"
    deriver.ingest_event(
        _event("e2", "ship", "2026-01-02T00:00:00"),
        object_id="o1",
        state_id="s2",
        observed_at=_dt("2026-01-02T00:00:01"),
    )
    assert deriver.generation == 0  # ordinary in-order arrival

    late = deriver.ingest_event(
        _event("e0", "register", "2025-12-31T00:00:00"),
        object_id="o1",
        state_id="s0",
        observed_at=_dt("2026-01-02T00:00:02"),
    )
    assert late.is_correction is True
    assert deriver.generation == 1


def test_correction_bumps_generation_exactly_once_even_when_also_late() -> None:
    deriver = IncrementalObjectCentricDeriver(allowed_lateness=timedelta(minutes=5))
    deriver.ingest_event(
        _event("e1", "create", "2026-01-01T00:00:00"),
        object_id="o1",
        state_id="s1",
        observed_at=_dt("2026-01-01T00:00:01"),
    )
    deriver.ingest_event(
        _event("e3", "ship", "2026-01-03T00:00:00"),
        object_id="o1",
        state_id="s3",
        observed_at=_dt("2026-01-03T00:00:01"),
    )
    # This insertion is BOTH "late" per the watermark AND a correction path
    # (well inside a subsequent correct_event call) — generation must not
    # double count within one logical correction.
    deriver.ingest_event(
        _event("e2", "approve", "2026-01-02T00:00:00"),
        object_id="o1",
        state_id="s2",
        observed_at=_dt("2026-01-02T00:00:01"),
    )
    generation_before = deriver.generation
    delta = deriver.correct_event(
        _event("e2", "approve", "2026-01-02T00:00:00"),
        object_id="o1",
        state_id="s2-again",
        observed_at=_dt("2026-01-02T00:10:00"),
    )
    assert deriver.generation == generation_before + 1
    assert delta.generation == deriver.generation
    assert delta.is_correction is True


# ── replay determinism (item 5) ─────────────────────────────────────────────
def test_replaying_events_in_two_arrival_orders_converges_to_same_state() -> None:
    """Whether events for one object arrive strictly in order or with a late
    insertion, the FINAL aggregate DFG and final object state converge to the
    same result — replay determinism independent of arrival order."""
    events = [
        _event("e1", "create", "2026-01-01T00:00:00"),
        _event("e2", "approve", "2026-01-02T00:00:00"),
        _event("e3", "ship", "2026-01-03T00:00:00"),
    ]
    attribute_history = [
        TemporalAttributeValue(
            name="status", value="new", valid_from=_dt("2026-01-01T00:00:00")
        ),
        TemporalAttributeValue(
            name="status", value="approved", valid_from=_dt("2026-01-02T00:00:00")
        ),
        TemporalAttributeValue(
            name="status", value="shipped", valid_from=_dt("2026-01-03T00:00:00")
        ),
    ]

    in_order = IncrementalObjectCentricDeriver()
    in_order.observe_object_attributes("o1", attribute_history)
    for index, event in enumerate(events):
        in_order.ingest_event(
            event, object_id="o1", state_id=f"s{index}", observed_at=event.occurred_at
        )

    out_of_order = IncrementalObjectCentricDeriver()
    out_of_order.observe_object_attributes("o1", attribute_history)
    for index, event in enumerate((events[0], events[2], events[1])):
        out_of_order.ingest_event(
            event, object_id="o1", state_id=f"s{index}", observed_at=event.occurred_at
        )

    assert in_order.dfg_snapshot() == out_of_order.dfg_snapshot()
    assert [event.event_id for event in in_order.timeline("o1")] == [
        event.event_id for event in out_of_order.timeline("o1")
    ]
    final_state_in_order = in_order.object_state_as_of(
        "o1",
        _dt("2026-01-03T00:00:00"),
        state_id="final",
        observed_at=_dt("2026-01-03T00:00:01"),
    )
    final_state_out_of_order = out_of_order.object_state_as_of(
        "o1",
        _dt("2026-01-03T00:00:00"),
        state_id="final",
        observed_at=_dt("2026-01-03T00:00:01"),
    )
    assert final_state_in_order.attributes == final_state_out_of_order.attributes
