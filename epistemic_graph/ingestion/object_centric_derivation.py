"""Incremental, per-object directly-follows + object-state derivation.

CONCEPT:AU-KG.mining.incremental-object-centric-derivation — object-centric
process mining (`event_log_adapter.py`) and OCEL exchange (`ocel_adapter.py`)
both operate on a COMPLETE, static batch of events. Real event logs arrive
incrementally and out of order (a late-arriving event, a correction to an
already-ingested one). This module maintains derived state — a per-object
ordered event index, the aggregate directly-follows graph (DFG) those indexes
imply, and each object's ``ObjectState`` timeline — WITHOUT re-deriving it
from scratch on every arrival:

* **Ordered per-object event index** — each object's participating events are
  kept sorted by ``(occurred_at, sequence_tiebreaker, event_id)`` (the same
  total order :mod:`event_log_adapter` uses for its static projection), so an
  arrival's predecessor/successor within THAT object's timeline is a single
  bounded lookup, never a full-log rescan.
* **Bounded directly-follows update** — an arriving event only ever splits (or
  a removed event only ever merges) ONE adjacent pair in ONE object's
  timeline. The aggregate DFG update touches exactly the edges that pair
  contributed: remove the old ``(predecessor, successor)`` edge (if any),
  add ``(predecessor, new_event)`` and ``(new_event, successor)`` — an O(1)
  edit to the aggregate counts, not an O(n) DFG rebuild.
* **``ObjectState`` as-of an event, never invented.** An object's state at an
  event is the most recently known value (``valid_from <= occurred_at``) of
  each attribute that has EVER been observed for that object. An attribute
  with no known value yet is simply absent from the materialized state — it
  is never defaulted, guessed, or interpolated.
* **Watermark + derivation generation.** A per-source :class:`Watermark`
  tracks the latest ``occurred_at`` seen minus an allowed-lateness bound; an
  event older than the watermark is a correction to already-derived state, not
  a fresh arrival, and bumps the deriver's :attr:`generation` counter — so a
  reproducible run can be pinned to "derivation as of generation N" (feeding
  :mod:`process_conformance`'s frozen data/model boundary).

Pure Python, no engine/graph-transport dependency — this module derives over
:class:`~.semantic_event_model.ProcessEvent`/``TemporalAttributeValue`` values
already resident in an :class:`~.semantic_event_model.ObjectCentricGraphSlice`,
so it composes with both a static OCEL import and a live per-event stream.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta

from .semantic_event_model import (
    EventAttributeValue,
    ObjectState,
    ProcessEvent,
    TemporalAttributeValue,
)

__all__ = [
    "DerivationDelta",
    "DirectlyFollowsDelta",
    "IncrementalObjectCentricDeriver",
    "ObjectTimeline",
    "Watermark",
]


def _event_key(event: ProcessEvent) -> tuple[datetime, str, str]:
    """The total order every per-object timeline and the static projection share."""
    return (event.occurred_at, event.sequence_tiebreaker, event.event_id)


@dataclass(frozen=True, slots=True)
class DirectlyFollowsDelta:
    """One bounded edit to the aggregate directly-follows graph's edge counts.

    ``removed``/``added`` are ``(activity_a, activity_b)`` pairs with their
    signed count change — always at most one removed edge (the split
    predecessor->successor pair) and at most two added edges
    (predecessor->event, event->successor), or the mirror on removal.
    """

    removed: tuple[tuple[str, str], ...] = ()
    added: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class DerivationDelta:
    """What one ``ingest_event``/``correct_event`` call actually changed.

    Bounded by construction: ``dfg_delta`` never touches more than the one
    adjacent pair the arriving/removed event sits between, and
    ``revised_states`` only ever contains the states for events at or after
    the arrival point in ONE object's timeline (never the whole history).
    """

    object_id: str
    event_id: str
    generation: int
    is_correction: bool
    dfg_delta: DirectlyFollowsDelta
    revised_states: tuple[ObjectState, ...]


class Watermark:
    """Per-object-timeline bounded-lateness tracker.

    CONCEPT:AU-KG.mining.incremental-object-centric-derivation.

    ``allowed_lateness`` is the grace window: an event older than
    ``max_seen - allowed_lateness`` at the time it arrives is a LATE
    correction to already-materialized derived state (it bumps the deriver's
    generation), not an ordinary in-order arrival.
    """

    __slots__ = ("allowed_lateness", "max_seen")

    def __init__(self, allowed_lateness: timedelta = timedelta(0)) -> None:
        if allowed_lateness < timedelta(0):
            raise ValueError("allowed_lateness must not be negative")
        self.allowed_lateness = allowed_lateness
        self.max_seen: datetime | None = None

    def observe(self, occurred_at: datetime) -> bool:
        """Advance the watermark; return whether ``occurred_at`` arrived late."""
        late = self.max_seen is not None and (
            occurred_at < self.max_seen - self.allowed_lateness
        )
        if self.max_seen is None or occurred_at > self.max_seen:
            self.max_seen = occurred_at
        return late

    @property
    def current(self) -> datetime | None:
        """The watermark itself: ``max_seen - allowed_lateness``, or ``None``."""
        if self.max_seen is None:
            return None
        return self.max_seen - self.allowed_lateness


class ObjectTimeline:
    """Ordered event index for one object.

    Keyed by ``(occurred_at, tiebreaker, event_id)``.

    D-61-3 (partial): ``position_of`` now resolves via a maintained
    ``event_id -> key`` dict + ``bisect`` (O(log n)) instead of the previous
    O(n) linear scan over every event — the lookup ``remove``/``events_from``
    both depend on. ``insert``/``remove`` still mutate a plain ``list``
    (``list.insert``/``del`` remain an O(n) shift): a genuine O(log n) update
    too needs an indexed structure (e.g. a skip-list/B-tree) this module
    deliberately does NOT add as a new dependency here — see
    ``reports/deferred/lane-wave6-followup.md`` D-W6F-2 for why (adding one
    requires relocking the SHARED multi-worktree workspace lock, out of scope
    for a same-session drive-by). The BOUNDED-UPDATE property (an arrival only
    ever touches the one adjacent DFG edge pair) is unaffected either way —
    that bound was always in the update algorithm's blast radius, not the
    index's raw complexity.
    """

    __slots__ = ("_events", "_keys", "_key_by_id")

    def __init__(self) -> None:
        self._keys: list[tuple[datetime, str, str]] = []
        self._events: list[ProcessEvent] = []
        self._key_by_id: dict[str, tuple[datetime, str, str]] = {}

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self) -> Iterator[ProcessEvent]:
        return iter(self._events)

    def position_of(self, event_id: str) -> int | None:
        key = self._key_by_id.get(event_id)
        if key is None:
            return None
        # ``key`` includes ``event_id`` as its final tiebreaker component, so
        # it is unique — the bisected index is exact, not merely a candidate.
        return bisect.bisect_left(self._keys, key)

    def neighbors_of_key(
        self, key: tuple[datetime, str, str]
    ) -> tuple[ProcessEvent | None, ProcessEvent | None]:
        """The (predecessor, successor) that WOULD surround ``key`` if inserted."""
        index = bisect.bisect_left(self._keys, key)
        predecessor = self._events[index - 1] if index > 0 else None
        successor = self._events[index] if index < len(self._events) else None
        return predecessor, successor

    def insert(
        self, event: ProcessEvent
    ) -> tuple[ProcessEvent | None, ProcessEvent | None]:
        """Insert ``event`` in order; return its new (predecessor, successor)."""
        key = _event_key(event)
        index = bisect.bisect_left(self._keys, key)
        predecessor = self._events[index - 1] if index > 0 else None
        successor = self._events[index] if index < len(self._events) else None
        self._keys.insert(index, key)
        self._events.insert(index, event)
        self._key_by_id[event.event_id] = key
        return predecessor, successor

    def remove(
        self, event_id: str
    ) -> tuple[ProcessEvent, ProcessEvent | None, ProcessEvent | None]:
        """Remove ``event_id`` and return its old neighboring events."""
        index = self.position_of(event_id)
        if index is None:
            raise KeyError(f"object timeline has no event {event_id!r}")
        removed = self._events[index]
        predecessor = self._events[index - 1] if index > 0 else None
        successor = self._events[index + 1] if index + 1 < len(self._events) else None
        del self._events[index]
        del self._keys[index]
        del self._key_by_id[event_id]
        return removed, predecessor, successor

    def events_from(self, event_id: str) -> tuple[ProcessEvent, ...]:
        """Every event at or after ``event_id`` in this object's order.

        Used to bound ``ObjectState`` re-materialization to exactly the
        suffix a correction could have affected — never the whole timeline.
        """
        index = self.position_of(event_id)
        if index is None:
            raise KeyError(f"object timeline has no event {event_id!r}")
        return tuple(self._events[index:])

    def as_tuple(self) -> tuple[ProcessEvent, ...]:
        return tuple(self._events)


def _known_attributes_as_of(
    attribute_history: Iterable[TemporalAttributeValue],
    *,
    as_of: datetime,
) -> tuple[EventAttributeValue, ...]:
    """Most-recent-known value per attribute name at ``as_of``, never invented.

    An attribute with no recorded :class:`TemporalAttributeValue` at or before
    ``as_of`` is simply absent from the result — there is no default/None
    placeholder standing in for an unknown value.
    """
    latest: dict[str, TemporalAttributeValue] = {}
    for revision in attribute_history:
        if revision.valid_from > as_of:
            continue
        current = latest.get(revision.name)
        if current is None or revision.valid_from >= current.valid_from:
            latest[revision.name] = revision
    return tuple(
        EventAttributeValue(name=name, value=revision.value)
        for name, revision in sorted(latest.items())
    )


@dataclass
class IncrementalObjectCentricDeriver:
    """Maintain per-object timelines, the aggregate DFG, and object states.

    ``attribute_history`` is the caller-owned, append-only source of known
    object-attribute revisions (e.g. accumulated from
    :class:`~.semantic_event_model.BusinessObject.attributes` across arriving
    OCEL batches) — this deriver reads it to materialize ``ObjectState``
    without inventing values it never received; it does not itself decide
    what an attribute's value is.
    """

    allowed_lateness: timedelta = timedelta(0)
    generation: int = 0
    _timelines: dict[str, ObjectTimeline] = field(default_factory=dict)
    _watermarks: dict[str, Watermark] = field(default_factory=dict)
    _dfg_counts: dict[tuple[str, str], int] = field(default_factory=dict)
    _attribute_history: dict[str, list[TemporalAttributeValue]] = field(
        default_factory=dict
    )
    _known_events: dict[str, tuple[str, ProcessEvent]] = field(default_factory=dict)
    #: D-61-3: ``object_id -> object_type`` side index, populated once per
    #: newly-observed participation in ``ingest_event`` — replaces the O(total
    #: known events) scan ``_object_type_of`` used to do over every
    #: ``_known_events`` entry's participations on every lookup. First-known
    #: type wins (matches the scan's own "first match in iteration order"
    #: behavior it replaces).
    _object_types: dict[str, str] = field(default_factory=dict)

    def observe_object_attributes(
        self, object_id: str, attributes: Iterable[TemporalAttributeValue]
    ) -> None:
        """Register known attribute revisions for ``object_id`` (append-only)."""
        self._attribute_history.setdefault(object_id, []).extend(attributes)

    def dfg_snapshot(self) -> dict[tuple[str, str], int]:
        """The current aggregate directly-follows edge counts."""
        return dict(self._dfg_counts)

    def timeline(self, object_id: str) -> tuple[ProcessEvent, ...]:
        existing = self._timelines.get(object_id)
        return existing.as_tuple() if existing is not None else ()

    def object_state_as_of(
        self, object_id: str, at: datetime, *, state_id: str, observed_at: datetime
    ) -> ObjectState:
        """Materialize ``object_id``'s state at ``at`` from ONLY known revisions."""
        history = self._attribute_history.get(object_id, ())
        object_type = self._object_type_of(object_id)
        return ObjectState(
            state_id=state_id,
            object_id=object_id,
            object_type=object_type,
            valid_from=at,
            observed_at=observed_at,
            attributes=_known_attributes_as_of(history, as_of=at),
        )

    def _object_type_of(self, object_id: str) -> str:
        object_type = self._object_types.get(object_id)
        if object_type is None:
            raise KeyError(f"no known event participation for object {object_id!r}")
        return object_type

    @staticmethod
    def _pair(a: str | None, b: str | None) -> tuple[str, str] | None:
        if a is None or b is None:
            return None
        return (a, b)

    def _apply_edge(self, pair: tuple[str, str], *, sign: int) -> None:
        updated = self._dfg_counts.get(pair, 0) + sign
        if updated <= 0:
            self._dfg_counts.pop(pair, None)
        else:
            self._dfg_counts[pair] = updated

    def ingest_event(
        self,
        event: ProcessEvent,
        *,
        object_id: str,
        state_id: str,
        observed_at: datetime,
    ) -> DerivationDelta:
        """Insert one (event, object) participation.

        Update only the affected DFG segment.

        Returns the bounded delta: the (at most one) split predecessor/
        successor edge removed, the (at most two) new edges added, and the
        ``ObjectState``s revised — exactly the suffix of ``object_id``'s
        timeline from this event onward, since only THOSE states could have
        changed (every earlier state's "most recent known value as-of" is
        unaffected by an insertion after it).
        """
        is_late = self._watermarks.setdefault(
            object_id, Watermark(self.allowed_lateness)
        ).observe(event.occurred_at)
        if is_late:
            self.generation += 1

        timeline = self._timelines.setdefault(object_id, ObjectTimeline())
        old_predecessor, old_successor = timeline.neighbors_of_key(_event_key(event))
        removed_edge = self._pair(
            old_predecessor.activity if old_predecessor else None,
            old_successor.activity if old_successor else None,
        )
        timeline.insert(event)
        self._known_events[event.event_id] = (object_id, event)
        for participation in event.objects:
            self._object_types.setdefault(
                participation.object_id, participation.object_type
            )

        added_edges = tuple(
            pair
            for pair in (
                self._pair(
                    old_predecessor.activity if old_predecessor else None,
                    event.activity,
                ),
                self._pair(
                    event.activity,
                    old_successor.activity if old_successor else None,
                ),
            )
            if pair is not None
        )
        if removed_edge is not None:
            self._apply_edge(removed_edge, sign=-1)
        for pair in added_edges:
            self._apply_edge(pair, sign=1)

        revised_states = self._materialize_from(
            timeline, event.event_id, state_id=state_id, observed_at=observed_at
        )
        return DerivationDelta(
            object_id=object_id,
            event_id=event.event_id,
            generation=self.generation,
            is_correction=is_late,
            dfg_delta=DirectlyFollowsDelta(
                removed=(removed_edge,) if removed_edge is not None else (),
                added=added_edges,
            ),
            revised_states=revised_states,
        )

    def correct_event(
        self,
        event: ProcessEvent,
        *,
        object_id: str,
        state_id: str,
        observed_at: datetime,
    ) -> DerivationDelta:
        """Remove ``event.event_id`` if already known, then re-insert ``event``.

        A correction ALWAYS bumps :attr:`generation` — even one that lands
        back in the same position, or one whose ``occurred_at`` still sits
        after the current watermark (so :meth:`ingest_event`'s own lateness
        check would not itself have flagged it) — so a conformance run pinned
        to a prior generation stays reproducible against exactly the derived
        state that existed before the correction. When the watermark check
        DID already flag it as late, this does not double-count.
        """
        timeline = self._timelines.get(object_id)
        was_known = False
        if timeline is not None and timeline.position_of(event.event_id) is not None:
            was_known = True
            timeline.remove(event.event_id)
            self._known_events.pop(event.event_id, None)
        delta = self.ingest_event(
            event, object_id=object_id, state_id=state_id, observed_at=observed_at
        )
        if was_known and not delta.is_correction:
            self.generation += 1
            delta = replace(delta, generation=self.generation, is_correction=True)
        return delta

    def _materialize_from(
        self,
        timeline: ObjectTimeline,
        from_event_id: str,
        *,
        state_id: str,
        observed_at: datetime,
    ) -> tuple[ObjectState, ...]:
        object_id = self._known_events[from_event_id][0]
        history = self._attribute_history.get(object_id, ())
        states: list[ObjectState] = []
        for offset, affected in enumerate(timeline.events_from(from_event_id)):
            object_type = next(
                (
                    participation.object_type
                    for participation in affected.objects
                    if participation.object_id == object_id
                ),
                "",
            )
            states.append(
                ObjectState(
                    state_id=f"{state_id}:{offset}",
                    object_id=object_id,
                    object_type=object_type or self._object_type_of(object_id),
                    valid_from=affected.occurred_at,
                    observed_at=observed_at,
                    attributes=_known_attributes_as_of(
                        history, as_of=affected.occurred_at
                    ),
                )
            )
        return tuple(states)
