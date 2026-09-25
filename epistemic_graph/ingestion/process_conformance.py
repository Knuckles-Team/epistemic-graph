"""Discovery and conformance, formally separated.

CONCEPT:AU-KG.mining.process-conformance-checking.

Process MINING (`event_log_adapter.py` / the engine's directly-follows +
alpha-lite footprint) DISCOVERS a model from a trace projection — it answers
"what does the observed process look like?". Process CONFORMANCE answers a
different question: "does THIS run's behavior fit a GIVEN model?" Conflating
the two is a common process-mining footgun: a "conformance score" that
silently re-discovers its own reference model from the same data it is
checking is not conformance checking at all — it can never find a deviation
by construction.

This module keeps that boundary explicit and structural:

* :class:`ConformanceRun` freezes the FIVE things a conformance result depends
  on — the declared :class:`~.semantic_event_model.ProcessPerspective` the
  traces were flattened under, the graph-as-of pointer the traces were read
  from, the OCEL/tEKG mapping version, an opaque reference to the MODEL being
  checked against (never re-derived from the same run), and the export digest
  of the source data the run executed over. Two runs with identical frozen
  fields are, by construction, reproducible to the same result.
* :class:`Deviation` is one typed, positioned mismatch between an observed
  trace and the reference model — never a bare "doesn't fit" boolean.
* :func:`check_directly_follows_conformance` is the NATIVE, dependency-free
  conformance worker (footprint/local-conformance: every observed adjacent
  activity pair must be an edge the reference model actually allows) and is
  the DEFAULT authority. A heavier analytics library (PM4Py, or any other) may
  be wired in as an alternative :class:`ConformanceWorker` — see that
  protocol's docstring — but is always an optional WORKER computing
  ``Deviation``s inside a run this module still owns the identity/reproducibility
  contract for; it is never itself the data authority for what a
  ``ConformanceRun``/``Deviation`` IS.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from pydantic import Field, model_validator

from .semantic_event_model import (
    ProcessPerspective,
    SemanticBoundaryModel,
    _stable_id,
)

DeviationKind = Literal["unexpected_transition", "unexpected_start", "unexpected_end"]

__all__ = [
    "ConformanceRun",
    "ConformanceWorker",
    "Deviation",
    "check_directly_follows_conformance",
    "conformance_run_graph_slice",
    "run_conformance_check",
]


class ConformanceRun(SemanticBoundaryModel):
    """The frozen data/model boundary that makes a conformance result reproducible.

    Every field is part of the run's IDENTITY (see :meth:`run_digest`) — two
    ``ConformanceRun``s with the same digest are, by construction, checking
    the same traces against the same model and must report the same
    deviations regardless of which :class:`ConformanceWorker` computed them.
    """

    run_id: str = Field(min_length=1)
    perspective: ProcessPerspective
    graph_as_of: datetime
    mapping_version: str = Field(min_length=1)
    model_ref: str = Field(min_length=1)
    export_digest: str = Field(min_length=1)
    worker: str = "native-directly-follows"
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def run_digest(self) -> str:
        """Deterministic identity over the frozen data/model boundary ONLY.

        Excludes ``run_id``/``worker``/``created_at`` deliberately: those
        identify THIS execution, not what it checked. Two runs against the
        same perspective/graph-as-of/mapping/model/export by different
        workers share the same digest — the reproducibility guarantee.
        """
        payload = {
            "perspective_id": self.perspective.perspective_id,
            "perspective_object_types": sorted(self.perspective.object_types),
            "perspective_derivation_version": self.perspective.derivation_version,
            "graph_as_of": self.graph_as_of.astimezone(UTC).isoformat(),
            "mapping_version": self.mapping_version,
            "model_ref": self.model_ref,
            "export_digest": self.export_digest,
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(blob).hexdigest()


class Deviation(SemanticBoundaryModel):
    """One typed, positioned mismatch between an observed trace and the model."""

    deviation_id: str = Field(min_length=1)
    object_id: str = Field(min_length=1)
    position: int = Field(ge=0)
    kind: DeviationKind
    observed_activity: str = ""
    expected_activities: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _require_observed_or_expected(self) -> Deviation:
        if not self.observed_activity and not self.expected_activities:
            raise ValueError(
                "a deviation must name what was observed, what was expected, or both"
            )
        return self


class ConformanceWorker(Protocol):
    """Structural interface an OPTIONAL analytics library (e.g. PM4Py) may satisfy.

    A worker computes ``Deviation``s for one already-frozen :class:`ConformanceRun`
    over already-projected ``traces``; it never mints the run itself and never
    supplies ``allowed_edges`` — the reference model stays whatever
    ``model_ref``/``allowed_edges`` the caller of :func:`run_conformance_check`
    declared. This keeps a third-party library a pluggable COMPUTE backend,
    never the authority over what was checked or against what.
    """

    def __call__(
        self,
        traces: Sequence[tuple[str, ...]],
        object_ids: Sequence[str],
        allowed_edges: frozenset[tuple[str, str]],
        *,
        run: ConformanceRun,
        start_activities: frozenset[str] | None = None,
        end_activities: frozenset[str] | None = None,
    ) -> tuple[Deviation, ...]: ...


def check_directly_follows_conformance(
    traces: Sequence[tuple[str, ...]],
    object_ids: Sequence[str],
    allowed_edges: Iterable[tuple[str, str]],
    *,
    run: ConformanceRun,
    start_activities: Iterable[str] | None = None,
    end_activities: Iterable[str] | None = None,
) -> tuple[Deviation, ...]:
    """Native footprint/local-conformance worker (the default authority).

    For each trace, every OBSERVED adjacent activity pair must be a
    directly-follows edge the reference model (``allowed_edges``, e.g. a
    discovered directly-follows graph's edge set) actually contains. This is a
    bounded, real conformance technique — local/footprint conformance — not a
    claim to full alignment-based conformance (which additionally reasons
    about skipped/inserted activities via edit-distance alignment; that is
    exactly the kind of heavier technique an optional
    :class:`ConformanceWorker`, e.g. a PM4Py-backed one, may add without this
    module needing the dependency).

    ``start_activities``/``end_activities`` are OPTIONAL declared sets (e.g.
    from a discovered alpha-lite footprint's own start/end sets) — when
    omitted, start/end conformance is simply not checked, since a
    directly-follows edge set alone cannot distinguish "never legally starts
    the process" from "legally a mid-process activity that happens to open
    this particular trace".
    """
    if len(traces) != len(object_ids):
        raise ValueError("traces and object_ids must be the same length")
    allowed = frozenset(allowed_edges)
    starts = frozenset(start_activities) if start_activities is not None else None
    ends = frozenset(end_activities) if end_activities is not None else None
    deviations: list[Deviation] = []
    for trace, object_id in zip(traces, object_ids, strict=True):
        for position in range(len(trace) - 1):
            pair = (trace[position], trace[position + 1])
            if pair in allowed:
                continue
            deviation_digest = hashlib.sha256(
                f"{run.run_id}\x1f{object_id}\x1f{position}\x1f{pair}".encode()
            ).hexdigest()[:32]
            deviations.append(
                Deviation(
                    deviation_id=f"deviation:{deviation_digest}",
                    object_id=object_id,
                    position=position,
                    kind="unexpected_transition",
                    observed_activity=pair[1],
                    expected_activities=tuple(
                        sorted(
                            successor
                            for predecessor, successor in allowed
                            if predecessor == pair[0]
                        )
                    ),
                )
            )
        if trace and starts is not None and trace[0] not in starts:
            deviation_digest = hashlib.sha256(
                f"{run.run_id}\x1f{object_id}\x1fstart\x1f{trace[0]}".encode()
            ).hexdigest()[:32]
            deviations.append(
                Deviation(
                    deviation_id=f"deviation:{deviation_digest}",
                    object_id=object_id,
                    position=0,
                    kind="unexpected_start",
                    observed_activity=trace[0],
                    expected_activities=tuple(sorted(starts)),
                )
            )
        if trace and ends is not None and trace[-1] not in ends:
            deviation_digest = hashlib.sha256(
                f"{run.run_id}\x1f{object_id}\x1fend\x1f{trace[-1]}".encode()
            ).hexdigest()[:32]
            deviations.append(
                Deviation(
                    deviation_id=f"deviation:{deviation_digest}",
                    object_id=object_id,
                    position=len(trace) - 1,
                    kind="unexpected_end",
                    observed_activity=trace[-1],
                    expected_activities=tuple(sorted(ends)),
                )
            )
    return tuple(deviations)


def run_conformance_check(
    traces: Sequence[tuple[str, ...]],
    object_ids: Sequence[str],
    allowed_edges: Iterable[tuple[str, str]],
    *,
    run: ConformanceRun,
    worker: ConformanceWorker = check_directly_follows_conformance,
    start_activities: Iterable[str] | None = None,
    end_activities: Iterable[str] | None = None,
) -> tuple[ConformanceRun, tuple[Deviation, ...]]:
    """Execute ``worker`` over ``run``'s frozen boundary; return it unchanged.

    Returning the SAME ``run`` object (never mutating or re-deriving it from
    the traces/model just checked) is what keeps discovery and conformance
    formally separated end to end: the run's identity was fixed before this
    call, by its caller, from data this function does not get to revise.
    """
    deviations = worker(
        traces,
        object_ids,
        frozenset(allowed_edges),
        run=run,
        start_activities=None
        if start_activities is None
        else frozenset(start_activities),
        end_activities=None if end_activities is None else frozenset(end_activities),
    )
    return run, deviations


def conformance_run_graph_slice(
    run: ConformanceRun,
    deviations: Sequence[Deviation],
    *,
    source_ref: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Render one ``(ConformanceRun, deviations)`` result as canonical
    nodes/links for the existing ``ingest_graph_slice`` writer
    (CONCEPT:AU-KG.mining.process-conformance-checking) — the graph-writeback
    counterpart of :meth:`~.semantic_event_model.ObjectCentricGraphSlice
    .to_graph_slice`, using the SAME ``_stable_id`` scheme so a
    ``CHECKED_UNDER_PERSPECTIVE`` edge lands on the perspective node an OCEL
    commit already materialized under that ``source_ref``, when one exists.

    D-61-1: makes a conformance result queryable from the graph ("which
    conformance runs happened, with what deviations") — it does not, by
    itself, make ``run_conformance_check`` write anywhere; a caller commits
    the returned ``(entities, links)`` through ``ingest_graph_slice``
    (mirrors every other connector's commit idiom).
    """
    run_node_id = _stable_id("conformance-run", source_ref, run.run_id)
    perspective_node_id = _stable_id(
        "process-perspective", source_ref, run.perspective.perspective_id
    )
    entities: list[dict[str, Any]] = [
        {
            "id": run_node_id,
            "node_type": "ConformanceRun",
            "source_record_id": run.run_id,
            "run_digest": run.run_digest(),
            "perspective_id": run.perspective.perspective_id,
            "graph_as_of": run.graph_as_of.isoformat(),
            "mapping_version": run.mapping_version,
            "model_ref": run.model_ref,
            "export_digest": run.export_digest,
            "worker": run.worker,
            "created_at": run.created_at.isoformat(),
            "source_ref": source_ref,
        }
    ]
    links: list[dict[str, Any]] = [
        {
            "source": run_node_id,
            "target": perspective_node_id,
            "relationship": "CHECKED_UNDER_PERSPECTIVE",
        }
    ]
    for deviation in sorted(deviations, key=lambda value: value.deviation_id):
        deviation_node_id = _stable_id("deviation", source_ref, deviation.deviation_id)
        entities.append(
            {
                "id": deviation_node_id,
                "node_type": "Deviation",
                "source_record_id": deviation.deviation_id,
                "object_id": deviation.object_id,
                "position": deviation.position,
                "kind": deviation.kind,
                "observed_activity": deviation.observed_activity,
                "expected_activities": list(deviation.expected_activities),
                "source_ref": source_ref,
            }
        )
        links.append(
            {
                "source": run_node_id,
                "target": deviation_node_id,
                "relationship": "HAS_DEVIATION",
            }
        )
    return entities, links
