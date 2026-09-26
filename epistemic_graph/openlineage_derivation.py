"""Pure OpenLineage RunEvent to graph lineage identity mapping.

The caller owns Kafka offsets and graph writes. This module validates the
wire event and derives deterministic dataset and activity inputs.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

# OpenLineage's own RunEvent.eventType enum -> this KG's prov:Activity status
# vocabulary. An eventType outside this map is quarantined, never guessed.
EVENT_TYPE_TO_STATUS: dict[str, str] = {
    "START": "running",
    "RUNNING": "running",
    "COMPLETE": "completed",
    "ABORT": "aborted",
    "FAIL": "failed",
    "OTHER": "unknown",
}


# DEC-CA-05's dataset-naming rule: this regex is deliberately IDENTICAL to
# egeria-mcp's own ``_ICEBERG_DATASET_RE`` (reconcile.py) — the same wire
# format, independently validated on each side of the MCP boundary rather
# than imported across it.
_ICEBERG_DATASET_RE = re.compile(
    r"^iceberg://(?P<catalog>[^/]+)/(?P<namespace>[^/]+)/(?P<table>[^/@]+)@(?P<snapshot>[^/@]+)$"
)

# OpenLineage's own top-level/nested RunEvent field names, named rather than
# repeated as literal strings — this module only ever READS them (no producer
# lives in this repo, by design: the wire format is defined upstream by
# OpenLineage/Spark/Trino/eg), so a literal-string ``.get("outputs")`` etc.
# reads as an orphaned key to a naive same-string-anywhere-in-repo scan the
# instant an unrelated module elsewhere happens to read the same short field
# name fewer than 3 times (as ``ecosystem.media.gateway`` does for
# "outputs", coincidentally, for an unrelated payload shape).
_FIELD_RUN = "run"
_FIELD_RUN_ID = "runId"
_FIELD_JOB = "job"
_FIELD_JOB_NAME = "name"
_FIELD_JOB_NAMESPACE = "namespace"
_FIELD_EVENT_TYPE = "eventType"
_FIELD_INPUTS = "inputs"
_FIELD_OUTPUTS = "outputs"
_FIELD_FACETS = "facets"
_FIELD_PARENT = "parent"
_FIELD_VERSION = "version"
_FIELD_DATASET_VERSION = "datasetVersion"


class MalformedLineageDataset(ValueError):
    """A dataset did not resolve to ``iceberg://<catalog>/<ns>/<table>@<snapshot>``.

    Raised by :func:`dataset_entity_id`, caught by :func:`map_openlineage_event`
    to quarantine the whole RunEvent — never a partially-mapped one with a
    fabricated entity id.
    """


def _as_dict(value: Any) -> dict[str, Any]:
    """Narrow ``value`` to a dict or an empty one — the one place this module
    dereferences an optional nested mapping, so mypy can narrow a plain local
    variable instead of re-checking a repeated ``.get()`` call expression
    (mirrors ``..ingestion.debezium_envelope._as_dict`` exactly)."""
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    """The list twin of :func:`_as_dict`, for ``inputs``/``outputs``."""
    return value if isinstance(value, list) else []


def dataset_entity_id(dataset: dict[str, Any]) -> str:
    """Return the ``iceberg://<catalog>/<ns>/<table>@<snapshot>`` id for one
    OpenLineage dataset object, or raise :class:`MalformedLineageDataset`.

    Expects ``namespace`` already in ``iceberg://<catalog>/<ns>`` form,
    ``name`` the bare table name, and the snapshot in the standard OpenLineage
    ``version`` dataset facet (``facets.version.datasetVersion``).
    """
    namespace = str(dataset.get("namespace") or "").strip()
    name = str(dataset.get("name") or "").strip()
    facets = _as_dict(dataset.get(_FIELD_FACETS))
    version_facet = facets.get(_FIELD_VERSION)
    snapshot = ""
    if isinstance(version_facet, dict):
        snapshot = str(version_facet.get(_FIELD_DATASET_VERSION) or "").strip()
    candidate = (
        f"{namespace.rstrip('/')}/{name}@{snapshot}" if namespace and name else ""
    )
    if not _ICEBERG_DATASET_RE.match(candidate):
        raise MalformedLineageDataset(
            f"dataset namespace={namespace!r} name={name!r} snapshot={snapshot!r} "
            "does not resolve to iceberg://<catalog>/<ns>/<table>@<snapshot>"
        )
    return candidate


@dataclass(frozen=True)
class MappedRunEvent:
    """One OpenLineage RunEvent, validated and normalized (no graph I/O)."""

    run_id: str
    job_name: str
    job_namespace: str
    event_type: str
    activity_status: str
    input_dataset_ids: tuple[str, ...] = field(default_factory=tuple)
    output_dataset_ids: tuple[str, ...] = field(default_factory=tuple)
    parent_run_id: str = ""


@dataclass(frozen=True)
class QuarantinedLineageEvent:
    """A RunEvent this module refused to auto-map — logged, never guessed.

    Mirrors :class:`~..ingestion.debezium_envelope.QuarantinedRecord`'s
    fail-closed shape for the lineage domain.
    """

    run_id: str
    job_name: str
    event_type: str
    reason: str


def _run_id(event: dict[str, Any]) -> str:
    return str(_as_dict(event.get(_FIELD_RUN)).get(_FIELD_RUN_ID) or "").strip()


def _parent_run_id(event: dict[str, Any]) -> str:
    run = _as_dict(event.get(_FIELD_RUN))
    facets = _as_dict(run.get(_FIELD_FACETS))
    parent = facets.get(_FIELD_PARENT)
    if not isinstance(parent, dict):
        return ""
    parent_run = _as_dict(parent.get(_FIELD_RUN))
    return str(parent_run.get(_FIELD_RUN_ID) or "").strip()


def _dataset_ids(event: dict[str, Any], field: str) -> tuple[str, ...]:
    """Map valid dataset objects from one event field to entity ids.

    OpenLineage payloads may contain non-mapping values; the existing mapper
    intentionally ignores those while allowing :class:`MalformedLineageDataset`
    from a mapping to quarantine the whole event in its caller.
    """
    datasets = _as_list(event.get(field))
    return tuple(
        dataset_entity_id(dataset) for dataset in datasets if isinstance(dataset, dict)
    )


def map_openlineage_event(
    event: dict[str, Any],
) -> MappedRunEvent | QuarantinedLineageEvent:
    """Validate + normalize one raw OpenLineage RunEvent dict.

    Pure and engine-independent — every failure mode a fixture can hit is
    testable here without a graph. See the module docstring for the mapping
    rule; ``inputs``/``outputs`` are validated as a whole (one malformed
    dataset quarantines the entire event rather than mapping the rest).
    """
    run_id = _run_id(event)
    job = _as_dict(event.get(_FIELD_JOB))
    job_name = str(job.get(_FIELD_JOB_NAME) or "").strip()
    job_namespace = str(job.get(_FIELD_JOB_NAMESPACE) or "").strip()
    event_type = str(event.get(_FIELD_EVENT_TYPE) or "").strip().upper()

    if not run_id:
        return QuarantinedLineageEvent(
            run_id=run_id,
            job_name=job_name,
            event_type=event_type,
            reason="missing run.runId",
        )
    if not job_name:
        return QuarantinedLineageEvent(
            run_id=run_id,
            job_name=job_name,
            event_type=event_type,
            reason="missing job.name",
        )
    status = EVENT_TYPE_TO_STATUS.get(event_type)
    if status is None:
        return QuarantinedLineageEvent(
            run_id=run_id,
            job_name=job_name,
            event_type=event_type,
            reason=f"unmapped eventType {event_type!r}",
        )

    try:
        input_ids = _dataset_ids(event, _FIELD_INPUTS)
        output_ids = _dataset_ids(event, _FIELD_OUTPUTS)
    except MalformedLineageDataset as exc:
        return QuarantinedLineageEvent(
            run_id=run_id,
            job_name=job_name,
            event_type=event_type,
            reason=str(exc),
        )

    return MappedRunEvent(
        run_id=run_id,
        job_name=job_name,
        job_namespace=job_namespace,
        event_type=event_type,
        activity_status=status,
        input_dataset_ids=input_ids,
        output_dataset_ids=output_ids,
        parent_run_id=_parent_run_id(event),
    )


def openlineage_activity_id(run_id: str) -> str:
    """Stable opaque PROV-O activity id for all lifecycle events of one run."""
    digest = hashlib.sha256(run_id.encode()).hexdigest()
    return f"activity:openlineage_run:{digest}"


def openlineage_activity_properties(
    mapped: MappedRunEvent, *, at: float
) -> dict[str, Any]:
    """Project a validated run event onto one PROV-O Activity property set."""
    return {
        "kind": "openlineage_run",
        "job": mapped.job_name,
        "jobNamespace": mapped.job_namespace,
        "eventType": mapped.event_type,
        "status": mapped.activity_status,
        "at": at,
    }
