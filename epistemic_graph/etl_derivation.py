"""Pure ETL result projection, aggregation, and lineage direction."""

from __future__ import annotations

from typing import Any

from .etl_result import EtlResult


def step_result(
    data: EtlResult | dict[str, Any],
    *,
    source: str | None = None,
    sink: str | None = None,
    mode: str | None = None,
) -> EtlResult:
    """Project one current internal step result onto the strict ETL wire schema."""
    if isinstance(data, EtlResult):
        return data
    fields = set(EtlResult.model_fields)
    payload = {key: value for key, value in data.items() if key in fields}
    details = {key: value for key, value in data.items() if key not in fields}
    payload.setdefault("source", source)
    payload.setdefault("sink", sink)
    payload.setdefault("mode", mode)
    payload["details"] = {**dict(payload.get("details") or {}), **details}
    return EtlResult.model_validate(payload)


def aggregate_counts(
    *steps: EtlResult | None,
) -> dict[str, int]:
    """Combine count fields from completed ETL steps in execution order."""
    counts: dict[str, int] = {}
    for step in steps:
        if step is None:
            continue
        for name, value in step.counts.items():
            counts[name] = counts.get(name, 0) + value
    return counts


def lineage_direction(source: str | None, sink: str | None) -> str:
    """Name the direction represented by the requested ETL endpoints."""
    if source and sink:
        return "through"
    if source:
        return "inbound"
    return "outbound"
