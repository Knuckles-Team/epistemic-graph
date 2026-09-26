"""Pure lifecycle closure rules for a bounded assimilation feature cohort."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

CLOSED_STATUSES = frozenset(
    {"satisfied", "implemented", "rejected", "superseded", "done"}
)
CLOSING_OUT_RELATIONS = frozenset({"SATISFIED_BY", "DERIVED_FROM_RESEARCH"})
CLOSING_IN_RELATIONS = frozenset({"SUPERSEDES"})


def status_is_closed(status: object) -> bool:
    """Classify a stored lifecycle status, independent of its graph adapter."""
    return str(status or "").lower() in CLOSED_STATUSES


def relation_closes(relation: str, *, incoming: bool) -> bool:
    """Classify an incident edge from the feature's perspective."""
    return relation in (CLOSING_IN_RELATIONS if incoming else CLOSING_OUT_RELATIONS)


def closed_feature_ids(
    features: Mapping[str, Mapping[str, Any]],
    edges: Iterable[tuple[str, str, str]],
) -> set[str]:
    """Derive closed ids from one bounded feature map and edge projection.

    Each edge tuple is ``(source_id, target_id, relation)``. The caller owns
    bounded graph reads and passes only relationships it has observed.
    """
    closed = {
        feature_id
        for feature_id, data in features.items()
        if status_is_closed(data.get("status"))
    }
    for source_id, target_id, relation in edges:
        if source_id in features and relation_closes(relation, incoming=False):
            closed.add(source_id)
        if target_id in features and relation_closes(relation, incoming=True):
            closed.add(target_id)
    return closed


def feature_properties(
    *,
    name: str,
    concept_ids: Iterable[str] = (),
    research_sources: Iterable[str] = (),
    status: str = "open",
    sdd_path: str = "",
    codebase: str = "",
) -> dict[str, Any]:
    """Project one lifecycle feature's stored property set."""
    return {
        "name": name,
        "concept_ids": list(concept_ids),
        "research_sources": list(research_sources),
        "status": status,
        "sdd_path": sdd_path,
        "codebase": codebase,
    }


def feature_ledger_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Normalize a supplied feature-ledger row for the graph write adapter."""
    feature_id = str(row.get("id") or "").strip()
    if not feature_id:
        return None
    concept = str(row.get("concept", "") or "")
    source = str(row.get("source", "") or "")
    return {
        "feature_id": feature_id,
        "name": str(row.get("name", feature_id)),
        "concept_ids": [concept] if concept and concept != "UNKNOWN" else [],
        "research_sources": [source] if source else [],
        "status": str(row.get("status", "open") or "open"),
        "sdd_path": str(row.get("target", "") or ""),
    }


def assimilation_edge_properties(
    status: str, assimilation_date: str | None = None
) -> dict[str, Any]:
    """Project one research-source-to-codebase provenance edge."""
    props: dict[str, Any] = {
        "_rel": "ASSIMILATED_INTO",
        "status": status,
        "concept": "AU-KG.query.vendor-agnostic-traversal",
    }
    if assimilation_date:
        props["assimilation_date"] = assimilation_date
    return props
