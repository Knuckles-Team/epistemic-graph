"""Deterministic projection of derived graph slices for governed ingestion.

SourceIngest commits raw connector records through catalog mappings. Derived
nodes and edges already have canonical graph types and use the native
ApplyChangeEnvelope commit path. This module owns their pure validation and
identity projection; it neither grants write authority nor advances a cursor.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def validate_graph_slice(
    entities: list[dict[str, Any]], relationships: list[dict[str, Any]]
) -> None:
    """Require canonical graph type keys before deriving a commit payload."""
    for entity in entities:
        if "type" in entity or not str(entity.get("node_type") or "").strip():
            raise ValueError(
                "graph-slice nodes require canonical node_type and may not use type"
            )
    for relationship in relationships:
        if (
            any(
                key in relationship
                for key in ("type", "rel_type", "relationship_type", "relation")
            )
            or not str(relationship.get("relationship") or "").strip()
        ):
            raise ValueError(
                "graph-slice edges require canonical relationship and no aliases"
            )


def graph_slice_digest(payload: dict[str, Any]) -> str:
    """Derive the exact replay identity of a canonical graph slice."""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


def edge_only_marker_entity(
    connector: str, source_instance: str, relationships: list[dict[str, Any]]
) -> dict[str, Any]:
    """Give an edge-only derived batch a stable primary delivery identity."""
    marker_digest = graph_slice_digest(
        {
            "connector": connector,
            "source_instance": source_instance,
            "relationships": relationships,
        }
    )
    return {
        "id": f"source-materialization:{marker_digest}",
        "node_type": "SourceMaterialization",
        "source_system": connector,
        "relationship_count": len(relationships),
    }


def graph_slice_primary(
    entities: list[dict[str, Any]], relationships: list[dict[str, Any]]
) -> dict[str, Any]:
    """Attach auxiliary nodes and edges to a copy of the primary row."""
    primary = dict(entities[0])
    if len(entities) > 1:
        primary["_nodes"] = [dict(item) for item in entities[1:]]
    if relationships:
        primary["_links"] = [dict(item) for item in relationships]
    return primary
