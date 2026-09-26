"""Deterministic projection of derived graph slices for governed ingestion.

SourceIngest commits raw connector records through catalog mappings. Derived
nodes and edges already have canonical graph types and use the native
ApplyChangeEnvelope commit path. This module owns their pure validation and
identity projection; it neither grants write authority nor advances a cursor.
"""

from __future__ import annotations

import hashlib
import json
import threading
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


class GraphSliceCapture:
    """Thread-safe write buffer for one native enrichment graph slice.

    Extractors keep their small ``add_node``/``add_edge`` protocol, but none of
    those calls reaches a durable backend. The completed slice is handed to
    ``ingest_graph_slice`` once, preserving ChangeEnvelope atomicity even when
    concept windows are extracted concurrently.
    """

    def __init__(self, read_backend: Any) -> None:
        self._read_backend = read_backend
        self._nodes: dict[str, dict[str, Any]] = {}
        self._edges: list[dict[str, Any]] = []
        self._edge_keys: set[str] = set()
        self._lock = threading.RLock()

    def add_node(
        self,
        node_id: str,
        label: str = "",
        **properties: Any,
    ) -> None:
        row = dict(properties)
        if "type" in row:
            raise ValueError(
                "node property 'type' is retired; the canonical node-class "
                "property is 'node_type' "
                "(project typed models with RegistryNode.to_graph_properties())"
            )
        node_type = str(row.pop("node_type", "") or label or "Entity")
        row["id"] = str(node_id)
        row["node_type"] = node_type
        with self._lock:
            current = self._nodes.setdefault(str(node_id), {"id": str(node_id)})
            current.update(row)

    def add_edge(
        self,
        source: str,
        target: str,
        rel_type: str = "",
        **properties: Any,
    ) -> None:
        row = dict(properties)
        aliases = frozenset(
            {"type", "rel_type", "relationship_type", "relation"}
        ).intersection(row)
        if aliases:
            raise ValueError(
                "edge properties ("
                + ", ".join(f"'{alias}'" for alias in sorted(aliases))
                + ") are retired; the canonical relationship property is 'relationship'"
            )
        edge_type = str(row.pop("relationship", "") or rel_type or "RELATED_TO")
        edge = {
            "source": str(source),
            "target": str(target),
            "relationship": edge_type,
            **row,
        }
        key = json.dumps(edge, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock:
            if key not in self._edge_keys:
                self._edge_keys.add(key)
                self._edges.append(edge)

    def semantic_search(self, *args: Any, **kwargs: Any) -> Any:
        search = getattr(self._read_backend, "semantic_search", None)
        return search(*args, **kwargs) if callable(search) else []

    def snapshot(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        with self._lock:
            return (
                [dict(self._nodes[node_id]) for node_id in sorted(self._nodes)],
                [
                    dict(row)
                    for row in sorted(
                        self._edges,
                        key=lambda item: json.dumps(
                            item, sort_keys=True, separators=(",", ":"), default=str
                        ),
                    )
                ],
            )
