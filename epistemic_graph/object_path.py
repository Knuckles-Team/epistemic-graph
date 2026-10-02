"""Shortest path between graph objects with optional node and edge labels.

The caller supplies a graph read adapter implementing ``get_shortest_path``
and ``query_cypher``. This keeps the object model in the graph package while
allowing callers to use their configured, authorized graph client.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["find_object_path"]


def _resolve_path(engine: Any, source_id: str, target_id: str) -> list[str] | None:
    """Return the shortest node-id path, trying the reverse direction too.

    An edge that only runs one direction in the property graph is still
    found by reversing a hit from ``target_id -> source_id``.
    """
    path = engine.get_shortest_path(source_id, target_id)
    if path:
        return path
    reverse_path = engine.get_shortest_path(target_id, source_id)
    return list(reversed(reverse_path)) if reverse_path else None


def _label_nodes(engine: Any, path: list[str]) -> dict[str, dict[str, Any]]:
    """Resolve a friendly type/name for every node on the path in one query."""
    labels: dict[str, dict[str, Any]] = {}
    try:
        rows = engine.query_cypher(
            "MATCH (n) WHERE n.id IN $ids "
            "RETURN n.id AS id, n.type AS type, n.name AS name",
            {"ids": path},
        )
    except Exception:
        logger.debug(
            "Node labeling query failed; path remains valid without labels",
            exc_info=True,
        )
        return labels
    for row in rows or []:
        node_id = row.get("id")
        if node_id:
            labels[node_id] = {"type": row.get("type"), "name": row.get("name")}
    return labels


def _annotate_hop(engine: Any, a: str, b: str) -> dict[str, Any]:
    """Look up the relationship type/confidence between two adjacent nodes."""
    rel, confidence = None, None
    try:
        erows = engine.query_cypher(
            "MATCH (x {id: $a})-[r]-(y {id: $b}) "
            "RETURN type(r) AS rel, r.confidence AS confidence LIMIT 1",
            {"a": a, "b": b},
        )
        if erows:
            rel = erows[0].get("rel")
            confidence = erows[0].get("confidence")
    except Exception:
        logger.debug(
            "Relationship annotation query failed; hop stays unannotated",
            exc_info=True,
        )
    return {"from": a, "to": b, "rel": rel, "confidence": confidence}


def _build_hops(engine: Any, path: list[str]) -> list[dict[str, Any]]:
    """Annotate every adjacent pair on the path with its relationship."""
    return [_annotate_hop(engine, a, b) for a, b in zip(path, path[1:], strict=False)]


def find_object_path(engine: Any, source_id: str, target_id: str) -> dict[str, Any]:
    """Find the shortest path between two objects and annotate each hop.

    Tries ``source_id -> target_id`` then the reverse, so an edge that only
    runs one direction in the property graph is still found. A missing path
    returns ``connected: False``.
    """
    if source_id == target_id:
        return {
            "source": source_id,
            "target": target_id,
            "connected": False,
            "error": "source and target are the same object",
        }

    path = _resolve_path(engine, source_id, target_id)
    if not path:
        return {
            "source": source_id,
            "target": target_id,
            "connected": False,
            "path": [],
        }

    labels = _label_nodes(engine, path)
    hops = _build_hops(engine, path)

    return {
        "source": source_id,
        "target": target_id,
        "connected": True,
        "length": len(path) - 1,
        "path": [{"id": node_id, **labels.get(node_id, {})} for node_id in path],
        "hops": hops,
    }
