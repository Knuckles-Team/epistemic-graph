"""Bounded self-model pointer reads over the graph's governed Cypher surface.

The supplied reader must be bound to an authenticated graph request. These
helpers validate the graph shape; they do not grant read authority themselves.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

ReadCypher = Callable[[str, dict[str, str]], list[dict[str, Any]]]

_CURRENT = (
    "MATCH (anchor {id: $anchor_id})-[:CURRENT_SELF_MODEL]->"
    "(sm:MemoryRetriever) RETURN sm LIMIT 2"
)
_PREVIOUS = (
    "MATCH (n {id: $node_id})-[:SUPERSEDES]->"
    "(prev:MemoryRetriever) RETURN prev LIMIT 2"
)


def _single_model(rows: list[dict[str, Any]], *, column: str) -> dict[str, Any] | None:
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError("self-model pointer is ambiguous")
    row = rows[0]
    if not isinstance(row, dict) or not isinstance(row.get(column), dict):
        raise ValueError("self-model pointer returned a malformed row")
    model = row[column]
    if (
        not isinstance(model.get("id"), str)
        or not model["id"]
        or model.get("node_type") != "memory_retriever"
        or type(model.get("version")) is not int
        or model["version"] < 1
    ):
        raise ValueError("self-model pointer target is malformed")
    return model


def read_current_self_model(
    read: ReadCypher, *, anchor_id: str = "self:agent-model"
) -> dict[str, Any] | None:
    """Return the sole current self-model or ``None`` when no pointer exists."""
    if not isinstance(anchor_id, str) or not anchor_id:
        raise ValueError("anchor_id must be a non-empty string")
    return _single_model(read(_CURRENT, {"anchor_id": anchor_id}), column="sm")


def read_previous_self_model(
    read: ReadCypher, *, node_id: str
) -> dict[str, Any] | None:
    """Return the sole predecessor of a self-model snapshot, if present."""
    if not isinstance(node_id, str) or not node_id:
        raise ValueError("node_id must be a non-empty string")
    previous = _single_model(read(_PREVIOUS, {"node_id": node_id}), column="prev")
    if previous is not None and previous["id"] == node_id:
        raise ValueError("self-model snapshot cannot supersede itself")
    return previous


__all__ = ["ReadCypher", "read_current_self_model", "read_previous_self_model"]
