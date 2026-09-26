"""Deterministic source-extraction projection into a graph-slice request."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol


class GraphNodeInput(Protocol):
    id: str
    type: str
    props: dict[str, Any]


class GraphEdgeInput(Protocol):
    source: str
    target: str
    rel_type: str
    props: dict[str, Any]


@dataclass(frozen=True)
class GraphSlice:
    entities: list[dict[str, Any]]
    relationships: list[dict[str, Any]]


def project_extraction_batch(
    nodes: Iterable[GraphNodeInput], edges: Iterable[GraphEdgeInput]
) -> GraphSlice:
    """Build the native graph-slice payload from typed extraction records.

    Extractor properties cannot override graph identity or relationship type.
    Invalid records fail before any caller starts a graph write.
    """
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    for node in nodes:
        if {"id", "node_type"} & node.props.keys():
            raise ValueError("source node properties contain reserved identity fields")
        entities.append(
            {
                "id": node.id,
                "node_type": node.type,
                **{
                    key: value for key, value in node.props.items() if value is not None
                },
            }
        )
    for edge in edges:
        if {"source", "target", "relationship"} & edge.props.keys():
            raise ValueError("source edge properties contain reserved identity fields")
        relationships.append(
            {
                "source": edge.source,
                "target": edge.target,
                "relationship": edge.rel_type,
                **{
                    key: value for key, value in edge.props.items() if value is not None
                },
            }
        )
    return GraphSlice(entities=entities, relationships=relationships)
