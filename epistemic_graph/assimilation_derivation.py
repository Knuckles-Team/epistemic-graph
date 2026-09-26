"""Deterministic feature classification and leverage derivation.

Callers supply bounded feature subgraphs; source reads, algorithm selection, and
graph writes remain with their owning integration layer.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass
class SynergyBundle:
    members: list[str]
    pillars: list[str]


@dataclass
class RankedFeature:
    feature_id: str
    score: float
    source_count: int
    centrality: float


def pillar_of(data: dict[str, Any]) -> str:
    """Derive an architectural pillar from an explicit tag or concept namespace."""
    if data.get("pillar"):
        return str(data["pillar"])
    for concept_id in data.get("concept_ids", []) or []:
        namespace = str(concept_id).split(".", 1)[0].upper()
        parts = namespace.split("-")
        head = parts[1] if len(parts) > 1 and parts[0] in {"AU", "EG"} else parts[0]
        if head:
            return head
    return ""


def connected_components(
    ids: set[str], adjacency: dict[str, set[str]]
) -> list[list[str]]:
    """Find components in an already bounded feature adjacency map."""
    seen: set[str] = set()
    components: list[list[str]] = []
    for start in ids:
        if start in seen:
            continue
        stack, component = [start], []
        seen.add(start)
        while stack:
            node = stack.pop()
            component.append(node)
            for neighbor in adjacency.get(node, ()):
                if neighbor not in seen:
                    seen.add(neighbor)
                    stack.append(neighbor)
        components.append(component)
    return components


def bundle_pillars(
    nodes: dict[str, dict], community: list[str], min_pillars: int
) -> list[str] | None:
    """Return a community's sorted pillars when its diversity threshold is met."""
    pillars = sorted({p for p in (pillar_of(nodes[node]) for node in community) if p})
    return pillars if len(pillars) >= min_pillars else None


def degree_centrality(
    ids: set[str], adjacency: dict[str, set[str]]
) -> dict[str, float]:
    """Normalize degree within a supplied feature subgraph."""
    denominator = float(max(1, len(ids) - 1))
    return {node: len(adjacency.get(node, ())) / denominator for node in ids}


def rank_feature_rows(
    ids: set[str], nodes: dict[str, dict], centrality: dict[str, float]
) -> list[RankedFeature]:
    """Score open feature ids from supplied source counts and centrality."""
    ranked: list[RankedFeature] = []
    for feature_id in ids:
        sources = nodes[feature_id].get("research_sources") or []
        source_count = max(1, len(sources))
        score = float(centrality.get(feature_id, 0.0))
        ranked.append(
            RankedFeature(
                feature_id=feature_id,
                score=round(source_count * (1.0 + score), 6),
                source_count=source_count,
                centrality=round(score, 6),
            )
        )
    ranked.sort(key=lambda row: (row.score, row.feature_id), reverse=True)
    return ranked


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def duplicate_clusters(ids: list[str], dup_pairs) -> list[list[str]]:
    """Union-find connected components over the duplicate pairs (size ≥ 2)."""
    parent = {n: n for n in ids}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b, _ in dup_pairs:
        if a in parent and b in parent:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb
    groups: dict[str, list[str]] = {}
    for n in ids:
        groups.setdefault(find(n), []).append(n)
    return [g for g in groups.values() if len(g) > 1]
