"""Bounded, graph-native latent-topology retrieval over governed Cypher reads.

This is the deterministic topology stage. Embedding a natural-language query
and learning a routing policy are separate capabilities; ``query`` is retained
as request context and never interpolated into Cypher. The supplied read
function must be bound to a verified caller and apply graph row policy.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any


def retrieve_latent_topology(
    query: str,
    *,
    read: Callable[[str], list[dict[str, Any]]],
    top_k: int = 5,
    routing_threshold: float = 0.7,
) -> list[dict[str, Any]]:
    """Rank nodes reachable by a bounded ``IS_A`` or ``CONTAINS`` path.

    Each Cypher read returns nodes after the server's RLS filter. The caller
    must additionally enforce its application-level read policy. A malformed
    row is discarded; a read failure propagates instead of returning a partial
    cross-policy result.
    """
    del query  # No query embedding is available at this deterministic stage.
    if not 1 <= top_k <= 100:
        raise ValueError("top_k must be between 1 and 100")
    if not math.isfinite(routing_threshold) or not 0.0 <= routing_threshold <= 1.0:
        raise ValueError("routing_threshold must be finite and between 0 and 1")

    # Cypher supports one relationship type per pattern. Two bounded reads
    # cover both hierarchy relations and also keep each response capped.
    by_id: dict[str, dict[str, Any]] = {}
    for relation in ("IS_A", "CONTAINS"):
        statement = (
            f"MATCH (entry)-[:{relation}*1..3]->(n) "
            f"WHERE n.importance_score > {routing_threshold:.17g} "
            f"RETURN n ORDER BY n.importance_score DESC LIMIT {top_k}"
        )
        for row in read(statement):
            if not isinstance(row, dict):
                continue
            node = row.get("n", row)
            if not isinstance(node, dict):
                continue
            node_id = node.get("id")
            raw_score = node.get("importance_score")
            if not isinstance(node_id, str) or not node_id:
                continue
            try:
                score = float(raw_score)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(score) or score <= routing_threshold:
                continue
            candidate = {"id": node_id, "name": node.get("name", ""), "score": score}
            prior = by_id.get(node_id)
            if prior is None or score > prior["score"]:
                by_id[node_id] = candidate
    return sorted(by_id.values(), key=lambda item: (-item["score"], item["id"]))[:top_k]


__all__ = ["retrieve_latent_topology"]
