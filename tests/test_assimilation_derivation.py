"""Pure assimilation derivation contract, independent of graph transport."""

import pytest

from epistemic_graph.assimilation_derivation import (
    bundle_pillars,
    connected_components,
    cosine_similarity,
    degree_centrality,
    duplicate_clusters,
    pillar_of,
    rank_feature_rows,
)

pytestmark = pytest.mark.no_engine


def test_pillar_and_cross_pillar_component():
    nodes = {
        "kg": {"concept_ids": ["AU-KG.memory.tiered-memory-caching"]},
        "orch": {"concept_ids": ["EG-ORCH.adapter.hot-cache-invalidation"]},
        "isolated": {"pillar": "AHE"},
    }
    adjacency = {"kg": {"orch"}, "orch": {"kg"}, "isolated": set()}
    components = connected_components(set(nodes), adjacency)
    assert {frozenset(c) for c in components} == {
        frozenset({"kg", "orch"}),
        frozenset({"isolated"}),
    }
    assert bundle_pillars(nodes, ["kg", "orch"], 2) == ["KG", "ORCH"]
    assert bundle_pillars(nodes, ["kg", "orch"], 3) is None
    assert pillar_of({"pillar": "OS", "concept_ids": ["AU-KG.x"]}) == "OS"
    assert pillar_of({}) == ""


def test_degree_and_leverage_rank_are_bounded_to_supplied_features():
    ids = {"a", "b", "c"}
    adjacency = {"a": {"b", "c"}, "b": {"a"}, "c": {"a"}}
    degree = degree_centrality(ids, adjacency)
    assert degree == {"a": 1.0, "b": 0.5, "c": 0.5}
    nodes = {
        "a": {"research_sources": ["paper"]},
        "b": {"research_sources": ["one", "two"]},
        "c": {},
    }
    ranked = rank_feature_rows(ids, nodes, degree)
    assert [(r.feature_id, r.score, r.source_count) for r in ranked] == [
        ("b", 3.0, 2),
        ("a", 2.0, 1),
        ("c", 1.5, 1),
    ]


def test_dedup_cosine_handles_orthogonal_and_zero_vectors():
    assert cosine_similarity([1.0, 0.0], [1.0, 0.0]) == 1.0
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == 0.0
    assert cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0


def test_dedup_clusters_transitively_connect_only_known_ids():
    clusters = duplicate_clusters(
        ["a", "b", "c", "d"],
        [("a", "b", 0.95), ("b", "c", 0.96), ("c", "unknown", 1.0)],
    )
    assert clusters == [["a", "b", "c"]]
