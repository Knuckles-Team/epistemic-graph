"""Graph corpus generator: planted ranking signals, topology and analytics ground
truth, each re-derived by a route that does not reuse the generator's own
construction (mirrors the sibling `test_synthetic_catalog.py` self-check style).
"""

from __future__ import annotations

import math

import pytest

from epistemic_graph.testing.synthetic.graph_corpus import (
    EPISODE_TYPE,
    ONTOLOGY_NODE_IDS,
    QUERY_TEXT,
    QUERY_VECTOR,
)
from epistemic_graph.testing.synthetic.graph_corpus_generate import (
    generate_memory_corpus,
)

pytestmark = pytest.mark.no_engine

SEEDS = (0, 7, 2026)


def _cosine(a: tuple[float, ...], b: tuple[float, ...]) -> float:
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def _term_hits(text: str, query: str) -> int:
    """How many query-term occurrences (case-insensitive, whitespace-split) a body
    contains -- a coarse, dependency-free proxy for "this body is the one a real
    BM25 index must rank highest", independent of the engine's own tokenizer."""
    terms = query.lower().split()
    words = text.lower().split()
    return sum(words.count(term) for term in terms)


@pytest.fixture(scope="module", params=SEEDS)
def corpus(request: pytest.FixtureRequest):
    return generate_memory_corpus(request.param)


def test_same_seed_same_bytes_and_seeds_differ() -> None:
    assert (
        generate_memory_corpus(3).model_dump_json()
        == generate_memory_corpus(3).model_dump_json()
    )
    assert (
        generate_memory_corpus(3).model_dump_json()
        != generate_memory_corpus(4).model_dump_json()
    )
    assert generate_memory_corpus(3).provenance.evidence == "synthetic"


def test_node_ids_are_salted_per_seed() -> None:
    a, b = generate_memory_corpus(1), generate_memory_corpus(2)
    assert a.target_id != b.target_id
    assert {n.node_id for n in a.nodes}.isdisjoint({n.node_id for n in b.nodes})


def test_vector_only_is_the_unique_cosine_maximum_among_episodes(corpus) -> None:
    episodes = [
        n for n in corpus.nodes if n.type == EPISODE_TYPE and n.embedding is not None
    ]
    scored = sorted(
        ((n.node_id, _cosine(n.embedding, QUERY_VECTOR)) for n in episodes),
        key=lambda row: row[1],
        reverse=True,
    )
    assert scored[0][0] == corpus.vec_only_id, scored
    assert scored[0][1] > scored[1][1], "vec_only must be a STRICT vector maximum"


def test_lex_only_is_the_unique_term_density_maximum_among_episodes(corpus) -> None:
    episodes = [
        n for n in corpus.nodes if n.type == EPISODE_TYPE and n.text is not None
    ]
    scored = sorted(
        ((n.node_id, _term_hits(n.text, QUERY_TEXT)) for n in episodes),
        key=lambda row: row[1],
        reverse=True,
    )
    assert scored[0][0] == corpus.lex_only_id, scored
    assert scored[0][1] > scored[1][1], "lex_only must be a STRICT term-density maximum"
    # `target` still carries every query term (once each) -- it is a real lexical
    # contender, just not the densest -- while `noise`/`chatter`/`vec_only` share
    # zero query terms with the query at all.
    by_id = {n.node_id: n for n in episodes}
    assert _term_hits(by_id[corpus.target_id].text, QUERY_TEXT) == len(
        QUERY_TEXT.split()
    )
    for zero_id in (corpus.vec_only_id, corpus.noise_id):
        assert _term_hits(by_id[zero_id].text, QUERY_TEXT) == 0


def test_expired_window_excludes_now_but_not_the_earlier_instant(corpus) -> None:
    expired = corpus.node(corpus.expired_id)
    assert expired.valid_from <= corpus.expired_still_live_ts < expired.valid_until
    assert not (expired.valid_from <= corpus.now_ts < (expired.valid_until or math.inf))


def test_relates_topology_matches_the_planted_two_hop_shape(corpus) -> None:
    out: dict[str, set[str]] = {}
    for edge in corpus.edges:
        if edge.relationship == "RELATES":
            out.setdefault(edge.source_id, set()).add(edge.target_id)
    one_hop = out.get(corpus.session_id, set())
    two_hop = one_hop | {t for src in one_hop for t in out.get(src, set())}
    assert one_hop == {corpus.target_id, corpus.chatter_id}
    assert two_hop == {corpus.target_id, corpus.chatter_id, corpus.lex_only_id}
    assert corpus.vec_only_id not in two_hop and corpus.expired_id not in two_hop


def test_analytics_fan_in_and_centrality_are_exact(corpus) -> None:
    in_degree: dict[str, int] = {}
    out_degree: dict[str, int] = {}
    for edge in corpus.edges:
        out_degree[edge.source_id] = out_degree.get(edge.source_id, 0) + 1
        in_degree[edge.target_id] = in_degree.get(edge.target_id, 0) + 1
    hub, spokes = corpus.analytics.hub_id, corpus.analytics.spoke_ids
    assert in_degree.get(hub, 0) == len(spokes)
    assert out_degree.get(hub, 0) == 0
    for spoke in spokes:
        assert out_degree.get(spoke, 0) == 1 and in_degree.get(spoke, 0) == 0

    # The served graph also holds the two class nodes the OWL axiom's
    # `rdfs:subClassOf` triple lowers to (`ONTOLOGY_NODE_IDS`; see its
    # docstring in `graph_corpus.py`) -- `corpus.nodes` alone (the explicitly
    # planted property-graph nodes) undercounts the real graph by exactly that
    # many.
    denom = len(corpus.nodes) + len(ONTOLOGY_NODE_IDS) - 1
    assert corpus.analytics.expected_hub_centrality == len(spokes) / denom
    assert corpus.analytics.expected_spoke_centrality == 1 / denom
    assert (
        corpus.analytics.expected_hub_centrality
        > corpus.analytics.expected_spoke_centrality
    )


def test_series_expected_mean_matches_an_independent_average(corpus) -> None:
    values = [value for _, value in corpus.series.points]
    assert corpus.series.expected_mean == sum(values) / len(values)
    assert corpus.series.window_to - corpus.series.window_from == corpus.series.width_ns


def test_ontology_turtle_declares_the_episode_subclass_axiom(corpus) -> None:
    assert "<http://mem/Episode>" in corpus.ontology_turtle
    assert "<http://mem/Memory>" in corpus.ontology_turtle
    assert "subClassOf" in corpus.ontology_turtle


def test_strict_node_model_refuses_bad_input(corpus) -> None:
    from pydantic import ValidationError

    from epistemic_graph.testing.synthetic.graph_corpus import CorpusNode

    dumped = corpus.nodes[0].model_dump()
    with pytest.raises(ValidationError):
        CorpusNode.model_validate(dumped | {"unexpected": 1})
