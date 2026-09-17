"""E2E: a generated multi-modal corpus, driven through the REAL served engine.

Every scenario below goes through the SAME path a real client uses: connect
over the socket (`clean_graph`, `tests/conftest.py`), stage a cross-modal
transaction (`ingest_memory_corpus`), commit it, then issue signed
`UnifiedQuery`/analytics/time-series RPCs and assert on CONCRETE values the
generator (`epistemic_graph.testing.synthetic.graph_corpus_generate`) planted
and knows the answer to -- never "did not error". This is the served-path
counterpart to the library-internal `tests/usecase_agent_memory.rs` fixture
(CONCEPT:EG-KG.query.usecase-agent-memory); see that file's fixture docstring
for the ranking-signal design this corpus reproduces.
"""

from __future__ import annotations

from epistemic_graph.testing.synthetic.graph_corpus import (
    MEMORY_CLASS_IRI,
    QUERY_TEXT,
    QUERY_VECTOR,
)
from epistemic_graph.testing.synthetic.graph_corpus_generate import (
    generate_memory_corpus,
)
from epistemic_graph.testing.synthetic.graph_corpus_ingest import ingest_memory_corpus

SEED = 20260917


def _episode_scan() -> list[dict[str, object]]:
    return [{"Scan": {"label": "Episode"}}]


def test_single_modality_baselines_land_on_their_planted_specialist(clean_graph):
    """Vector-alone and BM25-alone each top the node planted to win THAT one
    signal, proving both legs are wired end to end through real dispatch before
    the fused scenario composes them.

    PLANTED-BUG CATCH: a regression of the served BM25 binding back to its
    historical zero-hits behaviour (CONCEPT:EG-KG.query.served-text-index-
    unbound-finding) makes `RankText` return every episode UNSCORED (tied),
    so `lex_only_id` is no longer the unique top-1 and this assertion fails.
    A regression in `Rank`'s cosine math (e.g. comparing squared distance
    instead of cosine, or an unnormalized dot product) analogously breaks the
    vector assertion.
    """
    gc = clean_graph
    corpus = generate_memory_corpus(SEED)
    assert ingest_memory_corpus(gc, corpus) is True

    vec_ids = [
        row["id"]
        for row in gc.query.unified(
            _episode_scan()
            + [{"Rank": {"query": list(QUERY_VECTOR)}}, {"Limit": {"k": 5}}]
        )
    ]
    assert vec_ids[0] == corpus.vec_only_id, vec_ids

    text_ids = [
        row["id"]
        for row in gc.query.unified(
            _episode_scan() + [{"RankText": {"query": QUERY_TEXT}}, {"Limit": {"k": 5}}]
        )
    ]
    assert text_ids[0] == corpus.lex_only_id, text_ids


def test_fused_pipeline_honors_owl_and_bitemporal_asof(clean_graph):
    """ONE served plan fuses vector + BM25 + graph proximity (RRF), narrows to
    OWL-inferred `Memory` members, then pins the result to what was VALID at a
    given instant -- the full retrieval stack in a single `UnifiedQuery` RPC.

    PLANTED-BUG CATCH (three independent, in one plan):
      1. OWL: if `Op::Reason` stops bridging the property-graph `type` string to
         its IRI class (or the subclass axiom fails to materialize), the
         `Chatter`-typed node is never excluded and `chatter_id` leaks into the
         result.
      2. AS OF: if the bi-temporal filter used the wrong axis, ignored
         `valid_until`, or applied an off-by-one boundary, `expired_id` would
         either wrongly survive at `now_ts` or wrongly stay absent at
         `expired_still_live_ts` -- both are asserted below.
      3. Fusion: if RRF degenerated to any single leg's raw order, the winner
         would equal that leg's own top-1 (`vec_only_id` or `lex_only_id`)
         instead of the doubly/graph-relevant `target_id` -- asserted by the
         explicit inequality checks.
    """
    gc = clean_graph
    corpus = generate_memory_corpus(SEED)
    assert ingest_memory_corpus(gc, corpus) is True

    def fused(ts: float) -> list[str]:
        plan = _episode_scan() + [
            {
                "FuseRrf": {
                    "branches": [
                        [{"Rank": {"query": list(QUERY_VECTOR)}}],
                        [{"RankText": {"query": QUERY_TEXT}}],
                        [{"RankNodeDistance": {"center": corpus.session_id}}],
                    ],
                    "k": 0.0,
                }
            },
            {"Reason": {"target_class": MEMORY_CLASS_IRI, "ontology": ""}},
            {"AsOf": {"ts": ts, "axis": "Valid"}},
            {"Limit": {"k": 5}},
        ]
        return [row["id"] for row in gc.query.unified(plan)]

    now = fused(corpus.now_ts)
    assert now[0] == corpus.target_id, now
    assert corpus.chatter_id not in now, (
        "the OWL Reason leg must drop the non-Memory Chatter node"
    )
    assert corpus.expired_id not in now, (
        "AS OF at now_ts must drop the closed validity window"
    )
    assert now[0] != corpus.vec_only_id and now[0] != corpus.lex_only_id, (
        "the fused winner must differ from every single-modality winner"
    )

    earlier = fused(corpus.expired_still_live_ts)
    assert corpus.expired_id in earlier, (
        "AS OF at the earlier instant must keep the still-live episode"
    )


def test_graph_traversal_hop_bound_matches_the_planted_topology(clean_graph):
    """`Op::Traverse`'s `min..=max` hop bound is exact against a known adjacency
    list: 1 hop from the focal session reaches exactly its direct neighbors; 2
    hops adds exactly the transitive one; nothing 3+ hops away (or never linked
    at all) ever appears.

    PLANTED-BUG CATCH: an off-by-one hop bound (e.g. `< max` instead of
    `<= max`) would silently drop `lex_only_id` from the 2-hop result; an
    unbounded/ignored `max` would instead leak an unrelated node -- both are
    directly excluded by the exact-set assertions.
    """
    gc = clean_graph
    corpus = generate_memory_corpus(SEED)
    assert ingest_memory_corpus(gc, corpus) is True

    def reachable(max_hops: int) -> set[str]:
        plan = [
            {"Scan": {"label": "Session"}},
            {"Traverse": {"rel": "RELATES", "min": 1, "max": max_hops}},
            {"Limit": {"k": 50}},
        ]
        return {row["id"] for row in gc.query.unified(plan)}

    assert reachable(1) == {corpus.target_id, corpus.chatter_id}
    two_hop = reachable(2)
    assert two_hop == {corpus.target_id, corpus.chatter_id, corpus.lex_only_id}
    assert corpus.vec_only_id not in two_hop
    assert corpus.expired_id not in two_hop
    assert corpus.noise_id not in two_hop


def test_analytics_and_timeseries_read_back_match_planted_values(clean_graph):
    """Analytics (degree centrality + PageRank) and the time-series window
    aggregate are read back and checked against the generator's own exact
    formulas -- the "analytics -> read back" tail of the pipeline.

    PLANTED-BUG CATCH: a degree-centrality miscount (e.g. counting an edge
    twice, or normalizing by the wrong node count) breaks the exact-value
    assertions on both the hub and every spoke; a PageRank implementation that
    ignores edge direction would fail to rank the hub above its own spokes; a
    `WindowAgg`/`ts.window` bucket-alignment bug changes the returned mean away
    from the independently summed value.
    """
    gc = clean_graph
    corpus = generate_memory_corpus(SEED)
    assert ingest_memory_corpus(gc, corpus) is True

    hub, spokes = corpus.analytics.hub_id, corpus.analytics.spoke_ids
    degrees = dict(gc.analytics.degree_centrality_all())
    assert degrees[hub] == corpus.analytics.expected_hub_centrality
    for spoke in spokes:
        assert degrees[spoke] == corpus.analytics.expected_spoke_centrality

    ranks = dict(gc.analytics.pagerank(damping=0.85, iterations=100))
    assert all(ranks[hub] > ranks[spoke] for spoke in spokes), (
        "the fan-in hub must outrank every one of its own spokes",
        ranks,
    )

    windows = gc.timeseries.window(
        corpus.series.series_id,
        corpus.series.window_from,
        corpus.series.window_to,
        corpus.series.width_ns,
        "mean",
    )
    assert len(windows) == 1, windows
    _bucket_start, mean, count = windows[0]
    assert count == len(corpus.series.points)
    assert mean == corpus.series.expected_mean

    # Read back the raw points too (the "-> read back" tail): every planted
    # (ts, value) pair round-trips through the served range scan unchanged.
    ranged = gc.timeseries.range(
        corpus.series.series_id, corpus.series.window_from, corpus.series.window_to
    )
    assert ranged == [(ts, [value]) for ts, value in corpus.series.points]
