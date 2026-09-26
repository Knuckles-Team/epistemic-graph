"""Unit tests for deterministic name resolution."""

from __future__ import annotations

import pytest

from epistemic_graph.name_resolution import (
    has_high_entropy,
    normalize_name,
    resolve_entities,
    shannon_entropy,
)

pytestmark = pytest.mark.no_engine


def test_normalize_folds_case_punctuation_and_suffixes():
    # "OpenAI", "Open AI, Inc." and "OPENAI" all canonicalize identically.
    assert normalize_name("OpenAI") == "openai"
    assert normalize_name("Open AI, Inc.") == "openai"
    assert normalize_name("OPENAI") == "openai"
    assert normalize_name("Acme Corporation") == "acme"
    # order is preserved — these must NOT collide
    assert normalize_name("Sun Microsystems") != normalize_name("Microsystems Sun")


def test_shannon_entropy_basic():
    assert shannon_entropy("") == 0.0
    assert shannon_entropy("aaaa") == 0.0  # zero entropy, all identical
    assert shannon_entropy("ab") == 1.0  # 1 bit
    assert shannon_entropy("openai") > 2.0


def test_entropy_gate_rejects_generic_names():
    assert not has_high_entropy("ai")  # too short
    assert not has_high_entropy("aaaa")  # zero entropy
    assert has_high_entropy("openai")
    assert has_high_entropy("kubernetes")


def test_exact_merge_no_embeddings():
    # Three spellings of one entity → one exact merge cluster, zero ML needed.
    res = resolve_entities(
        [("n1", "OpenAI"), ("n2", "Open AI, Inc."), ("n3", "OPENAI")]
    )
    assert res.exact_merges == 2  # n1-n2, n1-n3
    assert res.lsh_merges == 0
    assert res.resolved_ids == {"n1", "n2", "n3"}
    assert all(tier == "exact" for *_, tier in res.merge_pairs)
    # survivor is stable (first id seen for the key)
    survivors = {a for a, _b, _s, _t in res.merge_pairs}
    assert survivors == {"n1"}


def test_low_entropy_names_are_not_auto_merged():
    # Two nodes both vaguely named "System" / "Data" must NOT merge on name alone;
    # they fall through to the embedding tier as residual.
    res = resolve_entities([("a", "System"), ("b", "System"), ("c", "Data")])
    assert res.merge_pairs == []
    assert res.low_entropy == 3
    assert {"a", "b", "c"} <= res.residual_ids
    assert res.resolved_ids == set()


def test_lsh_merges_typo_variant_without_llm():
    # A single-character typo on a long, specific name → high Jaccard → LSH merge.
    res = resolve_entities(
        [
            ("k1", "Kubernetes Cluster Alpha"),
            ("k2", "Kubernetes Cluster Alphaa"),  # typo
        ]
    )
    assert res.exact_merges == 0
    assert res.lsh_merges == 1
    assert res.resolved_ids == {"k1", "k2"}
    assert res.merge_pairs[0][3] == "lsh"
    assert res.merge_pairs[0][2] >= 0.9


def test_distinct_entities_do_not_merge():
    res = resolve_entities(
        [("p", "PostgreSQL"), ("k", "Kubernetes"), ("r", "Redis Cache Layer")]
    )
    assert res.merge_pairs == []
    assert res.resolved_ids == set()
    assert {"p", "k", "r"} == res.residual_ids


def test_deterministic_across_runs():
    items = [("a", "Apache Cassandra DB"), ("b", "Apache Cassandra Db")]
    r1 = resolve_entities(items)
    r2 = resolve_entities(items)
    assert r1.merge_pairs == r2.merge_pairs
