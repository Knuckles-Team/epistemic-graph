"""Parity contracts for the EG-owned deterministic hybrid scorer (EH-509)."""

from __future__ import annotations

import pytest

from epistemic_graph.hybrid_scoring import (
    HybridScoreConfig,
    cosine_similarity,
    score_documents,
    split_compound_name,
)


def test_compound_terms_and_weighted_scores() -> None:
    assert split_compound_name("MyClass_withMethod") == {
        "my",
        "class",
        "with",
        "method",
    }
    rows = score_documents(
        "alpha beta",
        [1.0, 0.0],
        [
            {"id": "match", "text": "alpha beta", "embedding": [1.0, 0.0]},
            {"id": "other", "text": "gamma", "embedding": [0.0, 1.0]},
        ],
    )
    assert [row["id"] for row in rows] == ["match"]
    assert rows[0]["semantic_score"] == 1.0
    assert rows[0]["keyword_score"] == 0.8
    assert rows[0]["combined_score"] == 0.944


def test_symbols_threshold_and_stable_ties() -> None:
    rows = score_documents(
        "registry node",
        [],
        [
            {"id": "first", "text": "", "symbols": ["RegistryNode"]},
            {"id": "second", "text": "", "symbols": ["RegistryNode"]},
        ],
        HybridScoreConfig(min_keyword_score=0.2, top_k=2),
    )
    assert [row["id"] for row in rows] == ["first", "second"]
    assert rows[0]["matched_symbols"] == ["RegistryNode"]
    assert rows[0]["keyword_score"] == 0.2


def test_cosine_zero_and_dimension_mismatch() -> None:
    assert cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0
    with pytest.raises(ValueError, match="embedding dimensions differ"):
        cosine_similarity([1.0], [1.0, 2.0])
