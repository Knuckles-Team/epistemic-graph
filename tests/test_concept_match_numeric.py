"""Batch cosine matching uses the bundled EG numeric kernel."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.no_engine
pytest.importorskip("epistemic_graph.numeric")

from epistemic_graph.concept_match_derivation import top_k_cosine  # noqa: E402


def test_top_k_cosine_orders_and_thresholds():
    vectors = [("a", [1.0, 0.0]), ("b", [0.0, 1.0]), ("c", [1.0, 1.0])]
    matched = top_k_cosine([1.0, 0.0], vectors, k=2, threshold=0.5)
    assert [concept for concept, _ in matched] == ["a", "c"]
    assert matched[0][1] == pytest.approx(1.0)


def test_top_k_cosine_zero_vector_and_stable_tie_order():
    vectors = [("z", [1.0, 0.0]), ("a", [1.0, 0.0])]
    assert top_k_cosine([0.0, 0.0], vectors, k=2, threshold=0.0) == []
    assert [concept for concept, _ in top_k_cosine([1.0, 0.0], vectors, 2, 0)] == [
        "a",
        "z",
    ]
