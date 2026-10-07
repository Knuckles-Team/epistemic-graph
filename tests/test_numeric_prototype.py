"""Acceptance tests against the installed Rust numeric extension (no stand-in)."""

from __future__ import annotations

import importlib.machinery
import math

import epistemic_graph.numeric as numeric
import pytest

pytestmark = pytest.mark.no_engine


def test_uses_native_extension() -> None:
    assert numeric.__kernel__ == "eg-numeric"
    assert any(
        numeric.__file__.endswith(suffix)
        for suffix in importlib.machinery.EXTENSION_SUFFIXES
    )


@pytest.mark.parametrize(
    ("query", "rows", "expected"),
    [
        ([1.0, 0.0], [[3.0, 4.0], [1.0, 0.0], [2.0, 0.0]], (1, 1.0)),
        ([1.0, 0.0], [[3.0, 4.0]], (0, 0.6)),
        ([], [[1.0]], None),
        ([1.0], [], None),
        ([0.0], [[1.0, 2.0]], None),
        ([1.0, 0.0], [[], [0.0], [-1.0, 0.0], [0.0, 1.0]], None),
    ],
)
def test_matching(query: list[float], rows: list[list[float]], expected) -> None:
    result = numeric.best_cosine_prototype(query, rows)
    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


@pytest.mark.parametrize("rows", [[[1.0]], [[1.0, 0.0], [1.0]]])
def test_nonzero_ragged_rows_refused(rows: list[list[float]]) -> None:
    with pytest.raises(ValueError, match="shape mismatch"):
        numeric.best_cosine_prototype([1.0, 0.0], rows)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, 1e308, 1e-300])
def test_ieee_policy_matches_native_primitives(value: float) -> None:
    magnitude = numeric.norm([value])
    if magnitude != 0.0:
        score = numeric.dot([value], [value]) / (magnitude * magnitude)
        assert not score > 0.0
    assert numeric.best_cosine_prototype([value], [[value]]) is None
    assert numeric.best_cosine_prototype([1.0, 0.0], [[math.nan, 0.0], [1.0, 0.0]]) == (
        1,
        1.0,
    )


def test_downstream_label_mapping_and_keyword_call() -> None:
    labels = ["orthogonal", "winner", "tied"]
    match = numeric.best_cosine_prototype(
        query=(1.0, 0.0), prototypes=((0.0, 1.0), (1.0, 0.0), (2.0, 0.0))
    )
    assert match is not None
    index, score = match
    assert labels[index] == "winner"
    assert score == 1.0


@pytest.mark.parametrize(
    ("query_size", "row_size", "count"),
    [(0, 0, 1_000_001), (1_000_001, 0, 0), (1, 1_000_000, 1), (0, 500_001, 2)],
)
def test_aggregate_and_container_limits(
    query_size: int, row_size: int, count: int
) -> None:
    query = [0.0] * query_size
    rows = [[0.0] * row_size] * count
    with pytest.raises(ValueError, match="element limit"):
        numeric.best_cosine_prototype(query, rows)


def test_exact_aggregate_limit_is_accepted() -> None:
    assert numeric.best_cosine_prototype([0.0], [[0.0] * 999_999]) is None


@pytest.mark.parametrize("rows", ["", b"", bytearray(), {}, ["1"], [["1"]], [[[1.0]]]])
def test_invalid_containers_are_refused(rows) -> None:
    with pytest.raises((ValueError, TypeError)):
        numeric.best_cosine_prototype([1.0], rows)


@pytest.mark.parametrize(
    ("query", "rows"),
    [([1.0, 0.0], [[1e-300]]), ([1e-300], [[1.0, 0.0]])],
)
def test_norm_underflow_does_not_hide_shape_errors(query, rows) -> None:
    with pytest.raises(ValueError, match="shape mismatch"):
        numeric.best_cosine_prototype(query, rows)
