"""Scoped topology retrieval contract without a live graph server."""

import pytest

from epistemic_graph.latent_topology import retrieve_latent_topology


def test_merges_bounded_hierarchy_reads_and_deduplicates() -> None:
    statements: list[str] = []

    def read(statement: str) -> list[dict]:
        statements.append(statement)
        if ":IS_A" in statement:
            return [
                {"n": {"id": "a", "name": "A", "importance_score": 0.8}},
                {"n": {"id": "b", "name": "B", "importance_score": 0.9}},
            ]
        return [
            {"n": {"id": "a", "name": "A", "importance_score": 0.8}},
            {"n": {"id": "c", "name": "C", "importance_score": 0.75}},
        ]

    result = retrieve_latent_topology("query", read=read, top_k=2)
    assert [row["id"] for row in result] == ["b", "a"]
    assert len(statements) == 2
    assert all(
        "*1..3" in statement and "LIMIT 2" in statement for statement in statements
    )


def test_rejects_invalid_bounds_before_a_read() -> None:
    def no_read(_statement: str) -> list[dict]:
        raise AssertionError("read should not run")

    with pytest.raises(ValueError, match="top_k"):
        retrieve_latent_topology("x", read=no_read, top_k=101)
    with pytest.raises(ValueError, match="routing_threshold"):
        retrieve_latent_topology("x", read=no_read, routing_threshold=float("nan"))


def test_read_failure_propagates_without_partial_result() -> None:
    calls = 0

    def read(_statement: str) -> list[dict]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise PermissionError("scope denied")
        return [{"n": {"id": "a", "importance_score": 0.9}}]

    with pytest.raises(PermissionError, match="scope denied"):
        retrieve_latent_topology("x", read=read)
