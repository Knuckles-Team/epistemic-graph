"""QueryClient.cypher sends the CypherQuery RPC and zips Raw(QueryResult) rows
into dicts (CONCEPT:KG-2.179) — mirroring the SQL path, since both return the
identical wire shape."""

from __future__ import annotations

from typing import Any

import msgpack
import pytest

from epistemic_graph.client import QueryClient


class _FakeClient:
    """Mimics the engine's decoded `Raw(QueryResult)` reply: `_send` returns the
    already-double-unpacked dict `{"columns": [...], "rows": [<row-blob>, ...]}`
    where each row blob is a MessagePack list of cells (what the server emits)."""

    def __init__(self, columns: list[str], rows: list[list[Any]]) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []
        self._columns = columns
        self._rows = rows

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return {
            "columns": self._columns,
            "rows": [msgpack.packb(cells) for cells in self._rows],
        }


@pytest.mark.asyncio
async def test_cypher_sends_rpc_and_zips_rows() -> None:
    fake = _FakeClient(
        columns=["a", "b"],
        rows=[["alice", "bob"], ["bob", "carol"]],
    )
    qc = QueryClient(fake)  # type: ignore[arg-type]
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b"
    out = await qc.cypher(query)

    # RPC name + params are exactly what the engine expects (the CypherQuery
    # variant carries a single `query` string).
    assert fake.sent == [("CypherQuery", {"query": query})]
    # Rows zipped into RETURN-column-keyed dicts.
    assert out == [
        {"a": "alice", "b": "bob"},
        {"a": "bob", "b": "carol"},
    ]


@pytest.mark.asyncio
async def test_cypher_property_projection_and_limit() -> None:
    fake = _FakeClient(columns=["a.name"], rows=[["Alice"]])
    qc = QueryClient(fake)  # type: ignore[arg-type]
    query = "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a.name LIMIT 1"
    out = await qc.cypher(query)
    assert fake.sent == [("CypherQuery", {"query": query})]
    assert out == [{"a.name": "Alice"}]


@pytest.mark.asyncio
async def test_cypher_empty_result() -> None:
    fake = _FakeClient(columns=["a"], rows=[])
    qc = QueryClient(fake)  # type: ignore[arg-type]
    out = await qc.cypher("MATCH (a:Nonexistent) RETURN a")
    assert out == []
    assert fake.sent == [("CypherQuery", {"query": "MATCH (a:Nonexistent) RETURN a"})]
