"""QueryClient.sql sends the Sql RPC and zips Raw(QueryResult) rows into dicts
(CONCEPT:EG-KG.query.read-only-sql-query)."""

from __future__ import annotations

from typing import Any

import msgpack
import pytest

from epistemic_graph.client import QueryClient

# Fake-client unit tests only -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


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
async def test_sql_sends_rpc_and_zips_rows() -> None:
    fake = _FakeClient(
        columns=["id", "rank"],
        rows=[["n2", 2], ["n3", 3]],
    )
    qc = QueryClient(fake)  # type: ignore[arg-type]
    out = await qc.sql("SELECT id, rank FROM nodes WHERE rank >= 2")

    # RPC name + params are exactly what the engine expects.
    assert fake.sent == [
        ("Sql", {"query": "SELECT id, rank FROM nodes WHERE rank >= 2"})
    ]
    # Rows zipped into column-keyed dicts.
    assert out == [
        {"id": "n2", "rank": 2},
        {"id": "n3", "rank": 3},
    ]


@pytest.mark.asyncio
async def test_sql_empty_result() -> None:
    fake = _FakeClient(columns=["id"], rows=[])
    qc = QueryClient(fake)  # type: ignore[arg-type]
    out = await qc.sql("SELECT id FROM nodes WHERE 1 = 0")
    assert out == []
    assert fake.sent == [("Sql", {"query": "SELECT id FROM nodes WHERE 1 = 0"})]
