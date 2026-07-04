"""NodeClient cross-graph union reads send the union RPCs (CONCEPT:EG-KG.query.cross-graph-union)."""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import NodeClient


class _FakeClient:
    def __init__(self, ret: Any = None) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []
        self._ret = ret

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return self._ret


GRAPHS = ["__commons__", "__ingest__"]


@pytest.mark.asyncio
async def test_properties_union_sends_rpc() -> None:
    fake = _FakeClient(ret=None)
    nc = NodeClient(fake)  # type: ignore[arg-type]
    out = await nc.properties_union("B", GRAPHS)
    assert fake.sent == [
        ("UnionGetNodeProperties", {"graphs": GRAPHS, "node_id": "B"})
    ]
    assert out is None


@pytest.mark.asyncio
async def test_properties_union_unpacks_bytes() -> None:
    import msgpack

    fake = _FakeClient(ret=msgpack.packb({"type": "Doc"}))
    nc = NodeClient(fake)  # type: ignore[arg-type]
    out = await nc.properties_union("B", GRAPHS)
    assert out == {"type": "Doc"}


@pytest.mark.asyncio
async def test_list_by_label_union_sends_rpc() -> None:
    fake = _FakeClient(ret=[("A", {"type": "Doc"}), ("B", {"type": "Doc"})])
    nc = NodeClient(fake)  # type: ignore[arg-type]
    out = await nc.list_by_label_union("Doc", GRAPHS, 5)
    assert fake.sent == [
        ("UnionGetNodesByLabel", {"graphs": GRAPHS, "label": "Doc", "limit": 5})
    ]
    assert {k for k, _ in out} == {"A", "B"}


@pytest.mark.asyncio
async def test_neighbors_union_sends_rpc() -> None:
    fake = _FakeClient(ret=["x", "y"])
    nc = NodeClient(fake)  # type: ignore[arg-type]
    out = await nc.neighbors_union("A", GRAPHS)
    assert fake.sent == [("UnionGetNeighbors", {"graphs": GRAPHS, "node_id": "A"})]
    assert out == ["x", "y"]
