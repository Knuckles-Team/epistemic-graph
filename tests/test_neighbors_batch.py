"""NodeClient.neighbors_batch sends ONE RPC for many nodes (D-DPF-1).

D-DPF-1: a live-pod profile showed `nodes.neighbors` called once PER base node
during grounding compile (40 calls totaling 272.63s). `properties_batch` /
`has_batch` already collapse the equivalent per-node calls for properties and
existence checks into one round-trip each; `neighbors_batch` closes the same
gap for neighbor reads. These tests pin the "one call, not N" contract at the
client boundary — a client-side per-id loop reintroducing the N+1 would fail
`test_neighbors_batch_sends_one_rpc_for_many_ids` even though the RETURNED
VALUES would still look correct, which is exactly the class of regression a
plain correctness test would miss.
"""

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


@pytest.mark.asyncio
async def test_neighbors_batch_sends_one_rpc_for_many_ids() -> None:
    """The regression test: N ids must cost exactly ONE `_send` call."""
    fake = _FakeClient(ret=[["a", ["b"]], ["b", ["a", "c"]], ["missing", []]])
    nc = NodeClient(fake)  # type: ignore[arg-type]
    out = await nc.neighbors_batch(["a", "b", "missing"])

    assert len(fake.sent) == 1, (
        "neighbors_batch must issue exactly one RPC regardless of how many "
        "ids are requested — a per-id loop reintroducing the N+1 would send "
        "len(node_ids) calls instead"
    )
    assert fake.sent == [
        ("GetNeighborsBatch", {"node_ids": ["a", "b", "missing"]})
    ]
    assert out == {"a": ["b"], "b": ["a", "c"], "missing": []}


@pytest.mark.asyncio
async def test_neighbors_batch_empty_input_sends_one_rpc_and_returns_empty() -> None:
    fake = _FakeClient(ret=[])
    nc = NodeClient(fake)  # type: ignore[arg-type]
    out = await nc.neighbors_batch([])
    assert len(fake.sent) == 1
    assert out == {}


@pytest.mark.asyncio
async def test_neighbors_batch_preserves_input_order_in_the_request() -> None:
    fake = _FakeClient(ret=[])
    nc = NodeClient(fake)  # type: ignore[arg-type]
    await nc.neighbors_batch(["z", "a", "m"])
    assert fake.sent == [("GetNeighborsBatch", {"node_ids": ["z", "a", "m"]})]
