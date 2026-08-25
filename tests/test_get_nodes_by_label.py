"""NodeClient.list_by_label sends the bounded labeled-fetch RPC (CONCEPT:EG-KG.txn.per-graph-write-isolation)."""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import NodeClient

# Fake-client unit tests only -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


class _FakeClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return [("n1", {"type": "Agent"})]


@pytest.mark.asyncio
async def test_list_by_label_sends_bounded_rpc() -> None:
    fake = _FakeClient()
    nc = NodeClient(fake)  # type: ignore[arg-type]
    out = await nc.list_by_label("Agent", 5)
    assert fake.sent == [
        ("GetNodesByLabel", {"label": "Agent", "after": None, "limit": 5})
    ]
    assert out == [("n1", {"type": "Agent"})]


@pytest.mark.asyncio
async def test_list_by_label_default_limit_zero() -> None:
    fake = _FakeClient()
    nc = NodeClient(fake)  # type: ignore[arg-type]
    await nc.list_by_label("Concept")
    assert fake.sent == [
        ("GetNodesByLabel", {"label": "Concept", "after": None, "limit": 0})
    ]


@pytest.mark.asyncio
async def test_list_by_label_sends_exclusive_reconciliation_cursor() -> None:
    fake = _FakeClient()
    nc = NodeClient(fake)  # type: ignore[arg-type]
    await nc.list_by_label("SourceRecord", 1000, after="record:0099")
    assert fake.sent == [
        (
            "GetNodesByLabel",
            {
                "label": "SourceRecord",
                "after": "record:0099",
                "limit": 1000,
            },
        )
    ]
