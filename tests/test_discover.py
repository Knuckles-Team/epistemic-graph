"""``GraphOperationsClient.discover`` sends the one-round-trip Discover RPC
(CONCEPT:EG-KG.retrieval.one-round-trip-discovery) — hybrid keyword + semantic discovery that returns hydrated
node text.

The engine-side ranking (HNSW dense retrieval + keyword re-rank over
``name``/``description``/``type`` + text hydration) is covered by the Rust
dispatch test ``test_discover_blends_keyword_and_semantic``; this asserts the
Python client marshals the keyword set, embedding, and ``k`` into exactly one
``Discover`` call and returns the hydrated rows unchanged.
"""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import GraphOperationsClient


class _FakeClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return [
            {
                "id": "deploy_runbook",
                "name": "Deploy Runbook",
                "description": "how to deploy a service",
                "type": "doc",
                "score": 0.87,
            }
        ]


@pytest.mark.asyncio
async def test_discover_sends_one_rpc() -> None:
    fake = _FakeClient()
    gc = GraphOperationsClient(fake)  # type: ignore[arg-type]
    out = await gc.discover(["deploy", "service"], [0.1, 0.2, 0.3], k=7)
    assert fake.sent == [
        (
            "Discover",
            {
                "keywords": ["deploy", "service"],
                "query_embedding": [0.1, 0.2, 0.3],
                "k": 7,
            },
        )
    ]
    assert out[0]["id"] == "deploy_runbook"
    assert out[0]["name"] == "Deploy Runbook"
    assert out[0]["type"] == "doc"
    assert "score" in out[0]


@pytest.mark.asyncio
async def test_discover_empty_embedding_keyword_only() -> None:
    """An empty embedding (embedder/vLLM unavailable) is a valid keyword-only call
    and defaults ``k`` to 5."""
    fake = _FakeClient()
    gc = GraphOperationsClient(fake)  # type: ignore[arg-type]
    await gc.discover(["deploy"], [])
    assert fake.sent[0][0] == "Discover"
    assert fake.sent[0][1] is not None
    assert fake.sent[0][1]["query_embedding"] == []
    assert fake.sent[0][1]["k"] == 5
