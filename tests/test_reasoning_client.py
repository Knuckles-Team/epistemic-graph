"""Durable reasoning client methods expose the current fenced wire contract."""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import EpistemicGraphClient, QueryClient

# Fake-client unit tests only -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


class _FakeClient(EpistemicGraphClient):
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        self.sent.append((method, params))
        return {"ok": True}


@pytest.mark.asyncio
async def test_recompute_carries_the_source_graph_fence() -> None:
    fake = _FakeClient()
    query = QueryClient(fake)

    assert await query.recompute_materialization("derived", 41) == {"ok": True}
    assert fake.sent == [
        (
            "RecomputeMaterialization",
            {"derived_id": "derived", "expected_source_graph_version": 41},
        )
    ]


@pytest.mark.asyncio
async def test_reasoning_status_reads_use_the_durable_projection_methods() -> None:
    fake = _FakeClient()
    query = QueryClient(fake)

    await query.materialization_status("derived")
    await query.stale_materializations()
    assert fake.sent == [
        ("MaterializationStatus", {"id": "derived"}),
        ("StaleMaterializations", {}),
    ]
