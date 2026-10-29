"""UQL forwards a server-issued foreign-read execution reference unchanged."""

from __future__ import annotations

from typing import Any

import pytest

from medulla.client import EpistemicGraphClient, QueryClient

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
        return {"Rows": {"columns": [], "rows": [], "warnings": []}}


@pytest.mark.asyncio
async def test_uql_forwards_server_issued_foreign_execution() -> None:
    fake = _FakeClient()
    execution = {"execution_id": 17, "query_digest": "0" * 64}

    result = await QueryClient(fake).uql(
        "MATCH (:Issue) WHERE state = $state",
        {"state": "open"},
        foreign_execution=execution,
    )

    assert result == {"kind": "rows", "columns": [], "rows": [], "warnings": []}
    assert fake.sent == [
        (
            "Uql",
            {
                "text": "MATCH (:Issue) WHERE state = $state",
                "params": {"state": {"Str": "open"}},
                "foreign_execution": execution,
            },
        )
    ]


@pytest.mark.asyncio
async def test_uql_omits_foreign_execution_by_default() -> None:
    fake = _FakeClient()

    await QueryClient(fake).uql("MATCH (:Issue)")

    assert fake.sent == [
        ("Uql", {"text": "MATCH (:Issue)", "params": {}}),
    ]


@pytest.mark.asyncio
async def test_uql_prepare_forwards_the_exact_executable_text() -> None:
    fake = _FakeClient()

    await QueryClient(fake).uql("MATCH (:Issue)", prepare=True)

    assert fake.sent == [
        ("Uql", {"text": "MATCH (:Issue)", "params": {}, "prepare": True}),
    ]
