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


@pytest.mark.asyncio
@pytest.mark.parametrize("with_execution", [False, True])
async def test_unified_forwards_only_the_server_issued_execution(
    with_execution: bool,
) -> None:
    class _UnifiedClient(_FakeClient):
        async def _send(
            self,
            method: str,
            params: dict[str, Any] | None = None,
            graph: str | None = None,
            *,
            idempotency_key: str | None = None,
        ) -> Any:
            self.sent.append((method, params))
            return [["node-1", 0.5]]

    fake = _UnifiedClient()
    plan = [{"ForeignScan": {"source": {"Named": {"name": "tickets"}}, "join": False}}]
    execution = {"execution_id": 17, "query_digest": "0" * 64}
    result = await QueryClient(fake).unified(
        plan, foreign_execution=execution if with_execution else None
    )
    expected: dict[str, Any] = {"plan": {"ops": plan}}
    if with_execution:
        expected["foreign_execution"] = execution
    assert fake.sent == [("UnifiedQuery", expected)]
    assert result == [{"id": "node-1", "score": 0.5}]


@pytest.mark.asyncio
async def test_unified_forwards_a_bounded_group_and_rejects_conflicting_forms() -> None:
    class _UnifiedClient(_FakeClient):
        async def _send(
            self,
            method: str,
            params: dict[str, Any] | None = None,
            graph: str | None = None,
            *,
            idempotency_key: str | None = None,
        ) -> Any:
            self.sent.append((method, params))
            return []

    fake = _UnifiedClient()
    plan = [{"ForeignScan": {"source": {"Named": {"name": "first"}}, "join": False}}]
    executions = [
        {"execution_id": 1, "query_digest": "0" * 64},
        {"execution_id": 2, "query_digest": "0" * 64},
    ]
    await QueryClient(fake).unified(plan, foreign_executions=executions)
    assert fake.sent == [
        ("UnifiedQuery", {"plan": {"ops": plan}, "foreign_executions": executions})
    ]
    with pytest.raises(ValueError, match="mutually exclusive"):
        await QueryClient(fake).unified(
            plan, foreign_execution=executions[0], foreign_executions=executions
        )
    assert len(fake.sent) == 1
