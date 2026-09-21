from __future__ import annotations

import asyncio

import pytest

from epistemic_graph.generated.agent_component import (
    AgentComponentKind,
    AgentComponentSearchPage,
    AgentComponentSearchRequest,
)
from epistemic_graph.generated.storage import send_agent_component_search

pytestmark = pytest.mark.no_engine


class _Client:
    def __init__(self) -> None:
        self.sent: tuple[str, object, object, object] | None = None

    async def _send(
        self,
        method: str,
        params: object,
        graph: object,
        *,
        idempotency_key: object,
    ) -> object:
        self.sent = method, params, graph, idempotency_key
        return {"entries": [], "next_cursor": "tenant-bound-page-2"}


def _request(**changes: object) -> AgentComponentSearchRequest:
    values: dict[str, object] = {
        "tenant_id": "tenant-a",
        "kinds": [AgentComponentKind.MCP_SERVER],
        "limit": 64,
    }
    values.update(changes)
    return AgentComponentSearchRequest.model_validate(values)


def test_kind_only_search_is_typed_and_uses_existing_method() -> None:
    client = _Client()
    result = asyncio.run(send_agent_component_search(client, _request(), "catalog"))

    assert isinstance(result, AgentComponentSearchPage)
    assert result.next_cursor == "tenant-bound-page-2"
    assert client.sent == (
        "AgentComponent",
        {
            "op": {
                "op": "search",
                "request": {
                    "tenant_id": "tenant-a",
                    "kinds": ["mcp_server"],
                    "limit": 64,
                },
            }
        },
        "catalog",
        None,
    )


@pytest.mark.parametrize(
    "search_request",
    [
        _request(kinds=[]),
        _request(task="eg:task/research"),
        _request(capabilities=["eg:capability/retrieval"]),
        _request(limit=0),
        _request(limit=257),
        _request(cursor="x" * 16_385),
    ],
)
def test_kind_only_adapter_enforces_selection_and_page_bounds(
    search_request: AgentComponentSearchRequest,
) -> None:
    with pytest.raises(ValueError):
        asyncio.run(send_agent_component_search(_Client(), search_request))
