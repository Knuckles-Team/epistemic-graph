from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from _client_fixtures import RecordingTransport, SentCall

import epistemic_graph
from epistemic_graph.client import EpistemicGraphClient, ServerRegistryClient

pytestmark = pytest.mark.no_engine

_DIGEST = "ab" * 32


def _entry(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "url": f"mcp-ref://{name}",
        "transport": "stdio",
        "desired": "enabled",
        "resources": {"tools": [name]},
        "ttl_secs": 60,
        "registered_at_ms": 1,
        "last_heartbeat_ms": 2,
        "lease_expires_at_ms": 60_002,
    }


class _RegistryTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None, str | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        assert idempotency_key is None
        self.calls.append((method, params, graph))
        request = (params or {})["request"]
        after = (request.get("cursor") or {}).get("after_name")
        if after is None:
            entries = [_entry("alpha")]
            next_cursor: dict[str, Any] | None = {
                "after_name": "alpha",
                "registry_revision": 7,
                "registry_digest": _DIGEST,
            }
        else:
            entries = [_entry("bravo")]
            next_cursor = None
        return {
            "schema_version": 1,
            "entries": entries,
            "next_cursor": next_cursor,
            "observed_at_ms": 3,
            "total_live": 2,
            "registry_revision": 7,
            "registry_digest": _DIGEST,
        }


def test_page_uses_only_the_generated_native_registry_method() -> None:
    transport = _RegistryTransport()
    client = ServerRegistryClient(cast(EpistemicGraphClient, transport))
    page = asyncio.run(client.page(limit=1))
    assert [entry.name for entry in page.entries] == ["alpha"]
    assert transport.calls == [
        ("ListRegisteredServers", {"request": {"limit": 1}}, None)
    ]


def test_list_all_exhausts_one_revision_and_digest_fenced_snapshot() -> None:
    transport = _RegistryTransport()
    client = ServerRegistryClient(cast(EpistemicGraphClient, transport))
    entries = asyncio.run(client.list_all(page_size=1))
    assert [entry.name for entry in entries] == ["alpha", "bravo"]
    assert len(transport.calls) == 2
    assert transport.calls[1][1] == {
        "request": {
            "limit": 1,
            "cursor": {
                "after_name": "alpha",
                "registry_revision": 7,
                "registry_digest": _DIGEST,
            },
        }
    }


def test_registry_types_and_client_are_wheel_root_exports() -> None:
    for name in (
        "RegisteredServerCursor",
        "RegisteredServerListPage",
        "RegisteredServerListRequest",
        "RegisteredServerView",
        "ServerRegistryClient",
    ):
        assert name in epistemic_graph.__all__
        assert getattr(epistemic_graph, name) is not None


class _RegisterTransport(RecordingTransport):
    def reply(self, call: SentCall) -> str:
        return "srv:alpha"


def test_register_sends_the_typed_transport_and_desired_state() -> None:
    transport = _RegisterTransport()
    client = ServerRegistryClient(cast(EpistemicGraphClient, transport))
    assert asyncio.run(
        client.register(
            "alpha",
            "mcp-ref://alpha",
            transport="streamable_http",
            desired="disabled",
        )
    )
    method, params, _graph, _key = transport.sent[0]
    assert method == "RegisterServer"
    assert params is not None
    assert params["transport"] == "streamable_http"
    assert params["desired"] == "disabled"


def test_registered_server_view_carries_the_typed_registration_claim() -> None:
    view = epistemic_graph.RegisteredServerView.model_validate(_entry("alpha"))
    assert view.transport == "stdio"
    assert view.desired == "enabled"
