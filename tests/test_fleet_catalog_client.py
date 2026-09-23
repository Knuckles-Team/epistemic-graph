from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

import epistemic_graph
from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.fleet_catalog import (
    FleetCatalogClient,
    FleetCatalogSnapshotError,
    fleet_row_id,
)

pytestmark = pytest.mark.no_engine

_DIGEST = "cd" * 32
_GRANT = "ef" * 32


def _acl() -> dict[str, Any]:
    return {
        "tenant_id": "tenant-a",
        "visibility": {"scope": "tenant"},
        "publisher": "principal:sha256:importer",
    }


def _tool(name: str) -> dict[str, Any]:
    return {
        "kind": "tool",
        "row": {
            "component": {
                "id": f"mcp:github/tool/{name}",
                "name": name,
                "description": f"{name} tool",
                "server_name": "github",
                "connector": "github",
                "enabled": True,
                "entry_revision": 1,
                "definition_digest": "sha256:" + "0" * 64,
                "acl": _acl(),
            },
            "input_schema_digest": None,
            "effect": "read",
            "tool_mode": "undeclared",
        },
    }


def _page(
    names: list[str], cursor: dict[str, Any] | None, total: int
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "tools",
        "rows": [_tool(name) for name in names],
        "total": total,
        "next_cursor": cursor,
        "commons_revision": 7,
        "snapshot_digest": _DIGEST,
        "observed_at_ms": 3,
    }


class _Transport:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        assert graph is None
        self.calls.append((method, params))
        return self.pages[len(self.calls) - 1]


def _client(pages: list[dict[str, Any]]) -> tuple[FleetCatalogClient, _Transport]:
    transport = _Transport(pages)
    return FleetCatalogClient(cast(EpistemicGraphClient, transport)), transport


def _cursor(after: str) -> dict[str, Any]:
    return {
        "after_name": after,
        "after_id": f"mcp:github/tool/{after}",
        "snapshot_digest": _DIGEST,
    }


def test_list_all_pages_one_digest_fenced_snapshot_through_the_list_op() -> None:
    client, transport = _client(
        [_page(["alpha"], _cursor("alpha"), 2), _page(["bravo"], None, 2)]
    )
    rows = asyncio.run(client.list_all("tools", grant_digests=[_GRANT], page_size=1))
    assert [fleet_row_id(row) for row in rows] == [
        "mcp:github/tool/alpha",
        "mcp:github/tool/bravo",
    ]
    method, params = transport.calls[1]
    assert method == "FleetCatalog"
    assert params is not None
    assert params["op"]["op"] == "list"
    request = params["op"]["request"]
    assert request["kind"] == "tools"
    assert request["grant_digests"] == [_GRANT]
    assert request["cursor"] == _cursor("alpha")


def test_list_all_refuses_a_snapshot_that_moved_between_pages() -> None:
    moved = _page(["bravo"], None, 2)
    moved["snapshot_digest"] = "ab" * 32
    client, _ = _client([_page(["alpha"], _cursor("alpha"), 2), moved])
    with pytest.raises(FleetCatalogSnapshotError):
        asyncio.run(client.list_all("tools", page_size=1))


def test_list_all_refuses_a_repeated_row() -> None:
    client, _ = _client(
        [_page(["alpha"], _cursor("alpha"), 2), _page(["alpha"], None, 2)]
    )
    with pytest.raises(FleetCatalogSnapshotError):
        asyncio.run(client.list_all("tools", page_size=1))


def test_writes_use_their_own_typed_operations() -> None:
    receipt = {
        "record_id": "srvobs:github:tenant_local",
        "revision": 1,
        "disposition": "written",
        "observed_at_ms": 5,
    }
    client, transport = _client([receipt, receipt])
    written = asyncio.run(
        client.record_discovery(
            epistemic_graph.FleetDiscoveryRecordRequest.model_validate(
                {
                    "server_name": "github",
                    "scope": {"authority": "tenant_local"},
                    "connector": "github",
                    "outcome": {"status": "unreachable", "error": "connection refused"},
                }
            )
        )
    )
    assert written.disposition == "written"
    assert transport.calls[0][1] is not None
    assert transport.calls[0][1]["op"]["op"] == "record_discovery"
    asyncio.run(
        client.set_override(
            epistemic_graph.FleetOverrideSetRequest.model_validate(
                {
                    "component_id": "mcp:skills/skill/triage",
                    "value": {"field": "skill_type", "skill_type": "workflow"},
                    "expected_revision": 0,
                }
            )
        )
    )
    op = transport.calls[1][1]
    assert op is not None
    assert op["op"]["op"] == "set_override"
    assert op["op"]["request"]["expected_revision"] == 0


def test_fleet_catalog_surface_is_a_wheel_root_export() -> None:
    for name in (
        "FleetCatalogClient",
        "FleetCatalogListRequest",
        "FleetCatalogPage",
        "FleetDiscoveryRecordRequest",
        "FleetWriteReceipt",
    ):
        assert name in epistemic_graph.__all__
        assert getattr(epistemic_graph, name) is not None
