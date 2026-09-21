from __future__ import annotations

import asyncio

import pytest

from epistemic_graph.generated.connector_pack import (
    ConnectorPackStatus,
    ConnectorPackStatusRequest,
)
from epistemic_graph.generated.storage import send_connector_pack_status

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
        digest = "11" * 32
        return {
            "schema_version": 2,
            "tenant_id": "tenant-a",
            "connector": "connector-a",
            "head": {
                "binding_revision": 4,
                "pack_digest": "22" * 32,
                "catalog": {
                    "configuration_revision": 8,
                    "catalog_generation": 12,
                    "snapshot_digest": digest,
                    "child_connection_generation": 3,
                    "authorization_scope_digest": "33" * 32,
                },
                "server_package_version": "2.0.0",
                "record_id": "pack:4",
                "committed_at_ms": 42,
            },
            "members": {"published": 2, "withdrawn": 0, "retired": 0},
            "warnings": [],
            "projection": {"projection": "none"},
        }


def test_status_query_returns_exact_durable_catalog_binding() -> None:
    client = _Client()
    request = ConnectorPackStatusRequest(tenant_id="tenant-a", connector="connector-a")
    result = asyncio.run(send_connector_pack_status(client, request, "catalog"))

    assert isinstance(result, ConnectorPackStatus)
    assert result.head is not None
    assert result.head.catalog.catalog_generation == 12
    assert result.head.catalog.snapshot_digest == "11" * 32
    assert client.sent == (
        "ConnectorPack",
        {
            "op": {
                "op": "status",
                "request": {
                    "tenant_id": "tenant-a",
                    "connector": "connector-a",
                },
            }
        },
        "catalog",
        None,
    )
