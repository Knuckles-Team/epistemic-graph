"""ServerRegistryClient.register sends the RegisterServer RPC
(CONCEPT:EG-KG.sharding.server-registry, W2.5)."""

from __future__ import annotations

from typing import Any

import pytest
from _untyped import untyped

from epistemic_graph.client import EpistemicGraphClient, ServerRegistryClient
from epistemic_graph.generated._runtime import ContractViolation

# Pure client-side logic over a `_FakeClient` -- never a real connection.
pytestmark = pytest.mark.no_engine


class _FakeClient(EpistemicGraphClient):
    def __init__(self, result: Any = "ok") -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []
        self._result = result

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        self.sent.append((method, params))
        return self._result


@pytest.mark.asyncio
async def test_register_sends_bounded_rpc_with_no_resources() -> None:
    fake = _FakeClient()
    src = ServerRegistryClient(fake)
    out = await src.register("portainer-agent", "mcp-ref://deadbeef", ttl_secs=120)
    assert fake.sent == [
        (
            "RegisterServer",
            {
                "name": "portainer-agent",
                "url": "mcp-ref://deadbeef",
                "resources_json": "",
                "ttl_secs": 120,
            },
        )
    ]
    assert out is True


@pytest.mark.asyncio
async def test_register_encodes_resources_as_sorted_opaque_json() -> None:
    fake = _FakeClient()
    src = ServerRegistryClient(fake)
    await src.register(
        "graph-os",
        "mcp-ref://cafef00d",
        resources={"b": 2, "a": 1},
        ttl_secs=300,
    )
    assert fake.sent == [
        (
            "RegisterServer",
            {
                "name": "graph-os",
                "url": "mcp-ref://cafef00d",
                "resources_json": '{"a":1,"b":2}',
                "ttl_secs": 300,
            },
        )
    ]


@pytest.mark.asyncio
async def test_register_default_ttl_is_a_positive_heartbeat_interval() -> None:
    fake = _FakeClient()
    src = ServerRegistryClient(fake)
    await src.register("default-ttl-server", "mcp-ref://0000")
    ((_, params),) = fake.sent
    assert params is not None
    assert isinstance(params["ttl_secs"], int) and params["ttl_secs"] > 0


@pytest.mark.asyncio
async def test_register_rejects_empty_name() -> None:
    fake = _FakeClient()
    src = ServerRegistryClient(fake)
    with pytest.raises(ValueError):
        await src.register("", "mcp-ref://deadbeef")
    assert fake.sent == []


@pytest.mark.asyncio
async def test_register_rejects_empty_url() -> None:
    fake = _FakeClient()
    src = ServerRegistryClient(fake)
    with pytest.raises(ValueError):
        await src.register("some-server", "")
    assert fake.sent == []


@pytest.mark.asyncio
async def test_register_rejects_non_positive_ttl() -> None:
    fake = _FakeClient()
    src = ServerRegistryClient(fake)
    with pytest.raises(ValueError):
        await src.register("some-server", "mcp-ref://deadbeef", ttl_secs=0)
    with pytest.raises(ValueError):
        await src.register("some-server", "mcp-ref://deadbeef", ttl_secs=-5)
    assert fake.sent == []


@pytest.mark.asyncio
async def test_register_rejects_non_mapping_resources() -> None:
    fake = _FakeClient()
    src = ServerRegistryClient(fake)
    with pytest.raises(ValueError):
        await src.register(
            "some-server",
            "mcp-ref://deadbeef",
            resources=untyped(["not", "a", "dict"]),
        )
    assert fake.sent == []


@pytest.mark.asyncio
async def test_register_rejects_a_non_string_engine_result() -> None:
    fake = _FakeClient(result=False)
    src = ServerRegistryClient(fake)
    with pytest.raises(ContractViolation, match="ResultPayload::String"):
        await src.register("some-server", "mcp-ref://deadbeef")
