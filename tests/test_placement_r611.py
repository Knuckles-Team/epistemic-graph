"""MEDULLA-PLACEMENT-R611: the client follows the engine's placement authority."""

from __future__ import annotations

import importlib
import types
from typing import Any

import pytest
from _client_fixtures import unused_reader, unused_writer

import medulla.client as client_module
from medulla import generated as _gen
from medulla import pool as pool_module
from medulla.client import MedullaClient
from medulla.placement import (
    PLACEMENT_STALE,
    Breaker,
    PlacementDirectory,
    PlacementStaleError,
    TenantPool,
    select_transport,
)


def _snapshot(epoch: int, endpoints: list[str], health: str = "healthy") -> dict:
    return {
        "cluster_id": "cluster-a",
        "membership_epoch": epoch,
        "placement_epoch": epoch,
        "groups": [
            {
                "group_id": 1,
                "leader_id": 1,
                "health": health,
                "members": [
                    {"node_id": i + 1, "client_endpoint": ep, "health": health}
                    for i, ep in enumerate(endpoints)
                ],
            }
        ],
    }


def _route(epoch: int, stale: bool = False) -> dict:
    return {
        "tenant_ref": "t",
        "partition_ref": "p",
        "group": 2,
        "epoch": epoch,
        "stale": stale,
        "placed": True,
        "endpoints": ["tcp://n2:9100"],
    }


class _FakeEngine:
    """Answers in order; a ValueError entry is raised like the real client's
    stale-snapshot check."""

    def __init__(self, snapshots: list[Any], routes: list[Any] | None = None) -> None:
        self._snapshots = list(snapshots)
        self._routes = list(routes or [])
        self.member_calls: list[int | None] = []
        self.route_calls: list[int] = []
        self.cluster_topology = types.SimpleNamespace(members=self._members)
        self.placement = types.SimpleNamespace(route=self._route)

    async def _members(self, *, min_placement_epoch: int | None = None) -> dict:
        self.member_calls.append(min_placement_epoch)
        answer = self._snapshots.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def _route(self, tenant: str, partition: str, client_epoch: int = 0) -> dict:
        self.route_calls.append(client_epoch)
        return self._routes.pop(0)


pytestmark = pytest.mark.no_engine


# --- R611.1 / R611.2: model and behaviour -----------------------------------


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.2")
def test_transport_selection_is_one_function() -> None:
    assert select_transport("/run/medulla.sock").kind == "unix"
    assert select_transport("unix:///run/medulla.sock").address == "/run/medulla.sock"
    assert select_transport("tcp://h:1").kind == "tcp"
    assert select_transport("tls://h:1").kind == "tls"
    assert select_transport("tls://h:1", client_cert=True).kind == "mtls"
    for bad in ("", "http://h:1", "tcp://"):
        with pytest.raises(ValueError):
            select_transport(bad)


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.2")
def test_breaker_opens_after_consecutive_failures_and_half_opens_on_timer() -> None:
    now = [0.0]
    breaker = Breaker(failure_threshold=2, reset_after_s=5.0, clock=lambda: now[0])
    breaker.record_failure()
    assert breaker.state == "closed"
    breaker.record_success()
    breaker.record_failure()
    assert breaker.state == "closed"  # success reset the count
    breaker.record_failure()
    assert breaker.state == "open" and not breaker.allow()
    now[0] = 4.999
    assert breaker.state == "open"
    now[0] = 5.0
    assert breaker.state == "half_open" and breaker.allow()
    breaker.record_failure()
    assert breaker.state == "open"
    now[0] = 10.0
    breaker.record_success()
    assert breaker.state == "closed"


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.2")
@pytest.mark.asyncio
async def test_tenant_pool_is_a_bounded_lru() -> None:
    closed: list[str] = []

    async def factory(tenant: str) -> str:
        return f"client-{tenant}"

    async def close(client: str) -> None:
        closed.append(client)

    pool: TenantPool[str] = TenantPool(factory, capacity=2, close=close)
    assert await pool.acquire("a") == "client-a"
    await pool.acquire("b")
    await pool.acquire("a")  # a is now most recent
    await pool.acquire("c")  # evicts b
    assert pool.tenants() == ["a", "c"]
    assert closed == ["client-b"] and pool.evictions == 1 and len(pool) == 2
    with pytest.raises(ValueError):
        await pool.acquire("")
    with pytest.raises(ValueError):
        TenantPool(factory, capacity=0)


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.2")
@pytest.mark.asyncio
async def test_route_is_the_engine_route_never_computed() -> None:
    engine = _FakeEngine([], [_route(3)])
    directory = PlacementDirectory(engine)  # type: ignore[arg-type]
    route = await directory.route("t", "p")
    assert route == _route(3)
    assert engine.route_calls == [0]
    assert directory.routing_epoch == 3


# --- R611.4: refusal --------------------------------------------------------


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.4")
@pytest.mark.asyncio
async def test_stale_snapshot_is_rejected_with_code_and_refetched() -> None:
    engine = _FakeEngine(
        [
            _snapshot(5, ["tcp://n1:9100"]),
            _snapshot(4, ["tcp://old:9100"]),
            _snapshot(6, ["tcp://n1:9100"]),
        ],
        [_route(5, stale=True), _route(6)],
    )
    directory = PlacementDirectory(engine)  # type: ignore[arg-type]
    await directory.refresh()
    snapshot = await directory.refresh()  # epoch 4 rejected, refetched at 6
    assert snapshot["placement_epoch"] == 6
    assert engine.member_calls == [0, 5, 5]
    route = await directory.route("t", "p")  # stale route rejected, refetched
    assert route["epoch"] == 6
    assert directory.metrics().stale_rejects == 2

    # Two stale answers in a row surface the stable code; nothing is accepted.
    engine = _FakeEngine(
        [
            _snapshot(5, ["tcp://n1:9100"]),
            ValueError("ClusterMembers placement snapshot is stale"),
            _snapshot(1, ["tcp://old:9100"]),
        ]
    )
    directory = PlacementDirectory(engine)  # type: ignore[arg-type]
    accepted = await directory.refresh()
    with pytest.raises(PlacementStaleError) as excinfo:
        await directory.refresh()
    assert excinfo.value.code == PLACEMENT_STALE
    assert str(excinfo.value).startswith(PLACEMENT_STALE)
    assert directory.routing_epoch == 5
    assert (await_snapshot := directory._snapshot) is accepted  # zero partial writes
    assert (
        await_snapshot["groups"][0]["members"][0]["client_endpoint"] == "tcp://n1:9100"
    )


# --- R611.5: evidence -------------------------------------------------------


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.5")
@pytest.mark.asyncio
async def test_metrics_after_one_success_and_one_refusal() -> None:
    async def factory(tenant: str) -> str:
        return tenant

    pool: TenantPool[str] = TenantPool(factory, capacity=1)
    engine = _FakeEngine(
        [_snapshot(2, ["tcp://n1:9100"]), _snapshot(1, ["x"]), _snapshot(1, ["x"])]
    )
    directory = PlacementDirectory(engine, tenant_pool=pool)  # type: ignore[arg-type]
    await directory.refresh()
    await pool.acquire("a")
    await pool.acquire("b")
    directory.breaker("tcp://n1:9100").record_failure()
    with pytest.raises(PlacementStaleError):
        await directory.refresh()
    metrics = directory.metrics()
    assert metrics.snapshots_accepted == 1
    assert metrics.stale_rejects == 1
    assert metrics.pool_size == 1 and metrics.evictions == 1
    assert metrics.breaker_state == {"tcp://n1:9100": "closed"}


# --- R611.3: wiring through the production client ---------------------------


def _context() -> dict[str, Any]:
    return {
        "principal": "subject-opaque",
        "tenant": "tenant-fixture",
        "audience": "engine-fixture",
        "agent_id": "agent-fixture",
        "roles": ["reader"],
        "scopes": ["cluster:topology-read", "cluster:placement-read"],
        "policy_version": "policy-v1",
        "delegation": ["subject-opaque", "agent-fixture"],
    }


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.3")
@pytest.mark.asyncio
async def test_client_shard_topology_and_route_go_through_generated_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent: list[str] = []

    async def send_members(client: Any, *args: Any, **kwargs: Any) -> Any:
        sent.append("ClusterMembers")
        return types.SimpleNamespace(
            payload=_snapshot(7, ["tcp://n1:9100", "tcp://n2:9100"])
        )

    async def send_route(client: Any, params: Any = None, *a: Any, **k: Any) -> Any:
        sent.append("PlacementRoute")
        assert params["request"]["client_epoch"] == 7
        return types.SimpleNamespace(payload=_route(7))

    monkeypatch.setattr(_gen.cluster, "send_cluster_members", send_members)
    monkeypatch.setattr(_gen.cluster, "send_placement_route", send_route)
    monkeypatch.setattr(
        client_module.ClusterTopologyClient,
        "_validate_and_verify",
        lambda self, answer, **_: answer,
    )
    monkeypatch.setattr(
        client_module, "_validate_placement_route", lambda answer, *_: answer
    )
    client = MedullaClient(
        unused_reader(),
        unused_writer(),
        "fixture-secret",
        "graph-fixture",
        verified_context=_context(),
    )
    topology = await client.placement_directory.shard_topology(
        local_endpoint="tcp://n1:9100"
    )
    assert topology["mode"] == "sharded"
    assert topology["placement_epoch"] == 7
    assert [e["endpoint"] for e in topology["endpoints"]] == [
        "tcp://n1:9100",
        "tcp://n2:9100",
    ]
    assert topology["endpoints"][0]["local"] is True
    assert topology["endpoints"][1]["local"] is False
    assert all(
        e["reachable"] and e["breaker"] == "closed" for e in topology["endpoints"]
    )
    route = await client.placement_directory.route("t", "p")
    assert route["group"] == 2
    assert sent == ["ClusterMembers", "PlacementRoute"]


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.3")
@pytest.mark.asyncio
async def test_connection_pool_uses_the_shared_transport_function(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    real = pool_module.select_transport

    def spy(endpoint: str, **kwargs: Any) -> Any:
        calls.append(endpoint)
        return real(endpoint, **kwargs)

    captured: dict[str, Any] = {}

    async def connect(**kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(pool_module, "select_transport", spy)
    monkeypatch.setattr(MedullaClient, "connect", connect)
    pool = pool_module.ConnectionPool(
        "unix:///run/medulla.sock",
        verified_context=_context(),
        auth_secret="s",
        max_size=1,
    )
    await pool._create_client()
    assert calls == ["unix:///run/medulla.sock"]
    assert captured["socket_path"] == "/run/medulla.sock"


@pytest.mark.spec("MEDULLA-PLACEMENT-R611.1")
@pytest.mark.asyncio
async def test_tenant_pool_is_the_native_eg_types_pool() -> None:
    """The client pool delegates to the medulla-types TenantPool: one implementation."""
    numeric = importlib.import_module("medulla.numeric")

    async def factory(tenant: str) -> str:
        return f"client-{tenant}"

    pool: TenantPool[str] = TenantPool(factory, capacity=1)
    assert isinstance(pool._pool, numeric.TenantPool)
    await pool.acquire("a")
    await pool.acquire("b")
    assert pool._pool.tenants() == ["b"] and pool._pool.evictions == 1
    with pytest.raises(ValueError, match="tenant"):
        await pool.acquire("bad\x00")
