"""Client-side placement authority consumer (MEDULLA-PLACEMENT-R611).

The engine is the only placement authority. This module never computes a
route: it asks the engine for the member list (``ClusterMembers``) and for the
full route of a graph (``PlacementRoute``) through the generated client, keeps
the newest snapshot it has accepted, and refuses anything older than its
routing epoch with the stable code ``PLACEMENT_STALE`` (then refetches once).

It also owns the three client mechanics every production caller shares:

* :func:`select_transport` -- the one function that maps an endpoint to a
  local-socket, TLS or mTLS transport. :class:`~medulla.pool.ConnectionPool`
  uses it; no other caller parses endpoint schemes.
* :class:`Breaker` -- a per-endpoint circuit breaker that opens after
  consecutive failures and half-opens on a timer.
* :class:`TenantPool` -- a bounded LRU of per-tenant clients.

:class:`PlacementMetrics` is the evidence surface: breaker state, pool size,
evictions and stale rejects.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

if TYPE_CHECKING:
    from .client import MedullaClient

PLACEMENT_STALE = "PLACEMENT_STALE"

TransportKind = Literal["unix", "tcp", "tls", "mtls"]
BreakerState = Literal["closed", "open", "half_open"]


class PlacementStaleError(ValueError):
    """A snapshot or route older than the client's routing epoch."""

    code = PLACEMENT_STALE

    def __init__(self, detail: str) -> None:
        super().__init__(f"{PLACEMENT_STALE}: {detail}")
        self.detail = detail


@dataclass(frozen=True)
class Transport:
    """The transport an endpoint selects."""

    kind: TransportKind
    address: str


def select_transport(endpoint: str, *, client_cert: bool = False) -> Transport:
    """Map an endpoint to its transport. The only endpoint-scheme parser.

    ``unix://path`` or a bare path selects the local socket; ``tcp://host:port``
    selects plain TCP; ``tls://host:port`` selects TLS, upgraded to mTLS when
    the caller presents a client certificate.
    """
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ValueError("endpoint must be a non-empty string")
    if endpoint.startswith("tls://"):
        address = endpoint[len("tls://") :]
        kind: TransportKind = "mtls" if client_cert else "tls"
    elif endpoint.startswith("tcp://"):
        address = endpoint[len("tcp://") :]
        kind = "tcp"
    elif endpoint.startswith("unix://"):
        address = endpoint[len("unix://") :]
        kind = "unix"
    elif "://" in endpoint:
        raise ValueError(f"unsupported endpoint scheme: {endpoint.split('://')[0]}")
    else:
        address = endpoint
        kind = "unix"
    if not address:
        raise ValueError("endpoint has no address")
    return Transport(kind=kind, address=address)


class Breaker:
    """Closed -> open after ``failure_threshold`` consecutive failures;
    open -> half-open once ``reset_after_s`` has elapsed; a half-open trial
    success closes it, a failure re-opens it."""

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        reset_after_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if reset_after_s <= 0:
            raise ValueError("reset_after_s must be positive")
        self._threshold = failure_threshold
        self._reset_after = reset_after_s
        self._clock = clock
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> BreakerState:
        if self._opened_at is None:
            return "closed"
        if self._clock() - self._opened_at >= self._reset_after:
            return "half_open"
        return "open"

    def allow(self) -> bool:
        return self.state != "open"

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        if self.state == "half_open":
            self._opened_at = self._clock()
            return
        self._failures += 1
        if self._failures >= self._threshold:
            self._opened_at = self._clock()


ClientT = TypeVar("ClientT")


class TenantPool(Generic[ClientT]):
    """A bounded LRU of per-tenant clients. The least recently used client is
    closed and evicted when a new tenant would exceed ``capacity``.

    Hits, recency and eviction are decided by the engine's own ``TenantPool``
    (medulla-types, through the bundled native module) so one implementation
    exists; this wrapper only builds and closes the async clients."""

    def __init__(
        self,
        factory: Callable[[str], Awaitable[ClientT]],
        *,
        capacity: int = 64,
        close: Callable[[ClientT], Awaitable[None]] | None = None,
    ) -> None:
        from .client import _native_numeric

        self._factory = factory
        self._close = close
        self._pool = _native_numeric("the tenant client pool").TenantPool(capacity)

    def __len__(self) -> int:
        return len(self._pool)

    @property
    def evictions(self) -> int:
        return int(self._pool.evictions)

    async def acquire(self, tenant: str) -> ClientT:
        if not isinstance(tenant, str):
            raise ValueError("tenant must be a non-empty string")
        client: ClientT | None = self._pool.get(tenant)
        if client is not None:
            return client
        client = await self._factory(tenant)
        evicted = self._pool.insert(tenant, client)
        if evicted is not None and self._close is not None:
            await self._close(evicted[1])
        return client

    def tenants(self) -> list[str]:
        return list(self._pool.tenants())


@dataclass
class PlacementMetrics:
    """Evidence: breaker state per endpoint, pool size, evictions, stale rejects."""

    breaker_state: dict[str, BreakerState] = field(default_factory=dict)
    pool_size: int = 0
    evictions: int = 0
    stale_rejects: int = 0
    snapshots_accepted: int = 0
    routes_accepted: int = 0


class PlacementDirectory:
    """The engine-authoritative topology and route view one client holds."""

    def __init__(
        self,
        client: MedullaClient,
        *,
        breaker_factory: Callable[[], Breaker] = Breaker,
        tenant_pool: TenantPool[Any] | None = None,
    ) -> None:
        self._client = client
        self._breaker_factory = breaker_factory
        self._breakers: dict[str, Breaker] = {}
        self._tenant_pool = tenant_pool
        self._snapshot: dict[str, Any] | None = None
        self._routing_epoch = 0
        self._stale_rejects = 0
        self._snapshots_accepted = 0
        self._routes_accepted = 0

    @property
    def routing_epoch(self) -> int:
        return self._routing_epoch

    def breaker(self, endpoint: str) -> Breaker:
        breaker = self._breakers.get(endpoint)
        if breaker is None:
            breaker = self._breaker_factory()
            self._breakers[endpoint] = breaker
        return breaker

    async def _fetch_snapshot(self) -> dict[str, Any]:
        try:
            snapshot = await self._client.cluster_topology.members(
                min_placement_epoch=self._routing_epoch
            )
        except ValueError as exc:
            if "stale" in str(exc) or "backwards" in str(exc):
                raise PlacementStaleError(str(exc)) from exc
            raise
        placement_epoch = snapshot["placement_epoch"]
        if placement_epoch < self._routing_epoch:
            raise PlacementStaleError(
                f"snapshot epoch {placement_epoch} < routing epoch "
                f"{self._routing_epoch}"
            )
        return snapshot

    async def refresh(self) -> dict[str, Any]:
        """Fetch the member list; a stale answer is rejected and refetched once."""
        try:
            snapshot = await self._fetch_snapshot()
        except PlacementStaleError:
            self._stale_rejects += 1
            snapshot = await self._fetch_snapshot()
        self._snapshot = snapshot
        self._routing_epoch = max(self._routing_epoch, snapshot["placement_epoch"])
        self._snapshots_accepted += 1
        return snapshot

    async def _fetch_route(self, tenant: str, partition: str) -> dict[str, Any]:
        route = await self._client.placement.route(
            tenant, partition, client_epoch=self._routing_epoch
        )
        if route.get("stale") or route["epoch"] < self._routing_epoch:
            raise PlacementStaleError(
                f"route epoch {route['epoch']} < routing epoch {self._routing_epoch}"
            )
        return route

    async def route(self, tenant: str, partition: str) -> dict[str, Any]:
        """The engine's full route for ``(tenant, partition)``. Never computed
        here. A stale route is rejected and refetched once."""
        try:
            route = await self._fetch_route(tenant, partition)
        except PlacementStaleError:
            self._stale_rejects += 1
            route = await self._fetch_route(tenant, partition)
        self._routing_epoch = max(self._routing_epoch, route["epoch"])
        self._routes_accepted += 1
        return route

    async def shard_topology(
        self, *, local_endpoint: str | None = None
    ) -> dict[str, Any]:
        """Shard topology for operator views: mode, endpoints with health and
        breaker state, and the epochs of the snapshot it came from."""
        snapshot = await self.refresh()
        endpoints: list[dict[str, Any]] = []
        seen: set[str] = set()
        for group in snapshot.get("groups", []):
            for member in group.get("members", []):
                endpoint = member["client_endpoint"]
                if endpoint in seen:
                    continue
                seen.add(endpoint)
                endpoints.append(
                    {
                        "endpoint": endpoint,
                        "node_id": str(member["node_id"]),
                        "local": endpoint == local_endpoint,
                        "reachable": member.get("health") == "healthy",
                        "breaker": self.breaker(endpoint).state,
                    }
                )
        groups = len(snapshot.get("groups", []))
        return {
            "mode": "sharded" if groups > 1 or len(endpoints) > 1 else "single",
            "cluster_id": snapshot["cluster_id"],
            "membership_epoch": snapshot["membership_epoch"],
            "placement_epoch": snapshot["placement_epoch"],
            "groups": groups,
            "endpoints": endpoints,
        }

    def metrics(self) -> PlacementMetrics:
        pool = self._tenant_pool
        return PlacementMetrics(
            breaker_state={ep: b.state for ep, b in self._breakers.items()},
            pool_size=len(pool) if pool is not None else 0,
            evictions=pool.evictions if pool is not None else 0,
            stale_rejects=self._stale_rejects,
            snapshots_accepted=self._snapshots_accepted,
            routes_accepted=self._routes_accepted,
        )
