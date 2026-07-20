"""Connection pooling and authoritative endpoint routing for epistemic-graph.

``ConnectionPool`` manages bounded connections to one service endpoint.
``ShardRouter`` accepts routes resolved by the engine placement authority; it
never hashes graph names or guesses placement.

Cross-graph union co-residency is placement metadata owned by the engine. The
client groups graphs by the endpoints returned by its authoritative resolver,
fans the existing per-shard union RPC out concurrently, and merges the results.
It never changes a route to manufacture affinity.

Single-endpoint homelab behavior is unchanged: with one endpoint every graph
co-resides trivially, so affinity is a no-op and scatter-gather degenerates to a
single per-shard call.

Failure mode (CONCEPT:EG-KG.ingest.ingest-lane-affinity): scatter-gather is **fail-loud per shard**. If any
shard's sub-union raises (e.g. one denied graph on that shard), the exception
propagates and the whole union fails — consistent with the engine's
fail-loud-per-shard durability contract. We do NOT silently degrade to a partial
union, because a caller cannot distinguish "node genuinely absent" from "a shard
was unreachable" in a deduped merge, and a silently partial union is a correctness
hazard. (A missing graph is still skipped engine-side, exactly as in the
single-shard union — that is an empty contribution, not an error.)

CONCEPT:EG-KG.backend.multiplexed-connections — bounded physical connections.

One ``EpistemicGraphClient`` already pipelines concurrent requests on a single
connection and demultiplexes out-of-order responses by ``Response.id``. A pool
adds bounded physical-connection isolation: a large response or failed stream
does not stall every caller, and independent work can use separate transport
flow-control windows. ``ConnectionPool.map_concurrent`` and
``ShardRouter.map_concurrent`` deliberately fan out only independent operations.
The pool auto-sizes to the box (``_auto_pool_size``); callers can set an explicit
cap when their deployment has a stricter connection budget.

Dependent operations remain sequential awaits inside one logical function. A
node write must complete before its edge writes begin; merely sharing a physical
connection is not an ordering primitive because current requests are pipelined.
"""

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from .client import (
    EpistemicGraphClient,
    RequestContextClaims,
    validate_request_context,
)

logger = logging.getLogger(__name__)


def _auto_pool_size() -> int:
    """Auto-size a per-endpoint connection pool to the box (CONCEPT:EG-KG.backend.multiplexed-connections).

    The pool gives concurrent shard writers and read fan-out independent physical
    flow-control windows while keeping the connection count bounded. Size by CPU
    count, clamped to a sane floor/ceiling. No environment knob: deployments that
    need a tighter budget pass an explicit cap.
    """
    cpu = os.cpu_count() or 4
    return max(8, min(2 * cpu, 64))


class ConnectionPool:
    """Async connection pool for EpistemicGraphClient instances."""

    def __init__(
        self,
        endpoint: str,
        *,
        verified_context: RequestContextClaims | dict[str, Any],
        auth_secret: str | None = None,
        min_size: int = 1,
        max_size: int | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.auth_secret = auth_secret or os.environ.get(
            "GRAPH_SERVICE_AUTH_SECRET", ""
        )
        if not self.auth_secret:
            raise ValueError("a non-empty authentication secret is required")
        self.verified_context = validate_request_context(verified_context)
        # CONCEPT:EG-KG.backend.multiplexed-connections — auto-size to the box when the caller doesn't pin a cap,
        # so M concurrent callers each get their own in-flight connection instead
        # of contending on one. An explicit cap is still honored.
        self.max_size = _auto_pool_size() if max_size is None else max_size
        self.min_size = min(min_size, self.max_size)
        self._pool: asyncio.Queue[EpistemicGraphClient] = asyncio.Queue(
            maxsize=self.max_size
        )
        self._active_connections = 0
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize the pool with min_size connections."""
        async with self._lock:
            for _ in range(self.min_size):
                client = await self._create_client()
                self._pool.put_nowait(client)

    async def _create_client(self) -> EpistemicGraphClient:
        if self.endpoint.startswith(("tcp://", "tls://")):
            use_tls = self.endpoint.startswith("tls://")
            tcp_addr = self.endpoint[6:]
            client = await EpistemicGraphClient.connect(
                tcp_addr=tcp_addr,
                auth_secret=self.auth_secret,
                verified_context=self.verified_context,
                tls=use_tls,
            )
        elif self.endpoint.startswith("unix://"):
            socket_path = self.endpoint[7:]
            client = await EpistemicGraphClient.connect(
                socket_path=socket_path,
                auth_secret=self.auth_secret,
                verified_context=self.verified_context,
            )
        else:
            # Default to socket if no scheme provided
            client = await EpistemicGraphClient.connect(
                socket_path=self.endpoint,
                auth_secret=self.auth_secret,
                verified_context=self.verified_context,
            )

        self._active_connections += 1
        return client

    async def acquire(self) -> EpistemicGraphClient:
        """Acquire a healthy connection from the pool."""
        try:
            client = self._pool.get_nowait()
            # Test health on checkout
            try:
                await client.ping()
            except Exception:
                logger.warning("Connection pool found dead connection, reopening...")
                async with self._lock:
                    self._active_connections -= 1
                client = await self._create_client()
            return client
        except asyncio.QueueEmpty:
            async with self._lock:
                if self._active_connections < self.max_size:
                    client = await self._create_client()
                    return client
            # Wait for an available connection
            client = await self._pool.get()
            return client

    def release(self, client: EpistemicGraphClient) -> None:
        """Release a connection back to the pool."""
        try:
            self._pool.put_nowait(client)
        except asyncio.QueueFull:
            # Shouldn't happen if managed correctly, but close excess
            asyncio.create_task(client.close())
            self._active_connections -= 1

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[EpistemicGraphClient]:
        """Acquire a connection for the ``with`` block, always releasing it.

        The leak-free way the hot path should hold a connection: ``async with
        pool.connection() as client: ...`` checks one out and returns it to the
        pool on exit (even on error), so a forgotten ``release`` can't strand a
        connection and starve the cap. A logical write that relies on ordering
        awaits each dependent operation before issuing the next inside this block.
        """
        client = await self.acquire()
        try:
            yield client
        finally:
            self.release(client)

    async def map_concurrent(
        self,
        ops: list[Callable[[EpistemicGraphClient], Awaitable[Any]]],
    ) -> list[Any]:
        """Run INDEPENDENT ``ops`` concurrently, each on its own connection
        (CONCEPT:EG-KG.backend.multiplexed-connections — parallelize the wire).

        Each ``fn`` is ``async (client) -> result``; every ``fn`` gets a distinct
        pooled connection, so the engine services them as parallel per-connection
        tasks instead of serializing them behind one. Results are returned in the
        SAME order as ``ops`` (``asyncio.gather`` order). With more ops than the
        pool cap, the surplus simply waits for a connection to free — correctness
        holds, parallelism is bounded by the cap.

        Use this ONLY for operations that are independent of each other. Anything
        that must be ordered relative to a sibling op (write-then-read,
        node-before-edge) belongs in a single ``fn`` on one connection, not split
        across two entries here.
        """

        async def _run(
            fn: Callable[[EpistemicGraphClient], Awaitable[Any]],
        ) -> Any:
            async with self.connection() as client:
                return await fn(client)

        return await asyncio.gather(*(_run(fn) for fn in ops))

    async def close_all(self) -> None:
        """Close all connections in the pool."""
        async with self._lock:
            while not self._pool.empty():
                client = self._pool.get_nowait()
                await client.close()
                self._active_connections -= 1


class ShardRouter:
    """Pool router driven by an engine-authoritative endpoint resolver."""

    def __init__(
        self,
        endpoints: list[str],
        *,
        verified_context: RequestContextClaims | dict[str, Any],
        auth_secret: str | None = None,
        min_size: int = 1,
        max_size: int | None = None,
        route_resolver: Callable[[str], str] | None = None,
    ) -> None:
        if not endpoints:
            raise ValueError("ShardRouter requires at least one endpoint")
        if len(endpoints) > 1 and route_resolver is None:
            raise ValueError(
                "multi-endpoint ShardRouter requires an authoritative route_resolver"
            )
        self.endpoints = endpoints
        self._route_resolver = route_resolver
        context = validate_request_context(verified_context)
        self.pools: dict[str, ConnectionPool] = {}
        for ep in endpoints:
            self.pools[ep] = ConnectionPool(
                ep,
                verified_context=context,
                auth_secret=auth_secret,
                min_size=min_size,
                max_size=max_size,
            )

    async def initialize(self) -> None:
        """Initialize all connection pools."""
        for pool in self.pools.values():
            await pool.initialize()

    def _get_shard_endpoint(self, graph_name: str) -> str:
        if len(self.endpoints) == 1:
            return self.endpoints[0]
        assert self._route_resolver is not None
        endpoint = self._route_resolver(graph_name)
        if endpoint not in self.pools:
            raise ValueError("authoritative route returned an unconfigured endpoint")
        return endpoint

    def group_by_shard(self, graph_names: list[str]) -> dict[str, list[str]]:
        """Group ``graph_names`` by their resolved shard endpoint (order-preserving).

        CONCEPT:EG-KG.ingest.ingest-lane-affinity — the partition a scatter-gather union fans out over.
        With affinity in play, a co-resident set collapses into one group, so the
        union takes a single per-shard call (the fast path); only graphs that
        genuinely live on different shards produce multiple groups.
        """
        groups: dict[str, list[str]] = {}
        for name in graph_names:
            ep = self._get_shard_endpoint(name)
            groups.setdefault(ep, []).append(name)
        return groups

    async def acquire(self, graph_name: str) -> EpistemicGraphClient:
        """Acquire a client for the specific graph."""
        ep = self._get_shard_endpoint(graph_name)
        client = await self.pools[ep].acquire()
        # Set the target graph for this checkout
        client._graph_name = graph_name
        return client

    def release(self, client: EpistemicGraphClient, graph_name: str) -> None:
        """Release a client back to its corresponding pool."""
        ep = self._get_shard_endpoint(graph_name)
        if ep in self.pools:
            self.pools[ep].release(client)
        else:
            asyncio.create_task(client.close())

    @contextlib.asynccontextmanager
    async def connection(self, graph_name: str) -> AsyncIterator[EpistemicGraphClient]:
        """Acquire ``graph_name``'s shard connection for the ``with`` block,
        always releasing it back to the right pool (CONCEPT:EG-KG.backend.multiplexed-connections).

        ``async with router.connection(graph) as client: ...`` — the leak-free way
        the hot write/read path holds a connection. Order-dependent operations on
        one graph (node-before-edge) are sequentially awaited inside one block.
        """
        ep = self._get_shard_endpoint(graph_name)
        client = await self.pools[ep].acquire()
        client._graph_name = graph_name
        try:
            yield client
        finally:
            self.pools[ep].release(client)

    async def map_concurrent(
        self,
        graph_name: str,
        ops: list[Callable[[EpistemicGraphClient], Awaitable[Any]]],
    ) -> list[Any]:
        """Run INDEPENDENT ``ops`` against ``graph_name`` concurrently, each on its
        own connection to that graph's shard (CONCEPT:EG-KG.backend.multiplexed-connections).

        The per-graph analogue of :meth:`ConnectionPool.map_concurrent`: all ``ops``
        target the same shard (so they land on the right writer) but each takes a
        distinct connection, so the engine services them as parallel tasks. Results
        keep ``ops`` order. Independent ops only — ordered siblings go in one ``fn``.
        """
        ep = self._get_shard_endpoint(graph_name)

        async def _run(
            fn: Callable[[EpistemicGraphClient], Awaitable[Any]],
        ) -> Any:
            async with self.pools[ep].connection() as client:
                client._graph_name = graph_name
                return await fn(client)

        return await asyncio.gather(*(_run(fn) for fn in ops))

    # ── Cross-shard scatter-gather union (CONCEPT:EG-KG.ingest.ingest-lane-affinity) ──────────────
    # Union across graphs that may live on DIFFERENT shards. Each per-shard
    # sub-union reuses the single-shard KG-2.171 RPC (no new engine method):
    # the group's graphs are co-resident on that shard, so the shard-local
    # handler unions them as usual. We then merge + dedupe across shards.

    async def _shard_union(
        self,
        endpoint: str,
        group: list[str],
        rpc: str,
        node_id: str | None,
        label: str | None,
        limit: int,
    ) -> Any:
        """Run one shard-local union RPC over ``group`` against ``endpoint``."""
        pool = self.pools[endpoint]
        client = await pool.acquire()
        # Route the request to THIS shard by binding to one of its graphs; the
        # union RPC re-enters the shard's registry for every graph in ``group``.
        client._graph_name = group[0]
        try:
            if rpc == "UnionGetNodeProperties":
                return await client.nodes.properties_union(node_id, group)  # type: ignore[arg-type]
            if rpc == "UnionGetNodesByLabel":
                return await client.nodes.list_by_label_union(label, group, limit)  # type: ignore[arg-type]
            if rpc == "UnionGetNeighbors":
                return await client.nodes.neighbors_union(node_id, group)  # type: ignore[arg-type]
            raise ValueError(f"unknown union rpc: {rpc}")
        finally:
            pool.release(client)

    async def properties_union(
        self, node_id: str, graphs: list[str]
    ) -> dict[str, Any] | None:
        """First-found node properties across ``graphs``, spanning shards.

        Preserves the single-shard union semantics: first non-null hit wins, in
        the caller's ``graphs`` order. Each shard returns its own first-found over
        its slice; we then re-apply graph order across shards to pick the global
        first. Fail-loud per shard (CONCEPT:EG-KG.ingest.ingest-lane-affinity).
        """
        groups = self.group_by_shard(graphs)
        if len(groups) == 1:
            ep, group = next(iter(groups.items()))
            return await self._shard_union(
                ep, group, "UnionGetNodeProperties", node_id, None, 0
            )
        # Order endpoints by the earliest graph each owns, so the global
        # first-found respects the caller's ``graphs`` order.
        order = {name: i for i, name in enumerate(graphs)}
        ordered_eps = sorted(
            groups.items(), key=lambda kv: min(order[g] for g in kv[1])
        )
        results = await asyncio.gather(
            *(
                self._shard_union(ep, group, "UnionGetNodeProperties", node_id, None, 0)
                for ep, group in ordered_eps
            )
        )
        for res in results:
            if res is not None:
                return res
        return None

    async def list_by_label_union(
        self, label: str, graphs: list[str], limit: int = 0
    ) -> list[tuple[str, Any]]:
        """Label scan unioned + deduped by id across ``graphs``, spanning shards.

        Each shard dedupes its own slice; we then dedupe again across shards by
        node id (first-found wins) and apply the global ``limit`` (0 ⇒ no cap).
        Fail-loud per shard (CONCEPT:EG-KG.ingest.ingest-lane-affinity).
        """
        groups = self.group_by_shard(graphs)
        per_shard = await asyncio.gather(
            *(
                self._shard_union(ep, group, "UnionGetNodesByLabel", None, label, 0)
                for ep, group in groups.items()
            )
        )
        seen: set[str] = set()
        merged: list[tuple[str, Any]] = []
        for rows in per_shard:
            for entry in rows or []:
                nid = entry[0]
                if nid in seen:
                    continue
                seen.add(nid)
                merged.append((nid, entry[1]))
                if limit and len(merged) >= limit:
                    return merged
        return merged

    async def neighbors_union(self, node_id: str, graphs: list[str]) -> list[str]:
        """Neighbour ids unioned + deduped across ``graphs``, spanning shards.

        Fail-loud per shard (CONCEPT:EG-KG.ingest.ingest-lane-affinity).
        """
        groups = self.group_by_shard(graphs)
        per_shard = await asyncio.gather(
            *(
                self._shard_union(ep, group, "UnionGetNeighbors", node_id, None, 0)
                for ep, group in groups.items()
            )
        )
        seen: set[str] = set()
        merged: list[str] = []
        for ids in per_shard:
            for nid in ids or []:
                if nid not in seen:
                    seen.add(nid)
                    merged.append(nid)
        return merged

    async def close_all(self) -> None:
        """Close all pools in the router."""
        for pool in self.pools.values():
            await pool.close_all()
