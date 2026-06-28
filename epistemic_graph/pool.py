"""Connection pooling and shard routing for epistemic-graph Tokio service.

Provides an async ConnectionPool to manage multiple connections to a single service endpoint,
and a ShardRouter that distributes graphs across multiple backend endpoints using
Highest Random Weight (HRW) consistent hashing.

CONCEPT:KG-2.181 — Cross-shard union co-residency + scatter-gather.

The engine's cross-graph UNION read (CONCEPT:KG-2.171) is *single-shard*: the
handler re-enters one shard's local registry, so every graph in a union set must
be CO-RESIDENT on the shard the request routes to. Plain HRW hashing scatters
related graphs (``__commons__`` and the per-lane ``__ingest_*`` graphs) across
shards, which would break the union read.

Two mechanisms close that gap, both purely client-side:

* **Affinity / co-residency** (``AffinityRegistry`` + ``ShardRouter._route_key``):
  a declared group of graphs is pinned to the SAME shard by routing every member
  through the group's *anchor* key instead of the graph's own name. The default
  group pins the ingest lanes to the ``__commons__`` shard so a union over
  ``{__commons__, __ingest__, __ingest_*}`` always lands on one shard and can use
  the fast single-shard union RPC directly.

* **Scatter-gather** (``ShardRouter.*_union``): when a union set still spans more
  than one shard (graphs in different affinity groups, or none), the router groups
  the graphs by their resolved shard endpoint, fans the existing per-shard union
  RPC out to each shard concurrently, then merges + dedupes the per-shard results.
  No new engine/Rust method is introduced — it reuses the KG-2.171 RPCs.

Single-endpoint homelab behavior is unchanged: with one endpoint every graph
co-resides trivially, so affinity is a no-op and scatter-gather degenerates to a
single per-shard call.

Failure mode (CONCEPT:KG-2.181): scatter-gather is **fail-loud per shard**. If any
shard's sub-union raises (e.g. one denied graph on that shard), the exception
propagates and the whole union fails — consistent with the engine's
fail-loud-per-shard durability contract. We do NOT silently degrade to a partial
union, because a caller cannot distinguish "node genuinely absent" from "a shard
was unreachable" in a deduped merge, and a silently partial union is a correctness
hazard. (A missing graph is still skipped engine-side, exactly as in the
single-shard union — that is an empty contribution, not an error.)

CONCEPT:EG-037 — Multiplexed engine connections (parallelize the wire).

One ``EpistemicGraphClient`` connection dispatches requests SERIALLY: ``_send``
holds a per-connection ``asyncio.Lock`` for the whole write→round-trip→read, and
— decisively — the *engine's* per-connection loop is itself serial (it awaits
``dispatch`` fully before reading the next frame, see
``src/server/transport.rs::handle_connection``). So M concurrent callers sharing
ONE connection are serialized on the wire even though each request already carries
a correlation id (``Request.id``/``Response.id``).

The high-value win that needs NO server change is **connection multiplexing**: a
real pool hands out N independent connections, and because the engine
``tokio::spawn``s one task PER connection, N pooled connections run as N parallel
server tasks. ``ConnectionPool.map_concurrent`` / ``ShardRouter.map_concurrent``
fan a set of INDEPENDENT operations out across the pool — each op on its own
connection — so wall-clock collapses from the serial sum to roughly one op. The
pool auto-sizes to the box (``_auto_pool_size``); there is no knob to tune.

Ordering is preserved where callers rely on it: a single logical write (a node
before its edges) runs as ONE ``fn`` on ONE connection via ``connection()`` /
``acquire``; only operations that are genuinely independent of each other are
spread across connections.

**Pipelining is a server follow-up, not done here.** True single-connection
pipelining (many in-flight requests demuxed by id) would need
``handle_connection`` to read-ahead and ``spawn`` a task per request, writing
responses back out-of-order as they complete — a change to
``src/server/transport.rs`` (the correlation-id field the demux needs already
exists in the protocol). Until then, single-connection pipelining buys no
concurrency (the engine reads the next frame only after replying), so we
multiplex CONNECTIONS instead and flag the server change as the next step.
"""

import asyncio
import contextlib
import hashlib
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from .client import EpistemicGraphClient

logger = logging.getLogger(__name__)


def _auto_pool_size() -> int:
    """Auto-size a per-endpoint connection pool to the box (CONCEPT:EG-037).

    The pool must give every concurrent caller hitting one shard its own
    in-flight connection — the K shard-writers, the staged pipeline's WRITE pool,
    and the read fan-out — so none serializes behind another on the wire. We size
    by CPU count (the server spawns a task per connection, so this is the parallel
    width the engine can actually service) clamped to a sane floor/ceiling. No env
    knob: the operator's law is auto-size, not tune.
    """
    cpu = os.cpu_count() or 4
    return max(8, min(2 * cpu, 64))


# CONCEPT:KG-2.181 — anchor key for the default ingest-lane affinity group.
_COMMONS_ANCHOR = "__commons__"


def _default_affinity_groups() -> dict[str, str]:
    """Default graph→anchor co-residency map.

    Pins the per-lane ingest graphs to the ``__commons__`` shard so a union read
    over the ingest lanes + commons resolves on one shard (CONCEPT:KG-2.171's
    co-residency contract). ``__ingest_*`` lanes are matched by prefix at routing
    time; the explicit map covers the bare lane name.

    Override / extend via ``GRAPH_AFFINITY_GROUPS`` — a comma-separated list of
    ``anchor:member1|member2|...`` clauses, e.g.
    ``__commons__:__ingest__|__ingest_news__,__hot__:__hot_a__|__hot_b__``.
    A member may be a literal graph name or a ``prefix*`` glob (trailing ``*``).
    """
    groups: dict[str, str] = {
        _COMMONS_ANCHOR: _COMMONS_ANCHOR,
        "__ingest__": _COMMONS_ANCHOR,
        # Prefix glob: any __ingest_<lane> co-resides with __commons__.
        "__ingest_*": _COMMONS_ANCHOR,
    }
    env = os.environ.get("GRAPH_AFFINITY_GROUPS", "").strip()
    if env:
        for clause in env.split(","):
            clause = clause.strip()
            if not clause or ":" not in clause:
                continue
            anchor, members = clause.split(":", 1)
            anchor = anchor.strip()
            if not anchor:
                continue
            groups.setdefault(anchor, anchor)
            for member in members.split("|"):
                member = member.strip()
                if member:
                    groups[member] = anchor
    return groups


class AffinityRegistry:
    """Maps a graph name to its co-residency *anchor* key (CONCEPT:KG-2.181).

    Routing by the anchor instead of the graph's own name forces every member of
    an affinity group onto the same shard. Members may be exact names or trailing
    ``*`` prefix globs. A graph in no group anchors to itself (plain HRW).
    """

    def __init__(self, groups: dict[str, str] | None = None) -> None:
        raw = groups if groups is not None else _default_affinity_groups()
        self._exact: dict[str, str] = {}
        self._prefixes: list[tuple[str, str]] = []
        for member, anchor in raw.items():
            if member.endswith("*"):
                self._prefixes.append((member[:-1], anchor))
            else:
                self._exact[member] = anchor
        # Longest prefix first so the most specific glob wins.
        self._prefixes.sort(key=lambda kv: len(kv[0]), reverse=True)

    def anchor_for(self, graph_name: str) -> str:
        """Return the routing key for ``graph_name`` (itself if no affinity)."""
        if graph_name in self._exact:
            return self._exact[graph_name]
        for prefix, anchor in self._prefixes:
            if graph_name.startswith(prefix):
                return anchor
        return graph_name


class ConnectionPool:
    """Async connection pool for EpistemicGraphClient instances."""

    def __init__(
        self,
        endpoint: str,
        auth_secret: str | None = None,
        min_size: int = 1,
        max_size: int | None = None,
        agent_id: str | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.auth_secret = auth_secret
        # Optional caller identity forwarded on every request for server-side
        # ACL enforcement (see the server's isolation layer).
        self.agent_id = agent_id
        # CONCEPT:EG-037 — auto-size to the box when the caller doesn't pin a cap,
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
        if self.endpoint.startswith("tcp://"):
            tcp_addr = self.endpoint[6:]
            client = await EpistemicGraphClient.connect(
                tcp_addr=tcp_addr, auth_secret=self.auth_secret, agent_id=self.agent_id
            )
        elif self.endpoint.startswith("unix://"):
            socket_path = self.endpoint[7:]
            client = await EpistemicGraphClient.connect(
                socket_path=socket_path,
                auth_secret=self.auth_secret,
                agent_id=self.agent_id,
            )
        else:
            # Default to socket if no scheme provided
            client = await EpistemicGraphClient.connect(
                socket_path=self.endpoint,
                auth_secret=self.auth_secret,
                agent_id=self.agent_id,
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
        connection and starve the cap. A single logical write that relies on
        ordering (a node before its edges) stays inside ONE such block — i.e. ONE
        connection — so its operations keep their on-the-wire order.
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
        (CONCEPT:EG-037 — parallelize the wire).

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
    """Routes graphs to specific shard endpoints using HRW consistent hashing."""

    def __init__(
        self,
        endpoints: list[str],
        auth_secret: str | None = None,
        min_size: int = 1,
        max_size: int | None = None,
        agent_id: str | None = None,
        affinity: AffinityRegistry | None = None,
    ) -> None:
        if not endpoints:
            raise ValueError("ShardRouter requires at least one endpoint")
        self.endpoints = endpoints
        # CONCEPT:KG-2.181 — co-residency layer. Routing keys pass through here
        # so an affinity group's members share one shard (anchor-keyed HRW).
        self.affinity = affinity if affinity is not None else AffinityRegistry()
        self.pools: dict[str, ConnectionPool] = {}
        for ep in endpoints:
            self.pools[ep] = ConnectionPool(
                ep, auth_secret, min_size, max_size, agent_id=agent_id
            )

    async def initialize(self) -> None:
        """Initialize all connection pools."""
        for pool in self.pools.values():
            await pool.initialize()

    def _route_key(self, graph_name: str) -> str:
        """Resolve the HRW routing key for ``graph_name`` via affinity.

        CONCEPT:KG-2.181 — members of an affinity group hash by their anchor key,
        which pins them to the same shard as the anchor (e.g. ingest lanes →
        ``__commons__``).
        """
        return self.affinity.anchor_for(graph_name)

    def _get_shard_endpoint(self, graph_name: str) -> str:
        # Route by the affinity anchor so co-resident graphs share a shard.
        key = self._route_key(graph_name)
        # Rendezvous hashing (HRW)
        max_score = -1
        best_endpoint = self.endpoints[0]

        for ep in self.endpoints:
            # Hash endpoint + routing key
            s = f"{ep}-{key}".encode()
            # MD5 used only for rendezvous/HRW shard selection, never security.
            score = int(hashlib.md5(s, usedforsecurity=False).hexdigest(), 16)  # nosec B324
            if score > max_score:
                max_score = score
                best_endpoint = ep

        return best_endpoint

    def group_by_shard(self, graph_names: list[str]) -> dict[str, list[str]]:
        """Group ``graph_names`` by their resolved shard endpoint (order-preserving).

        CONCEPT:KG-2.181 — the partition a scatter-gather union fans out over.
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
        always releasing it back to the right pool (CONCEPT:EG-037).

        ``async with router.connection(graph) as client: ...`` — the leak-free way
        the hot write/read path holds a connection. Order-dependent operations on
        one graph (node-before-edge) stay inside one block / one connection.
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
        own connection to that graph's shard (CONCEPT:EG-037).

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

    # ── Cross-shard scatter-gather union (CONCEPT:KG-2.181) ──────────────
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
        first. Fail-loud per shard (CONCEPT:KG-2.181).
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
        Fail-loud per shard (CONCEPT:KG-2.181).
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

        Fail-loud per shard (CONCEPT:KG-2.181).
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
