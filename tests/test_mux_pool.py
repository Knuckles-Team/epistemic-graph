"""CONCEPT:EG-037 — multiplexed connection pool (parallelize the wire).

Proves the client-side win: N INDEPENDENT operations dispatched over N pooled
connections run CONCURRENTLY (wall-clock ≪ serial sum), the pool reuses warm
connections and respects its cap, and order-dependent operations within a single
caller stay serialized on ONE connection.

The mock server mirrors the real engine's concurrency model: ``asyncio.start_server``
spawns one handler task per connection (the engine ``tokio::spawn``s per connection),
and each handler processes its connection's frames serially (the engine awaits
``dispatch`` before reading the next frame). So parallelism comes ONLY from holding
multiple connections — exactly what the pool multiplexes.
"""

import asyncio
import contextlib
import time

import msgpack
import pytest

from epistemic_graph.pool import ConnectionPool, ShardRouter, _auto_pool_size


class WorkMockServer:
    """Echo server that simulates per-request work and tracks concurrency.

    ``max_inflight`` is the high-water mark of simultaneously in-flight requests —
    the direct measure of how much the client parallelized the wire. ``received``
    is the arrival order of method names (for the ordering assertion).
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 9301, work: float = 0.05):
        self.host = host
        self.port = port
        self.work = work
        self.inflight = 0
        self.max_inflight = 0
        self.received: list[str] = []
        self.server: asyncio.AbstractServer | None = None

    async def handle_client(self, reader, writer):
        try:
            while True:
                len_buf = await reader.readexactly(4)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                body = await reader.readexactly(msg_len)
                req = msgpack.unpackb(body, raw=False)

                self.inflight += 1
                self.max_inflight = max(self.max_inflight, self.inflight)
                self.received.append(req.get("method"))
                if self.work:
                    await asyncio.sleep(self.work)
                self.inflight -= 1

                resp = msgpack.packb({"id": req.get("id"), "result": "pong"})
                writer.write(len(resp).to_bytes(4, byteorder="big"))
                writer.write(resp)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


def test_pool_auto_sizes_without_a_knob():
    # CONCEPT:EG-037 — no max_size given ⇒ auto-size to the box (no env knob).
    pool = ConnectionPool("tcp://127.0.0.1:1")
    assert pool.max_size == _auto_pool_size()
    assert pool.max_size >= 8
    # ShardRouter pools inherit the auto-size.
    router = ShardRouter(["tcp://127.0.0.1:1", "tcp://127.0.0.1:2"])
    for p in router.pools.values():
        assert p.max_size == _auto_pool_size()
    # An explicit cap is still honored.
    assert ConnectionPool("tcp://127.0.0.1:1", max_size=3).max_size == 3


@pytest.mark.asyncio
async def test_map_concurrent_parallelizes_the_wire():
    # The parallelism proof: 8 independent ops, each ~50ms of server work.
    # Serial sum = 8 * 50ms = 400ms; multiplexed over 8 connections ≈ 50ms.
    n = 8
    work = 0.05
    server = WorkMockServer(port=9301, work=work)
    await server.start()
    try:
        pool = ConnectionPool("tcp://127.0.0.1:9301", max_size=n)

        async def op(client):
            return await client._send("Op")

        t0 = time.perf_counter()
        results = await pool.map_concurrent([op] * n)
        elapsed = time.perf_counter() - t0

        assert results == ["pong"] * n
        serial_sum = n * work
        # Must be far under the serial sum — the wire ran in parallel.
        assert elapsed < serial_sum * 0.5, (
            f"map_concurrent took {elapsed:.3f}s, expected ≪ {serial_sum:.3f}s serial"
        )
        # The server actually saw multiple requests in flight at once.
        assert server.max_inflight == n, (
            f"expected {n} concurrent in-flight, saw {server.max_inflight}"
        )

        await pool.close_all()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_serial_single_connection_is_the_baseline():
    # Control: the SAME 8 ops on ONE connection serialize (max_inflight == 1) and
    # take ~ the serial sum — the bottleneck the pool removes.
    n = 8
    work = 0.05
    server = WorkMockServer(port=9302, work=work)
    await server.start()
    try:
        pool = ConnectionPool("tcp://127.0.0.1:9302", max_size=n)
        async with pool.connection() as client:
            t0 = time.perf_counter()
            for _ in range(n):
                assert await client._send("Op") == "pong"
            elapsed = time.perf_counter() - t0

        assert server.max_inflight == 1, "one connection must serialize requests"
        assert elapsed >= n * work * 0.8, "serial path should ~ sum the per-op work"
        await pool.close_all()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_pool_respects_cap_under_concurrency():
    # CONCEPT:EG-037 — with a cap of 2, four concurrent ops never exceed 2 in
    # flight: the surplus waits for a connection (correctness over saturation).
    server = WorkMockServer(port=9303, work=0.05)
    await server.start()
    try:
        pool = ConnectionPool("tcp://127.0.0.1:9303", max_size=2)

        async def op(client):
            return await client._send("Op")

        results = await pool.map_concurrent([op] * 4)
        assert results == ["pong"] * 4
        assert server.max_inflight <= 2, (
            f"cap=2 must bound in-flight, saw {server.max_inflight}"
        )
        assert server.max_inflight == 2, "two connections should run in parallel"
        # Never created more than the cap.
        assert pool._active_connections <= 2
        await pool.close_all()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_pool_reuses_warm_connections():
    # A released connection is reused, not re-dialed: two sequential checkouts hand
    # back the SAME client object.
    server = WorkMockServer(port=9304, work=0.0)
    await server.start()
    try:
        pool = ConnectionPool("tcp://127.0.0.1:9304", max_size=4)
        async with pool.connection() as c1:
            first = c1
            assert await c1._send("Op") == "pong"
        async with pool.connection() as c2:
            assert c2 is first, "warm connection should be reused"
        assert pool._active_connections == 1
        await pool.close_all()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_ordering_preserved_within_one_caller():
    # CONCEPT:EG-037 — a single logical write (node-before-edge) runs on ONE
    # connection inside one connection() block, so its ops keep wire order.
    server = WorkMockServer(port=9305, work=0.0)
    await server.start()
    try:
        pool = ConnectionPool("tcp://127.0.0.1:9305", max_size=4)
        async with pool.connection() as client:
            await client._send("AddNode")
            await client._send("AddEdge")
            await client._send("Commit")
        assert server.received == ["AddNode", "AddEdge", "Commit"]
        await pool.close_all()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_shard_router_map_concurrent_parallelizes_one_graph():
    # The per-graph multiplex: independent ops on one graph fan over that shard's
    # pool concurrently (each on its own connection), results in order.
    n = 6
    work = 0.05
    server = WorkMockServer(port=9306, work=work)
    await server.start()
    try:
        router = ShardRouter(["tcp://127.0.0.1:9306"])

        async def op(client):
            assert client._graph_name == "g:alpha"
            return await client._send("Op")

        t0 = time.perf_counter()
        results = await router.map_concurrent("g:alpha", [op] * n)
        elapsed = time.perf_counter() - t0

        assert results == ["pong"] * n
        assert elapsed < n * work * 0.5
        assert server.max_inflight >= 2, "router did not parallelize the wire"
        await router.close_all()
    finally:
        await server.stop()
