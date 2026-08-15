import asyncio
from typing import cast

import msgpack
import pytest
from conftest import request_context

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.pool import ConnectionPool, ShardRouter


# We mock a small echo/health server to test transport without running the full daemon.
class MockServer:
    def __init__(self, host="127.0.0.1", port=0):
        self.host = host
        self.port = port
        self.server = None
        self.requests_handled = 0

    async def handle_client(self, reader, writer):
        try:
            while True:
                len_buf = await reader.readexactly(4)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                msg_bytes = await reader.readexactly(msg_len)

                req = msgpack.unpackb(msg_bytes, raw=False)
                self.requests_handled += 1

                resp: dict[str, object] = {"id": req["id"]}
                if req["method"] == "Ping":
                    resp["result"] = "pong"
                elif req["method"] == "Health":
                    resp["result"] = {"status": "ok"}
                else:
                    resp["error"] = "unknown method"

                resp_bytes = msgpack.packb(resp)
                resp_len = len(resp_bytes).to_bytes(4, byteorder="big")

                writer.write(resp_len)
                writer.write(resp_bytes)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        # Port `0` asks the OS for an unused ephemeral port; read back whichever
        # one it actually bound so a caller that requested `0` can connect. A
        # caller that requested a specific port keeps binding that exact port
        # (this is a no-op reassignment in that case).
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


@pytest.mark.asyncio
async def test_frame_handling_and_pool():
    # Port `0` binds an OS-assigned ephemeral port instead of a fixed one --
    # 9101 happens to also be this repo's documented example Prometheus
    # `--metrics-addr`/`GRAPH_SERVICE_METRICS_ADDR` port (see AGENTS.md), so a
    # hardcoded 9101 here collides with an unrelated already-running engine on
    # a shared dev host ("address already in use"), independent of anything
    # this test actually exercises.
    server = MockServer(port=0)
    await server.start()

    try:
        pool = ConnectionPool(
            f"tcp://127.0.0.1:{server.port}",
            verified_context=request_context(),
            auth_secret="s",
            min_size=2,
            max_size=3,
        )
        await pool.initialize()

        # Test basic acquire and frame handling
        client1 = await pool.acquire()
        ping_res = await client1.ping()
        assert ping_res == "pong"

        health_res = await client1.health()
        assert health_res["status"] == "ok"

        # Test exhaustion
        client2 = await pool.acquire()
        client3 = await pool.acquire()

        # Max size is 3, fourth acquire should block. We use asyncio.wait_for to assert timeout
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.1)

        pool.release(client1)
        client4 = await pool.acquire()
        assert client4 is client1  # Reused connection

        pool.release(client2)
        pool.release(client3)
        pool.release(client4)

        await pool.close_all()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_rpc_timeout_is_bounded_and_connection_fatal():
    # CONCEPT:EG-KG.query.wire-protocol (B1) — a server that accepts the connection but never replies.
    # Pre-B1 the client awaited the read forever; now every RPC is bounded and a
    # timeout is connection-fatal (the stream is desynced, so it must reconnect).
    async def silent_handler(reader, writer):
        try:
            await reader.read()  # drain forever; never write a reply
        except Exception:  # noqa: BLE001
            pass

    # Port `0` binds an OS-assigned ephemeral port instead of a fixed one -- a
    # hardcoded port is a structural collision hazard against any co-resident
    # engine or concurrent test run (GOC-70).
    server = await asyncio.start_server(silent_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=0.2,
            heavy_timeout=0.2,
        )
        with pytest.raises(TimeoutError):
            await client.ping()
        assert client._closed is True, "timeout must close the desynced connection"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_send_reconnects_after_connection_drop():
    # CONCEPT:EG-KG.query.wire-protocol — a long-lived client whose connection is dropped (engine
    # restart / idle close / a prior poisoned stream) MUST self-heal on the next
    # call. Before the fix, _send set _closed=True but never re-dialed, so every
    # subsequent call reused the dead writer and the engine circuit breaker
    # latched OPEN forever (the host worker stalled with the queue backing up).
    connections = {"n": 0}

    async def handler(reader, writer):
        connections["n"] += 1
        drop_after_one = connections["n"] == 1
        try:
            while True:
                len_buf = await reader.readexactly(4)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                request = msgpack.unpackb(await reader.readexactly(msg_len), raw=False)
                resp_bytes = msgpack.packb({"id": request["id"], "result": "pong"})
                writer.write(len(resp_bytes).to_bytes(4, byteorder="big"))
                writer.write(resp_bytes)
                await writer.drain()
                if drop_after_one:
                    writer.close()  # engine drops this connection after one reply
                    return
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass

    # Port `0` binds an OS-assigned ephemeral port instead of a fixed one -- a
    # hardcoded port is a structural collision hazard against any co-resident
    # engine or concurrent test run (GOC-70).
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=1.0,
        )
        assert await client.ping() == "pong"  # served on the first connection

        # The server has closed the first connection. `_ensure_connection` now
        # re-dials in place whenever it observes `_closed` (set by the reader
        # loop's EOF handler) BEFORE issuing a call, not just on the call
        # AFTER a failure -- so whether the reader loop notices the drop
        # before or after this next `ping()` is issued is a scheduling race,
        # not a behavioral contract: it may transparently reconnect and
        # succeed immediately, or surface the single stale-write failure the
        # module docstring above describes. Either way, THE FIX under test is
        # that the client is never stuck: the call sequence below always ends
        # up connected again, on a NEW connection, and serving `pong`.
        try:
            result = await client.ping()
        except (ConnectionError, TimeoutError):
            assert client._closed is True
            result = await client.ping()
        assert result == "pong"
        assert client._closed is False
        assert connections["n"] == 2
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_shard_stickiness():
    routes = {
        "graph_alpha": "tcp://127.0.0.1:9101",
        "graph_beta": "tcp://127.0.0.1:9102",
    }
    router = ShardRouter(
        ["tcp://127.0.0.1:9101", "tcp://127.0.0.1:9102", "tcp://127.0.0.1:9103"],
        verified_context=request_context(),
        route_resolver=routes.__getitem__,
    )

    # Check that the same graph always maps to the same endpoint
    ep1 = router._get_shard_endpoint("graph_alpha")
    ep2 = router._get_shard_endpoint("graph_alpha")
    assert ep1 == ep2

    # The authority may place a different graph elsewhere; the answer must still be
    # one of the configured endpoints.
    ep3 = router._get_shard_endpoint("graph_beta")
    assert ep3 in router.endpoints


def test_multi_endpoint_router_requires_authoritative_resolver():
    endpoints = ["tcp://shard-a:9100", "tcp://shard-b:9100"]
    with pytest.raises(ValueError, match="authoritative route_resolver"):
        ShardRouter(
            endpoints,
            verified_context=request_context(),
            auth_secret="test-route-secret",
        )
    router = ShardRouter(
        endpoints,
        verified_context=request_context(),
        auth_secret="test-route-secret",
        route_resolver=lambda _graph: endpoints[1],
    )
    assert router._get_shard_endpoint("graph:test") == endpoints[1]


@pytest.mark.asyncio
async def test_pool_propagates_tls_endpoint_to_native_client(monkeypatch):
    captured = {}

    async def connect(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(EpistemicGraphClient, "connect", connect)
    pool = ConnectionPool(
        "tls://engine.example.invalid:9100",
        verified_context=request_context(),
        auth_secret="s",
        max_size=1,
    )
    await pool._create_client()
    assert captured["tcp_addr"] == "engine.example.invalid:9100"
    assert captured["tls"] is True


@pytest.mark.asyncio
async def test_write_drain_timeout_is_bounded_and_connection_fatal(monkeypatch):
    # CONCEPT:EG-KG.query.wire-protocol (B1) — a wedged engine that accepts the connection but stops
    # READING the socket makes the request flush (drain) back up forever. The write
    # path must be bounded just like the read path, and a stalled drain is
    # connection-fatal (it ran under the send lock, so it would otherwise wedge
    # every subsequent RPC on this connection).
    monkeypatch.setattr("epistemic_graph.client._WRITE_TIMEOUT", 0.2)

    class _HangingWriter:
        def write(self, _data):  # noqa: D401 — sync, mirrors StreamWriter
            pass

        async def drain(self):
            await asyncio.sleep(3600)  # engine never reads -> drain never completes

        def close(self):
            pass

    class _IdleReader:
        async def readexactly(self, _n):
            await asyncio.sleep(3600)  # never reached; drain fails first

    client = EpistemicGraphClient(
        cast(asyncio.StreamReader, _IdleReader()),
        cast(asyncio.StreamWriter, _HangingWriter()),
        auth_secret="s",
        graph_name="g",
        verified_context=request_context(),
        timeout=30,
        heavy_timeout=30,
    )
    with pytest.raises(TimeoutError):
        await client.ping()
    assert client._closed is True, "a stalled write must close the wedged connection"


@pytest.mark.asyncio
async def test_connect_is_bounded(monkeypatch):
    # CONCEPT:EG-KG.query.wire-protocol (B1) — a peer that accepts the TCP connection but never
    # completes the handshake must not hang the caller forever; connect() is bounded.
    async def _never_connects(*_a, **_k):
        await asyncio.sleep(3600)

    monkeypatch.setattr("asyncio.open_connection", _never_connects)
    with pytest.raises(TimeoutError):
        await EpistemicGraphClient.connect(
            tcp_addr="127.0.0.1:9199",
            auth_secret="s",
            verified_context=request_context(),
            connect_timeout=0.2,
        )
