import asyncio
from typing import cast

import msgpack
import pytest

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.pool import ConnectionPool, ShardRouter


# We mock a small echo/health server to test transport without running the full daemon.
class MockServer:
    def __init__(self, host="127.0.0.1", port=9100):
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

                resp: dict[str, object] = {}
                if req["method"] == "Ping":
                    resp = {"result": "pong"}
                elif req["method"] == "Health":
                    resp = {"result": {"status": "ok"}}
                else:
                    resp = {"error": "unknown method"}

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

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


@pytest.mark.asyncio
async def test_frame_handling_and_pool():
    server = MockServer(port=9101)
    await server.start()

    try:
        pool = ConnectionPool("tcp://127.0.0.1:9101", min_size=2, max_size=3)
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

    server = await asyncio.start_server(silent_handler, "127.0.0.1", 9120)
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr="127.0.0.1:9120",
            auth_secret="s",
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
                await reader.readexactly(msg_len)
                resp_bytes = msgpack.packb({"result": "pong"})
                writer.write(len(resp_bytes).to_bytes(4, byteorder="big"))
                writer.write(resp_bytes)
                await writer.drain()
                if drop_after_one:
                    writer.close()  # engine drops this connection after one reply
                    return
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass

    server = await asyncio.start_server(handler, "127.0.0.1", 9121)
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr="127.0.0.1:9121", auth_secret="s", timeout=1.0
        )
        assert await client.ping() == "pong"  # served on the first connection

        # The server has closed the first connection; the next call detects the
        # dead stream and marks it closed (the unavoidable single failure).
        with pytest.raises((ConnectionError, TimeoutError)):
            await client.ping()
        assert client._closed is True

        # THE FIX: the following call transparently reconnects (new connection,
        # connections["n"] == 2) and succeeds instead of failing forever.
        assert await client.ping() == "pong"
        assert client._closed is False
        assert connections["n"] == 2
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_shard_stickiness():
    router = ShardRouter(
        ["tcp://127.0.0.1:9101", "tcp://127.0.0.1:9102", "tcp://127.0.0.1:9103"]
    )

    # Check that the same graph always maps to the same endpoint
    ep1 = router._get_shard_endpoint("graph_alpha")
    ep2 = router._get_shard_endpoint("graph_alpha")
    assert ep1 == ep2

    # Different graph should (with high probability) map elsewhere, though HRW could collide.
    # But for a small set, let's just ensure stickiness and that it's in the valid set.
    ep3 = router._get_shard_endpoint("graph_beta")
    assert ep3 in router.endpoints


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
            tcp_addr="127.0.0.1:9199", auth_secret="s", connect_timeout=0.2
        )
