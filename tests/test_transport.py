import asyncio
import os
import threading
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
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


@pytest.mark.asyncio
async def test_frame_handling_and_pool():
    server = MockServer(port=9101)
    await server.start()

    try:
        pool = ConnectionPool(f"tcp://127.0.0.1:9101", min_size=2, max_size=3)
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
async def test_shard_stickiness():
    router = ShardRouter([
        "tcp://127.0.0.1:9101",
        "tcp://127.0.0.1:9102",
        "tcp://127.0.0.1:9103"
    ])

    # Check that the same graph always maps to the same endpoint
    ep1 = router._get_shard_endpoint("graph_alpha")
    ep2 = router._get_shard_endpoint("graph_alpha")
    assert ep1 == ep2

    # Different graph should (with high probability) map elsewhere, though HRW could collide.
    # But for a small set, let's just ensure stickiness and that it's in the valid set.
    ep3 = router._get_shard_endpoint("graph_beta")
    assert ep3 in router.endpoints
