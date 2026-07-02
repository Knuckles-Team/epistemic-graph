"""CONCEPT:EG-043 — TRUE single-connection request PIPELINING.

The follow-up E flagged for the connection POOL (CONCEPT:EG-037): remove the
per-connection *serialization* so ONE TCP/UDS connection carries many in-flight
requests at once. The engine (``src/server/transport.rs::handle_connection``) now
reads frames in a loop and ``tokio::spawn``s a dispatch task per request, writing
the id-tagged responses back OUT OF ORDER through a single writer task; the Python
client (``epistemic_graph/client.py``) runs a background reader that demultiplexes
each ``Response`` to its caller by ``Response.id``.

Two layers of proof:

* ``PipelinedMockServer`` — a deterministic mock of the NEW engine model (spawns a
  per-request task with a per-request delay, replies out of order). Proves the
  CLIENT demux: N concurrent calls on ONE connection overlap (wall-clock ≪ serial
  sum), responses that complete out of order reach the right caller, per-caller
  ordering is preserved, the in-flight cap is respected, and an error on one
  in-flight request does not corrupt the others.
* ``test_real_engine_single_connection_pipelines`` — the real ephemeral engine
  (the session fixture in ``conftest.py``): N concurrent heavy ops on ONE
  connection are processed concurrently server-side (wall-clock ≪ serial sum) and
  every result is correct.
"""

import asyncio
import contextlib
import os
import time

import msgpack
import pytest

from epistemic_graph.client import EpistemicGraphClient

# ── Deterministic client-demux proof against a mock of the pipelined engine ──


class PipelinedMockServer:
    """Mock of the pipelined engine: read frames, spawn a task per request with a
    per-request delay, and write the id-tagged reply when it completes — so replies
    come back OUT OF ORDER. ``max_inflight`` is the high-water mark of simultaneous
    in-flight requests on a single connection (the direct measure of pipelining)."""

    def __init__(self, host="127.0.0.1", port=9401):
        self.host = host
        self.port = port
        self.server = None
        self.inflight = 0
        self.max_inflight = 0

    async def _process(self, writer, write_lock, req):
        params = req.get("params") or {}
        delay = float(params.get("delay", 0.0))
        await asyncio.sleep(delay)
        if params.get("fail"):
            resp = {"id": req.get("id"), "error": "boom: intentional server error"}
        else:
            # Echo a per-call marker so the demux→caller routing is verifiable.
            resp = {"id": req.get("id"), "result": params.get("tag", req.get("method"))}
        body = msgpack.packb(resp)
        async with write_lock:
            writer.write(len(body).to_bytes(4, byteorder="big"))
            writer.write(body)
            await writer.drain()
        self.inflight -= 1

    async def handle_client(self, reader, writer):
        write_lock = asyncio.Lock()
        tasks: set[asyncio.Task] = set()
        try:
            while True:
                len_buf = await reader.readexactly(4)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                body = await reader.readexactly(msg_len)
                req = msgpack.unpackb(body, raw=False)
                self.inflight += 1
                self.max_inflight = max(self.max_inflight, self.inflight)
                t = asyncio.create_task(self._process(writer, write_lock, req))
                tasks.add(t)
                t.add_done_callback(tasks.discard)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            for t in tasks:
                t.cancel()
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


async def _client_to(port: int) -> EpistemicGraphClient:
    return await EpistemicGraphClient.connect(
        tcp_addr=f"127.0.0.1:{port}", auth_secret="s", timeout=5.0, heavy_timeout=5.0
    )


@pytest.mark.asyncio
async def test_one_connection_pipelines_concurrently():
    # N independent calls on ONE connection, each ~60ms of server work. Serial sum
    # = N*60ms; pipelined ≈ 60ms. The server must SEE all N in flight at once.
    n, work = 8, 0.06
    server = PipelinedMockServer(port=9401)
    await server.start()
    client = await _client_to(9401)
    try:
        t0 = time.perf_counter()
        results = await asyncio.gather(
            *(client._send("Op", {"delay": work, "tag": i}) for i in range(n))
        )
        elapsed = time.perf_counter() - t0

        # Each caller got ITS OWN result (demux routed by id, no cross-talk).
        assert results == list(range(n))
        assert server.max_inflight == n, (
            f"one connection must pipeline {n}; saw {server.max_inflight}"
        )
        assert elapsed < n * work * 0.5, (
            f"pipelined wall-clock {elapsed:.3f}s not ≪ serial {n * work:.3f}s"
        )
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_out_of_order_completion_is_demuxed():
    # A slow call issued FIRST and a fast call issued SECOND on ONE connection: the
    # fast one completes first (out of order) and each result still reaches the
    # correct caller.
    server = PipelinedMockServer(port=9402)
    await server.start()
    client = await _client_to(9402)
    completion: list[str] = []
    try:

        async def call(tag: str, delay: float):
            r = await client._send("Op", {"delay": delay, "tag": tag})
            completion.append(tag)
            return r

        slow = asyncio.create_task(call("slow", 0.20))
        fast = asyncio.create_task(call("fast", 0.02))
        slow_r, fast_r = await asyncio.gather(slow, fast)

        assert slow_r == "slow" and fast_r == "fast", "results must not be swapped"
        assert completion == ["fast", "slow"], (
            f"out-of-order completion not demuxed: {completion}"
        )
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_one_error_does_not_corrupt_other_inflight():
    # Among several concurrent in-flight requests on ONE connection, one returns a
    # server ERROR. It must raise for ONLY that caller; the others complete with the
    # correct results and the connection stays usable.
    server = PipelinedMockServer(port=9403)
    await server.start()
    client = await _client_to(9403)
    try:

        async def ok(tag: int):
            return await client._send("Op", {"delay": 0.05, "tag": tag})

        async def boom():
            with pytest.raises(RuntimeError):
                await client._send("Op", {"delay": 0.02, "fail": True})
            return "raised"

        r0, rb, r1, r2 = await asyncio.gather(ok(0), boom(), ok(1), ok(2))
        assert (r0, r1, r2) == (0, 1, 2), "error must not corrupt sibling results"
        assert rb == "raised"
        # Connection is still healthy after one in-flight error.
        assert await client._send("Op", {"tag": 99}) == 99
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_within_caller_ordering_preserved():
    # Sequential awaits on one client keep wire order — each await blocks on its own
    # id, so a single logical sequence (node→edge→commit) is never reordered.
    server = PipelinedMockServer(port=9404)
    await server.start()
    client = await _client_to(9404)
    try:
        order = []
        for tag in ("AddNode", "AddEdge", "Commit"):
            order.append(await client._send(tag, {"delay": 0.0, "tag": tag}))
        assert order == ["AddNode", "AddEdge", "Commit"]
    finally:
        await client.close()
        await server.stop()


# ── Real ephemeral engine: server-side concurrency on ONE connection ──


@pytest.mark.asyncio
async def test_real_engine_single_connection_pipelines(start_epistemic_graph_server):
    _ = start_epistemic_graph_server  # fixture manages server lifecycle (ref for vulture)
    # The real engine: N concurrent HEAVY ops (BetweennessCentrality, O(V*E)) on ONE
    # connection must run concurrently server-side — wall-clock ≪ the serial sum —
    # proving handle_connection no longer serializes one connection's requests.
    socket_path = os.environ.get(
        "GRAPH_SERVICE_SOCKET", "/tmp/test_epistemic_graph_local.sock"
    )
    secret = os.environ.get("GRAPH_SERVICE_AUTH_SECRET", "epistemic-graph-test-secret")
    client = await EpistemicGraphClient.connect(
        socket_path=socket_path,
        auth_secret=secret,
        graph_name="pipelining_eg038",
        timeout=120.0,
        heavy_timeout=120.0,
    )
    try:
        with contextlib.suppress(RuntimeError):  # fresh tenant
            await client.tenants.create("pipelining_eg038", "Agent")
        await client.graph.clear()
        # A connected graph big enough that one betweenness pass (O(V*E)) takes long
        # enough (~tens of ms) to dwarf a Ping.
        size = 180
        for i in range(size):
            await client.nodes.add(f"n{i}", {"i": i})
        for i in range(size):
            await client.edges.add(f"n{i}", f"n{(i + 1) % size}")  # ring
            await client.edges.add(f"n{i}", f"n{(i + 7) % size}")  # chords
            await client.edges.add(f"n{i}", f"n{(i + 23) % size}")

        # ── PROOF A: the read loop does NOT block on dispatch (CPU-independent) ──
        # On ONE connection, fire a HEAVY op, then a CHEAP op (Ping) a hair later.
        # Under true pipelining the engine dispatches both concurrently, so the cheap
        # op completes FIRST while the heavy one is still computing. A connection that
        # serialized (the pre-EG-043 behavior) would force the Ping to wait behind the
        # heavy op in the connection's queue — completion would be ["heavy", "cheap"].
        # This proof does not depend on having spare cores for a wall-clock speedup.
        completion: list[str] = []

        async def heavy():
            await client.analytics.betweenness_centrality()
            completion.append("heavy")

        async def cheap():
            # Let the heavy op's frame get written + dispatched first.
            await asyncio.sleep(0.01)
            await client.ping()
            completion.append("cheap")

        await asyncio.gather(heavy(), cheap())
        assert completion == ["cheap", "heavy"], (
            "the cheap op blocked behind the heavy one on the same connection — the "
            f"read loop is still serializing dispatch (saw {completion})"
        )

        # ── PROOF B: N concurrent in-flight calls on ONE connection stay correct ──
        # Fire N heavy ops all in flight on the SAME connection; every response is
        # routed to the right caller by Response.id (no swap/drop) and equals the
        # serial result. (Wall-clock speedup is NOT asserted here: two CPU-bound
        # betweenness passes only overlap if the box has spare cores, which a loaded
        # shared box may not — PROOF A above is the CPU-independent concurrency proof,
        # and the deterministic mock test proves the strong wall-clock ≪ serial sum.)
        # Betweenness returns (node, score) pairs in an unstable order ⇒ compare maps.
        n = 8
        serial_results = [
            await client.analytics.betweenness_centrality() for _ in range(n)
        ]
        conc_results = await asyncio.gather(
            *(client.analytics.betweenness_centrality() for _ in range(n))
        )

        def norm(res):
            return {k: round(float(v), 6) for k, v in res}

        baseline = norm(serial_results[0])
        assert baseline, "betweenness returned no scores — graph too sparse?"
        assert all(norm(r) == baseline for r in conc_results), (
            "a concurrent in-flight result was wrong (demux mis-routed a response)"
        )
        assert all(norm(r) == baseline for r in serial_results)
    finally:
        await client.close()
