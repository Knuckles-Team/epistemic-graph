"""Integration tests for the epistemic-graph Tokio service layer.

These tests require the service binary to be built and start a local
server instance per test session using a temporary UDS socket.
"""

import asyncio
import os
import signal
import subprocess
import tempfile
import time

import pytest
from conftest import (
    TEST_AGENT_ID,
    TEST_SIGNER_KEY,
    bootstrap_context,
    find_server_binary,
    request_context,
    strict_server_env,
)

# Skip all tests if the server binary is not available. `find_server_binary()`
# honors `CARGO_TARGET_DIR`/the repo's own `.cargo/config.toml` (`target-isolated`)
# -- a hardcoded `target/debug`+`target/release` fallback never resolves in an
# ordinary checkout of this repo (see conftest.py's `find_server_binary` doc).
_SERVER_BIN = find_server_binary() or os.path.join(
    os.path.dirname(__file__), "..", "target", "debug", "epistemic-graph-server"
)

pytestmark = pytest.mark.skipif(
    not os.path.isfile(_SERVER_BIN),
    reason="epistemic-graph-server binary not built. Run 'cargo build' first.",
)


@pytest.fixture(scope="module")
def service():
    """Start a service process for the test module."""
    tmpdir = tempfile.mkdtemp()
    socket_path = os.path.join(tmpdir, "test.sock")
    secret = "test-secret-key"
    # `persist_dir` MUST be passed through to `strict_server_env` explicitly.
    # This module launches its OWN dedicated server, independent of the shared
    # session engine in conftest.py -- but that session fixture's `os.environ.
    # update(server_env)` leaves `GRAPH_SERVICE_PERSIST_DIR` pointing at ITS OWN
    # persist dir in the ambient process environment for the rest of the pytest
    # run. Omitting `persist_dir` here means `env = {**os.environ, **strict_
    # server_env(...)}` silently inherits that ambient value instead of this
    # module's own, and the dedicated server then refuses to start ("persist dir
    # ... is already locked by another epistemic-graph engine") because the
    # still-running session engine already holds that directory's lock -- the
    # exact ambient-global-state class this repo's AGENTS.md (GOC-70) calls out.
    persist_dir = os.path.join(tmpdir, "persist")
    os.makedirs(persist_dir, exist_ok=True)

    proc = subprocess.Popen(
        [_SERVER_BIN, "--socket-path", socket_path],
        env={
            **os.environ,
            **strict_server_env(
                os.path.join(tmpdir, "security"),
                auth_secret=secret,
                persist_dir=persist_dir,
            ),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for the socket to appear.
    for _ in range(50):
        if os.path.exists(socket_path):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("Service failed to start within 5 seconds")

    from epistemic_graph.client import SyncEpistemicGraphClient

    bootstrap = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        auth_secret=secret,
        verified_context=bootstrap_context(),
    )
    try:
        bootstrap.consensus.bootstrap_system_identity(
            agent_id=TEST_AGENT_ID,
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
    finally:
        bootstrap.close()

    yield {"socket_path": socket_path, "auth_secret": secret, "proc": proc}

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    import shutil

    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def client_factory(service):
    """Factory for creating connected clients."""
    from epistemic_graph.client import EpistemicGraphClient

    clients = []

    async def _make(graph_name="__commons__"):
        c = await EpistemicGraphClient.connect(
            socket_path=service["socket_path"],
            auth_secret=service["auth_secret"],
            graph_name=graph_name,
            verified_context=request_context(),
        )
        clients.append(c)
        return c

    yield _make

    # Cleanup.
    async def _cleanup():
        for c in clients:
            try:
                await c.close()
            except Exception:
                pass

    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_cleanup())


# ── Tests ─────────────────────────────────────────────────────────────────


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_ping(service, client_factory):
    """Test basic connectivity with a Ping request."""

    async def _test():
        client = await client_factory()
        result = await client.ping()
        assert result == "pong"

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_node_crud(service, client_factory):
    """Test node add/has/remove via the service."""

    async def _test():
        client = await client_factory()
        await client.nodes.add("test:n1", {"type": "TestNode"})
        assert await client.nodes.has("test:n1") is True
        assert await client.nodes.has("nonexistent") is False
        await client.nodes.remove("test:n1")
        assert await client.nodes.has("test:n1") is False

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_create_if_absent_has_one_winner(service, client_factory):
    """Concurrent callers share one durable atomic create decision."""

    async def _test():
        client = await client_factory()
        results = await asyncio.gather(
            *(
                client.nodes.create_if_absent(
                    "create-once:shared", {"type": "Work", "writer": writer}
                )
                for writer in range(8)
            )
        )
        assert results.count(True) == 1
        winner = results.index(True)
        props = await client.nodes.properties("create-once:shared")
        assert props == {"type": "Work", "writer": winner}

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_compare_and_set(service, client_factory):
    """Backend-agnostic atomic claim via CompareAndSetNodeFields round-trip."""

    async def _test():
        client = await client_factory()
        await client.nodes.add("cas:task1", {"type": "Task", "status": "pending"})

        # Condition matches → claim succeeds and updates merge in.
        ok = await client.nodes.compare_and_set(
            "cas:task1",
            {"status": "pending"},
            {"status": "claimed", "owner": "worker-7"},
        )
        assert ok is True
        props = await client.nodes.properties("cas:task1")
        assert props["status"] == "claimed"
        assert props["owner"] == "worker-7"
        assert props["type"] == "Task"  # untouched field preserved

        # Condition no longer matches → second claim fails, node unchanged.
        ok2 = await client.nodes.compare_and_set(
            "cas:task1",
            {"status": "pending"},
            {"owner": "intruder"},
        )
        assert ok2 is False
        props2 = await client.nodes.properties("cas:task1")
        assert props2["owner"] == "worker-7"

        # Missing node → false.
        assert (
            await client.nodes.compare_and_set("cas:absent", {"x": None}, {"x": 1})
            is False
        )

        # null condition means "absent or null" → matches an unset field.
        await client.nodes.add("cas:task2", {"type": "Task"})
        assert (
            await client.nodes.compare_and_set(
                "cas:task2", {"owner": None}, {"owner": "worker-1"}
            )
            is True
        )

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_edge_crud(service, client_factory):
    """Test edge add/has/remove via the service."""

    async def _test():
        client = await client_factory()
        await client.nodes.add("e:a", {})
        await client.nodes.add("e:b", {})
        await client.edges.add("e:a", "e:b", {"weight": 1.5})
        assert await client.edges.has("e:a", "e:b") is True
        await client.edges.remove("e:a", "e:b")
        assert await client.edges.has("e:a", "e:b") is False

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_multi_graph(service, client_factory):
    """Test creating, listing, and deleting named graphs."""

    async def _test():
        client = await client_factory()
        await client.tenants.create("agent:test-worker", "Agent")
        graphs = await client.tenants.list()
        names = [g["name"] for g in graphs]
        assert "__commons__" in names
        assert "agent:test-worker" in names

        await client.tenants.delete("agent:test-worker")
        graphs = await client.tenants.list()
        names = [g["name"] for g in graphs]
        assert "agent:test-worker" not in names

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_channel_p2p(service, client_factory):
    """Test P2P channel creation, messaging, and close with imprint."""

    async def _test():
        client = await client_factory()
        await client.channels.create(
            "channel:p2p:a:b", "PeerToPeer", "agent:a", ["agent:b"]
        )
        await client.channels.send_message("channel:p2p:a:b", "agent:a", "hello")
        msgs = await client.channels.get_messages("channel:p2p:a:b")
        assert len(msgs) == 1
        assert msgs[0]["payload"] == "hello"

        imprint = await client.channels.close(
            "channel:p2p:a:b", topic_metadata="test p2p"
        )
        assert imprint["message_count"] == 1

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_channel_group(service, client_factory):
    """Test group channel join/leave/close lifecycle."""

    async def _test():
        client = await client_factory()
        await client.channels.create("channel:group:test", "Group", "agent:a", [])
        await client.channels.join("channel:group:test", "agent:b")
        members = await client.channels.get_members("channel:group:test")
        assert len(members) == 2

        await client.channels.leave("channel:group:test", "agent:b")
        members = await client.channels.get_members("channel:group:test")
        assert len(members) == 1

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_auth_required(service):
    """Test that unauthenticated requests are rejected."""

    async def _test():
        from epistemic_graph.client import EpistemicGraphClient

        client = await EpistemicGraphClient.connect(
            socket_path=service["socket_path"],
            auth_secret="wrong-secret",
            graph_name="__commons__",
            verified_context=request_context(),
        )
        with pytest.raises(RuntimeError, match="Authentication failed"):
            await client.ping()
        await client.close()

    asyncio.run(_test())


@pytest.mark.concept("CONCEPT:AU-KG.query.object-graph-mapper")
def test_service_algorithms(service, client_factory):
    """Test algorithm execution via the service."""

    async def _test():
        client = await client_factory()
        # Build a small graph.
        await client.nodes.add("algo:a", {})
        await client.nodes.add("algo:b", {})
        await client.nodes.add("algo:c", {})
        await client.edges.add("algo:a", "algo:b", {})
        await client.edges.add("algo:b", "algo:c", {})

        # Topological sort.
        order = await client.graph.topological_sort()
        assert order.index("algo:a") < order.index("algo:b")
        assert order.index("algo:b") < order.index("algo:c")

        # PageRank.
        ranks = await client.analytics.pagerank(damping=0.85, iterations=10)
        assert len(ranks) > 0

    asyncio.run(_test())
