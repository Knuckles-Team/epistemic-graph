"""Integration tests for the epistemic-graph Tokio service layer.

These tests require the service binary to be built and start a local
server instance per test session using a temporary UDS socket.
"""

import asyncio
import json
import os
import signal
import subprocess
import tempfile
import time

import pytest

# Skip all tests if the server binary is not available.
_SERVER_BIN = os.path.join(
    os.path.dirname(__file__), "..", "target", "debug", "epistemic-graph-server"
)
if not os.path.isfile(_SERVER_BIN):
    _SERVER_BIN = os.path.join(
        os.path.dirname(__file__), "..", "target", "release", "epistemic-graph-server"
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

    proc = subprocess.Popen(
        [_SERVER_BIN, "--socket-path", socket_path, "--auth-secret", secret],
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

    async def _make(graph_name="__bus__"):
        c = await EpistemicGraphClient.connect(
            socket_path=service["socket_path"],
            auth_secret=service["auth_secret"],
            graph_name=graph_name,
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


def test_service_ping(service, client_factory):
    """Test basic connectivity with a Ping request."""
    async def _test():
        client = await client_factory()
        result = await client.ping()
        assert result == "pong"
    asyncio.run(_test())


def test_service_node_crud(service, client_factory):
    """Test node add/has/remove via the service."""
    async def _test():
        client = await client_factory()
        await client.add_node("test:n1", {"type": "TestNode"})
        assert await client.has_node("test:n1") is True
        assert await client.has_node("nonexistent") is False
        await client.remove_node("test:n1")
        assert await client.has_node("test:n1") is False
    asyncio.run(_test())


def test_service_edge_crud(service, client_factory):
    """Test edge add/has/remove via the service."""
    async def _test():
        client = await client_factory()
        await client.add_node("e:a", {})
        await client.add_node("e:b", {})
        await client.add_edge("e:a", "e:b", {"weight": 1.5})
        assert await client.has_edge("e:a", "e:b") is True
        await client.remove_edge("e:a", "e:b")
        assert await client.has_edge("e:a", "e:b") is False
    asyncio.run(_test())


def test_service_multi_graph(service, client_factory):
    """Test creating, listing, and deleting named graphs."""
    async def _test():
        client = await client_factory()
        await client.create_graph("agent:test-worker", "Agent")
        graphs = await client.list_graphs()
        names = [g["name"] for g in graphs]
        assert "__bus__" in names
        assert "agent:test-worker" in names

        await client.delete_graph("agent:test-worker")
        graphs = await client.list_graphs()
        names = [g["name"] for g in graphs]
        assert "agent:test-worker" not in names
    asyncio.run(_test())


def test_service_channel_p2p(service, client_factory):
    """Test P2P channel creation, messaging, and close with imprint."""
    async def _test():
        client = await client_factory()
        await client.create_channel(
            "channel:p2p:a:b", "PeerToPeer", "agent:a", ["agent:b"]
        )
        await client.send_message("channel:p2p:a:b", "agent:a", "hello")
        msgs = await client.get_channel_messages("channel:p2p:a:b")
        assert len(msgs) == 1
        assert msgs[0]["payload"] == "hello"

        imprint = await client.close_channel(
            "channel:p2p:a:b", topic_metadata="test p2p"
        )
        assert imprint["message_count"] == 1
    asyncio.run(_test())


def test_service_channel_group(service, client_factory):
    """Test group channel join/leave/close lifecycle."""
    async def _test():
        client = await client_factory()
        await client.create_channel(
            "channel:group:test", "Group", "agent:a", []
        )
        await client.join_channel("channel:group:test", "agent:b")
        members = await client.get_channel_members("channel:group:test")
        assert len(members) == 2

        await client.leave_channel("channel:group:test", "agent:b")
        members = await client.get_channel_members("channel:group:test")
        assert len(members) == 1
    asyncio.run(_test())


def test_service_auth_required(service):
    """Test that unauthenticated requests are rejected."""
    async def _test():
        from epistemic_graph.client import EpistemicGraphClient
        client = await EpistemicGraphClient.connect(
            socket_path=service["socket_path"],
            auth_secret="wrong-secret",
            graph_name="__bus__",
        )
        with pytest.raises(RuntimeError, match="Authentication failed"):
            await client.ping()
        await client.close()
    asyncio.run(_test())


def test_service_algorithms(service, client_factory):
    """Test algorithm execution via the service."""
    async def _test():
        client = await client_factory()
        # Build a small graph.
        await client.add_node("algo:a", {})
        await client.add_node("algo:b", {})
        await client.add_node("algo:c", {})
        await client.add_edge("algo:a", "algo:b", {})
        await client.add_edge("algo:b", "algo:c", {})

        # Topological sort.
        order = await client.topological_sort()
        assert order.index("algo:a") < order.index("algo:b")
        assert order.index("algo:b") < order.index("algo:c")

        # PageRank.
        ranks = await client.pagerank(damping=0.85, iterations=10)
        assert len(ranks) > 0
    asyncio.run(_test())
