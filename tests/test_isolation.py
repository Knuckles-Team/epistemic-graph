"""ACL enforcement end-to-end: isolation rules are enforced in dispatch.

Runs against a dedicated server instance (not the shared session server) so
registering identities here cannot leak enforcing-mode into other tests.

Back-compat invariant: with no identities registered the server behaves
exactly as before (see test_no_rules_back_compat); the first RegisterIdentity
switches graph-targeted dispatch to enforcing mode.
"""

import os
import subprocess
import time

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SERVER_BIN = os.path.join(RUST_DIR, "target", "debug", "epistemic-graph-server")
SECRET = "isolation-test-secret"  # sanitizer:ignore — test-only value


@pytest.fixture(scope="module")
def isolation_server(tmp_path_factory):
    sock = str(tmp_path_factory.mktemp("isolation") / "isolation.sock")
    proc = subprocess.Popen(
        [SERVER_BIN, "--socket-path", sock],
        env={**os.environ, "GRAPH_SERVICE_AUTH_SECRET": SECRET},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not os.path.exists(sock):
        time.sleep(0.05)
    assert os.path.exists(sock), "isolation test server did not start"
    yield sock
    proc.terminate()
    proc.wait(timeout=10)


def _client(sock, agent_id=None, graph_name="__bus__"):
    return SyncEpistemicGraphClient.connect(
        socket_path=sock,
        auth_secret=SECRET,
        graph_name=graph_name,
        agent_id=agent_id,
    )


@pytest.mark.concept("CONCEPT:KG-2.19")
def test_no_rules_back_compat(isolation_server):
    """Before any identity is registered, peers can reach any graph."""
    anon = _client(isolation_server)
    try:
        anon.tenants.create("agent:precheck", "Agent")
        anon.nodes.add("n0", {"k": "v"})
        assert anon.ping() == "pong"
    finally:
        anon.close()


@pytest.mark.concept("CONCEPT:KG-2.19")
def test_acl_enforced_once_identities_registered(isolation_server):
    sock = isolation_server

    owner = _client(sock, agent_id="worker1", graph_name="agent:worker1")
    peer = _client(sock, agent_id="worker2", graph_name="agent:worker1")
    manager = _client(sock, agent_id="manager", graph_name="agent:worker1")
    try:
        # Register the hierarchy (manager supervises worker1/worker2).
        owner.consensus.register_identity(
            "manager", {"Manager": {"subordinates": ["worker1", "worker2"]}}, ["alpha"], "sig"
        )
        owner.consensus.register_identity("worker1", "Agent", ["alpha"], "sig")
        owner.consensus.register_identity("worker2", "Agent", ["alpha"], "sig")

        # Owner creates + writes its own graph.
        owner.tenants.create("agent:worker1", "Agent")
        owner.nodes.add("n1", {"type": "Fact"})
        assert owner.nodes.has("n1") is True

        # Peer is denied (read and write).
        with pytest.raises(RuntimeError, match="ACCESS_DENIED"):
            peer.nodes.add("intrusion", {})
        with pytest.raises(RuntimeError, match="ACCESS_DENIED"):
            peer.nodes.has("n1")

        # Manager has full access to the subordinate graph.
        assert manager.nodes.has("n1") is True
        manager.nodes.add("n2", {"by": "manager"})

        # The __bus__ stays open to everyone (channels keep working).
        bus = _client(sock, agent_id="worker2")
        try:
            assert bus.ping() == "pong"
            bus.nodes.add("bus-node", {})
            bus.channels.create("channel:p2p:worker1:worker2", "PeerToPeer", "worker1", ["worker2"])
            bus.channels.send_message("channel:p2p:worker1:worker2", "worker2", "hello")
            msgs = bus.channels.get_messages("channel:p2p:worker1:worker2")
            assert msgs and msgs[0]["payload"] == "hello"
        finally:
            bus.close()
    finally:
        owner.close()
        peer.close()
        manager.close()
