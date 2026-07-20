"""ACL enforcement end-to-end: isolation rules are enforced in dispatch.

Runs against a dedicated server instance (not the shared session server) and
initializes its first identity through the signed current bootstrap gate.
"""

import os
import subprocess
import time

import pytest
from conftest import (
    TEST_AGENT_ID,
    TEST_SIGNER_KEY,
    bootstrap_context,
    request_context,
    strict_server_env,
)

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SERVER_BIN = os.path.join(RUST_DIR, "target", "debug", "epistemic-graph-server")
SECRET = "test-isolation-secret"


@pytest.fixture(scope="module")
def isolation_server(tmp_path_factory):
    runtime = tmp_path_factory.mktemp("isolation")
    sock = str(runtime / "isolation.sock")
    proc = subprocess.Popen(
        [SERVER_BIN, "--socket-path", sock],
        env={
            **os.environ,
            **strict_server_env(str(runtime / "security"), auth_secret=SECRET),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not os.path.exists(sock):
        time.sleep(0.05)
    assert os.path.exists(sock), "isolation test server did not start"
    bootstrap = SyncEpistemicGraphClient.connect(
        socket_path=sock,
        auth_secret=SECRET,
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
    yield sock
    proc.terminate()
    proc.wait(timeout=10)


def _client(sock, agent_id=TEST_AGENT_ID, graph_name="__commons__"):
    return SyncEpistemicGraphClient.connect(
        socket_path=sock,
        auth_secret=SECRET,
        graph_name=graph_name,
        verified_context=request_context(agent_id=agent_id),
    )


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_unregistered_identity_is_denied(isolation_server):
    intruder = _client(
        isolation_server, agent_id="service:unregistered", graph_name="agent:restricted"
    )
    try:
        with pytest.raises(RuntimeError, match="ACCESS_DENIED"):
            intruder.nodes.add("n0", {"k": "v"})
    finally:
        intruder.close()


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_acl_enforced_once_identities_registered(isolation_server):
    sock = isolation_server

    system = _client(sock)
    owner = _client(sock, agent_id="service:worker-1", graph_name="agent:worker-1")
    peer = _client(sock, agent_id="service:worker-2", graph_name="agent:worker-1")
    manager = _client(sock, agent_id="service:manager", graph_name="agent:worker-1")
    try:
        # Register the hierarchy (manager supervises worker1/worker2).
        system.consensus.register_identity(
            "service:manager",
            {"Manager": {"subordinates": ["service:worker-1", "service:worker-2"]}},
            ["team:test"],
            [],
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
        system.consensus.register_identity(
            "service:worker-1",
            "Agent",
            ["team:test"],
            [],
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
        system.consensus.register_identity(
            "service:worker-2",
            "Agent",
            ["team:test"],
            [],
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )

        # Owner creates + writes its own graph.
        owner.tenants.create("agent:worker-1", "Agent")
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

        # The __commons__ stays open to everyone (channels keep working).
        bus = _client(sock, agent_id="service:worker-2")
        try:
            assert bus.ping() == "pong"
            bus.nodes.add("bus-node", {})
            bus.channels.create(
                "channel:p2p:worker-1:worker-2",
                "PeerToPeer",
                "service:worker-1",
                ["service:worker-2"],
            )
            bus.channels.send_message(
                "channel:p2p:worker-1:worker-2", "service:worker-2", "hello"
            )
            msgs = bus.channels.get_messages("channel:p2p:worker-1:worker-2")
            assert msgs and msgs[0]["payload"] == "hello"
        finally:
            bus.close()
    finally:
        system.close()
        owner.close()
        peer.close()
        manager.close()
