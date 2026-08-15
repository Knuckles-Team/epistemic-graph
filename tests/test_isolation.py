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
    find_server_binary,
    request_context,
    strict_server_env,
)

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# `find_server_binary()` honors `CARGO_TARGET_DIR`/the repo's own
# `.cargo/config.toml` (`target-isolated`) -- this module spawns its own server
# subprocess directly, so a hardcoded `target/debug` path never resolves in an
# ordinary checkout of this repo (see conftest.py's `find_server_binary` doc).
SERVER_BIN = find_server_binary() or os.path.join(
    RUST_DIR, "target", "debug", "epistemic-graph-server"
)
SECRET = "test-isolation-secret"


@pytest.fixture(scope="module")
def isolation_server(tmp_path_factory):
    runtime = tmp_path_factory.mktemp("isolation")
    sock = str(runtime / "isolation.sock")
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
    persist_dir = str(runtime / "persist")
    os.makedirs(persist_dir, exist_ok=True)
    proc = subprocess.Popen(
        [SERVER_BIN, "--socket-path", sock],
        env={
            **os.environ,
            **strict_server_env(
                str(runtime / "security"), auth_secret=SECRET, persist_dir=persist_dir
            ),
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
        # Under the `security` feature (in every `full`/default build) `check_access`
        # ignores `graph_type`/`graph_owner` entirely and decides PURELY from
        # `eg_capabilities`/`eg-core::rbac`'s durable grant policy
        # (`RbacPolicy::evaluate(identity.roles, resource, action)`) -- the legacy
        # "Agent-graph owner always has access" / "manager reaches subordinate's
        # graph" rules only exist in the `#[cfg(not(feature = "security"))]` branch,
        # which this build never compiles. So ownership/management here needs an
        # EXPLICIT RBAC role + grant, not just an `AgentIdentity.role` label --
        # `register_identity`'s `roles` (4th positional arg, an RBAC role-name list,
        # distinct from the `AgentRole` 2nd arg) is what `RbacPolicy::evaluate`
        # actually reads.
        system.rbac.add_role("worker1-access")
        system.rbac.add_grant("worker1-access", {"Graph": "agent:worker-1"}, "Read")
        system.rbac.add_grant("worker1-access", {"Graph": "agent:worker-1"}, "Write")
        system.rbac.add_role("commons-access")
        system.rbac.add_grant("commons-access", {"Graph": "__commons__"}, "Read")
        system.rbac.add_grant("commons-access", {"Graph": "__commons__"}, "Write")

        # Register the hierarchy (manager supervises worker1/worker2) -- the
        # `AgentRole`/`subordinates` labels are kept for identity bookkeeping, but
        # actual access is granted through the RBAC roles above: the manager and
        # the owning worker both get `worker1-access`; the peer worker gets only
        # `commons-access` (so it stays denied on `agent:worker-1` below).
        system.consensus.register_identity(
            "service:manager",
            {"Manager": {"subordinates": ["service:worker-1", "service:worker-2"]}},
            ["team:test"],
            ["worker1-access"],
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
        system.consensus.register_identity(
            "service:worker-1",
            "Agent",
            ["team:test"],
            ["worker1-access"],
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
        system.consensus.register_identity(
            "service:worker-2",
            "Agent",
            ["team:test"],
            ["commons-access"],
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )

        # Owner creates + writes its own graph. Row-level security
        # (CONCEPT:EG-KG.sharding.row-level-security) is a SEPARATE, per-row concern
        # from the graph-level ACL this test exercises: an untagged row (no `_owner`/
        # `_visibility`/`_grants` key at all) is `tagged=false` and denied to
        # everyone but `System`, even the identity that just wrote it -- explicit
        # `_visibility: "public"` is what makes a row visible to any caller who
        # already cleared graph-level ACL, which is the behavior this test wants to
        # observe (manager/owner both reading "n1" below).
        owner.tenants.create("agent:worker-1", "Agent")
        owner.nodes.add("n1", {"type": "Fact", "_visibility": "public"})
        assert owner.nodes.has("n1") is True

        # Peer is denied (read and write).
        with pytest.raises(RuntimeError, match="ACCESS_DENIED"):
            peer.nodes.add("intrusion", {})
        with pytest.raises(RuntimeError, match="ACCESS_DENIED"):
            peer.nodes.has("n1")

        # Manager has full access to the subordinate graph.
        assert manager.nodes.has("n1") is True
        manager.nodes.add("n2", {"by": "manager"})

        # The __commons__ stays open to every REGISTERED agent (channels keep
        # working) -- registered here via the `commons-access` RBAC role above.
        bus = _client(sock, agent_id="service:worker-2")
        try:
            assert bus.ping() == "pong"
            bus.nodes.add("bus-node", {})
            # `CreateChannel.creator` must equal the AUTHENTICATED caller
            # (`ACCESS_DENIED: channel creator must be caller` -- an unconditional
            # impersonation guard, see `test_service_layer.py`'s channel tests for
            # the same rule); `bus` is authenticated as "service:worker-2", so it
            # creates as itself with "service:worker-1" as the invited peer.
            bus.channels.create(
                "channel:p2p:worker-1:worker-2",
                "PeerToPeer",
                "service:worker-2",
                ["service:worker-1"],
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
