"""The per-principal control-lease kind allowlist, end to end (operator ruling
2026-09-24): a principal the deploy policy names may issue and transition only
its listed kinds; every other principal is unaffected for non-reserved kinds.
The `rbac.elevation` kind is reserved for the dedicated two-person elevation flow
regardless of this per-principal policy."""

import json
import os
import subprocess
import time
import uuid

import pytest
from conftest import (
    TEST_AGENT_ID,
    TEST_SIGNER_KEY,
    TEST_TENANT,
    bootstrap_context,
    find_server_binary,
    request_context,
    strict_server_env,
)

from epistemic_graph.client import SyncEpistemicGraphClient

SERVER_BIN = find_server_binary() or os.path.join(
    os.path.dirname(__file__), "..", "target", "debug", "epistemic-graph-server"
)
SECRET = "test-lease-kind-policy-secret"  # sanitizer:ignore
DEPUTY = "service:lease-deputy"
OTHER = "service:lease-other"
POLICY = json.dumps({DEPUTY: ["finance.order-proposal"]})


@pytest.fixture(scope="module")
def policy_server(tmp_path_factory):
    runtime = tmp_path_factory.mktemp("lease-kind-policy")
    sock = str(runtime / "lease.sock")
    persist_dir = str(runtime / "persist")
    os.makedirs(persist_dir, exist_ok=True)
    env = {
        **os.environ,
        **strict_server_env(
            str(runtime / "security"), auth_secret=SECRET, persist_dir=persist_dir
        ),
        "EPISTEMIC_GRAPH_CONTROL_LEASE_KIND_POLICY_JSON": POLICY,
    }
    proc = subprocess.Popen(
        [SERVER_BIN, "--socket-path", sock],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not os.path.exists(sock):
        time.sleep(0.05)
    assert os.path.exists(sock), "lease-kind-policy test server did not start"
    boot = _connect(sock, bootstrap_context())
    try:
        boot.consensus.bootstrap_system_identity(
            agent_id=TEST_AGENT_ID, signer_id=TEST_AGENT_ID, signer_key=TEST_SIGNER_KEY
        )
    finally:
        boot.close()
    system = _connect(sock, request_context())
    try:
        system.rbac.add_role("lease-writer")
        for action in ("Read", "Write"):
            system.rbac.add_grant("lease-writer", {"Graph": "__commons__"}, action)
        for agent in (DEPUTY, OTHER):
            system.consensus.register_identity(
                agent,
                "Agent",
                ["team:test"],
                ["lease-writer"],
                signer_id=TEST_AGENT_ID,
                signer_key=TEST_SIGNER_KEY,
            )
    finally:
        system.close()
    yield sock
    proc.terminate()
    proc.wait(timeout=10)


def _connect(sock, context):
    return SyncEpistemicGraphClient.connect(
        socket_path=sock,
        auth_secret=SECRET,
        graph_name="__commons__",
        verified_context=context,
    )


def _issue(client, kind):
    now = int(time.time() * 1000)
    lease_id = f"lease-{uuid.uuid4().hex[:12]}"
    return client.control_leases.issue(
        tenant=TEST_TENANT,
        lease_id=lease_id,
        kind=kind,
        grant={"why": "test"},
        issued_at_ms=now,
        expires_at_ms=now + 60_000,
        hard_expires_at_ms=now + 60_000,
        idempotency_key=f"issue-{lease_id}",
    )


def _revoke(client, lease):
    return client.control_leases.transition(
        tenant=TEST_TENANT,
        lease_id=lease["lease_id"],
        expected_revision=lease["revision"],
        to="revoked",
        idempotency_key=f"revoke-{lease['lease_id']}-{uuid.uuid4().hex[:6]}",
    )


@pytest.fixture
def deputy(policy_server):
    client = _connect(policy_server, request_context(agent_id=DEPUTY))
    yield client
    client.close()


@pytest.fixture
def other(policy_server):
    client = _connect(policy_server, request_context(agent_id=OTHER))
    yield client
    client.close()


def test_the_restricted_principal_writes_its_allowed_kind(deputy):
    issued = _issue(deputy, "finance.order-proposal")
    assert issued["outcome"] == "issued"
    assert _revoke(deputy, issued["lease"])["outcome"] == "applied"


@pytest.mark.parametrize("kind", ["browser.control", "rbac.elevation"])
def test_the_restricted_principal_is_refused_any_other_kind(deputy, kind):
    with pytest.raises(
        RuntimeError, match="ACCESS_DENIED.*may not write control leases"
    ):
        _issue(deputy, kind)


def test_the_restricted_principal_cannot_transition_another_kind(deputy, other):
    foreign = _issue(other, "browser.control")["lease"]
    with pytest.raises(RuntimeError, match="ACCESS_DENIED.*'browser.control'"):
        _revoke(deputy, foreign)
    assert _revoke(other, foreign)["outcome"] == "applied"


@pytest.mark.spec("EG-DURABLE-KERNEL-R030")
@pytest.mark.parametrize("kind", ["browser.control", "finance.order-proposal"])
def test_other_principals_are_unaffected(other, kind):
    assert _issue(other, kind)["outcome"] == "issued"


@pytest.mark.spec("EG-DURABLE-KERNEL-R030")
def test_other_principal_cannot_bypass_reserved_elevation_flow(other):
    with pytest.raises(RuntimeError, match="reserved for RbacElevation"):
        _issue(other, "rbac.elevation")
