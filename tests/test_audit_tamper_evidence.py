"""Tamper-evident audit round-trip against a live engine (CONCEPT:EG-KG.sharding.row-level-security,
feature `security`, folded into `full`).

Proves the `AdminClient.audit_verify` / `audit_prove_inclusion` bindings (added for
B-2 — `Method::AuditVerify`/`AuditProveInclusion` were handler-complete and compiled
into the default `full` build but had zero occurrences in `epistemic_graph/client.py`)
actually work end-to-end: commit real mutations, prove the hash-chained audit log
verifies clean, prove a Merkle inclusion proof for a real anchored node, and prove
verification actually FAILS when that node's durable content is changed after
anchoring -- a verifier never shown to reject anything is not a verifier.

`audit_prove_inclusion` needs at least one provenance anchor to exist
(`EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS`), which the shared session engine in
`conftest.py` does not enable (it would add a background sweep to every other
test in the suite for a feature only this file exercises). This module starts its
OWN short-lived server, with that sweep armed at a 1s interval, reusing the SAME
already-built `full` binary the session fixture built -- mirrors the
`test_service_layer.py` "own service fixture" pattern.
"""

from __future__ import annotations

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

from epistemic_graph.client import SyncEpistemicGraphClient

_SERVER_BIN = find_server_binary()

pytestmark = [
    pytest.mark.concept("CONCEPT:EG-KG.sharding.row-level-security"),
    pytest.mark.skipif(
        _SERVER_BIN is None,
        reason="epistemic-graph-server (full) binary not built",
    ),
]


@pytest.fixture(scope="module")
def anchor_service():
    """A dedicated server with provenance anchoring armed at a 1s tick."""
    tmpdir = tempfile.mkdtemp(prefix="eg-audit-")
    socket_path = os.path.join(tmpdir, "test.sock")
    secret = "audit-test-secret"  # sanitizer:ignore — test-only value
    persist_dir = os.path.join(tmpdir, "persist")
    os.makedirs(persist_dir, exist_ok=True)

    env = {
        **os.environ,
        **strict_server_env(os.path.join(tmpdir, "security"), auth_secret=secret),
        "GRAPH_SERVICE_PERSIST_DIR": persist_dir,
        "EPISTEMIC_GRAPH_PROVENANCE_ANCHOR_SECS": "1",
    }

    proc = subprocess.Popen(
        [_SERVER_BIN, "--socket-path", socket_path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for _ in range(50):
        if os.path.exists(socket_path):
            break
        time.sleep(0.1)
    else:
        out, err = proc.communicate(timeout=5)
        proc.kill()
        pytest.fail(
            f"anchor_service failed to start within 5s\nstdout={out!r}\nstderr={err!r}"
        )

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

    yield {"socket_path": socket_path, "auth_secret": secret}

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    import shutil

    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def client(anchor_service):
    c = SyncEpistemicGraphClient.connect(
        socket_path=anchor_service["socket_path"],
        auth_secret=anchor_service["auth_secret"],
        verified_context=request_context(),
    )
    c.graph.clear()
    yield c
    c.close()


def test_audit_verify_clean_chain(client):
    # `client` (function-scoped) clears the graph before yielding, and that
    # `ClearGraph` durable write is itself an audited entry (every `(graph,
    # method)` durable commit is -- `redb_store::append_audit_entry`), so the
    # baseline is 1 entry, not 0, before this test's own writes.
    before = client.admin.audit_verify()
    assert before["ok"] is True

    client.nodes.add("n1", {"type": "Widget", "v": 1})
    client.nodes.add("n2", {"type": "Widget", "v": 2})

    report = client.admin.audit_verify()
    assert report["ok"] is True, f"expected a clean chain: {report}"
    assert report["graph"] == "__commons__"
    assert report["entries"] == before["entries"] + 2
    assert report["first_broken_seq"] is None


def _wait_for_anchor(client, node_id: str, *, timeout_s: float = 10.0) -> dict:
    """Poll audit_prove_inclusion until the periodic anchor sweep has run at
    least once (1s tick -- see `anchor_service`). Raises the last error if the
    graph never gets anchored within the budget."""
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            report = client.admin.audit_prove_inclusion(node_id)
            if report["included"]:
                return report
        except Exception as error:  # engine reports "no anchor yet" as an error
            last_error = error
        time.sleep(0.25)
    raise AssertionError(f"node never got anchored within {timeout_s}s: {last_error}")


def test_audit_prove_inclusion_then_detects_ordinary_overwrite_as_tamper(client):
    # Provenance anchoring only windows :ToolCall/:RunTrace-labeled nodes
    # (src/server/persistence/provenance_anchor.rs::PROVENANCE_LABELS) -- an
    # ordinary node is never in scope, which this test also checks below.
    client.nodes.add("tc-1", {"node_type": "ToolCall", "v": 1})
    client.nodes.add("widget-1", {"node_type": "Widget", "v": 1})

    clean = _wait_for_anchor(client, "tc-1")
    assert clean["included"] is True
    assert clean["verified"] is True, f"freshly anchored node must verify: {clean}"
    assert clean["node_id"] == "tc-1"
    assert clean["graph"] == "__commons__"
    assert clean["anchored_root_sha256"] == clean["computed_root_sha256"]
    anchor_seq = clean["anchor_seq"]

    # A node that was never in the window: included=False, never a false "verified".
    out_of_window = client.admin.audit_prove_inclusion("widget-1", anchor_seq=anchor_seq)
    assert out_of_window["included"] is False
    assert out_of_window["verified"] is False

    # Overwrite tc-1's durable content through the ORDINARY served write path (not
    # a raw byte-flip -- the realistic tamper/insider-edit scenario), THEN re-check
    # against the SAME anchor.
    client.nodes.add("tc-1", {"node_type": "ToolCall", "v": "TAMPERED"})

    tampered = client.admin.audit_prove_inclusion("tc-1", anchor_seq=anchor_seq)
    assert tampered["included"] is True, "tc-1 is still part of that anchor's window"
    assert tampered["verified"] is False, (
        f"a changed node must fail inclusion verification against its old anchor: {tampered}"
    )
    assert tampered["computed_root_sha256"] != tampered["anchored_root_sha256"]
    # The ANCHORED root itself (chain-protected) must not have moved.
    assert tampered["anchored_root_sha256"] == clean["anchored_root_sha256"]

    # Provenance anchoring must never itself break the audit chain AuditVerify walks.
    audit_report = client.admin.audit_verify()
    assert audit_report["ok"] is True, f"chain must stay clean throughout: {audit_report}"
