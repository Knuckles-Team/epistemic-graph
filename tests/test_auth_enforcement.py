"""Current request-context authentication has no insecure startup path."""

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
# subprocesses directly, so a hardcoded `target/debug` path never resolves in
# an ordinary checkout of this repo (see conftest.py's `find_server_binary` doc).
SERVER_BIN = find_server_binary() or os.path.join(
    RUST_DIR, "target", "debug", "epistemic-graph-server"
)


def _clean_env(socket_path, *, auth_secret, **overrides):
    """Return an isolated strict server environment for one process.

    An override value of `None` REMOVES that key entirely (rather than
    setting it to the string `"None"`), so a test can exercise the true
    unset/default state of a variable `strict_server_env` otherwise bakes in
    (e.g. `EPISTEMIC_GRAPH_REQUIRE_OIDC`).
    """
    env = {
        k: v
        for k, v in os.environ.items()
        if k
        not in (
            "GRAPH_SERVICE_AUTH_SECRET",
            "EPISTEMIC_GRAPH_ALLOW_INSECURE",
            "EPISTEMIC_GRAPH_PROFILE",
            "EPISTEMIC_GRAPH_AUDIENCE",
            "EPISTEMIC_GRAPH_TENANT",
            "EPISTEMIC_GRAPH_POLICY_VERSION",
            "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
            "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON",
            "EPISTEMIC_GRAPH_REQUIRE_OIDC",
            "EPISTEMIC_GRAPH_OIDC_JWT_ISSUER",
            "EPISTEMIC_GRAPH_OIDC_JWT_AUDIENCE",
            "EPISTEMIC_GRAPH_OIDC_JWKS_URL",
            "OIDC_ISSUER",
            "OIDC_AUDIENCE",
            "GRAPH_SERVICE_PERSIST_DIR",
        )
    }
    env.update(strict_server_env(f"{socket_path}.security", auth_secret=auth_secret))
    env.setdefault("GRAPH_SERVICE_PERSIST_DIR", f"{socket_path}.persist")
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


def _spawn(socket_path, *extra_args, auth_secret="", **env_overrides):
    return subprocess.Popen(
        [SERVER_BIN, "--socket-path", socket_path, *extra_args],
        env=_clean_env(socket_path, auth_secret=auth_secret, **env_overrides),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _wait_for_socket(socket_path, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(socket_path):
            return True
        time.sleep(0.05)
    return False


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_empty_secret_refuses_to_start(tmp_path):
    sock = str(tmp_path / "no-secret.sock")
    proc = _spawn(sock)
    try:
        proc.wait(timeout=10)
    finally:
        proc.kill()
    assert proc.returncode == 2
    stderr = proc.stderr.read().decode()
    # Matches `main.rs`'s actual refusal message ("no auth secret configured —
    # refusing to start... Set GRAPH_SERVICE_AUTH_SECRET ... HMAC-SHA256
    # authentication.") -- the exact phrase "authentication secret" never
    # appears in it (a stale assertion, not a stale message: the message reads
    # fine on its own merits).
    assert "no auth secret configured" in stderr.lower()
    assert not os.path.exists(sock)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_removed_insecure_flag_is_rejected(tmp_path):
    sock = str(tmp_path / "removed-flag.sock")
    proc = _spawn(sock, "--allow-insecure", auth_secret="test-request-secret")
    try:
        proc.wait(timeout=10)
    finally:
        proc.kill()
    assert proc.returncode == 2
    assert not os.path.exists(sock)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_removed_insecure_env_does_not_enable_unauthenticated_start(tmp_path):
    sock = str(tmp_path / "removed-env.sock")
    proc = _spawn(sock, EPISTEMIC_GRAPH_ALLOW_INSECURE="1")
    try:
        proc.wait(timeout=10)
    finally:
        proc.kill()
    assert proc.returncode == 2
    assert not os.path.exists(sock)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_oidc_required_by_default_refuses_to_start_without_issuer(tmp_path):
    """SECURE BY DEFAULT since 2026-07-22 (closes the Identity boundary seam,
    reports/seam-closure-audit-2026-07-22.md's highest-priority finding):
    with `EPISTEMIC_GRAPH_REQUIRE_OIDC` genuinely UNSET (not merely defaulted
    by this test harness's own `strict_server_env` opt-out — that override is
    explicitly removed here) and no OIDC issuer configured, the server must
    refuse to start rather than silently accept HMAC-only identity. Before
    2026-07-22 this exact scenario started successfully; a test that would
    pass either way is worthless — this only passes if the real production
    default is actually secure.
    """
    sock = str(tmp_path / "oidc-default-required.sock")
    proc = _spawn(
        sock,
        auth_secret="test-request-secret",
        EPISTEMIC_GRAPH_REQUIRE_OIDC=None,  # truly unset: exercise the real default
    )
    try:
        proc.wait(timeout=15)
    finally:
        proc.kill()
    assert proc.returncode == 1
    stderr = proc.stderr.read().decode()
    assert "EPISTEMIC_GRAPH_REQUIRE_OIDC" in stderr, stderr
    assert not os.path.exists(sock)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_oidc_required_explicitly_also_refuses_to_start_without_issuer(tmp_path):
    """Same as above but with the posture set truthy explicitly, proving the
    gate responds to the variable itself and not just to some other unset
    dependency."""
    sock = str(tmp_path / "oidc-explicit-required.sock")
    proc = _spawn(
        sock,
        auth_secret="test-request-secret",
        EPISTEMIC_GRAPH_REQUIRE_OIDC="true",
    )
    try:
        proc.wait(timeout=15)
    finally:
        proc.kill()
    assert proc.returncode == 1
    stderr = proc.stderr.read().decode()
    assert "EPISTEMIC_GRAPH_REQUIRE_OIDC" in stderr, stderr
    assert not os.path.exists(sock)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_oidc_explicit_opt_out_allows_hmac_only_start(tmp_path):
    """The documented, deliberate escape hatch for local/dev use must still
    genuinely work end to end: `EPISTEMIC_GRAPH_REQUIRE_OIDC=false` restores
    pre-2026-07-22 HMAC-only-permitted startup AND request handling — the
    server must not just start, but actually serve an authenticated request
    with no OIDC token present."""
    sock = str(tmp_path / "oidc-explicit-opt-out.sock")
    proc = _spawn(
        sock,
        auth_secret="opt-out-secret",
        EPISTEMIC_GRAPH_REQUIRE_OIDC="false",
    )
    try:
        assert _wait_for_socket(sock)

        bootstrap = SyncEpistemicGraphClient.connect(
            socket_path=sock,
            auth_secret="opt-out-secret",
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

        good = SyncEpistemicGraphClient.connect(
            socket_path=sock,
            auth_secret="opt-out-secret",
            verified_context=request_context(),
        )
        try:
            assert good.ping() == "pong"
        finally:
            good.close()
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_wrong_secret_is_rejected(tmp_path):
    sock = str(tmp_path / "secret.sock")
    proc = _spawn(sock, auth_secret="right-secret")
    try:
        assert _wait_for_socket(sock)

        bootstrap = SyncEpistemicGraphClient.connect(
            socket_path=sock,
            auth_secret="right-secret",
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

        good = SyncEpistemicGraphClient.connect(
            socket_path=sock,
            auth_secret="right-secret",
            verified_context=request_context(),
        )
        try:
            assert good.ping() == "pong"
        finally:
            good.close()

        bad = SyncEpistemicGraphClient.connect(
            socket_path=sock,
            auth_secret="wrong-secret",
            verified_context=request_context(),
        )
        try:
            with pytest.raises(RuntimeError, match="Authentication failed"):
                bad.ping()
        finally:
            bad.close()
    finally:
        proc.terminate()
        proc.wait(timeout=10)
