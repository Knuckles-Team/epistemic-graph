"""Current request-context authentication has no insecure startup path."""

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


def _clean_env(socket_path, *, auth_secret, **overrides):
    """Return an isolated strict server environment for one process."""
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
        )
    }
    env.update(strict_server_env(f"{socket_path}.security", auth_secret=auth_secret))
    env.update(overrides)
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
    assert "authentication secret" in stderr.lower()
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
