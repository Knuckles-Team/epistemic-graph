"""Security gate: the server refuses to start without an auth secret.

An empty HMAC secret means every request is accepted unauthenticated, so the
server must only run that way behind an explicit insecure opt-out
(--allow-insecure / EPISTEMIC_GRAPH_ALLOW_INSECURE=1).
"""

import os
import subprocess
import time

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SERVER_BIN = os.path.join(RUST_DIR, "target", "debug", "epistemic-graph-server")


def _clean_env(**overrides):
    """Inherited env minus any auth/insecure knobs the session fixture sets."""
    env = {
        k: v
        for k, v in os.environ.items()
        if k not in ("GRAPH_SERVICE_AUTH_SECRET", "EPISTEMIC_GRAPH_ALLOW_INSECURE")
    }
    env.update(overrides)
    return env


def _spawn(socket_path, *extra_args, **env_overrides):
    return subprocess.Popen(
        [SERVER_BIN, "--socket-path", socket_path, *extra_args],
        env=_clean_env(**env_overrides),
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
    assert "refusing to start" in stderr
    assert "EPISTEMIC_GRAPH_ALLOW_INSECURE" in stderr
    assert not os.path.exists(sock)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_allow_insecure_flag_starts_and_warns(tmp_path):
    sock = str(tmp_path / "insecure-flag.sock")
    proc = _spawn(sock, "--allow-insecure")
    try:
        assert _wait_for_socket(sock), "server did not bind with --allow-insecure"
        client = SyncEpistemicGraphClient.connect(socket_path=sock, auth_secret="")
        try:
            assert client.ping() == "pong"
        finally:
            client.close()
    finally:
        proc.terminate()
        stdout, stderr = proc.communicate(timeout=10)
    # tracing's fmt subscriber writes to stdout.
    assert "WITHOUT authentication" in stdout.decode() + stderr.decode()


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_allow_insecure_env_starts(tmp_path):
    sock = str(tmp_path / "insecure-env.sock")
    proc = _spawn(sock, EPISTEMIC_GRAPH_ALLOW_INSECURE="1")
    try:
        assert _wait_for_socket(sock), "server did not bind with env opt-out"
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.mark.concept("CONCEPT:EG-KG.query.wire-protocol")
def test_wrong_secret_is_rejected(tmp_path):
    sock = str(tmp_path / "secret.sock")
    proc = _spawn(sock, GRAPH_SERVICE_AUTH_SECRET="right-secret")
    try:
        assert _wait_for_socket(sock)

        good = SyncEpistemicGraphClient.connect(
            socket_path=sock, auth_secret="right-secret"
        )
        try:
            assert good.ping() == "pong"
        finally:
            good.close()

        bad = SyncEpistemicGraphClient.connect(
            socket_path=sock, auth_secret="wrong-secret"
        )
        try:
            with pytest.raises(RuntimeError, match="Authentication failed"):
                bad.ping()
        finally:
            bad.close()
    finally:
        proc.terminate()
        proc.wait(timeout=10)
