"""Reference-counted graceful shutdown proof (CONCEPT:EG-KG.backend.tiny-shared).

Two contracts, both proven against the actual compiled binary on a private socket
+ persist dir (self-contained, independent of the session-wide ``full`` fixture):

1. **Idle shutdown** (``--idle-shutdown-secs 1``): the server self-terminates a
   grace period after its last client disconnects. We connect (writing a node so a
   real durable mutation is committed-before-ack), disconnect, then assert the
   process EXITS on its own within ~the grace window — and that the already
   committed durable state reloads with the node intact. A connection during the
   grace period would reset the timer, so we also assert the process is still alive
   immediately after a disconnect (the timer has not elapsed yet).

2. **SIGTERM is graceful**: a long-living (no idle flag) server is sent SIGTERM
   (what a supervisor / ``kill`` / agent-utilities stop uses) and must exit cleanly
   (code 0) without dropping the already committed write, proving the accept loop
   breaks on the signal and the process exits cleanly.

Both use the stock ``full`` build, which is redb-authoritative by default
(CONCEPT:AU-KG.backend.backend-modes) so every ``nodes.add`` is commit-before-ack.
"""

import os
import signal
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
AUTH_SECRET = "test-graceful-shutdown-secret"


def _build_full() -> str | None:
    r = subprocess.run(
        ["cargo", "build", "--features", "full"],
        cwd=RUST_DIR,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        print(r.stderr)
        return None
    binary = os.path.join(RUST_DIR, "target", "debug", "epistemic-graph-server")
    return binary if os.path.exists(binary) else None


def _launch(
    binary: str, socket_path: str, persist_dir: str, extra_env: dict
) -> subprocess.Popen:
    env = {
        **os.environ,
        **strict_server_env(
            os.path.join(persist_dir, "security"), auth_secret=AUTH_SECRET
        ),
        **extra_env,
    }
    if os.path.exists(socket_path):
        os.remove(socket_path)
    proc = subprocess.Popen(
        [binary, "--socket-path", socket_path, "--persist-dir", persist_dir],
        cwd=RUST_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for _ in range(120):
        if os.path.exists(socket_path):
            return proc
        time.sleep(0.25)
    proc.kill()
    raise RuntimeError("server did not bind its socket in time")


def _bootstrap(socket_path: str) -> None:
    client = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        auth_secret=AUTH_SECRET,
        verified_context=bootstrap_context(),
    )
    try:
        client.consensus.bootstrap_system_identity(
            agent_id=TEST_AGENT_ID,
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
    finally:
        client.close()


@pytest.mark.concept("CONCEPT:EG-KG.backend.tiny-shared")
def test_idle_shutdown_self_terminates_and_checkpoints(tmp_path):
    binary = _build_full()
    if binary is None:
        pytest.skip("cargo build --features full failed in this environment")
    assert binary is not None  # narrow for the type checker (skip raises above)

    socket_path = str(tmp_path / "idle.sock")
    persist_dir = str(tmp_path / "persist")
    os.makedirs(persist_dir, exist_ok=True)

    proc = _launch(
        binary,
        socket_path,
        persist_dir,
        {"EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS": "1"},
    )
    _bootstrap(socket_path)
    try:
        # Connect, write an acked durable node, then disconnect.
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path,
            graph_name="idle:test",
            auth_secret=AUTH_SECRET,
            verified_context=request_context(),
        )
        client.tenants.create("idle:test")
        client.nodes.add("survivor", {"type": "Node"})
        client.close()

        # Immediately after disconnect the 1s idle timer has NOT yet elapsed, so the
        # daemon must still be alive (a connection during the grace period resets it).
        assert proc.poll() is None, "server died before the idle grace period elapsed"

        # Within the grace window + slack the daemon must self-terminate.
        try:
            rc = proc.wait(timeout=4)
        except subprocess.TimeoutExpired:
            pytest.fail("idle-shutdown server did not self-terminate within 4s")
        assert rc == 0, f"idle shutdown exited non-zero: {rc}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    # Restart from the SAME persist dir (no idle flag): the committed node reloads.
    proc2 = _launch(binary, socket_path, persist_dir, {})
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path,
            graph_name="idle:test",
            auth_secret=AUTH_SECRET,
            verified_context=request_context(),
        )
        props = client.nodes.properties("survivor")
        assert props is not None, "acked node lost across idle shutdown"
        client.close()
    finally:
        proc2.terminate()
        try:
            proc2.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc2.kill()
            proc2.wait()
    if os.path.exists(socket_path):
        os.remove(socket_path)


@pytest.mark.concept("CONCEPT:EG-KG.backend.tiny-shared")
def test_sigterm_is_graceful(tmp_path):
    binary = _build_full()
    if binary is None:
        pytest.skip("cargo build --features full failed in this environment")
    assert binary is not None  # narrow for the type checker (skip raises above)

    socket_path = str(tmp_path / "sigterm.sock")
    persist_dir = str(tmp_path / "persist")
    os.makedirs(persist_dir, exist_ok=True)

    # No idle flag → long-living/persistent mode; SIGTERM must still be graceful.
    proc = _launch(binary, socket_path, persist_dir, {})
    _bootstrap(socket_path)
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path,
            graph_name="sigterm:test",
            auth_secret=AUTH_SECRET,
            verified_context=request_context(),
        )
        client.tenants.create("sigterm:test")
        client.nodes.add("survivor", {"type": "Node"})
        client.close()

        # A persistent server stays up while idle.
        time.sleep(1.0)
        assert proc.poll() is None, "persistent server self-terminated while idle"

        # SIGTERM → clean exit (code 0).
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pytest.fail("server did not shut down within 10s of SIGTERM")
        assert rc == 0, f"SIGTERM shutdown exited non-zero: {rc}"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    # The acked node survived graceful SIGTERM.
    proc2 = _launch(binary, socket_path, persist_dir, {})
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path,
            graph_name="sigterm:test",
            auth_secret=AUTH_SECRET,
            verified_context=request_context(),
        )
        props = client.nodes.properties("survivor")
        assert props is not None, "acked node lost across SIGTERM"
        client.close()
    finally:
        proc2.terminate()
        try:
            proc2.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc2.kill()
            proc2.wait()
    if os.path.exists(socket_path):
        os.remove(socket_path)
