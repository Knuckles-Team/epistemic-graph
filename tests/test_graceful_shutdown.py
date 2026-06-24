"""Reference-counted graceful shutdown proof (CONCEPT:KG-2.223).

Two contracts, both proven against the actual compiled binary on a private socket
+ persist dir (self-contained, independent of the session-wide ``full`` fixture):

1. **Idle shutdown** (``--idle-shutdown-secs 1``): the server self-terminates a
   grace period after its last client disconnects. We connect (writing a node so a
   real durable mutation is committed-before-ack), disconnect, then assert the
   process EXITS on its own within ~the grace window — and that the durable state
   it checkpointed on the way out reloads with the node intact (shutdown
   checkpoints, it does NOT drop acked writes). A connection arriving during the
   grace period would reset the timer, so we also assert the process is still alive
   immediately after a disconnect (the timer has not elapsed yet).

2. **SIGTERM is graceful**: a long-living (no idle flag) server is sent SIGTERM
   (what a supervisor / ``kill`` / agent-utilities stop uses) and must exit cleanly
   (code 0) with the acked write durably checkpointed — proving the accept loop now
   breaks on the signal and ``main()`` falls through to the persistence flush
   (previously dead code).

Both use the stock ``full`` build, which is redb-authoritative by default
(CONCEPT:KG-2.195) so every ``nodes.add`` is commit-before-ack.
"""

import os
import signal
import subprocess
import time

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AUTH_SECRET = "graceful-shutdown-test-secret"  # sanitizer:ignore — test-only


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
    env = {**os.environ, "GRAPH_SERVICE_AUTH_SECRET": AUTH_SECRET, **extra_env}
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


@pytest.mark.concept("CONCEPT:KG-2.223")
def test_idle_shutdown_self_terminates_and_checkpoints(tmp_path):
    binary = _build_full()
    if binary is None:
        pytest.skip("cargo build --features full failed in this environment")
    assert binary is not None  # narrow for the type checker (skip raises above)

    socket_path = f"/tmp/test_idle_shutdown_{os.getpid()}.sock"
    persist_dir = str(tmp_path / "persist")
    os.makedirs(persist_dir, exist_ok=True)

    proc = _launch(
        binary,
        socket_path,
        persist_dir,
        {"EPISTEMIC_GRAPH_IDLE_SHUTDOWN_SECS": "1"},
    )
    try:
        # Connect, write an acked durable node, then disconnect.
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name="idle:test", auth_secret=AUTH_SECRET
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

    # Restart from the SAME persist dir (no idle flag) — the node checkpointed on
    # graceful exit must reload (shutdown checkpoints, never drops acked writes).
    proc2 = _launch(binary, socket_path, persist_dir, {})
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name="idle:test", auth_secret=AUTH_SECRET
        )
        props = client.nodes.properties("survivor")
        assert props is not None, "acked node lost across idle-shutdown checkpoint"
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


@pytest.mark.concept("CONCEPT:KG-2.223")
def test_sigterm_is_graceful(tmp_path):
    binary = _build_full()
    if binary is None:
        pytest.skip("cargo build --features full failed in this environment")
    assert binary is not None  # narrow for the type checker (skip raises above)

    socket_path = f"/tmp/test_sigterm_{os.getpid()}.sock"
    persist_dir = str(tmp_path / "persist")
    os.makedirs(persist_dir, exist_ok=True)

    # No idle flag → long-living/persistent mode; SIGTERM must still be graceful.
    proc = _launch(binary, socket_path, persist_dir, {})
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name="sigterm:test", auth_secret=AUTH_SECRET
        )
        client.tenants.create("sigterm:test")
        client.nodes.add("survivor", {"type": "Node"})
        client.close()

        # A persistent server stays up while idle.
        time.sleep(1.0)
        assert proc.poll() is None, "persistent server self-terminated while idle"

        # SIGTERM → clean checkpointed exit (code 0).
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

    # The acked node survived the graceful SIGTERM checkpoint.
    proc2 = _launch(binary, socket_path, persist_dir, {})
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name="sigterm:test", auth_secret=AUTH_SECRET
        )
        props = client.nodes.properties("survivor")
        assert props is not None, "acked node lost across SIGTERM checkpoint"
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
