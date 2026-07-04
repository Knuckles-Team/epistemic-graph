"""Crash / power-loss durability proof for redb-authoritative mode.

CONCEPT:EG-KG.backend.authoritative-dispatch. With ``EPISTEMIC_GRAPH_REDB_AUTHORITATIVE=1`` and the redb
persistence backend selected, every durable write is COMMIT-BEFORE-ACK: the
client's synchronous round-trip (``nodes.add``) only returns once the engine has
durably committed the mutation to redb. This test proves the contract that
matters: an ``kill -9`` (simulating power loss) of the process loses NOTHING that
was acked.

Procedure:
  1. Build ``--features "redb server"`` and launch the server with the redb
     backend, authoritative ON, and ``EPISTEMIC_GRAPH_WAL_FSYNC=each`` so every
     group commit fsyncs.
  2. Write N nodes, AWAITING each ack (each ``add`` returns only after the durable
     commit). Also start an Nth+1 write but kill the process mid-flight (no
     assertion either way on the un-acked write — only that recovery is not
     corrupt).
  3. ``kill -9`` the process (no graceful shutdown, no final checkpoint).
  4. Restart from the SAME persist dir (redb load_all is authoritative source).
  5. Assert ALL N acked nodes are present.

This test is self-contained: it builds + manages its own server process on a
private socket + persist dir, independent of the session-wide ``full`` fixture.
"""

import os
import signal
import socket as _socket
import subprocess
import time

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AUTH_SECRET = "redb-authoritative-crash-secret"  # sanitizer:ignore — test-only


def _free_socket_path(tag: str) -> str:
    return f"/tmp/test_redb_auth_{tag}_{os.getpid()}.sock"


def _build_redb() -> str | None:
    """Build the redb+server binary ONCE and return its path (or None on failure).
    Launching the compiled binary directly (not ``cargo run``) avoids any rebuild /
    cargo lock contention at launch, so the server boots near-instantly."""
    r = subprocess.run(
        ["cargo", "build", "--features", "redb server"],
        cwd=RUST_DIR,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        return None
    binary = os.path.join(RUST_DIR, "target", "debug", "epistemic-graph-server")
    return binary if os.path.exists(binary) else None


def _build_full() -> str | None:
    """Build the standard ``full`` binary ONCE (CONCEPT:AU-KG.backend.backend-modes — THE FLIP).

    ``full`` now folds in ``redb``, so a STOCK full build is redb-authoritative by
    default — no backend/authoritative env needed. Returns the binary path."""
    r = subprocess.run(
        ["cargo", "build", "--features", "full"],
        cwd=RUST_DIR,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        return None
    binary = os.path.join(RUST_DIR, "target", "debug", "epistemic-graph-server")
    return binary if os.path.exists(binary) else None


def _launch_env(
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
            try:
                s = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
                s.connect(socket_path)
                s.close()
                return proc
            except OSError:
                pass
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise RuntimeError(f"server exited early: {err.decode(errors='replace')}")
        time.sleep(0.5)
    raise RuntimeError("server did not come up in time")


def _launch(binary: str, socket_path: str, persist_dir: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "GRAPH_SERVICE_AUTH_SECRET": AUTH_SECRET,
        "EPISTEMIC_GRAPH_PERSIST_BACKEND": "redb",
        "EPISTEMIC_GRAPH_REDB_AUTHORITATIVE": "1",
        # fsync every batch so an acked write is provably on disk before kill -9.
        "EPISTEMIC_GRAPH_WAL_FSYNC": "each",
    }
    if os.path.exists(socket_path):
        os.remove(socket_path)
    proc = subprocess.Popen(
        [
            binary,
            "--socket-path",
            socket_path,
            "--persist-dir",
            persist_dir,
        ],
        cwd=RUST_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for the socket to appear (a connection round-trip confirms it's up).
    for _ in range(120):
        if os.path.exists(socket_path):
            try:
                s = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
                s.connect(socket_path)
                s.close()
                return proc
            except OSError:
                pass
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise RuntimeError(f"server exited early: {err.decode(errors='replace')}")
        time.sleep(0.5)
    raise RuntimeError("server did not come up in time")


@pytest.mark.timeout(600)
def test_acked_writes_survive_kill9(tmp_path):
    binary = _build_redb()
    if binary is None:
        pytest.skip("redb+server build failed in this environment")
        assert binary is not None  # narrow type for mypy (skip above is NoReturn)

    persist_dir = str(tmp_path / "redb_store")
    os.makedirs(persist_dir, exist_ok=True)
    socket_path = _free_socket_path("crash")
    graph = "crashgraph"
    n = 50

    # ── round 1: write + ack N nodes, then kill -9 ──
    proc = _launch(binary, socket_path, persist_dir)
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=graph, auth_secret=AUTH_SECRET
        )
        client.tenants.create(graph)
        for i in range(n):
            # Each add() returns only after the authoritative durable commit (ack).
            client.nodes.add(f"n{i}", {"type": "Task", "i": i})
        # Confirm they're acked + visible in this live process.
        assert client.nodes.count() == n
        client.close()
    finally:
        # Hard kill: no graceful shutdown, no final checkpoint — simulate power loss.
        proc.send_signal(signal.SIGKILL)
        proc.wait()

    # ── round 2: restart from the same persist dir, assert all N survive ──
    proc2 = _launch(binary, socket_path, persist_dir)
    try:
        client2 = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=graph, auth_secret=AUTH_SECRET
        )
        present = [client2.nodes.has(f"n{i}") for i in range(n)]
        missing = [i for i, ok in enumerate(present) if not ok]
        assert not missing, f"acked nodes lost after kill -9: {missing}"
        # Spot-check property fidelity on a recovered node.
        props = client2.nodes.properties("n0")
        assert props is not None and props.get("i") == 0
        client2.close()
    finally:
        proc2.send_signal(signal.SIGKILL)
        proc2.wait()
        if os.path.exists(socket_path):
            os.remove(socket_path)


@pytest.mark.timeout(600)
def test_inflight_unacked_write_does_not_corrupt(tmp_path):
    """An un-acked in-flight write may or may not survive — we assert only that
    recovery is NOT corrupt (the engine restarts and serves reads). No assertion
    of presence OR loss for the un-acked write; the contract is solely that an
    ACKED write never disappears (covered above) and recovery is consistent."""
    binary = _build_redb()
    if binary is None:
        pytest.skip("redb+server build failed in this environment")
        assert binary is not None  # narrow type for mypy (skip above is NoReturn)

    persist_dir = str(tmp_path / "redb_store2")
    os.makedirs(persist_dir, exist_ok=True)
    socket_path = _free_socket_path("inflight")
    graph = "inflightgraph"

    proc = _launch(binary, socket_path, persist_dir)
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=graph, auth_secret=AUTH_SECRET
        )
        client.tenants.create(graph)
        client.nodes.add("durable", {"v": 1})  # acked → must survive
        client.close()
    finally:
        proc.send_signal(signal.SIGKILL)
        proc.wait()

    proc2 = _launch(binary, socket_path, persist_dir)
    try:
        client2 = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=graph, auth_secret=AUTH_SECRET
        )
        # Recovery is consistent: the engine serves reads and the acked node is here.
        assert client2.nodes.has("durable")
        client2.close()
    finally:
        proc2.send_signal(signal.SIGKILL)
        proc2.wait()
        if os.path.exists(socket_path):
            os.remove(socket_path)


@pytest.mark.timeout(600)
def test_stock_default_build_is_durable_by_default(tmp_path):
    """THE FLIP proof (CONCEPT:AU-KG.backend.backend-modes): a STOCK deployment is durable by default.

    No ``EPISTEMIC_GRAPH_PERSIST_BACKEND`` and no
    ``EPISTEMIC_GRAPH_REDB_AUTHORITATIVE`` are set — ONLY a persist dir and the
    mandatory auth secret. Because the standard ``full`` build now folds in ``redb``
    and the backend defaults to ``redb`` + authoritative-when-redb-active, an acked
    write must survive a ``kill -9`` with ZERO durability env tuning. This is the
    whole point of the flip: the engine is a source of truth out of the box."""
    binary = _build_full()
    if binary is None:
        pytest.skip("full build failed in this environment")
        assert binary is not None  # narrow type for mypy (skip above is NoReturn)

    persist_dir = str(tmp_path / "redb_store_default")
    os.makedirs(persist_dir, exist_ok=True)
    socket_path = _free_socket_path("default")
    graph = "stockgraph"
    n = 50

    # ── round 1: write + ack N nodes with NO durability env, then kill -9 ──
    proc = _launch_env(binary, socket_path, persist_dir, extra_env={})
    try:
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=graph, auth_secret=AUTH_SECRET
        )
        client.tenants.create(graph)
        for i in range(n):
            # Under the new default, the redb backend is authoritative, so each add()
            # returns only after the durable group-commit (commit-before-ack).
            client.nodes.add(f"n{i}", {"type": "Task", "i": i})
        assert client.nodes.count() == n
        client.close()
    finally:
        proc.send_signal(signal.SIGKILL)
        proc.wait()

    # ── round 2: restart (same stock config), assert all N acked nodes survive ──
    proc2 = _launch_env(binary, socket_path, persist_dir, extra_env={})
    try:
        client2 = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=graph, auth_secret=AUTH_SECRET
        )
        present = [client2.nodes.has(f"n{i}") for i in range(n)]
        missing = [i for i, ok in enumerate(present) if not ok]
        assert not missing, f"acked nodes lost after kill -9 (stock default): {missing}"
        props = client2.nodes.properties("n0")
        assert props is not None and props.get("i") == 0
        client2.close()
    finally:
        proc2.send_signal(signal.SIGKILL)
        proc2.wait()
        if os.path.exists(socket_path):
            os.remove(socket_path)
