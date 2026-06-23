"""Scale + deadlock regression for the one-time legacy → redb migration.

CONCEPT:KG-2.200. THE FLIP (CONCEPT:KG-2.195) made redb-authoritative the
default; the first authoritative boot over a pre-flip persist dir runs a one-time
``.mp``/``.wal`` → redb import (``src/main.rs``: detect "redb empty + legacy
present" → ``persist::load_all`` → ``checkpoint_all``).

That path was only ever exercised on ~1 graph / ~50 nodes, and it HUNG on a real
homelab dir (thousands of graphs + a large graph): a ``state.read().await`` guard
created in the ``if let`` scrutinee lived to the end of the ``if let`` block and
was therefore held ACROSS ``persist::load_all``'s ``state.write().await`` — a
permanent RwLock deadlock. The process went idle, redb stayed frozen at its tiny
bootstrap size, and the UDS socket was NEVER bound, so the engine never served.

This test reproduces the *shape* that triggered it — MANY graphs + one larger
graph — and asserts the migration now:
  (a) reaches "Listening on UDS" (no deadlock) and serves,
  (b) serves the correct node counts for the migrated graphs,
  (c) logs migration progress (the silent-hang half of the bug),
  (d) on a SECOND boot loads from redb and does NOT re-run the migration.

The fixture is produced by the engine itself in ``snapshot`` mode (its own ``.mp``
format, guaranteed loadable), then re-opened in redb-authoritative mode — exactly
the pre-flip → post-flip transition a deployed shard goes through.
"""

import os
import signal
import socket as _socket
import subprocess
import time

import pytest

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AUTH_SECRET = "redb-migration-scale-secret"  # sanitizer:ignore — test-only

# Many graphs (exercises the O(graphs) load + checkpoint path) + one larger graph
# (exercises a big single-graph snapshot decode). Kept modest so the test runs in
# CI in seconds while still crossing the PROGRESS_EVERY (500) threshold so the
# progress-logging branch fires.
N_SMALL_GRAPHS = 600
NODES_PER_SMALL = 3
LARGE_GRAPH = "biggraph"
LARGE_NODES = 4000


def _free_socket_path(tag: str) -> str:
    return f"/tmp/test_redb_mig_{tag}_{os.getpid()}.sock"


def _build_redb() -> str | None:
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


def _launch(
    binary: str, socket_path: str, persist_dir: str, backend: str, log_path: str
) -> subprocess.Popen:
    """Launch the server with the given persist backend; wait for the UDS socket to
    actually bind (a connect round-trip). Raises if it exits early OR never binds —
    the "never binds" path is exactly the deadlock this test guards against. The
    server's tracing output (stderr) is sent to ``log_path`` so the migration log is
    reliably captured even across a SIGKILL (a PIPE drops unread buffered data)."""
    env = {
        **os.environ,
        "GRAPH_SERVICE_AUTH_SECRET": AUTH_SECRET,
        "EPISTEMIC_GRAPH_PERSIST_BACKEND": backend,
    }
    if os.path.exists(socket_path):
        os.remove(socket_path)
    log_fh = open(log_path, "wb")
    proc = subprocess.Popen(
        [binary, "--socket-path", socket_path, "--persist-dir", persist_dir],
        cwd=RUST_DIR,
        env=env,
        stdout=log_fh,
        stderr=log_fh,
    )
    proc._log_fh = log_fh  # type: ignore[attr-defined]  # keep the fd open for the proc's life
    for _ in range(240):  # up to 120s — the migration runs BEFORE the bind
        if os.path.exists(socket_path):
            try:
                s = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
                s.connect(socket_path)
                s.close()
                return proc
            except OSError:
                pass
        if proc.poll() is not None:
            raise RuntimeError(
                "server exited early: " + _read_log(log_path)[-2000:]
            )
        time.sleep(0.5)
    # Never bound = the deadlock regression. Kill + surface the log.
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    raise RuntimeError(
        "server never bound its UDS socket (migration deadlock regression): "
        + _read_log(log_path)[-2000:]
    )


def _read_log(log_path: str) -> str:
    with open(log_path, "rb") as fh:
        return fh.read().decode(errors="replace")


def _drain(proc: subprocess.Popen, log_path: str) -> str:
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    fh = getattr(proc, "_log_fh", None)
    if fh is not None:
        fh.flush()
    return _read_log(log_path)


@pytest.mark.timeout(600)
def test_legacy_snapshot_migration_scales_and_serves(tmp_path):
    binary = _build_redb()
    if binary is None:
        pytest.skip("redb+server build failed in this environment")

    persist_dir = str(tmp_path / "store")
    os.makedirs(persist_dir, exist_ok=True)
    socket_path = _free_socket_path("scale")
    log1 = str(tmp_path / "boot1.log")
    log2_path = str(tmp_path / "boot2.log")
    log3_path = str(tmp_path / "boot3.log")

    # ── Phase 1: build a realistic LEGACY snapshot dir via the engine's own
    # snapshot backend (no redb file yet). This writes real `.mp` + manifest in the
    # exact on-disk format the migration must read. ──
    proc = _launch(binary, socket_path, persist_dir, backend="snapshot", log_path=log1)
    try:
        for g in range(N_SMALL_GRAPHS):
            name = f"tenant{g}"
            c = SyncEpistemicGraphClient.connect(
                socket_path=socket_path, graph_name=name, auth_secret=AUTH_SECRET
            )
            c.tenants.create(name)
            for i in range(NODES_PER_SMALL):
                c.nodes.add(f"n{i}", {"type": "T", "g": g, "i": i})
            c.close()
        c = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=LARGE_GRAPH, auth_secret=AUTH_SECRET
        )
        c.tenants.create(LARGE_GRAPH)
        for i in range(LARGE_NODES):
            c.nodes.add(f"n{i}", {"type": "Big", "i": i})
        c.close()
        # Force a checkpoint so every graph has a durable `.mp` on disk, then a clean
        # shutdown writes the manifest. We just kill after a short settle — the
        # snapshot backend checkpoints on its interval AND on the periodic tick; to
        # be deterministic, request an explicit checkpoint via lifecycle.
        admin = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name="__commons__", auth_secret=AUTH_SECRET
        )
        admin.checkpoint()
        admin.close()
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait()

    # The snapshot dir must now hold many `.mp` files but NO redb store yet.
    mp_files = [f for f in os.listdir(persist_dir) if f.endswith(".mp")]
    assert len(mp_files) >= N_SMALL_GRAPHS, f"only {len(mp_files)} .mp files written"
    assert not os.path.exists(os.path.join(persist_dir, "graph.redb"))

    # ── Phase 2: boot redb-authoritative against the legacy dir → migration runs. ──
    proc2 = _launch(binary, socket_path, persist_dir, backend="redb", log_path=log2_path)
    log2 = ""
    try:
        # (a) it bound the socket (no deadlock) — _launch already proved that.
        # (b) serves the correct node counts for migrated graphs.
        cl = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=LARGE_GRAPH, auth_secret=AUTH_SECRET
        )
        assert cl.nodes.count() == LARGE_NODES
        cl.close()
        for g in (0, N_SMALL_GRAPHS // 2, N_SMALL_GRAPHS - 1):
            cc = SyncEpistemicGraphClient.connect(
                socket_path=socket_path, graph_name=f"tenant{g}", auth_secret=AUTH_SECRET
            )
            assert cc.nodes.count() == NODES_PER_SMALL, f"tenant{g} wrong count"
            cc.close()
    finally:
        log2 = _drain(proc2, log2_path)

    # (c) migration progress was logged (the silent-hang half of the fix).
    assert "one-time migration" in log2
    assert "imported" in log2 and "into redb" in log2
    assert "Snapshot load progress" in log2, "expected periodic progress logging"
    assert "Listening on UDS" in log2

    # ── Phase 3: a SECOND redb boot loads from redb and does NOT re-migrate. ──
    proc3 = _launch(binary, socket_path, persist_dir, backend="redb", log_path=log3_path)
    try:
        cl = SyncEpistemicGraphClient.connect(
            socket_path=socket_path, graph_name=LARGE_GRAPH, auth_secret=AUTH_SECRET
        )
        assert cl.nodes.count() == LARGE_NODES
        cl.close()
    finally:
        log3 = _drain(proc3, log3_path)
        if os.path.exists(socket_path):
            os.remove(socket_path)

    assert "one-time migration" not in log3, "migration re-ran on a redb-populated boot"
    assert "loaded" in log3 and "graph(s) from redb" in log3
