"""Engine-side response-size guard (CONCEPT:EG-KG.ingest.resets-socket-so-assimilation) — round trip.

The unbounded ``GetNodes`` full-graph dump is refused once the graph exceeds the
``EPISTEMIC_GRAPH_MAX_RESPONSE_NODES`` cap: the engine returns a typed
``RESULT_TOO_LARGE`` error (NOT a gigabyte payload, NOT a dropped connection),
which the Python client surfaces as a catchable :class:`ResultTooLargeError`.

This spins up a DEDICATED server with a low cap (2) on its own socket so the
threshold is reachable with a handful of nodes — the session-wide server in
``conftest.py`` keeps the production default (50_000).
"""

from __future__ import annotations

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

from epistemic_graph.client import ResultTooLargeError, SyncEpistemicGraphClient

_SECRET = "test-epistemic-graph-secret"
_CAP = 2


@pytest.fixture(scope="module")
def capped_server(tmp_path_factory: pytest.TempPathFactory):
    rust_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    runtime = tmp_path_factory.mktemp("eg-guard")
    sock = str(runtime / "capped.sock")

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
        [
            "cargo",
            "run",
            "--features",
            # Not plain `full`: this module and `test_viz_client.py`/
            # `test_epi_gapfill_roundtrip.py`/`test_graceful_shutdown.py` all
            # build to the SAME shared `target-isolated` output (`.cargo/
            # config.toml`); if they requested different feature strings,
            # whichever ran later would pay a ~40-60s relink of the earlier
            # one's build -- often enough to blow the 60s pytest-timeout.
            # `viz-static-export` is unused here but a strict superset is
            # always a safe substitute for `full` alone.
            "full viz-static-export",
            "--bin",
            "epistemic-graph-server",
            "--",
            "--socket-path",
            sock,
        ],
        cwd=rust_dir,
        env={
            **os.environ,
            **strict_server_env(
                str(runtime / "security"),
                auth_secret=_SECRET,
                persist_dir=persist_dir,
            ),
            "EPISTEMIC_GRAPH_MAX_RESPONSE_NODES": str(_CAP),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(120):
            if os.path.exists(sock):
                break
            time.sleep(0.5)
        else:
            pytest.fail("capped epistemic-graph-server did not come up")
        bootstrap = SyncEpistemicGraphClient.connect(
            socket_path=sock,
            auth_secret=_SECRET,
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
    finally:
        proc.terminate()
        proc.wait()


def test_under_cap_returns_data(capped_server: str) -> None:
    client = SyncEpistemicGraphClient.connect(
        socket_path=capped_server,
        graph_name="guard:under",
        auth_secret=_SECRET,
        verified_context=request_context(),
    )
    client.tenants.create("guard:under")
    client.nodes.add("a", {"type": "X"})
    client.nodes.add("b", {"type": "X"})  # exactly at the cap (2) — still allowed
    nodes = client.nodes.list()
    assert len(nodes) == 2


def test_over_cap_raises_result_too_large(capped_server: str) -> None:
    client = SyncEpistemicGraphClient.connect(
        socket_path=capped_server,
        graph_name="guard:over",
        auth_secret=_SECRET,
        verified_context=request_context(),
    )
    client.tenants.create("guard:over")
    for i in range(_CAP + 1):  # one past the cap
        client.nodes.add(f"n{i}", {"type": "X"})

    with pytest.raises(ResultTooLargeError) as excinfo:
        client.nodes.list()
    msg = str(excinfo.value)
    assert msg.startswith("RESULT_TOO_LARGE")
    assert "get_nodes_by_label" in msg

    # The bounded read is UNAFFECTED — it still returns data over the cap.
    bounded = client.nodes.list_by_label("X", 10)
    assert len(bounded) == _CAP + 1
