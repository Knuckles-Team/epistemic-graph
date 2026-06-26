"""Engine-side response-size guard (CONCEPT:KG-2.264) — round trip.

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

from epistemic_graph.client import ResultTooLargeError, SyncEpistemicGraphClient

_SECRET = "epistemic-graph-test-secret"  # sanitizer:ignore — test-only value
_CAP = 2


@pytest.fixture(scope="module")
def capped_server(tmp_path_factory: pytest.TempPathFactory):
    rust_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sock = str(tmp_path_factory.mktemp("eg-guard") / "capped.sock")

    proc = subprocess.Popen(
        [
            "cargo",
            "run",
            "--features",
            "full",
            "--bin",
            "epistemic-graph-server",
            "--",
            "--socket-path",
            sock,
        ],
        cwd=rust_dir,
        env={
            **os.environ,
            "GRAPH_SERVICE_AUTH_SECRET": _SECRET,
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
        yield sock
    finally:
        proc.terminate()
        proc.wait()


def test_under_cap_returns_data(capped_server: str) -> None:
    client = SyncEpistemicGraphClient.connect(
        socket_path=capped_server,
        graph_name="guard:under",
        auth_secret=_SECRET,
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
