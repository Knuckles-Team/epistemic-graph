"""Round-trip tests for the native visualization render surface (D-VZ-1 lanes V4
"engine integration" / V6 "graph-native marks") — proves the new
``client.viz.render``/``client.viz.capability_matrix`` reach a REAL running
server built from THIS worktree and get back real, non-empty, format-appropriate
rendered bytes plus a real ``view_result``.

Self-contained: it manages its OWN server process (a dedicated binary built
with ``viz-static-export`` on top of ``full``, on its own socket + persist
dir), independent of the session-wide ``full`` fixture in ``conftest.py``
(which does not carry ``viz-static-export`` — deliberately opt-in, not folded
into ``full``; see the root ``Cargo.toml``'s doc comment on that feature). It
reuses ``conftest.py``'s ``EPISTEMIC_GRAPH_TEST_BINARY`` seam
(``_prebuilt_test_binary()``) before ever paying for its own ``cargo build`` —
a caller-supplied binary is trusted to have been built with a superset of the
features this module needs, exactly like the session fixture already trusts
it. Mirrors ``test_epi_gapfill_roundtrip.py``'s exact fixture pattern.
"""

from __future__ import annotations

import os
import socket as _socket
import subprocess
import time

import pytest
from conftest import (
    TEST_AGENT_ID,
    TEST_SIGNER_KEY,
    _prebuilt_test_binary,
    bootstrap_context,
    find_server_binary,
    request_context,
    strict_server_env,
)

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AUTH_SECRET = "test-viz-client-roundtrip-secret"
FEATURES = "full viz-static-export"


def _build() -> str | None:
    """Return the shared `EPISTEMIC_GRAPH_TEST_BINARY` (see conftest.py's
    `_prebuilt_test_binary()`) when one is configured, else build the
    viz-enabled binary ONCE and return its path (or None on failure)."""
    prebuilt = _prebuilt_test_binary()
    if prebuilt is not None:
        return prebuilt
    r = subprocess.run(
        ["cargo", "build", "--features", FEATURES],
        cwd=RUST_DIR,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        return None
    # `find_server_binary()` honors `CARGO_TARGET_DIR`/the repo's own
    # `.cargo/config.toml` (`target-isolated`) -- the build above lands wherever
    # that resolves, which is NOT necessarily the hardcoded legacy `target/debug`.
    return find_server_binary()


def _launch(
    binary: str, socket_path: str, state_dir: str, persist_dir: str
) -> subprocess.Popen:
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
    os.makedirs(persist_dir, exist_ok=True)
    env = {
        **os.environ,
        **strict_server_env(state_dir, auth_secret=AUTH_SECRET, persist_dir=persist_dir),
    }
    if os.path.exists(socket_path):
        os.remove(socket_path)
    proc = subprocess.Popen(
        [binary, "--socket-path", socket_path],
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


@pytest.fixture(scope="module")
def viz_client(tmp_path_factory):
    binary = _build()
    if binary is None:
        pytest.skip(f'build with --features "{FEATURES}" failed in this environment')
        return  # pragma: no cover - pytest.skip is NoReturn
    runtime = tmp_path_factory.mktemp("viz-client")
    socket_path = str(runtime / "engine.sock")
    proc = _launch(
        binary, socket_path, str(runtime / "security"), str(runtime / "persist")
    )
    bootstrap = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        auth_secret=AUTH_SECRET,
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
    client = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        graph_name="__commons__",
        auth_secret=AUTH_SECRET,
        verified_context=request_context(),
    )
    try:
        yield client
    finally:
        client.close()
        proc.terminate()
        proc.wait()
        if os.path.exists(socket_path):
            os.remove(socket_path)


_PNG_SIGNATURE = bytes([137, 80, 78, 71])


def _scatter_spec(dataset_ref: str) -> dict:
    return {
        "version": 1,
        "marks": [
            {
                "kind": "scatter",
                "data_ref": dataset_ref,
                "encodings": {"x": {"field": "x"}, "y": {"field": "y"}},
            }
        ],
    }


def _graph_spec(dataset_ref: str) -> dict:
    return {
        "version": 1,
        "marks": [{"kind": "graph", "data_ref": dataset_ref}],
    }


def test_capability_matrix_round_trip(viz_client):
    matrix = viz_client.viz.capability_matrix()
    entries = matrix["entries"]
    assert len(entries) > 0
    # 6 mark kinds x 3 surfaces per eg_viz_core::capability's own default matrix.
    assert len(entries) == 18
    graph_entries = [e for e in entries if e["mark"] == "graph"]
    assert len(graph_entries) == 3


def test_inline_columns_scatter_renders_a_real_png(viz_client):
    result = viz_client.viz.render(
        spec=_scatter_spec("ds:inline"),
        dataset={
            "InlineColumns": {
                "columns": {
                    "x": {"F64": [float(i) for i in range(20)]},
                    "y": {"F64": [float(i * 2) for i in range(20)]},
                }
            }
        },
        width_px=400,
        height_px=300,
        dataset_ref="ds:inline",
    )
    assert result["view_result"]["lod_tier"] == "direct"
    assert result["view_result"]["exact"] is True
    assert result["content_type"] == "image/png"
    png_bytes = result["bytes"]
    assert isinstance(png_bytes, (bytes, bytearray))
    assert len(png_bytes) > 8
    assert bytes(png_bytes[:4]) == _PNG_SIGNATURE


def test_synthetic_scatter_clusters_forces_density_tier(viz_client):
    result = viz_client.viz.render(
        spec=_scatter_spec("ds:synthetic-scatter"),
        dataset={
            "SyntheticScatterClusters": {
                "row_count": 5_000_000,
                "clusters": 5,
                "seed": 42,
            }
        },
        width_px=800,
        height_px=600,
        max_primitives=2_000_000,
        max_bytes=16 * 1024 * 1024,
        dataset_ref="ds:synthetic-scatter",
    )
    assert result["view_result"]["lod_tier"] == "density"
    assert result["view_result"]["exact"] is False
    png_bytes = result["bytes"]
    assert len(png_bytes) > 8
    assert bytes(png_bytes[:4]) == _PNG_SIGNATURE


def test_synthetic_graph_small_renders_at_direct_tier(viz_client):
    result = viz_client.viz.render(
        spec=_graph_spec("ds:graph-small"),
        dataset={
            "SyntheticGraph": {"node_count": 40, "edge_count": 80, "seed": 1}
        },
        width_px=400,
        height_px=300,
        dataset_ref="ds:graph-small",
    )
    assert result["view_result"]["lod_tier"] == "direct"
    assert result["view_result"]["exact"] is True
    assert result["view_result"]["row_count"] == 40
    png_bytes = result["bytes"]
    assert len(png_bytes) > 8
    assert bytes(png_bytes[:4]) == _PNG_SIGNATURE


def test_synthetic_graph_large_forces_density_tier(viz_client):
    result = viz_client.viz.render(
        spec=_graph_spec("ds:graph-large"),
        dataset={
            "SyntheticGraph": {
                "node_count": 300_000,
                "edge_count": 600_000,
                "seed": 2,
            }
        },
        width_px=800,
        height_px=600,
        # Deliberately tight: 300,000 nodes * 3 primitives/row = 900,000, well
        # over this budget, forcing Density.
        max_primitives=100_000,
        dataset_ref="ds:graph-large",
    )
    assert result["view_result"]["lod_tier"] == "density"
    assert result["view_result"]["exact"] is False
    assert result["view_result"]["row_count"] == 300_000
    png_bytes = result["bytes"]
    assert len(png_bytes) > 8
    assert bytes(png_bytes[:4]) == _PNG_SIGNATURE
    # The bounded-draw-op-count PROPERTY itself is proven directly against the
    # render plan in the Rust crate (`eg-viz-export`'s `render::graph_tests`);
    # here we just confirm the encoded image stays a modest size regardless of
    # the 300,000-node input, consistent with that bound.
    assert len(png_bytes) < 2_000_000


def test_invalid_spec_json_is_a_typed_error(viz_client):
    with pytest.raises(RuntimeError):
        viz_client.viz.render(
            spec={"not": "a valid ViewSpec"},
            dataset={
                "SyntheticScatterClusters": {
                    "row_count": 10,
                    "clusters": 1,
                    "seed": 0,
                }
            },
            dataset_ref="ds:bad-spec",
        )
