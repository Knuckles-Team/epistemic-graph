import pytest
import subprocess
import time
import socket
import os
from epistemic_graph.client import SyncEpistemicGraphClient


@pytest.fixture(scope="session", autouse=True)
def start_epistemic_graph_server():
    rust_dir = os.path.join(os.path.dirname(__file__), "..")
    rust_dir = os.path.abspath(rust_dir)
    socket_path = "/tmp/test_epistemic_graph_local.sock"
    # Real secret so the suite exercises the HMAC auth path end-to-end
    # (an empty secret makes the server refuse to start by design).
    auth_secret = "epistemic-graph-test-secret"  # sanitizer:ignore — test-only value

    if os.path.exists(socket_path):
        os.remove(socket_path)

    print("Starting epistemic-graph-server...")
    subprocess.run(
        ["cargo", "build", "--features", "server"], cwd=rust_dir, check=False
    )

    process = subprocess.Popen(
        [
            "cargo",
            "run",
            "--features",
            "server",
            "--bin",
            "epistemic-graph-server",
            "--",
            "--socket-path",
            socket_path,
        ],
        cwd=rust_dir,
        env={**os.environ, "GRAPH_SERVICE_AUTH_SECRET": auth_secret},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for _ in range(30):
        if os.path.exists(socket_path):
            break
        time.sleep(0.5)

    os.environ["GRAPH_SERVICE_SOCKET"] = socket_path
    os.environ["GRAPH_SERVICE_AUTH_SECRET"] = auth_secret

    yield process

    process.terminate()
    process.wait()
    if os.path.exists(socket_path):
        os.remove(socket_path)


@pytest.fixture
def clean_graph():
    """Returns a clean EpistemicGraphClient instance for each test case."""
    socket_path = os.environ.get(
        "GRAPH_SERVICE_SOCKET", "/tmp/test_epistemic_graph_local.sock"
    )
    client = SyncEpistemicGraphClient.connect(socket_path=socket_path)
    client.graph.clear()
    return client
