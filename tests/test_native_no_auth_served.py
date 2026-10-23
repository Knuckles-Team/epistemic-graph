"""Real native isolated-store proof for EG-IDENTITY-R003.1, no mock engine."""

import contextlib
import json
import os
import socket
import subprocess
import time

import msgpack
import pytest

pytestmark = [
    pytest.mark.no_engine,  # Each case owns its real isolated process.
    pytest.mark.spec("EG-IDENTITY-R003.1"),
]


def binary():
    from conftest import find_server_binary

    path = find_server_binary()
    explicit = os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY", "").strip()
    if explicit and path != os.path.abspath(explicit):
        pytest.fail("explicit current native server artifact did not validate")
    if path is None:
        pytest.skip("build the scoped native server before this served suite")
    return path


def test_binary_uses_explicit_current_artifact(tmp_path, monkeypatch):
    import hashlib

    artifact = tmp_path / "current-server"
    artifact.write_bytes(b"current source fixture")
    artifact.chmod(0o700)
    monkeypatch.setenv("EPISTEMIC_GRAPH_TEST_BINARY", str(artifact))
    monkeypatch.setenv(
        "EPISTEMIC_GRAPH_TEST_BINARY_SHA256",
        hashlib.sha256(artifact.read_bytes()).hexdigest(),
    )
    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path / "unrelated-target"))
    assert binary() == str(artifact)


@pytest.mark.parametrize("resolved", [None, "/historical/native-server"])
def test_binary_rejects_invalid_explicit_artifact(monkeypatch, resolved):
    import conftest

    monkeypatch.setenv("EPISTEMIC_GRAPH_TEST_BINARY", "/current/native-server")
    monkeypatch.setattr(conftest, "find_server_binary", lambda: resolved)
    with pytest.raises(pytest.fail.Exception, match="did not validate"):
        binary()


def test_binary_delegates_to_shared_resolver(monkeypatch):
    import conftest

    monkeypatch.delenv("EPISTEMIC_GRAPH_TEST_BINARY", raising=False)

    monkeypatch.setattr(
        conftest, "find_server_binary", lambda: "/current/native-server"
    )
    assert binary() == "/current/native-server"


def test_binary_without_available_artifact_skips(monkeypatch):
    import conftest

    monkeypatch.delenv("EPISTEMIC_GRAPH_TEST_BINARY", raising=False)
    monkeypatch.setattr(conftest, "find_server_binary", lambda: None)
    with pytest.raises(pytest.skip.Exception, match="scoped native server"):
        binary()


def clean_env():
    return {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("GRAPH_SERVICE_", "EPISTEMIC_GRAPH_", "OIDC_"))
    }


@contextlib.contextmanager
def server(tmp_path, *, mode="none", extra_env=None):
    with socket.socket(socket.AF_INET) as probe:
        probe.bind(("127.0.0.1", 0))
        sock = probe.getsockname()
    address = f"{sock[0]}:{sock[1]}"
    store = str(tmp_path / "data")
    with (tmp_path / "server.log").open("ab") as log:
        proc = subprocess.Popen(
            [
                binary(),
                "--auth-mode",
                mode,
                "--persist-dir",
                store,
                "--tcp-only",
                "--tcp-addr",
                address,
            ],
            env=clean_env() | (extra_env or {}),
            stdout=log,
            stderr=log,
        )
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    pytest.fail((tmp_path / "server.log").read_text())
                with socket.socket(socket.AF_INET) as probe:
                    try:
                        probe.connect(sock)
                        break
                    except OSError:
                        pass
                time.sleep(0.05)
            else:
                pytest.fail("native engine did not become ready")
            yield sock
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def rpc(sock, method, params=None, *, key=None, token=None, actor=None, request_id=1):
    carrier = (
        token
        if token is not None
        else (
            ""
            if key is None
            else "anon1." + json.dumps({"idempotency_key": key}, separators=(",", ":"))
        )
    )
    request = {
        "id": request_id,
        "graph": "__commons__",
        "auth_token": carrier,
        "method": method,
        "agent_id": None,
    }
    if params is not None:
        request["params"] = params
    if actor is not None:
        request["agent_id"] = actor
    payload = msgpack.packb(request, use_bin_type=True)
    with socket.socket(socket.AF_INET) as connection:
        connection.settimeout(20)
        connection.connect(sock)
        connection.sendall(len(payload).to_bytes(4, "big") + payload)

        def exact(n):
            data = b""
            while len(data) < n:
                chunk = connection.recv(n - len(data))
                assert chunk, "native engine closed connection"
                data += chunk
            return data

        return msgpack.unpackb(exact(int.from_bytes(exact(4), "big")), raw=False)


def node_params(value=1):
    return {
        "node_id": "anonymous-proof",
        "properties_msgpack": msgpack.packb({"value": value}),
    }


def ok(result):
    assert not result.get("error"), result
    return result.get("result")


def test_anonymous_real_write_read_restart_and_key_replay(tmp_path):
    with server(tmp_path) as sock:
        ok(rpc(sock, "AddNode", node_params(), key="durable-write"))
        assert ok(rpc(sock, "HasNode", {"node_id": "anonymous-proof"})) is True
        ok(rpc(sock, "AddNode", node_params(), key="durable-write"))
        conflict = rpc(sock, "AddNode", node_params(2), key="durable-write")
        assert conflict.get("error"), conflict
    with server(tmp_path) as sock:
        assert ok(rpc(sock, "HasNode", {"node_id": "anonymous-proof"})) is True
        ok(rpc(sock, "AddNode", node_params(), key="durable-write"))
        conflict = rpc(sock, "AddNode", node_params(2), key="durable-write")
        assert conflict.get("error"), conflict


def test_multiple_anonymous_clients_reusing_numeric_ids_clear_current_state(tmp_path):
    with server(tmp_path) as sock:
        ok(rpc(sock, "AddNode", node_params()))
        ok(rpc(sock, "ClearGraph"))
        ok(rpc(sock, "AddNode", node_params()))
        ok(rpc(sock, "ClearGraph"))
        assert ok(rpc(sock, "HasNode", {"node_id": "anonymous-proof"})) is False


@pytest.mark.parametrize(
    "token,actor",
    [
        ("eg2.not-a-credential", None),
        ("", "claimed-admin"),
        ('anon1.{"idempotency_key":"x","principal":"admin"}', None),
        ('anon1.{"idempotency_key":"x","mac":"fake"}', None),
        ('anon1.{"idempotency_key":"x","idempotency_key":"y"}', None),
        ("anon1." + json.dumps({"idempotency_key": " "}), None),
        ("anon1." + json.dumps({"idempotency_key": "a" * 257}), None),
        ("anon1." + json.dumps({"idempotency_key": "x\n"}), None),
        ("anon1." + " " * 2049, None),
    ],
)
def test_anonymous_rejects_signed_or_claimed_identity(tmp_path, token, actor):
    with server(tmp_path) as sock:
        response = rpc(sock, "Ping", token=token, actor=actor)
        assert response.get("error"), response
        assert response.get("error_detail") != "invalid request encoding", response


@pytest.mark.parametrize(
    "extra,env",
    [
        (["--auth-secret", "synthetic-conflict"], {}),
        (["--tcp-addr", "0.0.0.0:9100"], {}),
        (["--tcp-addr", "localhost:9100"], {}),
        (["--metrics-addr", "127.0.0.1:9101"], {}),
        (["--socket-mode", "0660"], {}),
        ([], {"EPISTEMIC_GRAPH_RAFT_NODE_ID": "1"}),
        ([], {"EPISTEMIC_GRAPH_PGWIRE_ADDR": "127.0.0.1:5432"}),
    ],
)
def test_anonymous_refuses_unsafe_startup(tmp_path, extra, env):
    result = subprocess.run(
        [
            binary(),
            "--auth-mode",
            "none",
            "--persist-dir",
            str(tmp_path / "data"),
            *extra,
        ],
        env=clean_env() | env,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode != 0
    assert not (tmp_path / "data" / "anonymous-store-v1").exists()


def test_anonymous_refuses_preexisting_unmarked_data(tmp_path):
    store = tmp_path / "data"
    store.mkdir()
    (store / "protected-state").write_text("unchanged")
    result = subprocess.run(
        [binary(), "--auth-mode", "none", "--persist-dir", str(store)],
        env=clean_env(),
        capture_output=True,
        timeout=20,
    )
    assert result.returncode != 0
    assert (store / "protected-state").read_text() == "unchanged"
    assert not (store / "anonymous-store-v1").exists()


@pytest.mark.parametrize("kind", ["symlink", "directory", "invalid-content"])
def test_anonymous_refuses_invalid_mode_marker(tmp_path, kind):
    store = tmp_path / "data"
    store.mkdir()
    marker = store / "anonymous-store-v1"
    if kind == "symlink":
        target = tmp_path / "marker-target"
        target.write_text("epistemic-graph anonymous local store v1\n")
        marker.symlink_to(target)
    elif kind == "directory":
        marker.mkdir()
    else:
        marker.write_text("not-an-anonymous-store")
    result = subprocess.run(
        [binary(), "--auth-mode", "none", "--persist-dir", str(store)],
        env=clean_env(),
        capture_output=True,
        timeout=20,
    )
    assert result.returncode != 0
    assert b"invalid anonymous marker" in result.stderr


def test_real_python_anonymous_client_reads_and_writes(tmp_path):
    import asyncio

    from epistemic_graph.client import EpistemicGraphClient

    async def exercise(sock):
        client = await EpistemicGraphClient.connect(
            auth_mode="none", tcp_addr=f"{sock[0]}:{sock[1]}"
        )
        try:
            assert not client.authenticated
            assert client._verified_context is None
            await client.nodes.add("python-anonymous-proof", {"value": 17})
            assert await client.nodes.has("python-anonymous-proof")
            await client.nodes.remove("python-anonymous-proof")
            assert not await client.nodes.has("python-anonymous-proof")
        finally:
            await client.close()

    with server(tmp_path) as sock:
        asyncio.run(exercise(sock))


def test_real_anonymous_blob_store_fetch_and_restart(tmp_path):
    import asyncio

    from epistemic_graph.client import EpistemicGraphClient

    payload = b"actual anonymous durable tool content"

    async def write(sock):
        client = await EpistemicGraphClient.connect(
            auth_mode="none", tcp_addr=f"{sock[0]}:{sock[1]}"
        )
        try:
            digest = await client.blob.store(payload)
            assert await client.blob.incref(digest) > 0
            assert await client.blob.fetch(digest) == payload
            return digest
        finally:
            await client.close()

    async def read(sock, digest):
        client = await EpistemicGraphClient.connect(
            auth_mode="none", tcp_addr=f"{sock[0]}:{sock[1]}"
        )
        try:
            assert await client.blob.fetch(digest) == payload
        finally:
            await client.close()

    with server(tmp_path) as sock:
        digest = asyncio.run(write(sock))
    with server(tmp_path) as sock:
        asyncio.run(read(sock, digest))


def test_anonymous_connector_sweep_needs_no_authenticated_policy(tmp_path):
    with server(tmp_path) as sock:
        ok(rpc(sock, "Ping"))
        # Observe more than one 500 ms idle sweep, not just listener readiness.
        time.sleep(1.1)
    log = (tmp_path / "server.log").read_text()
    assert "CONNECTOR_PACK_PROJECTION_SWEEP_FAILED" not in log
    assert "secure request context requires" not in log


def test_anonymous_connector_sweep_projects_committed_pack(tmp_path):
    import asyncio
    import hashlib

    from test_connector_pack_planted import _context, _upload

    from epistemic_graph.client import EpistemicGraphClient
    from epistemic_graph.testing.synthetic.packs.malformed import base_spec
    from epistemic_graph.testing.synthetic.packs.spec import assemble

    async def exercise(sock):
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"{sock[0]}:{sock[1]}", auth_mode="none"
        )
        try:
            connector = "anonymous-sweep"

            async def mutate(op, fields):
                key = (
                    f"connector-pack:{connector}:import:{fields['index']['pack_digest']}:0"
                    if op == "import"
                    else f"anonymous-sweep:{op}"
                )
                context = _context(key)
                context["tenant_id"] = "local"
                return await client._send(
                    "ConnectorPack",
                    {"op": {"op": op, "request": {"context": context, **fields}}},
                    idempotency_key=key,
                )

            await mutate(
                "bind",
                {
                    "connector": connector,
                    "importer": "principal:sha256:"
                    + hashlib.sha256(b"anonymous").hexdigest(),
                },
            )
            pack = assemble(base_spec(0, connector))
            index = pack.index.model_dump(mode="json")
            index["archive"]["blob_digest"] = await _upload(client, pack, connector)
            imported = await mutate(
                "import",
                {"index": index, "expected_head": None, "allow_mass_withdrawal": False},
            )
            assert imported["result"] == "imported", imported
            # Scoped opt0 builds prove behavior, not projection latency.
            deadline = time.monotonic() + 60
            while True:
                status = await client._send(
                    "ConnectorPack",
                    {
                        "op": {
                            "op": "status",
                            "request": {"tenant_id": "local", "connector": connector},
                        }
                    },
                )
                if status["projection"]["projection"] == "applied":
                    break
                assert status["projection"]["projection"] != "failed", status[
                    "projection"
                ]
                assert time.monotonic() < deadline, status["projection"]
                await asyncio.sleep(0.1)
            assert status["tenant_id"] == "local"
            assert (
                status["head"]["visible_record_id"] == imported["receipt"]["record_id"]
            )
            graph = status["projection"]["graph"]
            assert graph
            sources = await client._send("GraphSchemaList", {}, graph)
            assert sources["graph"] == graph
            source = next(
                item
                for item in sources["dynamic_sources"]
                if item["source_id"] == f"pack:{connector}"
            )
            assert source["origin"]["record_id"] == imported["receipt"]["record_id"]
            assert source["shapes_bytes"] > 0 and source["shapes_sha256"]
            assert source["ontology_bytes"] > 0 and source["ontology_sha256"]
        finally:
            await client.close()

    with server(tmp_path) as sock:
        asyncio.run(exercise(sock))
