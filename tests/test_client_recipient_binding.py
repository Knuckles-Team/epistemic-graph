"""Explicit local recipient confinement; no native engine or real proof issuer."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
from pathlib import Path

import msgpack
import pytest
from conftest import request_context

from epistemic_graph import client as client_module
from epistemic_graph.client import EpistemicGraphClient, SyncEpistemicGraphClient

pytestmark = pytest.mark.no_engine


def _claims(label="a"):
    return {**request_context(), "oidc_token": "synthetic-proof-" + label}


@pytest.fixture(autouse=True)
def _transport_env(monkeypatch):
    for key in tuple(os.environ):
        if key.startswith("GRAPH_SERVICE_TLS"):
            monkeypatch.delenv(key)


@pytest.fixture
def socket_dir():
    # UDS address length is bounded independently of pytest's checkout path.
    with tempfile.TemporaryDirectory(prefix="eg-recipient-") as directory:
        yield Path(directory)


@contextlib.asynccontextmanager
async def _server(path):
    frames, chunks, writers, tasks = [], [], [], set()

    async def serve(reader, writer):
        task = asyncio.current_task()
        assert task is not None
        tasks.add(task)
        writers.append(writer)
        try:
            while True:
                prefix = await reader.readexactly(4)
                chunks.append(prefix)
                body = await reader.readexactly(int.from_bytes(prefix, "big"))
                chunks.append(body)
                request = msgpack.unpackb(body, raw=False)
                frames.append(request)
                result = 1 if request["method"] == "NodeCount" else {"healthy": True}
                response = msgpack.packb({"id": request["id"], "result": result})
                writer.write(len(response).to_bytes(4, "big") + response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_unix_server(serve, str(path))
    try:
        yield frames, chunks, writers
    finally:
        server.close()
        for writer in writers:
            writer.close()
        if tasks:
            await asyncio.gather(*tuple(tasks))
        await server.wait_closed()


async def _connect(path, **kwargs):
    return await EpistemicGraphClient.connect(
        socket_path=str(path),
        required_socket_path=str(path),
        auth_secret="synthetic-test-secret",
        verified_context=request_context(),
        **kwargs,
    )


def _proof(request):
    envelope = json.loads(bytes.fromhex(request["auth_token"][4:]))
    assert "required_socket_path" not in envelope
    assert "required_socket_path" not in envelope["context"]
    return envelope["oidc_token"]


@pytest.mark.parametrize(
    "value",
    [
        "",
        "relative.sock",
        "@abstract",
        "\x00abstract",
        "/tmp/a\x00b",
        "unix:///tmp/a",
        "/tmp/../a",
        "/tmp//a",
        "//tmp/a",
        "/tmp/a/",
        42,
    ],
)
def test_noncanonical_recipient_rejected_without_dial(value, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("invalid recipient reached the dialer")

    monkeypatch.setattr(asyncio, "open_unix_connection", forbidden)
    monkeypatch.setattr(asyncio, "open_connection", forbidden)

    async def run():
        with pytest.raises(ValueError, match="required_socket_path"):
            await EpistemicGraphClient.connect(
                required_socket_path=value,
                verified_context=_claims(),
                auth_secret="synthetic-test-secret",
            )

    asyncio.run(run())


def test_missing_recipient_never_uses_fallback(socket_dir, monkeypatch):
    async def run():
        async with _server(socket_dir / "foreign") as (_, chunks, _):
            monkeypatch.setattr(
                client_module,
                "_resolve_uds_path",
                lambda _: str(socket_dir / "foreign"),
            )
            monkeypatch.setenv("GRAPH_SERVICE_SOCKET", str(socket_dir / "foreign"))
            with pytest.raises(FileNotFoundError):
                await _connect(socket_dir / "missing")
            assert chunks == []

    asyncio.run(run())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tcp_addr": "127.0.0.1:1"},
        {"tcp_addr": ""},
        {"tls": True},
        {"tls_server_hostname": "engine.example.invalid"},
        {"socket_path": "/another.sock"},
    ],
)
def test_contradictory_transport_rejected(socket_dir, kwargs, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("contradictory transport reached the dialer")

    monkeypatch.setattr(asyncio, "open_unix_connection", forbidden)
    monkeypatch.setattr(asyncio, "open_connection", forbidden)

    async def run():
        with pytest.raises(ValueError, match="required_socket_path"):
            await EpistemicGraphClient.connect(
                required_socket_path=str(socket_dir / "required"),
                verified_context=_claims(),
                auth_secret="synthetic-test-secret",
                **kwargs,
            )

    asyncio.run(run())


def test_symlink_recipient_rejected(socket_dir):
    (socket_dir / "alias").symlink_to(socket_dir / "real")

    async def run():
        with pytest.raises(ValueError, match="required_socket_path"):
            await _connect(socket_dir / "alias")

    asyncio.run(run())


def test_late_proof_and_concurrent_contexts_are_isolated(socket_dir, caplog):
    async def run():
        path = socket_dir / "engine"
        async with _server(path) as (frames, _, _):
            client = await _connect(path)
            try:
                await client.health()  # proof-free initial transport remains bound

                async def call(label):
                    with client.use_verified_context(
                        _claims(label), required_socket_path=str(path)
                    ):
                        await asyncio.sleep(0)
                        await client.health()

                await asyncio.gather(call("a"), call("b"))
                assert {_proof(frame) for frame in frames[1:]} == {
                    "synthetic-proof-a",
                    "synthetic-proof-b",
                }
                assert "oidc_token" not in client._effective_verified_context()
                with pytest.raises(ValueError, match="Bolt"):
                    client.fresh_bolt_auth_token()
                with client.use_verified_context(
                    _claims(), required_socket_path=str(path)
                ):
                    with pytest.raises(ValueError, match="Bolt"):
                        client.fresh_bolt_auth_token()
            finally:
                await client.close()
        assert "synthetic-proof" not in caplog.text

    asyncio.run(run())


def test_context_cannot_upgrade_unbound_or_change_recipient(socket_dir):
    async def run():
        path = socket_dir / "engine"
        async with _server(path) as (_, chunks, _):
            bound = await _connect(path)
            unbound = await EpistemicGraphClient.connect(
                socket_path=str(path),
                auth_secret="synthetic-test-secret",
                verified_context=request_context(),
            )
            try:
                for client, required in [
                    (unbound, path),
                    (bound, socket_dir / "other"),
                ]:
                    with pytest.raises(
                        ValueError, match="bound to required_socket_path"
                    ):
                        with client.use_verified_context(
                            _claims(), required_socket_path=str(required)
                        ):
                            pytest.fail("context admitted")
                assert chunks == []
            finally:
                await bound.close()
                await unbound.close()

    asyncio.run(run())


def test_missing_reconnect_then_same_recipient_restart(socket_dir, monkeypatch):
    async def run():
        path, foreign = socket_dir / "engine", socket_dir / "foreign"
        async with _server(foreign) as (_, foreign_chunks, _):
            async with _server(path):
                client = await _connect(path)
                await client.health()
                client._mark_dead(ConnectionError("synthetic disconnect"))
            path.unlink(missing_ok=True)
            monkeypatch.setenv("GRAPH_SERVICE_SOCKET", str(foreign))
            monkeypatch.setattr(
                client_module, "_resolve_uds_path", lambda _: str(foreign)
            )
            try:
                with client.use_verified_context(
                    _claims(), required_socket_path=str(path)
                ):
                    with pytest.raises(FileNotFoundError):
                        await client.health()
                    async with _server(path) as (frames, _, _):
                        await client.health()
                        assert _proof(frames[0]) == "synthetic-proof-a"
                assert foreign_chunks == []
                assert client._pending == {}
            finally:
                await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("corruption", ["writer", "generation", "endpoint", "closed"])
def test_guard_after_write_lock_wait_sends_zero_bytes(socket_dir, corruption):
    async def run():
        path, foreign = socket_dir / "engine", socket_dir / "foreign"
        async with (
            _server(path) as (_, chunks, _),
            _server(foreign) as (_, foreign_chunks, _),
        ):
            client = await _connect(path)
            _, foreign_writer = await asyncio.open_unix_connection(str(foreign))
            original_writer = client._writer
            try:
                await client._write_lock.acquire()
                with client.use_verified_context(
                    _claims(), required_socket_path=str(path)
                ):
                    call = asyncio.create_task(client.health())
                for _ in range(50):
                    if client._pending:
                        break
                    await asyncio.sleep(0)
                assert client._pending
                if corruption == "writer":
                    client._writer = foreign_writer
                elif corruption == "generation":
                    client._generation += 1
                elif corruption == "endpoint":
                    client._socket_path = str(foreign)
                else:
                    client._closed = True
                client._write_lock.release()
                with pytest.raises(ConnectionError, match="required_socket_path"):
                    await call
                assert client._pending == {}
                assert chunks == foreign_chunks == []
            finally:
                client._writer = original_writer
                await client.close()
                foreign_writer.close()
                await foreign_writer.wait_closed()

    asyncio.run(run())


@pytest.mark.parametrize("native_producer", [False, True])
def test_serialized_proof_cannot_follow_redirected_reconnect(
    socket_dir, monkeypatch, native_producer
):
    async def run():
        path, foreign = socket_dir / "engine", socket_dir / "foreign"
        async with (
            _server(path) as (_, chunks, _),
            _server(foreign) as (_, foreign_chunks, _),
        ):
            client = await _connect(path)
            original = client._python_send_payload
            native = client._sql_source_send_payload
            if native_producer:
                monkeypatch.setattr(
                    client_module, "_sql_source_native_codec", lambda: object()
                )
                monkeypatch.setattr(
                    client_module,
                    "_snapshot_sql_source_input",
                    lambda params, codec: params,
                )

                def build_native(params, **kwargs):
                    # Stand in only for the native codec, retaining the real
                    # selector, preparation admission and to_thread producer.
                    body = client._build_send_request("Health", params, **kwargs)
                    return msgpack.packb(body, use_bin_type=True)

                monkeypatch.setattr(client, "_build_sql_source_payload", build_native)

            async def payload(params, **kwargs):
                if native_producer:
                    result = await native(params, **kwargs)
                else:
                    result = await original("Health", params, **kwargs)
                assert _proof(msgpack.unpackb(result, raw=False)) == "synthetic-proof-a"
                client._mark_dead(ConnectionError("synthetic disconnect"))
                client._socket_path = str(foreign)
                return result

            if native_producer:
                monkeypatch.setattr(client, "_sql_source_send_payload", payload)
            else:

                async def python_payload(method, params, **kwargs):
                    return await payload(params, **kwargs)

                monkeypatch.setattr(client, "_python_send_payload", python_payload)
            try:
                with client.use_verified_context(
                    _claims(), required_socket_path=str(path)
                ):
                    with pytest.raises(ValueError, match="required_socket_path"):
                        await client._send(
                            "SqlSourceBatch" if native_producer else "Health"
                        )
                assert client._pending == {}
                assert chunks == foreign_chunks == []
            finally:
                await client.close()

    asyncio.run(run())


def test_sync_context_reaches_loop_and_namespace(socket_dir):
    async def run():
        path = socket_dir / "engine"
        async with _server(path) as (frames, _, _):

            def calls():
                client = SyncEpistemicGraphClient.connect(
                    socket_path=str(path),
                    required_socket_path=str(path),
                    auth_secret="synthetic-test-secret",
                    verified_context=request_context(),
                )
                try:
                    with client.use_verified_context(
                        _claims("sync"), required_socket_path=str(path)
                    ):
                        client.health()
                        assert client.nodes.count() == 1
                    with pytest.raises(ValueError, match="Bolt"):
                        client.fresh_bolt_auth_token()
                finally:
                    client.close()

            await asyncio.to_thread(calls)
            assert len(frames) == 2
            assert all(_proof(frame) == "synthetic-proof-sync" for frame in frames)

    asyncio.run(run())


def test_stale_waiter_does_not_poison_same_recipient_reconnect(socket_dir):
    async def run():
        path = socket_dir / "engine"
        async with _server(path) as (frames, _, _):
            client = await _connect(path)
            try:
                await client._write_lock.acquire()
                with client.use_verified_context(
                    _claims("old"), required_socket_path=str(path)
                ):
                    stale = asyncio.create_task(client.health())
                for _ in range(50):
                    if client._pending:
                        break
                    await asyncio.sleep(0)
                assert client._pending
                async with client._lock:
                    await client._reconnect()
                await client._ensure_connection()
                replacement = client._writer
                client._write_lock.release()
                with pytest.raises(ConnectionError, match="required_socket_path"):
                    await stale
                assert client._writer is replacement
                assert not client._closed
                assert not client._pending
                with client.use_verified_context(
                    _claims("new"), required_socket_path=str(path)
                ):
                    await client.health()
                assert len(frames) == 1
                assert _proof(frames[0]) == "synthetic-proof-new"
            finally:
                await client.close()

    asyncio.run(run())


def test_actual_foreign_peer_is_refused_before_any_write(socket_dir, monkeypatch):
    async def run():
        foreign = socket_dir / "foreign"
        async with _server(foreign) as (_, chunks, _):
            reader, writer = await asyncio.open_unix_connection(str(foreign))
            writes: list[bytes] = []
            monkeypatch.setattr(writer, "write", writes.append)

            async def redirected(*args, **kwargs):
                return reader, writer

            monkeypatch.setattr(asyncio, "open_unix_connection", redirected)
            with pytest.raises(ConnectionError, match="required_socket_path"):
                await _connect(socket_dir / "required")
            assert writer.is_closing()
            assert writes == chunks == []

    asyncio.run(run())


@pytest.mark.parametrize(
    "failure", [TimeoutError(), asyncio.IncompleteReadError(b"", 4)]
)
def test_old_write_failure_leaves_replacement_healthy(socket_dir, monkeypatch, failure):
    async def run():
        path = socket_dir / "engine"
        async with _server(path):
            client = await _connect(path)
            entered, release = asyncio.Event(), asyncio.Event()
            original_write = client._write_frame

            async def delayed_failure(*args, **kwargs):
                entered.set()
                await release.wait()
                raise failure

            monkeypatch.setattr(client, "_write_frame", delayed_failure)
            try:
                stale = asyncio.create_task(client.health())
                await entered.wait()
                async with client._lock:
                    await client._reconnect()
                release.set()
                with pytest.raises((TimeoutError, ConnectionError)):
                    await stale
                assert not client._closed
                assert not client._pending
                monkeypatch.setattr(client, "_write_frame", original_write)
                await client.health()
            finally:
                await client.close()

    asyncio.run(run())
