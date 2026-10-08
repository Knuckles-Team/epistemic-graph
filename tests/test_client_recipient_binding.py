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


@contextlib.contextmanager
def _synthetic_admission(claims):
    # Recipient-only fixtures have no expiring issuer; freshness cases below
    # use a locked renewable authority with a deterministic clock.
    if "oidc_token" in claims:
        assert claims["oidc_token"].startswith("synthetic-proof-")
    yield


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


@contextlib.asynccontextmanager
async def _two_recipients(socket_dir):
    path, foreign = socket_dir / "engine", socket_dir / "foreign"
    async with (
        _server(path) as (_, chunks, _),
        _server(foreign) as (_, foreign_chunks, _),
    ):
        yield path, foreign, chunks, foreign_chunks


async def _connect(path, **kwargs):
    return await EpistemicGraphClient.connect(
        socket_path=str(path),
        required_socket_path=str(path),
        write_admission=_synthetic_admission,
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
                write_admission=_synthetic_admission,
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
                        _claims(label),
                        required_socket_path=str(path),
                        write_admission=_synthetic_admission,
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
                    _claims(),
                    required_socket_path=str(path),
                    write_admission=_synthetic_admission,
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
                    _claims(),
                    required_socket_path=str(path),
                    write_admission=_synthetic_admission,
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
def test_guard_after_write_lock_wait_sends_zero_bytes(
    socket_dir, monkeypatch, corruption
):
    async def run():
        async with _two_recipients(socket_dir) as (
            path,
            foreign,
            chunks,
            foreign_chunks,
        ):
            client = await _connect(path)
            _, foreign_writer = await asyncio.open_unix_connection(str(foreign))
            original_writer = client._writer
            writes: list[bytes] = []
            foreign_writes: list[bytes] = []
            _record_writes(monkeypatch, original_writer, writes)
            _record_writes(monkeypatch, foreign_writer, foreign_writes)
            try:
                await client._write_lock.acquire()
                with client.use_verified_context(
                    _claims(),
                    required_socket_path=str(path),
                    write_admission=_synthetic_admission,
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
                assert writes == foreign_writes == chunks == foreign_chunks == []
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
        async with _two_recipients(socket_dir) as (
            path,
            foreign,
            chunks,
            foreign_chunks,
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
                    _claims(),
                    required_socket_path=str(path),
                    write_admission=_synthetic_admission,
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
                    write_admission=_synthetic_admission,
                    auth_secret="synthetic-test-secret",
                    verified_context=request_context(),
                )
                try:
                    with client.use_verified_context(
                        _claims("sync"),
                        required_socket_path=str(path),
                        write_admission=_synthetic_admission,
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
                    _claims("old"),
                    required_socket_path=str(path),
                    write_admission=_synthetic_admission,
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
                    _claims("new"),
                    required_socket_path=str(path),
                    write_admission=_synthetic_admission,
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
                await asyncio.wait_for(entered.wait(), 2)
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


class _RenewableAuthority:
    """A deterministic model of the AU lease's atomic proof/expiry lock."""

    def __init__(self):
        import threading

        self.lock = threading.Lock()
        self.now = 50
        self.expiry = 100
        self.claims = _claims("a")
        self.checked = []

    def renew(self):
        with self.lock:
            self.claims = _claims("b")
            self.expiry = 200

    @contextlib.contextmanager
    def admit(self, captured):
        with self.lock:
            if self.now >= self.expiry or captured != self.claims:
                # A verifier may accidentally include a bearer; EG must sanitize.
                raise PermissionError("private-verifier-detail synthetic-proof-a")
            self.checked.append(captured)
            yield


def _record_writes(monkeypatch, writer, writes):
    original = writer.write

    def record(data):
        writes.append(data)
        original(data)

    monkeypatch.setattr(writer, "write", record)


def _transition_authority(authority, transition):
    if transition in {"expire", "expire_and_renew"}:
        authority.now = 101
    if transition in {"renew", "expire_and_renew"}:
        authority.renew()


@pytest.mark.parametrize("delay", ["write_lock", "reconnect", "native_codec"])
@pytest.mark.parametrize("transition", ["expire", "renew", "expire_and_renew"])
def test_delayed_proof_never_outlives_current_authority(
    socket_dir, monkeypatch, caplog, delay, transition
):
    async def run():
        path = socket_dir / "engine"
        authority = _RenewableAuthority()
        async with _server(path) as (frames, _, _):
            client = await _connect(path)
            writes: list[bytes] = []
            entered, release = asyncio.Event(), asyncio.Event()
            _record_writes(monkeypatch, client._writer, writes)
            if delay == "write_lock":
                await client._write_lock.acquire()
            elif delay == "reconnect":
                original_open = client._open_streams

                async def blocked_open(*args, **kwargs):
                    entered.set()
                    await release.wait()
                    reader, writer, resolved = await original_open(*args, **kwargs)
                    _record_writes(monkeypatch, writer, writes)
                    return reader, writer, resolved

                monkeypatch.setattr(client, "_open_streams", blocked_open)
                client._mark_dead(ConnectionError("synthetic reconnect"))
            else:
                import threading

                ready, proceed = threading.Event(), threading.Event()
                monkeypatch.setattr(
                    client_module, "_sql_source_native_codec", lambda: object()
                )
                monkeypatch.setattr(
                    client_module,
                    "_snapshot_sql_source_input",
                    lambda params, codec: params,
                )

                def prepare(params, **kwargs):
                    request = client._build_send_request("Health", params, **kwargs)
                    ready.set()
                    assert proceed.wait(2)
                    return msgpack.packb(request, use_bin_type=True)

                monkeypatch.setattr(client, "_build_sql_source_payload", prepare)
            try:
                with client.use_verified_context(
                    authority.claims,
                    required_socket_path=str(path),
                    write_admission=authority.admit,
                ):
                    pending = asyncio.create_task(
                        client._send(
                            "SqlSourceBatch" if delay == "native_codec" else "Health"
                        )
                    )
                if delay == "write_lock":
                    for _ in range(50):
                        if client._pending:
                            break
                        await asyncio.sleep(0)
                    assert client._pending
                elif delay == "reconnect":
                    await asyncio.wait_for(entered.wait(), 2)
                else:
                    assert await asyncio.to_thread(ready.wait, 2)
                _transition_authority(authority, transition)
                if delay == "write_lock":
                    client._write_lock.release()
                elif delay == "reconnect":
                    release.set()
                else:
                    proceed.set()
                with pytest.raises(
                    PermissionError, match="no longer admitted"
                ) as refused:
                    await pending
                assert "synthetic-proof" not in str(refused.value)
                assert "private-verifier-detail" not in str(refused.value)
                assert "synthetic-proof" not in caplog.text
                assert not client._pending
                assert not client._closed
                assert writes == frames == []
                authority.renew()
                with client.use_verified_context(
                    authority.claims,
                    required_socket_path=str(path),
                    write_admission=authority.admit,
                ):
                    await client.health()
                assert len(frames) == 1
                assert _proof(frames[0]) == "synthetic-proof-b"
            finally:
                await client.close()

    asyncio.run(run())


def test_bound_proof_requires_its_own_admission(socket_dir):
    async def run():
        path = socket_dir / "engine"
        async with _server(path) as (_, chunks, _):
            with pytest.raises(ValueError, match="requires write_admission"):
                await EpistemicGraphClient.connect(
                    required_socket_path=str(path),
                    verified_context=_claims(),
                    auth_secret="synthetic-test-secret",
                )
            client = await _connect(path)
            try:
                with pytest.raises(ValueError, match="requires write_admission"):
                    with client.use_verified_context(
                        _claims(), required_socket_path=str(path)
                    ):
                        pytest.fail("inherited another context's admission")
                assert chunks == []
            finally:
                await client.close()

    asyncio.run(run())


def test_connect_proof_and_atomic_lock_cover_both_writes_only(socket_dir, monkeypatch):
    async def run():
        path = socket_dir / "engine"
        authority = _RenewableAuthority()
        async with _server(path) as (frames, _, _):
            client = await EpistemicGraphClient.connect(
                required_socket_path=str(path),
                verified_context=authority.claims,
                write_admission=authority.admit,
                auth_secret="synthetic-test-secret",
            )
            write, drain = client._writer.write, client._writer.drain
            writes: list[bytes] = []

            def checked_write(data):
                assert not authority.lock.acquire(blocking=False), (
                    "renewal lock released before frame completion"
                )
                writes.append(data)
                write(data)

            async def checked_drain():
                assert authority.lock.acquire(blocking=False), (
                    "renewal lock retained across await"
                )
                authority.lock.release()
                await drain()

            monkeypatch.setattr(client._writer, "write", checked_write)
            monkeypatch.setattr(client._writer, "drain", checked_drain)
            try:
                await client.health()
                assert len(writes) == 2
                assert _proof(frames[0]) == "synthetic-proof-a"
            finally:
                await client.close()

    asyncio.run(run())


def test_concurrent_admission_is_paired_and_defensively_copied(socket_dir):
    async def run():
        path = socket_dir / "engine"
        async with _server(path) as (frames, _, _):
            client = await _connect(path)
            checked = []

            @contextlib.contextmanager
            def admission(expected, captured):
                assert captured == _claims(expected)
                checked.append(expected)
                captured["oidc_token"] = "synthetic-proof-mutated"
                captured["roles"].append("mutation-must-not-reach-wire")
                yield

            async def call(label):
                import functools

                with client.use_verified_context(
                    _claims(label),
                    required_socket_path=str(path),
                    write_admission=functools.partial(admission, label),
                ):
                    await asyncio.sleep(0)
                    await client.health()

            try:
                await asyncio.gather(call("a"), call("b"))
                assert sorted(checked) == ["a", "b"]
                assert all(
                    json.loads(bytes.fromhex(frame["auth_token"][4:]))["context"][
                        "roles"
                    ]
                    == request_context()["roles"]
                    for frame in frames
                )
                assert {_proof(frame) for frame in frames} == {
                    "synthetic-proof-a",
                    "synthetic-proof-b",
                }
                assert "oidc_token" not in client._effective_verified_context()
            finally:
                await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["async_enter", "async_exit", "awaitable_enter"])
def test_async_admission_manager_is_refused_before_bytes(socket_dir, monkeypatch, mode):
    async def run():
        path = socket_dir / "engine"
        async with _server(path):
            client = await _connect(path)
            writes: list[bytes] = []
            monkeypatch.setattr(client._writer, "write", writes.append)

            class Manager:
                def __enter__(self):
                    return None

                def __exit__(self, *args):
                    return None

            async def asynchronous(*args):
                raise PermissionError("must run before write")

            if mode == "async_enter":
                monkeypatch.setattr(Manager, "__enter__", asynchronous)
            elif mode == "async_exit":
                monkeypatch.setattr(Manager, "__exit__", asynchronous)
            else:
                monkeypatch.setattr(Manager, "__enter__", lambda self: asynchronous())
            try:
                with client.use_verified_context(
                    _claims(),
                    required_socket_path=str(path),
                    write_admission=lambda _: Manager(),
                ):
                    with pytest.raises(PermissionError, match="no longer admitted"):
                        await client.health()
                assert writes == []
                assert not client._pending
                assert not client._closed
            finally:
                await client.close()

    asyncio.run(run())


def test_cleanup_failure_cannot_preserve_a_partial_frame(socket_dir, monkeypatch):
    async def run():
        path = socket_dir / "engine"
        async with _server(path):
            client = await _connect(path)
            writes: list[bytes] = []

            def broken_payload(data):
                writes.append(data)
                if len(writes) == 2:
                    raise BrokenPipeError("synthetic payload failure")

            @contextlib.contextmanager
            def broken_cleanup(captured):
                try:
                    yield
                finally:
                    raise ValueError("private-verifier-detail synthetic-proof-a")

            monkeypatch.setattr(client._writer, "write", broken_payload)
            try:
                with client.use_verified_context(
                    _claims(),
                    required_socket_path=str(path),
                    write_admission=broken_cleanup,
                ):
                    with pytest.raises(
                        ConnectionError, match="admission cleanup failed"
                    ) as refused:
                        await client.health()
                assert "synthetic-proof" not in str(refused.value)
                assert len(writes) == 2
                assert client._closed
                assert not client._pending
            finally:
                await client.close()

    asyncio.run(run())


def test_sync_delayed_call_rechecks_renewed_proof(socket_dir, monkeypatch):
    import threading

    async def run():
        path = socket_dir / "engine"
        authority = _RenewableAuthority()
        entered, release = threading.Event(), threading.Event()
        original_write_frame = EpistemicGraphClient._write_frame

        async def delayed_frame(client, *args, **kwargs):
            entered.set()
            assert await asyncio.to_thread(release.wait, 2)
            await original_write_frame(client, *args, **kwargs)

        monkeypatch.setattr(EpistemicGraphClient, "_write_frame", delayed_frame)
        async with _server(path) as (frames, chunks, _):

            def call():
                client = SyncEpistemicGraphClient.connect(
                    required_socket_path=str(path),
                    verified_context=request_context(),
                    auth_secret="synthetic-test-secret",
                )
                try:
                    with client.use_verified_context(
                        authority.claims,
                        required_socket_path=str(path),
                        write_admission=authority.admit,
                    ):
                        with pytest.raises(PermissionError, match="no longer admitted"):
                            client.health()
                finally:
                    client.close()

            task = asyncio.create_task(asyncio.to_thread(call))
            assert await asyncio.to_thread(entered.wait, 2)
            authority.now = 101
            authority.renew()
            release.set()
            await task
            assert frames == chunks == []

    asyncio.run(run())
