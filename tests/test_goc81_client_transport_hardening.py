"""GOC-81 — EG native client transport hardening.

W01: transport encryption is an explicit property of the endpoint/profile;
ambient CA variables belonging to unrelated HTTP libraries (``SSL_CERT_FILE``,
``REQUESTS_CA_BUNDLE``) must never select the protocol mode, only supply trust
material once TLS is already selected by explicit precedence.

W02: ``close()`` is an idempotent state transition that always awaits final
writer shutdown, regardless of reader-EOF ordering, transport error, caller
cancellation, partial connect failure, or concurrent close; reconnect allocates
a new lifecycle generation so a stale reader callback can never mark a newer
connection dead.

All tests here are self-contained (they run their own in-process TCP
listeners) and never require the shared native engine.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import os
import ssl
import subprocess

import msgpack
import pytest
from conftest import request_context

from epistemic_graph.client import (
    EpistemicGraphClient,
    SyncEpistemicGraphClient,
    _TlsDecision,
)

pytestmark = pytest.mark.no_engine


# ── Shared helpers ───────────────────────────────────────────────────────────


def _clear_tls_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip every TLS-mode-relevant env var so each test starts from a clean,
    fully-deterministic slate (no host-ambient `SSL_CERT_FILE` etc. leaking in).
    """
    for name in (
        "GRAPH_SERVICE_TLS",
        "GRAPH_SERVICE_TLS_CA",
        "GRAPH_SERVICE_TLS_CA_DIRECTORY",
        "GRAPH_SERVICE_TLS_CLIENT_CERT",
        "GRAPH_SERVICE_TLS_CLIENT_KEY",
        "GRAPH_SERVICE_TLS_CLIENT_KEY_PASSWORD",
        "GRAPH_SERVICE_TLS_SERVER_NAME",
        "GRAPH_SERVICE_TLS_ALLOWED_SERVER_NAMES",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "REQUESTS_CA_BUNDLE",
    ):
        monkeypatch.delenv(name, raising=False)


def _resources() -> tuple[int, int, collections.Counter[str]]:
    """Mirror ``tests/test_sync_client_lifecycle.py``'s FD/thread accounting so
    the 1,000-iteration stress test's "no growth" claim is measured the same
    way the rest of this suite already measures leaks.
    """
    fds = 0
    sockets = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:
            continue
        fds += 1
        sockets += target.startswith("socket:")
    import threading

    return (
        fds,
        sockets,
        collections.Counter(t.name for t in threading.enumerate()),
    )


@pytest.fixture()
def self_signed_cert(tmp_path):
    """A throwaway self-signed cert/key pair for a served TLS test.

    Generated with the system `openssl` CLI (no new test dependency) rather
    than a Python crypto library. RFC 2606 `.example.invalid` per this
    repo's public-repo hostname policy.
    """
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-subj",
            "/CN=epistemic-graph-test.example.invalid",
            "-addext",
            "subjectAltName=DNS:epistemic-graph-test.example.invalid",
        ],
        check=True,
        capture_output=True,
    )
    return str(cert), str(key)


class _EchoHealthServer:
    """A minimal length-prefixed msgpack echo server (Ping/Health only)."""

    def __init__(self, *, ssl_context: ssl.SSLContext | None = None) -> None:
        self.ssl_context = ssl_context
        self.server: asyncio.AbstractServer | None = None
        self.port = 0
        self.first_bytes: bytes | None = None
        self.last_ssl_object: ssl.SSLObject | None = None
        self._writers: list[asyncio.StreamWriter] = []

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.append(writer)
        # Only set for a TLS-wrapped connection -- proves a real handshake
        # completed (asyncio decrypts transparently below the StreamReader,
        # so raw ClientHello bytes are never visible at this layer; unlike
        # the plaintext test's negative check, there is no positive
        # "first byte was 0x16" signal to read here).
        self.last_ssl_object = writer.get_extra_info("ssl_object")
        try:
            while True:
                len_buf = await reader.readexactly(4)
                if self.first_bytes is None:
                    self.first_bytes = len_buf
                msg_len = int.from_bytes(len_buf, byteorder="big")
                body = await reader.readexactly(msg_len)
                req = msgpack.unpackb(body, raw=False)
                if req["method"] == "Hang":
                    # Deliberately never reply -- keeps this specific request
                    # genuinely in-flight (used by the "close during an
                    # in-flight request" test).
                    continue
                resp: dict[str, object] = {"id": req["id"]}
                if req["method"] == "Ping":
                    resp["result"] = "pong"
                elif req["method"] == "Health":
                    resp["result"] = {"status": "ok"}
                else:
                    resp["error"] = "unknown method"
                resp_bytes = msgpack.packb(resp)
                writer.write(len(resp_bytes).to_bytes(4, byteorder="big"))
                writer.write(resp_bytes)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, ssl.SSLError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            if writer in self._writers:
                self._writers.remove(writer)

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle, "127.0.0.1", 0, ssl=self.ssl_context
        )
        self.port = self.server.sockets[0].getsockname()[1]

    async def drop_all_connections(self) -> None:
        """Sever every currently-accepted connection (simulate the peer
        dropping the client) WITHOUT stopping the listener -- unlike
        `Server.close()`/`wait_closed()`, which only stop accepting NEW
        connections and (on this asyncio version) `wait_closed()` blocks
        forever while any accepted connection is still open.
        """
        for writer in list(self._writers):
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def stop(self) -> None:
        if self.server is not None:
            await self.drop_all_connections()
            self.server.close()
            await self.server.wait_closed()


# ── W01 — TLS mode-selection precedence (pure decision, no I/O) ────────────


def test_explicit_plaintext_wins_over_ambient_ca_vars(monkeypatch):
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("SSL_CERT_FILE", "/some/unrelated/http/library/ca.pem")
    decision = EpistemicGraphClient._resolve_tls_decision(
        False, client_cert=None, client_key=None, server_hostname=None
    )
    assert decision == _TlsDecision(False, "explicit-arg", None, "none")


def test_explicit_tls_true_enables_regardless_of_profile(monkeypatch):
    _clear_tls_env(monkeypatch)
    decision = EpistemicGraphClient._resolve_tls_decision(
        True, client_cert=None, client_key=None, server_hostname="svc.example.invalid"
    )
    assert decision.enabled is True
    assert decision.profile == "explicit-arg"


def test_named_profile_env_on_enables_tls(monkeypatch):
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("GRAPH_SERVICE_TLS", "on")
    decision = EpistemicGraphClient._resolve_tls_decision(
        None, client_cert=None, client_key=None, server_hostname=None
    )
    assert decision.enabled is True
    assert decision.profile == "named-profile"


def test_named_profile_env_off_disables_even_with_generic_ca_vars(monkeypatch):
    """An explicit `GRAPH_SERVICE_TLS=off` (the named profile's own switch) must
    win over the AMBIENT `SSL_CERT_FILE` -- both are "no argument passed", but
    one is a graph-service-specific explicit choice and the other is a bare,
    unrelated ambient variable.
    """
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("GRAPH_SERVICE_TLS", "off")
    monkeypatch.setenv("SSL_CERT_FILE", "/some/unrelated/http/library/ca.pem")
    decision = EpistemicGraphClient._resolve_tls_decision(
        None, client_cert=None, client_key=None, server_hostname=None
    )
    assert decision.enabled is False
    assert decision.profile == "named-profile"


def test_generic_ca_vars_alone_never_select_tls_mode(monkeypatch):
    """THE core GOC-81 W01 acceptance gate: bare presence of `SSL_CERT_FILE` /
    `SSL_CERT_DIR` / `REQUESTS_CA_BUNDLE` (ambient vars belonging to unrelated
    HTTP libraries)
    must never flip the mode to TLS when nothing else selected it.
    """
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("SSL_CERT_FILE", "/some/unrelated/http/library/ca.pem")
    monkeypatch.setenv("SSL_CERT_DIR", "/some/unrelated/http/library/ca.d")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/another/unrelated/bundle.pem")
    decision = EpistemicGraphClient._resolve_tls_decision(
        None, client_cert=None, client_key=None, server_hostname=None
    )
    assert decision == _TlsDecision(False, "default", None, "none")
    # And the higher-level context builder must agree: no context at all.
    assert (
        EpistemicGraphClient._resolve_tls(None, client_cert=None, client_key=None)
        is None
    )


def test_service_specific_ca_var_does_select_tls(monkeypatch):
    """Unlike the ambient/unrelated vars above, `GRAPH_SERVICE_TLS_CA` is this
    client's OWN named-profile CA source and legitimately selects TLS.
    """
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("GRAPH_SERVICE_TLS_CA", "/etc/epistemic-graph/ca.pem")
    decision = EpistemicGraphClient._resolve_tls_decision(
        None, client_cert=None, client_key=None, server_hostname=None
    )
    assert decision.enabled is True
    assert decision.profile == "named-profile"
    assert decision.trust_source == "ca_bundle"


def test_service_specific_ca_directory_selects_tls(monkeypatch):
    """The graph-service CA directory is a named profile input, unlike the
    generic `SSL_CERT_DIR` trust source, so it selects TLS even without a
    separate `GRAPH_SERVICE_TLS=on` switch.
    """
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv(
        "GRAPH_SERVICE_TLS_CA_DIRECTORY", "/etc/epistemic-graph/ca.d"
    )
    decision = EpistemicGraphClient._resolve_tls_decision(
        None, client_cert=None, client_key=None, server_hostname=None
    )
    assert decision == _TlsDecision(
        True, "named-profile", None, "ca_directory"
    )


def test_conflicting_tls_disabled_with_client_cert_is_rejected(monkeypatch):
    _clear_tls_env(monkeypatch)
    with pytest.raises(ValueError, match="explicitly disabled"):
        EpistemicGraphClient._resolve_tls_decision(
            False,
            client_cert="/tmp/client.pem",
            client_key="/tmp/client.key",
            server_hostname=None,
        )


def test_hostname_outside_allowlist_is_rejected(monkeypatch):
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("GRAPH_SERVICE_TLS_ALLOWED_SERVER_NAMES", "good.example.invalid")
    with pytest.raises(ValueError, match="allowlist"):
        EpistemicGraphClient._resolve_tls(
            True,
            client_cert=None,
            client_key=None,
            server_hostname="evil.example.invalid",
        )
    # The allowed name itself must still build a context (system trust store).
    context = EpistemicGraphClient._resolve_tls(
        True, client_cert=None, client_key=None, server_hostname="good.example.invalid"
    )
    assert isinstance(context, ssl.SSLContext)


def test_missing_trust_material_bad_ca_path_is_rejected(monkeypatch):
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("GRAPH_SERVICE_TLS_CA", "/no/such/ca/bundle.pem")
    with pytest.raises(ValueError, match="trust material"):
        EpistemicGraphClient._resolve_tls(
            True, client_cert=None, client_key=None, server_hostname=None
        )


def test_mtls_cert_without_key_is_rejected(monkeypatch):
    _clear_tls_env(monkeypatch)
    with pytest.raises(ValueError, match="both client certificate and key"):
        EpistemicGraphClient._resolve_tls(
            True,
            client_cert="/tmp/client.pem",
            client_key=None,
            server_hostname=None,
        )


def test_injected_ssl_context_rejects_client_cert_paths(monkeypatch):
    _clear_tls_env(monkeypatch)
    ctx = ssl.create_default_context()
    with pytest.raises(ValueError, match="injected TLS context"):
        EpistemicGraphClient._resolve_tls(
            ctx, client_cert="/tmp/client.pem", client_key="/tmp/client.key"
        )


@pytest.mark.parametrize(
    ("tls_arg", "env", "cert_arg", "expected_enabled"),
    [
        (False, {}, None, False),
        (True, {}, None, True),
        (None, {"GRAPH_SERVICE_TLS_CA": "/etc/x/ca.pem"}, None, True),
        (None, {"SSL_CERT_FILE": "/etc/unrelated/ca.pem"}, None, False),
        (
            None,
            {"GRAPH_SERVICE_TLS": "off", "SSL_CERT_FILE": "/etc/unrelated/ca.pem"},
            None,
            False,
        ),
        (None, {}, "/tmp/client.pem", True),
        (None, {}, None, False),
    ],
    ids=[
        "explicit-plaintext",
        "explicit-tls",
        "named-trust-profile",
        "generic-ca-vars-with-plaintext",
        "conflicting-profile-vs-ambient-resolved-by-explicit-off",
        "explicit-cert-arg",
        "product-default",
    ],
)
def test_tls_decision_matrix(monkeypatch, tls_arg, env, cert_arg, expected_enabled):
    """The full parameterized decision matrix required by the lane's test plan."""
    _clear_tls_env(monkeypatch)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    key_arg = "/tmp/client.key" if cert_arg else None
    decision = EpistemicGraphClient._resolve_tls_decision(
        tls_arg, client_cert=cert_arg, client_key=key_arg, server_hostname=None
    )
    assert decision.enabled is expected_enabled


# ── W01 — served-connection tests (async client) ────────────────────────────


@pytest.mark.asyncio
async def test_served_plaintext_connection_ignores_ssl_cert_file(monkeypatch):
    """Wire-level proof: a plaintext endpoint with `SSL_CERT_FILE` merely SET
    must never attempt a TLS handshake -- the server's first observed bytes
    are the plain length-prefix, not a TLS record header (0x16 = handshake).
    """
    _clear_tls_env(monkeypatch)
    monkeypatch.setenv("SSL_CERT_FILE", "/some/unrelated/http/library/ca.pem")

    server = _EchoHealthServer()
    await server.start()
    client = None
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"
        assert server.first_bytes is not None
        # A TLS ClientHello record starts with content-type 0x16; a plain
        # length-prefixed frame's first byte is the high byte of a small
        # length and is never 0x16 for these tiny request frames.
        assert server.first_bytes[0] != 0x16
    finally:
        if client is not None:
            await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_served_tls_connection_succeeds_when_explicitly_selected(
    monkeypatch, self_signed_cert
):
    """The mirror-image proof: when TLS IS explicitly selected (with correct
    trust material), the handshake actually happens and the RPC succeeds --
    W01 only forbids AMBIENT vars from selecting the mode, not TLS itself.
    """
    _clear_tls_env(monkeypatch)
    cert_path, key_path = self_signed_cert
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)

    server = _EchoHealthServer(ssl_context=server_ctx)
    await server.start()
    client = None
    try:
        # Trust material supplied via the client's OWN named-profile CA var,
        # set BEFORE connect() so `_resolve_tls` picks it up.
        monkeypatch.setenv("GRAPH_SERVICE_TLS_CA", cert_path)
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
            tls=True,
            tls_client_cert=None,
            tls_client_key=None,
            tls_server_hostname="epistemic-graph-test.example.invalid",
        )
        assert await client.ping() == "pong"
        assert server.last_ssl_object is not None, "expected a real completed TLS handshake"
        assert server.last_ssl_object.cipher() is not None
    finally:
        if client is not None:
            await client.close()
        await server.stop()


def test_served_tls_via_sync_client(monkeypatch, self_signed_cert):
    """Parity check: the sync wrapper reaches the SAME `_resolve_tls` path.

    Plain (non-async) test, deliberately -- `SyncEpistemicGraphClient` owns
    its OWN background event-loop thread; driving it from inside another
    already-running asyncio test loop via `run_in_executor` only adds
    irrelevant nested-loop scheduling risk. The served TLS listener instead
    runs on its own background thread with its own loop, exactly like a
    real, independent server process would.
    """
    import threading

    _clear_tls_env(monkeypatch)
    cert_path, key_path = self_signed_cert
    monkeypatch.setenv("GRAPH_SERVICE_TLS_CA", cert_path)
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)

    state: dict[str, object] = {}
    ready = threading.Event()
    stop = threading.Event()

    def _run_server() -> None:
        async def _main() -> None:
            server = _EchoHealthServer(ssl_context=server_ctx)
            await server.start()
            state["port"] = server.port
            ready.set()
            while not stop.is_set():
                await asyncio.sleep(0.02)
            await server.stop()

        asyncio.run(_main())

    thread = threading.Thread(target=_run_server, daemon=True)
    thread.start()
    assert ready.wait(timeout=5), "TLS test server never started"

    client = None
    try:
        client = SyncEpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{state['port']}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
            tls=True,
            tls_server_hostname="epistemic-graph-test.example.invalid",
        )
        assert client.ping() == "pong"
    finally:
        if client is not None:
            client.close()
        stop.set()
        thread.join(timeout=5)


# ── W02 — close-lifecycle idempotency ───────────────────────────────────────


@pytest.mark.asyncio
async def test_close_is_idempotent_repeated_calls():
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"
        await client.close()
        # Repeated closes are no-ops, not errors.
        await client.close()
        await client.close()
        assert client._closing is True
        assert client._closed is True
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_cancelled_close_finishes_shared_writer_shutdown():
    """Canceling one close waiter must not strand the shared teardown task;
    the caller observes cancellation only after the writer has been joined.
    """
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        writer = client._writer
        wait_started = asyncio.Event()
        release_wait = asyncio.Event()
        wait_closed_calls = {"n": 0}
        original_wait_closed = writer.wait_closed

        async def _controlled_wait_closed():
            wait_closed_calls["n"] += 1
            wait_started.set()
            await release_wait.wait()
            return await original_wait_closed()

        writer.wait_closed = _controlled_wait_closed
        close_task = asyncio.ensure_future(client.close())
        await asyncio.wait_for(wait_started.wait(), timeout=2.0)
        close_task.cancel()
        await asyncio.sleep(0)
        assert not close_task.done(), "canceled waiter must await shared teardown"

        release_wait.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(close_task, timeout=2.0)

        # A later close joins the already-completed shared task and does not
        # issue another writer wait.
        await client.close()
        assert wait_closed_calls["n"] == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_close_after_peer_eof_still_tears_down_writer():
    """THE core GOC-81 W02 regression: the reader loop observing EOF first
    must NOT make a later `close()` a silent no-op that skips
    `writer.close()`/`writer.wait_closed()` -- that was the leak.
    """
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"

        # Force the server to drop the connection so the reader loop observes
        # EOF and flips `_closed` (admission state) WITHOUT going through
        # `close()` at all. Sever only the accepted connection -- NOT the
        # listener -- since this asyncio version's `Server.wait_closed()`
        # blocks forever while any accepted connection is still open.
        await server.drop_all_connections()
        for _ in range(50):
            if client._closed:
                break
            await asyncio.sleep(0.02)
        assert client._closed is True, "reader EOF must flip admission state"

        writer = client._writer
        wrapped_wait_closed_calls = {"n": 0}
        original_wait_closed = writer.wait_closed

        async def _counting_wait_closed():
            wrapped_wait_closed_calls["n"] += 1
            return await original_wait_closed()

        writer.wait_closed = _counting_wait_closed

        # Pre-fix: `close()` checked `if not self._closed` and returned
        # immediately here without ever calling `wait_closed()`.
        await client.close()
        assert wrapped_wait_closed_calls["n"] == 1, (
            "close() must always await writer.wait_closed(), even after a "
            "prior reader-EOF already flipped _closed"
        )
        assert client._closing is True
    finally:
        with contextlib.suppress(Exception):
            await server.stop()


@pytest.mark.asyncio
async def test_close_preserves_first_terminal_error():
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"
        original = ConnectionError("original connection failure")
        client._mark_dead(original)
        await client.close()
        assert client._terminal_error is original
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_close_after_transport_error_closes_writer_once():
    """An error path may close the poisoned stream before the owner calls
    `close()`; the final close must still await it without issuing a duplicate
    writer-close request.
    """
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"

        writer = client._writer
        close_calls = {"n": 0}
        wait_closed_calls = {"n": 0}
        original_close = writer.close
        original_wait_closed = writer.wait_closed

        def _counting_close():
            close_calls["n"] += 1
            return original_close()

        async def _counting_wait_closed():
            wait_closed_calls["n"] += 1
            return await original_wait_closed()

        writer.close = _counting_close
        writer.wait_closed = _counting_wait_closed
        client._mark_dead(ConnectionError("transport failed"))
        await client.close()

        assert close_calls["n"] == 1
        assert wait_closed_calls["n"] == 1
        assert client._terminal_error is not None
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_close_during_connect_does_not_leak_or_hang(monkeypatch):
    """`close()` racing an in-flight `_reconnect()` must neither hang nor
    leave the freshly-dialed connection un-torn-down. Both share `self._lock`,
    so this exercises that serialization directly with a controllable dial.
    """
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"

        # Force the ADMISSION flag on so the next `_ensure_connection` call
        # reconnects, and make the dial itself controllable.
        client._mark_dead(ConnectionError("forced for test"))
        dial_started = asyncio.Event()
        release_dial = asyncio.Event()
        real_open_streams = EpistemicGraphClient._open_streams

        async def _slow_open_streams(*args, **kwargs):
            dial_started.set()
            await release_dial.wait()
            return await real_open_streams(*args, **kwargs)

        # `_open_streams` is a `@staticmethod`; the replacement must be too,
        # or `self._open_streams(...)` inside `_reconnect` binds `self` as an
        # extra leading positional argument.
        monkeypatch.setattr(
            EpistemicGraphClient, "_open_streams", staticmethod(_slow_open_streams)
        )

        reconnect_task = asyncio.ensure_future(client._ensure_connection())
        await asyncio.wait_for(dial_started.wait(), timeout=2.0)

        close_task = asyncio.ensure_future(client.close())
        await asyncio.sleep(0.05)
        assert not close_task.done(), "close() must wait for the in-flight dial, not race it"

        release_dial.set()
        await asyncio.wait_for(asyncio.gather(reconnect_task, close_task), timeout=2.0)
        assert client._closing is True
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_close_during_in_flight_request_fails_it_cleanly():
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=5.0,
        )
        # A method the mock server never replies to leaves the request
        # genuinely in-flight until something fails it.
        pending = asyncio.ensure_future(client._send("Hang"))
        await asyncio.sleep(0.05)
        assert not pending.done()

        await asyncio.wait_for(client.close(), timeout=2.0)

        with pytest.raises((ConnectionError, asyncio.CancelledError)):
            await asyncio.wait_for(pending, timeout=2.0)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_cancelled_request_releases_pending_and_close_is_clean():
    """Caller cancellation removes only that request from the demux map;
    the shared stream remains usable until the explicit owner close, which
    still performs one complete writer shutdown.
    """
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=5.0,
        )
        writer = client._writer
        close_calls = {"n": 0}
        wait_closed_calls = {"n": 0}
        original_close = writer.close
        original_wait_closed = writer.wait_closed

        def _counting_close():
            close_calls["n"] += 1
            return original_close()

        async def _counting_wait_closed():
            wait_closed_calls["n"] += 1
            return await original_wait_closed()

        writer.close = _counting_close
        writer.wait_closed = _counting_wait_closed

        pending = asyncio.ensure_future(client._send("Hang"))
        await asyncio.sleep(0.05)
        assert not pending.done()

        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert client._pending == {}, "cancelled request must not remain in demux state"

        await client.close()
        assert close_calls["n"] == 1
        assert wait_closed_calls["n"] == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_two_concurrent_closes_run_teardown_once():
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"

        writer = client._writer
        close_calls = {"n": 0}
        original_close = writer.close

        def _counting_close():
            close_calls["n"] += 1
            return original_close()

        writer.close = _counting_close

        await asyncio.gather(client.close(), client.close())
        assert close_calls["n"] == 1
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_reconnect_after_eof_uses_new_generation():
    server = _EchoHealthServer()
    await server.start()
    try:
        client = await EpistemicGraphClient.connect(
            tcp_addr=f"127.0.0.1:{server.port}",
            auth_secret="s",
            verified_context=request_context(),
            timeout=2.0,
        )
        assert await client.ping() == "pong"
        starting_generation = client._generation

        await server.stop()
        for _ in range(50):
            if client._closed:
                break
            await asyncio.sleep(0.02)

        server2 = _EchoHealthServer()
        # Re-dial to the SAME port is not guaranteed available; instead
        # exercise `_reconnect` directly against a fresh listener the client
        # already knows how to reach by re-pointing its remembered endpoint.
        await server2.start()
        client._tcp_addr = f"127.0.0.1:{server2.port}"
        try:
            assert await client.ping() == "pong"
            assert client._generation == starting_generation + 1

            # Invariant 5, proven directly: a callback bound to the OLD
            # (now-stale) generation must be a no-op against current state.
            client._terminal_error = None
            client._on_reader_terminated(
                starting_generation, ConnectionError("stale reader callback")
            )
            assert client._closed is False, "a stale-generation callback marked the NEW connection dead"
            assert client._terminal_error is None
        finally:
            await server2.stop()
    finally:
        with contextlib.suppress(Exception):
            await server.stop()


@pytest.mark.asyncio
async def test_thousand_connect_close_iterations_no_resource_growth():
    server = _EchoHealthServer()
    await server.start()
    try:
        await asyncio.sleep(0)  # let the server's accept loop settle
        baseline_tasks = len(asyncio.all_tasks())
        baseline_fds, baseline_sockets, _ = _resources()

        for _ in range(1000):
            client = await EpistemicGraphClient.connect(
                tcp_addr=f"127.0.0.1:{server.port}",
                auth_secret="s",
                verified_context=request_context(),
                timeout=2.0,
            )
            assert await client.ping() == "pong"
            await client.close()

        # Give the loop one tick to finish reaping any just-closed transports.
        await asyncio.sleep(0.1)
        final_tasks = len(asyncio.all_tasks())
        final_fds, final_sockets, _ = _resources()

        assert final_tasks <= baseline_tasks, "pending-task count must not grow"
        assert final_fds <= baseline_fds, "open file-descriptor count must not grow"
        assert final_sockets <= baseline_sockets, "open socket count must not grow"
    finally:
        await server.stop()
