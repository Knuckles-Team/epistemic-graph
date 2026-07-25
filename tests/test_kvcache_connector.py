"""Unit tests for the EG-187 Python KV-cache driver (CONCEPT:EG-KG.backend.shipped-pip-installable-python).

Runs a tiny in-process HTTP server implementing the EG-187 ``/kv`` surface
(``GET|PUT|HEAD /kv/<hash>``, ``GET /kv/<hash>/exists``, ``GET /kv/stats``) with
mandatory bearer-token enforcement, and exercises the driver end-to-end over a
real socket — no mocking of the HTTP layer, no third-party test deps.

Run standalone (bypass the slow engine-build conftest fixture)::

    python3 -m pytest tests/test_kvcache_connector.py --noconftest -q
"""

from __future__ import annotations

import json
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

import pytest

from epistemic_graph.kvcache import (
    HTTPX_AVAILABLE,
    HttpxTransport,
    KvCacheConfig,
    KvCacheStats,
    RemoteKVConnector,
    RemoteKVL2Connector,
    UrllibTransport,
)

_TEST_TOKEN = "test-kvcache-token"


class _KvState:
    """The server-side content-addressed store + stat counters."""

    def __init__(self) -> None:
        self.blocks: dict[str, bytes] = {}
        self.get_hits = 0
        self.get_misses = 0
        self.dedup_hits = 0
        self.require_token = _TEST_TOKEN


def _make_handler(state: _KvState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence test noise
            pass

        # -- auth ---------------------------------------------------------
        def _authed(self) -> bool:
            got = self.headers.get("Authorization", "")
            if got == f"Bearer {state.require_token}":
                return True
            self.send_response(401)
            self.end_headers()
            return False

        def _key(self) -> str | None:
            # /kv/<hash> or /kv/<hash>/exists
            parts = self.path.strip("/").split("/")
            if len(parts) >= 2 and parts[0] == "kv":
                return unquote(parts[1])
            return None

        # -- verbs --------------------------------------------------------
        def do_GET(self) -> None:
            if not self._authed():
                return
            if self.path == "/kv/stats":
                body = json.dumps(
                    {
                        "unique_blocks": len(state.blocks),
                        "total_refs": len(state.blocks),
                        "resident_bytes": sum(len(b) for b in state.blocks.values()),
                        "logical_bytes": 0,
                        "dedup_savings_bytes": 0,
                        "dedup_hits": state.dedup_hits,
                        "get_hits": state.get_hits,
                        "get_misses": state.get_misses,
                        "future_counter_the_client_ignores": 7,
                    }
                ).encode()
                self._respond(200, body, "application/json")
                return
            if self.path.endswith("/exists"):
                key = self._key()
                present = key in state.blocks
                body = json.dumps({"hash": key, "exists": present}).encode()
                self._respond(200, body, "application/json")
                return
            key = self._key()
            if key is not None and key in state.blocks:
                state.get_hits += 1
                self._respond(200, state.blocks[key], "application/octet-stream")
            else:
                state.get_misses += 1
                self._respond(404, b"")

        def do_HEAD(self) -> None:
            if not self._authed():
                return
            key = self._key()
            self.send_response(200 if key in state.blocks else 404)
            self.end_headers()

        def do_PUT(self) -> None:
            if not self._authed():
                return
            key = self._key()
            if key is None:
                self.send_response(400)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length", 0))
            data = self.rfile.read(length)
            new = key not in state.blocks
            if not new:
                state.dedup_hits += 1
            state.blocks[key] = data
            self.send_response(201 if new else 200)
            self.end_headers()

        # -- helper -------------------------------------------------------
        def _respond(self, status: int, body: bytes, ctype: str = "text/plain") -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

    return Handler


@pytest.fixture
def kv_server():
    state = _KvState()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # `server_address` is typed as an IPv4-or-IPv6 socket address (2- or 4-tuple, host
    # possibly `bytes`); this server always binds IPv4 (`"127.0.0.1"`), so normalize to
    # a plain str/int pair instead of unpacking the whole (union-typed) tuple.
    addr = server.server_address
    host = addr[0].decode() if isinstance(addr[0], bytes) else addr[0]
    port = addr[1]
    base_url = f"http://{host}:{port}"
    try:
        yield base_url, state
    finally:
        server.shutdown()
        server.server_close()


def _connector(base_url: str, token: str = _TEST_TOKEN) -> RemoteKVConnector:
    return RemoteKVConnector(
        KvCacheConfig(base_url=base_url, token=token, timeout_s=5.0)
    )


# --------------------------------------------------------------------------- #
# RemoteKVConnector round-trip
# --------------------------------------------------------------------------- #
def test_put_get_roundtrip(kv_server):
    base_url, _ = kv_server
    with _connector(base_url) as conn:
        assert conn.get("h1") is None  # miss before store
        assert conn.put("h1", b"kv-block-bytes\x00\x0a\xff") is True
        assert conn.get("h1") == b"kv-block-bytes\x00\x0a\xff"


def test_put_dedup_status(kv_server):
    base_url, state = kv_server
    with _connector(base_url) as conn:
        assert conn.put("dup", b"AAAA") is True  # 201 new
        assert conn.put("dup", b"AAAA") is True  # 200 dedup hit
    assert state.dedup_hits == 1


def test_exists_and_contains(kv_server):
    base_url, _ = kv_server
    with _connector(base_url) as conn:
        assert conn.contains("missing") is False
        assert conn.exists("missing") is False
        conn.put("present", b"x")
        assert conn.contains("present") is True  # HEAD
        assert conn.exists("present") is True  # GET /exists JSON


def test_stats_roundtrip(kv_server):
    base_url, _ = kv_server
    with _connector(base_url) as conn:
        conn.put("a", b"1234")
        conn.put("b", b"5678")
        conn.get("a")
        conn.get("nope")
        stats = conn.stats()
    assert isinstance(stats, KvCacheStats)
    assert stats.unique_blocks == 2
    assert stats.get_hits == 1
    assert stats.get_misses == 1
    assert stats.resident_bytes == 8


def test_content_addressed_keys_with_special_chars(kv_server):
    base_url, _ = kv_server
    # A token-hash key containing reserved URL characters must round-trip intact.
    key = "prefix/hash with+special=chars"
    with _connector(base_url) as conn:
        assert conn.put(key, b"payload") is True
        assert conn.get(key) == b"payload"
        assert conn.contains(key) is True


# --------------------------------------------------------------------------- #
# Bearer-token header
# --------------------------------------------------------------------------- #
def test_token_header_enforced(kv_server):
    base_url, state = kv_server
    state.require_token = "s3cr3t"
    # A wrong token → 401 → graceful miss.
    with _connector(base_url, token="wrong") as rejected:
        assert rejected.put("k", b"v") is False
        assert rejected.get("k") is None
        assert rejected.contains("k") is False
    # With the right token → success.
    with _connector(base_url, token="s3cr3t") as authed:
        assert authed.put("k", b"v") is True
        assert authed.get("k") == b"v"


def test_graceful_degradation_when_engine_down():
    # Nothing listening → every op degrades to a miss, never raises.
    conn = RemoteKVConnector(
        KvCacheConfig(base_url="http://127.0.0.1:1", token=_TEST_TOKEN, timeout_s=0.25)
    )
    try:
        assert conn.get("x") is None
        assert conn.put("x", b"y") is False
        assert conn.contains("x") is False
        assert conn.exists("x") is False
        assert conn.stats() == KvCacheStats()
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Config from environment (EG-187 vars)
# --------------------------------------------------------------------------- #
def test_config_from_env(monkeypatch):
    monkeypatch.setenv("EPISTEMIC_GRAPH_KVCACHE_ADDR", "10.0.0.5:9130")
    monkeypatch.setenv("EPISTEMIC_GRAPH_KVCACHE_TOKEN", "tok")
    monkeypatch.delenv("EPISTEMIC_GRAPH_KVCACHE_URL", raising=False)
    cfg = KvCacheConfig.from_env()
    assert cfg.base_url == "https://10.0.0.5:9130"
    assert cfg.token == "tok"


def test_config_from_env_bare_port(monkeypatch):
    monkeypatch.setenv("EPISTEMIC_GRAPH_KVCACHE_TOKEN", _TEST_TOKEN)
    monkeypatch.delenv("EPISTEMIC_GRAPH_KVCACHE_URL", raising=False)
    monkeypatch.setenv("EPISTEMIC_GRAPH_KVCACHE_ADDR", "9200")
    assert KvCacheConfig.from_env().base_url == "http://127.0.0.1:9200"


def test_tls_profile_uses_standard_ca_bundle_environment(monkeypatch):
    monkeypatch.setenv("EPISTEMIC_GRAPH_KVCACHE_TOKEN", _TEST_TOKEN)
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "runtime-ca-bundle.pem")
    config = KvCacheConfig.from_env()
    assert config.ca_bundle == "runtime-ca-bundle.pem"


def test_mtls_identity_must_be_a_pair():
    with pytest.raises(ValueError, match="configured together"):
        KvCacheConfig(token=_TEST_TOKEN, client_cert="runtime-client.pem")


def test_bearer_or_jwt_token_is_mandatory(monkeypatch):
    monkeypatch.delenv("EPISTEMIC_GRAPH_KVCACHE_TOKEN", raising=False)
    with pytest.raises(ValueError, match="token must be configured"):
        KvCacheConfig.from_env()
    with pytest.raises(ValueError, match="token must be configured"):
        KvCacheConfig(token="  ")


def test_non_loopback_plain_http_is_rejected():
    with pytest.raises(ValueError, match="require HTTPS"):
        KvCacheConfig(base_url="http://cache.example:9130", token=_TEST_TOKEN)
    config = KvCacheConfig(base_url="https://cache.example:9130", token=_TEST_TOKEN)
    assert config.base_url == "https://cache.example:9130"


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx extra is not installed")
def test_httpx_transport_cannot_disable_peer_verification():
    with pytest.raises(ValueError, match="cannot be disabled"):
        HttpxTransport(
            base_url="https://cache.example:9130",
            timeout_s=1.0,
            tls_context=False,
            max_connections=1,
        )


def test_urllib_transport_rejects_unverified_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with pytest.raises(ValueError, match="cannot be disabled"):
        UrllibTransport(timeout_s=1.0, tls_context=context)


# --------------------------------------------------------------------------- #
# RemoteKVL2Connector — LMCache native_plugin native-client contract
# --------------------------------------------------------------------------- #
def _drain(l2: RemoteKVL2Connector, future_id: int):
    """Block until the batch identified by future_id completes; return its record."""
    import select

    while True:
        select.select([l2.event_fd()], [], [], 2.0)
        for rec in l2.drain_completions():
            if rec[0] == future_id:
                return rec


def test_l2_native_plugin_batch_roundtrip(kv_server):
    base_url, _ = kv_server
    l2 = RemoteKVL2Connector(
        base_url=base_url, token=_TEST_TOKEN, num_workers=2, timeout_s=5.0
    )
    try:
        # submit_batch_set → PUT
        fid = l2.submit_batch_set(
            ["k1", "k2"], [memoryview(b"AAAA"), memoryview(b"BBBB")]
        )
        rec = _drain(l2, fid)
        assert rec[1] is True  # ok

        # submit_batch_exists → HEAD bitmap
        fid = l2.submit_batch_exists(["k1", "missing"])
        rec = _drain(l2, fid)
        assert rec[3] == [True, False]

        # submit_batch_get → GET into pre-allocated buffers
        buf1 = memoryview(bytearray(4))
        buf2 = memoryview(bytearray(4))
        fid = l2.submit_batch_get(["k1", "missing"], [buf1, buf2])
        rec = _drain(l2, fid)
        assert rec[3] == [True, False]
        assert bytes(buf1) == b"AAAA"
    finally:
        l2.close()


def test_l2_size_mismatch_is_miss(kv_server):
    base_url, _ = kv_server
    l2 = RemoteKVL2Connector(
        base_url=base_url, token=_TEST_TOKEN, num_workers=1, timeout_s=5.0
    )
    try:
        fid = l2.submit_batch_set(["big"], [memoryview(b"12345678")])
        _drain(l2, fid)
        # Fetch into an undersized buffer → reported as a miss (no corruption).
        buf = memoryview(bytearray(4))
        fid = l2.submit_batch_get(["big"], [buf])
        rec = _drain(l2, fid)
        assert rec[3] == [False]
    finally:
        l2.close()


def test_l2_injected_connector(kv_server):
    # adapter_params-style construction can also accept a pre-built connector.
    base_url, _ = kv_server
    conn = _connector(base_url)
    l2 = RemoteKVL2Connector(connector=conn, num_workers=1)
    try:
        assert l2.event_fd() >= 0
        fid = l2.submit_batch_set(["z"], [memoryview(b"zz")])
        _drain(l2, fid)
        assert conn.get("z") == b"zz"
    finally:
        l2.close()  # does not own conn
        conn.close()


# --------------------------------------------------------------------------- #
# Fleet-shared prefix reuse — two vLLM instances share blocks THROUGH one engine
# (W4.9 / CONCEPT:EG-KG.backend.networked-shared-kv). A shared in-memory engine
# behind a dependency-injected transport models "N instances, one engine".
# --------------------------------------------------------------------------- #
class _Resp:
    """A minimal `.status` / `.body` response the connector reads."""

    __slots__ = ("status", "body")

    def __init__(self, status: int, body: bytes = b"") -> None:
        self.status = status
        self.body = body


class _SharedFakeEngine:
    """One in-memory content-addressed /kv engine, SHARED across connectors."""

    def __init__(self) -> None:
        self.blocks: dict[str, bytes] = {}
        self.get_hits = 0
        self.get_misses = 0
        self.dedup_hits = 0


class _SharedTransport:
    """A `_Transport` over a shared :class:`_SharedFakeEngine` — every connector that
    holds one points at the SAME engine, so a block instance A PUTs is a HIT for B."""

    def __init__(self, engine: _SharedFakeEngine) -> None:
        self._engine = engine

    def request(self, method, url, *, body=None, headers=None):  # noqa: ANN001
        path = urlsplit(url).path
        eng = self._engine
        if path == "/kv/stats":
            return _Resp(
                200,
                json.dumps(
                    {
                        "unique_blocks": len(eng.blocks),
                        "get_hits": eng.get_hits,
                        "get_misses": eng.get_misses,
                        "dedup_hits": eng.dedup_hits,
                    }
                ).encode(),
            )
        if path.startswith("/kv/") and path.endswith("/exists"):
            key = unquote(path[len("/kv/") : -len("/exists")])
            return _Resp(
                200, json.dumps({"hash": key, "exists": key in eng.blocks}).encode()
            )
        if path.startswith("/kv/"):
            key = unquote(path[len("/kv/") :])
            if method == "PUT":
                new = key not in eng.blocks
                if not new:
                    eng.dedup_hits += 1
                eng.blocks[key] = bytes(body or b"")
                return _Resp(201 if new else 200)
            if method == "HEAD":
                return _Resp(200 if key in eng.blocks else 404)
            if method == "GET":
                if key in eng.blocks:
                    eng.get_hits += 1
                    return _Resp(200, eng.blocks[key])
                eng.get_misses += 1
                return _Resp(404)
        return _Resp(404)

    def close(self) -> None:  # pragma: no cover - trivial
        pass


def _shared_connector(engine: _SharedFakeEngine) -> RemoteKVConnector:
    return RemoteKVConnector(
        KvCacheConfig(base_url="http://127.0.0.1:9130", token=_TEST_TOKEN, timeout_s=5.0),
        transport=_SharedTransport(engine),
    )


def test_cross_instance_shared_prefix_hit():
    """A prefix block one instance offloads is a cache HIT for another, through the
    engine — driver-level proof of fleet-shared prefix reuse."""
    engine = _SharedFakeEngine()
    instance_a = _shared_connector(engine)
    instance_b = _shared_connector(engine)
    try:
        prefix = "tok:shared-system-prompt-prefix"
        page = b"attention-kv-page" * 32
        assert instance_b.get(prefix) is None  # B: cold miss (would recompute)
        assert instance_a.put(prefix, page) is True  # A: computes + offloads
        assert instance_b.contains(prefix) is True  # B sees it via the shared engine
        assert instance_b.get(prefix) == page  # cross-instance HIT — no recompute
    finally:
        instance_a.close()
        instance_b.close()
    assert engine.get_hits == 1 and engine.get_misses == 1  # measured hit-rate 0.5


def test_l2_mp_connector_carries_hybrid_recurrent_state():
    """LMCacheMPConnector (native_plugin L2, the MP path) carries a hybrid Mamba/GDN
    model's recurrent state ALONGSIDE attention-KV blocks — the non-regression guard for
    "hybrid models run via the MP connector, not V1". The recurrent snapshot is a
    distinct key with a different byte length; a V1 attention-only shape could not carry
    it, but the opaque-bytes MP native client does.
    """
    engine = _SharedFakeEngine()
    conn = _shared_connector(engine)
    l2 = RemoteKVL2Connector(connector=conn, num_workers=2)
    try:
        attn = b"A" * 4096  # attention KV page
        recurrent = b"M" * 777  # Mamba conv_state + ssm_state snapshot (different size)
        fid = l2.submit_batch_set(
            ["blk:attn", "blk:mamba-state"],
            [memoryview(attn), memoryview(recurrent)],
        )
        assert _drain(l2, fid)[1] is True  # both blocks offloaded

        # Both round-trip into correctly-sized buffers — recurrent state is NOT dropped.
        buf_attn = memoryview(bytearray(len(attn)))
        buf_rec = memoryview(bytearray(len(recurrent)))
        fid = l2.submit_batch_get(["blk:attn", "blk:mamba-state"], [buf_attn, buf_rec])
        rec = _drain(l2, fid)
        assert rec[3] == [True, True], "attention AND recurrent-state blocks both hit"
        assert bytes(buf_attn) == attn
        assert bytes(buf_rec) == recurrent
    finally:
        l2.close()
        conn.close()


def test_hybrid_recurrent_state_is_shared_cross_instance():
    """A hybrid model's recurrent-state block offloaded by instance A is reusable by
    instance B, exactly like an attention-KV page (pooled + shared by the engine)."""
    engine = _SharedFakeEngine()
    instance_a = _shared_connector(engine)
    instance_b = _shared_connector(engine)
    try:
        state_key = "tok:mamba-conv-ssm-state@layer7"
        recurrent = bytes(range(256)) * 3
        assert instance_a.put(state_key, recurrent) is True
        assert instance_b.get(state_key) == recurrent  # cross-instance recurrent reuse
    finally:
        instance_a.close()
        instance_b.close()
