"""Unit tests for the EG-187 Python KV-cache driver (CONCEPT:EG-KG.backend.shipped-pip-installable-python).

Runs a tiny in-process HTTP server implementing the EG-187 ``/kv`` surface
(``GET|PUT|HEAD /kv/<hash>``, ``GET /kv/<hash>/exists``, ``GET /kv/stats``) with
optional bearer-token enforcement, and exercises the driver end-to-end over a
real socket — no mocking of the HTTP layer, no third-party test deps.

Run standalone (bypass the slow engine-build conftest fixture)::

    python3 -m pytest tests/test_kvcache_connector.py --noconftest -q
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

import pytest

from epistemic_graph.kvcache import (
    KvCacheConfig,
    KvCacheStats,
    RemoteKVConnector,
    RemoteKVL2Connector,
)


class _KvState:
    """The server-side content-addressed store + stat counters."""

    def __init__(self) -> None:
        self.blocks: dict[str, bytes] = {}
        self.get_hits = 0
        self.get_misses = 0
        self.dedup_hits = 0
        self.require_token: str | None = None


def _make_handler(state: _KvState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence test noise
            pass

        # -- auth ---------------------------------------------------------
        def _authed(self) -> bool:
            if state.require_token is None:
                return True
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


def _connector(base_url: str, token: str | None = None) -> RemoteKVConnector:
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
    # Without the token → 401 → graceful miss.
    with _connector(base_url) as anon:
        assert anon.put("k", b"v") is False
        assert anon.get("k") is None
        assert anon.contains("k") is False
    # With the right token → success.
    with _connector(base_url, token="s3cr3t") as authed:
        assert authed.put("k", b"v") is True
        assert authed.get("k") == b"v"


def test_graceful_degradation_when_engine_down():
    # Nothing listening → every op degrades to a miss, never raises.
    conn = RemoteKVConnector(
        KvCacheConfig(base_url="http://127.0.0.1:1", timeout_s=0.25)
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
    assert cfg.base_url == "http://10.0.0.5:9130"
    assert cfg.token == "tok"


def test_config_from_env_bare_port(monkeypatch):
    monkeypatch.delenv("EPISTEMIC_GRAPH_KVCACHE_URL", raising=False)
    monkeypatch.setenv("EPISTEMIC_GRAPH_KVCACHE_ADDR", "9200")
    assert KvCacheConfig.from_env().base_url == "http://127.0.0.1:9200"


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
    l2 = RemoteKVL2Connector(base_url=base_url, num_workers=2, timeout_s=5.0)
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
    l2 = RemoteKVL2Connector(base_url=base_url, num_workers=1, timeout_s=5.0)
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
