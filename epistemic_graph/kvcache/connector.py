"""LMCache / vLLM remote-backend driver for the epistemic-graph KV-cache.

CONCEPT:EG-KG.backend.shipped-pip-installable-python — the shipped, pip-installable Python half of the EG-187 remote
KV-cache contract.

The engine exposes a shared, content-addressed KV-cache backend
(``eg_kvcache::SharedKvIndex``, CONCEPT:EG-KG.enrichment.content-address-separation) over a small HTTP surface
(CONCEPT:EG-KG.backend.is-configured-so-co). This module drives that surface from Python so that parallel
vLLM / LMCache workers pool their KV-cache blocks by token-hash: an identical KV
page produced by two workers is stored **once** (dedup) and a cold worker can
fetch a page a warm worker already computed.

Wire contract (see ``docs/architecture/kvcache-remote-backend.md``):

===========================  ================================================
``GET  /kv/<hash>``          block bytes (``200``) or ``404`` if absent
``PUT  /kv/<hash>``          store binary body; ``201`` new / ``200`` dedup hit
``HEAD /kv/<hash>``          ``200`` present / ``404`` absent (existence probe)
``GET  /kv/<hash>/exists``   ``200`` JSON ``{"hash":…,"exists":bool}``
``GET  /kv/stats``           ``200`` JSON occupancy + dedup stats
===========================  ================================================

``<hash>`` is the **caller's opaque token-hash key** (what LMCache computes over
the token ids of a block); the engine stores the body verbatim under it and does
NOT re-hash. Auth is an optional ``Authorization: Bearer <token>`` guard.

Graceful degradation is a hard requirement: this sits on the inference hot path,
so every network / protocol error is swallowed and mapped to a cache **miss**
(``get`` → ``None``, ``exists`` / ``contains`` → ``False``, ``put`` → ``False``,
``stats`` → zeros). The driver must never crash token generation.

Dependencies: the default transport is **stdlib** (``urllib.request``), so
``import epistemic_graph.kvcache`` works with only the base package installed.
When the optional ``lmcache`` extra (``pip install epistemic-graph[lmcache]``)
is present, ``httpx`` is used for pooled keep-alive connections instead.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import quote

from .config import KvCacheConfig

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

# Optional httpx acceleration — pulled by the ``lmcache`` extra. Absent ⇒ the
# stdlib urllib transport is used, so the base import never requires httpx.
try:  # pragma: no cover - exercised only where httpx is installed
    import httpx as _httpx
except Exception:  # noqa: BLE001 - any import failure ⇒ stdlib transport
    _httpx = None  # type: ignore[assignment]

HTTPX_AVAILABLE = _httpx is not None

# Transport-layer failures the connector swallows into a cache miss. urllib
# raises URLError/OSError; httpx (optional) raises httpx.HTTPError subclasses.
_TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (OSError, ValueError)
if HTTPX_AVAILABLE:  # pragma: no cover - only where httpx installed
    _TRANSPORT_ERRORS = (*_TRANSPORT_ERRORS, _httpx.HTTPError)


@dataclass(slots=True)
class KvCacheStats:
    """Parsed ``GET /kv/stats`` response (CONCEPT:EG-KG.backend.shipped-pip-installable-python).

    Fields mirror the engine's occupancy + dedup counters. Unknown extra keys
    are ignored so a newer engine can add counters without breaking the driver.
    """

    unique_blocks: int = 0
    total_refs: int = 0
    resident_bytes: int = 0
    logical_bytes: int = 0
    dedup_savings_bytes: int = 0
    dedup_hits: int = 0
    get_hits: int = 0
    get_misses: int = 0

    @classmethod
    def from_json(cls, body: Mapping[str, Any]) -> KvCacheStats:
        fields = {f for f in cls.__dataclass_fields__}  # noqa: C416
        return cls(**{k: int(v) for k, v in body.items() if k in fields})


@dataclass(slots=True)
class _Response:
    """A minimal transport-agnostic HTTP response."""

    status: int
    body: bytes = b""


class _Transport(Protocol):
    """The pluggable HTTP transport the connector drives.

    ``request`` must NEVER raise for an HTTP status (even 4xx/5xx) — it returns a
    :class:`_Response`. It may raise :class:`OSError` / transport errors, which
    the connector maps to a cache miss.
    """

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> _Response: ...

    def close(self) -> None: ...


class UrllibTransport:
    """Zero-dependency :class:`_Transport` over ``urllib.request`` (stdlib).

    Per-request (no pooling), but dependency-free — the default so the base
    package import needs nothing beyond stdlib.
    """

    def __init__(self, *, timeout_s: float, verify_tls: bool = True) -> None:
        self._timeout = timeout_s
        self._verify_tls = verify_tls
        self._opener = urllib.request.build_opener()

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> _Response:
        req = urllib.request.Request(url, data=body, method=method)
        for name, value in (headers or {}).items():
            req.add_header(name, value)
        kwargs: dict[str, Any] = {"timeout": self._timeout}
        if url.startswith("https://") and not self._verify_tls:
            import ssl

            # Explicit, narrow opt-out: only reached when the caller passed
            # verify_tls=False (default True) for an https:// endpoint — e.g. a
            # self-signed dev/test KV-cache HTTP surface. Never the default path.
            kwargs["context"] = ssl._create_unverified_context()  # nosec B323
        try:
            with self._opener.open(req, **kwargs) as resp:
                return _Response(status=resp.status, body=resp.read())
        except urllib.error.HTTPError as exc:
            # An HTTP status (404/401/…) is a normal outcome, not a transport
            # failure: read the body and return it so callers branch on status.
            return _Response(status=exc.code, body=exc.read())

    def close(self) -> None:  # pragma: no cover - trivial
        self._opener.close()


class HttpxTransport:
    """Pooled keep-alive :class:`_Transport` over ``httpx`` (optional extra)."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_s: float,
        verify_tls: bool,
        max_connections: int,
        client: Any | None = None,
    ) -> None:
        if _httpx is None:  # pragma: no cover - guarded by callers
            raise RuntimeError(
                "httpx is not installed (pip install epistemic-graph[lmcache])"
            )
        self._owns_client = client is None
        self._client = client or _httpx.Client(
            base_url=base_url,
            timeout=timeout_s,
            verify=verify_tls,
            limits=_httpx.Limits(max_connections=max_connections),
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        body: bytes | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> _Response:
        resp = self._client.request(
            method, url, content=body, headers=dict(headers or {})
        )
        return _Response(status=resp.status_code, body=resp.content)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class RemoteKVConnector:
    """LMCache/vLLM remote backend over the engine's ``/kv`` HTTP surface.

    CONCEPT:EG-KG.backend.shipped-pip-installable-python. Implements the LMCache remote-backend shape
    (``get`` / ``put`` / ``exists`` / ``contains`` / ``stats``) against EG-187
    with an optional bearer token, a short per-request timeout, and total
    graceful degradation — every error is a cache miss, never a raised exception
    on the inference path.

    Args:
        config: Endpoint / auth / timeout settings. Defaults to
            :class:`KvCacheConfig` defaults; use :meth:`from_env` to source from
            the engine's EG-187 environment.
        transport: Optional pre-built :class:`_Transport` (dependency injection —
            unit tests / callers can supply their own). When omitted an
            :class:`HttpxTransport` is used if ``httpx`` is installed, else the
            stdlib :class:`UrllibTransport`.
    """

    def __init__(
        self,
        config: KvCacheConfig | None = None,
        *,
        transport: _Transport | None = None,
    ) -> None:
        self.config = config or KvCacheConfig()
        self._owns_transport = transport is None
        self._transport = transport or self._build_transport()

    # -- construction ---------------------------------------------------------
    @classmethod
    def from_env(cls) -> RemoteKVConnector:
        """Build a driver from the engine's EG-187 environment (EG-337)."""
        return cls(KvCacheConfig.from_env())

    def _build_transport(self) -> _Transport:
        if HTTPX_AVAILABLE:
            headers = self._auth_headers()
            client = _httpx.Client(  # type: ignore[union-attr]
                base_url=self.config.base_url,
                timeout=self.config.timeout_s,
                verify=self.config.verify_tls,
                headers=headers,
                limits=_httpx.Limits(max_connections=self.config.max_connections),  # type: ignore[union-attr]
            )
            return HttpxTransport(
                base_url=self.config.base_url,
                timeout_s=self.config.timeout_s,
                verify_tls=self.config.verify_tls,
                max_connections=self.config.max_connections,
                client=client,
            )
        return UrllibTransport(
            timeout_s=self.config.timeout_s, verify_tls=self.config.verify_tls
        )

    def _auth_headers(self) -> dict[str, str]:
        if self.config.token:
            return {"Authorization": f"Bearer {self.config.token}"}
        return {}

    # -- key / url handling ---------------------------------------------------
    @staticmethod
    def _key_path(key: str) -> str:
        """Path for a block key. The key is opaque; percent-encode for URL safety.

        The engine stores the body verbatim under the decoded key, so encoding
        here is purely transport hygiene (keys may contain ``/`` or other
        reserved characters).
        """
        return f"/kv/{quote(str(key), safe='')}"

    def _url(self, path: str) -> str:
        return f"{self.config.base_url}{path}"

    def _request(
        self, method: str, path: str, *, body: bytes | None = None
    ) -> _Response | None:
        """Issue a request, returning ``None`` on any transport failure."""
        headers = dict(self._auth_headers())
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        try:
            return self._transport.request(
                method, self._url(path), body=body, headers=headers
            )
        except _TRANSPORT_ERRORS as exc:
            logger.warning(
                "kvcache %s %s failed, treating as miss: %s", method, path, exc
            )
            return None

    # -- LMCache remote-backend contract (CONCEPT:EG-KG.backend.shipped-pip-installable-python) --------------------
    def get(self, key: str) -> bytes | None:
        """Fetch KV-block bytes for ``key`` (LMCache load).

        Returns the block bytes on a ``200`` hit, ``None`` on a ``404`` miss, and
        ``None`` (a miss) on any transport/protocol error — never raises.
        """
        resp = self._request("GET", self._key_path(key))
        if resp is None:
            return None
        if resp.status == 200:
            return resp.body
        if resp.status != 404:
            logger.warning(
                "kvcache get(%s) unexpected status %s, treating as miss",
                key,
                resp.status,
            )
        return None

    def put(self, key: str, value: bytes) -> bool:
        """Store KV-block ``value`` under ``key`` (LMCache offload).

        Returns ``True`` when the engine accepted the block (``200`` dedup hit or
        ``201`` newly created) and ``False`` on any error — a failed offload is
        non-fatal (the block simply is not pooled).
        """
        resp = self._request("PUT", self._key_path(key), body=value)
        if resp is None:
            return False
        if resp.status in (200, 201):
            return True
        logger.warning("kvcache put(%s) rejected with status %s", key, resp.status)
        return False

    def contains(self, key: str) -> bool:
        """Existence probe for ``key`` via ``HEAD /kv/<hash>`` (EG-337).

        ``True`` on ``200``, ``False`` on ``404`` or any error. Used to skip an
        upload the cluster already has.
        """
        resp = self._request("HEAD", self._key_path(key))
        return resp is not None and resp.status == 200

    def exists(self, key: str) -> bool:
        """Method-agnostic existence probe via ``GET /kv/<hash>/exists`` (EG-337).

        Some LMCache call sites cannot issue ``HEAD``; this uses the JSON
        ``{"hash":…,"exists":bool}`` endpoint instead. Errors ⇒ ``False``.
        """
        resp = self._request("GET", f"{self._key_path(key)}/exists")
        if resp is None or resp.status != 200:
            return False
        try:
            body = json.loads(resp.body.decode("utf-8"))
        except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
            return False
        return bool(body.get("exists", False))

    def stats(self) -> KvCacheStats:
        """Fetch occupancy + dedup stats via ``GET /kv/stats`` (EG-337).

        Returns a parsed :class:`KvCacheStats`; on any error returns an all-zero
        instance rather than raising.
        """
        resp = self._request("GET", "/kv/stats")
        if resp is None or resp.status != 200:
            return KvCacheStats()
        try:
            return KvCacheStats.from_json(json.loads(resp.body.decode("utf-8")))
        except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as exc:
            logger.warning("kvcache stats() failed: %s", exc)
            return KvCacheStats()

    # -- lifecycle ------------------------------------------------------------
    def close(self) -> None:
        """Close the transport if this connector owns it (EG-337)."""
        if self._owns_transport:
            self._transport.close()

    def __enter__(self) -> RemoteKVConnector:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        self.close()
