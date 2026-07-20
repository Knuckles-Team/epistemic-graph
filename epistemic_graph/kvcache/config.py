"""Configuration for the epistemic-graph remote KV-cache driver.

CONCEPT:EG-KG.backend.shipped-pip-installable-python — the shipped, pip-installable Python LMCache/vLLM remote-backend
driver for the engine's EG-187 KV-cache endpoint.

The driver reads its endpoint and bearer token from the SAME environment the
engine's HTTP KV-cache listener (CONCEPT:EG-KG.backend.is-configured-so-co) is configured with, so a
co-located deploy shares one source of truth. This module has NO third-party
dependency (stdlib only) so ``import epistemic_graph.kvcache`` works without the
optional ``lmcache`` extra installed.
"""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from ipaddress import ip_address
from urllib.parse import urlsplit

#: Engine default bind for the KV-cache HTTP listener (EG-187). A driver on the
#: same host points here unless overridden.
DEFAULT_KVCACHE_ADDR = "127.0.0.1:9130"

_TRUE_TOKENS = {"1", "true", "yes", "on", "enable", "enabled"}


def _is_loopback_host(host: str) -> bool:
    """Return whether ``host`` is an explicit local-only destination."""
    normalized = host.rstrip(".").lower()
    if normalized == "localhost":
        return True
    try:
        return ip_address(normalized).is_loopback
    except ValueError:
        return False


def _validate_base_url(base_url: str) -> str:
    """Validate the current transport contract and return a normalized URL."""
    value = base_url.strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("KV-cache base_url must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password:
        raise ValueError("KV-cache credentials must not be embedded in base_url")
    if parsed.query or parsed.fragment:
        raise ValueError("KV-cache base_url must not contain a query or fragment")
    if parsed.scheme != "https" and not _is_loopback_host(parsed.hostname):
        raise ValueError("non-loopback KV-cache endpoints require HTTPS")
    return value


def _addr_to_base_url(addr: str) -> str:
    """Coerce an engine ``EPISTEMIC_GRAPH_KVCACHE_ADDR`` value into a client URL.

    The engine accepts a bare enable token (bind localhost default), a bare port,
    or a full ``host:port``. On the client side we only ever need a reachable
    base URL, so:

    * an already-qualified ``http://`` / ``https://`` value passes through;
    * a bare truthy enable token (``"1"``/``"on"``) ⇒ the localhost default;
    * a bare integer port ``9130`` becomes ``http://127.0.0.1:9130``;
    * a loopback ``host:port`` uses HTTP;
    * a non-loopback ``host:port`` uses HTTPS;
    * malformed addresses are rejected.
    """
    value = (addr or "").strip()
    if not value:
        return f"http://{DEFAULT_KVCACHE_ADDR}"
    if value.startswith(("http://", "https://")):
        return _validate_base_url(value)
    if value.lower() in _TRUE_TOKENS:
        return f"http://{DEFAULT_KVCACHE_ADDR}"
    if value.isdigit():
        return f"http://127.0.0.1:{value}"
    parsed = urlsplit(f"//{value}")
    if not parsed.hostname or parsed.port is None or parsed.path:
        raise ValueError(
            "KV-cache address must be host:port, a bare port, or an HTTP(S) URL"
        )
    scheme = "http" if _is_loopback_host(parsed.hostname) else "https"
    return _validate_base_url(f"{scheme}://{value}")


@dataclass(slots=True, kw_only=True)
class KvCacheConfig:
    """Endpoint + auth + timeout settings for :class:`RemoteKVConnector`.

    CONCEPT:EG-KG.backend.shipped-pip-installable-python. Prefer :meth:`from_env`, which mirrors the engine's EG-187
    environment variables so client and server stay in lockstep.

    Attributes:
        base_url: Base URL of the engine KV-cache HTTP surface, e.g.
            ``http://127.0.0.1:9130``. Endpoints hang off ``/kv/...``.
        token: Non-empty bearer credential sent as ``Authorization: Bearer
            <token>``. This may be a server-configured bearer secret or a JWT.
        timeout_s: Per-request timeout (seconds). Deliberately short: this sits on
            the inference hot path, so a slow engine degrades to a local miss
            quickly rather than stalling token generation.
        max_connections: Upper bound on pooled keep-alive connections (used by the
            optional httpx transport; the stdlib transport is per-request).
        ca_bundle: Optional runtime CA bundle resolved from standard
            ``SSL_CERT_FILE`` or ``REQUESTS_CA_BUNDLE`` configuration.
        ca_directory: Optional hashed CA directory (``SSL_CERT_DIR``).
        client_cert/client_key: Optional mTLS identity pair.
    """

    base_url: str = f"http://{DEFAULT_KVCACHE_ADDR}"
    token: str
    timeout_s: float = 2.0
    max_connections: int = 32
    ca_bundle: str | None = None
    ca_directory: str | None = None
    client_cert: str | None = None
    client_key: str | None = None
    client_key_password: str | None = None

    def __post_init__(self) -> None:
        self.base_url = _validate_base_url(
            self.base_url or f"http://{DEFAULT_KVCACHE_ADDR}"
        )
        self.token = self.token.strip()
        if not self.token:
            raise ValueError("KV-cache bearer/JWT token must be configured")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in self.token):
            raise ValueError("KV-cache bearer/JWT token contains control characters")
        if self.timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")
        if self.max_connections <= 0:
            raise ValueError("max_connections must be > 0")
        if bool(self.client_cert) != bool(self.client_key):
            raise ValueError(
                "mTLS client certificate and key must be configured together"
            )

    def ssl_context(self) -> ssl.SSLContext:
        """Build the one connector-wide TLS context from runtime configuration."""
        try:
            context = ssl.create_default_context(
                cafile=self.ca_bundle or None,
                capath=self.ca_directory or None,
            )
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            if self.client_cert and self.client_key:
                context.load_cert_chain(
                    certfile=self.client_cert,
                    keyfile=self.client_key,
                    password=self.client_key_password,
                )
        except (OSError, ssl.SSLError, ValueError):
            raise ValueError(
                "KV-cache TLS material is unavailable or invalid"
            ) from None
        return context

    @classmethod
    def from_env(
        cls,
        *,
        base_url: str | None = None,
        addr: str | None = None,
        token: str | None = None,
        timeout_s: float | None = None,
        max_connections: int | None = None,
    ) -> KvCacheConfig:
        """Build config from the engine's EG-187 environment variables.

        CONCEPT:EG-KG.backend.shipped-pip-installable-python. Recognized variables:

        * ``EPISTEMIC_GRAPH_KVCACHE_URL`` — explicit client base URL (wins).
        * ``EPISTEMIC_GRAPH_KVCACHE_ADDR`` — the engine bind value, coerced to a
          base URL via :func:`_addr_to_base_url`.
        * ``EPISTEMIC_GRAPH_KVCACHE_TOKEN`` — bearer token.
        * ``EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S`` — per-request timeout override.
        * ``EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS`` — pool ceiling override.
        * ``SSL_CERT_FILE`` or ``REQUESTS_CA_BUNDLE`` — PEM CA bundle.
        * ``SSL_CERT_DIR`` — hashed CA directory.
        * ``EPISTEMIC_GRAPH_KVCACHE_CLIENT_CERT`` / ``..._CLIENT_KEY`` — mTLS
          identity; optional ``..._CLIENT_KEY_PASSWORD`` supplies its password.
        """
        if base_url is not None:
            resolved_base_url = base_url.strip().rstrip("/")
        elif addr is not None:
            resolved_base_url = _addr_to_base_url(addr)
        else:
            explicit_url = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_URL", "").strip()
            resolved_base_url = (
                explicit_url.rstrip("/")
                if explicit_url
                else _addr_to_base_url(
                    os.environ.get("EPISTEMIC_GRAPH_KVCACHE_ADDR", DEFAULT_KVCACHE_ADDR)
                )
            )

        resolved_token = (
            token
            if token is not None
            else os.environ.get("EPISTEMIC_GRAPH_KVCACHE_TOKEN", "")
        )

        timeout_raw = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S")
        resolved_timeout_s = (
            float(timeout_s)
            if timeout_s is not None
            else float(timeout_raw)
            if timeout_raw
            else 2.0
        )

        conns_raw = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS")
        resolved_max_connections = (
            int(max_connections)
            if max_connections is not None
            else int(conns_raw)
            if conns_raw
            else 32
        )

        ca_bundle = (
            os.environ.get("SSL_CERT_FILE")
            or os.environ.get("REQUESTS_CA_BUNDLE")
            or None
        )
        ca_directory = os.environ.get("SSL_CERT_DIR") or None
        client_cert = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_CLIENT_CERT") or None
        client_key = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_CLIENT_KEY") or None
        client_key_password = (
            os.environ.get("EPISTEMIC_GRAPH_KVCACHE_CLIENT_KEY_PASSWORD") or None
        )

        return cls(
            base_url=resolved_base_url,
            token=resolved_token,
            timeout_s=resolved_timeout_s,
            max_connections=resolved_max_connections,
            ca_bundle=ca_bundle,
            ca_directory=ca_directory,
            client_cert=client_cert,
            client_key=client_key,
            client_key_password=client_key_password,
        )
