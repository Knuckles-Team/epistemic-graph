"""Configuration for the epistemic-graph remote KV-cache driver.

CONCEPT:EG-337 — the shipped, pip-installable Python LMCache/vLLM remote-backend
driver for the engine's EG-187 KV-cache endpoint.

The driver reads its endpoint and bearer token from the SAME environment the
engine's HTTP KV-cache listener (CONCEPT:EG-187) is configured with, so a
co-located deploy shares one source of truth. This module has NO third-party
dependency (stdlib only) so ``import epistemic_graph.kvcache`` works without the
optional ``lmcache`` extra installed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

#: Engine default bind for the KV-cache HTTP listener (EG-187). A driver on the
#: same host points here unless overridden.
DEFAULT_KVCACHE_ADDR = "127.0.0.1:9130"

_TRUE_TOKENS = {"1", "true", "yes", "on", "enable", "enabled"}


def _addr_to_base_url(addr: str) -> str:
    """Coerce an engine ``EPISTEMIC_GRAPH_KVCACHE_ADDR`` value into a client URL.

    The engine accepts a bare enable token (bind localhost default), a bare port,
    or a full ``host:port``. On the client side we only ever need a reachable
    ``http://host:port`` base URL, so:

    * an already-qualified ``http://`` / ``https://`` value passes through;
    * a bare truthy enable token (``"1"``/``"on"``) ⇒ the localhost default;
    * a bare integer port ``9130`` becomes ``http://127.0.0.1:9130``;
    * a ``host:port`` becomes ``http://host:port``;
    * anything else falls back to the localhost default.
    """
    value = (addr or "").strip()
    if not value:
        return f"http://{DEFAULT_KVCACHE_ADDR}"
    if value.startswith(("http://", "https://")):
        return value.rstrip("/")
    if value.lower() in _TRUE_TOKENS:
        return f"http://{DEFAULT_KVCACHE_ADDR}"
    if value.isdigit():
        return f"http://127.0.0.1:{value}"
    if ":" in value:
        return f"http://{value}"
    return f"http://{DEFAULT_KVCACHE_ADDR}"


def _bool_env(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in _TRUE_TOKENS


@dataclass(slots=True)
class KvCacheConfig:
    """Endpoint + auth + timeout settings for :class:`RemoteKVConnector`.

    CONCEPT:EG-337. Prefer :meth:`from_env`, which mirrors the engine's EG-187
    environment variables so client and server stay in lockstep.

    Attributes:
        base_url: Base URL of the engine KV-cache HTTP surface, e.g.
            ``http://127.0.0.1:9130``. Endpoints hang off ``/kv/...``.
        token: Bearer token sent as ``Authorization: Bearer <token>``. ``None``
            ⇒ anonymous (engine loopback default).
        timeout_s: Per-request timeout (seconds). Deliberately short: this sits on
            the inference hot path, so a slow engine degrades to a local miss
            quickly rather than stalling token generation.
        max_connections: Upper bound on pooled keep-alive connections (used by the
            optional httpx transport; the stdlib transport is per-request).
        verify_tls: TLS verification. Only disable for an explicit, justified
            insecure endpoint (plain-http loopback is unaffected).
    """

    base_url: str = f"http://{DEFAULT_KVCACHE_ADDR}"
    token: str | None = None
    timeout_s: float = 2.0
    max_connections: int = 32
    verify_tls: bool = True

    def __post_init__(self) -> None:
        self.base_url = (self.base_url or f"http://{DEFAULT_KVCACHE_ADDR}").rstrip("/")
        if self.timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")
        if self.max_connections <= 0:
            raise ValueError("max_connections must be > 0")

    @classmethod
    def from_env(cls) -> KvCacheConfig:
        """Build config from the engine's EG-187 environment variables.

        CONCEPT:EG-337. Recognized variables:

        * ``EPISTEMIC_GRAPH_KVCACHE_URL`` — explicit client base URL (wins).
        * ``EPISTEMIC_GRAPH_KVCACHE_ADDR`` — the engine bind value, coerced to a
          base URL via :func:`_addr_to_base_url`.
        * ``EPISTEMIC_GRAPH_KVCACHE_TOKEN`` — bearer token.
        * ``EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S`` — per-request timeout override.
        * ``EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS`` — pool ceiling override.
        * ``EPISTEMIC_GRAPH_KVCACHE_TLS_VERIFY`` — TLS verification toggle.
        """
        explicit_url = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_URL", "").strip()
        if explicit_url:
            base_url = explicit_url.rstrip("/")
        else:
            addr = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_ADDR", DEFAULT_KVCACHE_ADDR)
            base_url = _addr_to_base_url(addr)

        token = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_TOKEN") or None

        timeout_raw = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S")
        timeout_s = float(timeout_raw) if timeout_raw else 2.0

        conns_raw = os.environ.get("EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS")
        max_connections = int(conns_raw) if conns_raw else 32

        verify_tls = _bool_env(
            os.environ.get("EPISTEMIC_GRAPH_KVCACHE_TLS_VERIFY"), True
        )

        return cls(
            base_url=base_url,
            token=token,
            timeout_s=timeout_s,
            max_connections=max_connections,
            verify_tls=verify_tls,
        )
