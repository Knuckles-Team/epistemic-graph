"""Python LMCache/vLLM remote-backend driver for the EG-187 KV-cache endpoint.

CONCEPT:EG-KG.backend.shipped-pip-installable-python — a shipped, pip-installable driver that lets parallel vLLM /
LMCache workers use the engine's shared, content-addressed KV-cache backend
(CONCEPT:EG-KG.memory.byte-bounded-tiers/186) as a remote KV backend over its HTTP surface (CONCEPT:EG-KG.backend.is-configured-so-co).

Two entry points:

* :class:`RemoteKVConnector` — the plain remote-backend client implementing the
  LMCache ``get`` / ``put`` / ``exists`` / ``contains`` / ``stats`` shape over
  ``GET|PUT|HEAD /kv/<hash>``, ``GET /kv/<hash>/exists`` and ``GET /kv/stats``.
* :class:`RemoteKVL2Connector` — the LMCache ``native_plugin`` L2-adapter native
  client (``event_fd`` / ``submit_batch_*`` / ``drain_completions``) wrapping the
  above; load it via
  ``{"type": "native_plugin", "module_path": "epistemic_graph.kvcache",
  "class_name": "RemoteKVL2Connector", "adapter_params": {"base_url": "..."}}``.

The base package import needs **only stdlib** (``urllib``); ``httpx`` is an
optional acceleration pulled by ``pip install epistemic-graph[lmcache]``.
"""

from __future__ import annotations

from .config import DEFAULT_KVCACHE_ADDR, KvCacheConfig
from .connector import (
    HTTPX_AVAILABLE,
    HttpxTransport,
    KvCacheStats,
    RemoteKVConnector,
    UrllibTransport,
)
from .l2_connector import RemoteKVL2Connector

__all__ = [
    "DEFAULT_KVCACHE_ADDR",
    "HTTPX_AVAILABLE",
    "HttpxTransport",
    "KvCacheConfig",
    "KvCacheStats",
    "RemoteKVConnector",
    "RemoteKVL2Connector",
    "UrllibTransport",
]
