"""LMCache ``native_plugin`` L2-adapter connector for the EG-187 KV surface.

CONCEPT:EG-KG.backend.shipped-pip-installable-python — the LMCache-side *native connector* half of the driver. It
complements :class:`~epistemic_graph.kvcache.connector.RemoteKVConnector` (the
``get`` / ``put`` / ``exists`` / ``stats`` HTTP client) by exposing it through
the shape LMCache's **``native_plugin`` L2 adapter** loads.

Why this exists (vs the ``resp`` adapter)
-----------------------------------------
LMCache's decoupled ``lmcache server`` writes its L2 tier through an
``--l2-adapter`` plugin. The zero-code path points the built-in ``resp`` adapter
at the engine's Redis RESP wire — but that lands blocks in the engine's *generic
Redis keyspace*, so the engine's **content-addressed dedup (CONCEPT:EG-KG.enrichment.content-address-separation)** and
the **``/kv/stats`` counters (CONCEPT:EG-KG.backend.is-configured-so-co)** do NOT apply. This connector
instead speaks the **EG-187 HTTP KV surface**, so every L2 write is
content-addressed and deduped and the stats counters move.

The LMCache ``native_plugin`` contract
--------------------------------------
LMCache's ``NativeConnectorL2Adapter``
(``lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter``) owns the
hard machinery — the three L2 event fds, the task-id/completion demux thread,
client-side locking and byte accounting. It drives a small **native client** that
must expose exactly::

    event_fd() -> int
    submit_batch_set(keys, memviews) -> int      # future_id
    submit_batch_get(keys, memviews) -> int      # future_id
    submit_batch_exists(keys) -> int             # future_id
    drain_completions() -> list[tuple[int, bool, str, list[bool] | None]]
    close() -> None

:class:`RemoteKVL2Connector` is that native client, in pure Python (no ``lmcache``
import — so it stays unit-testable) over the EG-187 HTTP surface. It is designed
for pybind C++ connectors, so it is **asynchronous**: ``submit_*`` enqueues the
batch onto a thread pool and returns a ``future_id`` immediately; a background
worker performs the HTTP I/O and signals a Linux ``eventfd``; the wrapper polls
that fd and calls :meth:`drain_completions` to reap results.

It is loaded via ``--l2-adapter``::

    {"type": "native_plugin",
     "module_path": "epistemic_graph.kvcache",
     "class_name": "RemoteKVL2Connector",
     "adapter_params": {"base_url": "http://localhost:9130"}}

``adapter_params`` are forwarded verbatim as keyword arguments to the constructor
(all optional — with none supplied the connector reads the engine's EG-187
environment via :meth:`KvCacheConfig.from_env`). There is deliberately **no**
``submit_batch_delete``: the shared, content-addressed pool is evicted by the
engine's own tiered store (CONCEPT:EG-KG.memory.byte-bounded-tiers), so L2 delete is a no-op.

Graceful degradation is inherited from :class:`RemoteKVConnector`: every
transport/protocol error maps to a cache miss, so an unreachable engine never
crashes token generation — LMCache just recomputes.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from itertools import count
from typing import TYPE_CHECKING

from .config import KvCacheConfig, _addr_to_base_url
from .connector import RemoteKVConnector

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# A completion record is the tuple the LMCache wrapper's demux thread expects:
# ``(future_id, ok, error, result_bools)`` — ``result_bools`` is a per-key
# hit/success list for get/exists, or ``None`` for a store.


class RemoteKVL2Connector:
    """Native-client bridge from LMCache's ``native_plugin`` L2 adapter → EG-187.

    CONCEPT:EG-KG.backend.shipped-pip-installable-python. Instantiated by LMCache's ``native_plugin`` factory with the
    ``adapter_params`` dict spread as keyword arguments, then wrapped in
    ``NativeConnectorL2Adapter``. All parameters are optional; anything not
    supplied is sourced from the engine's EG-187 environment
    (:meth:`KvCacheConfig.from_env`).

    Requires Linux ``os.eventfd`` (Python 3.10+ on Linux) — the same POSIX-Linux
    target the engine and vLLM run on. The module imports cleanly everywhere;
    ``eventfd`` is only touched at construction.

    Args:
        base_url: Explicit engine KV base URL, e.g. ``http://localhost:9130``.
            Wins over ``addr``.
        addr: Engine bind value (``host:port`` / bare port / enable token),
            coerced to a base URL like ``EPISTEMIC_GRAPH_KVCACHE_ADDR``.
        token: Bearer token for the EG-187 surface.
        timeout_s: Per-request timeout (hot-path — keep short).
        num_workers: Background I/O worker threads (parallel batches). Also raises
            the pooled-connection ceiling to at least ``2 × num_workers``.
        max_connections: Explicit pooled-connection ceiling.
        verify_tls: TLS verification toggle.
        connector: Pre-built :class:`RemoteKVConnector` (dependency injection for
            tests); wins over all endpoint kwargs.
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        addr: str | None = None,
        token: str | None = None,
        timeout_s: float | None = None,
        num_workers: int = 8,
        max_connections: int | None = None,
        verify_tls: bool | None = None,
        connector: RemoteKVConnector | None = None,
    ) -> None:
        if not hasattr(os, "eventfd"):  # pragma: no cover - non-Linux guard
            raise RuntimeError(
                "RemoteKVL2Connector requires Linux os.eventfd (POSIX-Linux target)"
            )
        workers = max(int(num_workers), 1)
        if connector is not None:
            self._backend = connector
            self._owns_backend = False
        else:
            cfg = self._resolve_config(
                base_url=base_url,
                addr=addr,
                token=token,
                timeout_s=timeout_s,
                max_connections=max_connections,
                verify_tls=verify_tls,
                workers=workers,
            )
            self._backend = RemoteKVConnector(cfg)
            self._owns_backend = True

        self._efd: int = os.eventfd(0, self._eventfd_flags())
        self._completions: list[tuple[int, bool, str, list[bool] | None]] = []
        self._lock = threading.Lock()
        self._next_id = count(1)
        self._id_lock = threading.Lock()
        self._closed = False
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="eg-l2")
        logger.info(
            "RemoteKVL2Connector ready (base_url=%s, workers=%d)",
            self._backend.config.base_url,
            workers,
        )

    # -- construction helpers -------------------------------------------------
    @staticmethod
    def _eventfd_flags() -> int:
        """Non-blocking (so drain never stalls) + close-on-exec where available."""
        flags = os.EFD_NONBLOCK
        cloexec = getattr(os, "EFD_CLOEXEC", 0)
        return flags | cloexec

    @staticmethod
    def _resolve_config(
        *,
        base_url: str | None,
        addr: str | None,
        token: str | None,
        timeout_s: float | None,
        max_connections: int | None,
        verify_tls: bool | None,
        workers: int,
    ) -> KvCacheConfig:
        """Layer explicit ``adapter_params`` over the EG-187 environment defaults."""
        cfg = KvCacheConfig.from_env()
        updates: dict[str, object] = {}
        if base_url:
            updates["base_url"] = base_url.rstrip("/")
        elif addr:
            updates["base_url"] = _addr_to_base_url(addr)
        if token is not None:
            updates["token"] = token
        if timeout_s is not None:
            updates["timeout_s"] = float(timeout_s)
        if verify_tls is not None:
            updates["verify_tls"] = bool(verify_tls)
        # Give the transport at least 2 conns per worker unless pinned.
        if max_connections is not None:
            updates["max_connections"] = int(max_connections)
        else:
            updates["max_connections"] = max(workers * 2, cfg.max_connections)
        return replace(cfg, **updates)

    # -- native-client contract (CONCEPT:EG-KG.backend.shipped-pip-installable-python) ------------------------------
    def event_fd(self) -> int:
        """The pollable fd signalled on every batch completion (one for all ops)."""
        return self._efd

    def submit_batch_set(
        self,
        keys: Sequence[str],
        memviews: Sequence[memoryview],
    ) -> int:
        """Queue a batch offload (``PUT /kv/<hash>``); returns its ``future_id``.

        The KV bytes are **snapshotted synchronously** here — the caller may reuse
        or free the underlying buffers the moment ``submit`` returns.
        """
        payloads = [bytes(mv) for mv in memviews]
        return self._submit(self._do_set, list(keys), payloads)

    def submit_batch_get(
        self,
        keys: Sequence[str],
        memviews: Sequence[memoryview],
    ) -> int:
        """Queue a batch fetch (``GET /kv/<hash>``); returns its ``future_id``.

        The caller-provided ``memviews`` are the load buffers; the worker writes
        hit bytes into them and reports a per-key hit bitmap. The caller keeps the
        buffers valid until it reaps the completion.
        """
        return self._submit(self._do_get, list(keys), list(memviews))

    def submit_batch_exists(self, keys: Sequence[str]) -> int:
        """Queue a batch existence probe (``HEAD /kv/<hash>``); returns ``future_id``."""
        return self._submit(self._do_exists, list(keys))

    def drain_completions(self) -> list[tuple[int, bool, str, list[bool] | None]]:
        """Reap and clear all finished batches (called after the eventfd fires).

        Resets the eventfd counter first, then swaps out the queue — so a
        completion enqueued concurrently is never lost (its own eventfd write
        re-arms the poll for the next drain).
        """
        try:
            os.eventfd_read(self._efd)
        except (BlockingIOError, OSError):
            # Counter already 0 (spurious/late wake) — nothing to reset.
            pass
        with self._lock:
            done = self._completions
            self._completions = []
        return done

    def close(self) -> None:
        """Drain in-flight work, close the transport and the eventfd."""
        if self._closed:
            return
        self._closed = True
        self._pool.shutdown(wait=True)
        if self._owns_backend:
            self._backend.close()
        try:
            os.close(self._efd)
        except OSError:  # pragma: no cover - already closed
            pass

    # -- internals ------------------------------------------------------------
    def _next_future_id(self) -> int:
        with self._id_lock:
            return next(self._next_id)

    def _submit(
        self, work: Callable[..., tuple[bool, list[bool] | None]], *args: object
    ) -> int:
        """Assign a future id, dispatch the batch to the pool, return the id.

        A submit after :meth:`close` (or a pool rejection) completes the batch
        immediately as a failure so the wrapper never blocks waiting on it.
        """
        future_id = self._next_future_id()
        try:
            self._pool.submit(self._run, future_id, work, *args)
        except RuntimeError:
            self._complete(
                future_id, ok=False, error="connector closed", result_bools=None
            )
        return future_id

    def _run(
        self,
        future_id: int,
        work: Callable[..., tuple[bool, list[bool] | None]],
        *args: object,
    ) -> None:
        """Pool entry: run the op, translate any error into a failed completion."""
        try:
            ok, result_bools = work(*args)
            self._complete(future_id, ok=ok, error="", result_bools=result_bools)
        except Exception as exc:  # noqa: BLE001 - never crash the worker thread
            logger.warning("L2 batch %d failed: %s", future_id, exc)
            self._complete(future_id, ok=False, error=str(exc), result_bools=None)

    def _complete(
        self,
        future_id: int,
        *,
        ok: bool,
        error: str,
        result_bools: list[bool] | None,
    ) -> None:
        """Enqueue a completion and signal the eventfd (append THEN write)."""
        with self._lock:
            self._completions.append((future_id, ok, error, result_bools))
        os.eventfd_write(self._efd, 1)

    # -- op bodies (run on the pool) ------------------------------------------
    def _do_set(self, keys: list[str], payloads: list[bytes]) -> tuple[bool, None]:
        """Offload every block; success iff all PUTs were accepted."""
        ok = True
        for key, payload in zip(keys, payloads, strict=True):
            if not self._backend.put(key, payload):
                ok = False
        return ok, None

    def _do_get(
        self, keys: list[str], memviews: list[memoryview]
    ) -> tuple[bool, list[bool]]:
        """Fetch every block into its buffer; per-key hit bitmap in the result."""
        hits: list[bool] = []
        for key, mv in zip(keys, memviews, strict=True):
            hits.append(self._fill(mv, self._backend.get(key), key))
        return True, hits

    def _do_exists(self, keys: list[str]) -> tuple[bool, list[bool]]:
        """Probe existence for every key (used for lookup-and-lock)."""
        return True, [self._backend.contains(key) for key in keys]

    @staticmethod
    def _fill(mv: memoryview, data: bytes | None, key: str) -> bool:
        """Copy a fetched block into its load buffer; ``True`` on a clean hit.

        A miss (``None``) or a size mismatch is reported as a miss so a
        partial/oversized write can never corrupt the KV buffer — LMCache then
        recomputes the block.
        """
        if data is None:
            return False
        if len(data) != len(mv):
            logger.warning(
                "L2 get size mismatch for %s: got %d bytes into a %d-byte buffer; "
                "treating as miss",
                key,
                len(data),
                len(mv),
            )
            return False
        mv[:] = data
        return True
