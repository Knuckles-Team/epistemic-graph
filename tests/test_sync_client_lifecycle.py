"""Lifecycle regressions for the synchronous client-owned asyncio loop."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from epistemic_graph.client import (
    EpistemicGraphClient,
    SyncCallDeadlineExceeded,
    SyncEpistemicGraphClient,
    sync_call_deadline,
)

pytestmark = pytest.mark.no_engine


class _AsyncClient(EpistemicGraphClient):
    def __init__(self) -> None:
        self.close_calls = 0

    def __getattr__(self, _name: str) -> object:
        return object()

    async def close(self) -> None:
        self.close_calls += 1


class _CapabilityAsyncClient(_AsyncClient):
    def __init__(self, advertised: set[str]) -> None:
        super().__init__()
        self.advertised = advertised
        self.probes: list[str] = []

    async def supports(self, operation: str) -> bool:
        self.probes.append(operation)
        return operation in self.advertised


@pytest.mark.spec("EG-CONTRACT-R021")
def test_sync_supports_matches_async_capability_probe() -> None:
    """The sync wrapper exposes the same fail-closed capability result."""
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(
        target=loop.run_forever, name="dcdx98-capability-loop"
    )
    loop_thread.start()
    async_client = _CapabilityAsyncClient({"ReserveWorkItemResources"})
    client = SyncEpistemicGraphClient(async_client, loop, loop_thread)
    try:
        assert client.supports("ReserveWorkItemResources") is True
        assert client.supports("UpdateResourceHost") is False
        assert async_client.probes == [
            "ReserveWorkItemResources",
            "UpdateResourceHost",
        ]
    finally:
        client.close()


@pytest.mark.spec("EG-CONTRACT-R021")
def test_sync_deadline_cancels_blocked_graph_future_without_asyncio_run_tail() -> None:
    """A probe deadline cancels its graph future and releases its worker promptly."""
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, name="dcdx98-graph-loop")
    loop_thread.start()
    cancelled = threading.Event()

    class _BlockingNamespace:
        async def read(self) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

    wrapper = SyncEpistemicGraphClient._SyncWrapper(_BlockingNamespace(), loop)
    baseline_workers = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("asyncio_")
    }

    async def probe() -> None:
        with sync_call_deadline(0.05):
            with pytest.raises(SyncCallDeadlineExceeded):
                await asyncio.to_thread(wrapper.read)

    started = time.monotonic()
    try:
        asyncio.run(probe())
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=1)
        loop.close()

    assert time.monotonic() - started < 2
    assert cancelled.wait(timeout=1)
    assert {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("asyncio_")
    } == baseline_workers


@pytest.mark.spec("EG-CONTRACT-R021")
def test_failed_sync_connect_releases_its_loop_resources(monkeypatch) -> None:
    """A failed async dial must not strand the loop thread or selector FDs."""

    async def fail_connect(**_kwargs: object) -> _AsyncClient:
        raise ConnectionError("engine unavailable")

    monkeypatch.setattr(EpistemicGraphClient, "connect", fail_connect)
    original_stop = SyncEpistemicGraphClient._stop_loop
    stopped: list[tuple[asyncio.AbstractEventLoop, threading.Thread]] = []

    def track_stop(loop: asyncio.AbstractEventLoop, thread: threading.Thread) -> None:
        original_stop(loop, thread)
        stopped.append((loop, thread))

    monkeypatch.setattr(
        SyncEpistemicGraphClient, "_stop_loop", staticmethod(track_stop)
    )

    for attempt in range(32):
        with pytest.raises(ConnectionError, match="engine unavailable"):
            SyncEpistemicGraphClient.connect(verified_context={})
        assert len(stopped) == attempt + 1, "failed dial skipped owned-loop teardown"
        loop, thread = stopped[-1]
        assert loop.is_closed(), "selector FD remains open"
        assert not thread.is_alive(), "owned loop thread remains alive"


def test_successful_sync_close_releases_loop_resources_once(monkeypatch) -> None:
    """The successful path closes the selector and remains idempotent."""
    clients: list[_AsyncClient] = []

    async def connect(**_kwargs: object) -> _AsyncClient:
        client = _AsyncClient()
        clients.append(client)
        return client

    monkeypatch.setattr(EpistemicGraphClient, "connect", connect)
    # Unrelated cleanup changes process totals but cannot change client ownership.
    unrelated_loop = asyncio.new_event_loop()

    for _ in range(32):
        client = SyncEpistemicGraphClient.connect(verified_context={})
        unrelated_loop.close()
        client.close()
        client.close()
        assert client._loop.is_closed(), "owned selector remains open"
        assert not client._thread.is_alive(), "owned loop thread remains alive"

    assert all(client.close_calls == 1 for client in clients)


def test_sync_close_retries_loop_teardown_without_reclosing_transport(
    monkeypatch,
) -> None:
    """A transient stop/join failure remains retryable by a later close call."""
    async_client = _AsyncClient()

    async def connect(**_kwargs: object) -> _AsyncClient:
        return async_client

    monkeypatch.setattr(EpistemicGraphClient, "connect", connect)
    original_stop_loop = SyncEpistemicGraphClient._stop_loop
    stop_calls = 0

    def fail_once(loop, thread) -> None:
        nonlocal stop_calls
        stop_calls += 1
        if stop_calls > 1:
            original_stop_loop(loop, thread)

    monkeypatch.setattr(
        SyncEpistemicGraphClient,
        "_stop_loop",
        staticmethod(fail_once),
    )
    client = SyncEpistemicGraphClient.connect(verified_context={})

    client.close()
    assert not client._loop.is_closed()
    assert client._thread.is_alive()
    assert async_client.close_calls == 1

    client.close()
    assert client._loop.is_closed()
    assert async_client.close_calls == 1
    assert stop_calls == 2
    assert not client._thread.is_alive(), "owned loop thread remains alive"
