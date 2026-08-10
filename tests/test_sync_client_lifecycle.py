"""Lifecycle regressions for the synchronous client-owned asyncio loop."""

from __future__ import annotations

import collections
import asyncio
import os
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


def _resources() -> tuple[int, int, int, collections.Counter[str]]:
    sockets = 0
    epolls = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            target = os.readlink(f"/proc/self/fd/{fd}")
        except OSError:
            continue
        sockets += target.startswith("socket:")
        epolls += "eventpoll" in target
    return (
        len(os.listdir("/proc/self/fd")),
        sockets,
        epolls,
        collections.Counter(thread.name for thread in threading.enumerate()),
    )


class _AsyncClient:
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


def test_sync_supports_matches_async_capability_probe() -> None:
    """The sync wrapper exposes the same fail-closed capability result."""
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, name="dcdx98-capability-loop")
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


@pytest.mark.skipif(
    not os.path.isdir("/proc/self/fd"),
    reason="requires Linux /proc file-descriptor accounting",
)
def test_failed_sync_connect_releases_its_loop_resources(monkeypatch) -> None:
    """A failed async dial must not strand the loop thread or selector FDs."""

    async def fail_connect(**_kwargs: object) -> _AsyncClient:
        raise ConnectionError("engine unavailable")

    monkeypatch.setattr(EpistemicGraphClient, "connect", fail_connect)
    baseline = _resources()

    for _ in range(32):
        with pytest.raises(ConnectionError, match="engine unavailable"):
            SyncEpistemicGraphClient.connect(verified_context={})
        assert _resources() == baseline


@pytest.mark.skipif(
    not os.path.isdir("/proc/self/fd"),
    reason="requires Linux /proc file-descriptor accounting",
)
def test_successful_sync_close_releases_loop_resources_once(monkeypatch) -> None:
    """The successful path closes the selector and remains idempotent."""
    clients: list[_AsyncClient] = []

    async def connect(**_kwargs: object) -> _AsyncClient:
        client = _AsyncClient()
        clients.append(client)
        return client

    monkeypatch.setattr(EpistemicGraphClient, "connect", connect)
    baseline = _resources()

    for _ in range(32):
        client = SyncEpistemicGraphClient.connect(verified_context={})
        client.close()
        client.close()
        assert _resources() == baseline

    assert all(client.close_calls == 1 for client in clients)


@pytest.mark.skipif(
    not os.path.isdir("/proc/self/fd"),
    reason="requires Linux /proc file-descriptor accounting",
)
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
    baseline = _resources()
    client = SyncEpistemicGraphClient.connect(verified_context={})

    client.close()
    assert not client._loop.is_closed()
    assert async_client.close_calls == 1

    client.close()
    assert client._loop.is_closed()
    assert async_client.close_calls == 1
    assert stop_calls == 2
    assert _resources() == baseline
