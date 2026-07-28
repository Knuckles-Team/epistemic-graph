"""Lifecycle regressions for the synchronous client-owned asyncio loop."""

from __future__ import annotations

import collections
import os
import threading

import pytest

from epistemic_graph.client import EpistemicGraphClient, SyncEpistemicGraphClient

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
