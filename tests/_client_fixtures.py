"""Typed transport placeholders for pure client-side tests."""

from __future__ import annotations

import asyncio


class _UnusedStreamReader(asyncio.StreamReader):
    """Nominal reader for tests that never access the transport."""

    def __init__(self) -> None:
        # The client-side tests using this fixture exercise validation, signing,
        # or monkeypatched round trips, so initializing asyncio's live stream
        # machinery would add an event-loop dependency without testing it.
        pass


class _UnusedStreamWriter(asyncio.StreamWriter):
    """Nominal writer for tests that never access the transport."""

    def __init__(self) -> None:
        pass

    def __del__(self) -> None:
        # StreamWriter's destructor reads fields established by its production
        # constructor. This deliberately inert fixture owns no live transport.
        pass


def unused_reader() -> asyncio.StreamReader:
    """Return a correctly typed inert reader for a direct client fixture."""

    return _UnusedStreamReader()


def unused_writer() -> asyncio.StreamWriter:
    """Return a correctly typed inert writer for a direct client fixture."""

    return _UnusedStreamWriter()
