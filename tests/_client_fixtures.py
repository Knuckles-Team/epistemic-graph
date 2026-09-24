"""Typed transport placeholders for pure client-side tests."""

from __future__ import annotations

import asyncio
from typing import Any, NamedTuple


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


class SentCall(NamedTuple):
    """One `_send` a fake transport received, in `_send`'s own argument order."""

    method: str
    params: dict[str, Any] | None
    graph: str | None
    idempotency_key: str | None


class RecordingTransport:
    """A fake `EpistemicGraphClient` transport: `_send` records the call, then
    answers with `reply`. Subclasses decide the answer (and may assert on the
    call there); tests read `sent` in call order."""

    def __init__(self) -> None:
        self.sent: list[SentCall] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> Any:
        call = SentCall(method, params, graph, idempotency_key)
        self.sent.append(call)
        return self.reply(call)

    def reply(self, call: SentCall) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not answer {call.method}"
        )


class PayloadTransport(RecordingTransport):
    """Answers each method with its fixed payload."""

    def __init__(self, payloads: dict[str, Any]) -> None:
        super().__init__()
        self.payloads = payloads

    def reply(self, call: SentCall) -> Any:
        return self.payloads[call.method]
