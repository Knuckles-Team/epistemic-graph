"""Small fake for client adapter tests that only inspect outgoing typed requests."""

from __future__ import annotations


class CapturingEngine:
    def __init__(self, answer: object) -> None:
        self.answer = answer
        self.sent: list[tuple[str, object]] = []

    async def _send(
        self,
        method: str,
        params: object,
        graph: object,
        *,
        idempotency_key: object,
    ) -> object:
        self.sent.append((method, params))
        return self.answer
