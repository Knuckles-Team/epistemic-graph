"""The one transport shape the typed sub-clients drive.

`ConnectorPackClient` and `FleetCatalogClient` take any object with the
engine client's `_send` coroutine (the real `EpistemicGraphClient`, or a test
double), so they depend on this structural type rather than on the client
class itself.
"""

from __future__ import annotations

from typing import Any, Protocol


class EngineTransport(Protocol):
    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> Any: ...
