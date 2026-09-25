"""Metadata-only usage facts stored in the authenticated engine graph.

This is the narrow write/read-by-id contract for usage accounting.  Analytics
indexes and transcript search are deliberately separate engine operations.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import EpistemicGraphClient


_TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cache_creation_tokens",
    "cache_read_tokens",
    "reasoning_tokens",
)


def _opaque_ref(value: str, kind: str) -> str:
    prefix = f"pref_{kind}_"
    digest = value.removeprefix(prefix)
    if not value.startswith(prefix) or len(digest) != 64:
        raise ValueError(f"{kind} must be an opaque persistence reference")
    if any(ch not in "0123456789abcdef" for ch in digest):
        raise ValueError(f"{kind} must be an opaque persistence reference")
    return value


@dataclass(frozen=True, slots=True)
class UsageEventFact:
    """One immutable usage event, with no prompt, tool input, or host fields."""

    event_ref: str
    run_ref: str
    origin: str
    occurred_at: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_tokens: int = 0
    cache_read_tokens: int = 0
    reasoning_tokens: int = 0
    cost_microusd: int | None = None
    model_ref: str | None = None

    def __post_init__(self) -> None:
        _opaque_ref(self.event_ref, "usage_dedup")
        _opaque_ref(self.run_ref, "run")
        if self.model_ref is not None:
            _opaque_ref(self.model_ref, "model")
        if self.origin not in {"runtime", "ingested"}:
            raise ValueError("unsupported usage origin")
        if not self.occurred_at or len(self.occurred_at) > 40:
            raise ValueError("occurred_at must be a bounded timestamp")
        try:
            timestamp = datetime.fromisoformat(self.occurred_at)
        except ValueError as exc:
            raise ValueError("occurred_at must be an ISO-8601 timestamp") from exc
        if timestamp.tzinfo is None:
            raise ValueError("occurred_at must be an ISO-8601 timestamp")
        for field in _TOKEN_FIELDS:
            value = getattr(self, field)
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} must be a non-negative integer")
        if self.cost_microusd is not None and (
            type(self.cost_microusd) is not int or self.cost_microusd < 0
        ):
            raise ValueError("cost_microusd must be a non-negative integer")


class UsageFactStore:
    """Durable usage fact projection bound to the client's signed authority."""

    def __init__(self, client: EpistemicGraphClient, reference_key: bytes) -> None:
        if len(reference_key) < 32:
            raise ValueError("stable usage reference key must be at least 32 bytes")
        self._client = client
        self._reference_key = reference_key

    def _authority(self) -> tuple[str, str, str]:
        claims = self._client._effective_verified_context()
        tenant = claims["tenant"]
        principal = claims["principal"]
        agent_id = claims["agent_id"]
        if not tenant or not principal or not agent_id:
            raise PermissionError("verified usage authority required")
        key = self._reference_key
        tenant_ref = hmac.new(
            key, b"usage:tenant:\x00" + tenant.encode(), hashlib.sha256
        ).hexdigest()
        principal_ref = hmac.new(
            key, b"usage:principal:\x00" + principal.encode(), hashlib.sha256
        ).hexdigest()
        return tenant_ref, principal_ref, agent_id

    async def append_event(self, fact: UsageEventFact) -> bool:
        """Insert once; identical replay succeeds, conflicting replay fails closed."""

        tenant_ref, principal_ref, agent_id = self._authority()
        node_id = f"usage:event:{tenant_ref}:{fact.event_ref}"
        properties = {
            "type": "UsageEvent",
            "schema": "usage-event-fact-v1",
            "tenant_ref": tenant_ref,
            "principal_ref": principal_ref,
            "_owner": agent_id,
            "_visibility": "private",
            "event_ref": fact.event_ref,
            "run_ref": fact.run_ref,
            "origin": fact.origin,
            "occurred_at": fact.occurred_at,
            "input_tokens": fact.input_tokens,
            "output_tokens": fact.output_tokens,
            "cache_creation_tokens": fact.cache_creation_tokens,
            "cache_read_tokens": fact.cache_read_tokens,
            "reasoning_tokens": fact.reasoning_tokens,
            "cost_microusd": fact.cost_microusd,
            "model_ref": fact.model_ref,
        }
        if await self._client.nodes.create_if_absent(node_id, properties):
            return True
        existing = await self._client.nodes.properties(node_id)
        if existing != properties:
            raise ValueError("usage event identity conflict")
        return False

    async def event(self, event_ref: str) -> UsageEventFact | None:
        """Read by opaque ID within the signed tenant; never search all tenants."""

        _opaque_ref(event_ref, "usage_dedup")
        tenant_ref, _principal_ref, _agent_id = self._authority()
        node_id = f"usage:event:{tenant_ref}:{event_ref}"
        properties = await self._client.nodes.properties(node_id)
        if properties is None:
            return None
        if (
            properties.get("tenant_ref") != tenant_ref
            or properties.get("schema") != "usage-event-fact-v1"
        ):
            raise PermissionError("usage fact authority mismatch")
        fields = UsageEventFact.__dataclass_fields__
        return UsageEventFact(**{key: properties[key] for key in fields})
