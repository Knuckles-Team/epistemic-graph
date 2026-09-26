"""Deterministic identities and timestamps for engine time series."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def cypher_string(value: str) -> str:
    """Render one bounded time-series symbol as a native Cypher string."""
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 256
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("Time-series symbol is not safely representable")
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def to_nanoseconds(dt: datetime) -> int:
    """Convert a point timestamp to the engine's integer nanosecond key."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta = dt.astimezone(UTC) - _EPOCH
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000_000) + (
        delta.microseconds * 1_000
    )


def from_nanoseconds(ns: int) -> datetime:
    """Return the UTC datetime at Python's microsecond resolution."""
    return _EPOCH + timedelta(microseconds=ns // 1_000)


def series_id(symbol: str, tags: dict[str, str] | None) -> str:
    """Stable series id for one ``(symbol, tags)`` pair."""
    if not tags:
        return f"ts:{symbol}"
    canon = json.dumps(tags, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canon.encode("utf-8")).hexdigest()[:32]
    return f"ts:{symbol}:{digest}"
