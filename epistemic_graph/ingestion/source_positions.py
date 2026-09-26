"""Pure source cursor and content-version position derivation."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any


def typed_position(value: str | None, *, content: bool) -> dict[str, Any]:
    raw = str(value or "")
    if raw.isdecimal():
        return {"kind": "sequence", "value": int(raw)}
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return {"kind": "timestamp_millis", "value": int(parsed.timestamp() * 1000)}
    except (TypeError, ValueError, OverflowError):
        discriminator = "version_type" if content else "cursor_type"
        return {
            "kind": "opaque",
            "value": {discriminator: "connector_opaque_v1", "value": raw},
        }


def _numericposition_advances(left: Any, right: Any) -> bool:
    """Strictly-greater comparison for a sequence/timestamp position."""
    if left is None or right is None:
        return False
    try:
        return int(left) > int(right)
    except (TypeError, ValueError):
        return False


def _opaqueposition_advances(left: dict[str, Any], right: dict[str, Any]) -> bool:
    """A connector-opaque position advances only within the same cursor type."""
    left_type = left.get("cursor_type", left.get("version_type"))
    right_type = right.get("cursor_type", right.get("version_type"))
    return left_type == right_type and bool(left.get("value")) and left != right


def position_advances(next_value: dict[str, Any], prior: dict[str, Any]) -> bool:
    if next_value.get("kind") != prior.get("kind"):
        return False
    kind = next_value.get("kind")
    left = next_value.get("value")
    right = prior.get("value")
    if kind in {"sequence", "timestamp_millis"}:
        return _numericposition_advances(left, right)
    if kind == "opaque" and isinstance(left, dict) and isinstance(right, dict):
        return _opaqueposition_advances(left, right)
    return False


def cursor_partition(source_instance: str) -> str:
    return (
        hashlib.sha256(source_instance.encode("utf-8")).hexdigest()[:32]
        if source_instance
        else ""
    )


def _checkpoint_from_sequence(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return None


def _checkpoint_from_timestamp_millis(value: Any) -> str | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _checkpoint_from_opaque(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    raw = value.get("value")
    return str(raw) if raw not in (None, "") else None


#: Typed-position ``kind`` -> reader. A kind with no reader (or a reader that
#: cannot decode its value) yields ``None``, exactly as the previous if-chain's
#: terminal ``return None`` did: an undecodable position is never a checkpoint.
_CHECKPOINT_READERS: dict[str, Callable[[Any], str | None]] = {
    "sequence": _checkpoint_from_sequence,
    "timestamp_millis": _checkpoint_from_timestamp_millis,
    "opaque": _checkpoint_from_opaque,
}


def checkpoint_from_position(position: Any) -> str | None:
    if not isinstance(position, dict):
        return None
    reader = _CHECKPOINT_READERS.get(str(position.get("kind") or ""))
    return reader(position.get("value")) if reader is not None else None


def _advancedcontent_position(
    prior: dict[str, Any], material_digest: str
) -> dict[str, Any] | None:
    """Advance a prior typed content position, or ``None`` if it cannot be read.

    ``None`` means "no advancing position derivable from the prior value" and
    the caller falls back to the digest-derived position — the SAME outcome the
    previous inline chain reached by falling through its ``except``/``if`` arms.
    """
    kind = prior.get("kind")
    value = prior.get("value")
    if kind in {"sequence", "timestamp_millis"} and value is not None:
        try:
            return {"kind": kind, "value": int(value) + 1}
        except (TypeError, ValueError):
            return None
    if kind == "opaque" and isinstance(value, dict):
        version_type = str(value.get("version_type") or "connector_opaque_v1")
        return {
            "kind": "opaque",
            "value": {"version_type": version_type, "value": material_digest},
        }
    return None


def content_position(
    explicit: str | None,
    current: dict[str, Any] | None,
    material_digest: str,
) -> dict[str, Any]:
    """Choose an advancing content version without inventing wall-clock state."""
    if explicit:
        return typed_position(explicit, content=True)
    prior = (
        current.get("source_version")
        if isinstance(current, dict) and isinstance(current.get("source_version"), dict)
        else None
    )
    if isinstance(prior, dict):
        advanced = _advancedcontent_position(prior, material_digest)
        if advanced is not None:
            return advanced
    return typed_position(material_digest, content=True)
