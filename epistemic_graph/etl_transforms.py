"""Deterministic ETL transform primitives owned by epistemic-graph.

Pure record-to-entity mapping helpers. Source transport, credentials, and ingestion
orchestration are intentionally outside this module.
"""

from __future__ import annotations

import hashlib
from typing import Any

__all__ = ["dig", "coalesce", "stable_id", "cast", "rename", "flatten"]


def dig(record: dict[str, Any] | None, path: str, default: Any = None) -> Any:
    """Resolve a dotted path (``"attributes.name"``) within a nested dict.

    Return ``default`` for an absent component or a non-dictionary intermediate.
    """
    if not isinstance(record, dict):
        return default
    value: Any = record
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return default
        value = value[part]
    return value if value is not None else default


def coalesce(record: dict[str, Any] | None, *paths: str, default: Any = None) -> Any:
    """Return the first *truthy* dotted-path value across ``paths``, else ``default``.

    Generalizes the ``rec.get("a") or rec.get("b") or default`` idiom repeated
    across the ``_sync_*`` handlers (e.g. Firefly III's transaction name falling
    back from ``group_title`` to the first split's ``description``, or a
    ``document_title or f"Document {id}"`` placeholder) to dotted paths — matching
    the exact falsy-skips-too ``or`` semantics of the code it replaces (an absent
    field and an empty-string field both fall through), not a ``None``-only check.
    """
    for path in paths:
        value = dig(record, path)
        if value:
            return value
    return default


def stable_id(*parts: Any, prefix: str = "", hash_algo: str | None = None) -> str:
    """Build a deterministic node id from ``parts``.

    Documents the ``f"{source}:{type}:{external_id}"`` idiom every ``_sync_*``
    handler already hand-rolls (``firefly:account:{aid}``,
    ``dockerhub:{ns}/{name}``, ``plane:{instance}:issue:{iid}``, …) as one named
    primitive. Plain colon-joined concatenation is used when every part is
    already a short, stable natural key (the common case — readable ids, easy to
    grep in the KG). Pass ``hash_algo`` (any :mod:`hashlib` name, e.g. ``"sha1"``)
    when a part is NOT a stable/short key (long free text, a mutable compound
    object) so the id stays short and deterministic without embedding the raw
    value.
    """
    joined = ":".join(str(p) for p in parts if p not in (None, ""))
    if hash_algo:
        digest = hashlib.new(hash_algo, joined.encode("utf-8")).hexdigest()
        return f"{prefix}:{digest}" if prefix else digest
    return f"{prefix}:{joined}" if prefix else joined


def cast(value: Any, to: type, *, default: Any = None) -> Any:
    """Best-effort type coercion — never raises.

    Returns ``default`` when ``value`` is ``None`` or the coercion fails. The
    documented replacement for the ad hoc ``int(x) if x is not None else None``
    scattered across handlers building numeric KG entity fields (pull/star
    counts, balances, durations, …). ``bool`` coercion additionally understands
    common truthy/falsy string tokens (``"true"``/``"1"``/``"yes"``).
    """
    if value is None:
        return default
    try:
        if to is bool and isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return to(value)
    except (TypeError, ValueError):
        return default


def rename(record: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    """Return a copy of ``record`` with top-level keys renamed per ``mapping``
    (``{old_key: new_key}``). Unmapped keys pass through unchanged — a handler
    states only the fields whose source name differs from the KG property name.
    """
    return {mapping.get(k, k): v for k, v in record.items()}


def flatten(record: dict[str, Any], *, sep: str = ".") -> dict[str, Any]:
    """Flatten a nested dict into a single-level dict of ``sep``-joined keys.

    Mirrors the JSON:API-style envelope digging repeated across handlers (e.g.
    Firefly III / paperless's ``rec.get("attributes")`` unwrap) into one general
    primitive: any nested-dict source record becomes flat ``attributes.name``
    keys a handler ``.get()``s directly instead of hand-unwrapping each envelope
    level. Lists are kept as-is (not recursed into) — KG entity fields are scalars.
    """
    out: dict[str, Any] = {}

    def _walk(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for key, sub in value.items():
                _walk(f"{prefix}{sep}{key}" if prefix else str(key), sub)
        else:
            out[prefix] = value

    _walk("", record)
    return out
