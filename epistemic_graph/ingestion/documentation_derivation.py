"""Deterministic identities and verified-snapshot decisions for documentation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any


def documentation_content_digest(content: str) -> str:
    """Digest exact UTF-8 Markdown bytes without retaining source text."""
    if not isinstance(content, str):
        raise TypeError("Markdown content must be text")
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


def stable_documentation_id(prefix: str, *parts: str, length: int = 40) -> str:
    """Derive a stable object ID from ordered, separated identity components."""
    material = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}:{hashlib.sha256(material).hexdigest()[:length]}"


def removed_documentation_paths(
    current_keys: Iterable[tuple[str, str]],
    prior_keys: Iterable[tuple[str, str]],
    superseded_keys: Iterable[tuple[str, str]],
    *,
    snapshot_verified: bool,
) -> list[tuple[str, str]]:
    """Admit removals only when an authoritative snapshot proves absence."""
    current = set(current_keys)
    superseded = set(superseded_keys)
    if superseded & current:
        raise ValueError("a superseded documentation path is still present as current")
    removed = sorted((set(prior_keys) | superseded) - current)
    if removed and not snapshot_verified:
        raise ValueError(
            "verified snapshot is required before emitting documentation tombstones"
        )
    return removed


def documentation_snapshot_digest(
    snapshot_revision: str | None,
    records: Iterable[Mapping[str, Any]],
    removed_keys: Iterable[tuple[str, str]],
) -> str:
    """Digest a stable, ordered projection of one repository snapshot."""
    payload = json.dumps(
        {
            "revision": snapshot_revision or "",
            "records": list(records),
            "removed": list(removed_keys),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"
