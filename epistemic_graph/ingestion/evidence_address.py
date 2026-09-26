"""Stable, content-pinned identity for ingested evidence fragments."""

from __future__ import annotations

import hashlib
import re
import unicodedata

_SLUG_STRIP = re.compile(r"[^a-z0-9]+")
_WS = re.compile(r"\s+")


def slugify(text: str, *, max_length: int = 48) -> str:
    """Return a stable ASCII slug for a named fragment path segment."""
    folded = unicodedata.normalize("NFKD", str(text or ""))
    ascii_only = folded.encode("ascii", "ignore").decode("ascii").lower()
    slug = _SLUG_STRIP.sub("-", ascii_only).strip("-")
    return slug[:max_length].rstrip("-")


def content_digest(content: str | bytes) -> str:
    """Hash normalized text or raw bytes as a versioned content digest."""
    if isinstance(content, bytes):
        raw = content
    else:
        normalized = _WS.sub(" ", unicodedata.normalize("NFC", content)).strip()
        raw = normalized.encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def artifact_id_for(connector: str, source_instance: str, source_object_id: str) -> str:
    """Key an artifact to its source object, independent of its revision."""
    digest = hashlib.sha256(
        "\x1f".join((connector, source_instance, source_object_id)).encode("utf-8")
    ).hexdigest()
    return f"artifact:{digest[:40]}"


def path_anchor(kind: str, *, label: str = "", ordinal: int = 0) -> str:
    """Anchor a named unit by label or an unnamed unit by sibling ordinal."""
    slug = slugify(label) if label else ""
    return f"{kind}:{slug or ordinal}"


def fragment_id_for(artifact_id: str, path: tuple[str, ...] | list[str]) -> str:
    """Address a fragment by artifact and structural path, not body text."""
    joined = "/".join(str(segment) for segment in path)
    digest = hashlib.sha256(f"{artifact_id}\x1f{joined}".encode()).hexdigest()
    return f"fragment:{digest[:40]}"
