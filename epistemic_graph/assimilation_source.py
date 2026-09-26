"""Canonical research source IDs and content fingerprints."""

from __future__ import annotations

import hashlib
import re

_ARXIV = re.compile(r"arxiv\.org/(?:abs|pdf)/(\d+\.\d+)(?:v\d+)?", re.IGNORECASE)
_DOI = re.compile(r"(?:doi\.org/|doi:)\s*(10\.\S+)", re.IGNORECASE)
_WS = re.compile(r"\s+")
def canonical_source_id(uri: str) -> str:
    """Canonicalize a source URI so equivalent references collapse to one id.

    arxiv abs/pdf/versioned → ``arxiv:<id>``; DOI variants → ``doi:<id>``; other
    URLs → ``url:<host/path>`` (trailing slash + scheme stripped); file paths →
    ``file:<normalized path>``.
    """
    u = (uri or "").strip()
    if not u:
        return ""
    m = _ARXIV.search(u)
    if m:
        return f"arxiv:{m.group(1)}"
    m = _DOI.search(u)
    if m:
        return f"doi:{m.group(1).rstrip('/')}"
    if u.startswith(("http://", "https://")):
        rest = u.split("://", 1)[1].rstrip("/").lower()
        return f"url:{rest}"
    return f"file:{u.rstrip('/')}"


def content_fingerprint(text: str) -> str:
    """Stable per-item content hash (whitespace-normalized SHA-256, 16 hex)."""
    norm = _WS.sub(" ", (text or "").strip()).lower()
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]
