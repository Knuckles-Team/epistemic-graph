"""Canonical source identity for graph ingestion rows.

The source stamp is derived before submission so all ingestion paths agree on
the graph's ``source_system`` and ``domain`` properties. The graph remains the
authority for persisted provenance and receipts.
"""

from __future__ import annotations

from typing import Any


def stamp_source(props: dict[str, Any], source: str | None) -> dict[str, Any]:
    """Fill absent source properties from a nonempty canonical source id.

    Caller supplied values win. Internal writes with no source are unchanged.
    """
    src = (source or "").strip().lower()
    if not src:
        return props
    props.setdefault("source_system", src)
    props.setdefault("domain", src)
    return props
