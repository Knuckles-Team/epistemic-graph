"""Deterministic material for one atomic fact tombstone and evidence edge."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def supersession_material(
    entity_id: str, evidence_id: str | None, reason: str
) -> tuple[str, dict[str, Any] | None]:
    """Return a versioned delete sidecar for one native graph transaction.

    The version tag avoids replaying an old tombstone that committed before
    evidence edges became part of the same transaction.
    """
    material = json.dumps(
        {"entity_id": entity_id, "evidence_id": evidence_id or "", "reason": reason},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    version = f"supersession-v2:{hashlib.sha256(material).hexdigest()}"
    if not evidence_id:
        return version, None
    return version, {
        "_links": [
            {
                "source": evidence_id,
                "target": entity_id,
                "relationship": "supersedes",
                "_rel": "SUPERSEDES",
                "reason": reason,
                "concept": "AU-KG.ingest.fact-supersession",
            }
        ]
    }
