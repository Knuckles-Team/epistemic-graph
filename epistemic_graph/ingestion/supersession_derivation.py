"""Deterministic material for one atomic fact tombstone and evidence edge."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


def supersession_material(
    entity_id: str,
    evidence_id: str | None,
    reason: str,
    *,
    lifecycle_event: Mapping[str, Any] | None = None,
) -> tuple[str, dict[str, Any] | None]:
    """Return a versioned delete sidecar for one native graph transaction.

    The version tag avoids replaying an old tombstone that committed before
    evidence edges became part of the same transaction.
    """
    event_identity = (
        {
            "claim_id": str(lifecycle_event["claim_id"]),
            "from_state": str(lifecycle_event["from_state"]),
            "to_state": str(lifecycle_event["to_state"]),
            "reason": str(lifecycle_event["reason"]),
        }
        if lifecycle_event is not None
        else None
    )
    if event_identity is not None and (
        not evidence_id
        or event_identity["claim_id"] != evidence_id
        or event_identity["to_state"] != "retracted"
        or event_identity["reason"] != reason
        or event_identity["from_state"]
        not in {"proposed", "validated", "accepted", "deprecated"}
    ):
        raise ValueError("supersession lifecycle event does not match the claim")
    material_fields: dict[str, Any] = {
        "entity_id": entity_id,
        "evidence_id": evidence_id or "",
        "reason": reason,
    }
    if event_identity is not None:
        material_fields["lifecycle_event"] = event_identity
    material = json.dumps(
        material_fields,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    version_tag = (
        "supersession-v3" if lifecycle_event is not None else "supersession-v2"
    )
    version = f"{version_tag}:{hashlib.sha256(material).hexdigest()}"
    if not evidence_id and lifecycle_event is None:
        return version, None
    sidecar: dict[str, Any] = {}
    if evidence_id:
        sidecar["_links"] = [
            {
                "source": evidence_id,
                "target": entity_id,
                "relationship": "supersedes",
                "_rel": "SUPERSEDES",
                "reason": reason,
                "concept": "AU-KG.ingest.fact-supersession",
            }
        ]
    if lifecycle_event is not None:
        assert event_identity is not None
        event_key = json.dumps(
            event_identity, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        event_id = f"claim_lifecycle:{hashlib.sha256(event_key).hexdigest()[:40]}"
        sidecar["_nodes"] = [
            {
                "id": event_id,
                "node_type": "ClaimLifecycleEvent",
                **dict(lifecycle_event),
            }
        ]
    return version, sidecar
