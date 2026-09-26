"""Supersession material carries a stable atomic edge and new replay identity."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.supersession_derivation import supersession_material

pytestmark = pytest.mark.no_engine


def test_supersession_material_is_versioned_and_sensitive_to_evidence() -> None:
    version, sidecar = supersession_material("fact:1", "claim:2", "corrected")
    assert version.startswith("supersession-v2:")
    assert (version, sidecar) == supersession_material(
        "fact:1", "claim:2", "corrected"
    )
    assert sidecar == {
        "_links": [
            {
                "source": "claim:2",
                "target": "fact:1",
                "relationship": "supersedes",
                "_rel": "SUPERSEDES",
                "reason": "corrected",
                "concept": "AU-KG.ingest.fact-supersession",
            }
        ]
    }
    changed, _ = supersession_material("fact:1", "claim:3", "corrected")
    assert changed != version
    changed, _ = supersession_material("fact:1", "claim:2", "superseded")
    assert changed != version
    _, no_edge = supersession_material("fact:1", None, "corrected")
    assert no_edge is None


def test_materialized_claim_retraction_carries_event_in_same_sidecar() -> None:
    event = {
        "claim_id": "claim:2",
        "from_state": "accepted",
        "to_state": "retracted",
        "reason": "corrected",
        "actor": "loop_engine",
        "timestamp": "2026-09-26T12:00:00Z",
    }
    version, sidecar = supersession_material(
        "fact:1", "claim:2", "corrected", lifecycle_event=event
    )
    assert version.startswith("supersession-v3:")
    assert sidecar is not None
    assert len(sidecar["_links"]) == len(sidecar["_nodes"]) == 1
    node = sidecar["_nodes"][0]
    assert node["node_type"] == "ClaimLifecycleEvent"
    assert node["claim_id"] == "claim:2"
    assert node["to_state"] == "retracted"
    assert node["id"].startswith("claim_lifecycle:")
    retry = dict(event, timestamp="2026-09-26T12:05:00Z")
    retry_version, retry_sidecar = supersession_material(
        "fact:1", "claim:2", "corrected", lifecycle_event=retry
    )
    assert retry_version == version
    assert retry_sidecar["_nodes"][0]["id"] == node["id"]
    with pytest.raises(ValueError, match="does not match the claim"):
        supersession_material(
            "fact:1", "claim:other", "corrected", lifecycle_event=event
        )
