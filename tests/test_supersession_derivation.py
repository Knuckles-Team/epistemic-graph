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
