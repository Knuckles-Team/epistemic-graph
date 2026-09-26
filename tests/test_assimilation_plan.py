"""Grounded fallback plan text is derived in EG."""

from __future__ import annotations

import pytest

from epistemic_graph.assimilation_plan import PlanProposal, default_synth

pytestmark = pytest.mark.no_engine


def test_fallback_plan_contains_grounding_and_sources():
    proposal = default_synth(
        {
            "name": "Schema alignment",
            "pillar": "KG",
            "concept_ids": ["AU-KG.ingest.schema-alignment"],
            "sources": ["paper:1", "repo:2"],
            "synergies": ["feature:3"],
        }
    )
    assert proposal["title"] == "Assimilate: Schema alignment"
    assert "paper:1, repo:2" in proposal["body"]
    assert "feature:3" in proposal["body"]
    assert "AU-KG.ingest.schema-alignment" in proposal["body"]


def test_plan_proposal_default_is_reviewable_proposal():
    proposal = PlanProposal("feature:1", "plan:1", "title", "body")
    assert proposal.status == "proposed"
    assert proposal.sources == []
    assert proposal.synergies == []
