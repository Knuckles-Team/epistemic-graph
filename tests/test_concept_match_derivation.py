"""Concept identity and match decisions without graph or model dependencies."""

import pytest

from epistemic_graph.concept_match_derivation import (
    Match,
    candidate_match,
    canonical_concept_id,
    concept_key,
    cosine_verdict,
    decide_feature,
    explicit_id_match,
    feature_refs,
    parse_judge,
)

pytestmark = pytest.mark.no_engine


def test_declared_identity_matches_but_body_citation_does_not():
    semantic = "AU-KG.query.vendor-agnostic-traversal"
    key = concept_key("concept:old", {"concept_id": semantic})
    assert key == semantic.upper()
    assert canonical_concept_id("kg-2-7") == "KG-2.7"
    assert feature_refs(
        "research:new",
        {"concept_ids": [semantic, semantic], "content": "related KG-2.7"},
    ) == [semantic.upper()]
    assert feature_refs("research:new", {"content": f"builds on {semantic}"}) == []
    matched = explicit_id_match(
        "research:new", {"concept_ids": [semantic]}, {key: "concept:old"}
    )
    assert matched is not None
    assert matched.decision == "covered" and matched.best.method == "id"


def test_judge_parse_and_candidate_fusion():
    assert parse_judge('{"verdict":"covered","confidence":2,"why":"x"}') == (
        "covered",
        1.0,
        "x",
    )
    assert parse_judge("noise covered noise") == ("covered", 0.5, "")
    assert parse_judge("") == ("unrelated", 0.0, "")
    assert parse_judge('["covered"]') == ("unrelated", 0.0, "")
    assert parse_judge('{"verdict":"covered","confidence":NaN}') == (
        "covered",
        0.0,
        "",
    )
    assert cosine_verdict(0.82)[0] == "covered"
    assert cosine_verdict(0.6)[0] == "related"
    assert cosine_verdict(0.59)[0] == "unrelated"
    match = candidate_match("concept:a", 0.8, "related", 0.9, "llm_judge", "why")
    assert match.score == 0.86


def test_feature_decision_prefers_accepted_coverage():
    related = Match("concept:r", 0.9, "related", 0.9, 0.9, "llm_judge")
    rejected_covered = Match("concept:c0", 0.9, "covered", 0.5, 0.8, "llm_judge")
    accepted_covered = Match("concept:c1", 0.8, "covered", 0.8, 0.8, "llm_judge")
    assert decide_feature("f", [related, rejected_covered], 0.6).decision == "related"
    result = decide_feature("f", [related, accepted_covered], 0.6)
    assert result.decision == "covered" and result.best is accepted_covered
    assert decide_feature("f", [rejected_covered], 0.6).novelty_score == 1.0
