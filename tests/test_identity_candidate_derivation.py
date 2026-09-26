"""Pure identity evidence and review-candidate derivation contract."""

from dataclasses import dataclass, field

import pytest

from epistemic_graph.identity_candidate_derivation import (
    EntityRecord,
    IdentityEvidenceKind,
    aggregate_confidence,
    applicable_rules,
    derive_candidate,
    exact_identifier_evidence,
    name_evidence,
    pair_id,
    structural_evidence,
)

pytestmark = pytest.mark.no_engine


@dataclass
class Rule:
    identifier_fields: list[str] = field(default_factory=list)
    name_fields: list[str] = field(default_factory=list)
    exact_identifier_score: float = 0.98
    min_confidence_to_flag: float = 0.5
    scope: str = ""

    def applies(self, kind: str) -> bool:
        return self.scope in kind


def test_rule_scoped_evidence_and_review_only_candidate():
    a = EntityRecord("cmdb:a", "Payments Svc", "servicenow_cmdb", {"cmdb_id": "42"})
    b = EntityRecord("cmdb:b", "Payment Service", "servicenow_cmdb", {"cmdb_id": "42"})
    scoped = applicable_rules(a.kind, b.kind, [Rule(["cmdb_id"], scope="servicenow")])
    exact = exact_identifier_evidence(a, b, scoped)
    assert exact is not None and exact.kind == IdentityEvidenceKind.EXACT_IDENTIFIER
    assert exact.detail == "cmdb_id=42"
    structural = structural_evidence({"one", "two"}, {"two", "three"})
    assert structural is not None and structural.score == pytest.approx(1 / 3)
    evidence = [exact, name_evidence(a, b, 0.7, "fuzzy"), structural]
    candidate = derive_candidate(a, b, evidence, scoped, 0.5, "2026-09-26T00:00:00Z")
    assert candidate is not None
    assert candidate.status == "candidate"
    assert candidate.confidence > exact.score
    assert candidate.id == pair_id(b.id, a.id)


def test_no_fabricated_candidate_and_pack_threshold():
    a, b = EntityRecord("a", "one"), EntityRecord("b", "two")
    assert derive_candidate(a, b, [], [], 0.0, "now") is None
    assert (
        derive_candidate(a, a, [name_evidence(a, a, 1.0, "exact")], [], 0.0, "now")
        is None
    )
    weak = [name_evidence(a, b, 0.5, "fuzzy")]
    strict = [Rule(name_fields=["name"], min_confidence_to_flag=0.9)]
    assert derive_candidate(a, b, weak, strict, 0.5, "now") is None
    assert derive_candidate(a, b, weak, [], 0.5, "now") is not None


def test_product_complement_is_shared_and_nonfinite_evidence_fails_closed():
    assert aggregate_confidence([0.5, 0.5]) == 0.75
    assert aggregate_confidence([1.5, -0.2]) == 1.0
    assert aggregate_confidence([float("nan")]) == 0.0
    assert aggregate_confidence([float("inf")]) == 0.0
