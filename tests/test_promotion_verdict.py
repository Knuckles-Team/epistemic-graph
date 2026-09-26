"""Promotion verdict priority is deterministic and independent of AU gates."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from epistemic_graph.ingestion.promotion_verdict import (
    PromotionDecision,
    PromotionVerdict,
)

pytestmark = pytest.mark.no_engine


@dataclass
class Check:
    name: str
    passed: bool
    reason: str = ""


@pytest.mark.parametrize(
    ("failed", "expected"),
    [
        ([], PromotionDecision.CLEARED),
        (["confidence"], PromotionDecision.NEEDS_REVIEW),
        (["shacl"], PromotionDecision.REJECTED),
        (["pii"], PromotionDecision.QUARANTINED),
        (["confidence", "shacl"], PromotionDecision.REJECTED),
        (["pii", "shacl"], PromotionDecision.QUARANTINED),
    ],
)
def test_failed_gate_priority(failed: list[str], expected: PromotionDecision) -> None:
    checks = [
        Check(name, name not in failed, "failed" if name in failed else "")
        for name in (
            "classification_policy",
            "pii",
            "shacl",
            "dedup",
            "contradiction",
            "confidence",
        )
    ]
    verdict = PromotionVerdict("claim-1", checks)
    assert verdict.decision is expected
    assert verdict.to_dict()["decision"] == expected.value
    assert (verdict.summary == "all governance checks passed") == (not failed)
