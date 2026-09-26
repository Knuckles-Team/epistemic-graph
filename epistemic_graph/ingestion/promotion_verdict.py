"""Pure promotion gate verdict; steward workflow remains with the caller."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol


class GovernanceCheckLike(Protocol):
    name: str
    passed: bool
    reason: str


class PromotionDecision(StrEnum):
    """The governance verdict's outcome (mirrors ``kb.extraction_run``'s item
    taxonomy so the platform has ONE accepted/needs_review/rejected/quarantined
    vocabulary, not two)."""

    #: Every gate passed — eligible to advance to VALIDATED, still pending an
    #: explicit steward ``accept``.
    CLEARED = "cleared"
    #: A soft gate (dedup / contradiction / confidence) held it — stays
    #: PROPOSED, awaiting more evidence or a manual steward decision.
    NEEDS_REVIEW = "needs_review"
    #: A hard structural/policy/SHACL gate failed — retracted, never
    #: materialized.
    REJECTED = "rejected"
    #: The PII guard found sensitive content — retracted, never materialized.
    QUARANTINED = "quarantined"


@dataclass
class PromotionVerdict:
    """The full per-gate governance verdict for one candidate claim."""

    claim_id: str
    checks: list[GovernanceCheckLike] = field(default_factory=list)

    @property
    def decision(self) -> PromotionDecision:
        by_name = {c.name: c for c in self.checks}
        pii = by_name.get("pii")
        if pii is not None and not pii.passed:
            return PromotionDecision.QUARANTINED
        hard = ("classification_policy", "shacl")
        if any(not by_name[n].passed for n in hard if n in by_name):
            return PromotionDecision.REJECTED
        soft = ("dedup", "contradiction", "confidence")
        if any(not by_name[n].passed for n in soft if n in by_name):
            return PromotionDecision.NEEDS_REVIEW
        return PromotionDecision.CLEARED

    @property
    def summary(self) -> str:
        failing = [f"{c.name}: {c.reason}" for c in self.checks if not c.passed]
        return "; ".join(failing) if failing else "all governance checks passed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "decision": self.decision.value,
            "summary": self.summary,
            "checks": [
                {"name": c.name, "passed": c.passed, "reason": c.reason}
                for c in self.checks
            ],
        }
