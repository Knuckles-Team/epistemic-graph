"""Pure evidence and candidate derivation for entity identity review.

This module never confirms an identity merge. Callers resolve domain packs,
name similarity, graph neighbors, and governed writes outside this boundary.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

# A shared Wikidata QID is exact identifier evidence for world-reference
# alignment (EH-362), alongside generic source and CMDB identifiers.
GENERIC_IDENTIFIER_FIELDS: frozenset[str] = frozenset(
    {"cmdb_id", "external_id", "id", "wikidata_id"}
)


class IdentityRuleLike(Protocol):
    """Structural view of pack-provided identity rules."""

    identifier_fields: list[str]
    name_fields: list[str]
    exact_identifier_score: float
    min_confidence_to_flag: float

    def applies(self, kind: str) -> bool: ...


@dataclass(frozen=True)
class EntityRecord:
    id: str
    name: str
    kind: str = ""
    identifiers: dict[str, str] = field(default_factory=dict)


class IdentityEvidenceKind(StrEnum):
    EXACT_IDENTIFIER = "exact_identifier"
    NORMALIZED_NAME = "normalized_name"
    FUZZY_NAME = "fuzzy_name"
    STRUCTURAL_CONTEXT = "structural_context"


@dataclass(frozen=True)
class IdentityEvidence:
    kind: IdentityEvidenceKind
    detail: str
    score: float

    def as_dict(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "detail": self.detail, "score": self.score}


@dataclass
class EntityResolutionCandidate:
    """Review-only identity pair; construction never confirms a merge."""

    id: str
    entity_a: str
    entity_b: str
    evidence: list[IdentityEvidence]
    confidence: float
    status: str = "candidate"
    created_at: str = ""


def pair_id(a: str, b: str) -> str:
    """Content-addressed, order-independent identity pair ID."""
    lo, hi = sorted((a, b))
    digest = hashlib.sha256(f"{lo}:{hi}".encode()).hexdigest()[:32]
    return f"identity_candidate:{digest}"


def applicable_rules(
    a_kind: str, b_kind: str, rules: Sequence[IdentityRuleLike]
) -> list[IdentityRuleLike]:
    return [rule for rule in rules if rule.applies(a_kind) or rule.applies(b_kind)]


def identifier_fields(rules: Sequence[IdentityRuleLike]) -> tuple[set[str], float]:
    fields: set[str] = set()
    score = 0.98
    for rule in rules:
        if rule.identifier_fields:
            fields.update(rule.identifier_fields)
            score = max(score, rule.exact_identifier_score)
    if not fields:
        fields = set(GENERIC_IDENTIFIER_FIELDS)
    return fields, score


def exact_identifier_evidence(
    a: EntityRecord, b: EntityRecord, rules: Sequence[IdentityRuleLike]
) -> IdentityEvidence | None:
    fields, score = identifier_fields(rules)
    for field_name in sorted(fields):
        a_value, b_value = a.identifiers.get(field_name), b.identifiers.get(field_name)
        if a_value and b_value and a_value == b_value:
            return IdentityEvidence(
                kind=IdentityEvidenceKind.EXACT_IDENTIFIER,
                detail=f"{field_name}={a_value}",
                score=score,
            )
    return None


def name_evidence(
    a: EntityRecord, b: EntityRecord, score: float, tier: str
) -> IdentityEvidence:
    """Classify an already computed name-resolution result as evidence."""
    kind = (
        IdentityEvidenceKind.NORMALIZED_NAME
        if tier == "exact"
        else IdentityEvidenceKind.FUZZY_NAME
    )
    return IdentityEvidence(
        kind=kind, detail=f"{a.name!r} ~ {b.name!r} ({tier})", score=float(score)
    )


def structural_evidence(
    a_neighbors: set[str], b_neighbors: set[str]
) -> IdentityEvidence | None:
    """Jaccard evidence from caller-supplied neighbor sets, when overlapping."""
    if not a_neighbors or not b_neighbors:
        return None
    intersection = len(a_neighbors & b_neighbors)
    union = len(a_neighbors | b_neighbors)
    if union == 0 or intersection == 0:
        return None
    return IdentityEvidence(
        kind=IdentityEvidenceKind.STRUCTURAL_CONTEXT,
        detail=f"{intersection}/{union} shared neighbors",
        score=intersection / union,
    )


def candidate_threshold(
    rules: Sequence[IdentityRuleLike], min_confidence: float
) -> float:
    thresholds = [
        rule.min_confidence_to_flag
        for rule in rules
        if rule.identifier_fields or rule.name_fields
    ]
    return min(thresholds) if thresholds else min_confidence


def aggregate_confidence(confidences: Iterable[float]) -> float:
    """Combine independent evidence by the product complement of each score."""
    complement = 1.0
    for confidence in confidences:
        bounded = max(0.0, min(1.0, confidence)) if math.isfinite(confidence) else 0.0
        complement *= 1.0 - bounded
    return 1.0 - complement


def derive_candidate(
    a: EntityRecord,
    b: EntityRecord,
    evidence: list[IdentityEvidence],
    rules: Sequence[IdentityRuleLike],
    min_confidence: float,
    created_at: str,
) -> EntityResolutionCandidate | None:
    """Return a review candidate only when actual evidence clears the threshold."""
    if not evidence or a.id == b.id:
        return None
    confidence = aggregate_confidence(item.score for item in evidence)
    if confidence < candidate_threshold(rules, min_confidence):
        return None
    return EntityResolutionCandidate(
        id=pair_id(a.id, b.id),
        entity_a=a.id,
        entity_b=b.id,
        evidence=evidence,
        confidence=confidence,
        created_at=created_at,
    )
