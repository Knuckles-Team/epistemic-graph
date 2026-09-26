"""Pure concept identity and feature-match decisions for assimilation clients."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from typing import Any, Literal

Verdict = Literal["covered", "related", "unrelated"]

SEMANTIC_CONCEPT_ID = (
    r"[A-Z]{2}-(?:ORCH|KG|AHE|ECO|OS|GBOT)\."
    r"[A-Z0-9](?:[A-Z0-9._-]*[A-Z0-9_-])?"
)
LEGACY_CONCEPT_ID = r"[A-Z]{2,6}-\d+(?:\.\d+[a-z]?|-\d+)?"
CONCEPT_ID_RE = re.compile(
    rf"\b(({SEMANTIC_CONCEPT_ID})|({LEGACY_CONCEPT_ID}))\b",
    re.IGNORECASE,
)

COVERED_COSINE = 0.82
RELATED_COSINE = 0.6


@dataclass
class Match:
    """One adjudicated feature to concept candidate."""

    concept_id: str
    cosine: float
    verdict: Verdict
    confidence: float
    score: float
    method: str
    rationale: str = ""


@dataclass
class FeatureMatch:
    """The match decision for one feature."""

    feature_id: str
    decision: Verdict
    best: Match | None
    novelty_score: float
    matches: list[Match] = field(default_factory=list)


def canonical_concept_id(raw: str) -> str:
    """Normalize a semantic or durable historical concept ID token."""
    value = str(raw).strip()
    if re.fullmatch(SEMANTIC_CONCEPT_ID, value, re.IGNORECASE):
        return value.upper()
    match = re.match(r"^([A-Za-z]{2,6})-(\d+)[.\-](\d+[a-z]?)$", value)
    if match:
        return f"{match.group(1).upper()}-{match.group(2)}.{match.group(3)}"
    bare = re.match(r"^([A-Za-z]{2,6})-(\d+[a-z]?)$", value)
    return f"{bare.group(1).upper()}-{bare.group(2)}" if bare else value.upper()


def is_concept_id(value: str) -> bool:
    return bool(
        re.fullmatch(SEMANTIC_CONCEPT_ID, value, re.IGNORECASE)
        or re.fullmatch(LEGACY_CONCEPT_ID, value, re.IGNORECASE)
    )


def concept_key(node_id: str, data: dict[str, Any]) -> str | None:
    """Return the canonical ID represented by a concept node."""
    for candidate in (
        data.get("concept_id"),
        data.get("id"),
        node_id,
        data.get("name", ""),
    ):
        if candidate:
            key = canonical_concept_id(str(candidate))
            if is_concept_id(key):
                return key
    return None


def feature_refs(node_id: str, data: dict[str, Any]) -> list[str]:
    """Read a feature's declared identity; do not scan cited body prose."""
    refs: list[str] = []

    def add(value: Any) -> None:
        for match in CONCEPT_ID_RE.findall(str(value).upper()):
            token = match[0] if isinstance(match, tuple) else match
            candidate = canonical_concept_id(token)
            if is_concept_id(candidate) and candidate not in refs:
                refs.append(candidate)

    concept_ids = data.get("concept_ids")
    if isinstance(concept_ids, list | tuple):
        for concept_id in concept_ids:
            add(concept_id)
    add(node_id)
    add(data.get("name", ""))
    add(data.get("title", ""))
    return refs


def parse_judge(text: str) -> tuple[Verdict, float, str]:
    """Parse a judge response, degrading invalid responses to unrelated."""
    if not text:
        return "unrelated", 0.0, ""
    match = re.search(r"\{.*\}", text, re.DOTALL)
    raw = match.group(0) if match else text
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return "unrelated", 0.0, ""
        verdict = str(data.get("verdict", "")).strip().lower()
        if verdict not in ("covered", "related", "unrelated"):
            verdict = "unrelated"
        confidence = float(data.get("confidence", 0.0))
        if not math.isfinite(confidence):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        return verdict, confidence, str(data.get("why", ""))[:240]  # type: ignore[return-value]
    except (json.JSONDecodeError, ValueError, TypeError):
        lower = text.lower()
        for verdict in ("covered", "related", "unrelated"):
            if verdict in lower:
                return verdict, 0.5, ""
        return "unrelated", 0.0, ""


def explicit_id_match(
    feature_id: str, feature_data: dict[str, Any], concept_by_key: dict[str, str]
) -> FeatureMatch | None:
    """Highest precision match when declared feature identity exists in registry."""
    for ref in feature_refs(feature_id, feature_data):
        concept_id = concept_by_key.get(ref)
        if concept_id:
            match = Match(
                concept_id, 1.0, "covered", 1.0, 1.0, "id", "declared concept id"
            )
            return FeatureMatch(feature_id, "covered", match, 0.0, [match])
    return None


def cosine_verdict(cosine: float) -> tuple[Verdict, float, str]:
    """Deterministic no-LLM verdict from cosine similarity."""
    if cosine >= COVERED_COSINE:
        return "covered", cosine, "high embedding similarity"
    if cosine >= RELATED_COSINE:
        return "related", cosine, "moderate embedding similarity"
    return "unrelated", 0.0, ""


def candidate_match(
    concept_id: str,
    cosine: float,
    verdict: Verdict,
    confidence: float,
    method: str,
    rationale: str,
) -> Match:
    """Fuse retrieval similarity with a judged relationship."""
    score = round(0.4 * cosine + 0.6 * confidence, 6) if verdict != "unrelated" else 0.0
    return Match(
        concept_id, round(cosine, 6), verdict, confidence, score, method, rationale
    )


def decide_feature(
    feature_id: str, matches: list[Match], judge_accept: float
) -> FeatureMatch:
    """Prefer accepted covered matches, then related matches, then no match."""
    covered = [
        match
        for match in matches
        if match.verdict == "covered" and match.confidence >= judge_accept
    ]
    related = [match for match in matches if match.verdict == "related"]
    if covered:
        best = max(covered, key=lambda match: match.score)
        return FeatureMatch(
            feature_id, "covered", best, round(1.0 - best.score, 6), matches
        )
    if related:
        best = max(related, key=lambda match: match.score)
        return FeatureMatch(
            feature_id, "related", best, round(1.0 - 0.5 * best.score, 6), matches
        )
    return FeatureMatch(feature_id, "unrelated", None, 1.0, matches)


def normalized_or_none(fvec: list[float]) -> list[float] | None:
    """L2-normalize ``fvec``, or ``None`` when it has zero magnitude."""
    fnorm = math.sqrt(sum(value * value for value in fvec))
    if not fnorm:
        return None
    return [value / fnorm for value in fvec]


def cosine_scores(
    normalized: list[float], concept_vecs: list[tuple[str, list[float]]]
) -> list[tuple[str, float]]:
    """Cosine of each concept vector against an already-normalized ``normalized``."""
    from . import numeric

    ids = [cid for cid, _ in concept_vecs]
    vectors = [list(vector) for _, vector in concept_vecs]
    # Rank the complete bounded concept batch with one native matmul.  The
    # previous loop crossed the numeric boundary once per concept vector.
    dots = numeric.matmul(vectors, [[value] for value in normalized])
    norms = [math.sqrt(sum(value * value for value in vector)) for vector in vectors]
    return [
        (cid, float(row[0]) / norm)
        for cid, row, norm in zip(ids, dots, norms, strict=True)
        if norm
    ]


def top_k_cosine(
    fvec: list[float],
    concept_vecs: list[tuple[str, list[float]]],
    k: int,
    threshold: float,
) -> list[tuple[str, float]]:
    """Top-k concepts by cosine ≥ threshold over the native list boundary."""
    if not concept_vecs:
        return []
    normalized = normalized_or_none(fvec)
    if normalized is None:
        return []
    scored = cosine_scores(normalized, concept_vecs)
    scored.sort(key=lambda t: (-t[1], t[0]))
    return [
        (concept_id, score) for concept_id, score in scored[:k] if score >= threshold
    ]
