"""Deterministic hybrid ranking for bounded, already-authorized candidates.

This is the pure scoring core of AU's legacy HybridSearchScorer. Candidate
selection and tenant policy are deliberately outside this function: callers
must only provide rows the native engine has already authorized to read.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class HybridScoreConfig:
    semantic_weight: float = 0.72
    keyword_weight: float = 0.28
    min_semantic_score: float = 0.0
    min_keyword_score: float = 0.0
    min_combined_score: float = 0.1
    phrase_boost: float = 0.15
    top_k: int = 10


def split_compound_name(value: str) -> set[str]:
    """Split camelCase, PascalCase and snake_case into lexical terms."""
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", value)
    spaced = re.sub(r"([A-Z])([A-Z][a-z])", r"\1 \2", spaced)
    return {term for term in re.split(r"[\s_\-]+", spaced.lower()) if len(term) > 1}


def cosine_similarity(left: list[float], right: list[float]) -> float:
    """Return the legacy cosine score without a dependency on AU's numeric API."""
    if len(left) != len(right):
        raise ValueError("embedding dimensions differ")
    a = [float(value) for value in left]
    b = [float(value) for value in right]
    a_norm = math.sqrt(sum(value * value for value in a))
    b_norm = math.sqrt(sum(value * value for value in b))
    if a_norm == 0 or b_norm == 0:
        return 0.0
    return sum(x * y for x, y in zip(a, b, strict=True)) / (a_norm * b_norm)


def _symbol_coverage(symbols: list[str], terms: set[str]) -> tuple[float, list[str]]:
    matched: list[str] = []
    all_terms: set[str] = set()
    for symbol in symbols:
        symbol_terms = split_compound_name(symbol)
        if symbol_terms & terms:
            matched.append(symbol)
        all_terms.update(symbol_terms)
    coverage = sum(term in all_terms for term in terms) / len(terms) if terms else 0.0
    return coverage, matched


def _keyword_score(
    query: str,
    terms: set[str],
    text: str,
    symbols: list[str],
    config: HybridScoreConfig,
) -> tuple[float, list[str]]:
    if not terms:
        return 0.0, []
    text_terms = split_compound_name(text)
    text_coverage = sum(term in text_terms for term in terms) / len(terms)
    symbol_coverage, matched = (
        _symbol_coverage(symbols, terms) if symbols else (0.0, [])
    )
    phrase_boost = config.phrase_boost if query.strip().lower() in text.lower() else 0.0
    score = min(1.0, text_coverage * 0.65 + symbol_coverage * 0.2 + phrase_boost)
    return score, matched


def _combined_score(
    semantic: float, keyword: float, config: HybridScoreConfig
) -> float:
    total = config.semantic_weight + config.keyword_weight
    if total <= 0:
        return max(semantic, 0.0)
    return min(
        1.0,
        (config.semantic_weight * max(semantic, 0.0) + config.keyword_weight * keyword)
        / total,
    )


def score_documents(
    query: str,
    query_embedding: list[float],
    documents: list[dict[str, Any]],
    config: HybridScoreConfig | None = None,
) -> list[dict[str, Any]]:
    """Rank a bounded candidate list with the legacy hybrid scoring contract.

    The caller supplies only candidate rows already filtered by native policy;
    this function neither selects graph rows nor receives tenant assertions.
    It preserves input order for exact score ties, as Python's stable sort did
    in the AU implementation.
    """
    config = config or HybridScoreConfig()
    terms = split_compound_name(query)
    results: list[dict[str, Any]] = []
    for document in documents:
        embedding = document.get("embedding")
        semantic = (
            cosine_similarity(query_embedding, embedding)
            if embedding and query_embedding
            else 0.0
        )
        keyword, matched = _keyword_score(
            query,
            terms,
            document.get("text", ""),
            document.get("symbols", []),
            config,
        )
        combined = _combined_score(semantic, keyword, config)
        if max(semantic, 0.0) < config.min_semantic_score:
            continue
        if keyword < config.min_keyword_score or combined < config.min_combined_score:
            continue
        result = dict(document)
        result.update(
            semantic_score=round(semantic, 4),
            keyword_score=round(keyword, 4),
            combined_score=round(combined, 4),
            matched_symbols=matched,
        )
        results.append(result)
    results.sort(
        key=lambda item: (
            item["combined_score"],
            item["keyword_score"],
            item["semantic_score"],
        ),
        reverse=True,
    )
    return results[: config.top_k]
