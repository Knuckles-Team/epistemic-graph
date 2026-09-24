"""Brute-force expected answers, and the checks both engines must pass.

Exact workloads (point get, hops, transactional batch) must match the corpus
exactly. Ranked workloads are checked on what both engines' contracts promise:
every hit satisfies the prefilter, the hit count is ``min(k, eligible)``, and
for vector ranking every returned hit is at least as similar as the oracle's
k-th best within :data:`SIMILARITY_TOLERANCE` (f32 storage vs f64 oracle). BM25
parameters and tokenization differ between engines, so a text hit's RANK is not
asserted -- only that it contains the query term inside the prefilter.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field

from .dataset import Dataset, Doc

SIMILARITY_TOLERANCE = 1e-4


def cosine(left: Sequence[float], right: Sequence[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    norm = math.sqrt(sum(a * a for a in left)) * math.sqrt(sum(b * b for b in right))
    return dot / norm if norm else 0.0


@dataclass
class Oracle:
    docs: dict[str, Doc] = field(default_factory=dict)
    out: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    by_category: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    @classmethod
    def of(cls, dataset: Dataset) -> Oracle:
        oracle = cls()
        for doc in dataset.docs:
            oracle.add_doc(doc)
        for citation in dataset.citations:
            oracle.out[citation.source].add(citation.target)
        return oracle

    def add_doc(self, doc: Doc) -> None:
        self.docs[doc.key] = doc
        self.by_category[doc.category].append(doc.key)

    def one_hop(self, key: str) -> set[str]:
        return set(self.out.get(key, ()))

    def filtered_hop(self, key: str, min_year: int) -> set[str]:
        return {n for n in self.one_hop(key) if self.docs[n].year > min_year}

    def reach(self, key: str, hops: int) -> set[str]:
        frontier: set[str] = {key}
        seen: set[str] = set()
        for _ in range(hops):
            frontier = {n for node in frontier for n in self.out.get(node, ())} - seen
            seen |= frontier
        return seen

    def text_eligible(self, category: str, term: str) -> list[str]:
        return [
            key
            for key in self.by_category[category]
            if term in self.docs[key].text.split()
        ]

    def kth_similarity(
        self, candidates: Sequence[str], vector: Sequence[float], k: int
    ) -> float:
        scores = sorted(
            (cosine(self.docs[key].embedding, vector) for key in candidates),
            reverse=True,
        )
        return scores[min(k, len(scores)) - 1] if scores else -1.0


def check_point_get(oracle: Oracle, key: str, got: dict[str, object]) -> str | None:
    doc = oracle.docs[key]
    expected = {"category": doc.category, "year": doc.year}
    actual = {"category": got.get("category"), "year": got.get("year")}
    return None if actual == expected else f"{key}: {actual} != {expected}"


def check_set(label: str, expected: set[str], got: set[str]) -> str | None:
    if expected == got:
        return None
    missing, extra = sorted(expected - got)[:5], sorted(got - expected)[:5]
    return f"{label}: missing {missing} extra {extra}"


def check_text(
    oracle: Oracle, query: tuple[str, str, int], got: Sequence[str]
) -> str | None:
    category, term, k = query
    eligible = set(oracle.text_eligible(category, term))
    wrong = [key for key in got if key not in eligible]
    if wrong:
        return f"text {category}/{term}: {wrong[:5]} fail the prefilter or term"
    if len(set(got)) != len(got) or len(got) != min(k, len(eligible)):
        return (
            f"text {category}/{term}: {len(got)} hits, expected {min(k, len(eligible))}"
        )
    return None


def check_ranked(
    oracle: Oracle,
    candidates: Sequence[str],
    vector: Sequence[float],
    k: int,
    got: Sequence[str],
) -> str | None:
    allowed = set(candidates)
    if any(key not in allowed for key in got):
        return "vector: a hit is outside the candidate set"
    if len(set(got)) != len(got) or len(got) != min(k, len(allowed)):
        return f"vector: {len(got)} hits, expected {min(k, len(allowed))}"
    floor = oracle.kth_similarity(candidates, vector, k) - SIMILARITY_TOLERANCE
    weak = [key for key in got if cosine(oracle.docs[key].embedding, vector) < floor]
    return f"vector: {weak[:3]} rank below the exact top-{k}" if weak else None
