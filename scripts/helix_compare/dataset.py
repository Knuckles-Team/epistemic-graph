"""The seeded benchmark corpus and its pinned digest.

Documents carry a category, a year, a text body drawn from a Zipf vocabulary and
a unit embedding clustered by category. Every document cites up to
``citations_per_doc`` distinct EARLIER documents, so the citation graph is a DAG:
no traversal can walk back to its start, which keeps "k hops" meaning the same
thing in both engines' traversal semantics.

The corpus is a pure function of its :class:`DatasetSpec`. Its identity is the
sha256 of the canonical JSON lines (sorted keys, no whitespace); the spec file
pins that digest and the harness refuses to run on a corpus that does not match.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import random
from collections.abc import Iterator, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetSpec:
    seed: int
    docs: int
    citations_per_doc: int
    categories: int
    vocabulary: int
    words_per_doc: int
    dim: int
    year_min: int
    year_max: int

    @classmethod
    def from_json(cls, value: dict[str, int]) -> DatasetSpec:
        return cls(**{name: int(value[name]) for name in cls.__dataclass_fields__})


@dataclass(frozen=True)
class Doc:
    key: str
    category: str
    year: int
    text: str
    embedding: tuple[float, ...]


@dataclass(frozen=True)
class Citation:
    source: str
    target: str
    weight: float


@dataclass(frozen=True)
class Dataset:
    spec: DatasetSpec
    docs: tuple[Doc, ...]
    citations: tuple[Citation, ...]

    def canonical_lines(self) -> Iterator[bytes]:
        for doc in self.docs:
            yield _line({"doc": doc.__dict__})
        for citation in self.citations:
            yield _line({"cites": citation.__dict__})

    def digest(self) -> str:
        hasher = hashlib.sha256()
        for line in self.canonical_lines():
            hasher.update(line)
        return hasher.hexdigest()

    def logical_bytes(self) -> int:
        return sum(len(line) for line in self.canonical_lines())


def _line(value: object) -> bytes:
    text = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return text.encode("utf-8") + b"\n"


def word(rank: int) -> str:
    return f"w{rank:04d}"


def category(index: int) -> str:
    return f"c{index:02d}"


def _unit(values: list[float]) -> tuple[float, ...]:
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return tuple(round(value / norm, 4) for value in values)


def _centers(rng: random.Random, spec: DatasetSpec) -> list[list[float]]:
    return [
        [rng.gauss(0.0, 1.0) for _ in range(spec.dim)] for _ in range(spec.categories)
    ]


def zipf_cumulative(vocabulary: int) -> list[float]:
    """Cumulative Zipf(1) weights over the vocabulary ranks."""

    return list(itertools.accumulate(1.0 / (rank + 1) for rank in range(vocabulary)))


def _doc(
    rng: random.Random,
    spec: DatasetSpec,
    index: int,
    centers: list[list[float]],
    cumulative: Sequence[float],
) -> Doc:
    group = rng.randrange(spec.categories)
    ranks = rng.choices(
        range(spec.vocabulary), cum_weights=cumulative, k=spec.words_per_doc
    )
    noisy = [value + rng.gauss(0.0, 0.6) for value in centers[group]]
    return Doc(
        key=f"d{index:06d}",
        category=category(group),
        year=rng.randint(spec.year_min, spec.year_max),
        text=" ".join(word(rank) for rank in ranks),
        embedding=_unit(noisy),
    )


def _citations(rng: random.Random, spec: DatasetSpec, index: int) -> list[Citation]:
    targets = rng.sample(range(index), min(spec.citations_per_doc, index))
    return [
        Citation(f"d{index:06d}", f"d{target:06d}", round(rng.random(), 3))
        for target in sorted(targets)
    ]


def generate(spec: DatasetSpec) -> Dataset:
    """Build the corpus. Deterministic for a given spec and CPython's MT19937."""

    rng = random.Random(spec.seed)
    centers = _centers(rng, spec)
    cumulative = zipf_cumulative(spec.vocabulary)
    docs = tuple(
        _doc(rng, spec, index, centers, cumulative) for index in range(spec.docs)
    )
    citations = tuple(
        citation
        for index in range(spec.docs)
        for citation in _citations(rng, spec, index)
    )
    return Dataset(spec=spec, docs=docs, citations=citations)
