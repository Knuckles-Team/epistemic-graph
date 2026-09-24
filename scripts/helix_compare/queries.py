"""The seeded query set: one list of inputs per workload, identical for both engines.

Queries are drawn from their own RNG (the corpus seed + 1) so changing a
workload's size never perturbs the corpus. Ranked workloads only use inputs whose
eligible set holds at least ``k`` documents, so "top-k" means the same thing
whatever an engine does when fewer than ``k`` rows qualify.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass

from .dataset import Dataset, Doc, category, word
from .oracle import Oracle

TXN_PREFIX = "t"


@dataclass(frozen=True)
class WorkloadSpec:
    ops: int
    k: int = 10
    min_year: int = 0
    batch_docs: int = 0
    text_terms: int = 200


@dataclass(frozen=True)
class QuerySet:
    point_get: tuple[str, ...]
    one_hop: tuple[str, ...]
    filtered_hop: tuple[tuple[str, int], ...]
    # Each batch: new documents, each citing the existing document it copies.
    txn_batch: tuple[tuple[tuple[Doc, str], ...], ...]
    text_prefilter: tuple[tuple[str, str, int], ...]
    vector_prefilter: tuple[tuple[str, tuple[float, ...], int], ...]
    mixed: tuple[tuple[str, tuple[float, ...], int], ...]

    def digest(self) -> str:
        encoded = json.dumps(self.__dict__, sort_keys=True, default=_plain)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _plain(value: object) -> object:
    if isinstance(value, Doc):
        return value.__dict__
    raise TypeError(f"unexpected query value {value!r}")


def _vector(rng: random.Random, base: tuple[float, ...]) -> tuple[float, ...]:
    return tuple(round(value + rng.gauss(0.0, 0.05), 4) for value in base)


def _keys(
    rng: random.Random, dataset: Dataset, ops: int, low: int = 0
) -> tuple[str, ...]:
    return tuple(
        dataset.docs[rng.randrange(low, len(dataset.docs))].key for _ in range(ops)
    )


def _text_queries(
    rng: random.Random, dataset: Dataset, oracle: Oracle, spec: WorkloadSpec
) -> tuple[tuple[str, str, int], ...]:
    groups = dataset.spec.categories
    pool = [
        (category(group), word(rank))
        for group in range(groups)
        for rank in range(spec.text_terms)
        if len(oracle.text_eligible(category(group), word(rank))) >= spec.k
    ]
    return tuple((*rng.choice(pool), spec.k) for _ in range(spec.ops))


def _vector_queries(
    rng: random.Random, dataset: Dataset, spec: WorkloadSpec
) -> tuple[tuple[str, tuple[float, ...], int], ...]:
    picks = [dataset.docs[rng.randrange(len(dataset.docs))] for _ in range(spec.ops)]
    return tuple((doc.category, _vector(rng, doc.embedding), spec.k) for doc in picks)


def _mixed_queries(
    rng: random.Random, dataset: Dataset, oracle: Oracle, spec: WorkloadSpec
) -> tuple[tuple[str, tuple[float, ...], int], ...]:
    queries: list[tuple[str, tuple[float, ...], int]] = []
    while len(queries) < spec.ops:
        doc = dataset.docs[rng.randrange(len(dataset.docs) // 2, len(dataset.docs))]
        if len(oracle.reach(doc.key, 2)) >= spec.k:
            queries.append((doc.key, _vector(rng, doc.embedding), spec.k))
    return tuple(queries)


def _txn_batches(
    rng: random.Random, dataset: Dataset, spec: WorkloadSpec
) -> tuple[tuple[tuple[Doc, str], ...], ...]:
    def copy(op: int, index: int) -> tuple[Doc, str]:
        doc = dataset.docs[rng.randrange(len(dataset.docs))]
        key = f"{TXN_PREFIX}{op:05d}x{index:02d}"
        return Doc(key, doc.category, doc.year, doc.text, doc.embedding), doc.key

    return tuple(
        tuple(copy(op, index) for index in range(spec.batch_docs))
        for op in range(spec.ops)
    )


def generate(
    dataset: Dataset, oracle: Oracle, specs: dict[str, WorkloadSpec]
) -> QuerySet:
    rng = random.Random(dataset.spec.seed + 1)
    filtered = specs["filtered_hop"]
    return QuerySet(
        point_get=_keys(rng, dataset, specs["point_get"].ops),
        one_hop=_keys(rng, dataset, specs["one_hop"].ops, low=1),
        filtered_hop=tuple(
            (key, filtered.min_year) for key in _keys(rng, dataset, filtered.ops, low=1)
        ),
        txn_batch=_txn_batches(rng, dataset, specs["txn_batch"]),
        text_prefilter=_text_queries(rng, dataset, oracle, specs["text_prefilter"]),
        vector_prefilter=_vector_queries(rng, dataset, specs["vector_prefilter"]),
        mixed=_mixed_queries(rng, dataset, oracle, specs["mixed"]),
    )
