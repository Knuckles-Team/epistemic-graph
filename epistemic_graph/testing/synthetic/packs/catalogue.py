"""The complete list of planted pack defects."""

from __future__ import annotations

from .malformed import MalformedPack, base_spec
from .malformed_bodies import BODY_MUTATIONS
from .malformed_index import INDEX_MUTATIONS
from .malformed_semantics import SEMANTIC_MUTATIONS


def all_malformed(seed: int = 0) -> tuple[MalformedPack, ...]:
    """Every planted defect, ordered by rule then variant."""
    spec = base_spec(seed)
    mutations = (*INDEX_MUTATIONS, *BODY_MUTATIONS, *SEMANTIC_MUTATIONS)
    cases = [mutation(spec) for mutation in mutations]
    return tuple(sorted(cases, key=lambda c: (int(c.rule[1:]), c.variant)))
