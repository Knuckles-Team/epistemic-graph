"""The complete list of planted pack defects."""

from __future__ import annotations

from .malformed import MalformedPack, Mutation, base_spec
from .malformed_bodies import BODY_MUTATIONS
from .malformed_index import INDEX_MUTATIONS
from .malformed_semantics import SEMANTIC_MUTATIONS

#: Every planted-defect builder, one or more per rule G1-G21.
MUTATIONS: tuple[Mutation, ...] = (
    *INDEX_MUTATIONS,
    *BODY_MUTATIONS,
    *SEMANTIC_MUTATIONS,
)


def all_malformed(
    seed: int = 0, connector: str = "synthetic-mcp"
) -> tuple[MalformedPack, ...]:
    """Every planted defect, ordered by rule then variant."""
    spec = base_spec(seed, connector)
    cases = [mutation(spec) for mutation in MUTATIONS]
    return tuple(sorted(cases, key=lambda c: (int(c.rule[1:]), c.variant)))
