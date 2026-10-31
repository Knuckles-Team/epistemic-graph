"""Exhaustive reference answers for assembly items.

Independent of the generator: it knows nothing about roles or how an item was
built. Subsets are enumerated by size, so the first size with a feasible
assembly is provably the fewest-components size, and every feasible subset of
that size is ranked by the remaining objective keys.
"""

from __future__ import annotations

from collections.abc import Sequence
from itertools import combinations

from . import vocabulary as vocab
from .assembly import Candidate, Constraints, PlanItem

ObjectiveKey = tuple[int, int, int, int]


def _covers(members: Sequence[Candidate], capability: str) -> bool:
    return any(
        vocab.satisfies(provided, capability)
        for member in members
        for provided in member.classification
    )


def _closed_under_requires(members: Sequence[Candidate]) -> bool:
    chosen = {member.component_id for member in members}
    return all(set(member.requires) <= chosen for member in members)


def _within_budget(members: Sequence[Candidate], constraints: Constraints) -> bool:
    if constraints.cost_budget_micros is None:
        return True
    costs = [member.cost_micros for member in members]
    if constraints.strict_budget and any(cost is None for cost in costs):
        return False
    known = sum(cost for cost in costs if cost is not None)
    return known <= constraints.cost_budget_micros


def _model_rules(members: Sequence[Candidate], constraints: Constraints) -> bool:
    models = [member for member in members if member.kind == "model_profile"]
    if constraints.exactly_one_model and len(models) != 1:
        return False
    has_tools = any(member.kind == "tool" for member in members)
    return not (has_tools and any(model.supports_tools is False for model in models))


def feasible(members: Sequence[Candidate], item: PlanItem) -> bool:
    needed = set(item.required_capabilities)
    needed.update(cap for member in members for cap in member.required_capabilities)
    return (
        all(_covers(members, capability) for capability in needed)
        and _closed_under_requires(members)
        and _within_budget(members, item.constraints)
        and _model_rules(members, item.constraints)
    )


def objective(members: Sequence[Candidate]) -> ObjectiveKey:
    costs = [member.cost_micros for member in members]
    unknown = sum(cost is None for cost in costs)
    known = sum(cost for cost in costs if cost is not None)
    return (len(members), unknown, known, sum(member.p95_ms for member in members))


def universe(item: PlanItem) -> tuple[Candidate, ...]:
    return tuple(c for c in item.candidates if c.lifecycle == "published")


def _ranked_feasible_subsets(
    pool: Sequence[Candidate], size: int, item: PlanItem
) -> list[tuple[ObjectiveKey, tuple[str, ...]]]:
    """Every feasible ``size``-member subset of ``pool``, as (objective, sorted ids)."""
    ranked: list[tuple[ObjectiveKey, tuple[str, ...]]] = []
    for subset in combinations(pool, size):
        if not feasible(subset, item):
            continue
        ids = tuple(sorted(c.component_id for c in subset))
        ranked.append((objective(subset), ids))
    return ranked


def optimal_assemblies(
    item: PlanItem,
) -> tuple[ObjectiveKey, tuple[tuple[str, ...], ...]] | None:
    """The best objective and every assembly achieving it, or None if infeasible."""
    pool = universe(item)
    for size in range(1, len(pool) + 1):
        ranked = _ranked_feasible_subsets(pool, size, item)
        if ranked:
            best = min(key for key, _ in ranked)
            return best, tuple(sorted(ids for key, ids in ranked if key == best))
    return None


def uncoverable(item: PlanItem) -> tuple[str, ...]:
    """Required capabilities no published candidate can satisfy at all."""
    pool = universe(item)
    return tuple(cap for cap in item.required_capabilities if not _covers(pool, cap))
