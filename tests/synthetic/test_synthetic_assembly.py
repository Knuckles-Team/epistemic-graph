"""Assembly gold set: every planted answer re-derived by exhaustive search."""

from __future__ import annotations

from collections import Counter

import pytest
from pydantic import ValidationError

from epistemic_graph.testing.synthetic import assembly_oracle as oracle
from epistemic_graph.testing.synthetic.assembly import (
    Abstained,
    Constraints,
    GoldSet,
    PlanItem,
    Solved,
)
from epistemic_graph.testing.synthetic.assembly_generate import (
    DEFAULT_MIX,
    generate_gold_set,
)

pytestmark = pytest.mark.no_engine

SEEDS = (0, 1, 2)


@pytest.fixture(scope="module", params=SEEDS)
def gold_set(request: pytest.FixtureRequest) -> GoldSet:
    return generate_gold_set(request.param)


def _with_candidates(item: PlanItem, **update: object) -> PlanItem:
    candidates = tuple(c.model_copy(update=update) for c in item.candidates)
    return PlanItem.model_validate(
        item.model_dump() | {"candidates": tuple(c.model_dump() for c in candidates)}
    )


def test_mix_is_the_ruled_fifty_item_set(gold_set: GoldSet) -> None:
    assert Counter(i.kind for i in gold_set.items) == dict(DEFAULT_MIX)
    assert len(gold_set.items) == 50
    assert (
        generate_gold_set(0).model_dump_json() == generate_gold_set(0).model_dump_json()
    )


def test_oracle_reproduces_every_planted_answer(gold_set: GoldSet) -> None:
    for item in gold_set.items:
        found = oracle.optimal_assemblies(item)
        if isinstance(item.expected, Solved):
            assert found == (item.expected.objective, item.expected.acceptable), (
                item.item_id
            )
        else:
            assert found is None, item.item_id
            assert oracle.uncoverable(item) == item.expected.uncovered, item.item_id


def test_answer_kinds_match_item_kinds(gold_set: GoldSet) -> None:
    for item in gold_set.items:
        expected = item.expected
        if item.kind == "deterministic":
            assert isinstance(expected, Solved) and len(expected.acceptable) == 1
        elif item.kind == "acceptability":
            assert isinstance(expected, Solved) and len(expected.acceptable) == 2
        else:
            assert isinstance(expected, Abstained)


def test_every_distractor_family_appears(gold_set: GoldSet) -> None:
    roles = {c.role for item in gold_set.items for c in item.candidates}
    assert roles >= {
        "singleton",
        "near_cost",
        "unknown_cost",
        "requires_extra",
        "needs_extra_capability",
        "injection",
        "broader_only",
        "foreign_claim",
        "super_cover",
        "twin",
        "model",
        "inactive",
    }


def test_unknown_cost_is_never_zero(gold_set: GoldSet) -> None:
    """Pricing undeclared cost at zero must change at least one planted answer."""
    changed = 0
    for item in gold_set.items:
        if not isinstance(item.expected, Solved) or item.constraints.strict_budget:
            continue
        priced = _repriced(item)
        changed += oracle.optimal_assemblies(priced) != oracle.optimal_assemblies(item)
    assert changed > 0


def _repriced(item: PlanItem) -> PlanItem:
    candidates = tuple(
        (
            c.model_copy(update={"cost_micros": 0}) if c.cost_micros is None else c
        ).model_dump()
        for c in item.candidates
    )
    return PlanItem.model_validate(item.model_dump() | {"candidates": candidates})


def test_descriptions_never_change_an_answer(gold_set: GoldSet) -> None:
    for item in gold_set.items[:10]:
        neutral = _with_candidates(item, summary="neutral")
        assert oracle.optimal_assemblies(neutral) == oracle.optimal_assemblies(item)


def test_retiring_a_gold_member_changes_the_answer(gold_set: GoldSet) -> None:
    for item in (i for i in gold_set.items if i.kind == "deterministic"):
        gold = set(item.gold)
        candidates = tuple(
            c.model_dump()
            | (
                {"lifecycle": "retired"}
                if c.component_id in gold and c.kind == "tool"
                else {}
            )
            for c in item.candidates
        )
        retired = PlanItem.model_validate(
            item.model_dump() | {"candidates": candidates}
        )
        found = oracle.optimal_assemblies(retired)
        assert isinstance(item.expected, Solved), item.item_id
        assert found is None or found[1] != item.expected.acceptable, item.item_id


def test_strict_budget_requires_a_budget() -> None:
    with pytest.raises(ValidationError):
        Constraints(strict_budget=True)
