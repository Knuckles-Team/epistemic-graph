"""Seeded construction of assembly items: gold first, then worse distractors."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from . import vocabulary as vocab
from ._model import Provenance
from ._rng import SeededStream
from .assembly import (
    GENERATOR,
    GENERATOR_VERSION,
    Abstained,
    Candidate,
    Constraints,
    Distractor,
    GoldSet,
    ItemKind,
    Lifecycle,
    PlanItem,
    Solved,
)
from .catalog import FOREIGN_NAMESPACE, INJECTION_SUMMARY

_LEAVES = vocab.leaves(vocab.CAPABILITY_ROOT)
_TASKS = vocab.under(vocab.TASK_ROOT)
# The ruled 2.27.x gold-set size (DECIDE-LAYER-DESIGN §14 item 5), by item kind.
DEFAULT_MIX: tuple[tuple[ItemKind, int], ...] = (
    ("deterministic", 30),
    ("acceptability", 8),
    ("infeasible_uncovered", 6),
    ("infeasible_budget", 3),
    ("unknown_cost_strict", 3),
)


@dataclass
class _Builder:
    stream: SeededStream
    item_id: str
    candidates: list[Candidate] = field(default_factory=list)

    def add(self, role: Distractor, **facts: object) -> Candidate:
        name = f"synthetic/assembly/{self.item_id}/{len(self.candidates):02d}-{role}"
        facts.setdefault("summary", f"Synthetic {role} candidate.")
        facts.setdefault("p95_ms", self.stream.between(10, 200))
        candidate = Candidate.model_validate(
            {"component_id": name, "kind": "tool", "role": role, **facts}
        )
        self.candidates.append(candidate)
        return candidate


def _provider(stream: SeededStream, capability: str) -> str:
    """A leaf that satisfies ``capability``: itself, or one of its descendants."""
    below = vocab.leaves(capability)
    return stream.choice(below) if below else capability


def _terms(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


def _requirements(
    stream: SeededStream, use_task: bool
) -> tuple[str | None, tuple[str, ...]]:
    if use_task:
        task = stream.choice(_TASKS)
        return task, _terms(vocab.capabilities_for_task(task))
    return None, _terms(stream.sample(_LEAVES, stream.between(2, 4)))


def _groups(stream: SeededStream, required: Sequence[str]) -> list[list[str]]:
    order = stream.shuffled(required)
    count = stream.between(1, min(3, len(order)))
    cuts = sorted(stream.sample(range(1, len(order)), count - 1))
    bounds = [0, *cuts, len(order)]
    # Pairwise consecutive-bound iteration: `bounds[1:]` is always one shorter than
    # `bounds` by construction, so the length mismatch is intentional (strict=False).
    return [order[start:end] for start, end in zip(bounds, bounds[1:], strict=False)]


@dataclass
class _Gold:
    members: list[Candidate]
    groups: list[list[str]]
    providers: dict[str, str]

    @property
    def cost(self) -> int:
        return sum(member.cost_micros or 0 for member in self.members)


def _plant_gold(builder: _Builder, required: Sequence[str]) -> _Gold:
    stream = builder.stream
    providers = {cap: _provider(stream, cap) for cap in required}
    groups = _groups(stream, required)
    members = [
        builder.add(
            "gold",
            classification=_terms([providers[cap] for cap in group]),
            cost_micros=stream.between(1_000, 20_000),
        )
        for group in groups
    ]
    return _Gold(members, groups, providers)


def _singletons(builder: _Builder, gold: _Gold) -> None:
    for group, member in zip(gold.groups, gold.members, strict=True):
        for cap in group:
            # Alone in its group: same size, strictly dearer. Otherwise any use of
            # singletons needs more components, so it may be cheap.
            dearer = (member.cost_micros or 0) + builder.stream.between(1, 500)
            builder.add(
                "singleton",
                classification=(gold.providers[cap],),
                cost_micros=dearer if len(group) == 1 else 1,
            )


def _cheaper(member: Candidate) -> int:
    return max(0, (member.cost_micros or 0) - 1)


def _group_rivals(builder: _Builder, gold: _Gold, required: Sequence[str]) -> None:
    """Rivals for one gold member that each lose on exactly one objective key."""
    stream = builder.stream
    index = stream.below(len(gold.members))
    member = gold.members[index]
    cover = member.classification
    builder.add(
        "near_cost",
        classification=cover,
        cost_micros=(member.cost_micros or 0) + 1,
        p95_ms=max(0, member.p95_ms - 1),
    )
    builder.add("unknown_cost", classification=cover, p95_ms=0)
    helper = builder.add("helper", cost_micros=1)
    builder.add(
        "requires_extra",
        classification=cover,
        cost_micros=_cheaper(member),
        requires=(helper.component_id,),
    )
    # The extra capability must satisfy no requirement, or its cheap helper would
    # become a legitimate provider and change the planted answer.
    extra = stream.choice(
        [
            leaf
            for leaf in _LEAVES
            if not any(vocab.satisfies(leaf, r) for r in required)
        ]
    )
    builder.add("helper", classification=(extra,), cost_micros=1)
    builder.add(
        "needs_extra_capability",
        classification=cover,
        required_capabilities=(extra,),
        cost_micros=_cheaper(member),
    )
    builder.add(
        "injection",
        classification=cover,
        cost_micros=(member.cost_micros or 0) + 7,
        summary=INJECTION_SUMMARY,
    )


def _claim_traps(builder: _Builder, required: Sequence[str], gold: _Gold) -> None:
    for cap in required:
        parent = vocab.ancestors(gold.providers[cap])[0]
        if parent not in required and parent != vocab.CAPABILITY_ROOT:
            builder.add("broader_only", classification=(parent,), cost_micros=1)
            break
    leaf = gold.providers[required[0]].rsplit("/", 1)[-1]
    builder.add(
        "foreign_claim",
        declared_capabilities=(FOREIGN_NAMESPACE + leaf,),
        cost_micros=1,
    )


def _super_cover(builder: _Builder, gold: _Gold, constraints: Constraints) -> None:
    if len(gold.members) < 2:
        return
    cover = _terms([leaf for member in gold.members for leaf in member.classification])
    if constraints.strict_budget and constraints.cost_budget_micros is not None:
        builder.add(
            "super_cover",
            classification=cover,
            cost_micros=constraints.cost_budget_micros + 1,
        )
        return
    lifecycle: Lifecycle = builder.stream.choice(("retired", "withdrawn"))
    builder.add("super_cover", classification=cover, cost_micros=1, lifecycle=lifecycle)


def _models(builder: _Builder) -> Candidate:
    """Tool support decides: the cheapest model cannot drive the chosen tools."""
    stream = builder.stream
    price = stream.between(2_000, 9_000)
    builder.add(
        "model", kind="model_profile", cost_micros=price - 1, supports_tools=False
    )
    builder.add(
        "model", kind="model_profile", cost_micros=price + 5, supports_tools=True
    )
    return builder.add(
        "gold", kind="model_profile", cost_micros=price, supports_tools=True
    )


def _solved(members: Sequence[Candidate], twins: Sequence[tuple[str, str]]) -> Solved:
    ids = sorted(m.component_id for m in members)
    options = [tuple(ids)]
    for twin, replaced in twins:
        options.append(tuple(sorted([*(i for i in ids if i != replaced), twin])))
    return Solved(
        acceptable=tuple(sorted(options)),
        objective=(
            len(members),
            0,
            sum(m.cost_micros or 0 for m in members),
            sum(m.p95_ms for m in members),
        ),
    )


def _item(
    builder: _Builder,
    kind: ItemKind,
    requirement: tuple[str | None, tuple[str, ...]],
    constraints: Constraints,
    expected: Solved | Abstained,
) -> PlanItem:
    task, required = requirement
    return PlanItem(
        item_id=builder.item_id,
        kind=kind,
        task=task,
        required_capabilities=required,
        constraints=constraints,
        candidates=tuple(sorted(builder.candidates, key=lambda c: c.component_id)),
        expected=expected,
    )


def _twin(builder: _Builder, gold: _Gold) -> tuple[str, str]:
    original = builder.stream.choice(gold.members)
    copied = original.model_dump(include={"classification", "cost_micros", "p95_ms"})
    return builder.add("twin", **copied).component_id, original.component_id


def solvable_item(stream: SeededStream, item_id: str, kind: ItemKind) -> PlanItem:
    """A deterministic or acceptability item carrying every distractor family."""
    builder = _Builder(stream, item_id)
    requirement = _requirements(stream, use_task=stream.chance(1, 2))
    required = requirement[1]
    strict = stream.chance(1, 2)
    one_model = stream.chance(1, 3)
    gold = _plant_gold(builder, required)
    members = list(gold.members)
    if one_model:
        members.append(_models(builder))
    total = sum(m.cost_micros or 0 for m in members)
    constraints = Constraints(
        cost_budget_micros=total + stream.between(0, 500) if strict else None,
        strict_budget=strict,
        exactly_one_model=one_model,
    )
    _singletons(builder, gold)
    _group_rivals(builder, gold, required)
    _claim_traps(builder, required, gold)
    _super_cover(builder, gold, constraints)
    twins = [_twin(builder, gold)] if kind == "acceptability" else []
    return _item(builder, kind, requirement, constraints, _solved(members, twins))


def uncovered_item(stream: SeededStream, item_id: str) -> PlanItem:
    """Some requirements have only claims, parents or inactive providers."""
    builder = _Builder(stream, item_id)
    required = _terms(stream.sample(_LEAVES, 3))
    missing = _terms(stream.sample(required, stream.between(1, 2)))
    for capability in required:
        if capability not in missing:
            builder.add("gold", classification=(capability,), cost_micros=1_000)
            continue
        name = capability.rsplit("/", 1)[-1]
        inactive: Lifecycle = stream.choice(("retired", "withdrawn"))
        builder.add("inactive", classification=(capability,), lifecycle=inactive)
        builder.add("broader_only", classification=(vocab.ancestors(capability)[0],))
        builder.add("foreign_claim", declared_capabilities=(FOREIGN_NAMESPACE + name,))
    expected = Abstained(reasons=("uncovered_capability",), uncovered=missing)
    return _item(
        builder, "infeasible_uncovered", (None, required), Constraints(), expected
    )


def budget_item(stream: SeededStream, item_id: str) -> PlanItem:
    """Every candidate that covers anything costs more than the whole budget."""
    builder = _Builder(stream, item_id)
    required = _terms(stream.sample(_LEAVES, stream.between(2, 3)))
    budget = stream.between(1_000, 5_000)
    builder.add("gold", classification=required, cost_micros=budget + 1)
    for capability in required:
        price = budget + stream.between(1, 1_000)
        builder.add("singleton", classification=(capability,), cost_micros=price)
    builder.add("helper", cost_micros=1)
    constraints = Constraints(cost_budget_micros=budget, strict_budget=True)
    expected = Abstained(reasons=("infeasible",))
    return _item(builder, "infeasible_budget", (None, required), constraints, expected)


def unknown_cost_item(stream: SeededStream, item_id: str) -> PlanItem:
    """One requirement is served only by components that declare no cost."""
    builder = _Builder(stream, item_id)
    required = _terms(stream.sample(_LEAVES, 2))
    known, unknown = required
    builder.add("gold", classification=(known,), cost_micros=1_000)
    for _ in range(stream.between(1, 2)):
        builder.add("unknown_cost", classification=(unknown,))
    constraints = Constraints(cost_budget_micros=10**9, strict_budget=True)
    expected = Abstained(reasons=("unknown_fact",))
    return _item(
        builder, "unknown_cost_strict", (None, required), constraints, expected
    )


_BUILDERS = {
    "deterministic": lambda s, i: solvable_item(s, i, "deterministic"),
    "acceptability": lambda s, i: solvable_item(s, i, "acceptability"),
    "infeasible_uncovered": uncovered_item,
    "infeasible_budget": budget_item,
    "unknown_cost_strict": unknown_cost_item,
}


def generate_gold_set(
    seed: int, mix: Sequence[tuple[ItemKind, int]] = DEFAULT_MIX
) -> GoldSet:
    """The ruled ~50-item synthetic gold set: typed requirements to answers."""
    stream = SeededStream(seed, GENERATOR)
    items = [
        _BUILDERS[kind](stream.child(f"{kind}-{n}"), f"{kind}-{n:02d}")
        for kind, count in mix
        for n in range(count)
    ]
    provenance = Provenance(
        generator=GENERATOR, generator_version=GENERATOR_VERSION, seed=seed
    )
    return GoldSet(provenance=provenance, items=tuple(items))
