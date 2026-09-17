"""(b) Assembly plans with planted gold answers.

Each item is a typed requirement set, constraints and a candidate universe.
Generation builds the optimal assembly FIRST and then adds distractors that are
each strictly worse under the ruled lexicographic objective (DECIDE-LAYER-DESIGN
§7.3): every required capability covered, then fewest components, then declared
cost, then declared p95 latency. Infeasible items name the abstention they must
produce. `assembly_oracle` re-derives every answer by exhaustive search.

Objective details the design leaves to the solver lane are fixed here as stated
assumptions, and the generator only plants items whose answer does not depend on
them: cost and p95 aggregate by sum, and a component with undeclared cost ranks
below every declared cost (it is never treated as zero).
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator, model_validator

from . import vocabulary as vocab
from ._model import ComponentFields, Provenance, SyntheticModel, sorted_unique

GENERATOR = "assembly"
GENERATOR_VERSION = 1
MAX_CANDIDATES = 24

Lifecycle = Literal["published", "retired", "withdrawn"]
ItemKind = Literal[
    "deterministic",
    "acceptability",
    "infeasible_uncovered",
    "infeasible_budget",
    "unknown_cost_strict",
]
AbstainReason = Literal["uncovered_capability", "infeasible", "unknown_fact"]
Distractor = Literal[
    "gold",
    "singleton",
    "super_cover",
    "broader_only",
    "foreign_claim",
    "injection",
    "near_cost",
    "unknown_cost",
    "requires_extra",
    "needs_extra_capability",
    "helper",
    "inactive",
    "twin",
    "model",
]


class Candidate(ComponentFields):
    """The typed facts a solver may read. Summaries are never solver input."""

    cost_micros: int | None = Field(default=None, ge=0)
    p95_ms: int = Field(ge=0)
    supports_tools: bool | None = None
    lifecycle: Lifecycle = "published"
    role: Distractor

    @field_validator("classification", "required_capabilities", "requires")
    @classmethod
    def _sorted(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return sorted_unique(value, "candidate terms")


class Constraints(SyntheticModel):
    cost_budget_micros: int | None = Field(default=None, ge=0)
    strict_budget: bool = False
    exactly_one_model: bool = False

    @model_validator(mode="after")
    def _strict_needs_budget(self) -> Constraints:
        if self.strict_budget and self.cost_budget_micros is None:
            raise ValueError("a strict budget needs a budget")
        return self


class Solved(SyntheticModel):
    outcome: Literal["solved"] = "solved"
    acceptable: tuple[tuple[str, ...], ...] = Field(min_length=1)
    objective: tuple[int, int, int, int]


class Abstained(SyntheticModel):
    outcome: Literal["abstained"] = "abstained"
    reasons: tuple[AbstainReason, ...] = Field(min_length=1)
    uncovered: tuple[str, ...] = ()


class PlanItem(SyntheticModel):
    item_id: str = Field(min_length=1)
    kind: ItemKind
    task: str | None = None
    required_capabilities: tuple[str, ...] = Field(min_length=1)
    constraints: Constraints
    candidates: tuple[Candidate, ...] = Field(min_length=1, max_length=MAX_CANDIDATES)
    expected: Solved | Abstained = Field(discriminator="outcome")

    @field_validator("required_capabilities")
    @classmethod
    def _native(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not all(vocab.satisfies(iri, vocab.CAPABILITY_ROOT) for iri in value):
            raise ValueError("required capabilities must be native capability terms")
        return sorted_unique(value, "required_capabilities")

    @model_validator(mode="after")
    def _candidate_ids_unique(self) -> PlanItem:
        sorted_unique(tuple(c.component_id for c in self.candidates), "candidate ids")
        return self

    @property
    def gold(self) -> tuple[str, ...]:
        """The single exact answer of a deterministic item."""
        if not isinstance(self.expected, Solved) or len(self.expected.acceptable) != 1:
            raise ValueError(f"{self.item_id} has no unique gold assembly")
        return self.expected.acceptable[0]


class GoldSet(SyntheticModel):
    provenance: Provenance
    items: tuple[PlanItem, ...] = Field(min_length=1)
