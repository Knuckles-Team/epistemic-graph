"""(c) Outcome streams with planted success rates, propensities and policy values.

Contexts arrive with known weights; the logging policy picks an option with a
known exact propensity; the outcome is a success with a known exact rate for
that (context, option). From those tables the value of any target policy, the
mass it puts outside the logging support, and a head's calibration error are
computed exactly as fractions. That planted truth is what calibration, conformal
and off-policy estimators are tested against.

Records that must never be used as labels are planted too, each with the reason
(DECIDE-LAYER-DESIGN §6.4, §11.2): self-reported outcomes, LLM-resolved
abstentions, pinned decisions, propensities copied from a head's score, censored
runs and runs below the fidelity floor. They are appended to, never substituted
for, the admissible sample, so the admissible records stay an unbiased sample of
the logging policy.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Literal

from pydantic import Field, model_validator

from ._model import Probability, Provenance, SyntheticModel

GENERATOR = "outcomes"
GENERATOR_VERSION = 1

Fidelity = Literal[
    "full-step",
    "tool-calls",
    "final-output",
    "trace-incomplete",
    "outcome-uncertain",
    "cancelled",
]
Defect = Literal[
    "none",
    "self_reported",
    "llm_resolved_abstention",
    "pinned",
    "propensity_is_head_mass",
    "censored",
    "below_fidelity_floor",
]
FIDELITY_FLOOR: Fidelity = "tool-calls"


class Cell(SyntheticModel):
    """One (context, option) entry of a probability table."""

    context_id: str
    option_id: str
    probability: Probability


class Policy(SyntheticModel):
    name: str = Field(min_length=1)
    cells: tuple[Cell, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _rows_sum_to_one(self) -> Policy:
        totals: dict[str, Fraction] = {}
        for cell in self.cells:
            totals[cell.context_id] = totals.get(cell.context_id, Fraction(0)) + (
                cell.probability.as_fraction()
            )
        if any(total != 1 for total in totals.values()):
            raise ValueError(f"policy {self.name} rows must sum to exactly one")
        return self

    def probability(self, context_id: str, option_id: str) -> Fraction:
        for cell in self.cells:
            if (cell.context_id, cell.option_id) == (context_id, option_id):
                return cell.probability.as_fraction()
        return Fraction(0)


class OutcomeRecord(SyntheticModel):
    record_id: str
    context_id: str
    option_id: str
    recorded_propensity: Probability
    success: bool
    evaluation_class: Literal["observation", "claim"]
    independent_evaluator: bool
    fidelity: Fidelity
    head_score: Probability
    defect: Defect

    @property
    def admissible(self) -> bool:
        return self.defect == "none"


class PlantedValue(SyntheticModel):
    """A target policy's exact value, and the part an off-policy estimate can see.

    Importance weighting can only estimate ``supported_value``: the value earned
    on (context, option) pairs the logging policy ever plays. ``unsupported_mass``
    is the probability the target policy puts everywhere else.
    """

    policy: str
    value: Probability
    supported_value: Probability
    unsupported_mass: Probability


class ScoreMass(SyntheticModel):
    """One atom of the exact distribution of a nonconformity score."""

    score: Probability
    mass: Probability


class OutcomeStream(SyntheticModel):
    provenance: Provenance
    contexts: tuple[Cell, ...]
    success: tuple[Cell, ...]
    logging: Policy
    targets: tuple[Policy, ...]
    records: tuple[OutcomeRecord, ...]
    planted_values: tuple[PlantedValue, ...]
    planted_calibration_error: Probability
    # Nonconformity |outcome - head_score| under the logging distribution, sorted
    # by score, so the exact (1 - alpha) quantile and its coverage are known.
    planted_nonconformity: tuple[ScoreMass, ...]

    def conformal_quantile(self, alpha: Fraction) -> Fraction:
        """The smallest score whose cumulative mass reaches ``1 - alpha``."""
        total = Fraction(0)
        for atom in self.planted_nonconformity:
            total += atom.mass.as_fraction()
            if total >= 1 - alpha:
                return atom.score.as_fraction()
        raise ValueError("nonconformity masses do not sum to one")

    def success_rate(self, context_id: str, option_id: str) -> Fraction:
        return _lookup(self.success, context_id, option_id)

    def context_weight(self, context_id: str) -> Fraction:
        return _lookup(self.contexts, context_id, "*")

    def admissible_records(self) -> tuple[OutcomeRecord, ...]:
        return tuple(record for record in self.records if record.admissible)

    def planted(self, policy: str) -> PlantedValue:
        return next(value for value in self.planted_values if value.policy == policy)


def _lookup(cells: tuple[Cell, ...], context_id: str, option_id: str) -> Fraction:
    for cell in cells:
        if (cell.context_id, cell.option_id) == (context_id, option_id):
            return cell.probability.as_fraction()
    raise KeyError((context_id, option_id))
