"""Strict Pydantic bases every synthetic record derives from."""

from __future__ import annotations

from fractions import Fraction
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

EVIDENCE_LABEL: Literal["synthetic"] = "synthetic"

ComponentKind = Literal["model_profile", "tool", "skill"]


class SyntheticModel(BaseModel):
    """Immutable, closed and strictly typed: no coercion, no unknown fields."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ComponentFields(SyntheticModel):
    """The identity and capability shape every synthetic component -- a tool,
    skill or model profile -- carries, shared by the agent catalog
    (``catalog.SyntheticComponent``) and the assembly candidate universe
    (``assembly.Candidate``). Each subclass adds its own extra facts and
    validators on top of this common base."""

    component_id: str = Field(min_length=1)
    kind: ComponentKind
    summary: str = Field(min_length=1)
    classification: tuple[str, ...] = ()
    declared_capabilities: tuple[str, ...] = ()
    required_capabilities: tuple[str, ...] = ()
    requires: tuple[str, ...] = ()


class Provenance(SyntheticModel):
    """Marks generated evidence so it can never be mistaken for real data."""

    evidence: Literal["synthetic"] = EVIDENCE_LABEL
    generator: str = Field(min_length=1)
    generator_version: int = Field(ge=1)
    seed: int = Field(ge=0, lt=1 << 64)


class Probability(SyntheticModel):
    """An exact rational probability; the planted truth never passes through a float."""

    numerator: int = Field(ge=0)
    denominator: int = Field(gt=0)

    @model_validator(mode="after")
    def _at_most_one(self) -> Probability:
        if self.numerator > self.denominator:
            raise ValueError("a probability cannot exceed one")
        return self

    @classmethod
    def of(cls, value: Fraction) -> Probability:
        return cls(numerator=value.numerator, denominator=value.denominator)

    def as_fraction(self) -> Fraction:
        return Fraction(self.numerator, self.denominator)


def sorted_unique(values: tuple[str, ...], field: str) -> tuple[str, ...]:
    """Validate that a tuple of names is already in byte order without repeats."""
    if list(values) != sorted(set(values), key=str.encode):
        raise ValueError(f"{field} must be sorted by UTF-8 bytes and unique")
    return values
