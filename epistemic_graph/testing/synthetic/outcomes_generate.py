"""Seeded construction of outcome streams (see `outcomes`)."""

from __future__ import annotations

from collections.abc import Sequence
from fractions import Fraction
from math import lcm

from ._model import Probability, Provenance
from ._rng import SeededStream
from .outcomes import (
    GENERATOR,
    GENERATOR_VERSION,
    Cell,
    Defect,
    Fidelity,
    OutcomeRecord,
    OutcomeStream,
    PlantedValue,
    Policy,
    ScoreMass,
)

_GRID = 20
_OPTIONS = tuple(f"option-{i}" for i in range(4))
_CONTEXTS = tuple(f"context-{i}" for i in range(3))
_DEFECTS: tuple[Defect, ...] = (
    "self_reported",
    "llm_resolved_abstention",
    "pinned",
    "propensity_is_head_mass",
    "censored",
    "below_fidelity_floor",
)
_NO_DEFECT: Defect = "none"
_FIDELITY_OVERRIDE: dict[Defect, Fidelity] = {
    "censored": "outcome-uncertain",
    "below_fidelity_floor": "final-output",
}


def _p(value: Fraction) -> Probability:
    return Probability.of(value)


def _partition(stream: SeededStream, parts: int, floor: int) -> list[Fraction]:
    """``parts`` fractions of ``_GRID`` that sum to one, each at least ``floor``."""
    free = _GRID - parts * floor
    cuts = sorted(stream.between(0, free) for _ in range(parts - 1))
    bounds = [0, *cuts, free]
    # Pairwise consecutive-bound iteration: `bounds[1:]` is always one shorter than
    # `bounds` by construction, so the length mismatch is intentional (strict=False).
    return [
        Fraction(floor + hi - lo, _GRID)
        for lo, hi in zip(bounds, bounds[1:], strict=False)
    ]


def _logging(stream: SeededStream, unsupported: tuple[str, str]) -> Policy:
    cells = []
    for context in _CONTEXTS:
        options = [o for o in _OPTIONS if (context, o) != unsupported]
        masses = _partition(stream.child(context), len(options), 2)
        cells += [
            Cell(context_id=context, option_id=o, probability=_p(m))
            for o, m in zip(options, masses, strict=True)
        ]
    return Policy(name="logging", cells=tuple(cells))


def _greedy(success: Sequence[Cell]) -> Policy:
    cells = []
    for context in _CONTEXTS:
        row = [c for c in success if c.context_id == context]
        # `max` keeps the first of equal rates, and rows are in option-id order.
        best = max(row, key=lambda c: c.probability.as_fraction())
        one = _p(Fraction(1))
        cells.append(
            Cell(context_id=context, option_id=best.option_id, probability=one)
        )
    return Policy(name="greedy", cells=tuple(cells))


def _mixture(name: str, first: Policy, second: Policy) -> Policy:
    half = Fraction(1, 2)
    mass: dict[tuple[str, str], Fraction] = {}
    for policy in (first, second):
        for cell in policy.cells:
            key = (cell.context_id, cell.option_id)
            mass[key] = (
                mass.get(key, Fraction(0)) + half * cell.probability.as_fraction()
            )
    cells = tuple(
        Cell(context_id=c, option_id=o, probability=_p(m))
        for (c, o), m in sorted(mass.items())
    )
    return Policy(name=name, cells=cells)


def _draw(stream: SeededStream, weights: Sequence[tuple[str, Fraction]]) -> str:
    """Pick a key with exactly its rational weight, using one integer draw."""
    denominator = lcm(*(weight.denominator for _, weight in weights))
    point = Fraction(stream.below(denominator), denominator)
    total = Fraction(0)
    for key, weight in weights:
        total += weight
        if point < total:
            return key
    raise ValueError("weights do not sum to one")


def _head_score(rate: Fraction) -> Fraction:
    """The planted miscalibrated head: overconfident by a quarter of the headroom."""
    return rate + (1 - rate) / 4


def _planted_values(stream: OutcomeStream) -> tuple[PlantedValue, ...]:
    values = []
    for policy in stream.targets:
        total: dict[bool, Fraction] = {True: Fraction(0), False: Fraction(0)}
        unsupported = Fraction(0)
        for cell in policy.cells:
            mass = (
                stream.context_weight(cell.context_id) * cell.probability.as_fraction()
            )
            supported = stream.logging.probability(cell.context_id, cell.option_id) > 0
            total[supported] += mass * stream.success_rate(
                cell.context_id, cell.option_id
            )
            unsupported += Fraction(0) if supported else mass
        values.append(
            PlantedValue(
                policy=policy.name,
                value=_p(total[True] + total[False]),
                supported_value=_p(total[True]),
                unsupported_mass=_p(unsupported),
            )
        )
    return tuple(values)


def _calibration_error(stream: OutcomeStream) -> Fraction:
    error = Fraction(0)
    for cell in stream.logging.cells:
        rate = stream.success_rate(cell.context_id, cell.option_id)
        mass = stream.context_weight(cell.context_id) * cell.probability.as_fraction()
        error += mass * abs(_head_score(rate) - rate)
    return error


def _nonconformity(stream: OutcomeStream) -> tuple[ScoreMass, ...]:
    atoms: dict[Fraction, Fraction] = {}
    for cell in stream.logging.cells:
        rate = stream.success_rate(cell.context_id, cell.option_id)
        mass = stream.context_weight(cell.context_id) * cell.probability.as_fraction()
        score = _head_score(rate)
        for value, weight in ((1 - score, rate), (score, 1 - rate)):
            atoms[value] = atoms.get(value, Fraction(0)) + mass * weight
    return tuple(
        ScoreMass(score=_p(value), mass=_p(weight))
        for value, weight in sorted(atoms.items())
        if weight
    )


def _record(
    stream: SeededStream, index: int, tables: OutcomeStream, defect: Defect
) -> OutcomeRecord:
    context = _draw(
        stream, [(c.context_id, c.probability.as_fraction()) for c in tables.contexts]
    )
    row = [
        (c.option_id, c.probability.as_fraction())
        for c in tables.logging.cells
        if c.context_id == context
    ]
    option = _draw(stream, row)
    rate = tables.success_rate(context, option)
    propensity = tables.logging.probability(context, option)
    recorded = {
        "pinned": Fraction(1),
        "propensity_is_head_mass": _head_score(rate),
    }.get(defect, propensity)
    return OutcomeRecord(
        record_id=f"record-{index:06d}",
        context_id=context,
        option_id=option,
        recorded_propensity=_p(recorded),
        success=stream.chance(rate.numerator, rate.denominator),
        evaluation_class="claim"
        if defect in ("self_reported", "llm_resolved_abstention")
        else "observation",
        independent_evaluator=defect != "self_reported",
        fidelity=_FIDELITY_OVERRIDE.get(defect, "full-step"),
        head_score=_p(_head_score(rate)),
        defect=defect,
    )


def generate_outcome_stream(
    seed: int, *, records: int = 4_000, defects_each: int = 25
) -> OutcomeStream:
    stream = SeededStream(seed, GENERATOR)
    unsupported = (stream.choice(_CONTEXTS), stream.choice(_OPTIONS))
    weights = _partition(stream.child("contexts"), len(_CONTEXTS), 3)
    success = tuple(
        Cell(
            context_id=c,
            option_id=o,
            probability=_p(Fraction(stream.between(1, _GRID - 4), _GRID)),
        )
        for c in _CONTEXTS
        for o in _OPTIONS
    )
    logging = _logging(stream.child("logging"), unsupported)
    greedy = _greedy(success)
    tables = OutcomeStream(
        provenance=Provenance(
            generator=GENERATOR, generator_version=GENERATOR_VERSION, seed=seed
        ),
        contexts=tuple(
            Cell(context_id=c, option_id="*", probability=_p(w))
            for c, w in zip(_CONTEXTS, weights, strict=True)
        ),
        success=success,
        logging=logging,
        targets=(greedy, _mixture("half-greedy", greedy, logging)),
        records=(),
        planted_values=(),
        planted_calibration_error=_p(Fraction(0)),
        planted_nonconformity=(),
    )
    plan: list[Defect] = [_NO_DEFECT] * records + [
        d for d in _DEFECTS for _ in range(defects_each)
    ]
    draws = stream.child("records")
    rows = tuple(
        _record(draws.child(str(i)), i, tables, defect) for i, defect in enumerate(plan)
    )
    # Re-validate the finished stream rather than trusting an unchecked copy.
    return OutcomeStream.model_validate(
        {
            **dict(tables),
            "records": rows,
            "planted_values": _planted_values(tables),
            "planted_calibration_error": _p(_calibration_error(tables)),
            "planted_nonconformity": _nonconformity(tables),
        }
    )
