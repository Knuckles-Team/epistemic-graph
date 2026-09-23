"""An independent, pure-Python verifier of the engine's solver certificates.

The twin of ``eg_compute::solve::verify``. It trusts the model (the problem
statement a client sent to ``Solve``, or the ``model`` an ``AgentAssemble``
result returns) and nothing the solver produced: it lowers every typed
constraint to one linear row itself, recomputes the scalarised objective,
evaluates each Lagrangian dual in exact integers, walks the pre-order proof
tree with its own path, and only then judges the claimed status.

Python integers are unbounded, so the engine's checked-``i128`` overflow
refusals cannot arise here; the magnitude caps the engine enforces on the
certificate (denominator, multiplier) are enforced the same way.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ._digest import raw_digest

MODEL_DIGEST_DOMAIN = "eg-solve/model/v1"
MAX_BOUND_DENOMINATOR = 1 << 16
MAX_MULTIPLIER = 1 << 56


class CertificateError(ValueError):
    """A certificate that does not verify against its model."""


@dataclass(frozen=True)
class Row:
    """One lowered linear row ``sum(coefficient * x) relation rhs``."""

    terms: tuple[tuple[int, int], ...]
    relation: str
    rhs: int


def _unit(vars_: list[int], relation: str, rhs: int) -> Row:
    return Row(tuple(sorted((var, 1) for var in vars_)), relation, rhs)


def _implication(antecedent: int, consequents: list[int]) -> Row:
    terms = [(var, 1) for var in consequents] + [(antecedent, -1)]
    return Row(tuple(sorted(terms)), "greater_equal", 0)


def _lower(body: dict[str, Any]) -> Row:
    ((kind, value),) = body.items()
    lowering = {
        "linear": lambda v: Row(
            tuple(sorted((t["var"], t["coefficient"]) for t in v["terms"])),
            v["relation"],
            v["rhs"],
        ),
        "implication": lambda v: _implication(v["antecedent"], [v["consequent"]]),
        "implies_any": lambda v: _implication(v["antecedent"], v["consequents"]),
        "exactly_one": lambda v: _unit(v["vars"], "equal", 1),
        "at_most": lambda v: _unit(v["vars"], "less_equal", v["k"]),
        "at_least": lambda v: _unit(v["vars"], "greater_equal", v["k"]),
        "fix": lambda v: _unit([v["var"]], "equal", int(v["value"])),
    }
    return lowering[kind](value)


def _sublevels(levels: list[dict[str, Any]]) -> list[list[tuple[int, int]]]:
    out: list[list[tuple[int, int]]] = []
    for level in levels:
        unknown = [
            (t["var"], 1) for t in level["terms"] if t["coefficient"] == "unknown"
        ]
        if unknown:
            out.append(unknown)
        out.append(
            [
                (t["var"], t["coefficient"]["known"])
                for t in level["terms"]
                if t["coefficient"] != "unknown"
            ]
        )
    return out


def scalar_weights(levels: list[dict[str, Any]], variables: int) -> list[int]:
    """The exact lexicographic scalarisation the engine uses."""
    weights = [0] * variables
    multiplier = 1
    for sublevel in reversed(_sublevels(levels)):
        span = 1
        for var, coefficient in sublevel:
            weights[var] += multiplier * coefficient
            span += abs(coefficient)
        multiplier *= span
    return weights


@dataclass
class Model:
    """A lowered model: rows, scalar weights and the spec's digest."""

    spec: dict[str, Any]
    rows: list[Row] = field(init=False)
    weights: list[int] = field(init=False)
    digest: str = field(init=False)

    def __post_init__(self) -> None:
        self.rows = [
            _lower(constraint["body"]) for constraint in self.spec["constraints"]
        ]
        self.weights = scalar_weights(
            self.spec["objective"], len(self.spec["variables"])
        )
        self.digest = raw_digest(MODEL_DIGEST_DOMAIN, self.spec)

    def level_values(self, selected: list[bool]) -> list[dict[str, Any]]:
        values = []
        for level in self.spec["objective"]:
            chosen = [t for t in level["terms"] if selected[t["var"]]]
            known = sum(
                t["coefficient"]["known"]
                for t in chosen
                if t["coefficient"] != "unknown"
            )
            unknown = sum(1 for t in chosen if t["coefficient"] == "unknown")
            values.append({"known": str(known), "unknown_selected": unknown})
        return values


def _holds(relation: str, lhs: int, rhs: int) -> bool:
    return {"less_equal": lhs <= rhs, "greater_equal": lhs >= rhs, "equal": lhs == rhs}[
        relation
    ]


def check_incumbent(model: Model, incumbent: dict[str, Any]) -> int:
    """A feasible incumbent whose claimed objective matches the recomputed one."""
    selected = incumbent["selected"]
    if len(selected) != len(model.weights):
        raise CertificateError("the incumbent has the wrong number of variables")
    for index, row in enumerate(model.rows):
        lhs = sum(c for var, c in row.terms if selected[var])
        if not _holds(row.relation, lhs, row.rhs):
            raise CertificateError(f"the incumbent violates row {index}")
    scalar = sum(w for w, on in zip(model.weights, selected, strict=True) if on)
    objective = incumbent["objective"]
    if int(objective["scalar"]) != scalar or objective["levels"] != model.level_values(
        selected
    ):
        raise CertificateError("the incumbent's objective does not match its selection")
    return scalar
