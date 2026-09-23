"""Lagrangian duals, the pre-order proof tree, and the status judgement."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ._certificate import (
    MAX_BOUND_DENOMINATOR,
    MAX_MULTIPLIER,
    CertificateError,
    Model,
    Row,
    check_incumbent,
)

_SIGN_OK = {
    "greater_equal": lambda m: m > 0,
    "less_equal": lambda m: m < 0,
    "equal": lambda m: m != 0,
}


def _check_shape(model: Model, dual: dict[str, Any]) -> None:
    if not 0 < dual["denominator"] <= MAX_BOUND_DENOMINATOR:
        raise CertificateError("a dual's denominator is out of range")
    rows = [entry["row"] for entry in dual["entries"]]
    if any(a >= b for a, b in zip(rows, rows[1:], strict=False)):
        raise CertificateError("a dual's entries are not strictly ordered by row")
    for entry in dual["entries"]:
        if not 0 <= entry["row"] < len(model.rows):
            raise CertificateError("a dual names a row the model does not have")
        multiplier = int(entry["numerator"])
        relation = model.rows[entry["row"]].relation
        if not _SIGN_OK[relation](multiplier) or abs(multiplier) > MAX_MULTIPLIER:
            raise CertificateError("a dual multiplier has the wrong sign or magnitude")


def scaled_lagrangian(
    model: Model, dual: dict[str, Any], path: list[bool | None], feasibility: bool
) -> int:
    """``D * L(dual, path)`` against the objective, or the zero objective."""
    _check_shape(model, dual)
    denominator = dual["denominator"]
    if feasibility:
        reduced = [0] * len(model.weights)
    else:
        reduced = [weight * denominator for weight in model.weights]
    scaled = 0
    for entry in dual["entries"]:
        row = model.rows[entry["row"]]
        multiplier = int(entry["numerator"])
        scaled += multiplier * row.rhs
        for var, coefficient in row.terms:
            reduced[var] -= multiplier * coefficient
    for cost, assigned in zip(reduced, path, strict=True):
        scaled += {True: cost, False: 0, None: min(0, cost)}[assigned]
    return scaled


def integer_bound(scaled: int, denominator: int) -> int:
    """``ceil(scaled / denominator)`` in exact integers."""
    return -((-scaled) // denominator)


def _unsatisfiable(row: Row, path: list[bool | None]) -> bool:
    low = high = 0
    for var, coefficient in row.terms:
        assigned = path[var]
        if assigned is True:
            low += coefficient
            high += coefficient
        elif assigned is None:
            low += min(0, coefficient)
            high += max(0, coefficient)
    checks = {
        "less_equal": low > row.rhs,
        "greater_equal": high < row.rhs,
        "equal": low > row.rhs or high < row.rhs,
    }
    return checks[row.relation]


@dataclass
class Evidence:
    """What the verified leaves establish."""

    min_bound: int | None = None
    infeasibility_rows: list[int] = field(default_factory=list)
    bound_leaves: int = 0

    def add_bound(self, bound: int) -> None:
        self.bound_leaves += 1
        self.min_bound = bound if self.min_bound is None else min(self.min_bound, bound)


@dataclass
class _Walker:
    model: Model
    path: list[bool | None]
    fixed: list[int] = field(default_factory=list)
    open: list[list[Any]] = field(default_factory=list)
    finished: bool = False
    evidence: Evidence = field(default_factory=Evidence)

    def assign(self, var: int, value: bool) -> None:
        if not 0 <= var < len(self.path) or self.path[var] is not None:
            raise CertificateError(f"variable {var} is out of range or already fixed")
        self.path[var] = value
        self.fixed.append(var)

    def visit(self, node: dict[str, Any]) -> None:
        ((kind, body),) = node.items()
        if kind == "branch":
            depth = len(self.fixed)
            self.assign(body["var"], body["first"])
            self.open.append([body["var"], not body["first"], False, depth])
        elif kind == "forced":
            self.forced(body["var"], body["value"], body["row"])
        else:
            self.leaf(body["proof"])
            self.close()

    def forced(self, var: int, value: bool, row_id: int) -> None:
        if not 0 <= row_id < len(self.model.rows):
            raise CertificateError("a forced node names a row the model does not have")
        row = self.model.rows[row_id]
        self.assign(var, not value)
        justified = any(v == var for v, _ in row.terms) and _unsatisfiable(
            row, self.path
        )
        self.path[var] = value
        if not justified:
            raise CertificateError(
                f"forcing variable {var} is not justified by row {row_id}"
            )
        self.evidence.infeasibility_rows.append(row_id)

    def leaf(self, proof: dict[str, Any]) -> None:
        ((kind, body),) = proof.items()
        dual = body["dual"]
        scaled = scaled_lagrangian(self.model, dual, self.path, kind == "infeasible")
        if kind == "bound":
            self.evidence.add_bound(integer_bound(scaled, dual["denominator"]))
            return
        if scaled <= 0:
            raise CertificateError("an infeasibility leaf's dual proves nothing")
        self.evidence.infeasibility_rows.extend(
            entry["row"] for entry in dual["entries"]
        )

    def close(self) -> None:
        while self.open:
            var, second, started, depth = self.open[-1]
            self.open[-1][2] = True
            for fixed in self.fixed[depth:]:
                self.path[fixed] = None
            del self.fixed[depth:]
            if not started:
                self.path[var] = second
                self.fixed.append(var)
                return
            self.open.pop()
        self.finished = True


def walk(model: Model, nodes: list[dict[str, Any]]) -> Evidence:
    """Check a complete pre-order tree and return what its leaves prove."""
    walker = _Walker(model, [None] * len(model.weights))
    for node in nodes:
        if walker.finished:
            raise CertificateError("nodes follow the last leaf of the tree")
        walker.visit(node)
    if not walker.finished:
        raise CertificateError("the proof tree is incomplete")
    walker.evidence.infeasibility_rows = sorted(set(walker.evidence.infeasibility_rows))
    return walker.evidence


def root(model: Model, dual: dict[str, Any]) -> Evidence:
    """Check the root dual alone, at the empty assignment."""
    evidence = Evidence()
    scaled = scaled_lagrangian(model, dual, [None] * len(model.weights), False)
    evidence.add_bound(integer_bound(scaled, dual["denominator"]))
    return evidence


def incumbent_value(model: Model, certificate: dict[str, Any]) -> int | None:
    """The verified incumbent objective, when the certificate carries one."""
    incumbent = certificate.get("incumbent")
    return None if incumbent is None else check_incumbent(model, incumbent)
