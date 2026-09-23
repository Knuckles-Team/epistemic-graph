"""Judge a certificate's claimed status against independently verified facts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ._certificate import CertificateError, Model
from ._proof import Evidence, incumbent_value, root, walk


@dataclass(frozen=True)
class Verdict:
    """What a certificate was verified to establish.

    ``kind`` is one of ``proven_optimal``, ``optimality_requires_replay``,
    ``proven_gap``, ``proven_infeasible``, ``infeasibility_requires_replay`` or
    ``unresolved``, exactly as the engine's verifier names them.
    """

    kind: str
    objective: int | None = None
    lower_bound: int | None = None
    core: tuple[int, ...] = ()

    @property
    def supports_an_answer(self) -> bool:
        """Whether this verdict supports a ``Solved`` assembly."""
        return self.kind in {
            "proven_optimal",
            "optimality_requires_replay",
            "proven_gap",
        }


@dataclass(frozen=True)
class _Facts:
    incumbent: int | None
    evidence: Evidence
    tree: bool
    claimed: int | None
    certificate: dict[str, Any]


def _require(condition: bool, defect: str) -> None:
    if not condition:
        raise CertificateError(f"status defect: {defect}")


def _value(value: int | None, defect: str) -> int:
    if value is None:
        raise CertificateError(f"status defect: {defect}")
    return value


def _incumbent(facts: _Facts) -> int:
    return _value(facts.incumbent, "missing incumbent")


def _supported_bound(facts: _Facts) -> int:
    claimed = _value(facts.claimed, "lower bound mismatch")
    proven = _value(facts.evidence.min_bound, "no bound leaf")
    _require(claimed <= proven, "lower bound above proof")
    return claimed


def _nodes(facts: _Facts, body: dict[str, Any]) -> None:
    _require(
        body["nodes_expanded"] == facts.certificate["nodes_expanded"],
        "node count mismatch",
    )


def _budget_spent(facts: _Facts) -> None:
    spent = (
        facts.certificate["nodes_expanded"]
        == facts.certificate["config"]["node_budget"]
    )
    _require(spent, "node count mismatch")


def _optimal(facts: _Facts, _: Any) -> Verdict:
    best = _incumbent(facts)
    proven = _value(facts.evidence.min_bound, "no bound leaf")
    _require(proven >= best, "bound below incumbent")
    _require(facts.claimed == best, "lower bound mismatch")
    return Verdict("proven_optimal", objective=best)


def _optimal_by_search(facts: _Facts, body: Any) -> Verdict:
    _nodes(facts, body)
    _require(not facts.tree, "wrong proof kind")
    best = _incumbent(facts)
    bound = _supported_bound(facts)
    _require(bound <= best, "lower bound mismatch")
    return Verdict("optimality_requires_replay", objective=best, lower_bound=bound)


def _with_gap(facts: _Facts, body: Any) -> Verdict:
    _budget_spent(facts)
    best = _incumbent(facts)
    bound = _supported_bound(facts)
    gap = int(body["gap"])
    _require(gap > 0 and best - bound == gap, "gap mismatch")
    _require(
        gap <= int(facts.certificate["config"]["accepted_gap"]), "gap not accepted"
    )
    return Verdict("proven_gap", objective=best, lower_bound=bound)


def _no_incumbent_no_bound(facts: _Facts) -> None:
    _require(facts.incumbent is None, "unexpected incumbent")
    _require(facts.claimed is None, "lower bound mismatch")


def _infeasible(facts: _Facts, body: Any) -> Verdict:
    _no_incumbent_no_bound(facts)
    _require(facts.tree and facts.evidence.bound_leaves == 0, "wrong proof kind")
    _require(list(body["core"]) == facts.evidence.infeasibility_rows, "core mismatch")
    return Verdict("proven_infeasible", core=tuple(body["core"]))


def _infeasible_by_search(facts: _Facts, body: Any) -> Verdict:
    _nodes(facts, body)
    _no_incumbent_no_bound(facts)
    _require(not facts.tree, "wrong proof kind")
    return Verdict("infeasibility_requires_replay")


def _exhausted(facts: _Facts, _: Any) -> Verdict:
    _budget_spent(facts)
    bound = None if facts.claimed is None else _supported_bound(facts)
    if facts.incumbent is not None and bound is not None:
        accepted = int(facts.certificate["config"]["accepted_gap"])
        _require(facts.incumbent - bound > accepted, "gap accepted")
    return Verdict("unresolved", objective=facts.incumbent, lower_bound=bound)


_JUDGES = {
    "optimal": _optimal,
    "optimal_by_deterministic_search": _optimal_by_search,
    "feasible_with_gap": _with_gap,
    "infeasible": _infeasible,
    "infeasible_by_deterministic_search": _infeasible_by_search,
    "budget_exhausted": _exhausted,
}


def _status(certificate: dict[str, Any]) -> tuple[str, Any]:
    status = certificate["status"]
    if isinstance(status, str):
        return status, None
    ((kind, body),) = status.items()
    return kind, body


def verify_certificate(
    model_spec: dict[str, Any], certificate: dict[str, Any]
) -> Verdict:
    """Verify ``certificate`` against ``model_spec`` without trusting the solver.

    Raises :class:`CertificateError` naming the first defect found.
    """
    model = Model(model_spec)
    if certificate["model_digest"] != model.digest:
        raise CertificateError("the certificate was issued for a different model")
    if certificate["nodes_expanded"] > certificate["config"]["node_budget"]:
        raise CertificateError("status defect: node count mismatch")
    incumbent = incumbent_value(model, certificate)
    ((proof_kind, proof),) = certificate["proof"].items()
    tree = proof_kind == "tree"
    evidence = walk(model, proof["nodes"]) if tree else root(model, proof["dual"])
    claimed = certificate.get("lower_bound")
    facts = _Facts(
        incumbent=incumbent,
        evidence=evidence,
        tree=tree,
        claimed=None if claimed is None else int(claimed),
        certificate=certificate,
    )
    kind, body = _status(certificate)
    return _JUDGES[kind](facts, body)
