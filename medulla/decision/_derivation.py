"""Re-check a record's coverage derivations and evidence class.

The pure-Python twin of ``eg_types::decision::derivation::verify_record``: every
stored ``is_a`` chain must start at the covering component's own
classification (a claim) and climb declared native ``broader`` links
(definitions) to exactly the required capability, and the record's evidence
class must be the WEAKEST class among its premises and chain edges.
"""

from __future__ import annotations

from typing import Any

from ._ontology import is_direct_broader

#: Strength of each premise class, strongest first (a definition never weakens).
_STRENGTH = {"definition": 3, "proof": 2, "observation": 1, "claim": 0}
_EVIDENCE_BY_STRENGTH = {0: "claim", 1: "observation"}


class DerivationError(ValueError):
    """A stored derivation or evidence class that does not re-check."""


def weakest(classes: list[str]) -> str:
    """The evidence class of a conclusion resting on premises of ``classes``."""
    floor = min((_STRENGTH[name] for name in classes), default=_STRENGTH["definition"])
    return _EVIDENCE_BY_STRENGTH.get(floor, "proof")


def _check_root(edge: dict[str, Any], candidate: dict[str, Any], required: str) -> None:
    source = edge["source"]
    rooted = (
        edge["narrower"] == candidate["component_id"]
        and edge["class"] == "claim"
        and source.get("edge") == "component_classification"
        and source.get("component_id") == candidate["component_id"]
        and edge["broader"] in candidate["classification"]
    )
    if not rooted:
        raise DerivationError(f"{required}: the chain is not rooted in its component")


def _check_native(edges: list[dict[str, Any]], required: str) -> None:
    for index in range(1, len(edges)):
        edge, previous = edges[index], edges[index - 1]
        if edge["narrower"] != previous["broader"]:
            raise DerivationError(f"{required}: edge {index} is discontinuous")
        native = (
            edge["source"] == {"edge": "native_ontology"}
            and edge["class"] == "definition"
            and is_direct_broader(edge["narrower"], edge["broader"])
        )
        if not native:
            raise DerivationError(f"{required}: edge {index} is not in the vocabulary")


def verify_coverage(
    derivation: dict[str, Any], candidates: list[dict[str, Any]]
) -> None:
    """Re-check one stored coverage derivation, or raise :class:`DerivationError`."""
    required = derivation["required"]
    chain = derivation["chain"]
    covering = derivation.get("covered_by")
    if covering is None:
        if chain:
            raise DerivationError(f"{required}: an uncovered requirement has a chain")
        return
    candidate = next((c for c in candidates if c["component_id"] == covering), None)
    if candidate is None:
        raise DerivationError(f"{required}: '{covering}' is not a stored candidate")
    if not chain:
        raise DerivationError(f"{required}: a covered requirement has no chain")
    _check_root(chain[0], candidate, required)
    _check_native(chain, required)
    if chain[-1]["broader"] != required:
        raise DerivationError(f"{required}: the chain does not reach the requirement")


def premise_classes(record: dict[str, Any]) -> list[str]:
    """Every premise class the record's conclusion rests on."""
    classes = [premise["class"] for premise in record["premises"]]
    for derivation in record["derivations"]:
        classes.extend(edge["class"] for edge in derivation["chain"])
    return classes


def verify_derivations(record: dict[str, Any]) -> None:
    """Re-check every derivation and the evidence class of ``record``."""
    candidates = record["inputs"]["candidates"]
    for derivation in record["derivations"]:
        verify_coverage(derivation, candidates)
    derived = weakest(premise_classes(record))
    if derived != record["evidence_class"]:
        raise DerivationError(
            f"evidence class {record['evidence_class']!r} is not the weakest "
            f"premise ({derived!r})"
        )
