"""The pure-Python Decide-layer verifier agrees with the engine's golden vectors.

The golden file is written by the engine's own test
(``eg_compute::assemble::tests::golden``); these tests re-check every record
in it without the engine, and prove each check refuses a planted tamper.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from epistemic_graph import decision

pytestmark = pytest.mark.no_engine

GOLDEN = Path(__file__).resolve().parent / "fixtures/decision/assembly_golden_v1.json"


def _golden() -> list[dict[str, Any]]:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def _case(name: str) -> dict[str, Any]:
    return next(case for case in _golden() if case["name"] == name)


def test_every_golden_record_verifies() -> None:
    for case in _golden():
        verdict = decision.verify_assembly(case["record"], case["model"])
        solved = case["record"]["outcome"]["outcome"] == "solved"
        assert (verdict is not None) == solved, case["name"]
        if verdict is not None:
            assert verdict.supports_an_answer


def test_the_client_vocabulary_is_the_engines() -> None:
    record = _case("research")["record"]
    assert record["inputs"]["ontology_digest"] == decision.ontology_digest()


def test_a_claim_shown_as_a_proof_is_refused() -> None:
    record = copy.deepcopy(_case("research")["record"])
    assert record["evidence_class"] == "claim"
    record["evidence_class"] = "proof"
    record["record_digest"] = decision.record_digest(record)
    record["record_id"] = decision.record_id(record["record_digest"])
    with pytest.raises(decision.DerivationError):
        decision.verify_record(record)


def test_a_tampered_record_digest_is_refused() -> None:
    record = copy.deepcopy(_case("research")["record"])
    record["created_at_ms"] += 1
    with pytest.raises(decision.DecisionVerificationError):
        decision.verify_record(record)


def test_an_invented_ontology_edge_is_refused() -> None:
    record = copy.deepcopy(_case("research")["record"])
    chain = next(d["chain"] for d in record["derivations"] if len(d["chain"]) > 1)
    chain[1]["broader"] = "eg:capability/action"
    with pytest.raises(decision.DerivationError):
        decision.verify_coverage(
            next(d for d in record["derivations"] if d["chain"] is chain),
            record["inputs"]["candidates"],
        )


def test_a_tampered_certificate_is_refused() -> None:
    case = _case("research")
    certificate = copy.deepcopy(case["record"]["outcome"]["certificate"])
    selected = certificate["incumbent"]["selected"]
    certificate["incumbent"]["selected"] = [not on for on in selected]
    with pytest.raises(decision.CertificateError):
        decision.verify_certificate(case["model"], certificate)


def test_a_certificate_for_another_model_is_refused() -> None:
    case = _case("research")
    model = copy.deepcopy(case["model"])
    model["variables"][0] = model["variables"][0] + "-renamed"
    with pytest.raises(decision.CertificateError):
        decision.verify_certificate(model, case["record"]["outcome"]["certificate"])


def test_a_solved_record_without_its_model_is_refused() -> None:
    with pytest.raises(decision.DecisionVerificationError):
        decision.verify_assembly(_case("research")["record"], None)


def test_the_weakest_premise_classifies() -> None:
    assert decision.weakest([]) == "proof"
    assert decision.weakest(["definition"]) == "proof"
    assert decision.weakest(["proof", "observation"]) == "observation"
    assert decision.weakest(["definition", "claim", "observation"]) == "claim"
