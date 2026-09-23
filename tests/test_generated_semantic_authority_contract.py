from __future__ import annotations

import asyncio
from typing import Any

import pytest

from epistemic_graph.generated import reasoning
from epistemic_graph.generated.reasoning import (
    DatalogReasoningResult,
    OwlExplainResult,
    OwlPropertyFact,
    OwlReasonResult,
    ProofNodeWire,
    ShaclValidationReport,
)

# Static generated-client contract checks against a fake transport; no engine.
pytestmark = pytest.mark.no_engine


class _Client:
    def __init__(self, payloads: dict[str, dict[str, Any]]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, dict[str, Any] | None, str | None, str | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        self.calls.append((method, params, graph, idempotency_key))
        return self.payloads[method]


def test_committed_shacl_validation_returns_its_atomic_schema_identity() -> None:
    digest = "11" * 32
    client = _Client(
        {
            "ShaclValidate": {
                "schema_digests": [digest],
                "composed_digest": digest,
                "conforms": True,
                "results": [],
            }
        }
    )

    report = asyncio.run(
        reasoning.send_shacl_validate(
            client,
            {"shapes": None, "data_graph": ""},
            "tenant",
        )
    )

    assert isinstance(report, ShaclValidationReport)
    assert report.schema_digests == [digest]
    assert report.composed_digest == digest
    assert client.calls == [
        ("ShaclValidate", {"shapes": None, "data_graph": ""}, "tenant", None)
    ]


def test_owl_reason_property_proof_is_fully_typed() -> None:
    digest = "22" * 32
    client = _Client(
        {
            "OwlReason": {
                "schema_digests": [digest],
                "direct_subclasses": [["<Child>", "<Parent>"]],
                "subclasses": [["<Child>", "<Parent>"]],
                "subclass_conf": [1.0],
                "instances": [],
                "instance_conf": [],
                "property_facts": [
                    {
                        "subject": "<a>",
                        "predicate": "<partOf>",
                        "object": "<c>",
                        "asserted": False,
                        "rule": "RL-transitive",
                        "axiom": "<partOf> rdf:type owl:TransitiveProperty",
                        "premises": [
                            ["<a>", "<partOf>", "<b>"],
                            ["<b>", "<partOf>", "<c>"],
                        ],
                    }
                ],
                "consistent": True,
                "unsatisfiable": [],
            }
        }
    )

    result = asyncio.run(
        reasoning.send_owl_reason(
            client,
            {
                "ontology": "",
                "target_class": "",
                "class_base": "http://knuckles.team/kg#",
                "min_confidence": 0.0,
            },
            "tenant",
        )
    )

    assert isinstance(result, OwlReasonResult)
    assert isinstance(result.property_facts[0], OwlPropertyFact)
    assert result.property_facts[0].premises[1][2] == "<c>"


def test_explain_and_materialization_results_are_typed() -> None:
    digest = "33" * 32
    client = _Client(
        {
            "OwlExplain": {
                "schema_digests": [digest],
                "found": True,
                "tree": {
                    "sub": "<Child>",
                    "sup": "<Parent>",
                    "rule": "asserted",
                    "axioms": ["<Child> rdfs:subClassOf <Parent>"],
                    "confidence": 1.0,
                    "premises": [],
                },
                "consistent": True,
                "unsatisfiable": [],
            },
            "RunDatalogReasoning": {
                "schema_digests": [digest],
                "inferred_count": 1,
                "inferred_triples": [
                    {
                        "subject": "a",
                        "predicate": "PART_OF",
                        "object": "c",
                        "inference_type": "rust_datalog",
                        "materialized": "true",
                    }
                ],
            },
        }
    )

    explanation = asyncio.run(
        reasoning.send_owl_explain(
            client,
            {
                "ontology": "",
                "sub": "http://knuckles.team/kg#Child",
                "sup": "http://knuckles.team/kg#Parent",
            },
            "tenant",
        )
    )
    materialized = asyncio.run(
        reasoning.send_run_datalog_reasoning(
            client,
            {
                "subclass_relations": [],
                "subproperty_relations": [],
                "symmetric_properties": [],
                "transitive_properties": [],
                "inverse_properties": [],
                "domain_rules": [],
                "range_rules": [],
                "property_chains": [],
            },
            "tenant",
            idempotency_key="reason-1",
        )
    )

    assert isinstance(explanation, OwlExplainResult)
    assert isinstance(explanation.tree, ProofNodeWire)
    assert isinstance(materialized, DatalogReasoningResult)
    assert materialized.schema_digests == [digest]
