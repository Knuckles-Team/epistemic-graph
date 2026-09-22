"""What the generated contract must say about the 2.27.x contract wave.

The original wave declared ten methods before any were served. ConnectorPack
has now graduated with its durable handler and generated reconciliation client;
the remaining refusal-only methods must stay internal. Every request and result
body remains schematized and digest identities remain reproducible.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]

WAVE_METHODS = (
    "AgentAssemble",
    "DecisionCommit",
    "Decide",
    "DecisionFit",
    "DecisionEval",
    "Solve",
    "ConnectorPack",
    "GraphSchema",
    "GraphSchemaList",
    "MutationOutbox",
)

INTERNAL_WAVE_METHODS = tuple(name for name in WAVE_METHODS if name != "ConnectorPack")

#: `$defs` names the wave adds, one per module it introduces. Not the whole
#: set: these are the entry points every other new type hangs off, so a missing
#: one means a module failed to reach the schema at all.
WAVE_DEFS = (
    "AssemblyRequest",
    "DecisionRecord",
    "DecisionPolicy",
    "DecideRequest",
    "DecisionFitOp",
    "DecisionEvalOp",
    "SolveRequest",
    "Certificate",
    "ConnectorPackOp",
    "ConnectorPackIndex",
    "GraphSchemaOp",
    "MutationOutboxOp",
    "AgentComponentContentRequest",
    "AgentComponentContentResult",
)


def _json(relative: str):
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def _methods() -> dict:
    return {entry["id"]: entry for entry in _json("contract/methods.json")["methods"]}


def test_every_wave_method_is_internal_with_no_consumer() -> None:
    methods = _methods()
    for name in INTERNAL_WAVE_METHODS:
        entry = methods.get(name)
        assert entry is not None, f"{name} is absent from the generated contract"
        assert entry["stability"] == "internal", f"{name} must stay internal in S1"
        assert not entry.get("consumer_profiles"), f"{name} must declare no consumer"


def test_no_wave_method_is_reachable_from_the_python_client() -> None:
    ids = (ROOT / "epistemic_graph/generated/_ids.py").read_text(encoding="utf-8")
    dispatch = (ROOT / "epistemic_graph/generated/__init__.py").read_text(
        encoding="utf-8"
    )
    for name in INTERNAL_WAVE_METHODS:
        assert f'"{name}"' not in ids, f"{name} must not appear in METHOD_IDS yet"
        assert f'"{name}"' not in dispatch, f"{name} must not be sendable yet"


def test_every_wave_request_schema_resolves() -> None:
    schema = _json("contract/schemas/method.request.json")
    methods = _methods()
    for name in WAVE_METHODS:
        assert name in schema["methods"], f"{name} has no request schema"
        pointer = methods[name]["request_schema"]["schema"]
        assert pointer == (f"contract/schemas/method.request.json#/methods/{name}"), (
            f"{name}'s request schema pointer does not resolve"
        )


def test_every_wave_result_schema_resolves() -> None:
    methods = _methods()
    for name in WAVE_METHODS:
        result = methods[name]["result_schema"]
        assert result["kind"] == "declared", f"{name} declares no result"
        document, _, pointer = result["schema"].partition("#")
        assert (ROOT / document).is_file(), f"{name} names a missing schema document"
        assert pointer.endswith(name), f"{name}'s result pointer names another method"


def test_every_wave_type_reaches_the_schema() -> None:
    encoded = json.dumps(_json("contract/schemas/method.request.json"))
    for domain in ("storage", "query", "coordination", "compute", "reasoning"):
        encoded += json.dumps(_json(f"contract/schemas/result.{domain}.json"))
    encoded += json.dumps(_json("contract/schemas/result.transactions.json"))
    missing = [name for name in WAVE_DEFS if f'"{name}' not in encoded]
    assert not missing, f"these wave types never reached the schema: {missing}"


def test_the_agent_component_request_grows_a_content_branch() -> None:
    encoded = json.dumps(_json("contract/schemas/method.request.json"))
    assert "AgentComponentRequest" in encoded or "AgentComponentOp" in encoded
    assert '"content"' in encoded, "the component op union has no content branch"


def test_the_receipt_counts_match_the_wave() -> None:
    for copy in ("contract/receipt.json", "epistemic_graph/contract/receipt.json"):
        receipt = _json(copy)
        assert receipt["method_count"] == 428, copy
        assert receipt["internal_only_methods"] == 28, copy
        assert receipt["python_client_methods"] == 400, copy
        classification = receipt["result_classification"]
        assert classification["schematized"] == 415, copy
        assert classification["unclassified"] == 0, copy
        assert sum(classification.values()) == 428, copy


def test_the_receipt_declares_every_new_format_identity() -> None:
    identities = _json("contract/receipt.json")["format_identities"]
    expected = {
        "COMPONENT_CONTENT_SCHEMA_VERSION": "1",
        "CONNECTOR_PACK_SCHEMA_VERSION": "2",
        "PACK_IMPORT_RECORD_SCHEMA_VERSION": "2",
        "DECISION_RECORD_SCHEMA_VERSION": "1",
        "STATISTICAL_DECISION_RECORD_SCHEMA_VERSION": "2",
        "DECISION_POLICY_SCHEMA_VERSION": "1",
        "DECISION_JOB_SCHEMA_VERSION": "1",
        "SOLVE_RESULT_SCHEMA_VERSION": "1",
        "GRAPH_SCHEMA_RESULT_SCHEMA_VERSION": "1",
        "MUTATION_OUTBOX_VIEW_SCHEMA_VERSION": "1",
        "AGENT_COMPONENT_SCHEMA_VERSION": "3",
    }
    for name, value in expected.items():
        sites = identities.get(name)
        assert sites, f"{name} is not a declared format identity"
        assert [site["value"] for site in sites] == [value], name


@pytest.mark.parametrize(
    "relative,family",
    [
        ("contract/fixtures/connector_pack_digest_vectors.json", "connector-pack"),
        ("contract/fixtures/decision_digest_vectors.json", "decision"),
    ],
)
def test_the_digest_vectors_are_committed_and_named(relative: str, family: str) -> None:
    document = _json(relative)
    assert document["schema"] == "eg-digest-vectors/v1"
    assert document["family"] == family
    assert document["vectors"], f"{relative} carries no vectors"
    for vector in document["vectors"]:
        assert vector["name"], relative
        assert vector["domain"], f"{vector['name']} names no digest domain"
        assert vector["sha256"], f"{vector['name']} carries no expected value"
    names = [vector["name"] for vector in document["vectors"]]
    assert len(names) == len(set(names)), f"{relative} repeats a vector name"
