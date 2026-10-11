"""Generated GuardrailEntailment transport and typed-result contract checks."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from _client_fixtures import PayloadTransport
from pydantic import ValidationError

from epistemic_graph.generated import storage
from epistemic_graph.generated._runtime import ContractViolation, OpaqueResult

pytestmark = pytest.mark.no_engine


def _payload() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ontology_digest": "sha256:" + "a" * 64,
        "task_iri": "eg:task/research",
        "rules": [
            {
                "rule_id": "rule:audit",
                "source_generation": "generation-7",
                "entailment_path": [
                    {"narrower": "eg:task/research", "broader": "eg:task"}
                ],
            }
        ],
    }


def test_generated_guardrail_send_and_decode_preserve_entailment_premises() -> None:
    client = PayloadTransport({"GuardrailEntailment": _payload()})
    params = {"request": {"tenant_id": "tenant-a", "task_iri": "eg:task/research"}}
    result = asyncio.run(
        storage.send_guardrail_entailment(client, params, graph="__commons__")
    )
    assert client.sent[0].method == "GuardrailEntailment"
    assert client.sent[0].params == params
    assert client.sent[0].graph == "__commons__"
    decoded = storage.decode_guardrail_entailment(result)
    assert decoded.model_dump(mode="json") == _payload()
    assert decoded.rules[0].entailment_path[0].broader == "eg:task"


def test_generated_guardrail_rejects_missing_and_extra_request_fields() -> None:
    client = PayloadTransport({"GuardrailEntailment": _payload()})
    for request in [
        {"tenant_id": "tenant-a"},
        {"tenant_id": "tenant-a", "task_iri": "eg:task", "rules": []},
    ]:
        with pytest.raises(ValidationError):
            asyncio.run(storage.send_guardrail_entailment(client, {"request": request}))
    assert not client.sent


def test_generated_guardrail_decoder_refuses_malformed_or_cross_method_results() -> (
    None
):
    payload = _payload()
    del payload["rules"][0]["source_generation"]
    with pytest.raises(ContractViolation):
        storage.decode_guardrail_entailment(
            OpaqueResult("GuardrailEntailment", payload)
        )
    with pytest.raises(ValueError, match="CapabilityCoverage"):
        storage.decode_guardrail_entailment(
            OpaqueResult("CapabilityCoverage", _payload())
        )


def test_generated_guardrail_catalog_is_read_only_and_declares_store_errors() -> None:
    root = Path(__file__).resolve().parents[1]
    methods = json.loads((root / "contract/methods.json").read_text())["methods"]
    method = next(row for row in methods if row["id"] == "GuardrailEntailment")
    assert method["policy"]["mutates"] is False
    assert method["policy"]["authz_action"] == "agent:assemble-read"
    assert {
        "INVALID_ARGUMENT",
        "ACCESS_DENIED",
        "GUARDRAIL_SCOPE_TOO_LARGE",
        "MALFORMED_GUARDRAIL_RULE",
    } <= set(method["error_set"])
