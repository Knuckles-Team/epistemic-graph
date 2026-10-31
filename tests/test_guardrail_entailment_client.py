"""Generated GuardrailEntailment transport and typed-result contract checks."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from _client_fixtures import PayloadTransport
from pydantic import ValidationError

from medulla.generated import storage
from medulla.generated._runtime import ContractViolation, OpaqueResult

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
