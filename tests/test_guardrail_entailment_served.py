"""R127.2.3 acceptance through the generated client and real compiled engine.

The session fixture in conftest starts and authenticates the native server.
Unlike the no-engine client contract probes, these tests publish durable
GuardrailRule components over the wire and query the actual rule snapshot.
"""

from __future__ import annotations

import os
import uuid

import pytest
from conftest import TEST_TENANT, request_context
from test_canonical_body_served import _library_context, _skill_draft

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.generated import storage


async def _publish_rule(
    client: EpistemicGraphClient, rule_id: str, task_class: str, generation: str
) -> None:
    component = _skill_draft(rule_id)
    component.update(
        kind="guardrail_rule",
        summary="generated guardrail acceptance",
        attributes={
            "applies_to_class": task_class,
            "source_generation": generation,
        },
    )
    await storage.send_agent_component(
        client,
        {
            "op": {
                "op": "publish",
                "request": {
                    "context": _library_context(0),
                    "component": component,
                    "evaluation_receipt_digest": None,
                },
            }
        },
        idempotency_key=f"publish-{rule_id}",
    )


@pytest.mark.spec("EG-DECISION-ENGINE-R127.2.3")
@pytest.mark.asyncio
async def test_generated_guardrail_entailment_queries_published_rules() -> None:
    client = await EpistemicGraphClient.connect(
        socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
        verified_context=request_context(),
    )
    nonce = uuid.uuid4().hex
    parent_id, exact_id = f"rule:parent:{nonce}", f"rule:exact:{nonce}"
    generation = f"generation:{nonce}"
    try:
        await _publish_rule(client, parent_id, "eg:task", generation)
        await _publish_rule(client, exact_id, "eg:task/research", generation)
        result = storage.decode_guardrail_entailment(
            await storage.send_guardrail_entailment(
                client,
                {"request": {"tenant_id": TEST_TENANT, "task_iri": "eg:task/research"}},
            )
        )
        assert result.schema_version == 1
        assert result.task_iri == "eg:task/research"
        assert result.ontology_digest.startswith("sha256:")
        assert len(result.ontology_digest) == len("sha256:") + 64
        rules = {rule.rule_id: rule for rule in result.rules}
        assert rules[parent_id].source_generation == generation
        assert [step.model_dump() for step in rules[parent_id].entailment_path] == [
            {"narrower": "eg:task/research", "broader": "eg:task"}
        ]
        assert rules[exact_id].source_generation == generation
        assert rules[exact_id].entailment_path == []

        unrelated_task = f"urn:guardrail-acceptance:unrelated:{nonce}"
        unrelated = storage.decode_guardrail_entailment(
            await storage.send_guardrail_entailment(
                client,
                {"request": {"tenant_id": TEST_TENANT, "task_iri": unrelated_task}},
            )
        )
        assert unrelated.task_iri == unrelated_task
        assert unrelated.ontology_digest == result.ontology_digest
        assert unrelated.rules == []

        with pytest.raises(RuntimeError, match="ACCESS_DENIED"):
            await storage.send_guardrail_entailment(
                client,
                {
                    "request": {
                        "tenant_id": f"foreign:{nonce}",
                        "task_iri": "eg:task/research",
                    }
                },
            )
    finally:
        await client.close()
