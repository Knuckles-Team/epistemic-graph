"""EG-DECISION-ENGINE-R131.6: the limit guard over the served ``Decide`` wire.

An oversize order is submitted to ``Decide`` under a pinned policy that holds
a position-size limit. The answer is a typed ``limit_breach`` abstention naming
the rule. Committing that record through ``DecisionLog`` writes the breach as a
``LimitBreachObservation`` graph node linked to the signal node.
"""

import uuid

import pytest
from _decision_resident_fixture import publication, publish, request, schema_body
from conftest import TEST_TENANT
from test_task_plan_served import authenticated_plan_client  # noqa: F401

from epistemic_graph.decision._digest import policy_digest
from epistemic_graph.decision_client import commit_op, decision_policy_fields

pytestmark = pytest.mark.no_engine

SIGNAL = "urn:signal:order-1"


def _policy():
    """The engine default policy plus one limit rule, in declaration order."""
    q = {"numerator": 1, "denominator": 20}
    return {
        "schema_version": 1,
        "objective": {
            "order": "lexicographic",
            "levels": [
                "uncovered",
                "components",
                "declared_cost",
                "declared_p95_latency",
            ],
        },
        "unknown_cost": "exclude_when_strict",
        "accepted_gap": "0",
        "node_budget": 100000,
        "max_templates": 8,
        "max_slots": 6,
        "max_nogood_rounds": 8,
        "max_why_not_per_slot": 3,
        "a2a_requires_observation": True,
        "cold_start": {"mode": "deterministic_only"},
        "statistical": {
            "alpha": {"numerator": 1, "denominator": 10},
            "epsilon": q,
            "delta": q,
            "n_min": 100,
            "min_support": 10,
            "min_ess": {"scale": "q32", "value": 50 << 32},
            "min_outcome_fidelity": "tool_calls",
            "tenant_public_features": False,
            "audit_sample": q,
            "approved_commit_principals": [],
            "compact_after_ms": None,
            "drop_blob_after_ms": None,
            "limit_rules": [
                {
                    "id": "max-position",
                    "metric": "position_size",
                    "bound": 10000,
                    "scope": "account:a",
                }
            ],
        },
    }


def test_policy_digest_is_over_the_compact_declaration_order_json():
    # The same digest scheme as Rust `policy_digest`: pinned by the shared
    # assembly golden (tests/test_decision_verifier.py) for the base policy.
    fields = decision_policy_fields(_policy())
    assert fields["content_digest"] == policy_digest(_policy())
    assert fields["content_digest"].startswith("sha256:")
    assert fields["attributes"]["decision.policy"].startswith('{"schema_version":1,')


def _action(position):
    return {
        "graph": "__commons__",
        "signal_iri": SIGNAL,
        "observations": [
            {"metric": "position_size", "scope": "account:a", "value": position}
        ],
        "requested_bounds": [],
    }


def _publish_policy(client):
    component_id = "limit-policy-" + uuid.uuid4().hex
    published = publication(component_id, "decision_policy", schema_body())
    published["op"]["request"]["component"].update(decision_policy_fields(_policy()))
    result = client._send("AgentComponent", published)
    return {
        "component_id": component_id,
        "kind": "decision_policy",
        "definition_digest": result["component"]["definition_digest"],
    }


@pytest.mark.spec("EG-DECISION-ENGINE-R131.6")
def test_oversize_order_refused_and_recorded(authenticated_plan_client):  # noqa: F811
    client = authenticated_plan_client
    try:
        schema = publish(
            client, "limit-schema-" + uuid.uuid4().hex, "feature_schema", schema_body()
        )
        policy = _publish_policy(client)
        client.nodes.add(SIGNAL, {"node_type": "Signal"})

        def ask(position):
            asked = request(schema)
            asked["policy"] = {"policy": "pinned", "component": policy}
            asked["guarded_action"] = _action(position)
            return client._send("Decide", {"request": asked})["records"][0]

        within = ask(9000)
        assert within["outcome"]["outcome"] != "abstained" or not any(
            "limit_breach" in str(reason) for reason in within["outcome"]["reasons"]
        )

        record = ask(12000)
        assert record["outcome"] == {
            "outcome": "abstained",
            "reasons": [{"reason": "limit_breach", "rule_id": "max-position"}],
        }
        assert record["inputs"]["guarded_action"]["signal_iri"] == SIGNAL

        client._send("DecisionLog", {"op": commit_op(record)})
        observation = f"limit_breach:{record['record_id']}:max-position"
        assert client.nodes.neighbors(observation) == [SIGNAL]
        properties = client.nodes.properties(observation)
        assert properties["rule_id"] == "max-position"
        assert int(properties["observed"]) == 12000
        assert int(properties["bound"]) == 10000
    finally:
        client.close()
