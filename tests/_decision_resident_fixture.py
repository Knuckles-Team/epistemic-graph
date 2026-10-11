"""Real-wire resident scorer setup, mirroring Rust stat_tests/scorer_tests."""

from __future__ import annotations

from conftest import TEST_TENANT

from epistemic_graph import decision_stat as ds
from epistemic_graph.decision_client import (
    DeclaredOption,
    decide_request,
    declared,
    param,
)

DIGEST = "sha256:" + "1" * 64


def publication(component_id, kind, body, receipt=None):
    context = dict(
        request_id=1,
        principal="principal:sha256:" + "a" * 64,
        caller_principal="principal:sha256:" + "a" * 64,
        attempt_nonce="01" * 32,
        tenant_id=TEST_TENANT,
        actor_scope="action-scope:resident",
        purpose_id="agent-component:publish",
        policy_revision="policy:test",
        policy_digest=DIGEST,
        policy_decision_id="resident:test",
        idempotency_key=component_id,
        expected_revision=0,
        trace_id=None,
        created_at_ms=1,
    )
    component = dict(
        component_id=component_id,
        kind=kind,
        version="1.0.0",
        content_digest=DIGEST,
        content_ref=None,
        facts={"facts": "opaque"},
        provenance={"origin": "native"},
        summary=component_id,
        classification=[],
        requires=[],
        declared_capabilities=[],
        required_capabilities=[],
        declared_required_capabilities=[],
        attributes={},
        tenant_id=TEST_TENANT,
        actor_scope=context["actor_scope"],
        purpose_id=context["purpose_id"],
        policy_digest=DIGEST,
        source_revision="fixture-v1",
        source_revision_digest=DIGEST,
    )
    component.update(ds.body_attributes_for_publish(body))
    return {
        "op": {
            "op": "publish",
            "request": {
                "context": context,
                "component": component,
                "evaluation_receipt_digest": receipt,
            },
        }
    }


def publish(client, component_id, kind, body, receipt=None):
    result = client._send(
        "AgentComponent", publication(component_id, kind, body, receipt)
    )
    entry = result["component"]
    return {
        "component_id": component_id,
        "kind": kind,
        "definition_digest": entry["definition_digest"],
    }


def schema_body():
    return ds.feature_schema_body(
        [
            ds.feature("text", ds.text_bm25("summary", "query")),
            ds.feature("coverage", ds.coverage_fraction("needs")),
        ]
    )


def request(schema, head=None):
    options = [
        DeclaredOption(
            name,
            texts={"summary": summary},
            classification=("eg:capability/retrieval/web-search",),
        )
        for name, summary in [
            ("tool-a-search", "web search engine for pages"),
            ("tool-b-files", "write files to disk"),
            ("tool-c-mail", "send an email message"),
        ]
    ]
    value = decide_request(
        TEST_TENANT,
        ("route.tools", "route", "ordinary"),
        declared(options),
        schema,
        head=head,
        params=[
            param("needs", "iri_list", ["eg:capability/retrieval"]),
            param("query", "text", "web search engine"),
        ],
    )
    value["belief_as_of"] = [1000, 2000] if head else []
    return value


def dataset(matrix):
    return dict(
        schema_version=1,
        feature_schema_digest=ds.encode_body(schema_body())[0],
        feature_names=["text", "coverage"],
        scale="q32",
        items=[
            dict(
                item_id=f"gold-{i:04}",
                recorded_at_ms=1000,
                class_key="route",
                candidate_ids=matrix["candidate_ids"],
                features=[v + (((i % 7) - 3) << 24) for v in matrix["values"]],
                label={
                    "label": "gold",
                    "acceptable": [matrix["candidate_ids"][0]],
                    "source": "synthetic_construction",
                },
                audit_inclusion=None,
            )
            for i in range(400)
        ],
        synthetic=True,
    )


def job_requests(schema, data, artifact=None):
    common = dict(
        tenant_id=TEST_TENANT,
        idempotency_key="resident-fit" if artifact is None else "resident-eval",
        policy={"policy": "default"},
        window={"from_ms": 0, "to_ms": (1 << 64) - 1},
        source={"source": "inline", "dataset": data},
    )
    gold = ds.dataset_digest(data)
    if artifact is None:
        common.update(
            head_kind="option_attention",
            feature_schema=schema,
            label_regime={"regime": "full_label", "gold_set_digest": gold},
            optimiser={
                "max_iterations": 60,
                "tolerance": {"scale": "q32", "value": 1 << 12},
                "seed": 0,
            },
        )
    else:
        common.update(
            candidate={
                "candidate": "draft_artifact",
                "sha256": artifact["draft_sha256"],
                "length": artifact["draft_length"],
            },
            estimators=[],
            gold_set_digest=gold,
            mode={"mode": "off_policy"},
        )
    return {"op": {"op": "submit", "request": common}}


def succeeded(job, kind):
    assert job["state"]["state"] == "succeeded", job
    output = job["state"]["output"]
    assert output["output"] == kind, output
    return output


def fit_and_publish(client):
    schema = publish(client, "resident-schema", "feature_schema", schema_body())
    baseline = client._send("Decide", {"request": request(schema)})["records"][0]
    data = dataset(baseline["inputs"]["feature_matrix"])
    fitted = succeeded(client._send("DecisionFit", job_requests(schema, data)), "fit")
    assert fitted["draft"]["kind"] == "option_attention"
    assert fitted["draft"]["scorer"] is not None
    evaluated = succeeded(
        client._send("DecisionEval", job_requests(schema, data, fitted)), "eval"
    )
    receipt = evaluated["receipt"]
    assert receipt["passed"], receipt["failed_gates"]
    head = publish(
        client,
        "resident-head",
        "decision_head",
        fitted["draft"],
        receipt["receipt_digest"],
    )
    return request(schema, head)


def scores(client, asked):
    record = client._send("Decide", {"request": asked})["records"][0]
    assert record["inputs"]["head"] == asked["head"]
    assert record["synthetic_evidence"] is True
    assert record["explanation"] is None
    points = record["belief"]
    assert [point["as_of_ms"] for point in points] == [1000, 2000]
    for point in points:
        probabilities = point["probabilities"]
        assert len(probabilities) == 3
        assert all(value["scale"] == "q32" for value in probabilities)
        values = [value["value"] for value in probabilities]
        assert all(0 <= value <= ds.Q32_ONE for value in values)
        assert abs(sum(values) - ds.Q32_ONE) <= 8
        assert len(set(values)) > 1, (
            "a constant/no-op scorer cannot satisfy the fixture"
        )
    assert points[0]["probabilities"] == points[1]["probabilities"]
    return ds.canonical_body_bytes(points)
