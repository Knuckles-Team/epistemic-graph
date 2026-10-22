"""Explicit plans over real authenticated and isolated no-auth native wires."""

import copy
import hashlib
import time
import uuid
from typing import Any

import pytest
from _decision_resident_fixture import publication, publish, schema_body
from conftest import (
    TEST_AGENT_ID,
    TEST_AUDIENCE,
    TEST_POLICY_VERSION,
    TEST_SIGNER_KEY,
    TEST_TENANT,
    bootstrap_context,
    request_context,
    strict_server_env,
)

from epistemic_graph.client import EngineResponseError, SyncEpistemicGraphClient
from epistemic_graph.decision_client import task_plan_request, verify_task_plan_receipt

pytestmark = pytest.mark.no_engine  # Each test owns its actual loopback-TCP engine.


@pytest.fixture
def authenticated_plan_client(tmp_path, test_auth_secret):
    from test_native_no_auth_served import server

    env = strict_server_env(
        str(tmp_path / "security"),
        auth_secret=test_auth_secret,
        persist_dir=str(tmp_path / "data"),
    )
    env["EPISTEMIC_GRAPH_ENCRYPTION_KEY"] = "test-epistemic-graph-encryption-key"
    with server(tmp_path, mode="authenticated", extra_env=env) as address:
        tcp = f"{address[0]}:{address[1]}"
        bootstrap = SyncEpistemicGraphClient.connect(
            tcp_addr=tcp,
            auth_secret=test_auth_secret,
            verified_context=bootstrap_context(),
        )
        try:
            bootstrap.consensus.bootstrap_system_identity(
                agent_id=TEST_AGENT_ID,
                signer_id=TEST_AGENT_ID,
                signer_key=TEST_SIGNER_KEY,
            )
        finally:
            bootstrap.close()
        yield SyncEpistemicGraphClient.connect(
            tcp_addr=tcp,
            auth_secret=test_auth_secret,
            verified_context=request_context(),
        )


def _proposal():
    return {
        "tasks": [
            {
                "task_id": "join",
                "prompt": "Synthesize the security and API evidence.",
                "depends_on": ["a", "c"],
            },
            {"task_id": "c", "prompt": "Verify the API findings.", "depends_on": ["b"]},
            {
                "task_id": "b",
                "prompt": "Audit the API with evidence.",
                "depends_on": [],
            },
            {
                "task_id": "a",
                "prompt": "Audit security with evidence.",
                "depends_on": [],
            },
        ],
        "output_task_id": "join",
        "max_concurrency": 2,
        "max_steps": 4,
    }


def _persist_and_read(
    client,
    proposal,
    plan,
    *,
    tenant=TEST_TENANT,
    agent=TEST_AGENT_ID,
    audience=TEST_AUDIENCE,
    policy=TEST_POLICY_VERSION,
    authentication_method="local_process",
):
    """Bind the real evaluated graph through native admission and a live fence."""
    run_id = "task-plan-" + uuid.uuid4().hex
    now_ms = int(time.time() * 1000)
    context = {
        "schema_version": "2",
        "request_id": run_id,
        "trace_id": run_id,
        "subject_id": agent,
        "tenant_id": tenant,
        "agent_id": agent,
        "scopes": ["*"],
        "audience": audience,
        "authentication_method": authentication_method,
        "policy_version": policy,
        "graph": "__commons__",
        "placement_epoch": None,
        "issued_at_ms": now_ms,
        "expires_at_ms": now_ms + 60_000,
    }
    admitted = client.work_items.submit(
        {
            "run_id": run_id,
            "schema_version": "1",
            "context": context,
            "work_item_id": None,
            "idempotency_key": run_id + ":submit",
            "command_digest": plan["plan_digest"].removeprefix("sha256:"),
            "kind": "task-plan-fixture",
            "priority": 0,
            "depends_on": [],
            "input_ref": run_id,
            "policy_digest": plan["policy_digest"],
            "catalog_digest": "fixture-catalog",
            "model_digest": "deterministic-fixture",
            "max_attempts": 1,
            "deadline_unix": None,
            "metadata": {},
            "provenance_refs": [],
            "max_tenant_in_flight": 4096,
        }
    )
    claim = client.work_items.claim(
        {
            "schema_version": "1",
            "tenant_ref": tenant,
            "work_item_id": admitted["work_item_id"],
            "queue_ref": None,
            "resource_class": None,
            "fairness_group": None,
            "worker_ref": run_id,
            "now_ms": now_ms,
            "lease_ms": 60_000,
            "max_tenant_in_flight": 4096,
        },
        idempotency_key=run_id + ":claim",
    )
    assert claim["claimed"] is True
    request = {
        "tenant": tenant,
        "worker_id": run_id,
        "lease_epoch": claim["lease_epoch"],
        "fencing_token": claim["fencing_token"],
        "idempotency_key": run_id + ":plan",
        "task_plan_proposal": proposal,
        "snapshot": {
            "run_id": run_id,
            "revision": 0,
            "observed_at_ms": now_ms,
            "expires_at_ms": now_ms + 30_000,
            "items": [],
            "task_plan": plan,
        },
    }
    first = client._send(
        "PublishCurrentPlan",
        {"request": request},
        idempotency_key=request["idempotency_key"],
    )
    assert (
        client._send(
            "PublishCurrentPlan",
            {"request": request},
            idempotency_key=request["idempotency_key"],
        )
        == first
    )
    assert (
        client._send("GetCurrentPlan", {"tenant": tenant, "run_id": run_id})
        == request["snapshot"]
    )
    # A legacy scaffold update must preserve, rather than erase, the executable DAG.
    updated = copy.deepcopy(request)
    updated.pop("task_plan_proposal")
    updated["snapshot"].pop("task_plan")
    updated["snapshot"]["revision"] = 1
    updated["snapshot"]["items"] = [
        {"text": "Run bounded graph", "status": "in_progress"}
    ]
    updated["idempotency_key"] += ":scaffold"
    retained_receipt = client._send(
        "PublishCurrentPlan",
        {"request": updated},
        idempotency_key=updated["idempotency_key"],
    )
    assert (
        client._send(
            "PublishCurrentPlan",
            {"request": updated},
            idempotency_key=updated["idempotency_key"],
        )
        == retained_receipt
    )
    for timestamp in ("observed_at_ms", "expires_at_ms"):
        changed_time = copy.deepcopy(updated)
        changed_time["snapshot"][timestamp] -= 1
        with pytest.raises(EngineResponseError):
            client._send(
                "PublishCurrentPlan",
                {"request": changed_time},
                idempotency_key=updated["idempotency_key"],
            )
    restored = client._send("GetCurrentPlan", {"tenant": tenant, "run_id": run_id})
    assert restored == {**updated["snapshot"], "task_plan": plan}

    return run_id, restored


# spec: EG-DECISION-ENGINE-R134.111.3
# spec: EG-DECISION-ENGINE-R134.111.5
# spec: EG-DURABLE-KERNEL-R028.5.3
def test_explicit_plan_end_to_end(authenticated_plan_client):
    client = authenticated_plan_client
    try:
        pin = publish(
            client,
            "task-plan-schema-" + uuid.uuid4().hex,
            "feature_schema",
            schema_body(),
        )
        asked = task_plan_request(TEST_TENANT, "fixture-task-plan", _proposal(), pin)
        record = client._send("Decide", {"request": asked})["records"][0]
        plan = record["task_plan"]
        assert [task["task_id"] for task in plan["tasks"]] == ["a", "b", "c", "join"]
        assert plan["tasks"][2]["depends_on"] == ["b"]
        assert plan["max_concurrency"] == 2
        assert plan["total_tasks"] == plan["max_steps"] == 4
        assert plan["policy_revision"] == "bounded-explicit-dag-v1"
        assert record["evidence_class"] == "claim"
        assert record["resolution_kind"] == "constraint"
        assert record["outcome"] == {
            "outcome": "advisory",
            "scores": [],
            "calibrated": False,
        }
        assert record["tenant_id"] == TEST_TENANT
        assert record["record_digest"].startswith("sha256:")
        assert any(
            p["subject"] == "c" and "dependencies=b" in p["fact"]
            for p in record["premises"]
        )
        assert (
            verify_task_plan_receipt(record, asked, expected_principal=TEST_AGENT_ID)
            == plan
        )
        _persist_and_read(client, _proposal(), plan)
        for path, value in [
            (("tenant_id",), "foreign-tenant"),
            (("caller_principal",), "principal:sha256:" + "0" * 64),
            (("question", "question_id"), "different-question"),
            (("evidence_class",), "proof"),
            (("task_plan", "max_concurrency"), 3),
            (("task_plan", "input_digest"), "sha256:" + "0" * 64),
            (("task_plan", "plan_digest"), "sha256:" + "0" * 64),
        ]:
            changed = copy.deepcopy(record)
            target = changed
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with pytest.raises(ValueError, match="task_plan receipt mismatch"):
                verify_task_plan_receipt(
                    changed, asked, expected_principal=TEST_AGENT_ID
                )
        changed = copy.deepcopy(record)
        changed["task_plan"]["tasks"][0]["prompt"] = "Invented task"
        with pytest.raises(ValueError, match="task claims"):
            verify_task_plan_receipt(changed, asked, expected_principal=TEST_AGENT_ID)
        invalid = copy.deepcopy(asked)
        invalid["params"][0]["value"]["value"]["output_task_id"] = "b"
        with pytest.raises(Exception, match="disconnected_output"):
            client._send("Decide", {"request": invalid})
    finally:
        client.close()


# spec: EG-DECISION-ENGINE-R134.111.4
def test_task_plan_refuses_noncanonical_requests(authenticated_plan_client):
    client = authenticated_plan_client
    try:
        pin = publish(
            client,
            "refusal-schema-" + uuid.uuid4().hex,
            "feature_schema",
            schema_body(),
        )
        asked = task_plan_request(TEST_TENANT, "refused-task-plan", _proposal(), pin)
        cases = [
            (("question", "safety"), "security", "ordinary safety"),
            (("head",), pin, "no head"),
            (("belief_as_of",), [1], "belief slices"),
            (("params",), [], "one parameter"),
            (("params",), asked["params"] * 2, "one parameter"),
            (("params", 0, "name"), "model-policy", "must be named task_plan"),
            (
                ("params", 0, "value"),
                {"type": "text", "value": "not a plan"},
                "typed task_plan proposal",
            ),
            (
                ("candidates", "options"),
                list(reversed(asked["candidates"]["options"])),
                "exactly the sorted task ids",
            ),
            (
                ("candidates", "options", 0, "classification"),
                ["model-asserted:policy"],
                "without facts",
            ),
            (
                ("candidates",),
                {
                    "source": "agent_library",
                    "scope": {"kinds": ["tool"], "classification_under": None},
                },
                "declared task ids",
            ),
        ]
        for path, value, detail in cases:
            invalid = copy.deepcopy(asked)
            target: Any = invalid
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with pytest.raises(EngineResponseError, match=detail):
                client._send("Decide", {"request": invalid})
    finally:
        client.close()


# spec: EG-DECISION-ENGINE-R134.111.3
# spec: EG-DURABLE-KERNEL-R028.5.3
@pytest.mark.no_engine
def test_anonymous_task_plan_publication_and_restart(tmp_path):
    from test_native_no_auth_served import server

    expected_principal = "principal:sha256:" + hashlib.sha256(b"anonymous").hexdigest()
    with server(tmp_path) as address:
        client = SyncEpistemicGraphClient.connect(
            auth_mode="none", tcp_addr=f"{address[0]}:{address[1]}"
        )
        try:
            component_id = "anonymous-task-plan-schema-" + uuid.uuid4().hex
            draft = publication(component_id, "feature_schema", schema_body())
            request = draft["op"]["request"]
            request["context"].update(
                tenant_id="local",
                principal=expected_principal,
                caller_principal=expected_principal,
                policy_revision="no-auth",
            )
            request["component"]["tenant_id"] = "local"
            entry = client._send("AgentComponent", draft)["component"]
            pin = {
                "component_id": component_id,
                "kind": "feature_schema",
                "definition_digest": entry["definition_digest"],
            }
            asked = task_plan_request("local", "anonymous-plan", _proposal(), pin)
            record = client._send("Decide", {"request": asked})["records"][0]
            assert record["tenant_id"] == "local"
            assert record["caller_principal"] == expected_principal
            plan = verify_task_plan_receipt(
                record, asked, expected_principal="anonymous"
            )
            with pytest.raises(ValueError, match="caller principal"):
                verify_task_plan_receipt(record, asked, expected_principal="foreign")
            with pytest.raises(Exception, match="context does not match"):
                _persist_and_read(
                    client,
                    _proposal(),
                    plan,
                    tenant="local",
                    agent="anonymous",
                    audience="epistemic-graph",
                    policy="no-auth",
                )
            run_id, snapshot = _persist_and_read(
                client,
                _proposal(),
                plan,
                tenant="local",
                agent="anonymous",
                audience="epistemic-graph",
                policy="no-auth",
                authentication_method="none",
            )
            with pytest.raises(Exception, match="ACCESS_DENIED"):
                client._send("Decide", {"request": {**asked, "tenant_id": "foreign"}})
        finally:
            client.close()
    with server(tmp_path) as address:
        client = SyncEpistemicGraphClient.connect(
            auth_mode="none", tcp_addr=f"{address[0]}:{address[1]}"
        )
        try:
            assert (
                client._send("GetCurrentPlan", {"tenant": "local", "run_id": run_id})
                == snapshot
            )
        finally:
            client.close()
