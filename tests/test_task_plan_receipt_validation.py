"""EG-DECISION-ENGINE-R134.111.4: preserve receipt refusal order on extraction.

These unit checks exercise already-decoded content, not native transport or
strict generated-model validation (covered by the served task-plan suite).
"""

import copy
import hashlib

import pytest

from medulla import decision_client as dc
from medulla.decision._digest import digest_text

pytestmark = pytest.mark.no_engine


def question():
    return {
        "question": {"kind": "task_plan", "safety": "ordinary"},
        "head": None,
        "belief_as_of": [],
        "tenant_id": "tenant",
        "params": [{"name": "task_plan", "value": {"type": "task_plan"}}],
        "feature_schema": {},
        "policy": {},
    }


@pytest.mark.spec("EG-DECISION-ENGINE-R134.111.4")
def test_question_refuses_before_parameter_and_principal():
    asked = question()
    asked["question"]["kind"] = "other"
    asked["params"] = []
    with pytest.raises(ValueError, match="mismatch: question$"):
        dc._verify_task_plan_question(asked, "")
    asked["question"]["kind"] = "task_plan"
    with pytest.raises(ValueError, match="mismatch: proposal parameter$"):
        dc._verify_task_plan_question(asked, "")
    with pytest.raises(ValueError, match="mismatch: expected caller principal$"):
        dc._verify_task_plan_question(question(), " ")


def identity(asked):
    return {
        "caller_principal": "principal:sha256:" + hashlib.sha256(b"local").hexdigest(),
        "schema_version": 2,
        "tenant_id": asked["tenant_id"],
        "question": asked["question"],
        "record_id": "record",
        "record_digest": "digest",
        "evidence_class": "claim",
        "resolution_kind": "constraint",
        "outcome": {"outcome": "advisory", "scores": [], "calibrated": False},
        "inputs": {
            key: asked[key] for key in ("params", "feature_schema", "head", "policy")
        },
    }


@pytest.mark.spec("EG-DECISION-ENGINE-R134.111.4")
def test_identity_refusal_precedence_and_local_principal_binding():
    asked = question()
    received = identity(asked)
    dc._verify_task_plan_identity(received, asked, "local")
    received["tenant_id"] = "foreign"
    received["evidence_class"] = "measurement"
    with pytest.raises(ValueError, match="mismatch: caller principal$"):
        dc._verify_task_plan_identity(received, asked, "foreign")
    with pytest.raises(ValueError, match="mismatch: identity$"):
        dc._verify_task_plan_identity(received, asked, "local")
    received["tenant_id"] = asked["tenant_id"]
    with pytest.raises(ValueError, match="mismatch: evaluate-only evidence$"):
        dc._verify_task_plan_identity(received, asked, "local")


def claims():
    body = {
        "tasks": [{"task_id": "a", "prompt": "Bounded work", "depends_on": []}],
        "output_task_id": "a",
        "max_concurrency": 1,
        "max_steps": 1,
    }
    plan = {
        **copy.deepcopy(body),
        "total_tasks": 1,
        "policy_revision": "revision",
        "policy_digest": "policy",
    }
    plan["input_digest"] = digest_text("eg/task-plan-input/v1", body)
    plan["plan_digest"] = digest_text(
        "eg/task-plan/v1",
        [
            body["tasks"],
            "a",
            1,
            1,
            1,
            "revision",
            "policy",
        ],
    )
    asked = question()
    asked["params"][0]["value"]["value"] = body
    return asked, plan


@pytest.mark.spec("EG-DECISION-ENGINE-R134.111.4")
@pytest.mark.parametrize(
    "field, value, error",
    [
        ("max_concurrency", 2, "max_concurrency"),
        ("total_tasks", 2, "task claims"),
        ("policy_revision", "", "policy"),
        ("input_digest", "foreign", "input digest"),
        ("plan_digest", "foreign", "plan digest"),
    ],
)
def test_plan_claims_preserve_bindings(field, value, error):
    asked, plan = claims()
    assert dc._verify_task_plan_claims(plan, asked) is plan
    plan[field] = value
    with pytest.raises(ValueError, match=f"mismatch: {error}$"):
        dc._verify_task_plan_claims(plan, asked)
