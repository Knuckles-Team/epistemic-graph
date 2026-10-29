"""The public helper keeps model claims separate from engine authority."""

import pytest

from medulla.decision_client import task_plan_request

pytestmark = pytest.mark.no_engine


# spec: EG-DECISION-ENGINE-R134.111.1
def test_task_plan_request_uses_typed_proposal():
    proposal = {
        "tasks": [{"task_id": "a", "prompt": "Read evidence", "depends_on": []}],
        "output_task_id": "a",
        "max_steps": 1,
        "max_concurrency": 1,
    }
    pin = {
        "component_id": "schema",
        "kind": "feature_schema",
        "definition_digest": "sha256:" + "1" * 64,
    }
    result = task_plan_request("tenant", "question", proposal, pin)
    assert result["question"] == {
        "question_id": "question",
        "kind": "task_plan",
        "safety": "ordinary",
    }
    assert result["head"] is None
    assert result["params"][0]["value"] == {"type": "task_plan", "value": proposal}
    assert result["candidates"]["options"][0]["option_id"] == "a"
    with pytest.raises(ValueError):
        task_plan_request("tenant", "question", {**proposal, "authorize": True}, pin)
    with pytest.raises(ValueError):
        task_plan_request(
            "tenant", "question", {**proposal, "max_concurrency": "1"}, pin
        )
