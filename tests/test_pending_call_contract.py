"""Generated request and client forwarding checks, not native storage acceptance."""

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from epistemic_graph import client as client_module
from epistemic_graph.generated.models import (
    IssueControlLeaseRequest,
    PendingCallConsumption,
    PendingCallRegistration,
    TransitionControlLeaseRequest,
)

pytestmark = [pytest.mark.no_engine, pytest.mark.spec("EG-DURABLE-KERNEL-R039.3")]


def registration() -> dict[str, Any]:
    return {
        "work_item": {
            "work_item_id": "work-a",
            "expected_revision": 1,
            "expected_metadata": {},
        },
        "expected_lease": {
            "worker_ref": "worker-a",
            "lease_epoch": 1,
            "fencing_token": 1,
        },
    }


def test_generated_registration_and_consumption_shapes_are_closed() -> None:
    fields = dict(
        tenant="tenant-a",
        lease_id="pending-a",
        kind="work_item.pending_call",
        grant={"task_id": "task-a"},
        issued_at_ms=1000,
        expires_at_ms=2000,
        hard_expires_at_ms=2000,
        idempotency_key="issue-a",
    )
    issue = IssueControlLeaseRequest.model_validate(
        fields | {"pending_call_registration": registration()}
    )
    assert issue.pending_call_registration is not None
    assert issue.pending_call_registration.work_item.work_item_id == "work-a"
    assert (
        IssueControlLeaseRequest.model_validate(fields).pending_call_registration
        is None
    )
    with pytest.raises(ValidationError):
        IssueControlLeaseRequest.model_validate(
            fields
            | {"pending_call_registration": registration() | {"human_approved": True}}
        )
    consume = TransitionControlLeaseRequest.model_validate(
        dict(
            tenant="tenant-a",
            lease_id="pending-a",
            expected_revision=1,
            to="consumed",
            idempotency_key="consume-a",
            pending_call_consumption={
                "work_item": registration()["work_item"],
                "expected_grant": fields["grant"],
            },
        )
    )
    assert consume.pending_call_consumption is not None
    assert consume.pending_call_consumption.expected_grant == fields["grant"]


def test_client_forwards_optional_pending_preconditions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    async def issue(sender: Any, params: dict[str, Any], **kwargs: Any) -> Any:
        calls.append(params["request"])
        return SimpleNamespace(payload={"outcome": "issued", "lease": None})

    async def consume(sender: Any, params: dict[str, Any], **kwargs: Any) -> Any:
        calls.append(params["request"])
        return SimpleNamespace(payload={"outcome": "applied", "lease": None})

    monkeypatch.setattr(
        client_module._gen.coordination, "send_issue_control_lease", issue
    )
    monkeypatch.setattr(
        client_module._gen.coordination, "send_transition_control_lease", consume
    )
    transport: Any = object()
    client = client_module.ControlLeaseClient(transport)
    asyncio.run(
        client.issue(
            tenant="tenant-a",
            lease_id="pending-a",
            kind="work_item.pending_call",
            grant={},
            issued_at_ms=1000,
            expires_at_ms=2000,
            hard_expires_at_ms=2000,
            idempotency_key="issue-a",
            pending_call_registration=registration(),
        )
    )
    fence = {"work_item": registration()["work_item"], "expected_grant": {}}
    asyncio.run(
        client.transition(
            tenant="tenant-a",
            lease_id="pending-a",
            expected_revision=1,
            to="consumed",
            idempotency_key="consume-a",
            pending_call_consumption=fence,
        )
    )
    assert calls[0]["pending_call_registration"] == registration()
    assert calls[1]["pending_call_consumption"] == fence


@pytest.mark.spec("EG-DURABLE-KERNEL-R039.4")
def test_generated_plan_preimage_round_trips_and_refuses_unknown_authority() -> None:
    plan = {
        "lease_id": "plan-a",
        "expected_revision": 1,
        "expected_kind": "configured.plan",
        "expected_grant": {"op_id": "tool-a"},
    }
    registered = PendingCallRegistration.model_validate(registration() | {"plan": plan})
    consumed = PendingCallConsumption.model_validate(
        {
            "work_item": registration()["work_item"],
            "expected_grant": {},
            "plan": plan,
        }
    )
    assert registered.plan is not None
    assert registered.plan == consumed.plan
    assert registered.plan.model_dump() == plan
    with pytest.raises(ValidationError):
        PendingCallRegistration.model_validate(
            registration() | {"plan": plan | {"human_approved": True}}
        )
