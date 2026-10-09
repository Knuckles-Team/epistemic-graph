"""EG-CONTRACT-R048: generated DTOs refuse coerced, empty and duplicate inputs."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from epistemic_graph.generated import models

pytestmark = pytest.mark.no_engine


def _claim() -> dict[str, Any]:
    return {
        "schema_version": "1",
        "tenant_ref": "tenant:test",
        "work_item_id": None,
        "queue_ref": None,
        "resource_class": None,
        "fairness_group": None,
        "worker_ref": "worker:test",
        "now_ms": 1,
        "lease_ms": 1,
        "max_tenant_in_flight": 1,
    }


def _bundle() -> dict[str, Any]:
    return {
        "schema_version": "1",
        "bundle_id": "bundle:test",
        "resolved": False,
        "answer_ref": None,
        "claims": [],
        "policy_exclusions": ["x"],
        "next_action_refs": [],
    }


def test_valid_inputs_are_accepted() -> None:
    models.ClaimWorkItemRequest.model_validate(_claim())
    models.EvidenceBundle.model_validate(_bundle())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant_ref", ""),
        ("lease_ms", 0),
        ("now_ms", "1"),
        ("max_tenant_in_flight", 0),
        ("max_tenant_in_flight", 4097),
    ],
)
def test_claim_request_refuses(field: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        models.ClaimWorkItemRequest.model_validate({**_claim(), field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("resolved", "false"),
        ("resolved", 0),
        ("policy_exclusions", ["x", "x"]),
        ("bundle_id", ""),
        ("bundle_id", "   "),
    ],
)
def test_evidence_bundle_refuses(field: str, value: Any) -> None:
    with pytest.raises(ValidationError):
        models.EvidenceBundle.model_validate({**_bundle(), field: value})
