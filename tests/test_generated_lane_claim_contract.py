"""Current lane/claim contract: engine-owned bounds and wire scalar types."""

from __future__ import annotations

import json
from typing import Any

import pytest
from _method_vectors import VECTORS, vector_request
from pydantic import ValidationError

from epistemic_graph.generated import models

pytestmark = pytest.mark.no_engine


def _request(method: str) -> dict[str, Any]:
    vector = next(item for item in VECTORS if item["method"] == method)
    return vector_request(vector)["params"]["request"]


def _lane() -> dict[str, Any]:
    intent = _request("ReserveDevelopmentLane")["intent"]
    # Isolate each admission test from the independently tested vector defect.
    intent["input_fingerprint"] = "v1:" + "a" * 64
    return intent


def test_canonical_lane_vector_has_runtime_valid_fingerprint() -> None:
    fingerprint = _request("ReserveDevelopmentLane")["intent"]["input_fingerprint"]
    assert len(fingerprint.encode("utf-8")) == 67
    assert fingerprint.startswith("v1:")
    assert all(char in "0123456789abcdef" for char in fingerprint[3:])


@pytest.mark.parametrize(
    "field",
    [
        "tenant_ref",
        "request_id",
        "lane_id",
        "repository_id",
        "base_ref",
        "branch",
        "workspace_ref",
        "owner_id",
        "session_id",
        "fairness_group",
        "quota_policy_name",
        "quota_policy_version",
        "host_target_alias",
        "host_ref",
        "resource_reservation_id",
    ],
)
@pytest.mark.parametrize(
    ("value", "accepted"),
    [
        ("x", True),
        ("x" * 257, True),
        ("x" * 512, True),
        ("x" * 513, False),
        ("é" * 256, True),
        ("é" * 257, False),
        ("😀" * 128, True),
        ("😀" * 129, False),
        ("", False),
        ("x\x00y", False),
        ("x\x1fy", False),
    ],
)
def test_lane_text_uses_utf8_bytes(field: str, value: str, accepted: bool) -> None:
    intent = _lane()
    intent["host_target_kind"] = "inventory_alias"
    intent["host_target_alias"] = "host:one"
    intent[field] = value
    if accepted:
        parsed = models.DevelopmentLaneIntent.model_validate(intent)
        assert getattr(parsed, field) == value
    else:
        with pytest.raises(ValidationError) as error:
            models.DevelopmentLaneIntent.model_validate(intent)
        assert any(item["loc"] == (field,) for item in error.value.errors())


@pytest.mark.parametrize(
    "fingerprint",
    [
        "v2:" + "a" * 64,
        "v1:" + "a" * 63,
        "v1:" + "a" * 65,
        "v1:" + "A" * 64,
        "v1:" + "g" * 64,
        "v1:" + "a" * 64 + "\n",
    ],
)
def test_lane_fingerprint_refuses_noncanonical_values(fingerprint: str) -> None:
    intent = _lane()
    intent["input_fingerprint"] = fingerprint
    with pytest.raises(ValidationError) as error:
        models.DevelopmentLaneIntent.model_validate(intent)
    assert any(item["loc"] == ("input_fingerprint",) for item in error.value.errors())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("lease_ms", 0),
        ("max_tenant_in_flight", 0),
        ("max_tenant_in_flight", 4097),
        ("now_ms", "1"),
        ("lease_ms", 1.0),
        ("max_tenant_in_flight", True),
        ("tenant_ref", ""),
        ("worker_ref", " \t"),
    ],
)
def test_claim_request_refuses_invalid_bounds_and_types(field: str, value: Any) -> None:
    request = _request("ClaimWorkItem")
    request[field] = value
    with pytest.raises(ValidationError) as error:
        models.ClaimWorkItemRequest.model_validate(request)
    assert any(item["loc"] == (field,) for item in error.value.errors())


@pytest.mark.parametrize("quota", [1, 4096])
def test_claim_boundary_values_and_wire_version_are_accepted(quota: int) -> None:
    request = _request("ClaimWorkItem")
    request.update(lease_ms=1, max_tenant_in_flight=quota)
    parsed = models.ClaimWorkItemRequest.model_validate_json(json.dumps(request))
    assert parsed.model_dump(mode="json") == request


@pytest.mark.parametrize(
    ("field", "value"), [("ttl_ms", True), ("predicted_disk_bytes", "1")]
)
def test_lane_integer_fields_do_not_coerce(field: str, value: Any) -> None:
    intent = _lane()
    intent[field] = value
    with pytest.raises(ValidationError) as error:
        models.DevelopmentLaneIntent.model_validate(intent)
    assert any(item["loc"] == (field,) for item in error.value.errors())


@pytest.mark.parametrize("kind", ["local", "inventory_alias"])
def test_lane_wire_enum_strings_round_trip(kind: str) -> None:
    intent = _lane()
    intent["host_target_kind"] = kind
    intent["host_target_alias"] = "host:one" if kind == "inventory_alias" else None
    parsed = models.DevelopmentLaneIntent.model_validate_json(json.dumps(intent))
    assert parsed.model_dump(mode="json") == intent


@pytest.mark.parametrize(
    ("model_name", "field"),
    [
        ("ClaimWorkItemRequest", "work_item_id"),
        ("ClaimWorkItemRequest", "queue_ref"),
        ("ClaimWorkItemRequest", "resource_class"),
        ("ClaimWorkItemRequest", "fairness_group"),
        ("DevelopmentLaneIntent", "host_target_alias"),
    ],
)
@pytest.mark.parametrize("present", [True, False])
def test_required_nullable_fields_preserve_explicit_presence(
    model_name: str, field: str, present: bool
) -> None:
    payload = (
        _lane() if model_name == "DevelopmentLaneIntent" else _request("ClaimWorkItem")
    )
    payload[field] = None
    model = getattr(models, model_name)
    if present:
        assert model.model_validate(payload).model_dump(mode="json")[field] is None
    else:
        del payload[field]
        with pytest.raises(ValidationError) as error:
            model.model_validate(payload)
        assert any(item["loc"] == (field,) for item in error.value.errors())
