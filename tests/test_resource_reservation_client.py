"""Pure client validation for the dark native reservation protocol."""

from __future__ import annotations

import pytest

from epistemic_graph.client import (
    _resource_reservation_record,
    _resource_reservation_request,
    _resource_reservation_result,
    _resource_reservation_status_result,
)

pytestmark = pytest.mark.no_engine


def _request() -> dict[str, object]:
    return {
        "schema_version": "1",
        "tenant_ref": "tenant-a",
        "work_item_id": "work-1",
        "owner_id": "worker-a",
        "fence": "1",
        "lease_epoch": 1,
        "fencing_token": 1,
        "attempt": 1,
        "reservation_id": "reservation-1",
        "input_fingerprint": "v1:" + "a" * 64,
        "profile_name": "light-check",
        "profile_version": "1",
        "host_ref": "host-1",
        "requirement": {
            "cpu_weight": 1,
            "memory_mib": 256,
            "disk_mib": 256,
            "process_slots": 1,
        },
        "target_kind": "local",
        "target_alias": None,
        "repository_id": "repo",
        "branch": "main",
        "concurrency_key": "light-check",
        "concurrency_limit": None,
        "repository_exclusive": False,
        "branch_exclusive": False,
        "required_labels": [],
        "anti_affinity": [],
        "fairness_group": "default",
        "fairness_cost": 1,
        "disk_low_watermark_mib": None,
        "disk_high_watermark_mib": None,
        "disk_policy_key": "none",
        "reserved_at_ms": 10,
        "expires_at_ms": 910,
        "idempotency_key": "invocation-1",
        "now_ms": 10,
        "expected_host_revision": None,
        "expected_lifecycle_revision": None,
    }


def _record() -> dict[str, object]:
    return {
        "reservation_id": "reservation-1",
        "tenant_ref": "tenant-a",
        "owner_id": "worker-a",
        "work_item_id": "work-1",
        "fence": "1",
        "attempt": 1,
        "lease_epoch": 1,
        "fencing_token": 1,
        "input_fingerprint": "v1:" + "a" * 64,
        "host_ref": "host-1",
        "profile_name": "light-check",
        "profile_version": "1",
        "requirement": {
            "cpu_weight": 1,
            "memory_mib": 256,
            "disk_mib": 256,
            "process_slots": 1,
        },
        "capacity_snapshot": {
            "cpu_weight": 8,
            "memory_mib": 8192,
            "disk_mib": 10000,
            "process_slots": 4,
            "host_revision": 1,
        },
        "selected_target": {
            "kind": "local",
            "alias": None,
            "capability_labels": ["linux"],
        },
        "target_kind": "local",
        "target_alias": None,
        "repository_id": "repo",
        "branch": "main",
        "concurrency_key": "light-check",
        "concurrency_limit": None,
        "repository_exclusive": False,
        "branch_exclusive": False,
        "required_labels": [],
        "anti_affinity": [],
        "fairness_group": "default",
        "fairness_cost": 1,
        "disk_low_watermark_mib": None,
        "disk_high_watermark_mib": None,
        "disk_policy_key": "none",
        "reserved_at_ms": 10,
        "expires_at_ms": 910,
        "state": "reserved",
        "revision": 1,
        "lifecycle_revision": 1,
        "tombstone": False,
    }


def test_client_rejects_noncanonical_profile_versions() -> None:
    request = _request()
    request["profile_version"] = "01"
    with pytest.raises(ValueError, match="canonical integer"):
        _resource_reservation_request(request)

    record = _record()
    record["profile_version"] = "01"
    with pytest.raises(ValueError, match="canonical integer"):
        _resource_reservation_record(record)


def test_client_bounds_changed_ids_and_rejects_duplicate_labels() -> None:
    result = {
        "schema_version": "1",
        "decision": "accepted",
        "reservation_id": "reservation-1",
        "work_item_id": "work-1",
        "attempt": 1,
        "lease_epoch": 1,
        "fencing_token": 1,
        "lifecycle_revision": 1,
        "host_ref": "host-1",
        "host_revision": 1,
        "record": None,
        "state": "absent",
        "held_cpu_weight": 0,
        "held_memory_mib": 0,
        "held_disk_mib": 0,
        "held_process_slots": 0,
        "fairness_debt": 0,
        "tombstone": False,
        "changed_work_item_ids": ["work-1"] * 2,
    }
    with pytest.raises(ValueError, match="changed ids"):
        _resource_reservation_result(result)

    request = _request()
    request["required_labels"] = ["linux", "linux"]
    with pytest.raises(ValueError, match="unique"):
        _resource_reservation_request(request)


def _status_result(host_snapshot: dict[str, object] | None) -> dict[str, object]:
    return {
        "schema_version": "1",
        "complete": True,
        "next_cursor": None,
        "host_snapshot": host_snapshot,
        "host_ref": "host-1",
        "host_revision": 1,
        "held_cpu_weight": 0,
        "held_memory_mib": 0,
        "held_disk_mib": 0,
        "held_process_slots": 0,
        "fairness_debt": 0,
        "reservations": [],
        "orphan_count": 0,
        "superseded_count": 0,
    }


def _host_snapshot() -> dict[str, object]:
    capacity = {
        "cpu_weight": 8,
        "memory_mib": 8192,
        "disk_mib": 10000,
        "process_slots": 4,
    }
    return {
        "host_ref": "host-1",
        "revision": 1,
        "capacity": capacity,
        "observed": {**capacity, "cpu_weight": 1, "memory_mib": 256, "disk_mib": 100, "process_slots": 1},
        "heartbeat_at_ms": 10,
        "heartbeat_ttl_ms": 1000,
        "draining": False,
        "quarantined": False,
        "labels": [],
        "target_kind": "local",
        "target_alias": None,
        "disk_used_mib": 100,
        "disk_capacity_mib": 10000,
        "held_cpu_weight": 0,
        "held_memory_mib": 0,
        "held_disk_mib": 0,
        "held_process_slots": 0,
        "disk_policies": [],
    }


def test_status_reader_omits_ordinary_snapshot_and_accepts_only_valid_aggregate_snapshot() -> None:
    assert _resource_reservation_status_result(_status_result(None))["host_snapshot"] is None
    assert _resource_reservation_status_result(_status_result(_host_snapshot()))["host_snapshot"] is not None

    invalid_ttl = _host_snapshot()
    invalid_ttl["heartbeat_ttl_ms"] = 0
    with pytest.raises(ValueError, match="heartbeat_ttl_ms"):
        _resource_reservation_status_result(_status_result(invalid_ttl))

    invalid_target = _host_snapshot()
    invalid_target["target_kind"] = "inventory_alias"
    with pytest.raises(ValueError, match="target_alias"):
        _resource_reservation_status_result(_status_result(invalid_target))
