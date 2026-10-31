"""Host-start typed client transport, not issuer or live admission proof."""

# spec: MEDULLA-DURABLE-KERNEL-R030.1.1

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from _client_send_stub import CapturingEngine

from medulla.client import (
    ControlLeaseClient,
    MedullaClient,
    _control_lease_view,
)

pytestmark = pytest.mark.no_engine


def view() -> dict[str, Any]:
    return {
        "lease_id": "host-1",
        "kind": "host.start",
        "status": "active",
        "grant": {},
        "issued_at_ms": 1000,
        "expires_at_ms": 2000,
        "hard_expires_at_ms": 2000,
        "revision": 1,
    }


def test_legacy_grant_json_cannot_become_native_provenance() -> None:
    legacy = view()
    legacy["grant"] = {
        "host_start_provenance": {"schema_version": 1, "issuer_ref": "fake"}
    }
    assert "host_start_provenance" not in _control_lease_view(legacy)


@pytest.mark.parametrize(
    "provenance",
    [
        None,
        {},
        {"schema_version": 1},
        {"issuer_ref": "issuer"},
        {"schema_version": 1, "issuer_ref": "issuer", "extra": True},
        {"schema_version": 1, "issuer_ref": None},
        {"schema_version": 1, "issuer_ref": "   "},
        {"schema_version": 1, "issuer_ref": "é" * 257},
        {"schema_version": True, "issuer_ref": "issuer"},
        {"schema_version": 2, "issuer_ref": "issuer"},
        {"schema_version": 1, "issuer_ref": "issuer\nforged"},
        {"schema_version": 1, "issuer_ref": "issuer\u0085forged"},
    ],
)
def test_malformed_top_level_provenance_refuses(provenance: Any) -> None:
    malformed = view() | {"host_start_provenance": provenance}
    with pytest.raises(RuntimeError, match="provenance"):
        _control_lease_view(malformed)


@pytest.mark.parametrize("kind", ["identity.session", "approval.human", "HOST.START"])
def test_host_provenance_refuses_other_lease_kinds(kind: str) -> None:
    foreign = view() | {
        "kind": kind,
        "host_start_provenance": {"schema_version": 1, "issuer_ref": "native-issuer"},
    }
    with pytest.raises(RuntimeError, match="invalid native host-start provenance"):
        _control_lease_view(foreign)


def test_host_provenance_preserves_exact_utf8_boundary() -> None:
    valid = view() | {
        "host_start_provenance": {"schema_version": 1, "issuer_ref": "é" * 256}
    }
    assert _control_lease_view(valid) is valid


def test_dedicated_issue_sends_binding_without_caller_provenance() -> None:
    binding = {
        "principal_ref": "principal",
        "session_ref": "session",
        "workspace_ref": "workspace",
        "configuration_digest": "a" * 64,
        "interfaces": "terminal",
        "web_logs": False,
        "policy_revision": "policy-1",
        "session_expires_at_ms": 2000,
    }
    issued = view() | {
        "host_start_provenance": {"schema_version": 1, "issuer_ref": "native-issuer"}
    }
    engine = CapturingEngine(
        {"outcome": "issued", "lease": issued, "changed_work_item_ids": ["host-1"]}
    )
    client = ControlLeaseClient(cast(MedullaClient, engine))
    answer = asyncio.run(
        client.issue_host_start(
            tenant="tenant-a",
            lease_id="host-1",
            binding=binding,
            expires_at_ms=2000,
            idempotency_key="issue-1",
        )
    )
    assert answer["lease"] == issued
    assert engine.sent == [
        (
            "IssueHostStartGrant",
            {
                "request": {
                    "tenant": "tenant-a",
                    "lease_id": "host-1",
                    "binding": binding,
                    "expires_at_ms": 2000,
                    "idempotency_key": "issue-1",
                }
            },
        )
    ]
