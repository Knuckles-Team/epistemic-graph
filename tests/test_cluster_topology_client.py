"""Bounded, authenticated ClusterMembers client fixtures (NE-187).

These fixtures never contact an engine.  They exercise the client-side
authority boundary: only a schema-v1 snapshot signed by the configured secret,
bound to the active verified context, and no older than the last accepted
membership/placement epochs may expose endpoints to callers.
"""

from __future__ import annotations

import copy
import hashlib
import hmac
import inspect
import json
from typing import Any

import pytest

from epistemic_graph.client import ClusterTopologyClient

pytestmark = pytest.mark.no_engine


class _FakeClient:
    def __init__(self, answer: dict[str, Any]) -> None:
        self._answer = answer
        self._auth_secret = "topology-test-secret"

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        assert method == "ClusterMembers"
        assert params is None
        return self._answer

    def _effective_verified_context(self) -> dict[str, str]:
        return {
            "tenant": "tenant-a",
            "principal": "principal-a",
            "agent_id": "agent-a",
        }


def _snapshot(fake: _FakeClient) -> dict[str, Any]:
    topology = ClusterTopologyClient(fake)  # type: ignore[arg-type]
    cluster_id = "sha256:" + "a" * 64
    node_id = 7
    identity = topology._member_identity(cluster_id, node_id)
    members = [
        {
            "node_id": node_id,
            "member_identity": identity,
            "role": "leader",
            "client_endpoint": "tls://graph-a.example:8443",
            "tls_name": "graph-a.example",
            "health": "healthy",
            "certificate": {
                "id": "cert-ref-a",
                "rotation_epoch": 2,
                "not_before_ms": 100,
                "not_after_ms": 200,
            },
        }
    ]
    canonical_groups = [
        [
            0,
            node_id,
            [
                [
                    node_id,
                    identity,
                    "leader",
                    "tls://graph-a.example:8443",
                    "graph-a.example",
                    "healthy",
                    "cert-ref-a",
                    2,
                    100,
                    200,
                ]
            ],
        ]
    ]
    context = fake._effective_verified_context()
    payload = json.dumps(
        [
            "cluster-discovery-v1",
            cluster_id,
            4,
            9,
            context["tenant"],
            context["principal"],
            context["agent_id"],
            canonical_groups,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    signature = "hmac-sha256:" + hmac.new(
        fake._auth_secret.encode(),
        ClusterTopologyClient._DISCOVERY_DOMAIN + payload,
        hashlib.sha256,
    ).hexdigest()
    return {
        "schema_version": 1,
        "cluster_id": cluster_id,
        "epoch": 4,
        "membership_epoch": 4,
        "placement_epoch": 9,
        "leader": {"group_id": 0, "node_id": node_id},
        "leaders": [{"group_id": 0, "node_id": node_id}],
        "groups": [{"group_id": 0, "leader_id": node_id, "members": members}],
        "auth_binding": {
            key: "sha256:" + hashlib.sha256(context[source].encode()).hexdigest()
            for key, source in (
                ("tenant_digest", "tenant"),
                ("principal_digest", "principal"),
                ("agent_digest", "agent_id"),
            )
        },
        "signature": signature,
    }


@pytest.mark.asyncio
async def test_members_accepts_one_signed_context_bound_snapshot() -> None:
    fake = _FakeClient({})
    fake._answer = _snapshot(fake)
    client = ClusterTopologyClient(fake)  # type: ignore[arg-type]

    answer = await client.members(
        expected_cluster_id=fake._answer["cluster_id"],
        min_membership_epoch=4,
        min_placement_epoch=9,
    )

    assert answer["groups"][0]["members"][0]["client_endpoint"] == "tls://graph-a.example:8443"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda answer: answer.pop("signature"),
        lambda answer: answer.__setitem__("cluster_id", "sha256:" + "b" * 64),
        lambda answer: answer.__setitem__("membership_epoch", 3),
        lambda answer: answer["auth_binding"].__setitem__(
            "tenant_digest", "sha256:" + "f" * 64
        ),
        lambda answer: answer["groups"][0]["members"][0].__setitem__(
            "client_endpoint", "tcp://caller-supplied.invalid:1/path"
        ),
    ],
)
async def test_members_rejects_unsigned_stale_cross_bound_or_forged_snapshot(mutation: Any) -> None:
    fake = _FakeClient({})
    answer = _snapshot(fake)
    fake._answer = answer
    mutation(answer)
    client = ClusterTopologyClient(fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        await client.members()


def test_members_has_no_caller_supplied_endpoint_authority() -> None:
    parameters = inspect.signature(ClusterTopologyClient.members).parameters
    assert "endpoint" not in parameters
    assert "endpoints" not in parameters
