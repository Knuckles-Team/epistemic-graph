"""The Python binding for atomic EG tenant role admission."""

from types import SimpleNamespace

import pytest

from epistemic_graph import generated as _gen
from epistemic_graph.client import RbacClient

pytestmark = pytest.mark.no_engine


@pytest.mark.asyncio
async def test_atomic_tenant_admission_sends_exact_admin_op(monkeypatch):
    calls = []

    async def send(client, params):
        calls.append((client, params))
        return SimpleNamespace(payload=True)

    monkeypatch.setattr(_gen.security, "send_rbac_admin", send)
    raw_client = object()
    assert await RbacClient(raw_client).admit_tenant_principal("alice", "acme") is True
    assert calls == [
        (
            raw_client,
            {
                "op": {
                    "AdmitTenantPrincipal": {"agent_id": "alice", "tenant_slug": "acme"}
                }
            },
        )
    ]


@pytest.mark.asyncio
async def test_atomic_tenant_admission_rejects_malformed_result(monkeypatch):
    async def send(_client, _params):
        return SimpleNamespace(payload="false")

    monkeypatch.setattr(_gen.security, "send_rbac_admin", send)
    with pytest.raises(TypeError, match="non-boolean"):
        await RbacClient(object()).admit_tenant_principal("alice", "acme")
