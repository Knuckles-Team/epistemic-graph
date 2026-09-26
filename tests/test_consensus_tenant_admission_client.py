"""Narrow signed client binding for first-contact tenant admission."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from epistemic_graph import generated as _gen
from epistemic_graph.client import ConsensusClient

pytestmark = pytest.mark.no_engine


class _Client:
    def _new_operation_idempotency_key(self):
        return "admission-idem"

    def _sign_context_operation(self, **kwargs):
        self.signed = deepcopy(kwargs)
        return "svc:admission:attested"


@pytest.mark.asyncio
async def test_tenant_admission_signs_exact_method_and_uses_control_graph(monkeypatch):
    calls = []

    async def send(client, params, **kwargs):
        calls.append((client, params, kwargs))
        return SimpleNamespace(payload=True)

    monkeypatch.setattr(
        _gen.security, "send_admit_tenant_principal", send, raising=False
    )
    raw = _Client()
    assert await ConsensusClient(raw).admit_tenant_principal(
        "alice", "acme", signer_id="svc:admission", signer_key="service-key"
    )
    assert raw.signed == {
        "domain": "eg-admit-tenant-principal-v1",
        "method": "AdmitTenantPrincipal",
        "params": {
            "agent_id": "alice",
            "tenant_slug": "acme",
            "signature": "",
        },
        "graph": "__commons__",
        "idempotency_key": "admission-idem",
        "signer_id": "svc:admission",
        "signer_key": "service-key",
    }
    assert calls == [
        (
            raw,
            {
                "agent_id": "alice",
                "tenant_slug": "acme",
                "signature": "svc:admission:attested",
            },
            {"graph": "__commons__", "idempotency_key": "admission-idem"},
        )
    ]


@pytest.mark.asyncio
async def test_tenant_admission_rejects_bad_client_inputs_and_result(monkeypatch):
    async def send(_client, _params, **_kwargs):
        return SimpleNamespace(payload="true")

    monkeypatch.setattr(
        _gen.security, "send_admit_tenant_principal", send, raising=False
    )
    client = ConsensusClient(_Client())
    with pytest.raises(ValueError, match="agent_id"):
        await client.admit_tenant_principal(
            "", "acme", signer_id="svc", signer_key="key"
        )
    with pytest.raises(ValueError, match="tenant_slug"):
        await client.admit_tenant_principal(
            "alice", "", signer_id="svc", signer_key="key"
        )
    with pytest.raises(TypeError, match="non-boolean"):
        await client.admit_tenant_principal(
            "alice", "acme", signer_id="svc", signer_key="key"
        )
