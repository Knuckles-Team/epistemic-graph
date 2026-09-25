"""Focused metadata and authority contract for durable usage facts."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from epistemic_graph.usage_facts import UsageEventFact, UsageFactStore

KEY = b"stable-test-usage-identity-key-00000000"

pytestmark = pytest.mark.no_engine


class FakeNodes:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    async def create_if_absent(self, node_id: str, properties: dict) -> bool:
        if node_id in self.rows:
            return False
        self.rows[node_id] = properties
        return True

    async def properties(self, node_id: str) -> dict | None:
        return self.rows.get(node_id)


def _client(nodes: FakeNodes, tenant: str = "tenant-a") -> SimpleNamespace:
    claims = {
        "tenant": tenant,
        "principal": "service:usage",
        "agent_id": "service:usage",
    }
    return SimpleNamespace(
        nodes=nodes,
        _auth_secret="fixture-secret",
        _effective_verified_context=lambda: claims,
    )


def _fact() -> UsageEventFact:
    return UsageEventFact(
        event_ref="pref_usage_dedup_" + "a" * 64,
        run_ref="pref_run_" + "b" * 64,
        origin="runtime",
        occurred_at="2026-09-25T00:00:00Z",
        input_tokens=10,
        output_tokens=5,
        cost_microusd=12,
    )


@pytest.mark.asyncio
async def test_immutable_event_replay_and_tenant_isolation() -> None:
    nodes = FakeNodes()
    first = UsageFactStore(_client(nodes), KEY)
    fact = _fact()
    assert await first.append_event(fact) is True
    assert await first.append_event(fact) is False
    assert await first.event(fact.event_ref) == fact
    other = UsageFactStore(_client(nodes, "tenant-b"), KEY)
    assert await other.event(fact.event_ref) is None
    row = next(iter(nodes.rows.values()))
    assert "tenant-a" not in str(row)
    assert row["_owner"] == "service:usage"
    assert row["_visibility"] == "private"
    assert row["principal_ref"] != "service:usage"
    assert not {"content", "thinking_text", "input_json", "file_path"}.intersection(row)


@pytest.mark.asyncio
async def test_conflicting_replay_and_mismatched_authority_fail_closed() -> None:
    nodes = FakeNodes()
    store = UsageFactStore(_client(nodes), KEY)
    await store.append_event(_fact())
    node_id = next(iter(nodes.rows))
    nodes.rows[node_id]["input_tokens"] = 99
    with pytest.raises(ValueError, match="identity conflict"):
        await store.append_event(_fact())
    nodes.rows[node_id]["tenant_ref"] = "another"
    with pytest.raises(PermissionError, match="authority mismatch"):
        await store.event(_fact().event_ref)


def test_rejects_plaintext_ids_negative_tokens_and_content_timestamp() -> None:
    base = {
        name: getattr(_fact(), name) for name in UsageEventFact.__dataclass_fields__
    }
    for updates in (
        {"run_ref": "raw-run"},
        {"event_ref": "raw-event"},
        {"input_tokens": -1},
        {"occurred_at": "user@example.com"},
    ):
        with pytest.raises(ValueError):
            UsageEventFact(**(base | updates))
