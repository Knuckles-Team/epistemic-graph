"""``ConsensusClient.get_identity`` — the read half of ``register_identity``.

These fixtures never contact an engine (mirrors ``test_cluster_topology_client.py``'s
``_FakeClient`` convention): a bare async stand-in for :class:`EpistemicGraphClient`
that only implements ``_send``, which is all ``get_identity`` calls through.

The RPC exists specifically to let a caller distinguish three outcomes without
ambiguity: unregistered (``None``), registered-with-empty-roles (a real
:class:`AgentIdentity` whose ``roles`` list is empty), and a call failure (an
exception, never ``None``). Each gets its own test below.
"""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import ConsensusClient

pytestmark = pytest.mark.no_engine


class _FakeClient:
    """Records the exact wire call and returns a canned ``_send`` result."""

    def __init__(self, answer: Any) -> None:
        self._answer = answer
        self.calls: list[tuple[str, dict[str, Any] | None, str | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        self.calls.append((method, params, graph))
        if isinstance(self._answer, BaseException):
            raise self._answer
        return self._answer


@pytest.mark.asyncio
async def test_get_identity_sends_exact_method_name_and_commons_graph() -> None:
    fake = _FakeClient(None)
    client = ConsensusClient(fake)  # type: ignore[arg-type]

    await client.get_identity("agent-a")

    assert fake.calls == [("GetIdentity", {"agent_id": "agent-a"}, "__commons__")]


@pytest.mark.asyncio
async def test_get_identity_returns_none_when_unregistered() -> None:
    """Not registered at all -> ``None``, and nothing else."""
    fake = _FakeClient(None)
    client = ConsensusClient(fake)  # type: ignore[arg-type]

    result = await client.get_identity("never-registered")

    assert result is None


@pytest.mark.asyncio
async def test_get_identity_returns_confirmed_empty_identity_not_none() -> None:
    """Registered but holding no roles -> a real identity, roles == [], NOT None.

    This is the exact ambiguity the RPC exists to remove (see the
    ``SystemAdmissionError: existing_roles is unknown (None)`` boot log this
    RPC was added to fix): a confirmed-empty role set must be observably
    different from "unknown".
    """
    fake = _FakeClient(
        {"agent_id": "agent-b", "role": "Agent", "teams": [], "roles": []}
    )
    client = ConsensusClient(fake)  # type: ignore[arg-type]

    result = await client.get_identity("agent-b")

    assert result is not None
    assert result["agent_id"] == "agent-b"
    assert result["role"] == "Agent"
    assert result["teams"] == []
    assert result["roles"] == []


@pytest.mark.asyncio
async def test_get_identity_returns_populated_identity_including_manager_role() -> None:
    fake = _FakeClient(
        {
            "agent_id": "agent-c",
            "role": {"Manager": {"subordinates": ["agent-d"]}},
            "teams": ["team-1"],
            "roles": ["commons-access"],
        }
    )
    client = ConsensusClient(fake)  # type: ignore[arg-type]

    result = await client.get_identity("agent-c")

    assert result is not None
    assert result["role"] == {"Manager": {"subordinates": ["agent-d"]}}
    assert result["teams"] == ["team-1"]
    assert result["roles"] == ["commons-access"]


@pytest.mark.asyncio
async def test_get_identity_propagates_call_failure_not_none() -> None:
    """A transport/engine failure must raise, never be swallowed into ``None`` --
    conflating "call failed" with "not registered" is exactly the ambiguity bug
    this RPC exists to fix."""
    fake = _FakeClient(RuntimeError("engine unreachable"))
    client = ConsensusClient(fake)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="engine unreachable"):
        await client.get_identity("agent-e")


@pytest.mark.asyncio
async def test_get_identity_rejects_malformed_result_shape() -> None:
    """A response missing/adding fields must not be silently accepted as a
    partially-valid identity."""
    fake = _FakeClient({"agent_id": "agent-f", "role": "Agent", "teams": []})
    client = ConsensusClient(fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="missing required fields"):
        await client.get_identity("agent-f")


@pytest.mark.asyncio
async def test_get_identity_rejects_empty_agent_id() -> None:
    fake = _FakeClient(None)
    client = ConsensusClient(fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        await client.get_identity("")

    assert fake.calls == []
