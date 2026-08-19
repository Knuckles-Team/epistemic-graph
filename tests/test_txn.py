"""TxnClient stages the Txn* RPCs and threads the server-issued txn_id
(CONCEPT:EG-KG.txn.multi-op-occ-acid — multi-op OCC ACID transactions).

Fake client only — no running engine. Verifies the client builds the right
wire methods/params (msgpack-packed property/condition blobs, txn_id threading)
and that begin/commit/rollback surface the server's String/Bool payloads.
"""

from __future__ import annotations

from typing import Any

import msgpack
import pytest

from epistemic_graph.client import TxnClient


class _FakeClient:
    """Records each (method, params) and returns canned, per-method results that
    mimic the engine: BeginTxn → a txn_id String; Txn*/Commit/Rollback → Bool."""

    def __init__(self, commit_result: bool = True) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []
        self._commit_result = commit_result
        self._txn_id = "txn-0000000000000001"

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        self.sent.append((method, params))
        if method == "BeginTxn":
            return self._txn_id
        if method == "Commit":
            return self._commit_result
        # Txn*/Rollback all ack with Bool(true) server-side.
        return True


@pytest.mark.asyncio
async def test_txn_stage_commit_round_trip() -> None:
    fake = _FakeClient()
    txns = TxnClient(fake)  # type: ignore[arg-type]

    txn_id = await txns.begin()
    assert txn_id == "txn-0000000000000001"
    assert fake.sent[0] == ("BeginTxn", {"graph": None, "isolation": None})

    assert await txns.add_node(txn_id, "a", {"type": "Doc"}) is True
    assert await txns.add_node(txn_id, "b") is True
    assert await txns.add_edge(txn_id, "a", "b", {"w": 1}) is True
    assert await txns.commit(txn_id) is True

    methods = [m for m, _ in fake.sent]
    assert methods == ["BeginTxn", "TxnAddNode", "TxnAddNode", "TxnAddEdge", "Commit"]

    # The txn_id is threaded into every staged op + the commit.
    for method, params in fake.sent[1:]:
        assert params is not None and params["txn_id"] == txn_id, method

    # Property blobs are native binary MessagePack payloads.
    _, add_a = fake.sent[1]
    assert add_a is not None
    assert add_a["graph"] is None
    assert msgpack.unpackb(bytes(add_a["properties_msgpack"]), raw=False) == {
        "type": "Doc"
    }
    _, add_edge = fake.sent[3]
    assert add_edge is not None
    assert add_edge["graph"] is None
    assert add_edge["source_id"] == "a" and add_edge["target_id"] == "b"


@pytest.mark.asyncio
async def test_txn_cas_packs_condition_and_update_blobs() -> None:
    fake = _FakeClient()
    txns = TxnClient(fake)  # type: ignore[arg-type]
    txn_id = await txns.begin()
    await txns.cas(txn_id, "task", {"owner": None}, {"owner": "w1"})

    method, params = fake.sent[-1]
    assert method == "TxnCas"
    assert params is not None
    assert msgpack.unpackb(bytes(params["conditions_msgpack"]), raw=False) == {
        "owner": None
    }
    assert msgpack.unpackb(bytes(params["updates_msgpack"]), raw=False) == {
        "owner": "w1"
    }


@pytest.mark.asyncio
async def test_txn_commit_conflict_surfaces_false() -> None:
    # An OCC conflict comes back as Bool(false) — the client returns it verbatim.
    fake = _FakeClient(commit_result=False)
    txns = TxnClient(fake)  # type: ignore[arg-type]
    txn_id = await txns.begin()
    await txns.add_node(txn_id, "x")
    assert await txns.commit(txn_id) is False


@pytest.mark.asyncio
async def test_txn_rollback_acks() -> None:
    fake = _FakeClient()
    txns = TxnClient(fake)  # type: ignore[arg-type]
    txn_id = await txns.begin()
    await txns.add_node(txn_id, "x")
    assert await txns.rollback(txn_id) is True
    assert fake.sent[-1][0] == "Rollback"
