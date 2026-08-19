"""Adversarial fixtures for the Python client's binary MessagePack RPC fields.

The Rust protocol declares batch and lifecycle blobs as ``Vec<u8>``/``serde_bytes``.
These fixtures exercise the client boundary only: the fake transport captures the
typed request params, while a second MessagePack round-trip models the real framed
transport's ``use_bin_type=True`` encoder.  No engine, NumPy, or pandas is needed.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

import msgpack
import pytest

from epistemic_graph import client as client_module
from epistemic_graph.client import (
    EdgeClient,
    LifecycleClient,
    NodeClient,
    StatechartClient,
    TxnClient,
    WorkItemClient,
    _canonical_method_body,
)


class _CaptureClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        del graph, idempotency_key
        self.sent.append((method, params))
        if method == "ClaimNext":
            return None
        if method == "Statechart":
            return {"def_id": "def:fixture"}
        if method == "CasWorkItemMetadata":
            return {
                "schema_version": "1",
                "outcome": "applied",
                "work_item_id": "work:fixture",
                "changed_work_item_ids": ["work:fixture"],
            }
        return True


def _capture_wire(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Model the framed request encoder and decode its params without JSON."""

    request = msgpack.packb(
        {"method": method, "params": params}, use_bin_type=True
    )
    return msgpack.unpackb(request, raw=False)


def _assert_binary_blob(
    method: str,
    params: dict[str, Any],
    field: str,
    expected: Any,
) -> None:
    blob = params[field]
    assert type(blob) is bytes, (method, field, type(blob))
    assert msgpack.unpackb(blob, raw=False) == expected
    wire = _capture_wire(method, params)
    assert type(wire["params"][field]) is bytes
    assert msgpack.unpackb(wire["params"][field], raw=False) == expected


@pytest.mark.asyncio
async def test_batch_and_lifecycle_surfaces_use_binary_msgpack_blobs() -> None:
    fake = _CaptureClient()

    nodes = NodeClient(fake)  # type: ignore[arg-type]
    await nodes.add("node:1", {"kind": "Document"})
    await nodes.create_if_absent("node:2", {"state": "pending"})
    await nodes.compare_and_set("node:2", {"state": "pending"}, {"state": "leased"})
    await nodes.claim_next("Document", {"owner": "worker:1"})

    edges = EdgeClient(fake)  # type: ignore[arg-type]
    await edges.add("node:1", "node:2", {"relationship": "mentions"})
    await edges.supersede(
        "node:1",
        "node:3",
        "node:1",
        "node:2",
        "mentions",
        10,
        11,
        {"relationship": "references"},
    )

    txns = TxnClient(fake)  # type: ignore[arg-type]
    await txns.add_node("txn:1", "node:4", {"kind": "Note"})
    await txns.add_edge("txn:1", "node:1", "node:4", {"weight": 1})
    await txns.cas("txn:1", "node:4", {"state": None}, {"state": "ready"})

    lifecycle = LifecycleClient(fake)  # type: ignore[arg-type]
    operations = [{"op": "add_node", "id": "node:5", "properties": {"x": 1}}]
    await lifecycle.batch_update(operations)
    await lifecycle.multi_graph_batch_update({"graph:a": operations})

    statechart = StatechartClient(fake)  # type: ignore[arg-type]
    definition = {"name": "fixture", "states": ["ready"], "initial": "ready"}
    assert await statechart.define(definition) == "def:fixture"

    work_items = WorkItemClient(fake)  # type: ignore[arg-type]
    await work_items.cas_metadata(
        tenant="tenant:fixture",
        work_item_id="work:fixture",
        expected_status=["leased"],
        now_ms=1_000,
        expected_metadata={"attempt": 1},
        set_metadata={"attempt": 2},
    )

    by_method = {method: params for method, params in fake.sent}
    assert by_method["AddNode"] is not None
    _assert_binary_blob(
        "AddNode", by_method["AddNode"], "properties_msgpack", {"kind": "Document"}
    )
    _assert_binary_blob(
        "CreateNodeIfAbsent",
        by_method["CreateNodeIfAbsent"],
        "properties_msgpack",
        {"state": "pending"},
    )
    _assert_binary_blob(
        "CompareAndSetNodeFields",
        by_method["CompareAndSetNodeFields"],
        "conditions_msgpack",
        {"state": "pending"},
    )
    _assert_binary_blob(
        "CompareAndSetNodeFields",
        by_method["CompareAndSetNodeFields"],
        "updates_msgpack",
        {"state": "leased"},
    )
    _assert_binary_blob(
        "AddEdge",
        by_method["AddEdge"],
        "properties_msgpack",
        {"relationship": "mentions"},
    )
    _assert_binary_blob(
        "SupersedeEdge",
        by_method["SupersedeEdge"],
        "properties_msgpack",
        {"relationship": "references"},
    )
    _assert_binary_blob(
        "TxnAddNode", by_method["TxnAddNode"], "properties_msgpack", {"kind": "Note"}
    )
    _assert_binary_blob(
        "TxnAddEdge", by_method["TxnAddEdge"], "properties_msgpack", {"weight": 1}
    )
    _assert_binary_blob(
        "TxnCas", by_method["TxnCas"], "conditions_msgpack", {"state": None}
    )
    _assert_binary_blob(
        "TxnCas", by_method["TxnCas"], "updates_msgpack", {"state": "ready"}
    )
    _assert_binary_blob(
        "BatchUpdate", by_method["BatchUpdate"], "operations_msgpack", operations
    )

    multi_graph = by_method["MultiGraphBatchUpdate"]
    assert multi_graph is not None
    assert type(multi_graph["batches_msgpack"]) is bytes
    decoded_batches = msgpack.unpackb(multi_graph["batches_msgpack"], raw=False)
    assert decoded_batches == [
        ["graph:a", msgpack.packb(operations, use_bin_type=True)]
    ]
    assert type(decoded_batches[0][1]) is bytes

    statechart_params = by_method["Statechart"]
    assert statechart_params is not None
    _assert_binary_blob(
        "Statechart",
        statechart_params["op"]["Define"],
        "def_msgpack",
        definition,
    )

    work_item_request = by_method["CasWorkItemMetadata"]["request"]
    _assert_binary_blob(
        "CasWorkItemMetadata",
        work_item_request,
        "expected_metadata_msgpack",
        {"attempt": 1},
    )
    _assert_binary_blob(
        "CasWorkItemMetadata",
        work_item_request,
        "set_metadata_msgpack",
        {"attempt": 2},
    )
    canonical = msgpack.unpackb(
        _canonical_method_body(
            "CasWorkItemMetadata",
            {"request": work_item_request},
        ),
        raw=False,
    )
    assert isinstance(
        canonical["params"]["request"]["expected_metadata_msgpack"], list
    )
    assert isinstance(canonical["params"]["request"]["set_metadata_msgpack"], list)


@pytest.mark.asyncio
async def test_large_and_empty_batches_never_expand_to_integer_arrays() -> None:
    fake = _CaptureClient()
    lifecycle = LifecycleClient(fake)  # type: ignore[arg-type]

    # This is deliberately larger than a fixstr/fixmap so the old integer-array
    # representation would be visible as a materially different wire shape.
    operations = [
        {
            "op": "add_node",
            "id": "node:large",
            "properties": {"payload": "x" * (256 * 1024)},
        }
    ]
    await lifecycle.batch_update(operations)
    method, params = fake.sent[-1]
    assert method == "BatchUpdate"
    assert params is not None
    blob = params["operations_msgpack"]
    assert type(blob) is bytes
    assert msgpack.unpackb(blob, raw=False) == operations

    native_wire = msgpack.packb(
        {"method": method, "params": params}, use_bin_type=True
    )
    legacy_wire = msgpack.packb(
        {"method": method, "params": {"operations_msgpack": list(blob)}},
        use_bin_type=True,
    )
    decoded = msgpack.unpackb(native_wire, raw=False)
    assert type(decoded["params"]["operations_msgpack"]) is bytes
    assert isinstance(
        msgpack.unpackb(legacy_wire, raw=False)["params"]["operations_msgpack"],
        list,
    )
    assert len(native_wire) <= len(legacy_wire)

    await lifecycle.batch_update([])
    empty_params = fake.sent[-1][1]
    assert empty_params is not None
    _assert_binary_blob("BatchUpdate", empty_params, "operations_msgpack", [])

    await lifecycle.multi_graph_batch_update({})
    empty_multi = fake.sent[-1][1]
    assert empty_multi is not None
    _assert_binary_blob("MultiGraphBatchUpdate", empty_multi, "batches_msgpack", [])


@pytest.mark.asyncio
async def test_unsupported_batch_values_fail_closed_before_transport() -> None:
    fake = _CaptureClient()
    lifecycle = LifecycleClient(fake)  # type: ignore[arg-type]

    with pytest.raises((TypeError, ValueError)):
        await lifecycle.batch_update(
            [{"op": "add_node", "id": "bad", "properties": {"value": object()}}]
        )
    assert fake.sent == []


def test_binary_codec_has_no_numpy_or_pandas_runtime_dependency() -> None:
    source = inspect.getsource(client_module)
    assert "import numpy" not in source
    assert "import pandas" not in source
    assert "def _pack_binary_msgpack" in source
    assert Path(client_module.__file__).name == "client.py"


def test_architecture_and_soak_guards_reject_integer_array_transport() -> None:
    repo_root = Path(client_module.__file__).resolve().parents[1]
    architecture_source = (
        repo_root / "scripts" / "check_current_only_architecture.py"
    ).read_text(encoding="utf-8")
    soak_source = (repo_root / "scripts" / "soak_scale.py").read_text(encoding="utf-8")

    client_source = Path(client_module.__file__).read_text(encoding="utf-8")
    assert "list(msgpack.packb" not in client_source
    assert "def _pack_binary_msgpack(value: Any) -> bytes:" in architecture_source
    assert "list(msgpack.packb" not in soak_source
    assert "_pack_binary_msgpack(props)" in soak_source
    assert "_pack_binary_msgpack(updates)" in soak_source
