"""Local SQL codec qualification; native cases require the actual new extension.

No server, Cargo invocation, or replacement serializer is used. Missing native
helpers fail qualification rather than skipping or mocking successful parity.
"""

from __future__ import annotations

import asyncio
import copy
import threading
from types import SimpleNamespace
from typing import Any, cast

import msgpack
import pytest

import epistemic_graph.client as client_module
from epistemic_graph.client import (
    EpistemicGraphClient,
    QueryClient,
    SqlSourceBatchPreparation,
    _canonical_method_body,
    _snapshot_sql_source_input,
    _sql_source_native_codec,
)
from epistemic_graph.client_capabilities import (
    SQL_SOURCE_PREPARATION_CAPABILITY,
    require_client_capabilities,
)

pytestmark = pytest.mark.no_engine


def _batch() -> dict[str, Any]:
    return {
        "source": "jira",
        "partition": "",
        "position": {"kind": "sequence", "value": 2},
        "expected_previous": {"kind": "sequence", "value": 1},
        "source_descriptor": {
            "provider": "jira",
            "dataset": "issues",
            "metadata": b'{"z":-0.0,"a":1.0}',
        },
        "mapping_descriptor": {"format": "json", "content": b"mapping-v1"},
        "table": "issues",
        "columns": [
            "null",
            "int",
            "float",
            "text",
            "bool",
            "time",
            "bytes",
            "json",
            "vector",
        ],
        "rows": [
            [
                {"kind": "null"},
                {"kind": "int", "value": -(1 << 63)},
                {"kind": "finite_float", "value": -0.0},
                {"kind": "text", "value": "東京\nissue"},
                {"kind": "bool", "value": True},
                {"kind": "timestamp", "value": (1 << 63) - 1},
                {"kind": "bytes", "value": b"\x00\n\xff"},
                {"kind": "json", "value": b'{"e":1e-7,"n":18446744073709551615}'},
                {"kind": "finite_vector", "value": [1.5, -0.0]},
            ]
        ],
        "expected_schema_version": 0,
        "expected_schema_digest": "01" * 32,
    }


def test_native_sql_source_scalar_width_binary_and_json_parity() -> None:
    prepared = QueryClient.prepare_sql_source_batch(_batch())
    decoded = msgpack.unpackb(prepared.canonical_batch, raw=False)
    assert decoded["source_descriptor"]["metadata"] == b'{"a":1.0,"z":-0.0}'
    assert decoded["rows"][0][6]["value"] == b"\x00\n\xff"
    assert decoded["rows"][0][7]["value"] == b'{"e":1e-7,"n":18446744073709551615}'
    # Hand-encoded MessagePack float widths: f64 -0 and f32 [1.5, -0].
    assert b"\xcb\x80\x00\x00\x00\x00\x00\x00\x00" in prepared.canonical_batch
    assert b"\x92\xca\x3f\xc0\x00\x00\xca\x80\x00\x00\x00" in prepared.canonical_batch
    assert (
        _canonical_method_body("SqlSourceBatch", {"batch": _batch()})
        == prepared.method_body
    )
    manifest = require_client_capabilities((SQL_SOURCE_PREPARATION_CAPABILITY,))
    assert manifest["capabilities"][SQL_SOURCE_PREPARATION_CAPABILITY] is True


def test_native_json_uses_rust_float_format_and_rejects_duplicate_keys() -> None:
    assert (
        QueryClient.canonical_sql_source_json({"z": -0.0, "e": 1e-7})
        == b'{"e":1e-7,"z":-0.0}'
    )
    with pytest.raises(ValueError, match="duplicate"):
        QueryClient.canonical_sql_source_json(b'{"a":1,"a":2}', raw_json=True)


def test_native_batch_permutations_and_semantic_digest_dependencies() -> None:
    batch = _batch()
    first = QueryClient.prepare_sql_source_batch(batch)
    permuted = dict(reversed(list(batch.items())))
    permuted["source_descriptor"] = {
        "metadata": b'{"a":1.0,"z":-0.0}',
        "dataset": "issues",
        "provider": "jira",
    }
    assert QueryClient.prepare_sql_source_batch(permuted) == first
    changed = copy.deepcopy(batch)
    changed["rows"][0][3]["value"] = "changed"
    rows = QueryClient.prepare_sql_source_batch(changed)
    assert rows.source_digest == first.source_digest
    assert rows.mapping_digest == first.mapping_digest
    assert rows.batch_digest != first.batch_digest
    changed = copy.deepcopy(batch)
    changed["mapping_descriptor"]["content"] = b"mapping-v2"
    mapping = QueryClient.prepare_sql_source_batch(changed)
    assert mapping.source_digest == first.source_digest
    assert mapping.mapping_digest != first.mapping_digest
    assert mapping.batch_digest != first.batch_digest


@pytest.mark.parametrize("bad", [float("inf"), float("nan"), 1e100])
def test_native_f32_overflow_and_nonfinite_vector_fail(bad: float) -> None:
    _sql_source_native_codec()  # Missing helper must FAIL even preflight rejections.
    batch = _batch()
    batch["rows"][0][8]["value"] = [bad]
    with pytest.raises((ValueError, TypeError)):
        QueryClient.prepare_sql_source_batch(batch)


@pytest.mark.parametrize("encoded", [b"\xdd\xff\xff\xff\xff", b"\x80\x00"])
def test_native_malformed_and_trailing_msgpack_fail(encoded: bytes) -> None:
    codec = _sql_source_native_codec()
    with pytest.raises(ValueError):
        codec._prepare_sql_source_batch(encoded)


def test_native_prepared_copy_cannot_bypass_checked_bytes() -> None:
    prepared = QueryClient.prepare_sql_source_batch(_batch())
    forged = SqlSourceBatchPreparation(b"\xdd\xff\xff\xff\xff", *prepared[1:])
    with pytest.raises(ValueError):
        forged.as_params()


def test_snapshot_rejects_oversized_input_before_packer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    codec = SimpleNamespace(__sql_source_limits__=(64, 100, 8))
    packed: list[Any] = []
    monkeypatch.setattr(client_module, "_pack_binary_msgpack", packed.append)
    with pytest.raises(ValueError):
        client_module._sql_source_input_bytes({"bytes": b"x" * 65}, codec)
    assert packed == []


def test_snapshot_rejects_outer_parameter_sprawl_before_codec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[bool] = []
    monkeypatch.setattr(
        client_module, "_sql_source_native_codec", lambda: called.append(True)
    )
    with pytest.raises(ValueError, match="exactly one"):
        _canonical_method_body("SqlSourceBatch", {"batch": {}, "unexpected": {}})
    assert called == []


def test_snapshot_owns_containers_and_rejects_mutable_binary() -> None:
    codec = SimpleNamespace(__sql_source_limits__=(1024, 100, 8))
    original = {"rows": [[{"value": "before"}]]}
    snapshot = _snapshot_sql_source_input(original, codec)
    original["rows"][0][0]["value"] = "after"
    assert snapshot == {"rows": [[{"value": "before"}]]}
    with pytest.raises(TypeError):
        _snapshot_sql_source_input({"value": bytearray(b"mutable")}, codec)


def test_snapshot_caps_recursive_cycles_and_utf8() -> None:
    codec = SimpleNamespace(__sql_source_limits__=(10000, 100, 8))
    cycle: list[Any] = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match="nesting"):
        _snapshot_sql_source_input(cycle, codec)
    with pytest.raises(ValueError):
        _snapshot_sql_source_input(
            "東京" * 10, SimpleNamespace(__sql_source_limits__=(30, 100, 8))
        )


def test_async_cancellation_retains_worker_and_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    observed: list[Any] = []
    codec = SimpleNamespace(__sql_source_limits__=(1024, 100, 8))
    monkeypatch.setattr(client_module, "_sql_source_native_codec", lambda: codec)

    def worker(params: dict[str, Any], **kwargs: Any) -> bytes:
        started.set()
        assert release.wait(5), "fixture failed to release bounded worker"
        observed.append(params)
        finished.set()
        return b"discarded"

    async def exercise() -> None:
        client = object.__new__(EpistemicGraphClient)
        client._sql_source_prepare_lock = asyncio.Lock()
        monkeypatch.setattr(client, "_build_sql_source_payload", worker)
        params = {"batch": {"rows": [["before"]]}}
        task = asyncio.create_task(
            client._sql_source_send_payload(
                params, req_id=7, target_graph="fixture", idempotency_key=None
            )
        )
        try:
            assert await asyncio.to_thread(started.wait, 5)
            params["batch"]["rows"][0][0] = "after"
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert client._sql_source_prepare_lock.locked()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert finished.is_set()
        assert not client._sql_source_prepare_lock.locked()
        assert observed == [{"batch": {"rows": [["before"]]}}]

    asyncio.run(exercise())


def test_sql_source_batch_sends_through_the_generated_contract_function() -> None:
    calls: list[tuple[str, Any]] = []

    class Transport:
        async def _send(
            self,
            method: str,
            params: Any,
            graph: str | None = None,
            *,
            idempotency_key: str | None = None,
        ) -> Any:
            calls.append((method, params))
            return {"accepted": True}

    prepared = SqlSourceBatchPreparation(b"checked", "s", "m", "b", b"body")
    query = QueryClient(cast(EpistemicGraphClient, Transport()))
    assert asyncio.run(query.sql_source_batch(prepared)) == {"accepted": True}
    assert calls == [("SqlSourceBatch", {"batch": b"checked"})]
