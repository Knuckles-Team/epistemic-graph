"""Current-only Python bindings for governed modalities and KnowledgeStream."""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import KnowledgeStreamClient, ServedModalityClient


def _ref(namespace: str, token: str) -> str:
    return f"eg:{namespace}:{token * 32}"


def _cursor(*, family: str = "graph", batch_size: int = 2) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "family": family,
        "integrity_ref": _ref("integrity", "a"),
        "tenant_ref": _ref("tenant", "b"),
        "access_policy_ref": _ref("access-policy", "c"),
        "placement_ref": _ref("placement", "d"),
        "snapshot_ref": _ref("snapshot", "e"),
        "query_ref": _ref("query", "f"),
        "derivation_ref": _ref("derivation", "1"),
        "evidence_set_ref": _ref("evidence-set", "2"),
        "batch_size": batch_size,
        "row_offset": 2,
        "batch_index": 1,
        "exhausted": False,
    }


def _bundle() -> dict[str, Any]:
    return {
        "protocol_version": 1,
        "privacy": {"attested": True},
        "artifacts": [{}],
        "occurrences": [{}],
        "renditions": [],
        "segments": [],
        "features": [],
        "evidence_loci": [],
    }


class _FakeClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        assert params is not None
        self.sent.append((method, params))
        if method == "KnowledgeStream":
            request = params["request"]
            return {
                "schema_version": 1,
                "family": request["query"]["family"],
                "projection": "arrow_ipc_v1",
                "cursor": _cursor(
                    family=request["query"]["family"],
                    batch_size=request["batch_size"],
                ),
                "payload": b"ARROW1",
            }

        operation = params["op"]["operation"]
        if operation == "authority":
            return {
                "tenant_ref": _ref("tenant", "a"),
                "access_policy_ref": _ref("access-policy", "b"),
                "purpose_ref": _ref("purpose", "c"),
                "maximum_classification": "internal",
            }
        if operation in {"query", "native_query"}:
            occurrence = _ref("occurrence", "d")
            return {
                "records": [
                    {
                        "occurrence_id": occurrence,
                        "observation_version": 1,
                        "lifecycle": "active",
                        "bundle": _bundle(),
                        "value": {"content_ref": _ref("content", "e")},
                    }
                ],
                "next": occurrence,
            }
        if operation == "events":
            return [
                {
                    "sequence": 1,
                    "occurrence_id": _ref("occurrence", "d"),
                    "observation_version": 1,
                    "kind": "ingested",
                    "tenant_ref": _ref("tenant", "a"),
                    "access_policy_ref": _ref("access-policy", "b"),
                }
            ]
        if operation == "capabilities":
            return {
                "component_ready": True,
                "component_pass": 12,
                "component_not_applicable": 0,
                "component_total": 12,
            }
        if operation == "stats":
            return {
                "active_records": 1,
                "total_records": 1,
                "tombstoned_records": 0,
                "modality_index_postings": 1,
                "segment_index_postings": 1,
                "native_index_keys": 1,
                "native_index_postings": 1,
                "events": 1,
                "snapshot_bytes": 1,
            }
        if operation == "collect_tombstones":
            return {"collected": 1}
        return {
            "disposition": "Applied",
            "observation_version": 1,
            "event_sequence": 1,
        }


@pytest.mark.asyncio
async def test_knowledge_stream_uses_one_current_arrow_pull_shape() -> None:
    fake = _FakeClient()
    knowledge = KnowledgeStreamClient(fake)  # type: ignore[arg-type]

    first = await knowledge.pull(
        {"family": "graph", "label": "Capability", "limit": 20},
        batch_size=2,
    )
    assert first["payload"] == b"ARROW1"
    assert fake.sent[-1] == (
        "KnowledgeStream",
        {
            "request": {
                "schema_version": 1,
                "query": {
                    "family": "graph",
                    "label": "Capability",
                    "limit": 20,
                },
                "batch_size": 2,
                "projection": "arrow_ipc_v1",
            }
        },
    )

    await knowledge.pull(
        {"family": "graph", "label": "Capability", "limit": 20},
        batch_size=2,
        cursor=first["cursor"],
    )
    assert fake.sent[-1][1]["request"]["cursor"] == first["cursor"]


@pytest.mark.asyncio
async def test_knowledge_stream_rejects_retired_and_mismatched_shapes() -> None:
    fake = _FakeClient()
    knowledge = KnowledgeStreamClient(fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="unsupported fields"):
        await knowledge.pull(
            {
                "family": "cross_modal",
                "text": "MATCH (n) |> LIMIT 1",
                "reorder_filter_selectivity": 0.5,
            },  # type: ignore[arg-type]
            batch_size=4,
        )

    with pytest.raises(ValueError, match="family and batch size"):
        await knowledge.pull(
            {"family": "graph", "label": "", "limit": 0},
            batch_size=3,
            cursor=_cursor(batch_size=2),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_served_modality_methods_emit_exact_current_operations() -> None:
    fake = _FakeClient()
    modalities = ServedModalityClient(fake)  # type: ignore[arg-type]
    occurrence = _ref("occurrence", "d")
    idempotency = _ref("idempotency", "e")

    authority = await modalities.authority()
    assert authority["maximum_classification"] == "internal"
    await modalities.ingest(
        "document",
        idempotency_ref=idempotency,
        target_occurrence_id=occurrence,
        expected_version=None,
        bundle_msgpack=b"\x81\xa1x\x01",
        source_bytes=b"content",
    )
    page = await modalities.query(
        "document", segment_kind="paragraph", limit=25, include_cold=True
    )
    await modalities.delete(
        "document",
        idempotency_ref=idempotency,
        occurrence_id=occurrence,
        expected_version=1,
    )
    await modalities.move_to_cold("document", occurrence_id=occurrence)
    await modalities.restore("document", occurrence_id=occurrence)
    events = await modalities.events("document", after_sequence=0, limit=25)
    stats = await modalities.stats("document")
    collected = await modalities.collect_tombstones(
        "document", through_event_sequence=1
    )
    capabilities = await modalities.capabilities("document")

    assert page["next"] == occurrence
    assert events[0]["kind"] == "ingested"
    assert stats["active_records"] == 1
    assert collected == 1
    assert capabilities["component_pass"] == 12
    assert [params["op"]["operation"] for _, params in fake.sent] == [
        "authority",
        "ingest",
        "query",
        "delete",
        "move_to_cold",
        "restore",
        "events",
        "stats",
        "collect_tombstones",
        "capabilities",
    ]
    assert fake.sent[1] == (
        "ServedModality",
        {
            "op": {
                "operation": "ingest",
                "modality": "document",
                "idempotency_ref": idempotency,
                "target_occurrence_id": occurrence,
                "expected_version": None,
                "bundle_msgpack": b"\x81\xa1x\x01",
                "source_bytes": b"content",
            }
        },
    )


@pytest.mark.asyncio
async def test_served_modality_rejects_noncurrent_or_drifted_data() -> None:
    fake = _FakeClient()
    modalities = ServedModalityClient(fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="current served segment"):
        await modalities.query("document", segment_kind="text_span")
    with pytest.raises(ValueError, match="opaque reference"):
        await modalities.restore("document", occurrence_id="ordinary-identifier")

    original_send = fake._send

    async def drifted_send(method: str, params: dict[str, Any] | None = None) -> Any:
        result = await original_send(method, params)
        if params is not None and params["op"]["operation"] == "query":
            bundle = result["records"][0]["bundle"]
            bundle["evidence_spans"] = bundle.pop("evidence_loci")
        return result

    fake._send = drifted_send  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="evidence_loci"):
        await modalities.query("document")


@pytest.mark.asyncio
async def test_served_native_queries_emit_closed_typed_predicates() -> None:
    fake = _FakeClient()
    modalities = ServedModalityClient(fake)  # type: ignore[arg-type]

    await modalities.search_documents("evidence", page=2)
    await modalities.query_image_region(x=0.1, y=0.2, width=0.3, height=0.4)
    await modalities.query_similar_images(0x1234, maximum_distance=7)
    await modalities.query_audio_window(start_ms=100, end_ms=900, minimum_rms=0.25)
    await modalities.query_video_window(
        start_ms=0, end_ms=1_000, keyframes_only=True
    )

    predicates = [params["op"]["predicate"] for _, params in fake.sent]
    assert predicates == [
        {"predicate": "document_lexical", "term": "evidence", "page": 2},
        {
            "predicate": "image_region",
            "x": 0.1,
            "y": 0.2,
            "width": 0.3,
            "height": 0.4,
        },
        {
            "predicate": "image_perceptual_hash",
            "hash": 0x1234,
            "maximum_distance": 7,
        },
        {
            "predicate": "audio_window",
            "start_ms": 100,
            "end_ms": 900,
            "minimum_rms": 0.25,
        },
        {
            "predicate": "video_window",
            "start_ms": 0,
            "end_ms": 1_000,
            "keyframes_only": True,
        },
    ]
    assert all(params["op"]["operation"] == "native_query" for _, params in fake.sent)


@pytest.mark.asyncio
async def test_served_native_query_bounds_fail_before_transport() -> None:
    fake = _FakeClient()
    modalities = ServedModalityClient(fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="alphanumeric"):
        await modalities.search_documents("two terms")
    with pytest.raises(ValueError, match="normalized rectangle"):
        await modalities.query_image_region(x=0.9, y=0.0, width=0.2, height=0.5)
    with pytest.raises(ValueError, match="between 0 and 15"):
        await modalities.query_similar_images(1, maximum_distance=16)
    with pytest.raises(ValueError, match="greater than"):
        await modalities.query_audio_window(start_ms=10, end_ms=10)
    with pytest.raises(ValueError, match="4096 index buckets"):
        await modalities.query_video_window(start_ms=0, end_ms=4_096_001)
    with pytest.raises(TypeError, match="boolean"):
        await modalities.query_video_window(
            start_ms=0, end_ms=10, keyframes_only=1  # type: ignore[arg-type]
        )
    assert fake.sent == []
