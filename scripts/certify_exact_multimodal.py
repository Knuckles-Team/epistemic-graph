#!/usr/bin/env python3
"""Certify every served production modality on one exact installed artifact.

The caller supplies the executable and its expected digest.  This harness never
discovers or builds another engine.  It exercises only synthetic request-local
document, PNG, PCM/WAV, and ISOBMFF payloads, removes all runtime material, and
retains categorical, deterministic, path-free evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import struct
import sys
import tempfile
from pathlib import Path
from typing import Any

import msgpack
from certify_exact_fault_restart import (
    SECOND_TENANT,
    TENANT,
    CertificationError,
    ExactBinary,
    ExactEngine,
    _context,
    _fail,
    _new_ephemeral_authority,
    _validate_binary,
    _with_client,
    _write_evidence,
)

from epistemic_graph.client import SyncEpistemicGraphClient

SCHEMA_VERSION = 1
MODALITY_GRAPH = "exact-multimodal"
MODALITY_SOURCE_LIMIT = 1_024
MODALITIES = ("document", "image", "audio", "video")
MAX_PERFORMANCE_EVIDENCE_BYTES = 8 * 1024 * 1024
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
EXACT_BEHAVIOR_DIMENSIONS = (
    "artifact_identity_and_exact_round_trip",
    "atomic_stream_ingest_replay_and_delete",
    "native_storage_index_and_stats",
    "restart_restore_migration_and_index_backfill",
    "typed_query_selectivity_and_paging",
    "fault_atomicity_and_durable_event_outbox",
    "tenant_classification_and_authority",
    "provenance_evidence_and_lineage",
    "lifecycle_retention_and_tombstone_collection",
    "malformed_input_rejection",
    "per_modality_resource_bounds",
    "four_modality_performance_binding",
)
SEGMENTS = {
    "document": "paragraph",
    "image": "region",
    "audio": "audio_range",
    "video": "video_shot",
}
PNG_FIXTURE = bytes(
    [
        0x89,
        0x50,
        0x4E,
        0x47,
        0x0D,
        0x0A,
        0x1A,
        0x0A,
        0x00,
        0x00,
        0x00,
        0x0D,
        0x49,
        0x48,
        0x44,
        0x52,
        0x00,
        0x00,
        0x00,
        0x01,
        0x00,
        0x00,
        0x00,
        0x01,
        0x08,
        0x06,
        0x00,
        0x00,
        0x00,
        0x1F,
        0x15,
        0xC4,
        0x89,
        0x00,
        0x00,
        0x00,
        0x0D,
        0x49,
        0x44,
        0x41,
        0x54,
        0x78,
        0xDA,
        0x63,
        0xFC,
        0xCF,
        0xC0,
        0x50,
        0x0F,
        0x00,
        0x05,
        0xFE,
        0x02,
        0xFE,
        0x42,
        0x75,
        0x27,
        0x59,
        0x00,
        0x00,
        0x00,
        0x00,
        0x49,
        0x45,
        0x4E,
        0x44,
        0xAE,
        0x42,
        0x60,
        0x82,
    ]
)


def _opaque(
    namespace: str, modality: str, version: int, role: str, *, variant: int = 0
) -> str:
    token = hashlib.sha256(
        f"g14:{modality}:{variant}:{version}:{role}".encode("ascii")
    ).hexdigest()
    return f"eg:{namespace}:{token}"


def _boxed(kind: bytes, body: bytes) -> bytes:
    return struct.pack(">I", len(body) + 8) + kind + body


def _wav_fixture(*, silent: bool = False) -> bytes:
    samples = (
        (0,) * 8
        if silent
        else (0, 8_000, 16_000, 8_000, 0, -8_000, -16_000, -8_000)
    )
    pcm = b"".join(struct.pack("<h", sample) for sample in samples)
    return b"".join(
        (
            b"RIFF",
            struct.pack("<I", 36 + len(pcm)),
            b"WAVEfmt ",
            struct.pack("<IHHIIHH", 16, 1, 1, 8, 16, 2, 16),
            b"data",
            struct.pack("<I", len(pcm)),
            pcm,
        )
    )


def _mp4_fixture(*, duration_ms: int = 1_000) -> bytes:
    ftyp = _boxed(b"ftyp", b"isom" + struct.pack(">I", 0) + b"isom")
    mdat = _boxed(b"mdat", bytes((0, 1, 2, 3, 4, 5)))
    sample_offset = len(ftyp) + 8

    mvhd = bytearray(100)
    mvhd[12:16] = struct.pack(">I", 1_000)
    mvhd[16:20] = struct.pack(">I", duration_ms)
    tkhd = bytearray(84)
    tkhd[12:16] = struct.pack(">I", 1)
    mdhd = bytearray(24)
    mdhd[12:16] = struct.pack(">I", 1_000)
    mdhd[16:20] = struct.pack(">I", duration_ms)
    hdlr = bytearray(24)
    hdlr[8:12] = b"vide"

    sample_body = bytearray(78)
    sample_body[6:8] = struct.pack(">H", 1)
    sample_body[24:26] = struct.pack(">H", 2)
    sample_body[26:28] = struct.pack(">H", 1)
    sample_body[40:42] = struct.pack(">H", 1)
    sample_body[74:76] = struct.pack(">H", 24)
    sample_body[76:78] = struct.pack(">H", 0xFFFF)
    sample_entry = _boxed(b"raw ", bytes(sample_body))
    stsd = struct.pack(">I", 0) + struct.pack(">I", 1) + sample_entry
    stts = struct.pack(">IIII", 0, 1, 1, duration_ms)
    stsz = struct.pack(">III", 0, 6, 1)
    stco = struct.pack(">III", 0, 1, sample_offset)
    stsc = struct.pack(">IIIII", 0, 1, 1, 1, 1)
    stbl = b"".join(
        (
            _boxed(b"stsd", stsd),
            _boxed(b"stts", stts),
            _boxed(b"stsz", stsz),
            _boxed(b"stco", stco),
            _boxed(b"stsc", stsc),
        )
    )
    minf = _boxed(b"minf", _boxed(b"stbl", stbl))
    mdia = b"".join((_boxed(b"mdhd", bytes(mdhd)), _boxed(b"hdlr", bytes(hdlr)), minf))
    trak = _boxed(b"trak", _boxed(b"tkhd", bytes(tkhd)) + _boxed(b"mdia", mdia))
    moov = _boxed(b"moov", _boxed(b"mvhd", bytes(mvhd)) + trak)
    return ftyp + mdat + moov


def _sources() -> dict[str, tuple[bytes, bytes]]:
    return {
        "document": (b"alpha synthetic evidence", b"omega synthetic evidence"),
        "image": (PNG_FIXTURE, PNG_FIXTURE),
        "audio": (_wav_fixture(), _wav_fixture(silent=True)),
        "video": (_mp4_fixture(), _mp4_fixture(duration_ms=2_000)),
    }


def _address(modality: str, source: bytes, variant: int) -> dict[str, object]:
    if modality == "document":
        return {"kind": "character_range", "start": 0, "end": len(source)}
    if modality == "image":
        origin = 0.1 if variant == 0 else 0.7
        return {
            "kind": "image_region",
            "x": origin,
            "y": origin,
            "width": 0.2,
            "height": 0.2,
        }
    if modality == "audio":
        return {"kind": "audio_range", "start_ms": 0, "end_ms": 500}
    if modality == "video":
        return {"kind": "video_time_range", "start_ms": 0, "end_ms": 1_000}
    _fail("unknown_modality_fixture")


def _bundle(
    authority: dict[str, Any],
    modality: str,
    source: bytes,
    *,
    version: int,
    variant: int = 0,
    classification: str = "internal",
) -> tuple[dict[str, Any], str]:
    artifact = _opaque("artifact", modality, 1, "artifact", variant=variant)
    occurrence = _opaque("occurrence", modality, 1, "occurrence", variant=variant)
    rendition = _opaque("rendition", modality, 1, "rendition", variant=variant)
    segment = _opaque("segment", modality, 1, "segment", variant=variant)
    derivation_id = _opaque(
        "derivation", modality, version, "derivation", variant=variant
    )
    content_ref = f"eg:content:{hashlib.sha256(source).hexdigest()}"
    derivation = {
        "id": derivation_id,
        "transform_ref": _opaque(
            "transform", modality, version, "transform", variant=variant
        ),
        "implementation_ref": _opaque(
            "implementation", modality, version, "implementation", variant=variant
        ),
        "version_ref": _opaque(
            "version", modality, version, "version", variant=variant
        ),
        "model_ref": None,
        "inputs": [{"kind": "occurrence", "id": occurrence}],
    }
    return (
        {
            "protocol_version": 1,
            "privacy": {
                "scanner_ref": _opaque(
                    "scanner", modality, version, "scanner", variant=variant
                ),
                "policy_version_ref": _opaque(
                    "policyversion", modality, version, "privacy", variant=variant
                ),
                "raw_pii_persisted": False,
                "local_identifiers_persisted": False,
            },
            "artifacts": [
                {
                    "id": artifact,
                    "content_ref": content_ref,
                    "modality": modality,
                    "schema_ref": _opaque(
                        "schema", modality, version, "artifact", variant=variant
                    ),
                    "content_version": version,
                }
            ],
            "occurrences": [
                {
                    "id": occurrence,
                    "artifact_id": artifact,
                    "source_ref": _opaque(
                        "source", modality, 1, "source", variant=variant
                    ),
                    "observation_version": version,
                    "policy": {
                        "tenant_ref": authority["tenant_ref"],
                        "access_policy_ref": authority["access_policy_ref"],
                        "classification": classification,
                        "retention_policy_ref": _opaque(
                            "retention", modality, 1, "retention", variant=variant
                        ),
                        "deletion_policy_ref": _opaque(
                            "deletion", modality, 1, "deletion", variant=variant
                        ),
                        "legal_hold_ref": None,
                        "purpose_refs": [authority["purpose_ref"]],
                    },
                }
            ],
            "renditions": [
                {
                    "id": rendition,
                    "occurrence_id": occurrence,
                    "content_ref": content_ref,
                    "modality": modality,
                    "schema_ref": _opaque(
                        "schema", modality, version, "rendition", variant=variant
                    ),
                    "derivation": derivation,
                }
            ],
            "segments": [
                {
                    "id": segment,
                    "rendition_id": rendition,
                    "parent_segment_id": None,
                    "kind": SEGMENTS[modality],
                    "ordinal": 0,
                    "schema_ref": _opaque(
                        "schema", modality, version, "segment", variant=variant
                    ),
                }
            ],
            "features": [
                {
                    "id": _opaque(
                        "feature", modality, 1, "feature", variant=variant
                    ),
                    "subject": {"kind": "segment", "id": segment},
                    "kind": "statistic",
                    "value_ref": _opaque(
                        "value", modality, version, "value", variant=variant
                    ),
                    "schema_ref": _opaque(
                        "schema", modality, version, "feature", variant=variant
                    ),
                    "derivation": derivation,
                }
            ],
            "evidence_loci": [
                {
                    "id": _opaque("locus", modality, 1, "locus", variant=variant),
                    "subject": {"kind": "segment", "id": segment},
                    "address": _address(modality, source, variant),
                    "policy_ref": authority["access_policy_ref"],
                    "derivation_ref": derivation_id,
                }
            ],
        },
        occurrence,
    )


def _packed(bundle: dict[str, Any]) -> bytes:
    return msgpack.packb(bundle, use_bin_type=True)


def _load_performance_evidence(
    path_text: str, expected_digest: str, binary_digest: str
) -> dict[str, object]:
    """Bind G-14 to a passing G-37 run of the same immutable executable."""

    if not SHA256_PATTERN.fullmatch(expected_digest):
        _fail("invalid_performance_evidence_digest")
    path = Path(path_text)
    if not path.is_absolute():
        _fail("performance_evidence_path_must_be_absolute")
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or not 0 < metadata.st_size <= MAX_PERFORMANCE_EVIDENCE_BYTES
        ):
            _fail("invalid_performance_evidence_file")
        raw = bytearray()
        while chunk := os.read(descriptor, 64 * 1024):
            raw.extend(chunk)
            if len(raw) > MAX_PERFORMANCE_EVIDENCE_BYTES:
                _fail("invalid_performance_evidence_file")
    except CertificationError:
        raise
    except OSError:
        _fail("invalid_performance_evidence_file")
    finally:
        if descriptor is not None:
            os.close(descriptor)
    report_digest = hashlib.sha256(raw).hexdigest()
    if report_digest != expected_digest:
        _fail("performance_evidence_digest_mismatch")
    try:
        report = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError):
        _fail("invalid_performance_evidence_json")
    if not isinstance(report, dict):
        _fail("invalid_performance_evidence_schema")
    artifact = report.get("exact_artifact")
    modality_coverage = (
        report.get("coverage", {}).get("modality")
        if isinstance(report.get("coverage"), dict)
        else None
    )
    dataset = report.get("dataset")
    metric_results = report.get("metric_results")
    complexity_results = report.get("complexity_results")
    if (
        report.get("schema_version") != "1"
        or report.get("gate") != "G-37"
        or report.get("status") != "pass"
        or report.get("failures") != []
        or not isinstance(artifact, dict)
        or artifact.get("component") != "epistemic-graph-server"
        or artifact.get("sha256") != binary_digest
        or artifact.get("staged_copy_verified") is not True
        or not isinstance(modality_coverage, dict)
        or not isinstance(dataset, dict)
        or not isinstance(metric_results, dict)
        or not metric_results
        or not isinstance(complexity_results, dict)
        or not complexity_results
        or any(
            not isinstance(result, dict) or result.get("passed") is not True
            for result in (*metric_results.values(), *complexity_results.values())
        )
    ):
        _fail("performance_evidence_did_not_pass")
    modalities = modality_coverage.get("component_probes")
    ingests = modality_coverage.get("ingests_by_modality")
    queries = modality_coverage.get("native_query_samples_by_modality")
    growth = modality_coverage.get("index_growth_ratio_by_modality")
    records_per_modality = dataset.get("records_per_modality")
    expected_modalities = set(MODALITIES)
    if (
        modalities != list(MODALITIES)
        or not isinstance(ingests, dict)
        or set(ingests) != expected_modalities
        or not isinstance(queries, dict)
        or set(queries) != expected_modalities
        or not isinstance(growth, dict)
        or set(growth) != expected_modalities
        or isinstance(records_per_modality, bool)
        or not isinstance(records_per_modality, int)
        or records_per_modality < 1
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 1
            for value in (*ingests.values(), *queries.values())
        )
        or any(
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or value <= 0
            for value in growth.values()
        )
    ):
        _fail("performance_evidence_modality_coverage_incomplete")
    return {
        "gate": "G-37",
        "modalities": list(MODALITIES),
        "records_per_modality": records_per_modality,
        "report_sha256": report_digest,
        "same_artifact_verified": True,
        "status": "pass",
    }


def _expect_error(operation: Any, expected: str, failure: str) -> None:
    try:
        operation()
    except RuntimeError as error:
        if expected.casefold() not in str(error).casefold():
            _fail(failure)
        return
    _fail(failure)


def _assert_outcome(
    value: Any, *, disposition: str, version: int, sequence: int, failure: str
) -> None:
    if value != {
        "disposition": disposition,
        "observation_version": version,
        "event_sequence": sequence,
    }:
        _fail(failure)


def _assert_page(
    page: Any,
    *,
    occurrence: str,
    version: int,
    lifecycle: str,
    bundle: dict[str, Any],
    modality: str,
    source: bytes,
    failure: str,
) -> None:
    if not isinstance(page, dict) or set(page) != {"records", "next"}:
        _fail(failure)
    records = page["records"]
    if not isinstance(records, list) or len(records) != 1 or page["next"] != occurrence:
        _fail(failure)
    record = records[0]
    if (
        not isinstance(record, dict)
        or record.get("occurrence_id") != occurrence
        or record.get("observation_version") != version
        or record.get("lifecycle") != lifecycle
        or record.get("bundle") != bundle
        or not isinstance(record.get("value"), dict)
    ):
        _fail(failure)
    value = record["value"]
    if value.get("blob_ref") != hashlib.sha256(source).hexdigest():
        _fail(failure)
    encoded_value = msgpack.packb(value, use_bin_type=True)
    if source in encoded_value:
        _fail("normalized_modality_value_retained_raw_source")
    required = {
        "document": ("pages", "lexical_postings"),
        "image": ("regions", "perceptual_hash"),
        "audio": ("feature_windows", "segments"),
        "video": ("tracks", "frames", "shots"),
    }[modality]
    if any(field not in value for field in required):
        _fail(failure)


def _assert_empty(page: Any, failure: str) -> None:
    if page != {"records": [], "next": None}:
        _fail(failure)


def _native_query(client: SyncEpistemicGraphClient, modality: str) -> Any:
    if modality == "document":
        return client.modalities.search_documents("alpha", limit=1)
    if modality == "image":
        return client.modalities.query_image_region(
            x=0.05, y=0.05, width=0.3, height=0.3, limit=1
        )
    if modality == "audio":
        return client.modalities.query_audio_window(
            start_ms=0, end_ms=1_000, minimum_rms=0.1, limit=1
        )
    if modality == "video":
        return client.modalities.query_video_window(
            start_ms=1_200, end_ms=1_800, keyframes_only=False, limit=1
        )
    _fail("unknown_native_modality_query")


def _native_negative_query(client: SyncEpistemicGraphClient, modality: str) -> Any:
    if modality == "document":
        return client.modalities.search_documents("absent", limit=1)
    if modality == "image":
        return client.modalities.query_image_region(
            x=0.4, y=0.4, width=0.1, height=0.1, limit=1
        )
    if modality == "audio":
        return client.modalities.query_audio_window(
            start_ms=0, end_ms=1_000, minimum_rms=1.0, limit=1
        )
    if modality == "video":
        return client.modalities.query_video_window(
            start_ms=2_500, end_ms=3_000, keyframes_only=False, limit=1
        )
    _fail("unknown_native_modality_query")


def _stream_item(
    modality: str,
    bundle: dict[str, Any],
    occurrence: str,
    source: bytes,
    *,
    version: int,
    variant: int,
    role: str,
    expected_version: int | None = None,
) -> dict[str, Any]:
    return {
        "idempotency_ref": _opaque(
            "idempotency", modality, version, role, variant=variant
        ),
        "target_occurrence_id": occurrence,
        "expected_version": expected_version,
        "bundle_msgpack": _packed(bundle),
        "source_bytes": source,
    }


def _assert_paged_pair(
    client: SyncEpistemicGraphClient,
    modality: str,
    expected: dict[str, tuple[dict[str, Any], int, bytes]],
) -> None:
    observed: set[str] = set()
    after: str | None = None
    for _ in range(2):
        page = client.modalities.query(
            modality,
            segment_kind=SEGMENTS[modality],
            after_occurrence_id=after,
            limit=1,
        )
        records = page.get("records") if isinstance(page, dict) else None
        if not isinstance(records, list) or len(records) != 1:
            _fail("modality_paging_cardinality_mismatch")
        occurrence = records[0].get("occurrence_id")
        if occurrence not in expected or occurrence in observed:
            _fail("modality_paging_cursor_mismatch")
        bundle, version, source = expected[occurrence]
        _assert_page(
            page,
            occurrence=occurrence,
            version=version,
            lifecycle="active",
            bundle=bundle,
            modality=modality,
            source=source,
            failure="modality_paged_round_trip_mismatch",
        )
        observed.add(occurrence)
        after = page["next"]
    if observed != set(expected):
        _fail("modality_paging_inventory_mismatch")
    _assert_empty(
        client.modalities.query(
            modality,
            segment_kind=SEGMENTS[modality],
            after_occurrence_id=after,
            limit=1,
        ),
        "modality_paging_did_not_terminate",
    )


def _assert_stats(
    client: SyncEpistemicGraphClient,
    modality: str,
    *,
    active: int,
    total: int,
    tombstoned: int,
    events: int,
    indexes_present: bool,
) -> None:
    stats = client.modalities.stats(modality)
    if (
        stats.get("active_records") != active
        or stats.get("total_records") != total
        or stats.get("tombstoned_records") != tombstoned
        or stats.get("events") != events
        or not isinstance(stats.get("snapshot_bytes"), int)
        or stats["snapshot_bytes"] <= 0
    ):
        _fail("modality_storage_stats_mismatch")
    index_fields = (
        "modality_index_postings",
        "segment_index_postings",
        "native_index_keys",
        "native_index_postings",
    )
    if indexes_present != all(stats.get(field, 0) > 0 for field in index_fields):
        _fail("modality_index_stats_mismatch")


def _invalid_authentication_denied(engine: ExactEngine) -> None:
    client = SyncEpistemicGraphClient.connect(
        socket_path=str(engine.socket_path),
        auth_secret="synthetic-invalid-authority",
        graph_name=MODALITY_GRAPH,
        verified_context=_context(TENANT),
        timeout=5.0,
        connect_timeout=5.0,
    )
    try:
        _expect_error(
            client.modalities.authority,
            "authentication",
            "invalid_modality_authority_was_accepted",
        )
    finally:
        client.close()


def _assert_events(
    client: SyncEpistemicGraphClient, modality: str, expected_kinds: tuple[str, ...]
) -> None:
    events = client.modalities.events(modality, after_sequence=0, limit=10)
    if not isinstance(events, list) or len(events) != len(expected_kinds):
        _fail("modality_event_cardinality_mismatch")
    if [event.get("sequence") for event in events] != list(
        range(1, len(expected_kinds) + 1)
    ):
        _fail("modality_event_sequence_mismatch")
    if [event.get("kind") for event in events] != list(expected_kinds):
        _fail("modality_event_kind_mismatch")
    if any(
        not isinstance(event.get("tenant_ref"), str)
        or not isinstance(event.get("access_policy_ref"), str)
        for event in events
    ):
        _fail("modality_event_authority_missing")


def _assert_sources_absent(root: Path, sources: tuple[bytes, ...]) -> None:
    for path in root.rglob("*"):
        if path.is_symlink():
            _fail("modality_store_contains_symlink")
        if not path.is_file():
            continue
        try:
            for source in sources:
                overlap = b""
                with path.open("rb") as handle:
                    for block in iter(lambda: handle.read(1024 * 1024), b""):
                        candidate = overlap + block
                        if source in candidate:
                            _fail("raw_modality_source_was_persisted")
                        overlap = (
                            candidate[-(len(source) - 1) :] if len(source) > 1 else b""
                        )
        except OSError:
            _fail("modality_store_privacy_scan_failed")


FAULT_PHASES = (
    "before_rows",
    "after_rows_before_metadata",
    "before_commit",
    "after_commit_before_ack",
)


def _run_modality_fault_case(
    binary: ExactBinary,
    root: Path,
    authority: Any,
    modality: str,
    phase: str,
    source: bytes,
) -> dict[str, object]:
    engine = ExactEngine(binary, root, authority)
    bundle: dict[str, Any]
    occurrence: str
    try:
        engine.start(modality_source_limit=MODALITY_SOURCE_LIMIT)
        engine.bootstrap()
        _with_client(
            engine,
            "__commons__",
            lambda client: client.tenants.create(MODALITY_GRAPH),
        )
        client = engine.connect(MODALITY_GRAPH)
        try:
            view = client.modalities.authority()
            bundle, occurrence = _bundle(view, modality, source, version=1, variant=20)
        finally:
            client.close()
        engine.stop()
        engine.start(
            modality_source_limit=MODALITY_SOURCE_LIMIT,
            fault={
                "schema_version": SCHEMA_VERSION,
                "nonce": hashlib.sha256(f"g14:{modality}:{phase}".encode()).hexdigest(),
                "request_id": 1,
                "domain": "graph_snapshot",
                "phase": phase,
            },
        )
        client = engine.connect(MODALITY_GRAPH)
        returned = False
        try:
            client.modalities.ingest(
                modality,
                idempotency_ref=_opaque(
                    "idempotency", modality, 1, f"fault-{phase}", variant=20
                ),
                target_occurrence_id=occurrence,
                bundle_msgpack=_packed(bundle),
                source_bytes=source,
            )
            returned = True
        except Exception:
            pass
        finally:
            client.close()
        if returned:
            _fail("faulted_modality_mutation_returned")
        engine.wait_for_abort(1)
        engine.start(modality_source_limit=MODALITY_SOURCE_LIMIT)
        client = engine.connect(MODALITY_GRAPH)
        try:
            page = client.modalities.query(modality, limit=1)
            present = page != {"records": [], "next": None}
            expected_present = phase == "after_commit_before_ack"
            if present != expected_present:
                _fail("modality_fault_atomicity_mismatch")
            if expected_present:
                _assert_page(
                    page,
                    occurrence=occurrence,
                    version=1,
                    lifecycle="active",
                    bundle=bundle,
                    modality=modality,
                    source=source,
                    failure="modality_fault_recovery_mismatch",
                )
                _assert_stats(
                    client,
                    modality,
                    active=1,
                    total=1,
                    tombstoned=0,
                    events=1,
                    indexes_present=True,
                )
            else:
                _assert_empty(page, "precommit_modality_effect_survived")
                _assert_stats(
                    client,
                    modality,
                    active=0,
                    total=0,
                    tombstoned=0,
                    events=0,
                    indexes_present=False,
                )
        finally:
            client.close()
        return {
            "expected": "complete_effect" if expected_present else "no_effect",
            "modality": modality,
            "observed": "complete_effect" if present else "no_effect",
            "passed": True,
            "phase": phase,
        }
    finally:
        engine.stop()


def _run(
    binary: ExactBinary,
    binary_digest: str,
    performance: dict[str, object],
) -> dict[str, object]:
    authority = _new_ephemeral_authority()
    sources = _sources()
    all_valid_sources = tuple(source for pair in sources.values() for source in pair)
    if (
        set(sources) != set(MODALITIES)
        or any(len(pair) != 2 for pair in sources.values())
        or any(
            not source or len(source) > MODALITY_SOURCE_LIMIT
            for source in all_valid_sources
        )
    ):
        _fail("modality_fixture_inventory_invalid")

    with tempfile.TemporaryDirectory(prefix="eg-exact-multimodal-") as scratch:
        campaign_root = Path(scratch)
        root = campaign_root / "main"
        root.mkdir(mode=0o700)
        engine = ExactEngine(binary, root, authority)
        bundles: dict[str, dict[int, dict[str, Any]]] = {}
        occurrences: dict[str, dict[int, str]] = {}
        attempted_sources = list(all_valid_sources)
        event_kinds = (
            "ingested",
            "ingested",
            "updated",
            "moved_to_cold",
            "restored",
        )
        try:
            engine.start(modality_source_limit=MODALITY_SOURCE_LIMIT)
            engine.bootstrap()
            _with_client(
                engine,
                "__commons__",
                lambda client: client.tenants.create(MODALITY_GRAPH),
            )
            _invalid_authentication_denied(engine)
            client = engine.connect(MODALITY_GRAPH)
            try:
                modality_authority = client.modalities.authority()
                for modality in MODALITIES:
                    if client.modalities.capabilities(modality) != {
                        "component_ready": True,
                        "component_pass": 12,
                        "component_not_applicable": 0,
                        "component_total": 12,
                    }:
                        _fail("modality_component_tck_incomplete")
                    primary_source, secondary_source = sources[modality]
                    primary_v1, primary = _bundle(
                        modality_authority,
                        modality,
                        primary_source,
                        version=1,
                        variant=0,
                    )
                    primary_v2, _ = _bundle(
                        modality_authority,
                        modality,
                        primary_source,
                        version=2,
                        variant=0,
                    )
                    secondary_v1, secondary = _bundle(
                        modality_authority,
                        modality,
                        secondary_source,
                        version=1,
                        variant=1,
                        classification="restricted",
                    )
                    bundles[modality] = {0: primary_v2, 1: secondary_v1}
                    occurrences[modality] = {0: primary, 1: secondary}
                    initial_items = [
                        _stream_item(
                            modality,
                            primary_v1,
                            primary,
                            primary_source,
                            version=1,
                            variant=0,
                            role="stream-ingest",
                        ),
                        _stream_item(
                            modality,
                            secondary_v1,
                            secondary,
                            secondary_source,
                            version=1,
                            variant=1,
                            role="stream-ingest",
                        ),
                    ]
                    outcomes = client.modalities.ingest_stream(modality, initial_items)
                    for index, outcome in enumerate(outcomes, start=1):
                        _assert_outcome(
                            outcome,
                            disposition="Applied",
                            version=1,
                            sequence=index,
                            failure="modality_stream_ingest_mismatch",
                        )
                    replays = client.modalities.ingest_stream(modality, initial_items)
                    for index, outcome in enumerate(replays, start=1):
                        _assert_outcome(
                            outcome,
                            disposition="IdempotentReplay",
                            version=1,
                            sequence=index,
                            failure="modality_stream_replay_mismatch",
                        )

                    conflicting_replay = [dict(item) for item in initial_items]
                    conflicting_replay[0]["bundle_msgpack"] = _packed(primary_v2)
                    conflicting_replay[0]["expected_version"] = 1
                    _expect_error(
                        lambda modality=modality, items=conflicting_replay: (
                            client.modalities.ingest_stream(modality, items)
                        ),
                        "idempotency",
                        "modality_stream_conflicting_replay_was_accepted",
                    )
                    _assert_stats(
                        client,
                        modality,
                        active=2,
                        total=2,
                        tombstoned=0,
                        events=2,
                        indexes_present=True,
                    )

                    rollback_bundle, rollback_occurrence = _bundle(
                        modality_authority,
                        modality,
                        primary_source,
                        version=1,
                        variant=2,
                    )
                    rollback_items = [
                        _stream_item(
                            modality,
                            rollback_bundle,
                            rollback_occurrence,
                            primary_source,
                            version=1,
                            variant=2,
                            role="rollback-new",
                        ),
                        _stream_item(
                            modality,
                            primary_v2,
                            primary,
                            primary_source,
                            version=2,
                            variant=0,
                            role="rollback-conflict",
                            expected_version=99,
                        ),
                    ]
                    _expect_error(
                        lambda modality=modality, rollback_items=rollback_items: (
                            client.modalities.ingest_stream(modality, rollback_items)
                        ),
                        "observation version conflict",
                        "modality_stream_partial_rollback",
                    )
                    _assert_stats(
                        client,
                        modality,
                        active=2,
                        total=2,
                        tombstoned=0,
                        events=2,
                        indexes_present=True,
                    )

                    update = client.modalities.ingest(
                        modality,
                        idempotency_ref=_opaque(
                            "idempotency", modality, 2, "update", variant=0
                        ),
                        target_occurrence_id=primary,
                        expected_version=1,
                        bundle_msgpack=_packed(primary_v2),
                        source_bytes=primary_source,
                    )
                    _assert_outcome(
                        update,
                        disposition="Applied",
                        version=2,
                        sequence=3,
                        failure="modality_update_mismatch",
                    )

                    malformed_source = (
                        b"\xff" * 32
                        if modality == "document"
                        else f"malformed-invalid-{modality}".encode("ascii")
                    )
                    attempted_sources.append(malformed_source)
                    malformed, malformed_occurrence = _bundle(
                        modality_authority,
                        modality,
                        malformed_source,
                        version=1,
                        variant=10,
                    )
                    _expect_error(
                        lambda modality=modality, malformed=malformed, malformed_occurrence=malformed_occurrence, malformed_source=malformed_source: client.modalities.ingest(
                            modality,
                            idempotency_ref=_opaque(
                                "idempotency", modality, 1, "malformed", variant=10
                            ),
                            target_occurrence_id=malformed_occurrence,
                            bundle_msgpack=_packed(malformed),
                            source_bytes=malformed_source,
                        ),
                        "modality codec failure",
                        "malformed_modality_codec_was_accepted",
                    )
                    oversized = bytes([97 + MODALITIES.index(modality)]) * (
                        MODALITY_SOURCE_LIMIT + 1
                    )
                    attempted_sources.append(oversized)
                    oversized_bundle, oversized_occurrence = _bundle(
                        modality_authority,
                        modality,
                        oversized,
                        version=1,
                        variant=11,
                    )
                    _expect_error(
                        lambda modality=modality, oversized=oversized, oversized_bundle=oversized_bundle, oversized_occurrence=oversized_occurrence: client.modalities.ingest(
                            modality,
                            idempotency_ref=_opaque(
                                "idempotency", modality, 1, "oversized", variant=11
                            ),
                            target_occurrence_id=oversized_occurrence,
                            bundle_msgpack=_packed(oversized_bundle),
                            source_bytes=oversized,
                        ),
                        "configured resource limit",
                        "modality_resource_bound_was_not_enforced",
                    )
                    _expect_error(
                        lambda modality=modality, primary=primary, primary_source=primary_source: client.modalities.ingest(
                            modality,
                            idempotency_ref=_opaque(
                                "idempotency", modality, 1, "invalid-bundle", variant=12
                            ),
                            target_occurrence_id=primary,
                            bundle_msgpack=b"\xc1",
                            source_bytes=primary_source,
                        ),
                        "MessagePack",
                        "malformed_modality_bundle_was_accepted",
                    )

                    expected = {
                        primary: (primary_v2, 2, primary_source),
                        secondary: (secondary_v1, 1, secondary_source),
                    }
                    _assert_paged_pair(client, modality, expected)
                    selected = primary if modality != "video" else secondary
                    selected_bundle, selected_version, selected_source = expected[selected]
                    _assert_page(
                        _native_query(client, modality),
                        occurrence=selected,
                        version=selected_version,
                        lifecycle="active",
                        bundle=selected_bundle,
                        modality=modality,
                        source=selected_source,
                        failure="modality_native_selectivity_mismatch",
                    )
                    _assert_empty(
                        _native_negative_query(client, modality),
                        "modality_native_negative_query_matched",
                    )

                    limited = engine.connect(MODALITY_GRAPH, scopes=())
                    try:
                        limited_page = limited.modalities.query(modality, limit=10)
                        _assert_page(
                            limited_page,
                            occurrence=primary,
                            version=2,
                            lifecycle="active",
                            bundle=primary_v2,
                            modality=modality,
                            source=primary_source,
                            failure="classification_policy_filter_failed",
                        )
                        _expect_error(
                            lambda limited=limited, modality=modality: limited.modalities.stats(
                                modality
                            ),
                            "management scope",
                            "nonmanagement_modality_stats_were_visible",
                        )
                    finally:
                        limited.close()

                    cold = client.modalities.move_to_cold(
                        modality, occurrence_id=primary
                    )
                    _assert_outcome(
                        cold,
                        disposition="Applied",
                        version=3,
                        sequence=4,
                        failure="modality_cold_transition_mismatch",
                    )
                    _assert_page(
                        client.modalities.query(
                            modality,
                            after_occurrence_id=(
                                secondary if secondary < primary else None
                            ),
                            limit=1,
                            include_cold=True,
                        ),
                        occurrence=primary,
                        version=3,
                        lifecycle="cold",
                        bundle=primary_v2,
                        modality=modality,
                        source=primary_source,
                        failure="cold_modality_query_mismatch",
                    )
                    restored = client.modalities.restore(
                        modality, occurrence_id=primary
                    )
                    _assert_outcome(
                        restored,
                        disposition="Applied",
                        version=4,
                        sequence=5,
                        failure="modality_restore_mismatch",
                    )
                    _assert_events(client, modality, event_kinds)
            finally:
                client.close()

            engine.crash()
            engine.start(
                lazy_page_size=1, modality_source_limit=MODALITY_SOURCE_LIMIT
            )
            client = engine.connect(MODALITY_GRAPH)
            try:
                for modality in MODALITIES:
                    primary_source, secondary_source = sources[modality]
                    expected = {
                        occurrences[modality][0]: (
                            bundles[modality][0],
                            4,
                            primary_source,
                        ),
                        occurrences[modality][1]: (
                            bundles[modality][1],
                            1,
                            secondary_source,
                        ),
                    }
                    _assert_paged_pair(client, modality, expected)
                    _assert_stats(
                        client,
                        modality,
                        active=2,
                        total=2,
                        tombstoned=0,
                        events=5,
                        indexes_present=True,
                    )
            finally:
                client.close()

            # Prove tenant isolation while the first tenant still has live records.
            engine.stop()
            engine.start(
                tenant=SECOND_TENANT,
                lazy_page_size=1,
                modality_source_limit=MODALITY_SOURCE_LIMIT,
            )
            client = engine.connect(MODALITY_GRAPH, tenant=SECOND_TENANT)
            try:
                for modality in MODALITIES:
                    _assert_empty(
                        client.modalities.query(modality, limit=1, include_cold=True),
                        "cross_tenant_modality_was_visible",
                    )
                    if client.modalities.events(modality, after_sequence=0, limit=10):
                        _fail("cross_tenant_modality_event_was_visible")
                    _expect_error(
                        lambda modality=modality: client.modalities.ingest(
                            modality,
                            idempotency_ref=_opaque(
                                "idempotency", modality, 9, "cross-tenant", variant=0
                            ),
                            target_occurrence_id=occurrences[modality][0],
                            expected_version=4,
                            bundle_msgpack=_packed(bundles[modality][0]),
                            source_bytes=sources[modality][0],
                        ),
                        "forbidden",
                        "cross_tenant_modality_write_was_accepted",
                    )
            finally:
                client.close()

            engine.stop()
            engine.start(
                lazy_page_size=1, modality_source_limit=MODALITY_SOURCE_LIMIT
            )
            client = engine.connect(MODALITY_GRAPH)
            try:
                backup = client.admin.backup("g14-checkpoint", label="g14-synthetic")
                if not isinstance(backup, dict) or backup.get("shards") != 1:
                    _fail("modality_backup_failed")
                restored = client.admin.restore("g14-checkpoint", target_shards=2)
                if not isinstance(restored, dict) or restored.get("restored_shards") != 2:
                    _fail("modality_restore_migration_failed")
            finally:
                client.close()

            engine.stop()
            stages = [
                path
                for path in root.iterdir()
                if path.name.startswith("persist.restored-")
                and path.is_dir()
                and not path.is_symlink()
            ]
            if len(stages) != 1:
                _fail("modality_restore_stage_inventory_mismatch")
            original = root / "pre-migration-store"
            engine.persist_dir.rename(original)
            stages[0].rename(engine.persist_dir)
            engine.start(
                lazy_page_size=1,
                modality_source_limit=MODALITY_SOURCE_LIMIT,
                redb_shards=2,
            )
            client = engine.connect(MODALITY_GRAPH)
            try:
                for modality in MODALITIES:
                    primary_source, secondary_source = sources[modality]
                    expected = {
                        occurrences[modality][0]: (
                            bundles[modality][0],
                            4,
                            primary_source,
                        ),
                        occurrences[modality][1]: (
                            bundles[modality][1],
                            1,
                            secondary_source,
                        ),
                    }
                    _assert_paged_pair(client, modality, expected)
                    for variant, expected_version in ((0, 4), (1, 1)):
                        key = _opaque(
                            "idempotency", modality, 5, "delete", variant=variant
                        )
                        deleted = client.modalities.delete(
                            modality,
                            idempotency_ref=key,
                            occurrence_id=occurrences[modality][variant],
                            expected_version=expected_version,
                        )
                        _assert_outcome(
                            deleted,
                            disposition="Applied",
                            version=expected_version + 1,
                            sequence=6 + variant,
                            failure="modality_delete_mismatch",
                        )
                        replay = client.modalities.delete(
                            modality,
                            idempotency_ref=key,
                            occurrence_id=occurrences[modality][variant],
                            expected_version=expected_version,
                        )
                        _assert_outcome(
                            replay,
                            disposition="IdempotentReplay",
                            version=expected_version + 1,
                            sequence=6 + variant,
                            failure="modality_delete_replay_mismatch",
                        )
                    _assert_empty(
                        client.modalities.query(modality, limit=1, include_cold=True),
                        "deleted_modality_was_visible",
                    )
                    _assert_empty(
                        _native_query(client, modality),
                        "deleted_modality_native_query_was_visible",
                    )
                    _assert_stats(
                        client,
                        modality,
                        active=0,
                        total=2,
                        tombstoned=2,
                        events=7,
                        indexes_present=False,
                    )
                    _assert_events(
                        client, modality, event_kinds + ("deleted", "deleted")
                    )
            finally:
                client.close()

            engine.crash()
            engine.start(
                lazy_page_size=1,
                modality_source_limit=MODALITY_SOURCE_LIMIT,
                redb_shards=2,
            )
            client = engine.connect(MODALITY_GRAPH)
            try:
                for modality in MODALITIES:
                    _assert_stats(
                        client,
                        modality,
                        active=0,
                        total=2,
                        tombstoned=2,
                        events=7,
                        indexes_present=False,
                    )
                    if (
                        client.modalities.collect_tombstones(
                            modality, through_event_sequence=6
                        )
                        != 1
                    ):
                        _fail("modality_tombstone_retention_fence_mismatch")
                    _assert_stats(
                        client,
                        modality,
                        active=0,
                        total=1,
                        tombstoned=1,
                        events=7,
                        indexes_present=False,
                    )
                    if (
                        client.modalities.collect_tombstones(
                            modality, through_event_sequence=7
                        )
                        != 1
                    ):
                        _fail("modality_tombstone_collection_mismatch")
                    _assert_stats(
                        client,
                        modality,
                        active=0,
                        total=0,
                        tombstoned=0,
                        events=7,
                        indexes_present=False,
                    )
            finally:
                client.close()
            engine.crash()
            engine.start(
                lazy_page_size=1,
                modality_source_limit=MODALITY_SOURCE_LIMIT,
                redb_shards=2,
            )
            client = engine.connect(MODALITY_GRAPH)
            try:
                for modality in MODALITIES:
                    _assert_stats(
                        client,
                        modality,
                        active=0,
                        total=0,
                        tombstoned=0,
                        events=7,
                        indexes_present=False,
                    )
            finally:
                client.close()

        finally:
            engine.stop()

        fault_matrix = [
            _run_modality_fault_case(
                binary,
                campaign_root / f"fault-{modality}-{phase}",
                authority,
                modality,
                phase,
                sources[modality][0],
            )
            for modality in MODALITIES
            for phase in FAULT_PHASES
        ]
        _assert_sources_absent(campaign_root, tuple(attempted_sources))

    matrix = [
        {
            "component_tck_not_applicable": 0,
            "component_tck_pass": 12,
            "dimensions": {name: True for name in EXACT_BEHAVIOR_DIMENSIONS},
            "durable_events_before_collection": 7,
            "fault_phases": len(FAULT_PHASES),
            "modality": modality,
            "records_exercised": 2,
        }
        for modality in MODALITIES
    ]
    evidence = {
        "binary": {"sha256": binary_digest, "sealed_copy_verified": True},
        "certification": "epistemic-graph-exact-multimodal",
        "exact_behavior_dimensions": list(EXACT_BEHAVIOR_DIMENSIONS),
        "fault_matrix": fault_matrix,
        "matrix": matrix,
        "modalities": list(MODALITIES),
        "performance": performance,
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "dimensions_per_modality": len(EXACT_BEHAVIOR_DIMENSIONS),
            "fault_cases": len(fault_matrix),
            "modalities": len(matrix),
            "passed": len(matrix),
            "status": "pass",
        },
    }
    encoded = json.dumps(evidence, sort_keys=True, ensure_ascii=True)
    if authority.auth_secret in encoded or authority.signer_key in encoded:
        _fail("evidence_contains_ephemeral_authority")
    return evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certify four served modalities on one exact Epistemic Graph artifact."
    )
    parser.add_argument("--binary", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--performance-evidence", required=True)
    parser.add_argument("--performance-evidence-sha256", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    binary: ExactBinary | None = None
    try:
        output = Path(args.output)
        if output.is_symlink() or output.exists():
            _fail("evidence_destination_must_be_new")
        binary, digest = _validate_binary(args.binary, args.binary_sha256)
        performance = _load_performance_evidence(
            args.performance_evidence,
            args.performance_evidence_sha256,
            digest,
        )
        _write_evidence(output, _run(binary, digest, performance))
    except CertificationError as error:
        print(f"exact multimodal certification failed: {error}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "exact multimodal certification failed: unexpected_runtime_failure",
            file=sys.stderr,
        )
        return 1
    finally:
        if binary is not None:
            binary.close()
    print("exact multimodal certification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
