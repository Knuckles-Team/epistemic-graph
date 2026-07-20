#!/usr/bin/env python3
"""Fail CI if the P2 modality/KnowledgeBatch architecture is bypassed."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"P2 architecture gate failed: {message}")


def main() -> None:
    artifact = read("crates/eg-modality/src/artifact.rs")
    for identity in (
        "ArtifactId",
        "OccurrenceId",
        "RenditionId",
        "SegmentId",
        "FeatureId",
        "EvidenceLocusId",
    ):
        require(identity in artifact, f"missing universal identity {identity}")
    require("PrivacyAttestation" in artifact, "privacy attestation is not mandatory")
    require(
        "impl<'de> Deserialize<'de> for OpaqueRef" in artifact,
        "opaque references can bypass validation during decode",
    )

    for modality in ("document", "image", "audio", "video"):
        contract = read(f"crates/eg-{modality}/src/contract.rs")
        require(
            "is_production_ready()" in contract,
            f"{modality} lacks a 12/12 production TCK assertion",
        )
        require(
            "tck_not_applicable" not in contract,
            f"{modality} still exempts a production TCK dimension",
        )
        require(
            "impl GovernedModality for" in contract,
            f"{modality} has no fail-closed payload privacy validation",
        )
        require(
            "native_production_probe" in contract
            and "native_index_keys" in contract
            and "matches_native_predicate" in contract,
            f"{modality} is not bound to native production certification",
        )
        cargo = read(f"crates/eg-{modality}/Cargo.toml")
        require("serving =" in cargo, f"{modality} has no served runtime feature")

    for modality in ("document", "image", "audio", "video"):
        runtime = read(f"crates/eg-{modality}/src/runtime.rs")
        require("Noop" not in runtime, f"{modality} runtime still contains a no-op")
        require(
            f"Native{modality.title()}Runtime" in runtime,
            f"{modality} lacks a concrete native runtime",
        )
        require(
            "production_probe" in runtime
            and "malformed_and_resource_bounds" in runtime,
            f"{modality} lacks an executed native production probe",
        )
        cargo = read(f"crates/eg-{modality}/Cargo.toml")
        require('sha2 = "0.10"' in cargo, f"{modality} content identity is not SHA-256")
        require(
            "codec =" not in cargo and "extract =" not in cargo,
            f"{modality} retains a no-op codec/extractor feature",
        )

    document = read("crates/eg-document/src/decoder.rs")
    require(
        "LexicalPosting" in document
        and "MAX_POSTINGS" in document
        and "encode_lexeme" in document,
        "document runtime lacks bounded private lexical extraction",
    )
    image = read("crates/eg-image/src/runtime.rs")
    require(
        "ZlibDecoder" in image
        and "unfilter(" in image
        and "to_rgba(" in image
        and "difference_hash(" in image,
        "image runtime lacks native pixel decode and perceptual indexing",
    )
    audio = read("crates/eg-audio/src/runtime.rs")
    require(
        "AudioFeatureWindow" in audio
        and "spectral_centroid_bin" in audio
        and "MAX_DECODED_SAMPLES" in audio,
        "audio runtime lacks bounded waveform/spectral extraction",
    )
    video = read("crates/eg-video/src/runtime.rs")
    for marker in (
        "parse_sample_description",
        "parse_time_to_sample",
        "parse_sample_sizes",
        "parse_chunk_offsets",
        "decode_raw_rgb",
        "media_data_ranges",
        "pixel_depth",
        "validate_versioned_minimum",
        "optional_one_box",
    ):
        require(marker in video, f"video runtime lacks native {marker}")

    native = read("crates/eg-modality/src/native.rs")
    for marker in (
        "DocumentLexeme",
        "ImageRegion",
        "ImagePerceptualHash",
        "AudioWindow",
        "VideoWindow",
        "MAX_TEMPORAL_QUERY_BUCKETS",
        "spatial_cells",
        "temporal_buckets",
        "signature_candidate_bands",
        "2_788",
    ):
        require(marker in native, f"native predicate plane lacks {marker}")

    stream = read("crates/eg-plan/src/result_stream.rs")
    for family in (
        "Graph",
        "Sql",
        "Rdf",
        "Vector",
        "TimeSeries",
        "Job",
        "CrossModal",
    ):
        require(f"Self::{family}" in stream, f"missing KnowledgeBatch family {family}")
    for adapter in (
        "graph_result_stream",
        "sql_result_stream",
        "rdf_result_stream",
        "vector_result_stream",
        "time_series_result_stream",
        "job_result_stream",
        "cross_modal_result_stream",
    ):
        require(adapter in stream, f"missing KnowledgeBatch adapter {adapter}")
    require("write_arrow_ipc" in stream, "native batch stream has no bounded Arrow writer")
    require("safe_reference(&row.id)" in stream, "result ids are not forced opaque")

    handler = read("src/server/handlers/knowledge_stream.rs")
    wire = read("crates/eg-types/src/knowledge_stream.rs")
    require(
        "pub enum KnowledgeStreamQuery" in wire,
        "served KnowledgeBatch has no typed multi-family wire query",
    )
    require(
        "pub struct KnowledgeStreamCursorV1" in wire,
        "served KnowledgeBatch has no versioned authority-bound cursor",
    )
    require(
        "pub placement_ref: String" in wire,
        "served KnowledgeBatch cursor is not placement-bound",
    )
    require(
        "pub integrity_ref: String" in wire
        and "cursor_integrity(authority, cursor)" in handler,
        "served KnowledgeBatch cursor position is not integrity-bound",
    )
    for family in (
        "Graph",
        "Sql",
        "Rdf",
        "Vector",
        "TimeSeries",
        "Job",
        "CrossModal",
    ):
        require(f"Self::{family}" in wire, f"wire query omits {family}")
    require("ArrowIpcV1" in wire, "native KnowledgeStream projection is not Arrow IPC")
    require(
        "CompatibilityMsgpackV1" not in wire,
        "retired KnowledgeStream compatibility projection is still present",
    )

    protocol = read("crates/eg-types/src/protocol.rs")
    require(
        "Method::KnowledgeStream" in read("crates/eg-capabilities/src/lib.rs")
        and "KnowledgeStream {" in protocol,
        "KnowledgeStream is not a governed served protocol method",
    )
    for adapter in (
        "graph_result_stream",
        "sql_result_stream",
        "rdf_result_stream",
        "vector_result_stream",
        "time_series_result_stream",
        "job_result_stream",
        "cross_modal_result_stream",
    ):
        require(adapter in handler, f"served wire bypasses {adapter}")
    require(
        "resume_from(&native_cursor)" in handler,
        "served wire does not enforce native cursor resumption",
    )
    require(
        "KnowledgeStreamAuthority::from_verified" in read("src/server/dispatch.rs")
        and "keyed_ref(server_secret" in handler,
        "served wire authority is not keyed from verified RequestContext claims",
    )
    require(
        "placement_ref: keyed_opaque(authority, \"placement\"" in handler
        and "cursor.placement_ref != self.context.placement_ref" in stream,
        "served wire cursor does not fence placement changes",
    )
    require(
        'keyed_opaque(authority, "query"' in handler
        and 'keyed_opaque(authority, "result"' in handler,
        "query/result identifiers are not privacy-safe keyed references",
    )
    dispatch = read("src/server/dispatch.rs")
    graph_dispatch = dispatch[dispatch.find("async fn dispatch_graph_op_inner") :]
    acl = graph_dispatch.find("if let Err(denied) = check_graph_access")
    placement = graph_dispatch.find("let routed_raft")
    knowledge_stream = graph_dispatch.find(
        "if matches!(&method, Method::KnowledgeStream"
    )
    require(
        acl >= 0 and placement > acl and knowledge_stream > placement,
        "KnowledgeStream is routed before graph ACL/placement semantics",
    )

    served = read("crates/eg-modality/src/served.rs")
    require("UnsafePayload" in served, "served modalities do not reject unsafe payloads")
    require(
        "events_after_authorized" in served,
        "served event replay is not policy filtered",
    )
    require(
        "native_index: BTreeMap<NativeIndexKey" in served
        and "pub fn query_native(" in served
        and "NativeQueryStats" in served
        and "rebuild_indexes" in served,
        "served modalities lack rebuilt native posting queries",
    )
    served_tests = read("crates/eg-modality/tests/served_runtime.rs")
    require(
        "4_096" in served_tests
        and "stats.examined" in served_tests
        and "recover" in served_tests,
        "native posting query lacks bounded scale and recovery evidence",
    )
    fleet_tck = read("crates/eg-modality/tests/fleet_tck.rs")
    require(
        "native_probe_passed()" in fleet_tck,
        "served fleet TCK does not require the native production probe",
    )

    wire = read("crates/eg-types/src/protocol.rs")
    require("ServedModality" in wire, "main protocol has no served modality method")
    wire_types = read("crates/eg-types/src/modality.rs")
    for operation in (
        "Authority",
        "Ingest",
        "IngestStream",
        "Query",
        "NativeQuery",
        "Delete",
        "MoveToCold",
        "Restore",
        "Events",
        "Stats",
        "CollectTombstones",
        "Capabilities",
    ):
        require(operation in wire_types, f"wire omits served operation {operation}")

    handler = read("src/server/handlers/modality.rs")
    for runtime in (
        "NativeDocumentRuntime",
        "NativeImageRuntime",
        "NativeAudioRuntime",
        "NativeVideoRuntime",
    ):
        require(runtime in handler, f"server does not execute {runtime}")
    require(
        "from_verified" in handler and "RequestContextClaims" in handler,
        "modality authority is not derived from verified request claims",
    )
    require(
        "ValueCipher" in handler and "is_sealed" in handler,
        "runtime snapshots are not fail-closed AEAD state",
    )
    require(
        ".any(|occurrence| !authority.scope.authorizes_occurrence(occurrence))"
        in handler,
        "a cross-policy occurrence can hitchhike in an authorized returned bundle",
    )
    require(
        "EPISTEMIC_GRAPH_MODALITY_MAX_SOURCE_BYTES" in handler
        and "HARD_MAX_SOURCE_BYTES" in handler,
        "native decoding has no configurable hard resource ceiling",
    )
    require(
        "lexeme_ref" in handler
        and "ServedNativePredicate" in handler
        and "query_native" in handler,
        "server does not authority-key and execute native predicates",
    )
    require(
        "target_resource_closure" in handler
        and 'artifact.content_ref.namespace() != "content"' in handler
        and ".validate_certified()" in handler,
        "native modality ingest is not target-bound and certified before decoding",
    )
    require(
        "store_runtime_excluding_sources" in handler
        and "raw source would enter modality snapshot" in handler,
        "plaintext normalized snapshots are not checked for surviving source bytes",
    )
    require(
        "require_management" in handler and "through_event_sequence" in wire_types,
        "aggregate stats or retention collection is not management/fence governed",
    )
    python_client = read("epistemic_graph/client.py")
    for method in (
        "search_documents",
        "query_image_region",
        "query_similar_images",
        "query_audio_window",
        "query_video_window",
    ):
        require(f"async def {method}(" in python_client, f"Python client omits {method}")
    transport = read("src/server/transport.rs")
    require(
        "EPISTEMIC_GRAPH_MAX_REQUEST_BYTES" in transport
        and "HARD_MAX_REQUEST_FRAME_BYTES" in transport
        and "len > max_frame_bytes" in transport,
        "wire frame can allocate unbounded memory before modality validation",
    )
    require(
        "resource_gate_is_bounded_and_every_leaf_is_12_of_12" in handler,
        "server modality resource/correctness gate is absent",
    )

    dispatch = read("src/server/dispatch.rs")
    require(
        "dispatch_served_modality" in dispatch
        and "commit_conditional_mutation" in dispatch,
        "served modalities bypass the graph mutation gateway",
    )
    require(
        "ModalityAuthority::from_verified" in dispatch
        and "verified_context.claims()" in dispatch
        and "dispatch_served_modality" in dispatch,
        "served modalities permit unverified request authority",
    )
    mutation = read("src/server/mutation.rs")
    receipt = mutation[
        mutation.find("fn durable_receipt_method") : mutation.find(
            "/// Try to apply a coalescable", mutation.find("fn durable_receipt_method")
        )
    ]
    require(
        "durable_receipt_method" in mutation and "served_modality_v1" in mutation,
        "ephemeral source bytes can enter durable mutation receipts",
    )
    require(
        "canonical_body_bytes" not in receipt
        and "served-modality-receipt-v1" in receipt,
        "modality receipt retains a direct digest of the source-bearing wire body",
    )
    audit = read("src/audit.rs")
    require(
        'event_type == "authoritative_state_operation"' in audit
        and "AUTHORITATIVE_STATE_MUTATION" in audit,
        "state-backed modality commits do not append a digest-only audit link",
    )
    raft = read("src/raft/mod.rs")
    command_start = raft.find("pub struct SanitizedModalityRaftCommand")
    command_end = raft.find("/// The application request replicated through Raft")
    require(
        0 <= command_start < command_end,
        "the current sanitized modality Raft command is absent",
    )
    command = raft[command_start:command_end]
    require(
        "sealed_runtime_state" in command
        and "source_bytes" not in command
        and "deny_unknown_fields" in command
        and "sanitized_modality_tag" in command
        and "MAX_REPLICATED_MODALITY_STATE_BYTES" in command
        and "is_sealed" in command,
        "Raft modality command is not encrypted, authenticated, bounded, and source-free",
    )
    require(
        "encrypted_command_round_trips_without_raw_source" in raft
        and "unsealed_or_forged_replica_state_fails_closed" in raft,
        "sanitized modality Raft correctness/privacy tests are absent",
    )
    require(
        "mutation_batch_audit_and_outbox_retain_only_the_safe_receipt" in raft
        and "AUTHORITATIVE_STATE_MUTATION|sha256:" in raft,
        "Raft privacy test does not cover MutationBatch, audit, and outbox surfaces",
    )
    modality_dispatch = dispatch[
        dispatch.find("async fn replicate_served_modality(") : dispatch.find(
            "// ── Raft write-routing barrier"
        )
    ]
    require(
        "durable_receipt_method(&method)" in modality_dispatch
        and "let Method::ServedModality { op } = method else" in modality_dispatch
        and "SanitizedModalityMutation::from_served(&op)" in modality_dispatch
        and "SanitizedModalityRaftCommand::new(" in modality_dispatch
        and "command: crate::raft::ReplicatedMutation::served_modality(command)"
        in modality_dispatch,
        "source-bearing ServedModality can bypass the sanitized Raft constructor",
    )
    mutation_apply = read("src/mutation_apply.rs")
    require(
        "raw source bytes are replaced by the state-backed receipt" in mutation_apply
        and "if let Method::ServedModality { op }" in mutation_apply,
        "durability policy does not document the state-backed source-free modality path",
    )
    raft_store = read("src/raft/store.rs")
    require(
        "NativeMutationCommand::ServedModality { command }" in raft_store
        and "command.validate(&server_secret)?;" in raft_store
        and "command.sealed_runtime_state.clone()" in raft_store
        and "command.result_msgpack.clone()" in raft_store
        and ".map(super::SanitizedModalityRaftCommand::receipt_method)" in raft_store,
        "followers do not validate and deterministically commit sanitized modality state",
    )

    facade = read("Cargo.toml")
    require(
        "knowledge-batch = [" in facade
        and '"dep:eg-modality"' in facade[facade.find("knowledge-batch = [") : facade.find("]", facade.find("knowledge-batch = ["))]
        and '"eg-plan/knowledge-batch"' in facade
        and '"eg-types/knowledge-batch"' in facade,
        "facade does not expose native KnowledgeBatch",
    )
    full_line = next(
        (line for line in facade.splitlines() if line.startswith("full = [")), ""
    )
    require('"knowledge-batch"' in full_line, "full deployment omits KnowledgeBatch")
    require('"modality-serving"' in full_line, "full deployment omits live modality serving")

    print("P2 modality architecture gate passed")


if __name__ == "__main__":
    main()
