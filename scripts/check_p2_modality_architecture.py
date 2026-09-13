#!/usr/bin/env python3
"""Fail CI if the P2 modality/KnowledgeBatch architecture is bypassed."""

from __future__ import annotations

import re
from pathlib import Path

from method_policy_inventory import load_capability_sources, parse_method_policy_table
from rust_callgraph import reachable_source, top_level_fns
from rust_module_tree import read_module_tree

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def compiler_source(relative: str) -> str:
    """Return a Rust module's compiler-reachable source tree."""

    return read_module_tree(relative, root_dir=ROOT, include_tests=True)


def knowledge_stream_handler_source() -> str:
    """Return the one declared KnowledgeStream module tree."""

    legacy_handler = ROOT / "src/server/handlers/knowledge_stream.rs"
    require(
        not legacy_handler.exists(),
        "KnowledgeStream retains an ambiguous legacy handler module",
    )
    return "\n".join(
        read(path)
        for path in (
            "src/server/handlers/knowledge_stream/mod.rs",
            "src/server/handlers/knowledge_stream/families.rs",
            "src/server/handlers/knowledge_stream/stream.rs",
        )
    )


def require_knowledge_stream_authority(handler: str) -> None:
    """Pin the sole lease-bound served authority and page fences."""

    router = read("src/server/dispatch/router.rs")
    authority_path = reachable_source(router, "dispatch_governed_stream_write_methods")
    require(
        all(
            marker in authority_path
            for marker in (
                "KnowledgeStreamAuthority::from_verified_with_lease(",
                "auth_secret",
                "verified_context.claims()",
                "MintAuthorization::compute_mac",
                "MintAuthorization::new",
                "CarrierAuthority::from_verified",
                "mint_policy_decision_lease(",
                "AccessLevel::Read",
                "policy_store()",
            )
        )
        and "keyed_ref(server_secret" in handler,
        "served wire authority is not bound to a durable policy lease",
    )
    require(
        all(
            marker in handler
            for marker in (
                "pub(crate) fn from_verified_with_lease(",
                "authority.policy_lease = Some(lease)",
                "authority.policy_store = Some(policy_store)",
                "fn validate_stream_preflight(",
                "validate_stream_preflight(authority, caller, graph_name, carrier, "
                "&request)",
                "authority.validate_before()",
                "authority.validate_after()",
            )
        ),
        "KnowledgeStream lacks a lease-bound constructor or strict pre/post page "
        "fences",
    )


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"P2 architecture gate failed: {message}")


def require_native_runtime_contract(modality: str, runtime: str) -> None:
    """Require the concrete runtime's executed production probe contract."""

    require("Noop" not in runtime, f"{modality} runtime still contains a no-op")
    require(
        f"Native{modality.title()}Runtime" in runtime,
        f"{modality} lacks a concrete native runtime",
    )
    require(
        "production_probe" in runtime and "malformed_and_resource_bounds" in runtime,
        f"{modality} lacks an executed native production probe",
    )


def call_offset(body: str, name: str) -> int:
    """Offset of a call to `name` in `body`, or -1.

    Word-boundary matched on purpose: a plain `find` for
    `gate_graph_op_under_lock(` also matches
    `renamed_gate_graph_op_under_lock(`, so renaming the ACL gate away would
    have satisfied the ordering check below. Found by planting exactly that.
    """

    hit = re.search(rf"\b{name}\(", body)
    return hit.start() if hit else -1


def require_graph_dispatch_ordering(dispatch: str) -> None:
    """Graph ACL, then placement, then KnowledgeStream -- as an order of CALLS.

    That has always been the property. It used to be checkable as an order of
    byte offsets inside one enormous `dispatch_graph_op_inner`; the complexity
    program decomposed that function, moving the ACL check into
    `check_graph_op_access` (via `gate_graph_op_under_lock`), placement into
    `resolve_routed_raft`, and the KnowledgeStream arm into a post-lock router
    DEFINED LATER IN THE FILE. The offsets then said the ordering was violated
    while it held exactly -- the failure mode that gets a gate weakened rather
    than a bug fixed. See scripts/rust_callgraph.py.
    """

    fns = top_level_fns(dispatch)
    inner = fns.get("dispatch_graph_op_inner", "")
    require(inner != "", "dispatch_graph_op_inner is absent from dispatch.rs")
    acl = call_offset(inner, "gate_graph_op_under_lock")
    placement = call_offset(inner, "resolve_routed_raft")
    routing = call_offset(inner, "route_graph_op_method")
    require(
        acl >= 0 and placement > acl and routing > placement,
        "KnowledgeStream is routed before graph ACL/placement semantics",
    )
    # ...and each delegate still does what its name claims. Without these, the
    # ordering above could be satisfied by three helpers that check nothing.
    require(
        "check_graph_access(" in fns.get("check_graph_op_access", ""),
        "the graph ACL gate no longer performs the graph ACL check",
    )
    ks_arm = "if matches!(&method, Method::KnowledgeStream"
    ks_routers = [name for name, body in fns.items() if ks_arm in body]
    post_lock = reachable_source(dispatch, "route_graph_op_method")
    require(
        ks_routers != []
        and all(fns[name] in post_lock for name in ks_routers)
        and "dispatch_graph_op_inner" not in ks_routers,
        "KnowledgeStream is routed outside the post-lock router, so it no "
        "longer sits behind graph ACL and placement resolution",
    )


def require_universal_artifact_identity() -> None:
    """Universal artifact identities and fail-closed opaque-reference decoding."""

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


def require_modality_governed_contracts() -> None:
    """Every modality's 12/12 production TCK and governed-payload contract."""

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
        runtime = compiler_source(f"crates/eg-{modality}/src/runtime.rs")
        require_native_runtime_contract(modality, runtime)
        cargo = read(f"crates/eg-{modality}/Cargo.toml")
        require('sha2 = "0.10"' in cargo, f"{modality} content identity is not SHA-256")
        require(
            "codec =" not in cargo and "extract =" not in cargo,
            f"{modality} retains a no-op codec/extractor feature",
        )


def require_modality_native_runtimes() -> None:
    """Every modality's concrete native runtime and its bounded extraction."""

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
    video = compiler_source("crates/eg-video/src/runtime.rs")
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


def require_native_predicate_plane() -> None:
    """The native predicate plane the modalities index into."""

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


def require_knowledge_batch_result_stream() -> None:
    """The KnowledgeBatch families, adapters, and bounded Arrow writer."""

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
    require(
        "write_arrow_ipc" in stream, "native batch stream has no bounded Arrow writer"
    )
    require("safe_reference(&row.id)" in stream, "result ids are not forced opaque")


def require_served_knowledge_stream_wire() -> None:
    """The served KnowledgeStream wire: typed query, bound cursor, ACL ordering."""

    handler = knowledge_stream_handler_source()
    stream = read("crates/eg-plan/src/result_stream.rs")
    wire = read("crates/eg-types/src/knowledge_stream.rs")
    require(
        "pub enum KnowledgeStreamQuery" in wire,
        "served KnowledgeBatch has no typed multi-family wire query",
    )
    require(
        "pub struct KnowledgeStreamCursor" in wire,
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
    require("ArrowIpc" in wire, "native KnowledgeStream projection is not Arrow IPC")
    require(
        "CompatibilityMsgpackV1" not in wire,
        "retired KnowledgeStream compatibility projection is still present",
    )

    protocol = read("crates/eg-types/src/protocol.rs")
    registry_rows = parse_method_policy_table(load_capability_sources(ROOT))
    require(
        any(row.name == "KnowledgeStream" for row in registry_rows)
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
    require_knowledge_stream_authority(handler)
    require(
        'placement_ref: keyed_opaque(authority, "placement"' in handler
        and "cursor.placement_ref != self.context.placement_ref" in stream,
        "served wire cursor does not fence placement changes",
    )
    require(
        'keyed_opaque(authority, "query"' in handler
        and 'keyed_opaque(authority, "result"' in handler,
        "query/result identifiers are not privacy-safe keyed references",
    )
    dispatch = read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    require_graph_dispatch_ordering(dispatch)


def require_served_modality_plane() -> None:
    """Served modalities: unsafe-payload rejection, filtered replay, native postings."""

    # `served.rs` was split into a `served/` module tree (error/mutation/query/
    # records/indexes/protocol/recovery), so reading the parent file alone saw
    # only its `mod` declarations and reported every property below as missing
    # -- `UnsafePayload` lives in `served/error.rs` now. Read the whole
    # compiler-reachable tree, which is what these assertions have always meant.
    served = compiler_source("crates/eg-modality/src/served.rs")
    require(
        "UnsafePayload" in served, "served modalities do not reject unsafe payloads"
    )
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


def require_served_modality_protocol() -> None:
    """The served modality protocol method and its complete wire operation set."""

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


def require_modality_request_handler() -> None:
    """The server handler: native runtimes, verified claims, sealed state, ceilings."""

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


def require_target_bound_ingest(handler: str) -> None:
    """Require certified target closure and content binding before native decode."""

    require(
        "ResourceClosure::resolve(&bundle, &target)?" in handler
        and 'artifact.content_ref.namespace() != "content"' in handler
        and ".validate_certified()" in handler,
        "native modality ingest is not target-bound and certified before decoding",
    )


def require_modality_resource_bounds() -> None:
    """Configurable hard resource ceilings and authority-keyed native predicates."""

    handler = compiler_source("src/server/handlers/modality.rs")
    wire_types = read("crates/eg-types/src/modality.rs")
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
    require_target_bound_ingest(handler)
    require(
        "store_runtime_excluding_sources" in handler
        and "raw source would enter modality snapshot" in handler,
        "plaintext normalized snapshots are not checked for surviving source bytes",
    )
    require(
        "require_management" in handler and "through_event_sequence" in wire_types,
        "aggregate stats or retention collection is not management/fence governed",
    )


def require_modality_client_surface() -> None:
    """The Python client's served-modality surface."""

    python_client = read("epistemic_graph/client.py")
    for method in (
        "search_documents",
        "query_image_region",
        "query_similar_images",
        "query_audio_window",
        "query_video_window",
    ):
        require(
            f"async def {method}(" in python_client, f"Python client omits {method}"
        )


def require_modality_transport_path() -> None:
    """The transport/dispatch path a served modality request actually travels."""

    handler = read("src/server/handlers/modality.rs")
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

    dispatch = read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
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


def require_modality_mutation_governance() -> None:
    """Mutation, audit, and raft governance of served modality state."""

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


def require_modality_audit_and_replication() -> None:

    audit = read("src/audit.rs")
    require(
        'event_type == "authoritative_state_operation"' in audit
        and "AUTHORITATIVE_STATE_MUTATION" in audit,
        "state-backed modality commits do not append a digest-only audit link",
    )


def require_modality_raft_replication() -> None:
    """Raft replication of sanitized served-modality commands."""

    raft = read("src/raft/mod.rs")
    command_name = raft.find("pub struct SanitizedModalityRaftCommand")
    command_start = raft.rfind("#[serde(deny_unknown_fields)]", 0, command_name)
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
        "Raft modality command is not encrypted, authenticated, bounded, and "
        "source-free",
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


def require_modality_replication_dispatch() -> None:
    """The dispatch path that sanitizes and submits a modality replication."""

    dispatch = read_module_tree("src/server/dispatch.rs", root_dir=ROOT)
    # Same repair as the ordering check above: `replicate_served_modality` was
    # decomposed into `decode_modality_replication_inputs` ->
    # `build_modality_raft_command` -> `submit_modality_replication`, all
    # DEFINED ABOVE it, so a slice running forward from its header found none
    # of the five tokens below. Resolving by call graph keeps the assertion
    # scoped to the replication path and reads it through the extraction.
    modality_dispatch = reachable_source(dispatch, "replicate_served_modality")
    require(
        modality_dispatch != "",
        "replicate_served_modality is absent from dispatch.rs",
    )
    require(
        "durable_receipt_method(&method)" in modality_dispatch
        and "let Method::ServedModality { op } = method else" in modality_dispatch
        and "SanitizedModalityMutation::from_served(&op)" in modality_dispatch
        and "SanitizedModalityRaftCommand::new(" in modality_dispatch
        and "command: crate::raft::ReplicatedMutation::served_modality(command)"
        in modality_dispatch,
        "source-bearing ServedModality can bypass the sanitized Raft constructor",
    )


def require_modality_durability() -> None:
    """Source-free durability and deterministic follower commit of modality state."""

    mutation_apply = read("src/mutation_apply.rs")
    require(
        "raw source bytes are replaced by the state-backed receipt" in mutation_apply
        and "if let Method::ServedModality { op }" in mutation_apply,
        "durability policy does not document the state-backed source-free modality "
        "path",
    )
    raft_store = read("src/raft/store.rs")
    require(
        "NativeMutationCommand::ServedModality { command }" in raft_store
        # `&server_secret` or `server_secret`: WB1-EG-01 extracted this into
        # `validate_modality_command`, whose parameter is already `&str`, so
        # the borrow moved to the call site. The property is that the replica
        # validates the command before committing it.
        and re.search(r"command\.validate\(&?server_secret\)\?;", raft_store)
        is not None
        and "command.sealed_runtime_state.clone()" in raft_store
        and "command.result_msgpack.clone()" in raft_store
        and ".map(super::SanitizedModalityRaftCommand::receipt_method)" in raft_store,
        "followers do not validate and deterministically commit sanitized modality "
        "state",
    )


def require_facade_feature_exposure() -> None:
    """The facade features that actually ship KnowledgeBatch and modality serving."""

    facade = read("Cargo.toml")
    require(
        "knowledge-batch = [" in facade
        and '"dep:eg-modality"'
        in facade[
            facade.find("knowledge-batch = [") : facade.find(
                "]", facade.find("knowledge-batch = [")
            )
        ]
        and '"eg-plan/knowledge-batch"' in facade
        and '"eg-types/knowledge-batch"' in facade,
        "facade does not expose native KnowledgeBatch",
    )
    full_line = next(
        (line for line in facade.splitlines() if line.startswith("full = [")), ""
    )
    require('"knowledge-batch"' in full_line, "full deployment omits KnowledgeBatch")
    require(
        '"modality-serving"' in full_line, "full deployment omits live modality serving"
    )


def main() -> None:
    """Run every P2 modality architecture contract, in dependency order."""

    require_universal_artifact_identity()
    require_modality_governed_contracts()
    require_modality_native_runtimes()
    require_native_predicate_plane()
    require_knowledge_batch_result_stream()
    require_served_knowledge_stream_wire()
    require_served_modality_plane()
    require_served_modality_protocol()
    require_modality_request_handler()
    require_modality_resource_bounds()
    require_modality_client_surface()
    require_modality_transport_path()
    require_modality_mutation_governance()
    require_modality_audit_and_replication()
    require_modality_raft_replication()
    require_modality_replication_dispatch()
    require_modality_durability()
    require_facade_feature_exposure()
    print("P2 modality architecture gate passed")


if __name__ == "__main__":
    main()
