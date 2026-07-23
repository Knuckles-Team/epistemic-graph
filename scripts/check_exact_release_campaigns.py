#!/usr/bin/env python3
"""Static release gate for G-04, G-14, G-15, and G-17 campaigns."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_WIRES = {
    "native_rpc",
    "postgresql",
    "mysql",
    "mssql",
    "sqlite",
    "bolt",
    "redis",
    "amqp",
    "mqtt",
    "stomp",
}
EXPECTED_WIRE_FEATURES = {
    "server",
    "pgwire",
    "mysql-wire",
    "mssql-wire",
    "sqlite-wire",
    "bolt-wire",
    "redis-wire",
    "amqp-wire",
    "mqtt-wire",
    "stomp-wire",
}
EXPECTED_DATA_PATHS = {
    "graph",
    "property",
    "union",
    "semantic",
    "topology",
    "rdf",
    "time_series",
    "vector",
    "blob",
    "job",
    "sql",
    "cache",
    "kv",
    "broker",
}
EXPECTED_FAMILIES = {
    "graph",
    "sql",
    "rdf",
    "vector",
    "time_series",
    "job",
    "cross_modal",
}
EXPECTED_BATCH_REQUIREMENTS = {
    "arrow_parity",
    "pushdown",
    "bounded_streaming",
    "paging_resume",
    "cancellation",
    "backpressure",
    "snapshot_correctness",
}
EXPECTED_REASONING_CASES = {
    "projection_lag",
    "restart",
    "contradiction",
    "retraction",
    "valid_transaction_time_change",
    "causal_recomputation",
    "assumptions",
    "counterexamples",
    "repair",
}
EXPECTED_MODALITIES = {"document", "image", "audio", "video"}
EXPECTED_EXACT_BEHAVIOR_DIMENSIONS = {
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
}


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _literal(tree: ast.AST, name: str) -> object:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ValueError(f"{name} is not a literal assignment")


def _literal_set(tree: ast.AST, name: str) -> set[str]:
    value = _literal(tree, name)
    if not isinstance(value, (tuple, list, set)) or not all(
        isinstance(item, str) for item in value
    ):
        raise ValueError(f"{name} must be a literal string collection")
    return set(value)


def _require(source: str, tokens: set[str], label: str, errors: list[str]) -> None:
    for token in sorted(tokens):
        if token not in source:
            errors.append(f"{label}: missing {token!r}")


def _qualified_name(node: ast.AST) -> str | None:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def _call_names(tree: ast.AST) -> set[str]:
    return {
        name
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        if (name := _qualified_name(node.func)) is not None
    }


def _require_call_suffixes(
    tree: ast.AST, suffixes: set[str], label: str, errors: list[str]
) -> None:
    calls = _call_names(tree)
    for suffix in sorted(suffixes):
        if not any(name == suffix or name.endswith(f".{suffix}") for name in calls):
            errors.append(f"{label}: missing executable call {suffix!r}")


def _has_exact_fault_matrix(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if not isinstance(node, ast.ListComp) or len(node.generators) != 2:
            continue
        iterators = {
            generator.iter.id
            for generator in node.generators
            if isinstance(generator.iter, ast.Name)
        }
        if iterators == {"MODALITIES", "FAULT_PHASES"} and any(
            isinstance(child, ast.Call)
            and _qualified_name(child.func) == "_run_modality_fault_case"
            for child in ast.walk(node.elt)
        ):
            return True
    return False


def _check_harness_baseline(
    relative: str,
    source: str,
    errors: list[str],
) -> ast.AST | None:
    try:
        tree = ast.parse(source)
    except SyntaxError as error:
        errors.append(f"{relative}: syntax error at line {error.lineno}")
        return None
    _require(
        source,
        {
            '"--binary"',
            '"--binary-sha256"',
            '"--output"',
            "_validate_binary",
            "ExactEngine",
            "evidence_destination_must_be_new",
            '"sha256": binary_digest',
            "TemporaryDirectory",
            "evidence_contains_ephemeral_authority",
        },
        relative,
        errors,
    )
    for forbidden in (
        "cargo build",
        "cargo run",
        "target/debug",
        "target/release",
        "resolve_engine_binary",
        "shutil.which",
        "pytest.skip",
    ):
        if forbidden in source:
            errors.append(
                f"{relative}: forbidden discovery/local-data token {forbidden!r}"
            )
    if re.search(r'"binary"\s*:\s*str\s*\(', source):
        errors.append(f"{relative}: evidence retains the executable path")
    return tree


def main() -> int:
    errors: list[str] = []
    protocol_relative = "scripts/certify_exact_protocol_authorization.py"
    multimodal_relative = "scripts/certify_exact_multimodal.py"
    batch_relative = "scripts/certify_exact_knowledge_batch.py"
    reasoning_relative = "scripts/certify_exact_reasoning_repair.py"
    protocol = _read(protocol_relative)
    multimodal = _read(multimodal_relative)
    batch = _read(batch_relative)
    reasoning = _read(reasoning_relative)
    protocol_tree = _check_harness_baseline(protocol_relative, protocol, errors)
    multimodal_tree = _check_harness_baseline(multimodal_relative, multimodal, errors)
    batch_tree = _check_harness_baseline(batch_relative, batch, errors)
    reasoning_tree = _check_harness_baseline(reasoning_relative, reasoning, errors)

    if protocol_tree is not None:
        try:
            wires = _literal_set(protocol_tree, "WIRE_PROTOCOLS")
            data_paths = _literal_set(protocol_tree, "DATA_PATHS")
            wire_features = set(_literal(protocol_tree, "WIRE_FEATURES").values())
            listener_env = set(_literal(protocol_tree, "LISTENER_ENV").values())
            launcher_tree = ast.parse(_read("scripts/certify_exact_fault_restart.py"))
            allowed_listener_env = _literal_set(
                launcher_tree, "EXACT_OPTIONAL_LISTENER_ENV"
            )
        except (ValueError, AttributeError) as error:
            errors.append(f"protocol inventory: {error}")
        else:
            if wires != EXPECTED_WIRES:
                errors.append(
                    "protocol inventory is not the ten full-tier direct wires"
                )
            if wire_features != EXPECTED_WIRE_FEATURES:
                errors.append("wire feature mapping is incomplete")
            if data_paths != EXPECTED_DATA_PATHS:
                errors.append("generated terminal data-path matrix is incomplete")
            if listener_env != allowed_listener_env:
                errors.append(
                    "protocol listeners exceed or drift from launcher allowlist"
                )
    _require(
        protocol,
        {
            "_wait_for_listeners(addresses)",
            "_probe_native(engine)",
            "PROBES[protocol](addresses[protocol])",
            "peer.query.sql(sql) == []",
            "peer.rdf.sparql",
            "peer.graph.semantic_search",
            "peer.query.unified",
            "peer.timeseries.range",
            "peer.blob.fetch",
            "peer.jobs.status",
            '"KvGet"',
            "peer.broker.consume",
            "engine.start(tenant=SECOND_TENANT)",
            "cross_tenant_authorization_matrix_failed",
        },
        "protocol campaign",
        errors,
    )

    if multimodal_tree is not None:
        try:
            modalities = _literal_set(multimodal_tree, "MODALITIES")
            dimensions = _literal_set(
                multimodal_tree, "EXACT_BEHAVIOR_DIMENSIONS"
            )
        except ValueError as error:
            errors.append(f"multimodal inventory: {error}")
        else:
            if modalities != EXPECTED_MODALITIES:
                errors.append("multimodal inventory is not the exact four")
            if dimensions != EXPECTED_EXACT_BEHAVIOR_DIMENSIONS:
                errors.append("multimodal behavior inventory is not the exact twelve")
        _require_call_suffixes(
            multimodal_tree,
            {
                "modalities.ingest_stream",
                "modalities.ingest",
                "modalities.query",
                "modalities.search_documents",
                "modalities.query_image_region",
                "modalities.query_audio_window",
                "modalities.query_video_window",
                "modalities.stats",
                "modalities.move_to_cold",
                "modalities.restore",
                "modalities.events",
                "modalities.delete",
                "modalities.collect_tombstones",
                "modalities.capabilities",
                "admin.backup",
                "admin.restore",
                "engine.crash",
                "_load_performance_evidence",
                "_assert_sources_absent",
            },
            "multimodal campaign",
            errors,
        )
        if not _has_exact_fault_matrix(multimodal_tree):
            errors.append("multimodal campaign lacks the four-by-four fault matrix")

    if batch_tree is not None:
        try:
            families = _literal_set(batch_tree, "FAMILIES")
            requirements = _literal_set(batch_tree, "REQUIREMENTS")
        except ValueError as error:
            errors.append(f"KnowledgeBatch inventory: {error}")
        else:
            if families != EXPECTED_FAMILIES:
                errors.append("KnowledgeBatch family inventory is not the exact seven")
            if requirements != EXPECTED_BATCH_REQUIREMENTS:
                errors.append("KnowledgeBatch requirement inventory is incomplete")
    _require(
        batch,
        {
            "client.knowledge.pull",
            "arrow_schema_digest",
            "tampered_cursor_was_accepted",
            "first_client.close()",
            "BACKPRESSURE_SECONDS",
            "knowledge_batch_page_bound_violated",
            "cross_family_arrow_schema_mismatch",
            "changed_snapshot_cursor_was_accepted",
            "immutable_source_resumed",
        },
        "KnowledgeBatch campaign",
        errors,
    )

    if reasoning_tree is not None:
        try:
            cases = _literal_set(reasoning_tree, "CASES")
        except ValueError as error:
            errors.append(f"reasoning inventory: {error}")
        else:
            if cases != EXPECTED_REASONING_CASES:
                errors.append("reasoning campaign inventory is not the exact nine")
    _require(
        reasoning,
        {
            '"Stale"',
            '"Fresh"',
            "engine.stop()",
            "engine.start()",
            "reasoning_projection_restart_mismatch",
            "resolve_conflict",
            "what_changed(50, 200)",
            "what_would_invalidate",
            "causal_estimate",
            "causal_counterfactual",
            "causal_recomputation_not_deterministic",
            "stale_recompute_fence_was_accepted",
            "recompute_materialization",
            "belief_retraction_restart_mismatch",
        },
        "reasoning campaign",
        errors,
    )

    cargo = tomllib.loads(_read("Cargo.toml"))
    full = set(cargo.get("features", {}).get("full", []))
    missing_full = EXPECTED_WIRE_FEATURES - full
    if missing_full:
        errors.append(f"full feature lost direct wires: {sorted(missing_full)}")
    for feature in (
        "modality-serving",
        "knowledge-batch",
        "epistemic-tms",
        "epistemic-causal",
    ):
        if feature not in full:
            errors.append(f"full feature lost {feature}")

    universal = _read("scripts/check_universal_read_rls.py")
    _require(
        universal,
        {
            "generated served-read inventory",
            "GraphReadAuthority::from_verified",
            "CarrierAuthority::from_verified",
            "default-deny RLS",
            "result-cache actor key",
        },
        "universal read authority gate",
        errors,
    )
    modality = _read("scripts/check_p2_modality_architecture.py")
    _require(
        modality,
        {"KnowledgeBatch", "KnowledgeStreamCursorV1", "ArrowIpcV1", "cross_modal"},
        "modality architecture gate",
        errors,
    )
    analytics = _read("scripts/check_p2_analytics_reasoning_architecture.py")
    _require(
        analytics,
        {"reasoning_projection.rs", "claim_recompute(", "epistemic-tms"},
        "reasoning architecture gate",
        errors,
    )

    wrapper = _read("tests/test_exact_release_campaigns.py")
    _require(
        wrapper,
        {
            "certify_exact_protocol_authorization.py",
            "certify_exact_multimodal.py",
            "certify_exact_knowledge_batch.py",
            "certify_exact_reasoning_repair.py",
            "EPISTEMIC_GRAPH_TEST_BINARY",
            "EPISTEMIC_GRAPH_TEST_BINARY_SHA256",
            "EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE",
            "EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE_SHA256",
            "pytest.mark.exact_artifact",
            '"protocol_cases": 10',
            '"modalities": 4',
            '"dimensions_per_modality": 12',
            '"fault_cases": 16',
            '"families": 7',
            '"cases": 9',
        },
        "exact release pytest wrapper",
        errors,
    )
    if "cargo" in wrapper or "resolve_engine_binary" in wrapper:
        errors.append("exact release pytest wrapper discovers or builds an artifact")
    multimodal_contract_test = _read("tests/test_exact_multimodal_campaign_contract.py")
    _require(
        multimodal_contract_test,
        {
            "test_multimodal_campaign_inventory_is_current_and_complete",
            "test_multimodal_campaign_binds_only_the_supplied_artifact",
            "test_serial_exact_wrapper_requires_multimodal_summary",
            '"document", "image", "audio", "video"',
            '"dimensions_per_modality": 12',
            '"fault_cases": 16',
        },
        "multimodal static contract tests",
        errors,
    )

    docs = _read("docs/operations/exact-release-campaigns.md")
    nav = _read("mkdocs.yml")
    workflow = _read(".github/workflows/rust-ci.yml")
    _require(
        docs,
        {
            "certify_exact_protocol_authorization.py",
            "certify_exact_multimodal.py",
            "certify_exact_knowledge_batch.py",
            "certify_exact_reasoning_repair.py",
            "contains no executable or temporary path",
        },
        "release campaign operations guide",
        errors,
    )
    if "operations/exact-release-campaigns.md" not in nav:
        errors.append("MkDocs navigation omits the exact release campaign guide")
    if workflow.count("scripts/check_exact_release_campaigns.py") < 3:
        errors.append("Rust CI paths and steps do not enforce the exact campaign gate")
    if workflow.count("scripts/certify_exact_*.py") != 2:
        errors.append("Rust CI path filters omit exact campaign or shared-helper changes")

    if errors:
        print("exact release campaign architecture gate: FAIL")
        for error in errors:
            print(f"- {error}")
        return 1
    print("exact release campaign architecture gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
