#!/usr/bin/env python3
"""Static release gate for the G-04, G-14, G-15, and G-17 exact campaigns.

The campaigns only run against a supplied release binary, so nothing else
checks them statically. This gate keeps three kinds of property:

* each campaign's declared inventory (wires, data paths, modalities,
  behaviour dimensions, KnowledgeBatch families, reasoning cases) equals the
  release claim, read from literal assignments via ``ast``;
* bans: a harness may not discover, build, or skip its engine binary, and
  its evidence may not carry the executable path;
* structure: the multimodal campaign calls every served operation and runs
  the full modality x fault-phase matrix, and ``full`` ships every direct
  wire and the release features.

It deliberately does not pin source text, error strings, docs, or other
gates' implementation.
"""

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


def _load_campaign_trees(errors: list[str]) -> dict[str, ast.AST | None]:
    return {
        relative: _check_harness_baseline(relative, _read(relative), errors)
        for relative in (
            "scripts/certify_exact_protocol_authorization.py",
            "scripts/certify_exact_multimodal.py",
            "scripts/certify_exact_knowledge_batch.py",
            "scripts/certify_exact_reasoning_repair.py",
        )
    }


def _check_protocol_campaign(
    tree: ast.AST | None,
    errors: list[str],
) -> None:
    if tree is not None:
        try:
            wires = _literal_set(tree, "WIRE_PROTOCOLS")
            data_paths = _literal_set(tree, "DATA_PATHS")
            wire_features = set(_literal(tree, "WIRE_FEATURES").values())
            listener_env = set(_literal(tree, "LISTENER_ENV").values())
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


def _check_multimodal_campaign(
    tree: ast.AST | None,
    errors: list[str],
) -> None:
    if tree is None:
        return
    try:
        modalities = _literal_set(tree, "MODALITIES")
        dimensions = _literal_set(tree, "EXACT_BEHAVIOR_DIMENSIONS")
    except ValueError as error:
        errors.append(f"multimodal inventory: {error}")
    else:
        if modalities != EXPECTED_MODALITIES:
            errors.append("multimodal inventory is not the exact four")
        if dimensions != EXPECTED_EXACT_BEHAVIOR_DIMENSIONS:
            errors.append("multimodal behavior inventory is not the exact twelve")
    _require_call_suffixes(
        tree,
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
    if not _has_exact_fault_matrix(tree):
        errors.append("multimodal campaign lacks the four-by-four fault matrix")


def _check_batch_campaign(
    tree: ast.AST | None,
    errors: list[str],
) -> None:
    if tree is not None:
        try:
            families = _literal_set(tree, "FAMILIES")
            requirements = _literal_set(tree, "REQUIREMENTS")
        except ValueError as error:
            errors.append(f"KnowledgeBatch inventory: {error}")
        else:
            if families != EXPECTED_FAMILIES:
                errors.append("KnowledgeBatch family inventory is not the exact seven")
            if requirements != EXPECTED_BATCH_REQUIREMENTS:
                errors.append("KnowledgeBatch requirement inventory is incomplete")


def _check_reasoning_campaign(
    tree: ast.AST | None,
    errors: list[str],
) -> None:
    if tree is not None:
        try:
            cases = _literal_set(tree, "CASES")
        except ValueError as error:
            errors.append(f"reasoning inventory: {error}")
        else:
            if cases != EXPECTED_REASONING_CASES:
                errors.append("reasoning campaign inventory is not the exact nine")


def _check_feature_contract(errors: list[str]) -> None:
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


def _check_release_wrapper(errors: list[str]) -> None:
    wrapper = _read("tests/test_exact_release_campaigns.py")
    if "cargo" in wrapper or "resolve_engine_binary" in wrapper:
        errors.append("exact release pytest wrapper discovers or builds an artifact")


def _report(errors: list[str]) -> int:
    if errors:
        print("exact release campaign architecture gate: FAIL")
        for error in errors:
            print(f"- {error}")
        return 1
    print("exact release campaign architecture gate: PASS")
    return 0


def main() -> int:
    errors: list[str] = []
    trees = _load_campaign_trees(errors)
    protocol_relative = "scripts/certify_exact_protocol_authorization.py"
    multimodal_relative = "scripts/certify_exact_multimodal.py"
    batch_relative = "scripts/certify_exact_knowledge_batch.py"
    reasoning_relative = "scripts/certify_exact_reasoning_repair.py"
    _check_protocol_campaign(trees[protocol_relative], errors)
    _check_multimodal_campaign(trees[multimodal_relative], errors)
    _check_batch_campaign(trees[batch_relative], errors)
    _check_reasoning_campaign(trees[reasoning_relative], errors)
    _check_feature_contract(errors)
    _check_release_wrapper(errors)
    return _report(errors)


if __name__ == "__main__":
    raise SystemExit(main())
