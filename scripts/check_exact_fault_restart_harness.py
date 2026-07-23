#!/usr/bin/env python3
"""Static gate for exact-binary fault/restart certification architecture."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PHASES = {
    "before_rows",
    "after_rows_before_metadata",
    "before_commit",
    "after_commit_before_ack",
}
DOMAINS = {
    "graph_rows",
    "graph_snapshot",
    "rdf_dataset",
    "sql_catalog",
    "blob_store",
    "kv_store",
    "time_series",
    "analytics_job",
    "broker",
    "cross_modal",
    "multi_graph",
    "lifecycle",
    "control_plane",
}


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _require(source: str, tokens: set[str], label: str, errors: list[str]) -> None:
    for token in sorted(tokens):
        if token not in source:
            errors.append(f"{label}: missing {token!r}")


def _literal_string_set(tree: ast.AST, name: str) -> set[str]:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            continue
        value = node.value
        if isinstance(value, (ast.Tuple, ast.List, ast.Set)):
            result: set[str] = set()
            for item in value.elts:
                if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                    raise ValueError(f"{name} must contain only string literals")
                result.add(item.value)
            return result
    raise ValueError(f"{name} is not a literal collection")


def main() -> int:
    errors: list[str] = []
    harness = _read("scripts/certify_exact_fault_restart.py")
    try:
        tree = ast.parse(harness)
    except SyntaxError as error:
        print(f"exact fault/restart gate: harness syntax error at line {error.lineno}")
        return 1

    try:
        phases = _literal_string_set(tree, "PHASES")
        domains = _literal_string_set(tree, "DOMAINS")
    except ValueError as error:
        errors.append(f"harness inventory: {error}")
    else:
        if phases != PHASES:
            errors.append("harness phase inventory is not the current four-phase contract")
        if domains != DOMAINS:
            errors.append("harness mutation-domain inventory is incomplete")

    _require(
        harness,
        {
            '"--binary"',
            '"--binary-sha256"',
            '"--output"',
            "binary_digest_mismatch",
            "binary_must_not_be_symlink",
            "resource.RLIMIT_CORE",
            "_new_ephemeral_authority",
            "secrets.token_urlsafe",
            "evidence_contains_ephemeral_authority",
            "EPISTEMIC_GRAPH_CERTIFICATION_FAULT",
            "wait_for_abort",
            "signal.SIGABRT",
            "authoritative_restart_read",
            "exact_request_replay",
            "shutil.rmtree",
            "TemporaryDirectory",
            "tenant_qualified_time_series",
            "identical_local_series_ids",
            "spatial_restart_lazy_open",
            "PARTIAL_MATERIALIZATION",
            "EPISTEMIC_GRAPH_LAZY_OPEN_PAGE_SIZE",
            "partial_state_observed",
        },
        "harness",
        errors,
    )
    for forbidden in (
        'AUTH_SECRET = "',
        'SIGNER_KEY = "',
        "exact-certification-auth-secret",
        "exact-certification-operation-signer",
        "cargo build",
        "cargo run",
        "pytest.skip",
        "target/debug",
        "target/release",
        "resolve_engine_binary",
    ):
        if forbidden in harness:
            errors.append(f"harness: forbidden artifact fallback/build token {forbidden!r}")
    if '"binary": {"sha256": binary_digest}' not in harness:
        errors.append("harness evidence must retain only the exact binary digest")
    if re.search(r'"binary"\s*:\s*str\s*\(', harness):
        errors.append("harness evidence must not retain the local binary path")

    types = _read("crates/eg-types/src/mutation_batch.rs")
    _require(
        types,
        {
            "pub enum MutationCommitPhase",
            "BeforeRows",
            "AfterRowsBeforeMetadata",
            "BeforeCommit",
            "AfterCommitBeforeAck",
            "struct CertificationFaultSpec",
            "#[serde(deny_unknown_fields)]",
            'const ENV: &str = "EPISTEMIC_GRAPH_CERTIFICATION_FAULT"',
            "std::env::var_os(ENV)",
            "std::process::abort()",
            "spec.request_id != batch.context.request_id",
            "operation.domain == spec.domain",
        },
        "fault seam",
        errors,
    )

    graph_store = _read("src/redb_store.rs")
    sql_store = _read("crates/eg-query/src/tables/store.rs")
    native_store = _read("crates/eg-mutation-store/src/lib.rs")
    phase_variants = {
        "MutationCommitPhase::BeforeRows",
        "MutationCommitPhase::AfterRowsBeforeMetadata",
        "MutationCommitPhase::BeforeCommit",
        "MutationCommitPhase::AfterCommitBeforeAck",
    }
    _require(graph_store, phase_variants, "graph mutation store", errors)
    _require(sql_store, phase_variants, "SQL mutation store", errors)
    _require(native_store, phase_variants, "native mutation store", errors)
    _require(
        native_store,
        {
            "pub fn commit(wtx: WriteTransaction, batch: &MutationBatch)",
            "commit(wtx, batch)?;",
        },
        "native commit helper and saga",
        errors,
    )

    for relative in (
        "src/server/kv.rs",
        "src/server/blob/store.rs",
        "crates/eg-tsdb/src/store.rs",
        "crates/eg-jobs/src/store.rs",
        "crates/eg-core/src/rbac_persist.rs",
    ):
        source = _read(relative)
        if "eg_mutation_store::finish" not in source:
            errors.append(f"{relative}: no native MutationBatch finish call")
        if "eg_mutation_store::commit" not in source:
            errors.append(f"{relative}: native finish has no phase-aware commit helper")

    test = _read("tests/test_durable_crash.py")
    conftest = _read("tests/conftest.py")
    _require(
        test,
        {
            "EPISTEMIC_GRAPH_TEST_BINARY",
            "EPISTEMIC_GRAPH_TEST_BINARY_SHA256",
            "certify_exact_fault_restart.py",
            "matrix_cases\": 60",
            "pytest.mark.exact_artifact",
        },
        "pytest exact-artifact wrapper",
        errors,
    )
    for forbidden in ("cargo", "_build_redb", "_build_full"):
        if forbidden in test:
            errors.append(f"pytest exact-artifact wrapper: forbidden {forbidden!r}")
    _require(
        conftest,
        {"exact_artifact", "do not start (or implicitly Cargo-build)"},
        "shared fixture isolation",
        errors,
    )

    if errors:
        print("exact fault/restart architecture gate: FAIL")
        for error in errors:
            print(f"- {error}")
        return 1
    print("exact fault/restart architecture gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
