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


def _without_module_docstring(source: str) -> str:
    """`source` with its leading module docstring (if any) blanked out.

    A "forbidden token" substring scan over a whole file also matches prose in
    the module docstring EXPLAINING why the code deliberately avoids that token
    -- exactly the shape `tests/test_durable_crash.py`'s docstring takes,
    documenting why the wrapper does NOT invoke `cargo` directly by using the
    word "cargo" to say so. Only the leading docstring is blanked (replaced with
    equal-length whitespace, so reported positions/line numbers of anything
    else stay accurate); ordinary string literals used as real call arguments
    elsewhere in the file (e.g. a forbidden subprocess argv element) are left
    intact, so an actual violation is still caught.
    """
    tree = ast.parse(source)
    if not (
        tree.body
        and isinstance(tree.body[0], ast.Expr)
        and isinstance(tree.body[0].value, ast.Constant)
        and isinstance(tree.body[0].value.value, str)
    ):
        return source
    doc = tree.body[0]
    lines = source.splitlines(keepends=True)
    start_line, start_col = doc.lineno, doc.col_offset
    end_line, end_col = doc.end_lineno, doc.end_col_offset
    if start_line == end_line:
        line = lines[start_line - 1]
        lines[start_line - 1] = (
            line[:start_col] + " " * (end_col - start_col) + line[end_col:]
        )
    else:
        first = lines[start_line - 1]
        lines[start_line - 1] = first[:start_col] + " " * (len(first) - start_col)
        for ln in range(start_line, end_line - 1):
            body_len = len(lines[ln].rstrip("\n"))
            newline = "\n" if lines[ln].endswith("\n") else ""
            lines[ln] = " " * body_len + newline
        last = lines[end_line - 1]
        lines[end_line - 1] = " " * end_col + last[end_col:]
    return "".join(lines)


def _require(source: str, tokens: set[str], label: str, errors: list[str]) -> None:
    for token in sorted(tokens):
        if token not in source:
            errors.append(f"{label}: missing {token!r}")


def _literal_string_set(tree: ast.AST, name: str) -> set[str]:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            continue
        value = node.value
        if isinstance(value, (ast.Tuple, ast.List, ast.Set)):
            result: set[str] = set()
            for item in value.elts:
                if not isinstance(item, ast.Constant) or not isinstance(
                    item.value, str
                ):
                    raise ValueError(f"{name} must contain only string literals")
                result.add(item.value)
            return result
    raise ValueError(f"{name} is not a literal collection")


def _read_harness() -> tuple[str, ast.AST | None]:
    harness = _read("scripts/certify_exact_fault_restart.py")
    try:
        tree = ast.parse(harness)
    except SyntaxError as error:
        print(f"exact fault/restart gate: harness syntax error at line {error.lineno}")
        return harness, None
    return harness, tree


def _check_harness_inventory(tree: ast.AST) -> list[str]:
    errors: list[str] = []

    try:
        phases = _literal_string_set(tree, "PHASES")
        domains = _literal_string_set(tree, "DOMAINS")
    except ValueError as error:
        errors.append(f"harness inventory: {error}")
    else:
        if phases != PHASES:
            errors.append(
                "harness phase inventory is not the current four-phase contract"
            )
        if domains != DOMAINS:
            errors.append("harness mutation-domain inventory is incomplete")
    return errors


def _check_harness_contract(harness: str) -> list[str]:
    errors: list[str] = []

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
            errors.append(
                f"harness: forbidden artifact fallback/build token {forbidden!r}"
            )
    if '"binary": {"sha256": binary_digest}' not in harness:
        errors.append("harness evidence must retain only the exact binary digest")
    if re.search(r'"binary"\s*:\s*str\s*\(', harness):
        errors.append("harness evidence must not retain the local binary path")
    return errors


def _check_fault_seam_contract() -> list[str]:
    errors: list[str] = []
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
    return errors


def _check_store_contract() -> list[str]:
    errors: list[str] = []
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
    return errors


def _check_exact_artifact_contract() -> list[str]:
    errors: list[str] = []
    test = _read("tests/test_durable_crash.py")
    conftest = _read("tests/conftest.py")
    _require(
        test,
        {
            "EPISTEMIC_GRAPH_TEST_BINARY",
            "EPISTEMIC_GRAPH_TEST_BINARY_SHA256",
            "certify_exact_fault_restart.py",
            'matrix_cases": 60',
            "pytest.mark.exact_artifact",
        },
        "pytest exact-artifact wrapper",
        errors,
    )
    test_code = _without_module_docstring(test)
    for forbidden in ("cargo", "_build_redb", "_build_full"):
        if forbidden in test_code:
            errors.append(f"pytest exact-artifact wrapper: forbidden {forbidden!r}")
    _require(
        conftest,
        # Wording of the rationale comment drifted from "do not start" to "never
        # start" (both mean the same thing: skip starting, or implicitly
        # Cargo-building, the shared engine for exact_artifact/no_engine-only
        # runs) without the underlying fixture behavior changing at all.
        {"exact_artifact", "start (or implicitly Cargo-build)"},
        "shared fixture isolation",
        errors,
    )
    return errors


def _report(errors: list[str]) -> int:

    if errors:
        print("exact fault/restart architecture gate: FAIL")
        for error in errors:
            print(f"- {error}")
        return 1
    print("exact fault/restart architecture gate: PASS")
    return 0


def main() -> int:
    harness, tree = _read_harness()
    if tree is None:
        return 1
    errors = _check_harness_inventory(tree)
    errors.extend(_check_harness_contract(harness))
    errors.extend(_check_fault_seam_contract())
    errors.extend(_check_store_contract())
    errors.extend(_check_exact_artifact_contract())
    return _report(errors)


if __name__ == "__main__":
    raise SystemExit(main())
