#!/usr/bin/env python3
"""Static gate for exact-binary fault/restart certification architecture."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from rust_lexer import _balanced_span_from, _rust_code_mask
from rust_module_tree import read_module_tree

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


def _read_rust_module(relative: str) -> str:
    """The compiler-declared source of a Rust module, not one facade file.

    The fault seam this gate pins is a property of the `mutation_batch` module,
    not of `crates/eg-types/src/mutation_batch.rs` specifically. When that
    module was decomposed into `mutation_batch.rs` + `mutation_batch/**` every
    one of the twelve seam tokens moved into `mutation_batch/fault.rs` and
    `mutation_batch/model/request.rs`, and this gate reported the entire
    certification fault seam as deleted. Reading the module the way the compiler
    assembles it (test-inclusive, the exact superset of the single file that was
    read before) makes a decomposition invisible to the gate and a real deletion
    still fatal.
    """

    return read_module_tree(relative, root_dir=ROOT, include_tests=True)


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


def _rust_function(source: str, name: str) -> str:
    """Return one Rust function body with comments and literals out of scope."""

    mask = _rust_code_mask(source)
    match = re.search(rf"\bfn\s+{re.escape(name)}\s*\(", mask)
    if match is None:
        return ""
    opening = mask.find("{", match.end())
    if opening < 0:
        return ""
    closing = _balanced_span_from(mask, opening, "{", "}")
    return source[match.start() : closing + 1]


def _literal_string_set(tree: ast.AST, name: str) -> set[str]:
    try:
        value = next(
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(
                map(
                    lambda target: isinstance(target, ast.Name) and target.id == name,
                    node.targets,
                )
            )
            and isinstance(node.value, (ast.Tuple, ast.List, ast.Set))
        )
    except StopIteration as error:
        raise ValueError(f"{name} is not a literal collection") from error
    items = value.elts
    if not all(
        map(
            lambda item: isinstance(item, ast.Constant) and isinstance(item.value, str),
            items,
        )
    ):
        raise ValueError(f"{name} must contain only string literals")
    return {item.value for item in items}


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
    types = _read_rust_module("crates/eg-types/src/mutation_batch.rs")
    types_code = _rust_code_mask(types)
    _require(
        types_code,
        {
            "pub enum MutationCommitPhase",
            "BeforeRows",
            "AfterRowsBeforeMetadata",
            "BeforeCommit",
            "AfterCommitBeforeAck",
            "struct CertificationFaultSpec",
            "#[serde(deny_unknown_fields)]",
        },
        "fault seam",
        errors,
    )

    apply_fault = _rust_function(types, "apply_certification_fault")
    request_match = _rust_function(types, "batch_matches_request")
    if not apply_fault:
        errors.append(
            "fault seam apply path: missing function 'apply_certification_fault'"
        )
    else:
        apply_fault_code = _rust_code_mask(apply_fault)
        _require(
            apply_fault_code,
            {
                "const ENV: &str =",
                "std::env::var_os(ENV)",
                "std::process::abort()",
                "batch_matches_request(batch, spec.request_id)",
                "operation.domain == spec.domain",
            },
            "fault seam apply path",
            errors,
        )
        _require(
            apply_fault,
            {'const ENV: &str = "EPISTEMIC_GRAPH_CERTIFICATION_FAULT"'},
            "fault seam apply path",
            errors,
        )
    if not request_match:
        errors.append(
            "fault seam request identity helper: missing function "
            "'batch_matches_request'"
        )
    else:
        request_match_code = _rust_code_mask(request_match)
        _require(
            request_match_code,
            {
                "batch.envelope.operation()",
                "envelope.authority.request_id.as_str()",
                "crate::mutation_batch::request_opaque_id(request_id)",
            },
            "fault seam request identity helper",
            errors,
        )
    return errors


# crates/eg-mutation-store is deleted (split into eg-storage's
# StorageKernel and eg-transaction's MutationKernel). The
# MutationCommitPhase call sites and the commit() helper definition this
# check pins now live in eg-transaction/src/commit.rs; the saga call site
# that invokes it (`commit(write, batch)?;`) lives in the peer
# eg-transaction/src/saga.rs. Found by tracing where `MutationCommitPhase::`
# is actually referenced today, since the old
# `crates/eg-mutation-store/src/lib.rs` this gate read never held that
# content even before the deletion (it moved there only when the crate first
# split into `store/*.rs` submodules -- this specific check appears to have
# gone stale then and stayed stale until now; repointing it here restores
# real enforcement, not just a path fix).
_NATIVE_STORE_COMMIT_SOURCES = (
    "crates/eg-transaction/src/commit.rs",
    "crates/eg-transaction/src/saga.rs",
)

# `eg_mutation_store::finish`/`::commit` were free functions. Their
# successors, `MutationKernel::{finish, commit}`, are ONLY reachable as
# methods (eg-transaction/src/lib.rs re-exports no free finish/commit), so
# every migrated consumer now calls them as `<kernel-field>.finish(&write, ` /
# `<kernel-field>.commit(write` (observed across kv.rs, eg-tsdb, eg-jobs, and
# eg-core's rbac_persist/durable_write.rs -- some inline, some split onto
# their own line before `.finish`/`.commit`, so the check tolerates
# whitespace between the receiver and the call).
_MUTATION_KERNEL_FINISH_CALL = re.compile(r"\.mutations\s*\.finish\(&write")
_MUTATION_KERNEL_COMMIT_CALL = re.compile(r"\.mutations\s*\.commit\(write")


def _check_store_contract() -> list[str]:
    errors: list[str] = []
    # `redb_store.rs` is a compiler facade: the mutation-phase calls now live
    # in its declared `store_batch`/`store_mutation` children. Read the exact
    # source family the compiler assembles so a child omission cannot make the
    # graph mutation contract look satisfied by an unrelated facade comment.
    graph_store = _read_rust_module("src/redb_store.rs")
    sql_store = _read("crates/eg-query/src/tables/store.rs")
    native_store = "\n".join(_read(path) for path in _NATIVE_STORE_COMMIT_SOURCES)
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
            # `pub fn commit(wtx: WriteTransaction, batch: &MutationBatch)` ->
            # `pub(crate) fn commit<D: OwnerDomain>(write: AdmittedMutation<'_,
            # D>, batch: &MutationBatch)`: genuinely re-shaped (typed capability
            # + generic domain, no longer a bare redb WriteTransaction, and
            # crate-private rather than pub), not merely renamed -- pinned to
            # the real current signature.
            "pub(crate) fn commit<D: OwnerDomain>(\n"
            "    write: AdmittedMutation<'_, D>,\n"
            "    batch: &MutationBatch,\n"
            ") -> Result<(), String> {",
            # saga.rs's call site kept the same shape, only the parameter's
            # name changed (wtx -> write).
            "commit(write, batch)?;",
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
        if relative == "crates/eg-core/src/rbac_persist.rs":
            # The actual write path lives in this declared submodule, not the
            # rbac_persist.rs facade itself.
            source += "\n" + _read("crates/eg-core/src/rbac_persist/durable_write.rs")
        if "MutationKernel" not in source:
            errors.append(f"{relative}: does not hold a native MutationKernel")
        if not _MUTATION_KERNEL_FINISH_CALL.search(source):
            errors.append(f"{relative}: no native MutationBatch finish call")
        if not _MUTATION_KERNEL_COMMIT_CALL.search(source):
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
