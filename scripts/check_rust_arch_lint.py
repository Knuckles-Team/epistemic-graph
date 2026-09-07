#!/usr/bin/env python3
"""Run arch-lint with a truthful source and finding contract.

arch-lint 0.5.0's AL002 rule is a syntactic candidate finder: it does not
establish that a call is running on an async executor, and it cannot resolve
method receivers or ``std::fs`` aliases.  This wrapper retains its report
verbatim, adds a conservative context classification, and fails only for
qualified synchronous filesystem calls in lexical async context. Unknown receivers are
reported as unresolved evidence; they are never promoted to proven defects.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, namedtuple
from pathlib import Path
from typing import Any

import tomllib
from rust_context import (
    Context,
    GateError,
    Span,
    _brace_pairs,
    _context_from_spans,
    _fail,
    _lexical_scope,
    _spans,
)
from rust_lexer import _rust_code_mask
from scanner_contract import load_contract, resolve_binary, run_git, sanitized_env

ROOT = Path(__file__).resolve().parent.parent
POLICY = ROOT / "arch-lint.toml"
SCHEMA = "eg-arch-lint-gate/v1"

RULES = {
    "no-unwrap-expect": ("AL001", False, "error"),
    "no-sync-io": ("AL002", True, "error"),
    "no-error-swallowing": ("AL003", True, "warning"),
    "handler-complexity": ("AL004", True, "warning"),
    "require-thiserror": ("AL005", True, "warning"),
    "require-tracing": ("AL006", True, "warning"),
    "tracing-env-init": ("AL007", True, "warning"),
    "no-silent-result-drop": ("AL013", True, "warning"),
}
ENABLED_CODES = tuple(code for code, enabled, _severity in RULES.values() if enabled)
ENABLED_POLICY = {
    code: (name, severity)
    for name, (code, enabled, severity) in RULES.items()
    if enabled
}

FS_OPERATIONS = (
    "canonicalize",
    "copy",
    "create_dir",
    "create_dir_all",
    "hard_link",
    "metadata",
    "read",
    "read_dir",
    "read_link",
    "read_to_string",
    "remove_dir",
    "remove_dir_all",
    "remove_file",
    "rename",
    "set_permissions",
    "soft_link",
    "symlink_metadata",
    "write",
)
FILE_OPERATIONS = ("create", "open")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _validate_analyzer(analyzer: Any) -> None:
    """Reject any analyzer scope other than the reviewed repository-root scan."""

    if not isinstance(analyzer, dict) or set(analyzer) != {
        "root",
        "exclude",
        "respect_gitignore",
    }:
        _fail("arch-lint analyzer keys are not the reviewed closed set")
    if analyzer["root"] != "." or analyzer["respect_gitignore"] is not True:
        _fail("arch-lint must scan repository root and respect .gitignore")
    if analyzer["exclude"] != ["**/target/**", "**/target-*/**"]:
        _fail("arch-lint may exclude only isolated Cargo build products")


def _validate_rules(rules: Any) -> None:
    """Reject implicit rule selection: every supported rule states its own state."""

    if not isinstance(rules, dict) or set(rules) != set(RULES):
        _fail("arch-lint rules must explicitly name the complete supported set")
    for name, (_code, enabled, severity) in RULES.items():
        if rules[name] != {"enabled": enabled, "severity": severity}:
            _fail(
                f"arch-lint rule {name!r} must set "
                f"enabled={str(enabled).lower()}, severity={severity}"
            )


def load_policy(path: Path = POLICY) -> dict[str, Any]:
    """Load the closed arch-lint policy and reject implicit rule selection."""

    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise GateError(f"cannot read policy {path}: {exc}") from exc
    if set(document) != {"fail_on", "analyzer", "rules"}:
        _fail("arch-lint policy must contain only fail_on, analyzer, and rules")
    if document["fail_on"] != "error":
        _fail("arch-lint fail_on must remain 'error'")
    _validate_analyzer(document.get("analyzer"))
    _validate_rules(document.get("rules"))
    return document


def _git_paths(repo: Path, *args: str) -> list[str]:
    result = run_git([*args, "-z", "--", "*.rs"], cwd=repo, preserve_index=True)
    if result.returncode != 0:
        _fail(f"git {' '.join(args)} failed: {result.stderr.strip()[:300]}")
    return sorted(item for item in result.stdout.split("\0") if item)


def source_universe(repo: Path = ROOT) -> tuple[dict[str, str], dict[str, Any]]:
    """Return the exact tracked/staged Rust universe and its content identity."""

    tracked = _git_paths(repo, "ls-files", "--cached")
    untracked = _git_paths(repo, "ls-files", "--others", "--exclude-standard")
    if untracked:
        _fail(
            "untracked Rust source is outside the reviewed universe: "
            + ", ".join(untracked)
        )
    sources: dict[str, str] = {}
    digest = hashlib.sha256()
    for relative in tracked:
        path = repo / relative
        if not path.is_file():  # staged deletions do not belong to the scanner universe
            continue
        try:
            raw = path.read_bytes()
            text = raw.decode("utf-8")
        except (OSError, UnicodeError) as exc:
            raise GateError(f"cannot read Rust source {relative}: {exc}") from exc
        file_digest = _sha256(raw)
        digest.update(
            relative.encode("utf-8") + b"\0" + file_digest.encode("ascii") + b"\n"
        )
        sources[relative] = text
    head = run_git(["rev-parse", "HEAD"], cwd=repo, preserve_index=True)
    if head.returncode != 0:
        _fail(f"cannot resolve repository HEAD: {head.stderr.strip()[:300]}")
    return sources, {
        "kind": "git-enumerated-working-tree-rust",
        "repository_head": head.stdout.strip(),
        "files": len(sources),
        "sha256": digest.hexdigest(),
    }


def _offset(source: str, line: int, column: int) -> int:
    if line < 1 or column < 1:
        _fail(f"invalid source location {line}:{column}")
    lines = source.splitlines(keepends=True)
    if line > len(lines):
        _fail(f"source location line {line} exceeds {len(lines)}")
    value = sum(len(item) for item in lines[: line - 1]) + column - 1
    if value > sum(len(item) for item in lines[:line]):
        _fail(f"source location column {column} exceeds line {line}")
    return value


def _path_is_test(path: str) -> bool:
    parts = Path(path).parts
    return (
        bool(parts and parts[0] in {"tests", "benches", "examples"})
        or "tests" in parts
        or Path(path).name == "tests.rs"
    )


_DIRECT_DISPOSITIONS = {
    "test": ("test_context", "test, bench, example, or cfg(test) context"),
    "spawn_blocking": (
        "spawn_blocking_context",
        "blocking call is already off the async executor",
    ),
    "async": (
        "async_direct",
        "qualified synchronous filesystem call in async context",
    ),
    "sync": (
        "sync_context",
        "synchronous startup, offline, or ordinary function context",
    ),
}
_ALIAS_DISPOSITIONS = {
    "test": ("test_context", "aliased call is in test context"),
    "spawn_blocking": ("spawn_blocking_context", "aliased call is off the executor"),
    "async": (
        "async_alias",
        "aliased synchronous filesystem call in async context",
    ),
    "sync": ("sync_context", "aliased call is in synchronous context"),
}


def _context_kind(path: str, context: Context) -> str:
    """Name the execution context a filesystem call site sits in."""

    if _path_is_test(path) or context.test:
        return "test"
    if context.spawn_blocking:
        return "spawn_blocking"
    if context.async_context:
        return "async"
    return "sync"


def _disposition(
    path: str,
    context: Context,
    labels: dict[str, tuple[str, str]],
    *,
    receiver_unresolved: bool = False,
) -> tuple[str, str, bool]:
    """Classify one call site; only a proven async context is blocking."""

    if receiver_unresolved:
        return (
            "receiver_unresolved",
            "method receiver type is not proven by arch-lint 0.5.0",
            False,
        )
    kind = _context_kind(path, context)
    classification, reason = labels[kind]
    return classification, reason, kind == "async"


def _group_items(
    masked: str, start: int, end: int, brace_pairs: dict[int, int]
) -> list[tuple[int, int]]:
    """Split a Rust import group without splitting nested groups."""

    items: list[tuple[int, int]] = []
    item_start = start
    position = start
    while position < end:
        if masked[position] == "{":
            closing = brace_pairs.get(position)
            if closing is None or closing > end:
                _fail("unsupported or unbalanced grouped Rust import")
            position = closing + 1
            continue
        if masked[position] == ",":
            items.append((item_start, position))
            item_start = position + 1
        position += 1
    items.append((item_start, end))
    return items


_FsImport = namedtuple("_FsImport", ("kind", "alias", "operation", "start", "end"))
_GROUP_ITEM = re.compile(r"\s*(\w+)\s*(?:as\s+(\w+))?\s*")
_STD_FS_MODULE = re.compile(r"\buse\s+(?:::)?std::fs\s*(?:as\s+(\w+))?\s*;")
_STD_GROUP = re.compile(r"\buse\s+(?:::)?std::\s*\{")
_STD_FS_ITEM = re.compile(r"\buse\s+(?:::)?std::fs::(\w+)\s*(?:as\s+(\w+))?\s*;")
_STD_FS_GROUP = re.compile(r"\buse\s+(?:::)?std::fs::\{([^}]*)\}\s*;")
_STD_FS_GLOB = re.compile(r"\buse\s+(?:::)?std::fs::\*\s*;")
_GROUPED_FS_MODULE = re.compile(r"\s*fs\s*(?:as\s+(\w+))?\s*")
_GROUPED_FS_ITEM = re.compile(r"\s*fs\s*::\s*(\w+)\s*(?:as\s+(\w+))?\s*")
_GROUPED_FS_NESTED = re.compile(r"\s*fs\s*::\s*\{")
_GROUPED_FS_ANY = re.compile(r"\s*fs\s*::")


def _imported_operation(operation: str, alias: str | None) -> tuple[str, str, str]:
    """Name what a single ``std::fs`` import item makes callable."""

    if operation in FS_OPERATIONS:
        return "function", alias or operation, operation
    if operation == "File":
        return "file_type", alias or operation, operation
    return "", "", ""


def _scoped_import(
    kind: str,
    alias: str,
    operation: str,
    position: int,
    brace_pairs: dict[int, int],
    length: int,
) -> _FsImport:
    scope_start, scope_end = _lexical_scope(brace_pairs, position, length)
    return _FsImport(kind, alias, operation, scope_start, scope_end)


def _fs_group_imports(
    masked: str, brace_pairs: dict[int, int], start: int, end: int, position: int
) -> list[_FsImport]:
    """Read the items of a ``std::fs::{...}`` group into scoped imports."""

    imports: list[_FsImport] = []
    for item_start, item_end in _group_items(masked, start, end, brace_pairs):
        item = masked[item_start:item_end]
        pieces = _GROUP_ITEM.fullmatch(item)
        if pieces is None:
            if item.strip() == "*" or "{" in item:
                _fail("unsupported nested std::fs import cannot be proven safe")
            continue
        name, alias = pieces.groups()
        if name == "self":
            kind, bound, operation = "module", alias or "fs", ""
        else:
            kind, bound, operation = _imported_operation(name, alias)
        if kind:
            imports.append(
                _scoped_import(
                    kind, bound, operation, position, brace_pairs, len(masked)
                )
            )
    return imports


def _std_group_fs_imports(masked: str, brace_pairs: dict[int, int]) -> list[_FsImport]:
    """Read ``use std::{... fs ...};`` groups into scoped imports."""

    imports: list[_FsImport] = []
    for match in _STD_GROUP.finditer(masked):
        outer_open = match.end() - 1
        outer_close = brace_pairs.get(outer_open)
        if outer_close is None:
            _fail("unsupported or unbalanced std grouped import")
        semicolon = outer_close + 1
        while semicolon < len(masked) and masked[semicolon].isspace():
            semicolon += 1
        if semicolon >= len(masked) or masked[semicolon] != ";":
            _fail("unsupported std grouped import without a terminating semicolon")
        for item_start, item_end in _group_items(
            masked, outer_open + 1, outer_close, brace_pairs
        ):
            imports.extend(
                _grouped_fs_item(
                    masked, brace_pairs, item_start, item_end, semicolon + 1
                )
            )
    return imports


def _grouped_fs_item(
    masked: str,
    brace_pairs: dict[int, int],
    item_start: int,
    item_end: int,
    position: int,
) -> list[_FsImport]:
    """Read one item of a ``use std::{...}`` group that names ``fs``."""

    item = masked[item_start:item_end]
    module = _GROUPED_FS_MODULE.fullmatch(item)
    if module:
        return [
            _scoped_import(
                "module",
                module.group(1) or "fs",
                "",
                position,
                brace_pairs,
                len(masked),
            )
        ]
    flat = _GROUPED_FS_ITEM.fullmatch(item)
    if flat:
        operation, alias = flat.groups()
        kind, bound, resolved = _imported_operation(operation, alias)
        if not kind:
            return []
        return [
            _scoped_import(kind, bound, resolved, position, brace_pairs, len(masked))
        ]
    nested = _GROUPED_FS_NESTED.match(item)
    if not nested:
        if _GROUPED_FS_ANY.match(item):
            _fail("unsupported std::fs import cannot be proven safe")
        return []
    inner_open = item_start + nested.end() - 1
    inner_close = brace_pairs.get(inner_open)
    if inner_close is None or masked[inner_close + 1 : item_end].strip():
        _fail("unsupported nested std::fs grouped import")
    return _fs_group_imports(masked, brace_pairs, inner_open + 1, inner_close, position)


def _fs_imports(masked: str, brace_pairs: dict[int, int]) -> list[_FsImport]:
    """Collect every ``std::fs`` binding AL002 cannot resolve, with its scope."""

    if _STD_FS_GLOB.search(masked):
        _fail("unsupported std::fs glob import cannot be proven safe")
    imports = [
        _scoped_import(
            "module", match.group(1) or "fs", "", match.end(), brace_pairs, len(masked)
        )
        for match in _STD_FS_MODULE.finditer(masked)
    ]
    imports.extend(_std_group_fs_imports(masked, brace_pairs))
    for match in _STD_FS_ITEM.finditer(masked):
        kind, bound, operation = _imported_operation(*match.groups())
        if kind:
            imports.append(
                _scoped_import(
                    kind, bound, operation, match.end(), brace_pairs, len(masked)
                )
            )
    for match in _STD_FS_GROUP.finditer(masked):
        imports.extend(
            _fs_group_imports(
                masked, brace_pairs, match.start(1), match.end(1), match.end()
            )
        )
    return imports


def _alias_call_sites(
    masked: str, imports: list[_FsImport]
) -> set[tuple[int, str, str]]:
    """Find the calls each aliased ``std::fs`` binding makes reachable."""

    operations = "|".join(FS_OPERATIONS)
    files = "|".join(FILE_OPERATIONS)
    patterns = {
        "module": rf"\b{{alias}}::((?:{operations})|File::(?:{files}))\s*\(",
        "function": r"(?<![:\w]){alias}\s*\(",
        "file_type": rf"\b{{alias}}::({files})\s*\(",
    }
    sites: set[tuple[int, str, str]] = set()
    for entry in imports:
        fragment = masked[entry.start : entry.end]
        pattern = patterns[entry.kind].format(alias=re.escape(entry.alias))
        for match in re.finditer(pattern, fragment):
            if entry.kind == "function":
                operation = entry.operation
            elif entry.kind == "file_type":
                operation = f"File::{match.group(1)}"
            else:
                operation = match.group(1)
            sites.add((entry.start + match.start(), operation, match.group(0)))
    return sites


def _line_column(line_starts: list[int], offset: int) -> tuple[int, int]:
    line = 1 + sum(start <= offset for start in line_starts[1:])
    return line, offset - line_starts[line - 1] + 1


def _alias_calls(
    path: str,
    source: str,
    *,
    masked: str | None = None,
    spans: list[Span] | None = None,
) -> list[dict[str, Any]]:
    """Find std::fs calls hidden from AL002 by module/function aliases."""

    masked = _rust_code_mask(source) if masked is None else masked
    spans = _spans(masked) if spans is None else spans
    brace_pairs = _brace_pairs(masked)
    sites = _alias_call_sites(masked, _fs_imports(masked, brace_pairs))
    line_starts = [0]
    line_starts.extend(match.end() for match in re.finditer("\n", source))
    found: list[dict[str, Any]] = []
    for offset, operation, expression in sorted(sites):
        classification, reason, blocking = _disposition(
            path, _context_from_spans(spans, offset), _ALIAS_DISPOSITIONS
        )
        line, column = _line_column(line_starts, offset)
        found.append(
            {
                "code": "EG-AL002-ALIAS",
                "operation": f"std::fs::{operation}",
                "expression": expression.strip(),
                "location": {"file": path, "line": line, "column": column},
                "classification": classification,
                "reason": reason,
                "blocking": blocking,
            }
        )
    return found


def _has_fs_import(source: str) -> bool:
    return bool(re.search(r"\buse\s+(?:::)?std::(?:fs\b|\{[^;]*\bfs\b)", source))


def _validated_violation_path(finding: Any, index: int, sources: dict[str, str]) -> str:
    """Prove one raw violation obeys the enabled policy; name its source path."""

    if not isinstance(finding, dict):
        _fail(f"violation {index} is not an object")
    location = finding.get("location")
    if not isinstance(location, dict) or not isinstance(location.get("file"), str):
        _fail(f"violation {index} has no valid location")
    if finding.get("code") not in ENABLED_CODES:
        _fail(f"violation {index} uses a rule outside the enabled policy")
    expected_rule, expected_severity = ENABLED_POLICY[finding["code"]]
    if finding.get("rule") != expected_rule:
        _fail(
            f"violation {index} rule identity does not match "
            f"{finding['code']}={expected_rule}"
        )
    if finding.get("severity") != expected_severity:
        _fail(
            f"violation {index} severity does not match "
            f"{finding['code']}={expected_severity}"
        )
    path = location["file"].removeprefix("./")
    if path not in sources:
        _fail(f"violation {index} references source outside the universe: {path}")
    return path


def _validated_al002_paths(report: dict[str, Any], sources: dict[str, str]) -> set[str]:
    """Prove the raw report obeys the enabled policy; name its AL002 sources."""

    if not isinstance(report, dict) or set(report) != {"violations", "files_checked"}:
        _fail("arch-lint JSON must contain exactly violations and files_checked")
    violations = report["violations"]
    if not isinstance(violations, list) or report["files_checked"] != len(sources):
        _fail(
            f"arch-lint files_checked={report.get('files_checked')!r} does not match "
            f"source universe={len(sources)}"
        )
    paths = [
        _validated_violation_path(finding, index, sources)
        for index, finding in enumerate(violations)
    ]
    return {
        path
        for path, finding in zip(paths, violations, strict=True)
        if finding.get("code") == "AL002"
    }


def _al002_disposition(
    finding: dict[str, Any], index: int, sources: dict[str, str], spans: list[Span]
) -> dict[str, Any]:
    """Classify one raw AL002 violation against its proven lexical context."""

    location = finding["location"]
    path = location["file"].removeprefix("./")
    receiver_unresolved = bool(
        re.search(r"`\.[A-Za-z_]\w*\(\)`", str(finding.get("message", "")))
    )
    offset = _offset(sources[path], location.get("line"), location.get("column"))
    classification, reason, blocking = _disposition(
        path,
        _context_from_spans(spans, offset),
        _DIRECT_DISPOSITIONS,
        receiver_unresolved=receiver_unresolved,
    )
    return {
        "raw_violation_index": index,
        "classification": classification,
        "reason": reason,
        "blocking": blocking,
    }


def _masked_analyses(
    sources: dict[str, str], paths: set[str]
) -> dict[str, tuple[str, list[Span]]]:
    """Mask and span each source once, so no path is analysed twice."""

    return {
        path: (masked, _spans(masked))
        for path in sorted(paths)
        for masked in [_rust_code_mask(sources[path])]
    }


def _alias_findings(
    sources: dict[str, str],
    alias_paths: set[str],
    analyses: dict[str, tuple[str, list[Span]]],
) -> list[dict[str, Any]]:
    """Collect the aliased std::fs calls AL002 cannot see, in path order."""

    return [
        item
        for path in sorted(alias_paths)
        for item in _alias_calls(
            path, sources[path], masked=analyses[path][0], spans=analyses[path][1]
        )
    ]


def _blocking_violations(
    violations: list[Any],
    dispositions: list[dict[str, Any]],
    aliases: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collect every finding this gate treats as blocking, in report order."""

    blockers = [item for item in dispositions if item["blocking"]]
    blockers.extend(
        {
            "raw_violation_index": index,
            "classification": "raw_error",
            "reason": "enabled non-AL002 rule reported error severity",
            "blocking": True,
        }
        for index, finding in enumerate(violations)
        if finding.get("code") != "AL002" and finding.get("severity") == "error"
    )
    blockers.extend(item for item in aliases if item["blocking"])
    return blockers


def classify_report(report: dict[str, Any], sources: dict[str, str]) -> dict[str, Any]:
    """Validate a raw report and add conservative, non-mutating dispositions."""

    al002_paths = _validated_al002_paths(report, sources)
    violations = report["violations"]
    alias_paths = {path for path, source in sources.items() if _has_fs_import(source)}
    analyses = _masked_analyses(sources, al002_paths | alias_paths)
    dispositions = [
        _al002_disposition(
            finding,
            index,
            sources,
            analyses[finding["location"]["file"].removeprefix("./")][1],
        )
        for index, finding in enumerate(violations)
        if finding.get("code") == "AL002"
    ]
    aliases = _alias_findings(sources, alias_paths, analyses)
    blockers = _blocking_violations(violations, dispositions, aliases)
    return {
        "raw_report": report,
        "raw_summary": {
            "files_checked": report["files_checked"],
            "violations": len(violations),
            "by_code": dict(
                sorted(Counter(str(item.get("code")) for item in violations).items())
            ),
        },
        "al002_dispositions": dispositions,
        "al002_disposition_summary": dict(
            sorted(Counter(item["classification"] for item in dispositions).items())
        ),
        "alias_findings": aliases,
        "blocking_violations": blockers,
    }


def parse_scanner_json(stdout: str) -> dict[str, Any]:
    """Parse the JSON payload after arch-lint's stdout tracing preamble."""

    marker = stdout.find('{\n  "violations"')
    if marker < 0:
        _fail("arch-lint stdout contains no JSON report")
    try:
        report = json.loads(stdout[marker:])
    except json.JSONDecodeError as exc:
        raise GateError(f"arch-lint returned malformed JSON: {exc}") from exc
    if not isinstance(report, dict):
        _fail("arch-lint JSON report is not an object")
    return report


def _tool_identity(executable: str, expected: str, repo: Path) -> dict[str, str]:
    try:
        version = subprocess.run(
            [executable, "--version"],
            cwd=repo,
            env=sanitized_env(),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        raw = Path(executable).read_bytes()
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise GateError(f"cannot identify arch-lint: {exc}") from exc
    got = version.stdout.strip()
    if version.returncode != 0 or got not in {expected, f"arch-lint {expected}"}:
        _fail(f"arch-lint version drift: expected {expected!r}, got {got!r}")
    return {
        "name": "arch-lint",
        "version": expected,
        "binary": executable,
        "sha256": _sha256(raw),
    }


def run_gate(repo: Path = ROOT, policy_path: Path = POLICY) -> dict[str, Any]:
    load_policy(policy_path)
    policy_digest = _sha256(policy_path.read_bytes())
    contract = load_contract(repo / "pyproject.toml")
    executable = resolve_binary("arch-lint", "ARCH_LINT_BIN")
    identity = _tool_identity(executable, contract.arch_lint_version, repo)
    sources, universe = source_universe(repo)
    command = [
        executable,
        "check",
        "--format",
        "json",
        "--config",
        str(policy_path),
        "--rules",
        ",".join(ENABLED_CODES),
        ".",
    ]
    try:
        result = subprocess.run(
            command,
            cwd=repo,
            env=sanitized_env(),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise GateError(f"could not run arch-lint: {exc}") from exc
    if result.returncode not in {0, 1}:
        _fail(f"arch-lint exited {result.returncode}: {result.stderr.strip()[:500]}")
    raw_report = parse_scanner_json(result.stdout)
    classified = classify_report(raw_report, sources)
    _post_sources, post_universe = source_universe(repo)
    if post_universe != universe:
        _fail("Rust source universe changed while arch-lint was running")
    if _sha256(policy_path.read_bytes()) != policy_digest:
        _fail("arch-lint policy changed while the scanner was running")
    post_identity = _tool_identity(executable, contract.arch_lint_version, repo)
    if post_identity != identity:
        _fail("arch-lint tool identity changed while the scanner was running")
    return {
        "schema": SCHEMA,
        "tool": identity,
        "policy": {
            "path": policy_path.relative_to(repo).as_posix(),
            "sha256": policy_digest,
            "enabled_rules": list(ENABLED_CODES),
            "fail_on": "error",
        },
        "source_universe": universe,
        **classified,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, help="optional receipt path outside the repository"
    )
    args = parser.parse_args(argv)
    try:
        receipt = run_gate()
        rendered = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
        if args.output:
            destination = args.output.resolve()
            try:
                destination.relative_to(ROOT.resolve())
            except ValueError:
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(rendered, encoding="utf-8")
            else:
                _fail("receipt output must be outside the repository")
        else:
            sys.stdout.write(rendered)
        return 1 if receipt["blocking_violations"] else 0
    except GateError as exc:
        print(f"rust-arch-lint: cannot run: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
