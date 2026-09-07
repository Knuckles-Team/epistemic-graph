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
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn

import tomllib
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


class GateError(RuntimeError):
    """The policy, environment, scanner report, or source universe is invalid."""


def _fail(message: str) -> NoReturn:
    raise GateError(message)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
    analyzer = document.get("analyzer")
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
    rules = document.get("rules")
    if not isinstance(rules, dict) or set(rules) != set(RULES):
        _fail("arch-lint rules must explicitly name the complete supported set")
    for name, (_code, enabled, severity) in RULES.items():
        if rules[name] != {"enabled": enabled, "severity": severity}:
            _fail(
                f"arch-lint rule {name!r} must set "
                f"enabled={str(enabled).lower()}, severity={severity}"
            )
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


def _brace_pairs(masked: str) -> dict[int, int]:
    stack: list[int] = []
    pairs: dict[int, int] = {}
    for index, char in enumerate(masked):
        if char == "{":
            stack.append(index)
        elif char == "}" and stack:
            pairs[stack.pop()] = index
    return pairs


@dataclass(frozen=True)
class Span:
    start: int
    end: int
    kind: str

    def contains(self, offset: int) -> bool:
        return self.start <= offset <= self.end


@dataclass(frozen=True)
class Context:
    test: bool
    spawn_blocking: bool
    async_context: bool


_VIS = r"(?:pub(?:\s*\([^)]*\))?\s+)?"
_FN_START = re.compile(
    _VIS
    + r"(?P<qualifiers>(?:(?:async|const|unsafe)\s+|extern(?:\s+\"[^\"]*\")?\s+)*)"
    + r"fn\s+(?:r#)?[A-Za-z_]\w*",
    re.MULTILINE,
)
_MOD = re.compile(_VIS + r"\bmod\s+\w+\s*\{", re.MULTILINE)
_ASYNC_BLOCK = re.compile(r"\basync(?:\s+move)?\s*\{")
_ASYNC_CLOSURE = re.compile(r"\basync(?:\s+move)?\s*\|[^|]*\|")
_CFG_TOKEN = re.compile(r"[A-Za-z_]\w*|[(),=]")


def _preceding_attrs(masked: str, start: int) -> str:
    """Return the contiguous attribute block immediately before an item."""

    prefix = masked[max(0, start - 1000) : start]
    match = re.search(r"((?:#\s*\[[^\]]+\]\s*)+)$", prefix)
    return match.group(1) if match else ""


class _CfgParser:
    def __init__(self, expression: str) -> None:
        self.tokens = _CFG_TOKEN.findall(expression)
        self.index = 0

    def parse(self) -> set[bool]:
        if self.index >= len(self.tokens):
            return {False, True}
        name = self.tokens[self.index]
        self.index += 1
        if self.index < len(self.tokens) and self.tokens[self.index] == "=":
            self.index += 1
            if self.index < len(self.tokens) and self.tokens[self.index] not in {
                ",",
                ")",
            }:
                self.index += 1
            return {False, True}
        if self.index >= len(self.tokens) or self.tokens[self.index] != "(":
            return {False} if name == "test" else {False, True}
        self.index += 1
        arguments: list[set[bool]] = []
        while self.index < len(self.tokens) and self.tokens[self.index] != ")":
            arguments.append(self.parse())
            if self.index < len(self.tokens) and self.tokens[self.index] == ",":
                self.index += 1
        if self.index < len(self.tokens):
            self.index += 1
        if name == "not" and len(arguments) == 1:
            return {not value for value in arguments[0]}
        if name == "all":
            values = {True}
            for argument in arguments:
                values = {left and right for left in values for right in argument}
            return values
        if name == "any":
            values = {False}
            for argument in arguments:
                values = {left or right for left in values for right in argument}
            return values
        return {False, True}


def _attrs_are_test(attrs: str) -> bool:
    if re.search(r"#\s*\[\s*(?:[A-Za-z_]\w*::)*test\b", attrs):
        return True
    for cfg in re.finditer(r"#\s*\[\s*cfg\s*\(([^]]*)\)\s*\]", attrs):
        if True not in _CfgParser(cfg.group(1)).parse():
            return True
    return False


def _function_body(masked: str, signature_start: int) -> int | None:
    """Resolve a function body without mistaking signature const braces for it."""

    stack: list[str] = []
    closing = {"(": ")", "[": "]", "<": ">", "{": "}"}
    position = signature_start
    while position < len(masked):
        char = masked[position]
        if char == "{" and not stack:
            return position
        if char in "([":
            stack.append(closing[char])
        elif char == "{":
            stack.append("}")
        elif char == "<" and "}" not in stack:
            stack.append(">")
        elif stack and char == stack[-1]:
            stack.pop()
        elif char == ";" and not stack:
            return None
        elif char == "}" and not stack:
            _fail("function signature reached an enclosing scope before its body")
        position += 1
    _fail("function signature has no provable body or terminating semicolon")


def _lexical_scope(
    brace_pairs: dict[int, int], position: int, source_length: int
) -> tuple[int, int]:
    containing = [
        (start, end) for start, end in brace_pairs.items() if start < position < end
    ]
    if not containing:
        return 0, source_length
    start, end = min(containing, key=lambda pair: pair[1] - pair[0])
    return start + 1, end


def _name_shadowed(fragment: str, name: str, call_offset: int) -> bool:
    prefix = fragment[:call_offset]
    escaped = re.escape(name)
    declarations = (
        rf"\blet\s+(?:mut\s+)?{escaped}\b",
        rf"\b(?:fn|mod|struct|enum|const|static|type)\s+{escaped}\b",
        rf"\bfn\s+\w+[^{{;]*\([^)]*\b{escaped}\s*:",
    )
    return any(re.search(pattern, prefix) for pattern in declarations)


def _closing_paren(masked: str, opening: int) -> int | None:
    depth = 0
    for position in range(opening, len(masked)):
        if masked[position] == "(":
            depth += 1
        elif masked[position] == ")":
            depth -= 1
            if depth == 0:
                return position
    return None


def _expression_end(masked: str, start: int, limit: int) -> int:
    """Return the conservative end of one closure expression body."""

    stack: list[str] = []
    closing = {"(": ")", "[": "]", "{": "}"}
    position = start
    while position < limit:
        char = masked[position]
        if char in closing:
            stack.append(closing[char])
        elif stack and char == stack[-1]:
            stack.pop()
        elif not stack and char in {",", ";", ")", "]", "}"}:
            return max(start, position - 1)
        position += 1
    return max(start, limit - 1)


def _async_closure_spans(masked: str, brace_pairs: dict[int, int]) -> list[Span]:
    spans: list[Span] = []
    for match in _ASYNC_CLOSURE.finditer(masked):
        body = match.end()
        while body < len(masked) and masked[body].isspace():
            body += 1
        if body >= len(masked):
            spans.append(Span(match.start(), len(masked), "async_closure"))
            continue
        if masked[body] == "{" and body in brace_pairs:
            spans.append(Span(body, brace_pairs[body], "async_closure"))
            continue
        _scope_start, scope_end = _lexical_scope(
            brace_pairs, match.start(), len(masked)
        )
        spans.append(
            Span(body, _expression_end(masked, body, scope_end), "async_closure")
        )
    return spans


def _spawn_blocking_spans(masked: str) -> list[Span]:
    spans: list[Span] = []
    brace_pairs = _brace_pairs(masked)
    closure = r"\s*\(\s*(?:move\s*)?\|[^|]*\|"

    def add_calls(pattern: str, start: int, end: int, alias: str | None = None) -> None:
        fragment = masked[start:end]
        for match in re.finditer(pattern + closure, fragment):
            if alias and _name_shadowed(fragment, alias, match.start()):
                continue
            absolute_start = start + match.start()
            opening = masked.find("(", absolute_start, start + match.end())
            closing = _closing_paren(masked, opening) if opening >= 0 else None
            if closing is not None:
                spans.append(Span(start + match.end(), closing, "spawn_blocking"))

    add_calls(r"(?<![:\w])::tokio::task::spawn_blocking", 0, len(masked))
    imports: list[tuple[str, int, int]] = []
    direct = re.compile(r"\buse\s+::tokio::task::spawn_blocking\s*(?:as\s+(\w+))?\s*;")
    for match in direct.finditer(masked):
        start, end = _lexical_scope(brace_pairs, match.start(), len(masked))
        imports.append((match.group(1) or "spawn_blocking", start, end))
    grouped = re.compile(r"\buse\s+::tokio::task::\{([^}]*)\}\s*;")
    for match in grouped.finditer(masked):
        for item in match.group(1).split(","):
            parsed = re.fullmatch(r"\s*spawn_blocking\s*(?:as\s+(\w+))?\s*", item)
            if parsed:
                start, end = _lexical_scope(brace_pairs, match.start(), len(masked))
                imports.append((parsed.group(1) or "spawn_blocking", start, end))
    for alias, start, end in imports:
        add_calls(rf"(?<![:\w]){re.escape(alias)}", start, end, alias)
    return spans


def _spans(masked: str) -> list[Span]:
    pairs = _brace_pairs(masked)
    spans: list[Span] = []
    for match in _MOD.finditer(masked):
        brace = match.end() - 1
        attrs = _preceding_attrs(masked, match.start())
        if brace in pairs and _attrs_are_test(attrs):
            spans.append(Span(brace, pairs[brace], "test"))
    for match in _FN_START.finditer(masked):
        brace = _function_body(masked, match.end())
        if brace is None:
            continue
        if brace not in pairs:
            _fail("resolved function body has no matching closing brace")
        attrs = _preceding_attrs(masked, match.start())
        if _attrs_are_test(attrs):
            spans.append(Span(brace, pairs[brace], "test"))
        qualifiers = match.group("qualifiers")
        spans.append(
            Span(
                brace,
                pairs[brace],
                "async_fn" if re.search(r"\basync\b", qualifiers) else "sync_fn",
            )
        )
    for match in _ASYNC_BLOCK.finditer(masked):
        brace = match.end() - 1
        if brace in pairs:
            spans.append(Span(brace, pairs[brace], "async_block"))
    spans.extend(_async_closure_spans(masked, pairs))
    spans.extend(_spawn_blocking_spans(masked))
    return spans


def context_at(source: str, offset: int) -> Context:
    masked = _rust_code_mask(source)
    return _context_from_spans(_spans(masked), offset)


def _context_from_spans(spans: list[Span], offset: int) -> Context:
    containing = [span for span in spans if span.contains(offset)]
    test = any(span.kind == "test" for span in containing)
    spawn = any(span.kind == "spawn_blocking" for span in containing)
    execution_contexts = [
        span
        for span in containing
        if span.kind in {"sync_fn", "async_fn", "async_block", "async_closure"}
    ]
    innermost = (
        min(execution_contexts, key=lambda span: span.end - span.start)
        if execution_contexts
        else None
    )
    is_async = bool(
        innermost and innermost.kind in {"async_fn", "async_block", "async_closure"}
    )
    return Context(test=test, spawn_blocking=spawn, async_context=is_async)


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


def _disposition(
    path: str, context: Context, *, receiver_unresolved: bool
) -> tuple[str, str, bool]:
    if receiver_unresolved:
        return (
            "receiver_unresolved",
            "method receiver type is not proven by arch-lint 0.5.0",
            False,
        )
    if _path_is_test(path) or context.test:
        return "test_context", "test, bench, example, or cfg(test) context", False
    if context.spawn_blocking:
        return (
            "spawn_blocking_context",
            "blocking call is already off the async executor",
            False,
        )
    if context.async_context:
        return (
            "async_direct",
            "qualified synchronous filesystem call in async context",
            True,
        )
    return (
        "sync_context",
        "synchronous startup, offline, or ordinary function context",
        False,
    )


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
    imports: list[tuple[str, str, str, int, int]] = []

    def add_import(kind: str, alias: str, operation: str, position: int) -> None:
        scope_start, scope_end = _lexical_scope(brace_pairs, position, len(masked))
        imports.append((kind, alias, operation, scope_start, scope_end))

    def add_fs_group(start: int, end: int, position: int) -> None:
        for item_start, item_end in _group_items(masked, start, end, brace_pairs):
            item = masked[item_start:item_end]
            pieces = re.fullmatch(r"\s*(\w+)\s*(?:as\s+(\w+))?\s*", item)
            if pieces and pieces.group(1) == "self":
                add_import("module", pieces.group(2) or "fs", "", position)
            elif pieces and pieces.group(1) in FS_OPERATIONS:
                add_import(
                    "function",
                    pieces.group(2) or pieces.group(1),
                    pieces.group(1),
                    position,
                )
            elif pieces and pieces.group(1) == "File":
                add_import(
                    "file_type",
                    pieces.group(2) or pieces.group(1),
                    pieces.group(1),
                    position,
                )
            elif item.strip() == "*" or "{" in item:
                _fail("unsupported nested std::fs import cannot be proven safe")

    for match in re.finditer(r"\buse\s+(?:::)?std::fs\s*(?:as\s+(\w+))?\s*;", masked):
        add_import("module", match.group(1) or "fs", "", match.end())
    for match in re.finditer(r"\buse\s+(?:::)?std::\s*\{", masked):
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
            item = masked[item_start:item_end]
            pieces = re.fullmatch(r"\s*fs\s*(?:as\s+(\w+))?\s*", item)
            if pieces:
                add_import("module", pieces.group(1) or "fs", "", semicolon + 1)
                continue
            flat = re.fullmatch(r"\s*fs\s*::\s*(\w+)\s*(?:as\s+(\w+))?\s*", item)
            if flat:
                operation, alias = flat.groups()
                if operation in FS_OPERATIONS:
                    add_import("function", alias or operation, operation, semicolon + 1)
                elif operation == "File":
                    add_import(
                        "file_type", alias or operation, operation, semicolon + 1
                    )
                continue
            nested = re.match(r"\s*fs\s*::\s*\{", item)
            if not nested:
                if re.match(r"\s*fs\s*::", item):
                    _fail("unsupported std::fs import cannot be proven safe")
                continue
            inner_open = item_start + nested.end() - 1
            inner_close = brace_pairs.get(inner_open)
            if inner_close is None or masked[inner_close + 1 : item_end].strip():
                _fail("unsupported nested std::fs grouped import")
            add_fs_group(inner_open + 1, inner_close, semicolon + 1)
    for match in re.finditer(
        r"\buse\s+(?:::)?std::fs::(\w+)\s*(?:as\s+(\w+))?\s*;", masked
    ):
        operation, alias = match.groups()
        if operation in FS_OPERATIONS:
            add_import("function", alias or operation, operation, match.end())
        elif operation == "File":
            add_import("file_type", alias or operation, operation, match.end())
    for match in re.finditer(r"\buse\s+(?:::)?std::fs::\{([^}]*)\}\s*;", masked):
        add_fs_group(match.start(1), match.end(1), match.end())
    if re.search(r"\buse\s+(?:::)?std::fs::\*\s*;", masked):
        _fail("unsupported std::fs glob import cannot be proven safe")

    found: list[dict[str, Any]] = []
    candidates: list[tuple[int, str, str]] = []
    operation_pattern = "|".join(FS_OPERATIONS)
    for kind, alias, operation, scope_start, scope_end in imports:
        fragment = masked[scope_start:scope_end]
        if kind == "module":
            pattern = (
                rf"\b{re.escape(alias)}::"
                rf"((?:{operation_pattern})|File::(?:{'|'.join(FILE_OPERATIONS)}))\s*\("
            )
            for match in re.finditer(pattern, fragment):
                candidates.append(
                    (scope_start + match.start(), match.group(1), match.group(0))
                )
        elif kind == "function":
            for match in re.finditer(rf"(?<![:\w]){re.escape(alias)}\s*\(", fragment):
                candidates.append(
                    (scope_start + match.start(), operation, match.group(0))
                )
        else:
            for match in re.finditer(
                rf"\b{re.escape(alias)}::({'|'.join(FILE_OPERATIONS)})\s*\(",
                fragment,
            ):
                candidates.append(
                    (
                        scope_start + match.start(),
                        f"File::{match.group(1)}",
                        match.group(0),
                    )
                )

    line_starts = [0]
    line_starts.extend(match.end() for match in re.finditer("\n", source))
    for offset, operation, expression in sorted(set(candidates)):
        context = _context_from_spans(spans, offset)
        if _path_is_test(path) or context.test:
            classification, reason, blocking = (
                "test_context",
                "aliased call is in test context",
                False,
            )
        elif context.spawn_blocking:
            classification, reason, blocking = (
                "spawn_blocking_context",
                "aliased call is off the executor",
                False,
            )
        elif context.async_context:
            classification, reason, blocking = (
                "async_alias",
                "aliased synchronous filesystem call in async context",
                True,
            )
        else:
            classification, reason, blocking = (
                "sync_context",
                "aliased call is in synchronous context",
                False,
            )
        line = 1 + sum(start <= offset for start in line_starts[1:])
        column = offset - line_starts[line - 1] + 1
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


def classify_report(report: dict[str, Any], sources: dict[str, str]) -> dict[str, Any]:
    """Validate a raw report and add conservative, non-mutating dispositions."""

    if not isinstance(report, dict) or set(report) != {"violations", "files_checked"}:
        _fail("arch-lint JSON must contain exactly violations and files_checked")
    violations = report["violations"]
    if not isinstance(violations, list) or report["files_checked"] != len(sources):
        _fail(
            f"arch-lint files_checked={report.get('files_checked')!r} does not match "
            f"source universe={len(sources)}"
        )
    al002_paths: set[str] = set()
    for index, finding in enumerate(violations):
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
        if finding.get("code") == "AL002":
            al002_paths.add(path)
    alias_paths = {path for path, source in sources.items() if _has_fs_import(source)}
    analyses = {
        path: (masked, _spans(masked))
        for path in sorted(al002_paths | alias_paths)
        for source in [sources[path]]
        for masked in [_rust_code_mask(source)]
    }
    dispositions: list[dict[str, Any]] = []
    for index, finding in enumerate(violations):
        location = finding.get("location")
        path = location["file"].removeprefix("./")
        if finding.get("code") != "AL002":
            continue
        message = finding.get("message", "")
        receiver_unresolved = bool(re.search(r"`\.[A-Za-z_]\w*\(\)`", str(message)))
        offset = _offset(sources[path], location.get("line"), location.get("column"))
        context = _context_from_spans(analyses[path][1], offset)
        classification, reason, blocking = _disposition(
            path, context, receiver_unresolved=receiver_unresolved
        )
        dispositions.append(
            {
                "raw_violation_index": index,
                "classification": classification,
                "reason": reason,
                "blocking": blocking,
            }
        )
    aliases = [
        item
        for path in sorted(alias_paths)
        for source in [sources[path]]
        for item in _alias_calls(
            path, source, masked=analyses[path][0], spans=analyses[path][1]
        )
    ]
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
