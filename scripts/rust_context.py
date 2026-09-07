#!/usr/bin/env python3
"""Lexical execution-context analysis of Rust source for the architecture gate.

arch-lint 0.5.0 reports a call site without proving what it runs on. Deciding
that is a source-analysis question — which spans (test item, ``spawn_blocking``
body, async fn/block/closure, ordinary fn) enclose an offset, which cfg
predicates can hold with ``test`` false, and which lexical scope an import
binds in. That analysis is this module's, so the gate wrapper is left with
policy, report validation, and the receipt.

Every scanner here fails closed: source it cannot decide raises ``GateError``
or widens to both truth values rather than proving a call safe.
"""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass
from typing import NoReturn

from rust_lexer import _rust_code_mask


class GateError(RuntimeError):
    """The policy, environment, scanner report, or source universe is invalid."""


def _fail(message: str) -> NoReturn:
    raise GateError(message)


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


_EITHER = frozenset({False, True})


def _cfg_combination(name: str, arguments: list[frozenset[bool]]) -> frozenset[bool]:
    """Fold a cfg operator's argument truth sets; unknown operators stay open."""

    if name == "not" and len(arguments) == 1:
        return frozenset(not value for value in arguments[0])
    if name == "all":
        return frozenset(map(all, itertools.product(*arguments)))
    if name == "any":
        return frozenset(map(any, itertools.product(*arguments)))
    return _EITHER


class _CfgParser:
    """Truth-set evaluator for a cfg predicate with ``test`` held false.

    A predicate this evaluator cannot decide yields both truth values, so an
    undecidable configuration is never reported as unreachable.
    """

    def __init__(self, expression: str) -> None:
        self.tokens = _CFG_TOKEN.findall(expression)
        self.index = 0

    def _peek(self) -> str | None:
        return self.tokens[self.index] if self.index < len(self.tokens) else None

    def _accept(self, token: str) -> bool:
        if self._peek() != token:
            return False
        self.index += 1
        return True

    def _arguments(self) -> list[frozenset[bool]]:
        arguments: list[frozenset[bool]] = []
        while self.index < len(self.tokens) and self._peek() != ")":
            arguments.append(self.parse())
            self._accept(",")
        if self.index < len(self.tokens):
            self.index += 1
        return arguments

    def parse(self) -> frozenset[bool]:
        name = self._peek()
        if name is None:
            return _EITHER
        self.index += 1
        if self._accept("="):
            if self._peek() not in {None, ",", ")"}:
                self.index += 1
            return _EITHER
        if not self._accept("("):
            return frozenset({False}) if name == "test" else _EITHER
        return _cfg_combination(name, self._arguments())


def _attrs_are_test(attrs: str) -> bool:
    if re.search(r"#\s*\[\s*(?:[A-Za-z_]\w*::)*test\b", attrs):
        return True
    for cfg in re.finditer(r"#\s*\[\s*cfg\s*\(([^]]*)\)\s*\]", attrs):
        if True not in _CfgParser(cfg.group(1)).parse():
            return True
    return False


_DELIMITERS = {"(": ")", "[": "]", "{": "}"}


def _track_delimiter(stack: list[str], char: str) -> None:
    """Advance the delimiter-depth stack of a Rust function signature."""

    if stack and char == stack[-1]:
        stack.pop()
    elif char in _DELIMITERS:
        stack.append(_DELIMITERS[char])
    elif char == "<" and "}" not in stack:
        stack.append(">")


def _function_body(masked: str, signature_start: int) -> int | None:
    """Resolve a function body without mistaking signature const braces for it."""

    stack: list[str] = []
    for position in range(signature_start, len(masked)):
        char = masked[position]
        if not stack:
            if char == "{":
                return position
            if char == ";":
                return None
            if char == "}":
                _fail("function signature reached an enclosing scope before its body")
        _track_delimiter(stack, char)
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


def _module_test_spans(masked: str, pairs: dict[int, int]) -> list[Span]:
    """Span every inline ``mod`` whose attributes prove it is test-only."""

    spans: list[Span] = []
    for match in _MOD.finditer(masked):
        brace = match.end() - 1
        if brace in pairs and _attrs_are_test(_preceding_attrs(masked, match.start())):
            spans.append(Span(brace, pairs[brace], "test"))
    return spans


def _function_spans(masked: str, pairs: dict[int, int]) -> list[Span]:
    """Span every function body, recording whether it is async and test-only."""

    spans: list[Span] = []
    for match in _FN_START.finditer(masked):
        brace = _function_body(masked, match.end())
        if brace is None:
            continue
        if brace not in pairs:
            _fail("resolved function body has no matching closing brace")
        if _attrs_are_test(_preceding_attrs(masked, match.start())):
            spans.append(Span(brace, pairs[brace], "test"))
        qualifiers = match.group("qualifiers")
        kind = "async_fn" if re.search(r"\basync\b", qualifiers) else "sync_fn"
        spans.append(Span(brace, pairs[brace], kind))
    return spans


def _async_block_spans(masked: str, pairs: dict[int, int]) -> list[Span]:
    """Span every ``async { .. }`` block with a resolvable closing brace."""

    return [
        Span(match.end() - 1, pairs[match.end() - 1], "async_block")
        for match in _ASYNC_BLOCK.finditer(masked)
        if match.end() - 1 in pairs
    ]


def _spans(masked: str) -> list[Span]:
    """Collect every execution-context span a call site can sit inside."""

    pairs = _brace_pairs(masked)
    spans = _module_test_spans(masked, pairs)
    spans.extend(_function_spans(masked, pairs))
    spans.extend(_async_block_spans(masked, pairs))
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
