#!/usr/bin/env python3
"""Rust lexical scanning shared by the repository's static scanners.

Comment, literal, and balanced-delimiter recognition is one authority: the
module-tree reader, the architecture lint wrapper, and the dispatch
decomposition gate all read the same masked source, so a lexical
disagreement between them would silently change what each gate can see.
Every scanner here fails closed on source it cannot lex.
"""

from __future__ import annotations

import re


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Rust source scanner failed: {message}")


def _balanced_code_step(
    source: str,
    index: int,
    char: str,
    following: str,
    opener: str,
    closer: str,
    depth: int,
) -> tuple[str, int, int, int | None]:
    """Advance one code-state character in the balanced-span scanner."""

    token = char + following
    if token == "//":
        return "line-comment", depth, 1, None
    if token == "/*":
        return "block-comment", depth, 1, None
    if char == '"':
        return "string", depth, 0, None
    if char == "'":
        char_literal = _CHAR_LITERAL.match(source, index)
        if char_literal is not None:
            return "code", depth, char_literal.end() - index - 1, None
        return "code", depth, 0, None
    if char == opener:
        return "code", depth + 1, 0, None
    if char == closer:
        depth -= 1
        if depth == 0:
            return "code", depth, 0, index
    return "code", depth, 0, None


def _balanced_non_code_step(
    state: str,
    char: str,
    following: str,
    block_comment_depth: int,
) -> tuple[str, int, int]:
    """Advance one comment/string/character-literal scanner state."""

    if state == "line-comment":
        return ("code" if char == "\n" else state), block_comment_depth, 0
    if state == "block-comment":
        token = char + following
        if token == "/*":
            return state, block_comment_depth + 1, 1
        if token == "*/":
            block_comment_depth -= 1
            return (
                "code" if block_comment_depth == 0 else state,
                block_comment_depth,
                1,
            )
        return state, block_comment_depth, 0
    quote = '"' if state == "string" else "'"
    if char == "\\":
        return state, block_comment_depth, 1
    return ("code" if char == quote else state), block_comment_depth, 0


def _balanced_span_from(source: str, start: int, opener: str, closer: str) -> int:
    """Index of the `closer` that balances the `opener` at `start`, comment/string-aware.

    The position-based core `_balanced_block` (and the call-graph resolution in
    `_routing_call_offset`/`_function_with_callees`) share, factored out so the
    latter can locate a function's body directly from a known start index instead
    of re-searching the whole source with `_function`'s marker-based lookup for
    every candidate — that repeated whole-source re-search is O(candidates ×
    file size) and was measured costing ~5s on dispatch.rs's ~600-function scale.
    """
    require(source[start] == opener, f"expected {opener!r} at position {start}")
    depth = 0
    index = start
    state = "code"
    block_comment_depth = 0
    while index < len(source):
        char = source[index]
        following = source[index + 1] if index + 1 < len(source) else ""
        if state == "code":
            state, depth, skip, closing_index = _balanced_code_step(
                source, index, char, following, opener, closer, depth
            )
            block_comment_depth = int(state == "block-comment")
        else:
            state, block_comment_depth, skip = _balanced_non_code_step(
                state, char, following, block_comment_depth
            )
            closing_index = None
        if closing_index is not None:
            return closing_index
        index += skip + 1
    require(False, f"unterminated balanced block starting at position {start}")
    return -1  # unreachable; keeps static type checkers total


_RAW_LITERAL_START = re.compile(r'(?:b|c)?r(?P<hashes>#{0,255})"')
_CHAR_LITERAL = re.compile(
    r"b?'(?:\\(?:.|x[0-9A-Fa-f]{2}|u\{[0-9A-Fa-f_]+\})|[^'\\\n])'"
)
_STRING_LITERAL_START = re.compile(r'(?:b|c)?"')


def _blank_span(masked: list[str], source: str, start: int, end: int) -> None:
    """Blank one masked span in place, retaining source offsets and newlines."""

    for position in range(start, end):
        if source[position] != "\n":
            masked[position] = " "


def _line_comment_end(source: str, start: int) -> int:
    """Return the offset after the ``//`` comment opening at ``start``."""

    end = source.find("\n", start + 2)
    return len(source) if end < 0 else end


def _block_comment_end(source: str, start: int) -> int:
    """Return the offset after the nestable ``/*`` comment opening at ``start``."""

    depth = 1
    cursor = start + 2
    while cursor < len(source) and depth:
        if source.startswith("/*", cursor):
            depth += 1
            cursor += 2
        elif source.startswith("*/", cursor):
            depth -= 1
            cursor += 2
        else:
            cursor += 1
    require(depth == 0, "unterminated Rust block comment")
    return cursor


def _raw_string_end(source: str, opener: re.Match[str]) -> int:
    """Return the offset after the raw string whose opener already matched."""

    terminator = '"' + opener.group("hashes")
    end = source.find(terminator, opener.end())
    require(end >= 0, "unterminated Rust raw string")
    return end + len(terminator)


def _quoted_string_end(source: str, quote: int) -> int:
    """Return the offset after the escapable string whose quote is at ``quote``."""

    end = quote + 1
    while end < len(source):
        if source[end] == "\\":
            end += 2
            continue
        if source[end] == '"':
            end += 1
            break
        end += 1
    require(
        end <= len(source) and source[end - 1] == '"',
        "unterminated Rust literal",
    )
    return end


def _rust_lexical_span(source: str, index: int) -> tuple[int, bool] | None:
    """Return the end offset and literal flag of the token opening at ``index``.

    ``None`` means ``index`` opens neither a comment nor a literal. Byte and C
    string prefixes belong to their literal token and are reported with it.
    """

    if source.startswith("//", index):
        return _line_comment_end(source, index), False
    if source.startswith("/*", index):
        return _block_comment_end(source, index), False
    raw = _RAW_LITERAL_START.match(source, index)
    if raw is not None:
        return _raw_string_end(source, raw), True
    character = _CHAR_LITERAL.match(source, index)
    if character is not None:
        return character.end(), True
    quoted = _STRING_LITERAL_START.match(source, index)
    if quoted is not None:
        return _quoted_string_end(source, quoted.end() - 1), True
    return None


def _rust_mask(source: str, *, literals: bool) -> str:
    """Blank comments and optionally literals while retaining source offsets."""

    masked = list(source)
    index = 0
    while index < len(source):
        span = _rust_lexical_span(source, index)
        if span is None:
            index += 1
            continue
        end, is_literal = span
        if literals or not is_literal:
            _blank_span(masked, source, index, end)
        index = end
    return "".join(masked)


def _rust_code_mask(source: str) -> str:
    return _rust_mask(source, literals=True)


def _rust_comments_mask(source: str) -> str:
    return _rust_mask(source, literals=False)
