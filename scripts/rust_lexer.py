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


def _delimiter_depths(mask: str) -> list[tuple[int, int, int]]:
    """Return brace, parenthesis, and bracket depth before every character."""

    depths = [(0, 0, 0)] * (len(mask) + 1)
    current = [0, 0, 0]
    closing = {"}": 0, ")": 1, "]": 2}
    opening = {"{": 0, "(": 1, "[": 2}
    for position, char in enumerate(mask):
        depths[position] = tuple(current)
        if char in opening:
            current[opening[char]] += 1
        elif char in closing:
            current[closing[char]] -= 1
        require(min(current) >= 0, "unbalanced Rust module delimiters")
    depths[-1] = tuple(current)
    require(depths[-1] == (0, 0, 0), "unbalanced Rust module delimiters")
    return depths


def _top_level_parts(mask: str, start: int, end: int) -> list[tuple[int, int]]:
    """Split an attribute argument span at top-level commas."""

    parts: list[tuple[int, int]] = []
    part_start = start
    depths = [0, 0]
    opening = {"(": 0, "[": 1}
    closing = {")": 0, "]": 1}
    for position in range(start, end):
        char = mask[position]
        if char in opening:
            depths[opening[char]] += 1
        elif char in closing:
            depths[closing[char]] -= 1
        elif char == "," and depths == [0, 0]:
            parts.append((part_start, position))
            part_start = position + 1
        require(min(depths) >= 0, "unbalanced Rust attribute arguments")
    require(depths == [0, 0], "unbalanced Rust attribute arguments")
    parts.append((part_start, end))
    return parts


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


# Rust module declarations are lexical input to the compiler-declared source
# walker. Keep the macro/template, cfg, and item-boundary readers beside the
# comment/literal masker so every scanner shares the same delimiter rules.
_ATTRIBUTE_START = re.compile(r"#\s*(?P<inner>!)?\s*\[")
_MACRO_RULES_START = re.compile(
    r"\bmacro_rules\s*!\s*(?:r#)?[A-Za-z_][A-Za-z0-9_]*\s*(?P<opener>[{([])"
)
_MACRO_ATTRIBUTE_TEMPLATE = re.compile(
    r"\s*\$\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*:\s*(?P<fragment>[A-Za-z_][A-Za-z0-9_]*))?\s*"
)
_CFG_TOKEN = re.compile(
    r"\s*(?:(?P<ident>[A-Za-z_][A-Za-z0-9_]*)|"
    r'(?P<string>"(?:\\.|[^"\\])*")|(?P<punct>[(),=]))'
)
_CFG_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_ITEM_KIND = re.compile(
    r"\b(fn|struct|enum|union|impl|trait|mod|const|static|type|use|extern|macro_rules)\b"
)
_MACRO_ITEM = re.compile(
    r"\b[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*!\s*$"
)
_ITEM_DELIMITER_DELTAS = {"(": (1, 0), ")": (-1, 0), "[": (0, 1), "]": (0, -1)}


def _macro_rule_bodies(mask: str):
    for macro in _MACRO_RULES_START.finditer(mask):
        opener = macro.start("opener")
        opening = mask[opener]
        closer = _balanced_span_from(
            mask, opener, opening, {"{": "}", "(": ")", "[": "]"}[opening]
        )
        body_start = opener + 1
        body = mask[body_start:closer]
        yield body_start, body, _delimiter_depths(body)


def _macro_rule_arrows(body: str, depths: list[tuple[int, int, int]]) -> list[int]:
    return [
        position
        for position in range(len(body) - 1)
        if body.startswith("=>", position) and depths[position] == (0, 0, 0)
    ]


def _macro_rule_end(
    body: str, arrow: int, depths: list[tuple[int, int, int]]
) -> int:
    for position in range(arrow + 2, len(body)):
        if body[position] in ";," and depths[position] == (0, 0, 0):
            return position
    return len(body)


def _macro_attribute_templates(
    body: str, start: int, end: int
) -> list[tuple[int, str, str | None]]:
    templates: list[tuple[int, str, str | None]] = []
    for attribute in _ATTRIBUTE_START.finditer(body, start, end):
        bracket = body.find("[", attribute.start(), attribute.end())
        closer = _balanced_span_from(body, bracket, "[", "]")
        template = _MACRO_ATTRIBUTE_TEMPLATE.fullmatch(body[bracket + 1 : closer])
        if template is not None:
            templates.append(
                (attribute.start(), template.group("name"), template.group("fragment"))
            )
    return templates


def _macro_templates_for_rule(
    body_start: int,
    body: str,
    arrow: int,
    rule_start: int,
    rule_end: int,
) -> set[int]:
    parsed = _macro_attribute_templates(body, rule_start, rule_end)
    bindings = {
        name for position, name, fragment in parsed if position < arrow and fragment == "meta"
    }
    return {
        body_start + position
        for position, name, fragment in parsed
        if _macro_template_is_emitted(position, arrow, name, fragment, bindings)
    }


def _macro_template_is_emitted(
    position: int,
    arrow: int,
    name: str,
    fragment: str | None,
    bindings: set[str],
) -> bool:
    if position < arrow:
        return fragment == "meta" and name in bindings
    return fragment is None and name in bindings


def _macro_rule_template_attribute_starts(mask: str) -> set[int]:
    """Return proven ``#[$meta]`` template positions inside ``macro_rules!``."""

    templates: set[int] = set()
    for body_start, body, depths in _macro_rule_bodies(mask):
        arrows = _macro_rule_arrows(body, depths)
        require(arrows, "macro_rules definition has no top-level rule")
        rule_start = 0
        for arrow in arrows:
            rule_end = _macro_rule_end(body, arrow, depths)
            templates.update(
                _macro_templates_for_rule(
                    body_start, body, arrow, rule_start, rule_end
                )
            )
            rule_start = rule_end + 1
    return templates


def _cfg_tokens(expression: str) -> list[str]:
    tokens: list[str] = []
    position = 0
    while position < len(expression):
        if not expression[position:].strip():
            break
        match = _CFG_TOKEN.match(expression, position)
        require(match is not None, f"unsupported Rust cfg expression: {expression}")
        tokens.append(
            match.group("ident") or match.group("string") or match.group("punct")
        )
        position = match.end()
    return tokens


class _CfgParser:
    def __init__(self, expression: str) -> None:
        self.expression = expression
        self.tokens = _cfg_tokens(expression)
        self.position = 0

    def parse(self) -> tuple:
        tree = self._atom()
        require(
            self.position == len(self.tokens),
            f"trailing Rust cfg tokens: {self.expression}",
        )
        return tree

    def _atom(self) -> tuple:
        name = self._identifier()
        if self._peek() == "(":
            return self._operator(name)
        return self._value(name)

    def _identifier(self) -> str:
        require(
            self.position < len(self.tokens),
            f"incomplete Rust cfg expression: {self.expression}",
        )
        name = self.tokens[self.position]
        require(
            _CFG_IDENT.fullmatch(name) is not None,
            f"invalid Rust cfg atom: {name}",
        )
        self.position += 1
        return name

    def _peek(self) -> str | None:
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def _operator(self, name: str) -> tuple:
        require(
            name in {"all", "any", "not"},
            f"unsupported Rust cfg operator: {name}",
        )
        self.position += 1
        values: list[tuple] = []
        while self._peek() is not None and self._peek() != ")":
            values.append(self._atom())
            if self._peek() != ",":
                break
            self.position += 1
            if self._peek() == ")":
                break
        require(
            self._peek() == ")",
            f"unclosed Rust cfg operator: {self.expression}",
        )
        self.position += 1
        require(
            name != "not" or len(values) == 1,
            "Rust cfg not() must have one argument",
        )
        return (name, tuple(values))

    def _value(self, name: str) -> tuple:
        value = name
        if self._peek() == "=":
            self.position += 1
            token = self._peek()
            require(
                token is not None and token.startswith('"'),
                f"Rust cfg value must be a string: {self.expression}",
            )
            value = f"{name}={token}"
            self.position += 1
        return ("atom", value)


def _parse_cfg_expression(expression: str) -> tuple:
    return _CfgParser(expression).parse()


def _cfg_all(values: list[bool | None]) -> bool | None:
    if False in values:
        return False
    return True if all(value is True for value in values) else None


def _cfg_any(values: list[bool | None]) -> bool | None:
    if True in values:
        return True
    return False if all(value is False for value in values) else None


def _cfg_not(values: list[bool | None]) -> bool | None:
    return None if values[0] is None else not values[0]


_CFG_EVALUATORS = {"all": _cfg_all, "any": _cfg_any, "not": _cfg_not}


def _evaluate_cfg(tree: tuple, values: dict[str, bool]) -> bool | None:
    kind, payload = tree
    if kind == "atom":
        return False if payload == "test" else values.get(payload)
    evaluated = [_evaluate_cfg(child, values) for child in payload]
    return _CFG_EVALUATORS[kind](evaluated)


def _item_block_end(mask: str, start: int, position: int) -> tuple[int, bool]:
    header = mask[start:position]
    item_kind = _ITEM_KIND.search(header)
    macro_item = _MACRO_ITEM.search(header)
    if item_kind is None and macro_item is not None:
        closer = _balanced_span_from(mask, position, "{", "}")
        end = closer + 2 if mask[closer + 1 :].startswith(";") else closer + 1
        return end, False
    require(item_kind is not None, "unsupported cfg-disabled Rust item")
    semicolon_bound = item_kind.group(1) in {"const", "static", "type", "use"}
    if item_kind.group(1) == "const":
        after_const = header[item_kind.end() :]
        const_function = re.match(
            r"\s+(?:(?:unsafe|async)\s+)*(?:extern(?:\s+\"[^\"]*\")?\s+)?fn\b",
            after_const,
        )
        semicolon_bound = const_function is None
    closer = _balanced_span_from(mask, position, "{", "}")
    return (closer, True) if semicolon_bound else (closer + 1, False)


def _item_end(mask: str, start: int, limit: int) -> int:
    parens = 0
    brackets = 0
    position = start
    while position < limit:
        char = mask[position]
        delta = _ITEM_DELIMITER_DELTAS.get(char)
        if delta is not None:
            parens += delta[0]
            brackets += delta[1]
        elif char == ";" and parens == 0 and brackets == 0:
            return position + 1
        elif char == "{" and parens == 0 and brackets == 0:
            end, continue_scan = _item_block_end(mask, start, position)
            if not continue_scan:
                return end
            position = end
        position += 1
    require(False, "test-only Rust item has no terminator")
    return -1
