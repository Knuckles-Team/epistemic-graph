#!/usr/bin/env python3
"""Compiler-declared Rust module-tree reader for repository static scanners.

The walker follows external/inline ``mod`` items and literal ``include!`` inputs,
evaluates whether cfg-gated items are production-reachable, and fails closed on
missing, ambiguous, cyclic, unsupported, or orphaned source declarations.
"""

from __future__ import annotations

import re
from collections import namedtuple
from pathlib import Path

from rust_lexer import (
    _balanced_span_from,
    _rust_code_mask,
    _rust_comments_mask,
    require,
)

_MODULE_ITEM = re.compile(
    r"(?:(?:pub(?:\s*\([^)]*\))?)\s+)?(?:unsafe\s+)?"
    r"mod\s+(?:r#)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<term>[;{])"
)
_MODULE_KEYWORD = re.compile(r"(?<!r#)\bmod\b")
_INCLUDE_ITEM = re.compile(r"include\s*!\s*\(")
_ATTRIBUTE_START = re.compile(r"#\s*(?P<inner>!)?\s*\[")
_MACRO_RULES_START = re.compile(
    r"\bmacro_rules\s*!\s*(?:r#)?[A-Za-z_][A-Za-z0-9_]*\s*(?P<opener>[{([])"
)
_CFG_TOKEN = re.compile(
    r"\s*(?:(?P<ident>[A-Za-z_][A-Za-z0-9_]*)|"
    r'(?P<string>"(?:\\.|[^"\\])*")|(?P<punct>[(),=]))'
)
# `derive`, like the lint attributes, takes a parenthesized path list and is
# inert for module discovery: unlike `cfg`, `cfg_attr` and `path` it can neither
# drop an item nor name a source file, so it cannot move a `mod` declaration.
# Rejecting it made every eg-types module tree unreadable (195 occurrences of
# `cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))`), which
# is what kept four architecture gates pinned to single facade files.
_CFG_ATTR_INERT_ATTRIBUTES = {
    "allow",
    "deny",
    "derive",
    "doc",
    "forbid",
    "recursion_limit",
    "warn",
}


_RustAttribute = namedtuple(
    "_RustAttribute", ("start", "end", "inner", "name", "arguments")
)
_RustSourceInput = namedtuple("_RustSourceInput", ("path", "module_dir", "predicates"))
_RustModuleFamily = namedtuple(
    "_RustModuleFamily", ("production", "with_tests", "production_paths", "all_paths")
)


def _macro_rule_template_attribute_starts(mask: str) -> set[int]:
    """Return proven ``#[$meta]`` template positions inside ``macro_rules!``.

    A dollar-prefixed bracket is not a Rust attribute at macro-definition time:
    it is a token template. It is safe to skip only when the same macro arm's
    matcher binds that metavariable with the ``meta`` fragment specifier. Arm
    locality matters; a binding from a different rule must not legitimize an
    otherwise malformed or unbound template.
    """

    templates: set[int] = set()
    for macro in _MACRO_RULES_START.finditer(mask):
        opener = macro.start("opener")
        opening = mask[opener]
        closing = {"{": "}", "(": ")", "[": "]"}[opening]
        closer = _balanced_span_from(mask, opener, opening, closing)
        body_start = opener + 1
        body = mask[body_start:closer]
        depths = _delimiter_depths(body)
        arrows = [
            position
            for position in range(len(body) - 1)
            if body.startswith("=>", position) and depths[position] == (0, 0, 0)
        ]
        require(arrows, "macro_rules definition has no top-level rule")
        rule_start = 0
        for arrow in arrows:
            rule_end = len(body)
            for position in range(arrow + 2, len(body)):
                if body[position] in ";," and depths[position] == (0, 0, 0):
                    rule_end = position
                    break

            parsed_templates: list[tuple[re.Match[str], re.Match[str]]] = []
            for attribute in _ATTRIBUTE_START.finditer(body, rule_start, rule_end):
                bracket = body.find("[", attribute.start(), attribute.end())
                attribute_closer = _balanced_span_from(body, bracket, "[", "]")
                template = re.fullmatch(
                    r"\s*\$\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
                    r"(?:\s*:\s*(?P<fragment>[A-Za-z_][A-Za-z0-9_]*))?\s*",
                    body[bracket + 1 : attribute_closer],
                )
                if template is None:
                    continue
                parsed_templates.append((attribute, template))

            # A macro attribute is bound only by the exact matcher token
            # `#[$name:meta]`. A standalone `$name:meta` elsewhere in the
            # matcher cannot retroactively legitimize a bare `#[$name]` token.
            bindings = {
                template.group("name")
                for attribute, template in parsed_templates
                if attribute.start() < arrow and template.group("fragment") == "meta"
            }
            for attribute, template in parsed_templates:
                name = template.group("name")
                fragment = template.group("fragment")
                in_matcher = attribute.start() < arrow
                if (in_matcher and fragment == "meta" and name in bindings) or (
                    not in_matcher and fragment is None and name in bindings
                ):
                    templates.add(body_start + attribute.start())
            rule_start = rule_end + 1
    return templates


def _rust_attributes(mask: str) -> list[_RustAttribute]:
    attributes: list[_RustAttribute] = []
    macro_templates = _macro_rule_template_attribute_starts(mask)
    position = 0
    while match := _ATTRIBUTE_START.search(mask, position):
        opener = mask.find("[", match.start(), match.end())
        closer = _balanced_span_from(mask, opener, "[", "]")
        if match.start() in macro_templates:
            position = closer + 1
            continue
        name_match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)", mask[opener + 1 : closer])
        require(name_match is not None, "Rust attribute must start with an identifier")
        name = name_match.group(1)
        cursor = opener + 1 + name_match.end()
        while cursor < closer and mask[cursor].isspace():
            cursor += 1
        arguments: tuple[int, int] | None = None
        if name in {"cfg", "cfg_attr"}:
            require(
                cursor < closer and mask[cursor] == "(",
                f"Rust {name} requires arguments",
            )
            argument_closer = _balanced_span_from(mask, cursor, "(", ")")
            require(
                not mask[argument_closer + 1 : closer].strip(),
                f"trailing tokens in Rust {name} attribute",
            )
            arguments = (cursor + 1, argument_closer)
        attributes.append(
            _RustAttribute(
                start=match.start(),
                end=closer + 1,
                inner=match.group("inner") is not None,
                name=name,
                arguments=arguments,
            )
        )
        position = closer + 1
    return attributes


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


def _parse_cfg_expression(expression: str) -> tuple:
    tokens = _cfg_tokens(expression)
    position = 0

    def parse() -> tuple:
        nonlocal position
        require(position < len(tokens), f"incomplete Rust cfg expression: {expression}")
        name = tokens[position]
        require(
            re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is not None,
            f"invalid Rust cfg atom: {name}",
        )
        position += 1
        if position < len(tokens) and tokens[position] == "(":
            require(
                name in {"all", "any", "not"}, f"unsupported Rust cfg operator: {name}"
            )
            position += 1
            values: list[tuple] = []
            if position < len(tokens) and tokens[position] != ")":
                while True:
                    values.append(parse())
                    if position < len(tokens) and tokens[position] == ",":
                        position += 1
                        if position < len(tokens) and tokens[position] == ")":
                            break
                        continue
                    break
            require(
                position < len(tokens) and tokens[position] == ")",
                f"unclosed Rust cfg operator: {expression}",
            )
            position += 1
            require(
                name != "not" or len(values) == 1,
                "Rust cfg not() must have one argument",
            )
            return (name, tuple(values))
        value = name
        if position < len(tokens) and tokens[position] == "=":
            position += 1
            require(
                position < len(tokens) and tokens[position].startswith('"'),
                f"Rust cfg value must be a string: {expression}",
            )
            value = f"{name}={tokens[position]}"
            position += 1
        return ("atom", value)

    tree = parse()
    require(position == len(tokens), f"trailing Rust cfg tokens: {expression}")
    return tree


def _top_level_parts(mask: str, start: int, end: int) -> list[tuple[int, int]]:
    parts: list[tuple[int, int]] = []
    part_start = start
    parens = 0
    brackets = 0
    for position in range(start, end):
        char = mask[position]
        if char == "(":
            parens += 1
        elif char == ")":
            parens -= 1
        elif char == "[":
            brackets += 1
        elif char == "]":
            brackets -= 1
        elif char == "," and parens == 0 and brackets == 0:
            parts.append((part_start, position))
            part_start = position + 1
        require(min(parens, brackets) >= 0, "unbalanced Rust attribute arguments")
    require(parens == 0 and brackets == 0, "unbalanced Rust attribute arguments")
    parts.append((part_start, end))
    return parts


def _conditional_cfg_trees(
    attribute_source: str, mask: str, attr: _RustAttribute
) -> list[tuple]:
    require(attr.arguments is not None, "Rust cfg_attr requires arguments")
    parts = _top_level_parts(mask, *attr.arguments)
    require(len(parts) >= 2, "Rust cfg_attr requires a predicate and attribute")
    predicate = _parse_cfg_expression(attribute_source[slice(*parts[0])])
    trees: list[tuple] = []
    for start, end in parts[1:]:
        nested = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)", mask[start:end])
        require(nested is not None, "unsupported Rust cfg_attr attribute")
        name = nested.group(1)
        cursor = start + nested.end()
        while cursor < end and mask[cursor].isspace():
            cursor += 1
        if name == "path":
            require(False, "conditional Rust path attribute is unsupported")
        if name == "cfg_attr":
            require(False, "nested Rust cfg_attr is unsupported")
        if name in _CFG_ATTR_INERT_ATTRIBUTES:
            _validate_inert_conditional_attribute(
                attribute_source,
                mask,
                name,
                cursor,
                end,
            )
            continue
        require(name == "cfg", f"unsupported conditional Rust attribute: {name}")
        require(
            cursor < end and mask[cursor] == "(",
            "conditional Rust cfg requires arguments",
        )
        closer = _balanced_span_from(mask, cursor, "(", ")")
        require(
            not mask[closer + 1 : end].strip(),
            "trailing conditional Rust cfg tokens",
        )
        conditional = _parse_cfg_expression(attribute_source[cursor + 1 : closer])
        trees.append(("any", (("not", (predicate,)), conditional)))
    return trees


def _validate_inert_conditional_attribute(
    attribute_source: str,
    mask: str,
    name: str,
    cursor: int,
    end: int,
) -> None:
    failure = f"unsupported conditional Rust attribute shape: {name}"
    if name in {"allow", "deny", "derive", "forbid", "warn"}:
        require(cursor < end and mask[cursor] == "(", failure)
        closer = _balanced_span_from(mask, cursor, "(", ")")
        require(not mask[closer + 1 : end].strip(), failure)
        item = r"[A-Za-z_][A-Za-z0-9_]*(?:\s*::\s*[A-Za-z_][A-Za-z0-9_]*)*"
        require(
            re.fullmatch(
                rf"\s*{item}(?:\s*,\s*{item})*\s*,?\s*", mask[cursor + 1 : closer]
            )
            is not None,
            failure,
        )
        return
    if name == "doc":
        tail = mask[cursor:end]
        if tail.lstrip().startswith("="):
            equals = cursor + len(tail) - len(tail.lstrip())
            _static_string_literal(attribute_source[equals + 1 : end], failure)
            return
        require(cursor < end and mask[cursor] == "(", failure)
        closer = _balanced_span_from(mask, cursor, "(", ")")
        require(not mask[closer + 1 : end].strip(), failure)
        require(mask[cursor + 1 : closer].strip() == "hidden", failure)
        return
    require(name == "recursion_limit", failure)
    tail = mask[cursor:end]
    require(tail.lstrip().startswith("="), failure)
    equals = cursor + len(tail) - len(tail.lstrip())
    _static_string_literal(attribute_source[equals + 1 : end], failure)


def _cfg_predicates(
    attribute_source: str,
    mask: str,
    attrs: list[_RustAttribute],
) -> tuple[tuple, ...]:
    trees: list[tuple] = []
    for attr in attrs:
        if attr.name == "cfg":
            require(attr.arguments is not None, "Rust cfg requires arguments")
            trees.append(
                _parse_cfg_expression(attribute_source[slice(*attr.arguments)])
            )
        elif attr.name == "cfg_attr":
            trees.extend(_conditional_cfg_trees(attribute_source, mask, attr))
    return tuple(trees)


def _predicates_possible_without_test(trees: tuple[tuple, ...]) -> bool:
    if not trees:
        return True

    atoms: set[str] = set()

    def collect(tree: tuple) -> None:
        if tree[0] == "atom":
            if tree[1] != "test":
                atoms.add(tree[1])
            return
        for child in tree[1]:
            collect(child)

    def evaluate(tree: tuple, values: dict[str, bool]) -> bool | None:
        kind, payload = tree
        if kind == "atom":
            return False if payload == "test" else values.get(payload)
        evaluated = [evaluate(child, values) for child in payload]
        if kind == "all":
            if False in evaluated:
                return False
            return True if all(value is True for value in evaluated) else None
        if kind == "any":
            if True in evaluated:
                return True
            return False if all(value is False for value in evaluated) else None
        return None if evaluated[0] is None else not evaluated[0]

    for tree in trees:
        collect(tree)
    require(
        len(atoms) <= 16,
        "Rust cfg expression exceeds the static production-proof bound",
    )
    names = sorted(atoms)

    def satisfiable(position: int, values: dict[str, bool]) -> bool:
        evaluated = [evaluate(tree, values) for tree in trees]
        if False in evaluated:
            return False
        if all(value is True for value in evaluated):
            return True
        if position == len(names):
            return False
        name = names[position]
        values[name] = True
        if satisfiable(position + 1, values):
            return True
        values[name] = False
        if satisfiable(position + 1, values):
            return True
        del values[name]
        return False

    return satisfiable(0, {})


def _attributes_before(
    attributes: list[_RustAttribute], mask: str, item_start: int
) -> list[_RustAttribute]:
    selected: list[_RustAttribute] = []
    cursor = item_start
    for attr in reversed(attributes):
        if attr.end > cursor:
            continue
        if mask[attr.end : cursor].strip() or attr.inner:
            break
        selected.append(attr)
        cursor = attr.start
    selected.reverse()
    return selected


def _static_string_literal(value: str, context: str) -> str:
    value = value.strip()
    normal = re.fullmatch(r'"([^"\\]*)"', value)
    raw = re.fullmatch(r'r(?P<hashes>#{0,255})"(?P<value>.*)"(?P=hashes)', value, re.S)
    require(
        normal is not None or raw is not None,
        f"{context} must be one static Rust string literal",
    )
    return normal.group(1) if normal is not None else raw.group("value")


def _path_override(comments_mask: str, attrs: list[_RustAttribute]) -> str | None:
    paths = [attr for attr in attrs if attr.name == "path"]
    require(len(paths) <= 1, "Rust module has multiple path attributes")
    if not paths:
        return None
    attr = paths[0]
    body = comments_mask[attr.start : attr.end]
    match = re.fullmatch(r"#\s*\[\s*path\s*=\s*(?P<value>.*?)\s*\]", body, re.S)
    require(match is not None, "unsupported Rust path attribute")
    return _static_string_literal(match.group("value"), "Rust path attribute")


def _resolve_module_child(
    path: Path,
    module_dir: Path,
    path_base: Path,
    name: str,
    attrs: list[_RustAttribute],
    comments_mask: str,
    predicates: tuple[tuple, ...],
) -> _RustSourceInput:
    explicit = _path_override(comments_mask, attrs)
    child_module_dir = module_dir / name
    if explicit is not None:
        return _RustSourceInput(path_base / explicit, child_module_dir, predicates)
    candidates = (
        module_dir / f"{name}.rs",
        child_module_dir / "mod.rs",
    )
    existing = tuple(candidate for candidate in candidates if candidate.is_file())
    require(
        len(existing) == 1,
        f"declared Rust module must resolve to exactly one file: {path}::{name}",
    )
    return _RustSourceInput(existing[0], child_module_dir, predicates)


def _delimiter_depths(mask: str) -> list[tuple[int, int, int]]:
    depths = [(0, 0, 0)] * (len(mask) + 1)
    braces = 0
    parens = 0
    brackets = 0
    for position, char in enumerate(mask):
        depths[position] = (braces, parens, brackets)
        if char == "{":
            braces += 1
        elif char == "}":
            braces -= 1
        elif char == "(":
            parens += 1
        elif char == ")":
            parens -= 1
        elif char == "[":
            brackets += 1
        elif char == "]":
            brackets -= 1
        require(
            min(braces, parens, brackets) >= 0,
            "unbalanced Rust module delimiters",
        )
    depths[len(mask)] = (braces, parens, brackets)
    require(depths[-1] == (0, 0, 0), "unbalanced Rust module delimiters")
    return depths


def _item_end(mask: str, start: int, limit: int) -> int:
    parens = 0
    brackets = 0
    position = start
    while position < limit:
        char = mask[position]
        if char == "(":
            parens += 1
        elif char == ")":
            parens -= 1
        elif char == "[":
            brackets += 1
        elif char == "]":
            brackets -= 1
        elif char == ";" and parens == 0 and brackets == 0:
            return position + 1
        elif char == "{" and parens == 0 and brackets == 0:
            header = mask[start:position]
            item_kind = re.search(
                r"\b(fn|struct|enum|union|impl|trait|mod|const|static|type|use|extern|macro_rules)\b",
                header,
            )
            macro_item = re.search(
                r"\b[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*!\s*$",
                header,
            )
            if item_kind is None and macro_item is not None:
                closer = _balanced_span_from(mask, position, "{", "}")
                return closer + 2 if mask[closer + 1 :].startswith(";") else closer + 1
            require(item_kind is not None, "unsupported cfg-disabled Rust item")
            semicolon_bound = item_kind.group(1) in {"const", "static", "type", "use"}
            if item_kind.group(1) == "const":
                after_const = header[item_kind.end() :]
                const_function = re.match(
                    r'\s+(?:(?:unsafe|async)\s+)*(?:extern(?:\s+"[^"]*")?\s+)?fn\b',
                    after_const,
                )
                semicolon_bound = const_function is None
            closer = _balanced_span_from(mask, position, "{", "}")
            if not semicolon_bound:
                return closer + 1
            position = closer
        position += 1
    require(False, "test-only Rust item has no terminator")
    return -1


def _include_path(
    path: Path,
    mask: str,
    comments_mask: str,
    opener: int,
) -> tuple[Path, int]:
    closer = _balanced_span_from(mask, opener, "(", ")")
    relative = _static_string_literal(
        comments_mask[opener + 1 : closer], f"include! path in {path}"
    )
    return path.parent / relative, closer + 1


def _leading_inner_attributes(
    attributes: list[_RustAttribute],
    mask: str,
    depths: list[tuple[int, int, int]],
    start: int,
    limit: int,
) -> list[_RustAttribute]:
    selected: list[_RustAttribute] = []
    cursor = start
    scope_depth = depths[start]
    for attr in attributes:
        if attr.start < start or attr.end > limit:
            continue
        if depths[attr.start] != scope_depth:
            continue
        if mask[cursor : attr.start].strip() or not attr.inner:
            break
        selected.append(attr)
        cursor = attr.end
    return selected


def _production_source_and_children(
    source_input: _RustSourceInput, source: str, include_tests: bool
) -> tuple[str, list[_RustSourceInput]]:
    path = source_input.path
    mask = _rust_code_mask(source)
    comments_mask = _rust_comments_mask(source)
    depths = _delimiter_depths(mask)
    attributes = _rust_attributes(mask)
    excluded: list[tuple[int, int]] = []
    children: list[_RustSourceInput] = []
    consumed_modules: set[int] = set()
    consumed_includes: set[int] = set()

    def visit_scope(
        start: int,
        limit: int,
        module_dir: Path,
        path_base: Path,
        inherited_predicates: tuple[tuple, ...],
    ) -> None:
        scope_depth = depths[start]
        inner = _leading_inner_attributes(attributes, mask, depths, start, limit)
        inner_predicates = (
            () if include_tests else _cfg_predicates(comments_mask, mask, inner)
        )
        scope_predicates = (*inherited_predicates, *inner_predicates)
        if not include_tests and not _predicates_possible_without_test(
            scope_predicates
        ):
            excluded.append((start, limit))
            return

        for attr in attributes:
            if attr.inner or attr.start < start or attr.end > limit:
                continue
            if depths[attr.start] != scope_depth:
                continue
            attrs = _attributes_before(attributes, mask, attr.end)
            item_predicates = (
                *scope_predicates,
                *_cfg_predicates(comments_mask, mask, attrs),
            )
            if not include_tests and not _predicates_possible_without_test(
                item_predicates
            ):
                excluded.append((attrs[0].start, _item_end(mask, attrs[-1].end, limit)))

        items: list[tuple[int, str, re.Match[str]]] = []
        modules: list[re.Match[str]] = []
        for declaration in _MODULE_ITEM.finditer(mask, start, limit):
            if depths[declaration.start()] != scope_depth:
                continue
            modules.append(declaration)
            items.append((declaration.start(), "module", declaration))
        for keyword in _MODULE_KEYWORD.finditer(mask, start, limit):
            if depths[keyword.start()] != scope_depth:
                continue
            require(
                any(
                    declaration.start() <= keyword.start() < declaration.end()
                    for declaration in modules
                ),
                "unsupported Rust module declaration",
            )
        for inclusion in _INCLUDE_ITEM.finditer(mask, start, limit):
            if depths[inclusion.start()] == scope_depth:
                items.append((inclusion.start(), "include", inclusion))

        for _, kind, item in sorted(items, key=lambda value: value[0]):
            attrs = _attributes_before(attributes, mask, item.start())
            item_predicates = (
                scope_predicates
                if include_tests
                else (
                    *scope_predicates,
                    *_cfg_predicates(comments_mask, mask, attrs),
                )
            )
            production = include_tests or _predicates_possible_without_test(
                item_predicates
            )
            if kind == "include":
                consumed_includes.add(item.start())
                opener = item.end() - 1
                closer = _balanced_span_from(mask, opener, "(", ")")
                if production:
                    include_path, _ = _include_path(path, mask, comments_mask, opener)
                    children.append(
                        _RustSourceInput(include_path, module_dir, item_predicates)
                    )
                else:
                    excluded.append((item.start(), closer + 1))
                continue

            declaration = item
            consumed_modules.add(declaration.start())
            name = declaration.group("name")
            if declaration.group("term") == ";":
                if production:
                    children.append(
                        _resolve_module_child(
                            path,
                            module_dir,
                            path_base,
                            name,
                            attrs,
                            comments_mask,
                            item_predicates,
                        )
                    )
                continue
            opener = declaration.start("term")
            closer = _balanced_span_from(mask, opener, "{", "}")
            if production:
                child_module_dir = module_dir / name
                visit_scope(
                    opener + 1,
                    closer,
                    child_module_dir,
                    child_module_dir,
                    item_predicates,
                )
            else:
                excluded.append((declaration.start(), closer + 1))

    visit_scope(
        0,
        len(source),
        source_input.module_dir,
        path.parent,
        source_input.predicates,
    )
    sanitized = list(source)
    for start, end in excluded:
        for position in range(start, end):
            if sanitized[position] != "\n":
                sanitized[position] = " "
    sanitized_source = "".join(sanitized)
    active_mask = _rust_code_mask(sanitized_source)

    # Rust permits item-producing macros and module items in block scope.  This
    # deliberately small module-tree interpreter only follows declarations at a
    # module's item scope.  Silently ignoring an active declaration in a
    # function/const/block would let contract-bearing source evade the scan, so
    # every active-looking declaration must have been consumed by visit_scope.
    # We fail closed rather than attempting to expand arbitrary Rust macros.
    active_modules = list(_MODULE_ITEM.finditer(active_mask))
    for declaration in active_modules:
        require(
            declaration.start() in consumed_modules,
            "compiler-active Rust module declaration was not consumed by the "
            f"module-tree traversal: {path}",
        )
    for keyword in _MODULE_KEYWORD.finditer(active_mask):
        require(
            any(
                declaration.start() in consumed_modules
                and declaration.start() <= keyword.start() < declaration.end()
                for declaration in active_modules
            ),
            "compiler-active Rust module declaration was not consumed by the "
            f"module-tree traversal: {path}",
        )
    for inclusion in _INCLUDE_ITEM.finditer(active_mask):
        require(
            inclusion.start() in consumed_includes,
            "compiler-active Rust include was not consumed by the module-tree "
            f"traversal: {path}",
        )
    return sanitized_source, children


def _visit_module_tree(
    source_input: _RustSourceInput,
    allowed_root: Path,
    include_tests: bool,
    loaded: set[tuple[Path, Path, tuple[tuple, ...]]],
    visiting: set[Path],
    sources: list[str],
    paths: set[Path],
) -> None:
    path = source_input.path.resolve()
    module_dir = source_input.module_dir.resolve()
    key = (path, module_dir, source_input.predicates)
    require(
        path.is_relative_to(allowed_root),
        f"Rust module path escapes root directory: {path}",
    )
    require(path.is_file(), f"missing declared Rust module: {path}")
    require(path not in visiting, f"cyclic Rust module declaration: {path}")
    if key in loaded:
        return
    visiting.add(path)
    loaded.add(key)
    paths.add(path)
    source = path.read_text(encoding="utf-8")
    production_source, children = _production_source_and_children(
        _RustSourceInput(path, module_dir, source_input.predicates),
        source,
        include_tests,
    )
    sources.append(production_source)
    for child_input in children:
        _visit_module_tree(
            child_input,
            allowed_root,
            include_tests,
            loaded,
            visiting,
            sources,
            paths,
        )
    visiting.remove(path)


def _load_module_tree(
    relative: str, *, root_dir: Path, include_tests: bool = False
) -> tuple[str, set[Path], Path]:
    """Load a Rust module tree and return its source, files, and module directory."""

    allowed_root = root_dir.resolve()
    root = (allowed_root / relative).resolve()
    require(root.is_file(), f"missing Rust facade or module tree: {relative}")
    root_module_dir = (
        root.parent
        if root.name in {"mod.rs", "lib.rs", "main.rs"}
        else root.with_suffix("")
    )
    loaded: set[tuple[Path, Path, tuple[tuple, ...]]] = set()
    visiting: set[Path] = set()
    sources: list[str] = []
    paths: set[Path] = set()
    _visit_module_tree(
        _RustSourceInput(root, root_module_dir, ()),
        allowed_root,
        include_tests,
        loaded,
        visiting,
        sources,
        paths,
    )
    return "\n".join(sources), paths, root_module_dir.resolve()


def read_module_tree(
    relative: str, *, root_dir: Path, include_tests: bool = False
) -> str:
    """Read a Rust module and its compiler-declared source tree recursively.

    Rust supports both conventional ``foo/bar.rs`` children and ``#[path]``
    files beside a root. Inline modules and literal ``include!`` inputs are
    traversed too. Production mode evaluates ``cfg`` for satisfiability with
    ``test=false`` and blanks test-only items before assertions can inspect them.
    Missing or unsupported declared inputs fail closed.
    """

    source, _, _ = _load_module_tree(
        relative, root_dir=root_dir, include_tests=include_tests
    )
    return source


def read_module_paths(
    relative: str, root_dir: Path, *, include_tests: bool = True
) -> set[Path]:
    """Return the compiler-declared source closure without sibling orphan checks.

    This narrower API is suitable for materializing a staged root such as
    ``src/lib.rs`` whose containing directory owns unrelated Rust roots. Every
    returned path is resolved and declarations still fail closed.
    """

    _, paths, _ = _load_module_tree(
        relative, root_dir=root_dir, include_tests=include_tests
    )
    return paths


def read_compiler_family(relative: str, root_dir: Path) -> _RustModuleFamily:
    """Load separate production/test views and reject undeclared child files.

    The compiler discovers children from ``mod``/``include!`` declarations, not
    from a scanner-maintained filename list.  The test-inclusive walk recognizes
    every declared child regardless of ``cfg(test)``; any conventional ``*.rs``
    below the root module directory that it did not visit is therefore an orphan
    and fails closed instead of silently escaping the contract proof.
    """

    production, production_paths, module_dir = _load_module_tree(
        relative, root_dir=root_dir
    )
    with_tests, all_paths, test_module_dir = _load_module_tree(
        relative, root_dir=root_dir, include_tests=True
    )
    require(module_dir == test_module_dir, "Rust module root changed between views")
    require(
        production_paths <= all_paths,
        f"production Rust module is absent from test-inclusive tree: {relative}",
    )
    if module_dir.is_dir():
        discovered = {path.resolve() for path in module_dir.rglob("*.rs")}
        orphans = discovered - all_paths
        require(
            not orphans,
            "orphan Rust module files are not compiler-reachable from "
            f"{relative}: {[str(path) for path in sorted(orphans)]}",
        )
    return _RustModuleFamily(production, with_tests, production_paths, all_paths)
