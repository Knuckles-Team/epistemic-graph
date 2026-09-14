#!/usr/bin/env python3
"""Fail closed when a published contract method has no dispatch path.

Why this exists
---------------
``contract/methods.json`` is the wire promise: every method in it is something
a client may legally send.  The server answers a method it does not recognise
from ONE terminal arm in ``handlers/graph_ops/terminal.rs`` -- "Method not
available in this server build".  A method that reaches that arm is published,
documented, generated into the Python client, counted in the policy ledger, and
answers every caller with an error.  Nothing in the unit-test suite notices: the
handler it was supposed to reach is tested directly and passes.

The properties
--------------
1.  **Catalog/enum agreement.**  Every contract method id is a variant of the
    wire ``Method`` enum, and every variant is in the catalog.  A catalog entry
    with no variant cannot be sent at all; a variant with no entry is served
    without a published policy.

2.  **Dispatch reachability.**  Every contract method is matched in PATTERN
    position inside a function that answers with a ``Response`` -- that is, in
    a dispatch arm and not merely in a policy classifier.  A method matched
    nowhere in a response-producing function falls through to the terminal
    catch-all and cannot be served.

    Pattern position is established structurally (a ``Method::X`` occurrence
    followed, at the same delimiter depth, by ``=>`` with only a destructuring
    group, alternative patterns or a guard in between), so a CONSTRUCTION of
    the same variant -- building a receipt, a replay record, a sanitized
    method -- is not mistaken for an arm.

    "Answers with a ``Response``" is read off the enclosing function's return
    type.  That is what separates a router arm from ``requires_write`` or
    ``is_admin_authz_action``, which match every variant exhaustively and
    dispatch none of them.  Keying on file names instead would break the first
    time a handler moved.

3.  **Refusal stubs are internal-only.**  A dispatch arm that answers with an
    error and calls no handler is a REFUSAL: the catalog's way of freezing a
    wire shape before its implementation lands.  That is legitimate only while
    the method is ``Stability::Internal`` with no consumer profile.  A method
    published as ``stable``, generated into the client, whose every arm is a
    refusal, is a promise the server does not keep.

4.  **Digest round-trip (agent/delegation family).**  A validator no legal
    stored value can satisfy is a dispatch path that exists and can never
    succeed -- exactly the ``AgentGraphEntryRef::validate`` defect, where the
    reference required a bare 64-hex digest while every producer in the
    contract emits ``sha256:<hex>``.  Every unit test passed, because the tests
    built the value the validator wanted rather than the value the store holds.

    So: classify each digest validator by the form it ACCEPTS, classify each
    digest producer by the form it EMITS, and require that a field validated by
    one is fed by the other.  Scope is stated explicitly in the receipt: the
    agent library / component / graph / template contracts and delegation.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from rust_lexer import (
    _balanced_span_from,
    _delimiter_depths,
    _rust_code_mask,
    _rust_comments_mask,
    _top_level_parts,
)
from rust_module_tree import read_compiler_family, read_module_paths, read_module_tree

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "eg-contract-method-reachability-gate/v1"

CONTRACT = "contract/methods.json"
PROTOCOL = "crates/eg-types/src/protocol.rs"
SERVER_ROOT = "src/server/mod.rs"
TERMINAL_CATCH_ALL = "src/server/handlers/graph_ops/terminal.rs"

# The round-trip check's declared scope.  Anything outside it is NOT covered,
# and the receipt says so rather than implying whole-contract coverage.
DIGEST_SCOPE = (
    "crates/eg-types/src/agent_library.rs",
    "crates/eg-types/src/agent_component.rs",
    "crates/eg-types/src/agent_graph.rs",
    "crates/eg-types/src/agent_template.rs",
    "crates/eg-types/src/delegation.rs",
)

_METHOD_USE = re.compile(r"\bMethod::(?P<name>[A-Za-z_][A-Za-z0-9_]*)")
_FN_HEADER = re.compile(
    r"(?m)^(?P<indent>[ \t]*)(?:pub(?:\s*\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?"
    r"(?:unsafe\s+)?(?:extern\s+\"[^\"]*\"\s+)?fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
)
_HANDLER_CALL = re.compile(
    r"\b(?:handle|dispatch|try_handle|route|authorize_and_route|serve|run)"
    r"[A-Za-z0-9_]*\s*\("
)
_DIGEST_FIELD = re.compile(
    r"(?m)^\s*(?:pub\s+)?(?P<field>[A-Za-z_][A-Za-z0-9_]*digest)\s*:\s*String\s*,"
)
_CALL = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_VALIDATE_CALL = re.compile(
    r"\b(?P<fn>[A-Za-z_][A-Za-z0-9_]*)\(\s*\"(?P<field>[A-Za-z0-9_]+)\"\s*,"
)


class GateError(RuntimeError):
    """The gate could not establish its universe and must not report green."""


@dataclass(frozen=True)
class Arm:
    method: str
    path: str
    function: str
    returns: str
    refusal: bool
    answers: bool


def read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        raise GateError(f"required source is absent: {relative}")
    return path.read_text(encoding="utf-8")


def contract_methods() -> list[dict]:
    document = json.loads(read(CONTRACT))
    methods = document.get("methods")
    if not isinstance(methods, list) or not methods:
        raise GateError("contract/methods.json declares no methods")
    declared = document.get("method_count")
    if declared != len(methods):
        raise GateError(
            f"contract method_count {declared} disagrees with {len(methods)} entries"
        )
    return methods


def protocol_source() -> str:
    """Read the compiler-declared production protocol family.

    ``protocol.rs`` is a facade.  The wire ``Method`` enum is assembled from
    the declared ``protocol/method`` children, so reading only the facade can
    never establish the wire universe.  The family reader follows those
    declarations and rejects an unlinked Rust child instead of silently
    allowing a method fragment to escape this gate.
    """

    try:
        return read_compiler_family(PROTOCOL, ROOT).production
    except SystemExit as error:
        raise GateError(str(error)) from error


def _method_enum_body(source: str) -> str:
    """Return the literal ``Method`` enum body when a tree has one."""

    marker = "pub enum Method {"
    start = source.find(marker)
    if start < 0:
        raise GateError("the wire Method enum is absent from the protocol module")
    body = source[start + len(marker) :]
    mask = _rust_code_mask(body)
    try:
        end = _balanced_span_from("{" + mask, 0, "{", "}") - 1
    except SystemExit as error:
        raise GateError("the wire Method enum is unterminated") from error
    return mask[:end]


def _method_chunk_bodies(source: str) -> list[str]:
    """Return only compiler-reachable declarative Method fragments."""

    chunk_bodies: list[str] = []
    for chunk in re.finditer(r"macro_rules!\s+__eg_method_chunk_\d+\s*\{", source):
        end = source.find("pub(crate) use __eg_method_chunk_", chunk.end())
        if end < 0:
            raise GateError("protocol Method chunk is unterminated or not re-exported")
        chunk_bodies.append(source[chunk.end() : end])
    if not chunk_bodies:
        raise GateError("protocol Method enum has no readable variant body")
    return chunk_bodies


def _method_variant_names(source: str) -> set[str]:
    # The fragment body comes from a macro rule, where a comment or string can
    # contain text that looks like an enum row.  Apply the shared Rust lexer
    # before matching so only compiler-visible tokens contribute variants.
    source = _rust_code_mask(source)
    return set(
        re.findall(
            r"^    ([A-Z][A-Za-z0-9_]*)\s*(?:\{|\(|,)",
            source,
            re.MULTILINE,
        )
    )


def protocol_variants() -> set[str]:
    """Top-level variant identifiers of the compiler-reachable ``Method``."""

    source = protocol_source()
    variants = _method_variant_names(_method_enum_body(source))
    if not variants:
        variants = _method_variant_names("\n".join(_method_chunk_bodies(source)))
    if not variants:
        raise GateError("no variant parsed out of the wire Method enum")
    return variants


_SPAN_CACHE: dict[str, list[tuple[int, int, str, str]]] = {}


def _function_spans(mask: str) -> list[tuple[int, int, str, str]]:
    """(start, end, name, return type) for every `fn` in one file."""

    cached = _SPAN_CACHE.get(mask)
    if cached is not None:
        return cached
    spans: list[tuple[int, int, str, str]] = []
    for header in _FN_HEADER.finditer(mask):
        opener = mask.find("{", header.end())
        arrow = mask.find("->", header.end())
        signature_end = opener if opener >= 0 else len(mask)
        returns = (
            mask[arrow + 2 : signature_end].strip()
            if 0 <= arrow < signature_end
            else "()"
        )
        if opener < 0:
            continue
        try:
            closer = _balanced_span_from(mask, opener, "{", "}")
        except SystemExit:
            continue
        spans.append((header.start(), closer + 1, header.group("name"), returns))
    _SPAN_CACHE[mask] = spans
    return spans


def _enclosing(
    spans: list[tuple[int, int, str, str]], position: int
) -> tuple[str, str]:
    best: tuple[str, str] = ("<file scope>", "()")
    width = None
    for start, end, name, returns in spans:
        if start <= position < end and (width is None or end - start < width):
            best, width = (name, returns), end - start
    return best


def _outside(position: tuple, scope: tuple) -> bool:
    """True when `position` is shallower than `scope` in some delimiter kind.

    Depths are a (brace, paren, bracket) triple.  Python compares tuples
    lexicographically, which is NOT the question being asked here: a closing
    parenthesis inside a match arm is `(1, 2, 0)` against an arm scope of
    `(1, 1, 0)` and must read as INSIDE.  Comparing componentwise is the whole
    point of keeping three counters.
    """

    return any(value < bound for value, bound in zip(position, scope, strict=True))


def _brace_openers(mask: str) -> dict[int, int]:
    """closer -> opener for every brace pair in one file."""

    stack: list[int] = []
    pairs: dict[int, int] = {}
    for index, character in enumerate(mask):
        if character == "{":
            stack.append(index)
        elif character == "}" and stack:
            pairs[index] = stack.pop()
    return pairs


def _arm_start(mask: str, depths: list, openers: dict, arrow: int) -> int:
    """Where the match arm ending at `arrow` begins.

    Walks back to the previous arm boundary.  Three things end the previous
    arm: leaving the match block, a `,`/`;`/`=>` at the arm's own depth, and a
    block-bodied arm's closing brace (a block-bodied arm may omit its trailing
    comma).  That last case is identified by its OPENER following a `=>`,
    which is what tells it apart from a destructuring pattern's brace --
    telling them apart by what FOLLOWS the `}` was wrong for
    `Method::A { .. } | Method::B { .. } =>`, where the first pattern's brace
    is followed by `|`.
    """

    depth = depths[arrow]
    for cursor in range(arrow - 1, -1, -1):
        if _outside(depths[cursor], depth):
            return cursor + 1
        if depths[cursor] == depth and (
            mask[cursor] in ",;" or mask.startswith("=>", cursor - 1)
        ):
            return cursor + 1
        opener = openers.get(cursor)
        if opener is not None and mask[:opener].rstrip().endswith("=>"):
            return cursor + 1
    return 0


def _match_arm_spans(mask: str, depths: list) -> list[tuple[int, int, int, str]]:
    """One span per match arm PATTERN, with any guard cut off.

    Returning SPANS rather than testing each occurrence is what makes the
    binding and alternation forms come out right: `method @ (Method::A { .. }
    | Method::B { .. }) => method` is ONE pattern span holding two variants.
    An earlier attempt walked forward from each variant looking for `=>` at
    that variant's own delimiter depth and missed every one of them, because
    the `=>` sits outside the grouping parentheses -- twenty-four live methods
    were reported unreachable.
    """

    openers = _brace_openers(mask)
    spans: list[tuple[int, int, int, str]] = []
    for arrow in range(len(mask) - 1):
        if not mask.startswith("=>", arrow):
            continue
        start = _arm_start(mask, depths, openers, arrow)
        body = mask[start:arrow]
        guard = next(
            (
                match
                for match in re.finditer(r"\bif\b", body)
                if depths[start + match.start()] == depths[arrow]
            ),
            None,
        )
        spans.append(
            (start, start + (guard.start() if guard else len(body)), arrow, "arm")
        )
    return spans


def _binding_spans(mask: str, depths: list) -> list[tuple[int, int, int, str]]:
    """`let` / `if let` / `while let` / `let .. else`, up to the pattern's `=`."""

    spans: list[tuple[int, int, int, str]] = []
    for binding in re.finditer(r"\b(?:if\s+let|while\s+let|let)\b", mask):
        depth = depths[binding.end() - 1]
        cursor = binding.end()
        while cursor < len(mask) and not (
            depths[cursor] == depth
            and (
                mask[cursor] == ";"
                or (mask[cursor] == "=" and not mask.startswith("==", cursor))
            )
        ):
            cursor += 1
        spans.append((binding.end(), cursor, cursor, "binding"))
    return spans


def _matches_macro_spans(mask: str) -> list[tuple[int, int, int, str]]:
    """`matches!(value, PATTERN)` -- everything after the first argument."""

    spans: list[tuple[int, int, int, str]] = []
    for macro in re.finditer(r"\bmatches!\s*\(", mask):
        opener = macro.end() - 1
        try:
            closer = _balanced_span_from(mask, opener, "(", ")")
        except SystemExit:
            continue
        parts = _top_level_parts(mask, opener + 1, closer)
        if len(parts) >= 2:
            spans.append((parts[1][0], closer, closer, "matches"))
    return spans


def _pattern_spans(mask: str, depths: list) -> list[tuple[int, int, int, str]]:
    """Every region of `mask` in which an identifier is a PATTERN, not a value.

    Rust writes patterns in the places this tree uses, and none of them can be
    confused with a constructor call: a match arm, a `let`-family binding, and
    the second and later arguments of `matches!`.
    """

    return (
        _match_arm_spans(mask, depths)
        + _binding_spans(mask, depths)
        + _matches_macro_spans(mask)
    )


def _arm_body(mask: str, depths: list, arrow: int) -> str:
    depth = depths[arrow]
    cursor = arrow + 2
    while cursor < len(mask) and mask[cursor].isspace():
        cursor += 1
    if cursor < len(mask) and mask[cursor] == "{":
        try:
            return mask[cursor : _balanced_span_from(mask, cursor, "{", "}") + 1]
        except SystemExit:
            return mask[cursor:]
    end = cursor
    while end < len(mask):
        if depths[end] < depth:
            break
        if depths[end] == depth and mask[end] == ",":
            break
        end += 1
    return mask[cursor:end]


def _dispatch_universe() -> list[Path]:
    paths = sorted(read_module_paths(SERVER_ROOT, ROOT, include_tests=False))
    if not paths:
        raise GateError("the server module tree resolved to no source")
    if not any(path.as_posix().endswith(TERMINAL_CATCH_ALL) for path in paths):
        raise GateError(
            "the terminal unknown-method arm is outside the scanned dispatch tree: "
            "the gate cannot prove what falls through to it"
        )
    return paths


def _is_refusal(mask: str, depths: list, enclosing: list, answers: bool) -> bool:
    """A dispatch arm that answers with an error and invokes no handler."""

    arm_spans = [span for span in enclosing if span[3] == "arm"]
    if not answers or not arm_spans:
        return False
    body = _arm_body(mask, depths, min(span[2] for span in arm_spans))
    return (
        "Response::err" in body
        and "Response::ok" not in body
        and not _HANDLER_CALL.search(body)
    )


def _file_arms(relative: str, mask: str) -> tuple[list[Arm], set[str]]:
    """The dispatch arms one file declares, and the calls its routers make."""

    depths = _delimiter_depths(mask)
    spans = _function_spans(mask)
    routing_calls: set[str] = set()
    for start, end, _name, returns in spans:
        if "Response" in returns:
            routing_calls.update(_CALL.findall(mask[start:end]))
    patterns = _pattern_spans(mask, depths)
    arms: list[Arm] = []
    for use in _METHOD_USE.finditer(mask):
        enclosing = [span for span in patterns if span[0] <= use.start() < span[1]]
        if not enclosing:
            continue
        function, returns = _enclosing(spans, use.start())
        answers = "Response" in returns
        if not answers and returns.strip() != "bool":
            continue
        arms.append(
            Arm(
                use.group("name"),
                relative,
                function,
                returns.strip(),
                _is_refusal(mask, depths, enclosing, answers),
                answers,
            )
        )
    return arms, routing_calls


def dispatch_arms() -> tuple[list[Arm], set[str]]:
    """Every `Method::X` written in pattern position in the dispatch tree."""

    arms: list[Arm] = []
    routing_calls: set[str] = set()
    for path in _dispatch_universe():
        file_arms, calls = _file_arms(
            path.relative_to(ROOT).as_posix(),
            _rust_code_mask(path.read_text(encoding="utf-8")),
        )
        arms.extend(file_arms)
        routing_calls |= calls
    return arms, routing_calls


# --------------------------------------------------------------------------
# Property 4 -- digest round-trip
# --------------------------------------------------------------------------

_PREFIX_CONSTANT = re.compile(
    r'const\s+DIGEST_PREFIX\s*:\s*&str\s*=\s*"(?P<value>[^"]*)"'
)
_STRUCT_DECL = re.compile(r"\bstruct\s+(?P<name>[A-Z][A-Za-z0-9_]*)\s*\{")
_IMPL_BLOCK = re.compile(r"\bimpl\s+(?P<name>[A-Z][A-Za-z0-9_]*)\s*\{")
_LITERAL = re.compile(r"\b(?P<name>Self|[A-Z][A-Za-z0-9_]*)\s*\{")

BARE = "bare 64-hex"
PREFIXED = "sha256:<64 hex>"


def _declared_form(body: str) -> str | None:
    """What one function does with a digest, read off its body.

    A validator ACCEPTS the prefixed form when it (or a helper it calls) strips
    the digest prefix, and the bare form when it measures 64 hex characters
    without stripping anything.  A producer EMITS the prefixed form when it
    formats the prefix constant in front of the hex encoding.
    """

    if 'format!("{DIGEST_PREFIX}' in body or 'format!("sha256:' in body:
        return f"produces:{PREFIXED}"
    if "strip_prefix(DIGEST_PREFIX)" in body or 'strip_prefix("sha256:")' in body:
        return f"accepts:{PREFIXED}"
    if "== 64" in body or "!= 64" in body:
        return f"accepts:{BARE}"
    return None


def _inherited_form(
    name: str, body: str, bodies: dict[str, str], forms: dict[str, str]
) -> str | None:
    """A delegating validator takes the form of the one helper it calls."""

    inherited = {
        forms.get(inner)
        for inner in bodies
        if inner != name and re.search(rf"\b{re.escape(inner)}\s*\(", body)
    } - {None}
    return inherited.pop() if len(inherited) == 1 else None


def _digest_forms(mask: str, text: str) -> dict[str, str]:
    """Classify each digest validator/producer function in one contract module."""

    spans = _function_spans(mask)
    bodies = {name: text[start:end] for start, end, name, _returns in spans}
    forms = {
        name: form
        for name, body in bodies.items()
        if (form := _declared_form(body)) is not None
    }
    for _ in range(4):
        for name, body in bodies.items():
            if name not in forms and (
                form := _inherited_form(name, body, bodies, forms)
            ):
                forms[name] = form
    return forms


def _balanced(mask: str, opener: int, pair: str) -> int | None:
    try:
        return _balanced_span_from(mask, opener, pair[0], pair[1])
    except SystemExit:
        return None


# Keyed by the source text itself: `id()` is reused after a string is
# collected, so an id-keyed cache silently answers for the wrong file.
_IMPL_CACHE: dict[str, list[tuple[int, int, str]]] = {}


def _impl_blocks(mask: str) -> list[tuple[int, int, str]]:
    cached = _IMPL_CACHE.get(mask)
    if cached is None:
        cached = []
        for block in _IMPL_BLOCK.finditer(mask):
            opener = block.end() - 1
            closer = _balanced(mask, opener, "{}")
            if closer is not None:
                cached.append((opener, closer, block.group("name")))
        _IMPL_CACHE[mask] = cached
    return cached


def _enclosing_impl(mask: str, position: int) -> str | None:
    best, width = None, None
    for opener, closer, name in _impl_blocks(mask):
        if not opener < position < closer:
            continue
        if width is None or closer - opener < width:
            best, width = name, closer - opener
    return best


def _validated_digest_fields(mask: str, text: str, forms: dict[str, str]) -> dict:
    """(struct, field) -> the digest form its `validate` accepts."""

    accepted: dict[tuple[str, str], str] = {}
    for call in _VALIDATE_CALL.finditer(text):
        form = forms.get(call.group("fn"), "")
        if not form.startswith("accepts:") or not call.group("field").endswith(
            "digest"
        ):
            continue
        owner = _enclosing_impl(mask, call.start())
        if owner:
            accepted[(owner, call.group("field"))] = form.split(":", 1)[1]
    return accepted


_PARAMETER = re.compile(
    r"\b(?P<name>[a-z_][A-Za-z0-9_]*)\s*:\s*&?(?:mut\s+)?"
    r"(?:Box\s*<\s*)?(?P<type>[A-Z][A-Za-z0-9_]*)"
)
_FIELD_READ = re.compile(
    r"&?\s*(?P<binding>[a-z_][A-Za-z0-9_]*)\s*\.\s*(?P<field>[A-Za-z_][A-Za-z0-9_]*digest)\b"
)


def _parameter_types(mask: str, spans: list, position: int) -> dict[str, str]:
    """binding -> declared type, for the parameters of the enclosing fn.

    A `from_entry(entry: &AgentGraphEntry)` that copies `entry.definition_digest`
    is carrying THAT type's stored form into the reference it builds.  Reading
    the parameter list is what makes the copy's provenance exact instead of a
    guess from the field's name.
    """

    best, width = None, None
    for start, end, _name, _returns in spans:
        if start <= position < end and (width is None or end - start < width):
            best, width = (start, end), end - start
    if best is None:
        return {}
    opener = mask.find("(", best[0])
    closer = _balanced(mask, opener, "()") if opener >= 0 else None
    if closer is None:
        return {}
    return {
        match.group("name"): match.group("type")
        for match in _PARAMETER.finditer(mask[opener + 1 : closer])
    }


def _local_binding(
    mask: str, text: str, spans: list, position: int, name: str
) -> str | None:
    """The initializer of `let <name> = ..;` in the function enclosing `position`."""

    best, width = None, None
    for start, end, _fn, _returns in spans:
        if start <= position < end and (width is None or end - start < width):
            best, width = (start, end), end - start
    if best is None:
        return None
    binding = re.search(
        rf"\blet\s+(?:mut\s+)?{re.escape(name)}\s*(?::[^=;]*)?=",
        mask[best[0] : best[1]],
    )
    if binding is None:
        return None
    cursor = best[0] + binding.end()
    end = mask.find(";", cursor)
    return text[cursor : end if end >= 0 else best[1]]


def _assigned_field(mask: str, text: str, spans: list, start: int, end: int):
    """(field, value expression) for one struct-literal entry, or None."""

    entry = text[start:end]
    match = re.match(
        r"\s*(?P<field>[A-Za-z_][A-Za-z0-9_]*)\s*:(?P<value>.*)", entry, re.S
    )
    if match is not None:
        if not match.group("field").endswith("digest"):
            return None
        return match.group("field"), match.group("value")
    # Field-init shorthand: `AgentGraphEntry { definition_digest, .. }` carries
    # the form of the local binding of the same name, which is where the
    # entry's digest is actually computed.
    shorthand = re.fullmatch(r"\s*(?P<field>[A-Za-z_][A-Za-z0-9_]*digest)\s*", entry)
    if shorthand is None:
        return None
    field = shorthand.group("field")
    value = _local_binding(mask, text, spans, start, field)
    return None if value is None else (field, value)


_BARE_ADAPTERS = (" unprefixed_digest(", ".strip_prefix(")
_PREFIXED_LITERALS = ('format!("{DIGEST_PREFIX}', 'format!("sha256:')


def _literal_form(value: str) -> str | None:
    """The form an expression states outright: an adapter or a formatted prefix."""

    if any(marker in f" {value}" for marker in _BARE_ADAPTERS):
        return BARE
    if any(marker in value for marker in _PREFIXED_LITERALS):
        return PREFIXED
    return None


def _copied_form(
    value: str, authoritative: dict, parameters: dict[str, str]
) -> str | None:
    """The form a COPY carries, resolved through the parameter it reads from."""

    read = {
        authoritative[(parameters[copy.group("binding")], copy.group("field"))]
        for copy in _FIELD_READ.finditer(value)
        if (parameters.get(copy.group("binding")), copy.group("field")) in authoritative
    }
    return read.pop() if len(read) == 1 else None


def _value_form(
    value: str,
    forms: dict[str, str],
    authoritative: dict,
    parameters: dict[str, str],
) -> str | None:
    """The digest form one assigned expression carries, or None if unknown."""

    stated = _literal_form(value)
    if stated is not None:
        return stated
    emitted = {
        forms[fn].split(":", 1)[1]
        for fn in forms
        if forms[fn].startswith("produces:")
        and re.search(rf"\b{re.escape(fn)}\s*\(", value)
    }
    if len(emitted) == 1:
        return emitted.pop()
    return _copied_form(value, authoritative, parameters)


def _literal_owner(mask: str, literal: re.Match[str], relevant: set[str]) -> str | None:
    name = literal.group("name")
    if name != "Self":
        return name if name in relevant else None
    owner = _enclosing_impl(mask, literal.start())
    return owner if owner in relevant else None


def _construction_forms(
    mask: str,
    text: str,
    forms: dict[str, str],
    authoritative: dict,
    relevant: set[str],
) -> dict:
    """(struct, field) -> the digest forms production code actually assigns."""

    produced: dict[tuple[str, str], set[str]] = {}
    spans = _function_spans(mask)
    for literal in _LITERAL.finditer(mask):
        owner = _literal_owner(mask, literal, relevant)
        if owner is None:
            continue
        opener = literal.end() - 1
        closer = _balanced(mask, opener, "{}")
        if closer is None:
            continue
        for start, end in _top_level_parts(mask, opener + 1, closer):
            assigned = _assigned_field(mask, text, spans, start, end)
            if assigned is None:
                continue
            field, value = assigned
            form = _value_form(
                value, forms, authoritative, _parameter_types(mask, spans, start)
            )
            if form is not None:
                produced.setdefault((owner, field), set()).add(form)
    return produced


def _digest_structs(mask: str, text: str) -> set[str]:
    """Contract types that STORE a digest.

    Construction scanning is restricted to these, so the server tree is walked
    for the types that matter rather than for every struct literal in it.
    """

    names: set[str] = set()
    for declaration in _STRUCT_DECL.finditer(mask):
        opener = declaration.end() - 1
        closer = _balanced(mask, opener, "{}")
        if closer is not None and _DIGEST_FIELD.search(text[opener:closer]):
            names.add(declaration.group("name"))
    return names


def _contract_digest_model() -> tuple[dict, dict, list, set[str]]:
    """The contract's digest validators, producers, and digest-bearing types."""

    forms: dict[str, str] = {}
    accepted: dict[tuple[str, str], str] = {}
    sources: list[tuple[str, str, str]] = []
    structs: set[str] = set()
    for relative in DIGEST_SCOPE:
        source = read_module_tree(relative, root_dir=ROOT)
        mask = _rust_code_mask(source)
        text = _rust_comments_mask(source)
        if _PREFIX_CONSTANT.search(text) is None and "sha256:" not in text:
            raise GateError(f"no digest form is declared in {relative}")
        module_forms = _digest_forms(mask, text)
        if not module_forms:
            raise GateError(f"no digest validator or producer parsed in {relative}")
        forms.update(module_forms)
        accepted.update(_validated_digest_fields(mask, text, module_forms))
        sources.append((relative, mask, text))
        structs |= _digest_structs(mask, text)
    return forms, accepted, sources, structs


def digest_round_trip() -> list[dict]:
    """Digest fields whose validator can never accept what production stores.

    The `AgentGraphEntryRef::validate` defect in full: the reference required a
    bare 64-character hex digest, `from_entry` copied the stored
    `definition_digest`, and every producer in the contract emits
    `sha256:<hex>`.  No legal stored value could satisfy the validator, so the
    graph arm of `kg-delegate` could not admit anything -- and every unit test
    passed, because each one built the value the validator wanted.
    """

    forms, accepted, sources, structs = _contract_digest_model()

    # PRODUCTION source only.  A `#[cfg(test)]` fixture builds whatever value
    # its own assertion wants -- that is exactly how the defect survived every
    # unit test -- so a test construction is not evidence about the store.
    server = read_module_tree(SERVER_ROOT, root_dir=ROOT)
    consumers = sources + [
        (SERVER_ROOT, _rust_code_mask(server), _rust_comments_mask(server))
    ]

    # The authoritative form of a STORED field, keyed by the type that owns it.
    # Keying by field name alone made the four layers' identically named
    # `definition_digest` fields one concept and pointed a proven finding at
    # the wrong type.
    #
    # Two passes.  The first classifies only DIRECT evidence -- a formatted
    # prefix, an `unprefixed_digest` adapter, a local binding computed by a
    # known producer -- which establishes the stored form of a type the
    # contract never validates, such as `AgentGraphEntry`.  The second pass can
    # then resolve a COPY (`entry.definition_digest.clone()`) through the
    # enclosing function's parameter types.
    authoritative = dict(accepted)
    for _relative, mask, text in consumers:
        for key, values in _construction_forms(mask, text, forms, {}, structs).items():
            if key not in authoritative and len(values) == 1:
                authoritative[key] = next(iter(values))
    produced: dict[tuple[str, str], set[str]] = {}
    for _relative, mask, text in consumers:
        for key, values in _construction_forms(
            mask, text, forms, authoritative, structs
        ).items():
            produced.setdefault(key, set()).update(values)

    return [
        {
            "type": owner,
            "field": field,
            "accepts": accepts,
            "production_assigns": sorted(produced[(owner, field)]),
            "consequence": (
                "no value production code stores can satisfy the validator: "
                "the dispatch path exists and can never admit anything"
            ),
        }
        for (owner, field), accepts in sorted(accepted.items())
        if produced.get((owner, field)) and accepts not in produced[(owner, field)]
    ]


def _routed_arms(arms: list[Arm], routing_calls: set[str]) -> dict[str, list[Arm]]:
    """Group the arms that actually route, by method.

    A method may be routed by a `-> bool` CLASSIFIER rather than by its own arm
    -- `is_resource_reservation_method` gates a whole family into the native
    store route.  That is a real dispatch path, so a classifier counts when a
    Response-producing function in the dispatch tree calls it.

    Limitation, stated rather than hidden: an access-policy predicate that a
    router also calls (`requires_write`) satisfies this test too, so a method
    named ONLY by such a predicate would be over-credited.  The receipt lists
    every classifier-only method with the classifier that credited it, so the
    evidence is auditable instead of implicit.
    """

    by_method: dict[str, list[Arm]] = {}
    for arm in arms:
        if arm.answers or arm.function in routing_calls:
            by_method.setdefault(arm.method, []).append(arm)
    return by_method


def _is_frozen_shape(entry: dict) -> bool:
    """A method the catalog froze the wire shape of before implementing it."""

    return entry["stability"] == "internal" and not entry["consumer_profiles"]


def _refusal_stubs(catalog: dict, by_method: dict[str, list[Arm]]) -> tuple[list, list]:
    """Published refusal stubs, and the legitimate internal ones."""

    refusal_only = sorted(
        name
        for name, arms in by_method.items()
        if name in catalog and all(arm.refusal for arm in arms)
    )
    internal = [name for name in refusal_only if _is_frozen_shape(catalog[name])]
    published = [
        {
            "method": name,
            "stability": catalog[name]["stability"],
            "consumer_profiles": catalog[name]["consumer_profiles"],
            "arms": [f"{arm.path}::{arm.function}" for arm in by_method[name]],
        }
        for name in refusal_only
        if name not in internal
    ]
    return published, internal


def run_gate() -> dict:
    catalog = {method["id"]: method for method in contract_methods()}
    variants = protocol_variants()
    arms, routing_calls = dispatch_arms()
    by_method = _routed_arms(arms, routing_calls)
    published_refusals, internal_refusals = _refusal_stubs(catalog, by_method)

    return {
        "schema": SCHEMA,
        "contract_methods": len(catalog),
        "wire_variants": len(variants),
        "dispatch_arms": sum(len(value) for value in by_method.values()),
        "classifier_routed_methods": {
            name: sorted({f"{arm.path}::{arm.function}" for arm in method_arms})
            for name, method_arms in by_method.items()
            if not any(arm.answers for arm in method_arms)
        },
        "dispatched_methods": len(set(by_method) & set(catalog)),
        "catalog_without_wire_variant": sorted(set(catalog) - variants),
        "wire_variant_without_catalog_entry": sorted(variants - set(catalog)),
        "undispatched_methods": sorted(
            name for name in catalog if name not in by_method
        ),
        "published_refusal_stubs": published_refusals,
        "internal_refusal_stubs": internal_refusals,
        "digest_round_trip_scope": list(DIGEST_SCOPE),
        "digest_round_trip_findings": digest_round_trip(),
    }


def _failures(receipt: dict) -> list[str]:
    """One line per finding, naming the property that was violated."""

    failures = [
        f"{name}: published in the contract with no wire Method variant"
        for name in receipt["catalog_without_wire_variant"]
    ]
    failures += [
        f"{name}: wire Method variant with no contract entry"
        for name in receipt["wire_variant_without_catalog_entry"]
    ]
    failures += [
        f"{name}: no dispatch arm in any Response-producing function — the "
        "request falls through to the terminal unknown-method answer"
        for name in receipt["undispatched_methods"]
    ]
    failures += [
        f"{stub['method']}: every dispatch arm is a typed refusal, but the "
        f"method is published as {stub['stability']} for "
        f"{stub['consumer_profiles'] or 'no'} consumer(s) "
        f"({', '.join(stub['arms'])})"
        for stub in receipt["published_refusal_stubs"]
    ]
    failures += [
        f"{finding['type']}::{finding['field']}: validator accepts "
        f"{finding['accepts']} but production assigns "
        f"{' / '.join(finding['production_assigns'])} — {finding['consequence']}"
        for finding in receipt["digest_round_trip_findings"]
    ]
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit the full receipt")
    args = parser.parse_args(argv)
    try:
        receipt = run_gate()
    except GateError as exc:
        print(f"contract-method-reachability gate: CANNOT RUN: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(receipt, indent=2, sort_keys=True))

    failures = _failures(receipt)
    if failures:
        print(
            f"contract-method-reachability gate: FAIL: {len(failures)} finding(s)",
            file=sys.stderr,
        )
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(
        "contract-method-reachability gate: OK: "
        f"{receipt['dispatched_methods']}/{receipt['contract_methods']} contract "
        f"method(s) reach a dispatch arm ({receipt['dispatch_arms']} arm(s)); "
        f"{len(receipt['internal_refusal_stubs'])} internal refusal stub(s); "
        f"digest round-trip covered for {len(DIGEST_SCOPE)} contract module(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
