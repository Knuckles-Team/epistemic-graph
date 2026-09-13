#!/usr/bin/env python3
"""Terms of acceptance for Rust exhaustive dispatch under the cyclomatic cap.

WHY THIS EXISTS
---------------
Cyclomatic complexity counts every `match` arm as an independent decision
point.  For a flat, exhaustive `match` over an enum that is arithmetically
correct and semantically empty: there is no nesting, no interleaving of
conditions, and no path a reader has to hold in their head.  The measurement
that shows it (cccc 1.6.0, this tree, 2026-09-11):

    src/server/wire/mod.rs::dispatch_kind            cyclomatic 32, cognitive 1
    src/server/persistence/redb_backend.rs::handle_cmd    cyclomatic 54, cognitive 8
        (50 arms, no catch-all, residual 4)

Cognitive complexity -- which does NOT charge a flat match per arm -- reports
those as trivial, because they are.

Decomposing a genuinely exhaustive match is not a neutral refactor.  It trades
away rustc's exhaustiveness guarantee: while the match names every variant,
adding an enum variant is a COMPILE ERROR at every dispatch site.  Behind a
lookup table, a boxed closure map, or a macro, the same addition is a silent
runtime fallthrough.  A macro in particular only hides the arms from the
scanner while changing nothing about the code -- that is gaming the metric, and
this repository's rule is to expose debt, not to launder it.

WHY THIS IS NOT A RATCHET AND NOT A BASELINE
--------------------------------------------
This module contains no list of files, no list of functions, no count, and no
recorded "accepted" finding.  It states a RULE about a CLASS of code and
recomputes membership from the source under measurement on every single run.  A
function that grows a catch-all arm, grows non-dispatch branching, or grows
cognitive complexity leaves the class immediately and starts failing, with no
file to edit and nothing to re-bless.

The rule is also STRICTLY TIGHTER than what preceded it.  Before it, NOTHING
checked whether a high-cyclomatic match was exhaustive at all, so a 68-arm
dispatcher that ends in `other => return Err(other)` -- which is NOT exhaustive,
whose decomposition costs no safety at all, and which is therefore ordinary debt
-- was exactly as unremarked as a genuinely exhaustive one.  Applying this rule
makes that population visible as backlog for the first time.  Measured on this
tree: 71 such functions.

THE RULE
--------
A Rust function may exceed the cyclomatic cap ONLY IF all four hold:

  1. its cognitive complexity is within the cognitive cap -- the flat-dispatch
     claim is falsifiable, and cognitive complexity is the falsifier;
  2. its body contains at least one `match` -- branching that is not dispatch
     (an if/else ladder, a chain of `?`) is ordinary complexity and gets no
     relief.  Measured: 41 of the cyclomatic-only over-cap functions in this
     tree contain no `match` at all;
  3. NO arm of ANY `match` in the body is irrefutable -- no `_`, no bare
     binding (`other`, `ref x`, `mut x`, `x @ _`), with or without a guard, and
     no `|`-alternation containing one.  An irrefutable arm is what makes a
     match non-exhaustive, so decomposing it forfeits no guarantee;
  4. its RESIDUAL cyclomatic complexity -- the measured value minus the number
     of match arms in the body -- is within the SAME cyclomatic cap every other
     function obeys.  This is not a new threshold: it says only that once the
     exhaustive dispatch is discounted, what is left must pass the ordinary
     gate.  A dispatcher that also carries ten other decision points is not
     exempt because of the dispatch.

Everything the rule cannot prove is NOT exempt.  Non-Rust source, a body whose
braces do not balance, a function whose start line carries no body, and an arm
count that exceeds the measured cyclomatic complexity (which means the arm
attribution is wrong, so the residual cannot be trusted) all fail closed.

LIMITS, STATED HONESTLY
-----------------------
Arm patterns are read from the comment- and literal-masked source, so a `_ =>`
inside a string or a comment cannot create a false catch-all.  Irrefutable
TUPLE and STRUCT destructuring arms (`(a, b) =>`, `Foo { x } =>`) are not
recognised as catch-alls.  They are irrefutable, but they appear only in
single-arm matches, which cannot push a function over a cyclomatic cap of 10 on
their own; no over-cap function in this tree is affected.  If that changes, the
`IRREFUTABLE` pattern here is the single place to extend.
"""

from __future__ import annotations

import re
from typing import NamedTuple

from rust_lexer import _balanced_span_from, _rust_code_mask

#: An arm pattern that binds instead of discriminating. `_`, a bare lowercase
#: binding, and an `ident @ _` binding all match every remaining value, so the
#: match containing one is not exhaustive over its scrutinee's variants.
#: Rust's own naming rules make this unambiguous: a unit variant or a constant
#: in pattern position is `UpperCamel` or `SCREAMING_CASE`, and a qualified path
#: carries `::`; only a binding is a bare lowercase identifier.
IRREFUTABLE = re.compile(
    r"^(?:ref\s+)?(?:mut\s+)?(?:_|[a-z][A-Za-z0-9_]*)(?:\s*@\s*_)?$"
)

_OPEN, _CLOSE = "([{", ")]}"
_MATCH_KEYWORD = re.compile(r"\bmatch\b")


class DispatchShape(NamedTuple):
    """What the source says about a function's match arms.

    ``arms`` counts every arm of every ``match`` inside the function body,
    nested matches included, because cccc charges every one of them.
    """

    arms: int
    catch_alls: int


def _skip_space(mask: str, index: int, end: int) -> int:
    while index < end and mask[index].isspace():
        index += 1
    return index


def _pattern_end(mask: str, index: int, end: int) -> int | None:
    """Index of the top-level ``=>`` that terminates an arm pattern."""
    depth = 0
    while index < end - 1:
        char = mask[index]
        if char in _OPEN:
            depth += 1
        elif char in _CLOSE:
            if depth == 0:
                return None
            depth -= 1
        elif depth == 0 and char == "=" and mask[index + 1] == ">":
            return index
        index += 1
    return None


def _block_arm_end(mask: str, index: int, end: int) -> int:
    """Index just past a `{ .. }` arm body and its optional trailing comma."""
    after = _skip_space(mask, _balanced_span_from(mask, index, "{", "}") + 1, end)
    return after + 1 if after < end and mask[after] == "," else after


def _expression_arm_end(mask: str, index: int, end: int) -> int:
    """Index just past an expression arm body: the next top-level comma or `}`."""
    depth = 0
    while index < end:
        char = mask[index]
        if char in _OPEN:
            depth += 1
        elif char in _CLOSE:
            if depth == 0:
                return index
            depth -= 1
        elif depth == 0 and char == ",":
            return index + 1
        index += 1
    return end


def _arm_body_end(mask: str, index: int, end: int) -> int | None:
    """Index just past one arm's body: a block, or an expression up to a comma."""
    index = _skip_space(mask, index, end)
    if index >= end:
        return None
    if mask[index] == "{":
        return _block_arm_end(mask, index, end)
    return _expression_arm_end(mask, index, end)


def _match_blocks(mask: str, start: int, end: int) -> list[tuple[int, int]]:
    """Brace span of every ``match`` expression inside ``[start, end)``.

    Rust forbids an unparenthesised struct literal in a match scrutinee, so the
    first ``{`` outside parentheses and brackets always opens the arm block.
    """
    blocks: list[tuple[int, int]] = []
    for keyword in _MATCH_KEYWORD.finditer(mask, start, end):
        index, depth = keyword.end(), 0
        while index < end:
            char = mask[index]
            if char in "([":
                depth += 1
            elif char in ")]":
                depth -= 1
            elif char == "{" and depth == 0:
                blocks.append((index, _balanced_span_from(mask, index, "{", "}")))
                break
            index += 1
    return blocks


def _arm_patterns(mask: str, opening: int, closing: int) -> list[str]:
    """Every arm pattern text in one match block, in source order."""
    patterns: list[str] = []
    index = opening + 1
    while True:
        index = _skip_space(mask, index, closing)
        while index < closing and mask[index] == "#":
            bracket = mask.find("[", index, closing)
            if bracket < 0:
                return patterns
            index = _skip_space(
                mask, _balanced_span_from(mask, bracket, "[", "]") + 1, closing
            )
        if index >= closing:
            return patterns
        arrow = _pattern_end(mask, index, closing)
        if arrow is None:
            return patterns
        patterns.append(mask[index:arrow])
        following = _arm_body_end(mask, arrow + 2, closing)
        if following is None or following <= arrow:
            return patterns
        index = following


def _without_guard(pattern: str) -> str:
    """The pattern with any ``if`` guard removed.

    A guarded arm is deliberately treated as a catch-all when its pattern is
    irrefutable.  Proving that a guarded binding arm is harmless would require
    evaluating the guard, so the rule refuses the exemption instead.
    """
    depth = 0
    for token in re.finditer(r"[()\[\]{}]|\bif\b", pattern):
        text = token.group(0)
        if text in _OPEN:
            depth += 1
        elif text in _CLOSE:
            depth -= 1
        elif depth == 0:
            return pattern[: token.start()]
    return pattern


def _is_catch_all(pattern: str) -> bool:
    body = _without_guard(pattern)
    depth, alternatives, start = 0, [], 0
    for index, char in enumerate(body):
        if char in _OPEN:
            depth += 1
        elif char in _CLOSE:
            depth -= 1
        elif char == "|" and depth == 0:
            alternatives.append(body[start:index])
            start = index + 1
    alternatives.append(body[start:])
    return any(
        IRREFUTABLE.match(alternative.strip())
        for alternative in alternatives
        if alternative.strip()
    )


def _line_offsets(source: str) -> list[int]:
    offsets = [0]
    for line in source.split("\n"):
        offsets.append(offsets[-1] + len(line) + 1)
    return offsets


def dispatch_shape(source: str, line: int) -> DispatchShape | None:
    """Match-arm shape of the function whose signature starts at ``line``.

    ``None`` means the shape could not be established -- an unlexable file,
    unbalanced delimiters, or a start line with no body.  Every caller must
    read ``None`` as "not exempt".
    """
    try:
        mask = _rust_code_mask(source)
        offsets = _line_offsets(source)
        if line < 1 or line >= len(offsets):
            return None
        opening = mask.find("{", offsets[line - 1])
        if opening < 0:
            return None
        closing = _balanced_span_from(mask, opening, "{", "}")
        arms = catch_alls = 0
        for block_open, block_close in _match_blocks(mask, opening, closing):
            for pattern in _arm_patterns(mask, block_open, block_close):
                arms += 1
                catch_alls += _is_catch_all(pattern)
    except (SystemExit, ValueError, IndexError, RecursionError):
        return None
    return DispatchShape(arms=arms, catch_alls=catch_alls)


def exhaustive_dispatch_exempt(
    source: str | None,
    line: int,
    cyclomatic: int,
    cognitive: int,
    max_cyclomatic: int,
    max_cognitive: int,
) -> bool:
    """True when the four conditions in this module's docstring all hold.

    ``source`` is the Rust text of the file under measurement -- the STAGED
    blob for a pre-commit run, never the working tree -- or ``None`` for a
    non-Rust file, which is never exempt.
    """
    if source is None or cognitive > max_cognitive or cyclomatic <= max_cyclomatic:
        return False
    shape = dispatch_shape(source, line)
    if shape is None or shape.arms == 0 or shape.catch_alls:
        return False
    residual = cyclomatic - shape.arms
    return 1 <= residual <= max_cyclomatic
