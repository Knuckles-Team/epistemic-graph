#!/usr/bin/env python3
"""Slice a Rust source file by CALL GRAPH rather than by byte offset.

Why this exists
---------------
Several architecture gates in this repo assert a property about "the graph
dispatch path" by slicing `dispatch.rs` from the header of
`dispatch_graph_op_inner` to the end of the file and then searching that slice
for literal tokens. That worked while the whole path lived in one enormous
function. The complexity program decomposed it, and the decomposition moved
those properties one or two calls DOWN -- the graph ACL check into
`check_graph_op_access`, the read authority into `resolve_graph_read_authority`,
the KnowledgeStream routing into `route_graph_authority_surfaces` -- and
reindented the surviving call sites from twelve spaces to eight.

Both effects break a positional, whitespace-coupled assertion while the
architecture itself is fully intact. A gate that reports a property MISSING
when it has merely moved is worse than no gate: the next person's cheapest way
to a green commit is to weaken the assertion.

So: resolve the property through the call graph, and compare on squashed
whitespace. A helper the entry point does not actually call contributes
nothing, so a token still cannot be satisfied by dead code.

This is deliberately a lexical approximation, not a compiler. It assumes
rustfmt'd source: a top-level item's header starts at column 0 and its body
ends at the first line that is exactly `}`. That is true of every file it is
pointed at, and it fails LOUD (an empty body, hence a failing gate) rather
than silently matching the wrong region if it ever stops being true.
"""

from __future__ import annotations

import re

_HEADER = re.compile(
    r"^(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?(?:extern\s+\"[^\"]*\"\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)
_CALL = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def top_level_fns(src: str) -> dict[str, str]:
    """Every top-level `fn` in `src`, name -> body (header through its `}`)."""

    lines = src.splitlines(keepends=True)
    starts: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        match = _HEADER.match(line)
        if match:
            starts.append((i, match.group(1)))
    out: dict[str, str] = {}
    for i, name in starts:
        end = i
        while end < len(lines) and lines[end].rstrip("\n") != "}":
            end += 1
        # A name may be defined twice under mutually exclusive `#[cfg]`s; keep
        # both so a token in either arm still resolves.
        out[name] = out.get(name, "") + "".join(lines[i : end + 1])
    return out


def reachable_source(src: str, root: str) -> str:
    """`root`'s body plus every top-level fn in `src` it transitively calls.

    Callees are matched lexically by `name(`, so this over-approximates
    slightly (a same-named method call pulls the free function in). It never
    UNDER-approximates, which is the direction that matters: a gate must not
    report a property missing because it moved into a helper.
    """

    fns = top_level_fns(src)
    if root not in fns:
        return ""
    seen: set[str] = {root}
    queue = [root]
    while queue:
        body = fns[queue.pop()]
        for name in set(_CALL.findall(body)):
            if name in fns and name not in seen:
                seen.add(name)
                queue.append(name)
    return "\n".join(fns[name] for name in sorted(seen))


def squash(text: str) -> str:
    """Collapse every whitespace run to one space, and close method chains up.

    Extraction reindents a call site without changing what it passes, and
    pushing a receiver over rustfmt's width budget breaks
    `s.channels.authorize_member(` into three lines. Matching on squashed text
    asserts the RECEIVER and ARGUMENTS, which are the property, and ignores the
    line breaking, which is not.

    Whitespace around `.` is dropped for that reason; whitespace elsewhere is
    preserved as a single space, so an argument list still has to match
    argument for argument.
    """

    return re.sub(r"\s*\.\s*", ".", re.sub(r"\s+", " ", text))
