"""Deterministic control-flow projection through pass-through graph nodes.

Both the ARIS EPC lift (``extractors/aris.py``) and the Camunda BPMN
sequence-flow lift (``extractors/camunda.py``) collapse a raw control-flow
graph down to edges between "lifted" (first-class) nodes, walking through
non-lifted pass-through nodes (events/gateways not promoted to their own
node) along the way — the SAME algorithm, deliberately kept shaped alike so
an ARIS EPC and its Camunda BPMN counterpart are structurally comparable.
The traversal is an EG-owned derivation; source adapters retain their own
id-namespacing and node/edge emission.
"""

from __future__ import annotations

from collections import deque


def _collapse_from(
    src: str,
    lifted: set[str],
    outgoing: dict[str, list[tuple[str, str | None]]],
) -> list[tuple[str, str, str | None]]:
    """BFS from ONE lifted ``src`` through non-lifted intermediates until
    reaching another lifted node, collapsing pass-through nodes along the
    way. The FIRST condition encountered on a collapsed path wins. Returns
    ``(src, target, condition)`` triples, each target emitted at most once.
    """
    found: list[tuple[str, str, str | None]] = []
    seen_targets: set[str] = set()
    frontier = deque(outgoing.get(src, []))
    visited: set[str] = {src}
    while frontier:
        tgt, condition = frontier.popleft()
        if tgt in lifted:
            if tgt not in seen_targets:
                seen_targets.add(tgt)
                found.append((src, tgt, condition))
            continue
        if tgt in visited:
            continue  # bounded walk through pass-through nodes
        visited.add(tgt)
        for nxt, nxt_condition in outgoing.get(tgt, []):
            frontier.append((nxt, condition or nxt_condition))
    return found


def collapse_to_lifted_targets(
    lifted: set[str],
    outgoing: dict[str, list[tuple[str, str | None]]],
) -> list[tuple[str, str, str | None]]:
    """Collapse every lifted source to its lifted targets (see
    :func:`_collapse_from`), across the whole ``lifted`` set."""
    result: list[tuple[str, str, str | None]] = []
    for src in sorted(lifted):
        result.extend(_collapse_from(src, lifted, outgoing))
    return result
