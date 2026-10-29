"""The engine's native agent vocabulary, as data the generators can plant against.

This mirrors the compiled table in `crates/eg-types/src/agent_ontology.rs`
(capabilities and modalities with `broader`, tasks with `requires`). Two checks
keep the mirror honest: a static test compares the IRI sets with the Rust
source, and the catalog end-to-end scenario compares every subsumption answer
with the engine's own capability search.
"""

from __future__ import annotations

from typing import NamedTuple

CAPABILITY_ROOT = "eg:capability"
MODALITY_ROOT = "eg:modality"
TASK_ROOT = "eg:task"
ACTION = "eg:capability/action"


class Term(NamedTuple):
    iri: str
    broader: str | None
    requires: tuple[str, ...] = ()


_CAPABILITY_TREE: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "retrieval",
        ("web-search", "vector-search", "graph-query", "sql-query", "document-read"),
    ),
    ("generation", ("text", "code", "image", "speech")),
    ("analysis", ("summarize", "classify", "extract", "compare", "evaluate")),
    ("reasoning", ("plan", "decompose", "verify", "critique")),
    ("memory", ("store", "recall")),
    (
        "action",
        ("file-write", "http-request", "process-exec", "message-send", "schedule"),
    ),
)

_MODALITIES = ("text", "image", "audio", "video", "structured")

_TASKS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("research", ("retrieval", "analysis/summarize", "reasoning/plan")),
    ("implement", ("generation/code", "retrieval/document-read", "action/file-write")),
    ("review", ("analysis/evaluate", "reasoning/critique", "retrieval/document-read")),
    ("operate", ("action", "retrieval/graph-query", "reasoning/verify")),
    ("communicate", ("generation/text", "analysis/summarize", "action/message-send")),
)


def _capability(path: str) -> str:
    return f"{CAPABILITY_ROOT}/{path}"


def _build_terms() -> tuple[Term, ...]:
    terms = [Term(CAPABILITY_ROOT, None)]
    for branch, leaves in _CAPABILITY_TREE:
        terms.append(Term(_capability(branch), CAPABILITY_ROOT))
        terms.extend(
            Term(_capability(f"{branch}/{leaf}"), _capability(branch))
            for leaf in leaves
        )
    terms.append(Term(MODALITY_ROOT, None))
    terms.extend(Term(f"{MODALITY_ROOT}/{name}", MODALITY_ROOT) for name in _MODALITIES)
    terms.append(Term(TASK_ROOT, None))
    for task, needs in _TASKS:
        requires = tuple(_capability(need) for need in needs)
        terms.append(Term(f"{TASK_ROOT}/{task}", TASK_ROOT, requires))
    return tuple(terms)


TERMS: tuple[Term, ...] = _build_terms()
_BY_IRI = {entry.iri: entry for entry in TERMS}


def term(iri: str) -> Term | None:
    return _BY_IRI.get(iri)


def is_native(iri: str) -> bool:
    return iri in _BY_IRI


def ancestors(iri: str) -> tuple[str, ...]:
    """Every broader term of ``iri``, nearest first, excluding itself."""
    chain: list[str] = []
    current = _BY_IRI.get(iri)
    while current is not None and current.broader is not None:
        chain.append(current.broader)
        current = _BY_IRI.get(current.broader)
    return tuple(chain)


def satisfies(provided: str, required: str) -> bool:
    """Directional subsumption: a specific term satisfies its general ancestors."""
    return provided == required or required in ancestors(provided)


def under(root: str) -> tuple[str, ...]:
    """Every native term strictly below ``root``, in table order."""
    return tuple(entry.iri for entry in TERMS if root in ancestors(entry.iri))


def leaves(root: str) -> tuple[str, ...]:
    """Terms below ``root`` that nothing else names as broader."""
    parents = {entry.broader for entry in TERMS}
    return tuple(iri for iri in under(root) if iri not in parents)


def capabilities_for_task(task_iri: str) -> tuple[str, ...]:
    found = _BY_IRI.get(task_iri)
    return found.requires if found is not None else ()


def is_side_effecting_term(iri: str) -> bool:
    return satisfies(iri, ACTION)
