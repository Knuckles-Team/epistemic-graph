"""The native agent vocabulary, mirrored for the pure-Python verifier.

A copy of ``crates/eg-types/src/agent_ontology.rs``. It is never trusted on
its own: :func:`ontology_digest` must equal the ``ontology_digest`` a record
pins, or the verifier refuses to judge the record at all.
"""

from __future__ import annotations

from ._digest import digest_text

AGENT_ONTOLOGY_DIGEST_DOMAIN = "eg/agent-ontology/v1"

#: ``(iri, label, broader, requires)`` in the engine's table order.
TERMS: tuple[tuple[str, str, str | None, tuple[str, ...]], ...] = (
    ("eg:capability", "capability", None, ()),
    ("eg:capability/retrieval", "retrieval", "eg:capability", ()),
    ("eg:capability/retrieval/web-search", "web search", "eg:capability/retrieval", ()),
    (
        "eg:capability/retrieval/vector-search",
        "vector search",
        "eg:capability/retrieval",
        (),
    ),
    (
        "eg:capability/retrieval/graph-query",
        "graph query",
        "eg:capability/retrieval",
        (),
    ),
    ("eg:capability/retrieval/sql-query", "sql query", "eg:capability/retrieval", ()),
    (
        "eg:capability/retrieval/document-read",
        "document read",
        "eg:capability/retrieval",
        (),
    ),
    ("eg:capability/generation", "generation", "eg:capability", ()),
    (
        "eg:capability/generation/text",
        "text generation",
        "eg:capability/generation",
        (),
    ),
    (
        "eg:capability/generation/code",
        "code generation",
        "eg:capability/generation",
        (),
    ),
    (
        "eg:capability/generation/image",
        "image generation",
        "eg:capability/generation",
        (),
    ),
    (
        "eg:capability/generation/speech",
        "speech synthesis",
        "eg:capability/generation",
        (),
    ),
    ("eg:capability/analysis", "analysis", "eg:capability", ()),
    ("eg:capability/analysis/summarize", "summarize", "eg:capability/analysis", ()),
    ("eg:capability/analysis/classify", "classify", "eg:capability/analysis", ()),
    ("eg:capability/analysis/extract", "extract", "eg:capability/analysis", ()),
    ("eg:capability/analysis/compare", "compare", "eg:capability/analysis", ()),
    ("eg:capability/analysis/evaluate", "evaluate", "eg:capability/analysis", ()),
    ("eg:capability/reasoning", "reasoning", "eg:capability", ()),
    ("eg:capability/reasoning/plan", "plan", "eg:capability/reasoning", ()),
    ("eg:capability/reasoning/decompose", "decompose", "eg:capability/reasoning", ()),
    ("eg:capability/reasoning/verify", "verify", "eg:capability/reasoning", ()),
    ("eg:capability/reasoning/critique", "critique", "eg:capability/reasoning", ()),
    ("eg:capability/memory", "memory", "eg:capability", ()),
    ("eg:capability/memory/store", "store", "eg:capability/memory", ()),
    ("eg:capability/memory/recall", "recall", "eg:capability/memory", ()),
    ("eg:capability/action", "action", "eg:capability", ()),
    ("eg:capability/action/file-write", "file write", "eg:capability/action", ()),
    ("eg:capability/action/http-request", "http request", "eg:capability/action", ()),
    (
        "eg:capability/action/process-exec",
        "process execution",
        "eg:capability/action",
        (),
    ),
    ("eg:capability/action/message-send", "message send", "eg:capability/action", ()),
    ("eg:capability/action/schedule", "schedule", "eg:capability/action", ()),
    ("eg:modality", "modality", None, ()),
    ("eg:modality/text", "text", "eg:modality", ()),
    ("eg:modality/image", "image", "eg:modality", ()),
    ("eg:modality/audio", "audio", "eg:modality", ()),
    ("eg:modality/video", "video", "eg:modality", ()),
    ("eg:modality/structured", "structured data", "eg:modality", ()),
    ("eg:task", "task", None, ()),
    (
        "eg:task/research",
        "research",
        "eg:task",
        (
            "eg:capability/retrieval",
            "eg:capability/analysis/summarize",
            "eg:capability/reasoning/plan",
        ),
    ),
    (
        "eg:task/implement",
        "implement",
        "eg:task",
        (
            "eg:capability/generation/code",
            "eg:capability/retrieval/document-read",
            "eg:capability/action/file-write",
        ),
    ),
    (
        "eg:task/review",
        "review",
        "eg:task",
        (
            "eg:capability/analysis/evaluate",
            "eg:capability/reasoning/critique",
            "eg:capability/retrieval/document-read",
        ),
    ),
    (
        "eg:task/operate",
        "operate",
        "eg:task",
        (
            "eg:capability/action",
            "eg:capability/retrieval/graph-query",
            "eg:capability/reasoning/verify",
        ),
    ),
    (
        "eg:task/communicate",
        "communicate",
        "eg:task",
        (
            "eg:capability/generation/text",
            "eg:capability/analysis/summarize",
            "eg:capability/action/message-send",
        ),
    ),
)

_BROADER: dict[str, str | None] = {iri: broader for iri, _, broader, _ in TERMS}
_MAX_DEPTH = 16


def ontology_digest() -> str:
    """The digest of this vocabulary, computed exactly as the engine does."""
    view = [
        {"iri": iri, "label": label, "broader": broader, "requires": list(requires)}
        for iri, label, broader, requires in TERMS
    ]
    return digest_text(AGENT_ONTOLOGY_DIGEST_DOMAIN, view)


def is_direct_broader(narrower: str, broader: str) -> bool:
    """Whether ``broader`` is the declared immediate parent of ``narrower``."""
    return narrower in _BROADER and _BROADER[narrower] == broader
