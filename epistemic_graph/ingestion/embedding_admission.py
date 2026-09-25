"""Deterministic embedding-admission classifier (EH-269).

CONCEPT:AU-KG.ingest.embedding-admission

``INGESTION-ECONOMICS-DESIGN.md`` §0 found ingest-time embedding applied to
essentially everything crossing the ``ChangeEnvelope`` boundary by explicit
design — the D-EMB chokepoint in ``envelope_ingest.py`` exists precisely so
no connector can bypass it — gated only by four cheap, content-blind
predicates (upsert+typed-payload, text-unchanged short-circuit,
empty-derived-text, a global on/off flag). This module is the missing FIFTH
gate: a deterministic, per-unit classifier deciding whether a specific piece
of derived text is worth a vector at all.

**Where this plugs in.** ``envelope_ingest._prepare_embedding_envelopes``
calls :func:`classify_unit` on every candidate AFTER ``_stage_embedding_change``
has already computed the post-merge entity text, and BEFORE that text is
added to the batch handed to the embedder. This is INSIDE the chokepoint, not
a per-connector opt-out — the exact shape the design brief requires ("do not
add per-connector opt-outs, which would recreate the bypass problem the
chokepoint was built to prevent").

**Design constraints (binding):**

* **Deterministic.** The same unit always gets the same verdict. No LLM
  calls, no randomness, no wall-clock/environment dependence — a
  classifier that disagrees with itself on a replay would make the
  provenance work in the neighbouring lane (EH-274) unreproducible.
* **A classifier is a TABLE, not an if-chain** (BUILD-CONTRACT §2). Every
  rule below is one row of a fixed, documented, most-specific-first
  sequence — a new rule is added as a ROW, never as another ``elif``.
* **Conservative on the "a cheaper index already answers this" claim.**
  Only :data:`ContentClass.STRUCTURED_TOKEN` makes that claim, and only for
  content that provably cannot benefit from a vector: a UUID, a hex hash, a
  semver string, a bare URL, or an email address has no semantic neighbours
  worth finding — exact match or a graph traversal on the same field already
  answers "find X" for it. Deliberately narrow: an ordinary single-word or
  hyphenated NAME is NOT treated as a structured token (see
  ``classify_by_shape``'s docstring) — it is a human-assigned identifier,
  not a machine one, and can be a real semantic-search target.
  Every OTHER "maybe a cheaper index answers this" case is left ADMITTED
  rather than guessed at, because under-embedding silently degrades
  retrieval and is much harder to detect than over-embedding
  (INGESTION-ECONOMICS-DESIGN.md §2) — see the module-level
  ``NOT YET IMPLEMENTED`` note near the bottom for the evidence-gated
  handoff on broadening this rule.

**What is intentionally NOT here (scope honesty, see this lane's WRAPUP):**

* **Retrieval-telemetry feedback** ("demote never-retrieved classes") is
  :mod:`agent_utilities.knowledge_graph.ingestion.admission_feedback` (EH-398): :data:`ContentClass` values are the
  label EG's per-class retrieval usage is keyed on, and the loop only ever
  PROPOSES a table change for review -- nothing auto-demotes.
* **True column cardinality** (distinct-value counts) is a corpus-level
  statistic this per-unit, stateless classifier cannot compute. SQL-column
  admission (:func:`classify_sql_column`) uses name/shape heuristics as a
  conservative PROXY for "this is a high-cardinality free-text column",
  not a measured cardinality. Flagged, not silently assumed.
* **Near-duplicate (fuzzy) dedupe** is out of scope here — the MinHash
  machinery for that already exists for `similar_to` RESEMBLANCE edges on
  the code-symbol side, but instantiating it as a text-level ingest-time
  gate is a separate piece of work. :func:`dedupe_by_content_hash` only
  covers EXACT duplicate text (deterministic, cheap, no false positives).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

__all__ = [
    "ContentClass",
    "AdmissionVerdict",
    "NEVER_EMBED_CLASSES",
    "classify_unit",
    "classify_sql_column",
    "is_sql_row_connector",
    "sql_free_text_fields",
    "content_hash",
    "dedupe_by_content_hash",
]


class ContentClass(StrEnum):
    """The bucket a unit of candidate-embedding text was classified into.

    Values are stable strings on purpose: they are also the label EG's
    per-class retrieval usage and EH-398's admission proposals key on. Renaming a
    value here silently invalidates any telemetry keyed on the old string —
    add a new member instead of renaming.
    """

    PROSE = "prose"
    GENERATED = "generated"
    LOCKFILE = "lockfile"
    VENDORED = "vendored"
    MINIFIED = "minified"
    BINARY = "binary"
    SQL_ENUM = "sql_enum"
    SQL_FOREIGN_KEY = "sql_foreign_key"
    SQL_TIMESTAMP = "sql_timestamp"
    SQL_BOOLEAN = "sql_boolean"
    SQL_NUMERIC = "sql_numeric"
    STRUCTURED_TOKEN = "structured_token"  # sanitizer:ignore — enum, not credential
    TOO_SHORT = "too_short"


#: Classes that never receive a vector, regardless of length/entropy — the
#: cheapest win the design calls out, and the one to get right first.
#: ``ContentClass.PROSE`` is the only member NOT in this set: everything the
#: table below does not affirmatively reject is admitted.
NEVER_EMBED_CLASSES: frozenset[ContentClass] = frozenset(
    {
        ContentClass.GENERATED,
        ContentClass.LOCKFILE,
        ContentClass.VENDORED,
        ContentClass.MINIFIED,
        ContentClass.BINARY,
        ContentClass.SQL_ENUM,
        ContentClass.SQL_FOREIGN_KEY,
        ContentClass.SQL_TIMESTAMP,
        ContentClass.SQL_BOOLEAN,
        ContentClass.SQL_NUMERIC,
        ContentClass.STRUCTURED_TOKEN,
        ContentClass.TOO_SHORT,
    }
)

_REASONS: dict[ContentClass, str] = {
    ContentClass.PROSE: "eligible free text",
    ContentClass.GENERATED: (
        "generated file/content — never embed (regenerable, no authored signal)"
    ),
    ContentClass.LOCKFILE: "dependency lockfile — machine-written, no semantic content",
    ContentClass.VENDORED: (
        "vendored/third-party path — not this project's authored content"
    ),
    ContentClass.MINIFIED: (
        "minified asset — no token-level semantic structure survives minification"
    ),
    ContentClass.BINARY: "binary media by extension — not text",
    ContentClass.SQL_ENUM: (
        "SQL column classified as enum/categorical — answerable by an exact predicate"
    ),
    ContentClass.SQL_FOREIGN_KEY: (
        "SQL column classified as a foreign key/id — answerable by a graph edge"
    ),
    ContentClass.SQL_TIMESTAMP: (
        "SQL column classified as a timestamp/date — answerable by a range predicate"
    ),
    ContentClass.SQL_BOOLEAN: "SQL column is boolean — two-valued, no semantic content",
    ContentClass.SQL_NUMERIC: "SQL column is numeric — answerable by a range predicate",
    ContentClass.STRUCTURED_TOKEN: (
        "single machine token (no whitespace) — exact match already answers it"
    ),
    ContentClass.TOO_SHORT: "below the minimum length with any semantic signal",
}


@dataclass(frozen=True)
class AdmissionVerdict:
    """One unit's admission decision.

    ``admit`` is derived from ``content_class`` alone
    (``content_class not in NEVER_EMBED_CLASSES``) and checked in
    ``__post_init__`` rather than trusted from a caller — a verdict can never
    silently drift from the table that produced it (BUILD-CONTRACT's
    no-ratchet rule applied to this dataclass's own invariant).
    """

    admit: bool
    content_class: ContentClass
    reason: str

    def __post_init__(self) -> None:
        expected = self.content_class not in NEVER_EMBED_CLASSES
        if self.admit != expected:
            raise ValueError(
                f"AdmissionVerdict.admit={self.admit} contradicts "
                f"NEVER_EMBED_CLASSES for {self.content_class}"
            )


def _verdict(content_class: ContentClass) -> AdmissionVerdict:
    return AdmissionVerdict(
        admit=content_class not in NEVER_EMBED_CLASSES,
        content_class=content_class,
        reason=_REASONS[content_class],
    )


# ── content class: path/filename identity ───────────────────────────────────
# Never-embed by NAME alone — cheapest, most certain signal, checked first.

_LOCKFILE_BASENAMES = frozenset(
    {
        "package-lock.json",
        "npm-shrinkwrap.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "cargo.lock",
        "poetry.lock",
        "pipfile.lock",
        "gemfile.lock",
        "go.sum",
        "composer.lock",
        "mix.lock",
        "flake.lock",
        "uv.lock",
    }
)

_GENERATED_FILENAME_SUFFIXES = (
    ".pb.go",
    "_pb2.py",
    "_pb2_grpc.py",
    ".g.dart",
    ".freezed.dart",
    ".generated.cs",
    ".designer.cs",
    ".generated.ts",
)

_MINIFIED_FILENAME_SUFFIXES = (".min.js", ".min.css", ".min.map")

_VENDORED_PATH_SEGMENTS = frozenset(
    {
        "vendor",
        "vendored",
        "node_modules",
        "third_party",
        "3rdparty",
        ".venv",
        "venv",
        "site-packages",
        "dist",
        "build",
        "target",
        "bower_components",
    }
)

_BINARY_EXTENSIONS = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".ico",
        ".pdf",
        ".zip",
        ".tar",
        ".gz",
        ".7z",
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".woff",
        ".woff2",
        ".ttf",
        ".otf",
        ".mp4",
        ".mp3",
        ".wasm",
        ".class",
        ".jar",
        ".pyc",
    }
)

#: Common field names a connector uses for a file path — checked in this
#: priority order (mirrors ``derive_entity_text``'s own priority-list idiom).
#:
#: **Known interaction with the privacy gate (found while testing this
#: lane).** ``envelope_ingest.py`` runs persistence-privacy sanitization
#: BEFORE this classifier — correctly: privacy must win over economics. That
#: gate's ``_LOCATION_FIELDS`` (``security/persistence_privacy.py``)
#: unconditionally redacts any field literally named ``path`` or
#: ``file_path`` to the constant string ``"[REDACTED_LOCATION]"``,
#: REGARDLESS of value, before it ever reaches here. So for a connector that
#: names its field ``path``/``file_path``, path-based classification below
#: silently sees no usable signal (falls through as "no path", not a
#: crash — degrades gracefully to the content-marker/shape rules). Only
#: ``relpath`` (what ``git_markdown.py`` actually uses in production),
#: ``filepath``, and ``filename`` survive that redaction. This is NOT a bug
#: to fix here — it is the correct precedence — but it means a future
#: connector wanting this classifier's path-based rules to see its file path
#: should emit it under ``relpath``/``filename``, not ``path``/``file_path``.
_PATH_FIELDS = ("relpath", "path", "file_path", "filepath", "filename")


def _path_from_row(row: dict[str, Any]) -> str:
    for key in _PATH_FIELDS:
        value = row.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _basename(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def _extension(path: str) -> str:
    name = _basename(path)
    return f".{name.rsplit('.', 1)[-1]}" if "." in name else ""


# A TABLE, not an if-chain: (predicate over a lowercased path, resulting
# class), evaluated top to bottom; the first match wins.
_PATH_RULES: tuple[tuple[Callable[[str], bool], ContentClass], ...] = (
    (lambda p: _basename(p) in _LOCKFILE_BASENAMES, ContentClass.LOCKFILE),
    (
        lambda p: _basename(p).endswith(_GENERATED_FILENAME_SUFFIXES),
        ContentClass.GENERATED,
    ),
    (
        lambda p: _basename(p).endswith(_MINIFIED_FILENAME_SUFFIXES),
        ContentClass.MINIFIED,
    ),
    (
        lambda p: any(seg in _VENDORED_PATH_SEGMENTS for seg in p.split("/")[:-1]),
        ContentClass.VENDORED,
    ),
    (lambda p: _extension(p) in _BINARY_EXTENSIONS, ContentClass.BINARY),
)


def classify_by_path(row: dict[str, Any]) -> ContentClass | None:
    """Filename/path-only classification. ``None`` when the row carries no
    path signal at all (the caller falls through to content-based rules)."""
    path = _path_from_row(row)
    if not path:
        return None
    lower = path.lower()
    for predicate, content_class in _PATH_RULES:
        if predicate(lower):
            return content_class
    return None


# ── content class: content marker scan ──────────────────────────────────────

_GENERATED_MARKERS = (
    "@generated",
    "do not edit",
    "code generated by",
    "autogenerated",
    "auto-generated",
    "this file is automatically generated",
)

#: Only the head of the text is scanned — generated-file headers are always
#: near the top, and scanning the whole text would be wasted work on every
#: candidate for a marker that, by convention, is never buried mid-file.
_MARKER_SCAN_CHARS = 400


def classify_by_content_marker(text: str) -> ContentClass | None:
    if not text:
        return None
    head = text[:_MARKER_SCAN_CHARS].lower()
    if any(marker in head for marker in _GENERATED_MARKERS):
        return ContentClass.GENERATED
    return None


# ── content class: the text's own shape ─────────────────────────────────────

#: Below this many characters there is no semantic signal to embed (a bare
#: word or two) — the design's explicit "size bounds" criterion, lower half.
_MIN_SEMANTIC_CHARS = 12

#: Deliberately NARROW: only shapes that provably carry no similarity signal
#: beyond exact match — a UUID, a hex hash, a semver string, a bare URL, or
#: an email address. An ordinary single-word/hyphenated NAME (e.g.
#: ``"rec-object-1"``, ``"web-server-01"``) is explicitly NOT here: it has no
#: whitespace either, but it is a human-assigned identifier that can be a
#: real semantic-search target ("find servers named web-*"), and the design
#: brief is explicit that under-embedding is worse than over-embedding and
#: harder to detect — a bare "no whitespace" test would have caught those
#: too, which is exactly the over-claim this module's docstring warns against
#: (caught by this lane's own test suite: see
#: ``test_ingest_envelopes_batch_auto_embeds_in_one_call`` regressing before
#: this rule was narrowed to concrete, evidenced patterns).
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
_HEX_HASH_RE = re.compile(r"^[0-9a-f]{32,64}$", re.IGNORECASE)
_URL_RE = re.compile(r"^[a-z][a-z0-9+.\-]*://\S+$", re.IGNORECASE)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PURE_NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")
_SEMVER_RE = re.compile(r"^v?\d+\.\d+\.\d+([.\-][0-9A-Za-z.\-]+)?$")

_STRUCTURED_TOKEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    _UUID_RE,
    _HEX_HASH_RE,
    _URL_RE,
    _EMAIL_RE,
    _PURE_NUMERIC_RE,
    _SEMVER_RE,
)


def classify_by_shape(text: str) -> ContentClass | None:
    stripped = text.strip()
    if len(stripped) < _MIN_SEMANTIC_CHARS:
        return ContentClass.TOO_SHORT
    if any(pattern.match(stripped) for pattern in _STRUCTURED_TOKEN_PATTERNS):
        return ContentClass.STRUCTURED_TOKEN
    return None


# ── SQL/CDC column typing ────────────────────────────────────────────────────
# "SQL columns specifically: embed only high-cardinality free text. Never an
# enum, foreign key, timestamp, boolean or numeric." (design doc §2). Only
# ``debezium_envelope.py``'s CDC path stamps rows with a known, literal
# 1:1 column mapping (``connector="cdc"``, see that module) — every other
# connector's typed_payload is a hand-shaped record, not a raw table row, so
# this table is scoped to that one deterministic signal rather than guessed
# at for arbitrary connectors.

_SQL_ROW_CONNECTORS = frozenset({"cdc"})

_SQL_FK_NAME_SUFFIXES = ("_id", "_fk", "_ref")
_SQL_TIMESTAMP_NAME_HINTS = ("_at", "_on", "_date", "_time", "timestamp")
_SQL_ENUM_NAME_HINTS = (
    "status",
    "state",
    "category",
    "kind",
    "level",
    "priority",
    "severity",
    "role",
    "code",
)
#: ISO-8601-shaped date/datetime VALUE, independent of the column name — a
#: column named unhelpfully (e.g. ``col_7``) whose value is still plainly a
#: timestamp is caught by shape, not just by name.
_ISO_DATETIME_VALUE = re.compile(
    r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}([.+Z].*)?)?$"
)


def is_sql_row_connector(connector: str) -> bool:
    return connector in _SQL_ROW_CONNECTORS


def classify_sql_column(name: str, value: Any) -> ContentClass | None:
    """Classify ONE SQL/CDC column's value. ``None`` means "candidate free
    text" — NOT "admitted"; it still passes through :func:`classify_unit`'s
    generic rules (size bounds, structured-token, etc.) like any other text.
    """
    lname = name.lower()
    if isinstance(value, bool):
        return ContentClass.SQL_BOOLEAN
    if isinstance(value, (int, float)):
        return ContentClass.SQL_NUMERIC
    if lname == "id" or lname.endswith(_SQL_FK_NAME_SUFFIXES):
        return ContentClass.SQL_FOREIGN_KEY
    if lname.endswith(_SQL_TIMESTAMP_NAME_HINTS):
        return ContentClass.SQL_TIMESTAMP
    if isinstance(value, str) and _ISO_DATETIME_VALUE.match(value.strip()):
        return ContentClass.SQL_TIMESTAMP
    if any(hint in lname for hint in _SQL_ENUM_NAME_HINTS):
        return ContentClass.SQL_ENUM
    return None


def _drop_as_sql_column(key: str, value: Any, protected_fields: frozenset[str]) -> bool:
    if key in protected_fields:
        return False
    if not isinstance(value, str):
        # Non-string values never reach the fallback text concatenation
        # anyway (see ``derive_entity_text_snapshot``'s own ``isinstance``
        # guard) — nothing to drop.
        return False
    return classify_sql_column(key, value) is not None


def sql_free_text_fields(
    connector: str, row: dict[str, Any], *, protected_fields: frozenset[str]
) -> dict[str, Any]:
    """For a SQL/CDC-row-shaped envelope, the subset of ``row`` eligible to
    reach text derivation (never enum/FK/timestamp/bool/numeric). A no-op
    passthrough for every other connector — this filter only applies where
    column identity is actually known (see module docstring's cardinality
    caveat: this is a name/shape PROXY for "high-cardinality free text", not
    a measured one).

    The caller supplies structural and priority fields from its text
    projection so the filter cannot silently drift from that projection.

    Applying this to BOTH the durable "current" properties and the incoming
    "row" before they reach ``_stage_embedding_change`` has a second,
    load-bearing effect beyond the embedding decision: a write that only
    changes an excluded column (e.g. a status transition) no longer looks
    like a text change at all, so the existing "text-unchanged short-circuit"
    gate fires for it too — fewer spurious re-embeds, not just fewer
    ineligible ones.
    """
    if not is_sql_row_connector(connector):
        return row
    return {
        key: value
        for key, value in row.items()
        if not _drop_as_sql_column(key, value, protected_fields)
    }


# ── the classifier's single entry point ─────────────────────────────────────


def classify_unit(
    *, connector: str, row: dict[str, Any], text: str
) -> AdmissionVerdict:
    """The one entry point ``envelope_ingest``'s chokepoint calls.

    Fixed, deterministic order: path identity first (cheapest, most
    certain), then a content-marker scan, then the text's own shape. Falls
    through to :data:`ContentClass.PROSE` (admitted) when nothing rejects it.

    ``row`` should already have had :func:`sql_free_text_fields` applied by
    the caller for a SQL-row-shaped envelope — this function classifies the
    ``text`` it is given, it does not re-derive it.
    """
    for classifier in (
        lambda: classify_by_path(row),
        lambda: classify_by_content_marker(text),
        lambda: classify_by_shape(text),
    ):
        content_class = classifier()
        if content_class is not None:
            return _verdict(content_class)
    return _verdict(ContentClass.PROSE)


# ── entropy / duplication: exact content-hash dedupe ────────────────────────


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="surrogatepass")).hexdigest()


def dedupe_by_content_hash(
    items: list[tuple[int, str]],
) -> tuple[list[tuple[int, str]], dict[int, int]]:
    """Collapse exact-duplicate texts before they reach the embedder.

    Returns ``(unique_items, alias_map)``: ``unique_items`` is one
    ``(position, text)`` per distinct hash (first occurrence wins — stable,
    deterministic), and ``alias_map`` maps every NON-representative position
    to the representative position whose vector it should share. The
    embedder is called once per DISTINCT text; the caller copies the
    representative's vector to every aliased position.

    Only exact duplicates are covered (a straight content hash) — near-
    duplicate/fuzzy matching is explicitly out of scope here, see the module
    docstring.
    """
    seen: dict[str, int] = {}
    unique: list[tuple[int, str]] = []
    alias: dict[int, int] = {}
    for position, text in items:
        digest = content_hash(text)
        representative = seen.get(digest)
        if representative is None:
            seen[digest] = position
            unique.append((position, text))
        else:
            alias[position] = representative
    return unique, alias
