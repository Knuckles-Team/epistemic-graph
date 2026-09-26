"""Engine-owned evidence object and fragment graph projection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from .evidence_address import (
    artifact_id_for,
    content_digest,
    fragment_id_for,
    path_anchor,
)


class EvidenceEnvelope(Protocol):
    """AU delivery envelope shape consumed by evidence projection."""

    connector: str
    source_instance: str
    source_object_id: str
    provenance: dict[str, Any]
    payload_type: str
    blob_ref: str | None
    envelope_id: str
    idempotency_key: str
    source_version: str
    schema_version: str
    ontology_mapping_version: str
    tenant: str
    classification: Any
    retention: str | None
    legal_hold: bool
    source_acl: Any


# ── Graph vocabulary ─────────────────────────────────────────────────────────
# Node/edge labels the spine materializes through ``ingest_graph_slice``.  They
# mirror the existing Document/Chunk pair's HAS_CHUNK / CHUNK_OF convention
# (``ontology/document_processing.py``) rather than inventing a new one.
ARTIFACT_NODE_TYPE = "Artifact"
FRAGMENT_NODE_TYPE = "Fragment"
HAS_FRAGMENT_EDGE = "HAS_FRAGMENT"
FRAGMENT_OF_EDGE = "FRAGMENT_OF"
HAS_CHILD_FRAGMENT_EDGE = "HAS_CHILD_FRAGMENT"
PARENT_FRAGMENT_EDGE = "PARENT_FRAGMENT"
NEXT_FRAGMENT_EDGE = "NEXT_FRAGMENT"
HAS_ARTIFACT_EDGE = "HAS_ARTIFACT"
ARTIFACT_OF_EDGE = "ARTIFACT_OF"

#: The structural kinds a fragment may take.  Deliberately aligned with the
#: engine's ``ArtifactLocus.kind`` vocabulary (``document_span`` /
#: ``table_cell_range`` / ``page_box`` / ``row_version``) so a fragment renders
#: straight into an engine evidence locus without a second taxonomy.
FRAGMENT_KINDS: frozenset[str] = frozenset(
    {
        "document",
        "section",
        "heading",
        "paragraph",
        "list",
        "list_item",
        "table",
        "table_row",
        "table_cell",
        "code_block",
        "quote",
        "page",
        "span",
        "record",
        "field",
        # The domain-pack framework's three additional structural units
        # (CONCEPT:AU-KG.ingest.domain-pack-framework, D-GP2-2) — a corpus
        # structure the generic markdown/PDF/record fragmenters above don't
        # themselves produce, but which the SAME addressable-citation
        # contract (this module, not a rival stand-in) must still be able to
        # name so every consumer speaks one Fragment vocabulary:
        "frontmatter_key",  # one YAML frontmatter key/value pair
        "link",  # one inline `[text](href)` markdown link
        "json_field",  # one dotted-path field in a JSON document/API record
    }
)
FragmentKind = Literal[
    "document",
    "section",
    "heading",
    "paragraph",
    "list",
    "list_item",
    "table",
    "table_row",
    "table_cell",
    "code_block",
    "quote",
    "page",
    "span",
    "record",
    "field",
    "frontmatter_key",
    "link",
    "json_field",
]


@dataclass(frozen=True)
class Fragment:
    """One addressable citation unit inside an :class:`Artifact`.

    CONCEPT:AU-KG.ingest.stable-fragment-address.

    Attributes:
        fragment_id: The stable **address** — derived from ``artifact_id`` +
            ``path`` only.  Survives a body edit; a citation stores THIS.
        artifact_id: The owning artifact.
        kind: One of :data:`FRAGMENT_KINDS`.
        path: The scoped structural path, one ``<kind>:<anchor>`` segment per
            ancestor level ending with this fragment's own segment.  Human
            readable via :attr:`address`.
        text: The fragment's own text (a table's ``text`` is its caption/header
            line, not its rows — rows are child fragments).
        content_hash: ``sha256:<hex>`` over the normalized text.  Changes when
            and only when the content changes.
        ordinal: Position among *siblings* under the same parent (0-based).
        sequence: Position in whole-artifact document order (0-based) — a total
            order over every fragment, so a flat "give me fragments 12..18" read
            works without walking the tree.
        depth: Nesting depth (0 = top level under the artifact).
        parent_fragment_id: The enclosing fragment, or ``None`` at top level.
        char_start / char_end: Character span in the artifact's extracted text.
            ``-1`` when the artifact has no linear text (e.g. an API record).
        label: The fragment's own name when it has one (heading text, table
            caption) — what anchored its path segment.
        locus_kind: The engine ``ArtifactLocus.kind`` this fragment renders to.
        attributes: Kind-specific extras (a table row's column values, a page
            number, a code block's language).  Never governance — governance
            lives on the artifact/envelope.
    """

    fragment_id: str
    artifact_id: str
    kind: FragmentKind
    path: tuple[str, ...]
    text: str
    content_hash: str
    ordinal: int = 0
    sequence: int = 0
    depth: int = 0
    parent_fragment_id: str | None = None
    char_start: int = -1
    char_end: int = -1
    label: str = ""
    locus_kind: str = "document_span"
    attributes: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.kind not in FRAGMENT_KINDS:
            raise ValueError(
                f"Fragment.kind must be one of {sorted(FRAGMENT_KINDS)}, "
                f"got {self.kind!r}"
            )
        if not self.content_hash.startswith("sha256:"):
            raise ValueError(
                "Fragment.content_hash must be a 'sha256:<hex>' digest "
                f"(see content_digest()), got {self.content_hash!r}"
            )
        if not self.path:
            raise ValueError(
                "Fragment.path must have at least one '<kind>:<anchor>' segment — "
                "an empty path has no stable address."
            )
        expected = fragment_id_for(self.artifact_id, self.path)
        if self.fragment_id != expected:
            raise ValueError(
                "Fragment.fragment_id must be fragment_id_for(artifact_id, path) — "
                f"got {self.fragment_id!r}, expected {expected!r}.  Build fragments "
                "with Fragment.at() so the address can never drift from the path."
            )

    @classmethod
    def at(
        cls,
        *,
        artifact_id: str,
        kind: FragmentKind,
        parent_path: tuple[str, ...] = (),
        text: str = "",
        label: str = "",
        ordinal: int = 0,
        **kwargs: Any,
    ) -> Fragment:
        """Build a fragment at ``parent_path + <this segment>``.

        The single sanctioned constructor: it derives the path segment, the
        address, and the content hash together, so the three can never disagree.
        """
        path = (*parent_path, path_anchor(kind, label=label, ordinal=ordinal))
        return cls(
            fragment_id=fragment_id_for(artifact_id, path),
            artifact_id=artifact_id,
            kind=kind,
            path=path,
            text=text,
            content_hash=content_digest(text),
            ordinal=ordinal,
            label=label,
            depth=len(parent_path),
            **kwargs,
        )

    @property
    def address(self) -> str:
        """The human-readable structural path, e.g. ``h2:getting-started/p:2``."""
        return "/".join(self.path)

    @property
    def version_id(self) -> str:
        """``<fragment_id>#<short content hash>`` — a content-pinned citation.

        Use this when a citation must be immutable (an audit record, a published
        claim's evidence).  Use :attr:`fragment_id` when it must *follow* the
        fragment through edits.  Both are needed; neither substitutes.

        Separator is ``#``, not ``@``: the engine's ``ApplyChangeEnvelope``
        commit path (``change_envelope.rs``'s ``validate_safe_text``) rejects
        any ``@`` in inline text outright as an email/host-leak privacy guard —
        by design, and correctly so; a blanket exemption for ``@`` would weaken
        that guard fleet-wide. ``#`` carries the same "pin a version" meaning
        (a URL fragment identifier pins a specific view of a resource) without
        colliding with the privacy scan (see D-GM-4 / D-GS856-6 / D-MW-1 /
        D-MW-2 in the deferred ledger for the full investigation).
        """
        return f"{self.fragment_id}#{self.content_hash[7:23]}"

    def to_locus(self) -> dict[str, Any]:
        """Render an engine ``ArtifactLocus``-shaped selector for this fragment.

        Matches ``agent_utilities.protocols.epistemic_operations.ArtifactLocus``
        (``kind`` / ``start`` / ``end`` / ``selector``) so a candidate claim can
        cite this fragment as engine-native evidence without a second mapping.
        """
        return {
            "kind": self.locus_kind,
            "start": self.char_start if self.char_start >= 0 else None,
            "end": self.char_end if self.char_end >= 0 else None,
            "selector": {
                "fragment_id": self.fragment_id,
                "artifact_id": self.artifact_id,
                "address": self.address,
                "content_hash": self.content_hash,
            },
        }

    def to_node(self) -> dict[str, Any]:
        """Render the graph-slice entity row for this fragment.

        ``node_type``-keyed (never ``type``) so it is directly admissible to
        :func:`~..ingestion.envelope_ingest.ingest_graph_slice`.
        """
        row: dict[str, Any] = {
            "id": self.fragment_id,
            "node_type": FRAGMENT_NODE_TYPE,
            "artifact_id": self.artifact_id,
            "fragment_kind": self.kind,
            "address": self.address,
            "text": self.text,
            "content_hash": self.content_hash,
            "version_id": self.version_id,
            "ordinal": self.ordinal,
            "sequence": self.sequence,
            "depth": self.depth,
            "char_start": self.char_start,
            "char_end": self.char_end,
            "locus_kind": self.locus_kind,
        }
        if self.label:
            row["label"] = self.label
        if self.parent_fragment_id:
            row["parent_fragment_id"] = self.parent_fragment_id
        for key, value in self.attributes.items():
            row.setdefault(f"attr_{key}", value)
        return row


@dataclass(frozen=True)
class Artifact:
    """One retrieved source object, keyed to the :class:`ChangeEnvelope`

    that delivered it (CONCEPT:AU-KG.ingest.evidence-spine-artifact).

    An artifact is the *object* — a markdown file, a PDF, an API record, a row
    set — not one delivery of it.  :attr:`artifact_id` is therefore keyed to
    source identity and stays put across revisions, while
    :attr:`content_hash` identifies the revision.  Governance is NOT redeclared
    here: it is carried verbatim off the envelope, which is the trust boundary
    that decided it.

    Attributes:
        artifact_id: Deterministic id from connector + instance + source object.
        connector / source_instance / source_object_id: Source identity, copied
            from the envelope.
        media_type: IANA media type of the retrieved bytes
            (``text/markdown``, ``application/pdf``, ``application/json``).
        content_hash: ``sha256:<hex>`` over the artifact's content.
        byte_length: Size of the retrieved content in bytes.
        content_ref: Where the bytes live when they are not inline (a blob key /
            URI — the envelope's ``blob_ref``), else ``""``.
        envelope_id / idempotency_key / source_version / schema_version /
            ontology_mapping_version: The delivery this artifact was extracted
            from — ``ontology_mapping_version`` is the domain-pack revision
            that mapped the raw payload onto graph facts, the last link in a
            source-to-claim lineage walk
            (CONCEPT:AU-KG.retrieval.source-to-claim-lineage).
        classification / retention / legal_hold / external_access: Governance,
            copied from the envelope.
        fragments: The artifact's fragments in document order.
        provenance: Free-form lineage, copied from the envelope and extended
            with the fragmenter that produced :attr:`fragments`.
    """

    artifact_id: str
    connector: str
    media_type: str
    content_hash: str

    source_instance: str = ""
    source_object_id: str = ""
    byte_length: int = 0
    content_ref: str = ""

    envelope_id: str = ""
    idempotency_key: str = ""
    source_version: str = ""
    schema_version: str = "1"
    ontology_mapping_version: str = ""
    tenant: str = ""

    classification: str = "internal"
    retention: str | None = None
    legal_hold: bool = False
    external_access: dict[str, Any] | None = None

    title: str = ""
    fragments: tuple[Fragment, ...] = field(default_factory=tuple)
    provenance: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.content_hash.startswith("sha256:"):
            raise ValueError(
                "Artifact.content_hash must be a 'sha256:<hex>' digest "
                f"(see content_digest()), got {self.content_hash!r}"
            )
        for fragment in self.fragments:
            if fragment.artifact_id != self.artifact_id:
                raise ValueError(
                    "Artifact.fragments must all belong to this artifact — "
                    f"{fragment.fragment_id!r} claims artifact "
                    f"{fragment.artifact_id!r}, not {self.artifact_id!r}."
                )

    @classmethod
    def from_envelope(
        cls,
        envelope: EvidenceEnvelope,
        *,
        content: str | bytes,
        media_type: str = "",
        fragments: tuple[Fragment, ...] | list[Fragment] = (),
        title: str = "",
        fragmenter: str = "",
        source_object_id: str = "",
    ) -> Artifact:
        """Build an artifact for the object *envelope* delivered.

        Governance, source identity, and revision are read off the envelope
        rather than re-derived — the envelope is the gate that already decided
        them, and re-deriving is how a payload gets to spoof its own ACL.

        ``source_object_id`` overrides the envelope's own object id for the
        SINGLE case where the envelope's id is content-derived (the
        ``DocumentProcessor`` path hashes content into its ``doc_id``).  The
        artifact must key to the stable *object* — the path/URL — or every edit
        forks a new artifact and every citation to it is orphaned.
        """
        object_id = source_object_id or envelope.source_object_id
        artifact_id = artifact_id_for(
            envelope.connector, envelope.source_instance, object_id
        )
        raw = content.encode("utf-8") if isinstance(content, str) else content
        provenance = dict(envelope.provenance)
        if fragmenter:
            provenance["fragmenter"] = fragmenter
        return cls(
            artifact_id=artifact_id,
            connector=envelope.connector,
            source_instance=envelope.source_instance,
            source_object_id=object_id,
            media_type=media_type or _media_type_for(envelope.payload_type),
            content_hash=content_digest(content),
            byte_length=len(raw),
            content_ref=envelope.blob_ref or "",
            envelope_id=envelope.envelope_id,
            idempotency_key=envelope.idempotency_key,
            source_version=envelope.source_version,
            schema_version=envelope.schema_version,
            ontology_mapping_version=envelope.ontology_mapping_version,
            tenant=envelope.tenant,
            classification=envelope.classification.value,
            retention=envelope.retention,
            legal_hold=envelope.legal_hold,
            external_access=(
                envelope.source_acl.model_dump()
                if envelope.source_acl is not None
                else None
            ),
            title=title,
            fragments=tuple(fragments),
            provenance=provenance,
        )

    def to_node(self) -> dict[str, Any]:
        """Render the graph-slice entity row for this artifact."""
        row: dict[str, Any] = {
            "id": self.artifact_id,
            "node_type": ARTIFACT_NODE_TYPE,
            "connector": self.connector,
            "source_instance": self.source_instance,
            "source_object_id": self.source_object_id,
            "media_type": self.media_type,
            "content_hash": self.content_hash,
            "byte_length": self.byte_length,
            "fragment_count": len(self.fragments),
            "envelope_id": self.envelope_id,
            "idempotency_key": self.idempotency_key,
            "source_version": self.source_version,
            "schema_version": self.schema_version,
            "ontology_mapping_version": self.ontology_mapping_version,
            "classification": self.classification,
            "legal_hold": self.legal_hold,
        }
        if self.title:
            row["title"] = self.title
        if self.content_ref:
            row["content_ref"] = self.content_ref
        if self.retention:
            row["retention"] = self.retention
        if self.external_access is not None:
            row["external_access"] = self.external_access
        return row

    def to_graph_slice(
        self, *, document_id: str = ""
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return ``(entities, relationships)`` for ``ingest_graph_slice``.

        The artifact is entity[0] — the slice's primary object — and every
        fragment follows, so the whole spine commits in ONE atomic envelope
        rather than as a fragment-per-write stream that could half-land.

        Args:
            document_id: When this artifact was retrieved alongside an extracted
                ``Document`` node (the ``DocumentProcessor`` path), link the two
                so a reader can pivot from the retrieval unit (``Chunk``) to the
                citation unit (``Fragment``) through their shared source object.
        """
        entities: list[dict[str, Any]] = [self.to_node()]
        relationships: list[dict[str, Any]] = []
        if document_id:
            relationships.append(
                {
                    "source": document_id,
                    "target": self.artifact_id,
                    "relationship": HAS_ARTIFACT_EDGE,
                }
            )
            relationships.append(
                {
                    "source": self.artifact_id,
                    "target": document_id,
                    "relationship": ARTIFACT_OF_EDGE,
                }
            )
        by_parent: dict[str | None, list[Fragment]] = {}
        for fragment in self.fragments:
            entities.append(fragment.to_node())
            relationships.append(
                {
                    "source": self.artifact_id,
                    "target": fragment.fragment_id,
                    "relationship": HAS_FRAGMENT_EDGE,
                    "sequence": fragment.sequence,
                }
            )
            relationships.append(
                {
                    "source": fragment.fragment_id,
                    "target": self.artifact_id,
                    "relationship": FRAGMENT_OF_EDGE,
                }
            )
            if fragment.parent_fragment_id:
                relationships.append(
                    {
                        "source": fragment.parent_fragment_id,
                        "target": fragment.fragment_id,
                        "relationship": HAS_CHILD_FRAGMENT_EDGE,
                        "ordinal": fragment.ordinal,
                    }
                )
                relationships.append(
                    {
                        "source": fragment.fragment_id,
                        "target": fragment.parent_fragment_id,
                        "relationship": PARENT_FRAGMENT_EDGE,
                    }
                )
            by_parent.setdefault(fragment.parent_fragment_id, []).append(fragment)
        # Sibling order is an explicit edge, not an implied property read — a
        # reader walking evidence needs "what came next" without a sort.
        for siblings in by_parent.values():
            ordered = sorted(siblings, key=lambda f: (f.ordinal, f.sequence))
            for left, right in zip(ordered, ordered[1:], strict=False):
                relationships.append(
                    {
                        "source": left.fragment_id,
                        "target": right.fragment_id,
                        "relationship": NEXT_FRAGMENT_EDGE,
                    }
                )
        return entities, relationships


def _media_type_for(payload_type: str) -> str:
    """Map a :attr:`ChangeEnvelope.payload_type` tag to an IANA media type."""
    return {
        "json": "application/json",
        "markdown": "text/markdown",
        "text": "text/plain",
        "html": "text/html",
        "blob": "application/octet-stream",
    }.get(payload_type, "application/octet-stream")
