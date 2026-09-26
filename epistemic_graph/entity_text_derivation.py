"""Deterministic entity text and CAS input derivation for semantic indexing."""

from __future__ import annotations

from typing import Any


def entity_text(node_type: str, name: str, summary: str = "", extra: str = "") -> str:
    """Compose the text used to embed an entity."""
    parts = [name]
    if summary:
        parts.append(summary)
    if extra:
        parts.append(extra)
    return " — ".join(p for p in parts if p)


# Field-name priority for the generic (connector-agnostic) text extractor below.
# Every typed-entity connector (ServiceNow, LeanIX, GitHub, Twenty, Jellyfin, ...)
# builds its own ``{"id", "type", **props}`` record shape (see
# ``ChangeEnvelope.from_connector_record``) with its own field names, so there is
# no single canonical "the title field" — these are the common ones observed
# across the fleet, checked in priority order. NAME_FIELDS come first (identity),
# then SUMMARY_FIELDS (free text), mirroring ``entity_text``'s (name, summary)
# shape.
_ENTITY_NAME_FIELDS: tuple[str, ...] = (
    "name",
    "title",
    "displayName",
    "display_name",
    "label",
    "subject",
    "short_description",
)
_ENTITY_SUMMARY_FIELDS: tuple[str, ...] = (
    "description",
    "summary",
    "body",
    "content",
    "comment",
    "message",
    "text",
    "notes",
    # Appended, not inserted, so a real description always wins where one
    # exists. Added for D-HYD-4's discovery-eligibility pass (2026-08-06):
    # measured live, `Prompt` nodes carry NO description/summary/content field
    # at all but DO carry `system_prompt` (8/8 sampled) — without this, every
    # Prompt embeds from its bare name alone. `synonyms` (a JSON-list-shaped
    # string of alternate phrasings, e.g. MCPServer nodes) is genuine
    # query-matching signal the priority list otherwise never reaches.
    "system_prompt",
    "synonyms",
)
# Never embed identifiers, timestamps, urls, or other low-signal/high-churn
# fields even when they happen to be strings — keeps ``derive_entity_text``
# deterministic and avoids polluting the embedding with noise.
#
# "type"/"node_type" are here (EH-269, found while wiring the ingest-time
# embedding admission classifier) because the fallback loop below ALREADY
# seeds ``fallback_parts`` with the resolved ``node_type`` before this loop
# runs — without this exclusion, an entity with no name/summary field re-adds
# the SAME value a second time by iterating over the "type"/"node_type" key
# too, producing a nonsense doubled string like ``"Order — Order"`` as the
# embedded text for every such entity, not just a KG-2.46-shaped few. That
# doubled string is real (non-token, multi-word) text, so it used to slip
# past every size/shape check and get embedded — pure noise, exactly what
# this ledger row exists to stop.
_ENTITY_TEXT_SKIP_KEYS = frozenset(
    {
        "id",
        "type",
        "node_type",
        "embedding",
        "text",
        "tenant_id",
        "tenant",
        "source_instance",
        "source_system",
        "domain",
        "classification",
        "retention",
        "legal_hold",
        "external_access",
        "_links",
        "_features",
        "_evidence",
        "_nodes",
    }
)
# Property-value length cap and overall text cap keep one pathological field
# (e.g. an inlined document body) from producing an oversized embedding input.
_ENTITY_FIELD_VALUE_CAP = 2000
_ENTITY_TEXT_CAP = 4000


def derive_entity_text_snapshot(
    props: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Return entity text plus the exact property values that selected it.

    CONCEPT:AU-KG.ingest.entity-embedding-at-write — every typed-entity
    connector builds a differently-shaped ``dict`` (see
    ``ChangeEnvelope.from_connector_record``'s docstring), so there is no
    single field name to embed. This checks a priority list of common
    name/title fields, then common description/body fields, then — only if
    neither produced anything — falls back to concatenating every short
    string-valued leaf property (skipping ids/timestamps/governance fields)
    so an entity with unusual field names still gets *something* embedded
    rather than silently landing with no vector.

    The snapshot is suitable for an atomic compare-and-set fence around a slow
    embedding call: if any field that selected or contributed text changes, the
    CAS fails instead of persisting a vector derived from stale content. Missing
    well-known fields are included as ``None`` because adding a higher-priority
    name or summary while the embedder is running also changes the derived text.

    Returns ``("", conditions)`` when no usable text was found. Callers must
    treat that as "defer this record", not as an embedding value.
    """
    if not props:
        return "", {}
    conditions = {
        key: props.get(key)
        for key in (
            "type",
            "node_type",
            *_ENTITY_NAME_FIELDS,
            *_ENTITY_SUMMARY_FIELDS,
        )
    }
    node_type = str(props.get("type") or props.get("node_type") or "")
    name = ""
    for key in _ENTITY_NAME_FIELDS:
        value = props.get(key)
        if isinstance(value, str) and value.strip():
            name = value.strip()[:_ENTITY_FIELD_VALUE_CAP]
            break
    summary = ""
    for key in _ENTITY_SUMMARY_FIELDS:
        value = props.get(key)
        if isinstance(value, str) and value.strip():
            summary = value.strip()[:_ENTITY_FIELD_VALUE_CAP]
            break
    if name or summary:
        return (
            entity_text(node_type, name or node_type, summary)[:_ENTITY_TEXT_CAP],
            conditions,
        )

    # Fallback: no recognized field matched — concatenate short string leaves so
    # an unusual connector shape still yields embeddable text.
    fallback_parts: list[str] = [node_type] if node_type else []
    for key, value in props.items():
        if key in _ENTITY_TEXT_SKIP_KEYS or key in _ENTITY_NAME_FIELDS:
            continue
        # The fallback considers every current non-skipped value. Fence even
        # non-string values so a concurrent change from (say) numeric to text
        # cannot make this snapshot stale without failing the CAS.
        conditions[key] = value
        if isinstance(value, str) and value.strip() and len(value) < 500:
            fallback_parts.append(value.strip())
        if sum(len(p) for p in fallback_parts) > _ENTITY_TEXT_CAP:
            break
    return " — ".join(fallback_parts)[:_ENTITY_TEXT_CAP], conditions


def derive_entity_text(props: dict[str, Any]) -> str:
    """Best-effort connector-agnostic text extraction for embedding an entity.

    Returns "" when no usable text was found (callers must treat that as
    "skip embedding this record", not as an error). See
    :func:`derive_entity_text_snapshot` when a caller needs a concurrency fence.
    """
    return derive_entity_text_snapshot(props)[0]
