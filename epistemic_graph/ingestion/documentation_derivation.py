"""Deterministic identities and verified-snapshot decisions for documentation."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)


def documentation_content_digest(content: str) -> str:
    """Digest exact UTF-8 Markdown bytes without retaining source text."""
    if not isinstance(content, str):
        raise TypeError("Markdown content must be text")
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


def stable_documentation_id(prefix: str, *parts: str, length: int = 40) -> str:
    """Derive a stable object ID from ordered, separated identity components."""
    material = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}:{hashlib.sha256(material).hexdigest()[:length]}"


def removed_documentation_paths(
    current_keys: Iterable[tuple[str, str]],
    prior_keys: Iterable[tuple[str, str]],
    superseded_keys: Iterable[tuple[str, str]],
    *,
    snapshot_verified: bool,
) -> list[tuple[str, str]]:
    """Admit removals only when an authoritative snapshot proves absence."""
    current = set(current_keys)
    superseded = set(superseded_keys)
    if superseded & current:
        raise ValueError("a superseded documentation path is still present as current")
    removed = sorted((set(prior_keys) | superseded) - current)
    if removed and not snapshot_verified:
        raise ValueError(
            "verified snapshot is required before emitting documentation tombstones"
        )
    return removed


def documentation_snapshot_digest(
    snapshot_revision: str | None,
    records: Iterable[Mapping[str, Any]],
    removed_keys: Iterable[tuple[str, str]],
) -> str:
    """Digest a stable, ordered projection of one repository snapshot."""
    payload = json.dumps(
        {
            "revision": snapshot_revision or "",
            "records": list(records),
            "removed": list(removed_keys),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def documentation_primary_payload(
    fields: Mapping[str, Any], *, schema_version: str, mapping_version: str
) -> dict[str, Any]:
    """Build the retrieval-facing page row from validated source metadata."""
    status = str(fields["lifecycle"])
    retrieval_status = status if fields["current"] else "archived"
    payload: dict[str, Any] = {
        "id": fields["document_id"],
        "node_type": "DocumentationPage",
        "repository_id": fields["repository_id"],
        "source_path": fields["source_path"],
        "source_kind": "markdown",
        "corpus": fields["source_instance"] or fields["repository_id"],
        "relpath": fields["source_path"],
        "source_ref": fields["source_ref"],
        "source_revision": fields["source_revision"],
        "content_digest": fields["content_digest"],
        "concept_ids": list(fields["concept_ids"]),
        "lifecycle_state": status,
        "status": retrieval_status,
        "current": fields["current"],
        "deprecated": fields["deprecated"],
        "archived": fields["archived"],
        "valid_from": fields["valid_from"],
        "recorded_at": fields["recorded_at"],
        "documentation_schema_version": schema_version,
        "ontology_mapping_version": mapping_version,
        "acl_verified": True,
        "acl_before_retrieval": True,
    }
    for optional in ("superseded_by", "snapshot_digest", "tombstone_reason"):
        if fields.get(optional):
            payload[optional] = fields[optional]
    return payload


def documentation_revision_nodes(fields: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build current and superseded revision rows without performing writes."""
    nodes: list[dict[str, Any]] = [
        {
            "id": fields["revision_id"],
            "node_type": "DocumentationRevision",
            "document_id": fields["document_id"],
            "repository_id": fields["repository_id"],
            "source_path": fields["source_path"],
            "source_revision": fields["source_revision"],
            "content_digest": fields["content_digest"],
            "concept_ids": list(fields["concept_ids"]),
            "lifecycle_state": str(fields["lifecycle"]),
            "current": fields["current"],
            "valid_from": fields["valid_from"],
            "recorded_at": fields["recorded_at"],
        }
    ]
    if fields.get("previous_revision"):
        previous_digest = fields.get("previous_digest") or ""
        if not _DIGEST_RE.fullmatch(previous_digest):
            raise ValueError(
                "previous_digest is required when previous_revision is supplied"
            )
        nodes.append(
            {
                "id": stable_documentation_id(
                    "doc-revision",
                    fields["document_id"],
                    fields["previous_revision"],
                    previous_digest,
                ),
                "node_type": "DocumentationRevision",
                "document_id": fields["document_id"],
                "repository_id": fields["repository_id"],
                "source_path": fields["source_path"],
                "source_revision": fields["previous_revision"],
                "content_digest": previous_digest,
                "lifecycle_state": "superseded",
                "current": False,
                "archived": True,
                "valid_until": fields["valid_from"],
                "recorded_at": fields["recorded_at"],
            }
        )
    return nodes
