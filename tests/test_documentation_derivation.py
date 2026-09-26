"""Documentation identities and removal decisions require no application runtime."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.documentation_derivation import (
    documentation_content_digest,
    documentation_primary_payload,
    documentation_revision_nodes,
    documentation_snapshot_digest,
    removed_documentation_paths,
    stable_documentation_id,
)

pytestmark = pytest.mark.no_engine


def test_content_and_object_ids_are_stable_and_source_text_is_not_retained() -> None:
    digest = documentation_content_digest("# Guide\n")
    assert digest == (
        "sha256:bc553ffe57e544498b12a9865dbf3abc2004c474e349c52c378eaa402287424b"
    )
    assert stable_documentation_id("doc-page", "repo", "docs/guide.md") == (
        "doc-page:416738525f62430352648a8e1e5c27fd48db2b70"
    )
    assert stable_documentation_id("doc-page", "repo", "docs/guide.md") != (
        stable_documentation_id("doc-page", "repo/docs", "guide.md")
    )
    with pytest.raises(TypeError, match="Markdown content must be text"):
        documentation_content_digest(b"# Guide\n")  # type: ignore[arg-type]


def test_removed_paths_require_verified_snapshot() -> None:
    current = [("repo", "docs/new.md")]
    prior = {("repo", "docs/old.md")}
    with pytest.raises(ValueError, match="verified snapshot"):
        removed_documentation_paths(current, prior, (), snapshot_verified=False)
    assert removed_documentation_paths(current, prior, (), snapshot_verified=True) == [
        ("repo", "docs/old.md")
    ]
    with pytest.raises(ValueError, match="still present as current"):
        removed_documentation_paths(
            current,
            prior,
            current,
            snapshot_verified=True,
        )


def test_snapshot_digest_is_canonical_and_changes_on_removal() -> None:
    record = {
        "repository_id": "repo",
        "source_path": "docs/new.md",
        "source_revision": "a" * 40,
        "content_digest": documentation_content_digest("# New\n"),
        "lifecycle": "current",
    }
    digest = documentation_snapshot_digest("a" * 40, [record], [])
    reordered = dict(reversed(list(record.items())))
    assert digest == documentation_snapshot_digest("a" * 40, [reordered], [])
    assert digest != documentation_snapshot_digest(
        "a" * 40, [record], [("repo", "docs/old.md")]
    )


def test_documentation_rows_preserve_retrieval_and_revision_history() -> None:
    fields = {
        "document_id": "doc:guide",
        "revision_id": "revision:new",
        "repository_id": "repo",
        "source_path": "docs/guide.md",
        "source_instance": "",
        "source_ref": "repo://docs/guide.md",
        "source_revision": "b" * 40,
        "content_digest": documentation_content_digest("# New"),
        "concept_ids": ("CONCEPT:guide",),
        "lifecycle": "superseded",
        "current": False,
        "deprecated": False,
        "archived": True,
        "valid_from": "2026-09-26T00:00:00Z",
        "recorded_at": "2026-09-26T01:00:00Z",
        "previous_revision": "a" * 40,
        "previous_digest": documentation_content_digest("# Old"),
        "superseded_by": "doc:replacement",
        "snapshot_digest": None,
        "tombstone_reason": None,
    }
    page = documentation_primary_payload(
        fields, schema_version="1", mapping_version="governed-markdown-v1"
    )
    revisions = documentation_revision_nodes(fields)
    assert page["status"] == "archived"
    assert page["lifecycle_state"] == "superseded"
    assert page["acl_before_retrieval"] is True
    assert page["concept_ids"] == ["CONCEPT:guide"]
    assert page["superseded_by"] == "doc:replacement"
    assert revisions[0]["id"] == "revision:new"
    assert revisions[1]["id"] == stable_documentation_id(
        "doc-revision", "doc:guide", "a" * 40, fields["previous_digest"]
    )
    assert revisions[1]["valid_until"] == fields["valid_from"]
    assert revisions[1]["archived"] is True


def test_previous_revision_requires_exact_digest() -> None:
    fields = {
        "revision_id": "revision:new",
        "document_id": "doc:guide",
        "repository_id": "repo",
        "source_path": "docs/guide.md",
        "source_revision": "b" * 40,
        "content_digest": documentation_content_digest("# New"),
        "concept_ids": (),
        "lifecycle": "current",
        "current": True,
        "valid_from": "2026-09-26T00:00:00Z",
        "recorded_at": "2026-09-26T01:00:00Z",
        "previous_revision": "a" * 40,
        "previous_digest": "bad",
    }
    with pytest.raises(ValueError, match="previous_digest is required"):
        documentation_revision_nodes(fields)
