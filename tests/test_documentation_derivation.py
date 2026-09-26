"""Documentation identities and removal decisions require no application runtime."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.documentation_derivation import (
    documentation_content_digest,
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
