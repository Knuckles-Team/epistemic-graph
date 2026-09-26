"""Evidence addresses survive content edits and unrelated insertions."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.evidence_address import (
    artifact_id_for,
    content_digest,
    fragment_id_for,
    path_anchor,
    slugify,
)

pytestmark = pytest.mark.no_engine


def test_artifact_identity_is_source_scoped_but_revision_independent() -> None:
    artifact = artifact_id_for("git", "runbooks", "settlement.md")
    assert artifact == artifact_id_for("git", "runbooks", "settlement.md")
    assert artifact != artifact_id_for("git", "archive", "settlement.md")
    assert "settlement.md" not in artifact


def test_fragment_address_stays_stable_when_body_changes() -> None:
    artifact = artifact_id_for("git", "runbooks", "settlement.md")
    path = (
        path_anchor("heading", label="Operations"),
        path_anchor("paragraph", ordinal=2),
    )
    address = fragment_id_for(artifact, path)
    assert address == fragment_id_for(artifact, path)
    assert content_digest("On-call rotates weekly.") != content_digest(
        "On-call rotates daily."
    )
    assert (
        fragment_id_for(artifact, (path[0], path_anchor("paragraph", ordinal=3)))
        != address
    )


def test_unicode_and_cosmetic_text_normalize_without_collapsing_binary() -> None:
    assert slugify("Café") == slugify("Cafe\u0301")
    assert content_digest("line one\n line two") == content_digest("line one line two")
    assert content_digest(b"line one\n line two") != content_digest(
        b"line one line two"
    )
