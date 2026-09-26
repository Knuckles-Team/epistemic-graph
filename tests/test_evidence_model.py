"""Evidence graph projection remains owned by the engine."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from epistemic_graph.ingestion.evidence_model import Artifact, Fragment

pytestmark = pytest.mark.no_engine


def test_artifact_and_fragment_project_one_citable_graph_slice() -> None:
    envelope = SimpleNamespace(
        connector="git",
        source_instance="runbooks",
        source_object_id="settlement.md",
        provenance={"commit": "abc"},
        payload_type="markdown",
        blob_ref=None,
        envelope_id="delivery-1",
        idempotency_key="key-1",
        source_version="abc",
        schema_version="1",
        ontology_mapping_version="v1",
        tenant="acme",
        classification=SimpleNamespace(value="internal"),
        retention=None,
        legal_hold=False,
        source_acl=None,
    )
    artifact = Artifact.from_envelope(
        envelope, content="# Settlement", title="Settlement"
    )
    fragment = Fragment.at(
        artifact_id=artifact.artifact_id,
        kind="heading",
        text="Settlement",
        label="Settlement",
    )
    artifact = Artifact.from_envelope(
        envelope, content="# Settlement", fragments=(fragment,)
    )

    entities, relationships = artifact.to_graph_slice(document_id="document-1")
    assert [entity["node_type"] for entity in entities] == ["Artifact", "Fragment"]
    assert entities[1]["id"] == fragment.fragment_id
    assert entities[1]["version_id"] == fragment.version_id
    assert {edge["relationship"] for edge in relationships} >= {
        "HAS_ARTIFACT",
        "HAS_FRAGMENT",
        "FRAGMENT_OF",
    }
    assert fragment.to_locus()["selector"]["content_hash"] == fragment.content_hash


def test_foreign_fragment_is_rejected() -> None:
    fragment = Fragment.at(artifact_id="artifact:other", kind="paragraph", text="text")
    with pytest.raises(ValueError, match="must all belong"):
        Artifact(
            artifact_id="artifact:this",
            connector="git",
            media_type="text/markdown",
            content_hash="sha256:abc",
            fragments=(fragment,),
        )
