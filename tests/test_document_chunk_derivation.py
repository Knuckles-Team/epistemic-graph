"""Pure document-chunk projection keeps identities stable across replays."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.document_chunk_derivation import verbatim_chunk_slice

pytestmark = pytest.mark.no_engine


def test_verbatim_chunks_have_exact_text_and_document_adjacency() -> None:
    chunks = ["first sentence", "x" * 201]
    entities, edges = verbatim_chunk_slice("doc:1", "Guide", chunks, "web-document")
    replay_entities, replay_edges = verbatim_chunk_slice(
        "doc:1", "Guide", chunks, "web-document"
    )

    assert (entities, edges) == (replay_entities, replay_edges)
    assert [node["id"] for node in entities] == ["doc:1:chunk:0", "doc:1:chunk:1"]
    assert entities[0]["name"] == "Guide §1"
    assert entities[1]["description"] == "x" * 200
    assert entities[1]["trusted_answer"] == "x" * 201
    assert [edge["target"] for edge in edges] == ["doc:1", "doc:1"]
    assert all(edge["relationship"] == "PART_OF" for edge in edges)
