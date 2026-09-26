"""Pure document chunk graph projection after caller-owned text splitting."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def verbatim_chunk_slice(
    document_id: str,
    title: str,
    chunks: Iterable[str],
    source_kind: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Project exact chunks and stable adjacency into one document write slice."""
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    for index, chunk in enumerate(chunks):
        block_id = f"{document_id}:chunk:{index}"
        entities.append(
            {
                "id": block_id,
                "node_type": "idea_block",
                "name": f"{title} §{index + 1}",
                "description": chunk[:200],
                "trusted_answer": chunk,
                "source_document_id": document_id,
                "source": source_kind,
            }
        )
        relationships.append(
            {
                "source": block_id,
                "target": document_id,
                "relationship": "PART_OF",
            }
        )
    return entities, relationships
