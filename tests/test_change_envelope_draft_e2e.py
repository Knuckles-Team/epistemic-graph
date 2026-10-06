"""``client.changes.apply`` end to end against the session engine.

A client authors only the envelope draft; the engine mints the mutation's
scope identity, version expectation and admission envelope from the verified
request, commits it through the governed ChangeEnvelope path and records it.
"""

from __future__ import annotations

import hashlib
from typing import Any

import msgpack


def _envelope(graph: str, node_id: str, key: str, sequence: int) -> dict[str, Any]:
    body = {"type": "Document", "title": node_id}
    return {
        "schema_version": 1,
        "envelope_id": f"envelope:{node_id}:{sequence}",
        "mutation": {
            "batch_id": f"batch:{node_id}:{sequence}",
            "graph": graph,
            "idempotency_key": key,
            "operations": [
                {
                    "ordinal": 0,
                    "surface": "graph",
                    "domain": "graph_rows",
                    "method": {
                        "method": "AddNode",
                        "params": {
                            "node_id": node_id,
                            "properties_msgpack": msgpack.packb(body),
                        },
                    },
                }
            ],
            "outbox": [],
        },
        "content_version": {
            "object_id": node_id,
            "digest_algorithm": "sha256",
            "digest": hashlib.sha256(repr(body).encode()).hexdigest(),
            "source_version": {"kind": "sequence", "value": sequence},
        },
        # Every material object carries its policy proof; the client stamps the
        # verified tenant onto it.
        "policies": [
            {
                "policy_id": f"policy:{node_id}",
                "operation": "upsert",
                "object_id": node_id,
                "classification": "internal",
                "policy_version": "policy-v1",
                "subject_set_digest": "b" * 64,
            }
        ],
        "privacy": {
            "policy_version": "privacy-v1",
            "sanitizer_version": "sanitizer-v1",
            "sanitized_payload_digest": "c" * 64,
        },
    }


def _graph_name(client: Any) -> str:
    inner = getattr(client, "_client", client)
    return str(inner._graph_name)


def test_changes_apply_commits_a_draft_under_engine_minted_authority(clean_graph):
    graph = _graph_name(clean_graph)
    applied = clean_graph.changes.apply(
        _envelope(graph, "doc:draft-1", "changes-e2e:doc:draft-1:1", 1)
    )
    assert applied["envelope_id"] == "envelope:doc:draft-1:1"
    assert applied["replayed"] is False
    props = clean_graph.nodes.properties("doc:draft-1")
    assert props is not None and props["title"] == "doc:draft-1"
    stored = clean_graph.changes.get("envelope:doc:draft-1:1")
    assert stored is not None
    assert (
        clean_graph.changes.content_version("doc:draft-1")["source_version"]["value"]
        == 1
    )


def test_changes_apply_batch_commits_every_draft(clean_graph):
    graph = _graph_name(clean_graph)
    results = clean_graph.changes.apply_batch(
        [
            _envelope(graph, f"doc:batch-{index}", f"changes-e2e:batch:{index}", 1)
            for index in range(3)
        ]
    )
    assert [result["status"] for result in results] == ["applied"] * 3
    for index in range(3):
        assert clean_graph.nodes.properties(f"doc:batch-{index}") is not None
