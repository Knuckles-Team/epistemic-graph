"""Drive a :class:`MemoryCorpus` through the REAL served client.

Every node, embedding, edge, OWL axiom and time-series measurement stages into
ONE server-side transaction (the `TxnClient` surface,
CONCEPT:EG-KG.txn.multi-op-occ-acid) and lands atomically at `commit` -- a
genuine cross-modal ingest through the signed dispatch path, not a
library-internal shortcut. Callers pass a connected
``SyncEpistemicGraphClient`` (or any object exposing the same ``.txn``
namespace); this module has no import-time dependency on the transport.
"""

from __future__ import annotations

from typing import Any

from .graph_corpus import CorpusNode, MemoryCorpus


def _node_properties(node: CorpusNode) -> dict[str, Any]:
    properties: dict[str, Any] = {"type": node.type, "valid_from": node.valid_from}
    if node.text is not None:
        properties["text"] = node.text
    if node.valid_until is not None:
        properties["valid_until"] = node.valid_until
    return properties


def ingest_memory_corpus(client: Any, corpus: MemoryCorpus) -> bool:
    """Stage every node/embedding/edge/axiom/measurement of ``corpus`` into one
    transaction and commit it. Returns the server's commit result (``True`` =>
    applied and durable; ``False`` => OCC conflict, never expected against a
    freshly cleared graph)."""
    txn_id = client.txn.begin()
    for node in corpus.nodes:
        assert client.txn.add_node(txn_id, node.node_id, _node_properties(node)), (
            node.node_id
        )
        if node.embedding is not None:
            assert client.txn.add_embedding(
                txn_id, node.node_id, list(node.embedding)
            ), node.node_id
    for edge in corpus.edges:
        staged = client.txn.add_edge(
            txn_id, edge.source_id, edge.target_id, {"relationship": edge.relationship}
        )
        assert staged, (edge.source_id, edge.target_id)
    assert client.txn.axiom(txn_id, corpus.ontology_turtle)
    assert client.txn.add_measurement(
        txn_id,
        corpus.series.series_id,
        [(ts, [value]) for ts, value in corpus.series.points],
    )
    return bool(client.txn.commit(txn_id))
