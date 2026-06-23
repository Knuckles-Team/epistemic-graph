# CONCEPT:KG-2.19 — Epistemic Graph Service Client
#
# Async Python client for the Tokio-based epistemic-graph service.
# Communicates over UDS or TCP using Length-prefixed MessagePack framing
# with HMAC-SHA256 authentication.

from __future__ import annotations

import asyncio
import builtins
import contextlib
import hashlib
import hmac
import inspect
import logging
import os
import threading
from typing import Any

import msgpack

logger = logging.getLogger(__name__)


class NodeClient:
    """CONCEPT:KG-2.0 — Topology Node Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add(self, node_id: str, properties: dict[str, Any] | None = None) -> None:
        await self._client._send(
            "AddNode",
            {
                "node_id": node_id,
                "properties_msgpack": list(msgpack.packb(properties or {})),
            },
        )

    async def remove(self, node_id: str) -> None:
        await self._client._send("RemoveNode", {"node_id": node_id})

    async def has(self, node_id: str) -> bool:
        return await self._client._send("HasNode", {"node_id": node_id})

    async def compare_and_set(
        self, node_id: str, conditions: dict[str, Any], updates: dict[str, Any]
    ) -> bool:
        """Atomic compare-and-set on a node's property blob (CONCEPT:KG-2 backend-
        agnostic atomic claim). If every ``(field, expected)`` in ``conditions``
        matches the node's current value (a MISSING field reads as ``None``), the
        ``updates`` are merged in and ``True`` is returned; otherwise (node absent,
        any condition fails, or decode fails) the node is untouched and ``False``
        is returned. The read-modify-write runs atomically in the engine, so this
        is a backend-agnostic atomic claim for ``:Task``/``:Loop`` nodes."""
        return await self._client._send(
            "CompareAndSetNodeFields",
            {
                "node_id": node_id,
                "conditions_msgpack": list(msgpack.packb(conditions)),
                "updates_msgpack": list(msgpack.packb(updates)),
            },
        )

    async def list(self) -> builtins.list[tuple[str, str]]:
        return await self._client._send("GetNodes")

    async def list_by_label(
        self, label: str, limit: int = 0
    ) -> builtins.list[tuple[str, Any]]:
        """At most ``limit`` ``(id, properties)`` whose type/label matches ``label``
        (``limit=0`` ⇒ no cap). Bounded-payload alternative to ``list()`` so a
        ``MATCH (n:Label) … LIMIT k`` does not materialize the whole graph."""
        return await self._client._send(
            "GetNodesByLabel", {"label": label, "limit": int(limit)}
        )

    async def properties(self, node_id: str) -> dict[str, Any] | None:
        raw_val = await self._client._send("GetNodeProperties", {"node_id": node_id})
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            import msgpack

            return msgpack.unpackb(raw_val, raw=False)
        return raw_val

    async def properties_batch(
        self, node_ids: builtins.list[str]
    ) -> dict[str, dict[str, Any] | None]:
        """Fetch properties for many nodes in ONE round-trip (CONCEPT:KG-2.16).

        Returns a mapping ``node_id -> properties`` (``None`` for ids absent from
        the graph). Collapses what would be N ``properties()`` calls — and N
        network round-trips — into a single request.
        """
        rows = await self._client._send(
            "GetNodePropertiesBatch", {"node_ids": list(node_ids)}
        )
        out: dict[str, dict[str, Any] | None] = {}
        for entry in rows or []:
            nid, blob = entry[0], entry[1]
            out[nid] = msgpack.unpackb(blob, raw=False) if blob is not None else None
        return out

    async def has_batch(self, node_ids: builtins.list[str]) -> dict[str, bool]:
        """Existence check for many nodes in one round-trip."""
        ids = list(node_ids)
        flags = await self._client._send("HasNodesBatch", {"node_ids": ids})
        return dict(zip(ids, flags or [], strict=False))

    async def count(self) -> int:
        return await self._client._send("NodeCount")

    async def ids(self) -> builtins.list[str]:
        return await self._client._send("NodeIds")

    async def in_degree(self, node_id: str) -> int:
        return await self._client._send("InDegree", {"node_id": node_id})

    async def out_degree(self, node_id: str) -> int:
        return await self._client._send("OutDegree", {"node_id": node_id})

    async def predecessors(self, node_id: str) -> builtins.list[str]:
        return await self._client._send("GetPredecessors", {"node_id": node_id})

    async def successors(self, node_id: str) -> builtins.list[str]:
        return await self._client._send("GetSuccessors", {"node_id": node_id})

    async def neighbors(self, node_id: str) -> builtins.list[str]:
        return await self._client._send("GetNeighbors", {"node_id": node_id})

    # ── Cross-graph union reads (CONCEPT:KG-2.171) ───────────────────────
    # Read across a SET of content graphs as if one, so writes can be partitioned
    # across per-graph write locks (each lane its own graph) while reads see the
    # union. Missing lane graphs in the set are skipped engine-side.

    async def properties_union(
        self, node_id: str, graphs: builtins.list[str]
    ) -> dict[str, Any] | None:
        """First-found node properties across ``graphs`` (in order)."""
        raw_val = await self._client._send(
            "UnionGetNodeProperties", {"graphs": list(graphs), "node_id": node_id}
        )
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            import msgpack

            return msgpack.unpackb(raw_val, raw=False)
        return raw_val

    async def list_by_label_union(
        self, label: str, graphs: builtins.list[str], limit: int = 0
    ) -> builtins.list[tuple[str, Any]]:
        """Label scan unioned + deduped by id across ``graphs`` (``limit=0`` ⇒ no cap)."""
        return await self._client._send(
            "UnionGetNodesByLabel",
            {"graphs": list(graphs), "label": label, "limit": int(limit)},
        )

    async def neighbors_union(
        self, node_id: str, graphs: builtins.list[str]
    ) -> builtins.list[str]:
        """Neighbour ids unioned + deduped across every graph that holds the anchor."""
        return await self._client._send(
            "UnionGetNeighbors", {"graphs": list(graphs), "node_id": node_id}
        )


class EdgeClient:
    """CONCEPT:KG-2.0 — Topology Edge Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add(
        self, source_id: str, target_id: str, properties: dict[str, Any] | None = None
    ) -> None:
        await self._client._send(
            "AddEdge",
            {
                "source_id": source_id,
                "target_id": target_id,
                "properties_msgpack": list(msgpack.packb(properties or {})),
            },
        )

    async def remove(self, source_id: str, target_id: str) -> None:
        await self._client._send(
            "RemoveEdge", {"source_id": source_id, "target_id": target_id}
        )

    async def has(self, source_id: str, target_id: str) -> bool:
        return await self._client._send(
            "HasEdge", {"source_id": source_id, "target_id": target_id}
        )

    async def list(self) -> builtins.list[tuple[str, str, builtins.list[int] | bytes]]:
        return await self._client._send("GetEdges")

    async def properties(self, source_id: str, target_id: str) -> dict[str, Any] | None:
        raw_val = await self._client._send(
            "GetEdgeProperties", {"source_id": source_id, "target_id": target_id}
        )
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            import msgpack

            return msgpack.unpackb(raw_val, raw=False)
        return raw_val

    async def properties_batch(
        self, edges: builtins.list[tuple[str, str]]
    ) -> builtins.list[builtins.list[dict[str, Any]]]:
        """Fetch properties for many edges in ONE round-trip (CONCEPT:KG-2.16).

        Returns a list parallel to ``edges``; each element is the list of property
        dicts for that ``(source, target)`` pair (a pair may carry multiple edges;
        an empty list means no such edge).
        """
        pairs = [list(e) for e in edges]
        rows = await self._client._send("GetEdgePropertiesBatch", {"edges": pairs})
        out: builtins.list[builtins.list[dict[str, Any]]] = []
        for per_edge in rows or []:
            out.append(
                [
                    msgpack.unpackb(blob, raw=False)
                    for blob in per_edge
                    if blob is not None
                ]
            )
        return out

    async def count(self) -> int:
        return await self._client._send("EdgeCount")


class GraphOperationsClient:
    """CONCEPT:KG-2.6 — Graph Algorithms Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def clear(self) -> None:
        await self._client._send("ClearGraph")

    async def parse_repository(self, root_path: str) -> None:
        await self._client._send("ParseRepository", {"root_path": root_path})

    async def parse_file(self, file_path: str, source: bytes) -> dict[str, Any]:
        return await self._client._send(
            "ParseFile", {"file_path": file_path, "source": source}
        )

    async def parse_files(self, files: list[tuple[str, bytes]]) -> list[dict[str, Any]]:
        """Parse many files in ONE round-trip (CONCEPT:KG-2.16 batch op).

        ``files`` is a list of ``(file_path, source_bytes)``. Returns one parse
        result per input file, **in input order**, each with the same shape as
        :meth:`parse_file`. The payload mirrors the ``BatchUpdate`` convention: a
        single MessagePack blob (``Vec<(String, bytes)>`` engine-side).
        """
        blob = msgpack.packb([[fp, src] for fp, src in files])
        return await self._client._send("ParseFiles", {"files_msgpack": blob})

    async def index_repository(self, files: list[tuple[str, bytes]]) -> dict[str, Any]:
        """Parse a batch AND resolve cross-file edges in ONE round-trip
        (CONCEPT:KG-2.8r).

        ``files`` is a list of ``(file_path, source_bytes)`` — the SAME blob as
        :meth:`parse_files`, but the batch is treated as one resolution scope (a
        repository, or a delta set). Unlike :meth:`parse_files` (one raw result
        per file), this returns a SINGLE merged ``IndexResult`` dict::

            {"nodes": [...], "edges": [...],          # IMPLEMENTS + resolved
             "symbols_extracted": int, "files_parsed": int,
             "calls_resolved": int, "calls_unresolved": int,
             "imports_resolved": int, "imports_unresolved": int}

        ``edges`` carry resolved ``calls`` (symbol→symbol) and ``depends_on``
        (file→file) edge types pointing at real node ids — the cross-file step
        feature clustering / impact analysis run over. Use this to ingest a
        repository's symbol graph; use :meth:`parse_files` only when per-file raw
        results are wanted.
        """
        blob = msgpack.packb([[fp, src] for fp, src in files])
        return await self._client._send("IndexRepository", {"files_msgpack": blob})

    async def add_embedding(self, node_id: str, embedding: list[float]) -> None:
        await self._client._send(
            "AddEmbedding", {"node_id": node_id, "embedding": embedding}
        )

    async def semantic_search(
        self, query_embedding: list[float], n_results: int = 5
    ) -> list[tuple[str, float]]:
        return await self._client._send(
            "SemanticSearch",
            {"query_embedding": query_embedding, "n_results": n_results},
        )

    async def match_ontology_terms(self, query: str) -> list[dict[str, Any]]:
        """CONCEPT:EG-010 — embedding-free lexical classification gate.

        Returns the capability-node terms (Tool/Skill/MCPServer names+synonyms)
        that appear as whole words in ``query``, each as
        ``{term, node_type, label, score}``. The "free" tier between structural
        routing and semantic search: a non-empty result means the turn names a
        real fleet capability and should escalate to the full graph.
        """
        return await self._client._send(
            "MatchOntologyTerms",
            {"query": query},
        )

    async def spectral_cluster(
        self, vectors: list[list[float]], max_k: int, domain: str
    ) -> list[dict[str, Any]]:
        return await self._client._send(
            "SpectralCluster", {"vectors": vectors, "max_k": max_k, "domain": domain}
        )

    async def hypergraph_encode_interaction(
        self,
        pos_a: int,
        pos_b: int,
        pos_dim: int,
        hidden_dim: int,
        out_dim: int,
        seed: int,
    ) -> list[float]:
        return await self._client._send(
            "HypergraphEncodeInteraction",
            {
                "pos_a": pos_a,
                "pos_b": pos_b,
                "pos_dim": pos_dim,
                "hidden_dim": hidden_dim,
                "out_dim": out_dim,
                "seed": seed,
            },
        )

    async def batch_cosine_similarity(
        self, query: list[float], targets: list[list[float]]
    ) -> list[float]:
        return await self._client._send(
            "BatchCosineSimilarity", {"query": query, "targets": targets}
        )

    async def find_similar_pairs(
        self,
        embeddings: list[list[float]],
        ids: list[str],
        threshold: float,
        use_lsh: bool,
        lsh_num_tables: int,
        lsh_hash_size: int,
        seed: int,
    ) -> list[tuple[str, str, float]]:
        return await self._client._send(
            "FindSimilarPairs",
            {
                "embeddings": embeddings,
                "ids": ids,
                "threshold": threshold,
                "use_lsh": use_lsh,
                "lsh_num_tables": lsh_num_tables,
                "lsh_hash_size": lsh_hash_size,
                "seed": seed,
            },
        )

    async def vf2_subgraph_match(
        self, pattern: EpistemicGraphClient
    ) -> list[dict[str, str]]:
        return await self._client._send(
            "Vf2SubgraphMatch", {"pattern_graph_name": pattern._graph_name}
        )

    async def topological_sort(self) -> list[str]:
        return await self._client._send("TopologicalSort")

    async def find_cycle(self) -> list[str] | None:
        return await self._client._send("FindCycle")

    async def shortest_path(self, source_id: str, target_id: str) -> list[str] | None:
        return await self._client._send(
            "GetShortestPath", {"source_id": source_id, "target_id": target_id}
        )

    async def blast_radius(self, node_id: str, max_depth: int) -> list[str]:
        return await self._client._send(
            "GetBlastRadius", {"node_id": node_id, "max_depth": max_depth}
        )

    async def get_subgraph(self, node_ids: list[str]) -> dict[str, Any]:
        """Batch-fetch the induced subgraph in ONE round-trip.

        Returns ``{"nodes": [{"id", "properties"}, ...], "edges":
        [{"source", "target", "properties"}, ...]}`` with properties already
        decoded server-side. Replaces N per-node ``GetNodeProperties`` calls plus
        a full ``GetEdges`` scan — ship the node-id set, get everything back once.
        """
        return await self._client._send("GetSubgraph", {"node_ids": node_ids})

    async def connected_components(self) -> list[list[str]]:
        return await self._client._send("ConnectedComponents")

    async def strongly_connected_components(self) -> list[list[str]]:
        """CONCEPT:KG-2.16 — Tarjan's SCC via Tokio service."""
        return await self._client._send("StronglyConnectedComponents")

    async def minimum_spanning_tree(self) -> list[tuple[str, str, float]]:
        """CONCEPT:KG-2.16 — Kruskal's MST via Tokio service."""
        return await self._client._send("MinimumSpanningTree")

    async def community_detection(self, resolution: float = 1.0) -> list[list[str]]:
        return await self._client._send(
            "CommunityDetection", {"resolution": resolution}
        )

    async def community_detect_ephemeral(
        self,
        node_ids: list[str],
        edges: list[tuple[str, str]],
        resolution: float = 1.0,
    ) -> list[list[str]]:
        """Stateless community detection over an inline call graph (Phase: holistic).

        Runs detection on the passed nodes/edges WITHOUT loading them into a tenant
        — no bulk-load round-trip, no throwaway tenant, no persistence. Replaces the
        load-tenant-then-detect pattern for the ingest community pass.
        """
        return await self._client._send(
            "CommunityDetectEphemeral",
            {"node_ids": node_ids, "edges": edges, "resolution": resolution},
        )

    async def graph_coloring(self) -> list[tuple[str, int]]:
        return await self._client._send("GraphColoring")

    async def compute_similarity_edges(self, threshold: float) -> int:
        return await self._client._send(
            "ComputeSimilarityEdges", {"threshold": threshold}
        )


class AnalyticsClient:
    """CONCEPT:KG-2.6 — Analytics and Centrality Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def degree_centrality(self, node_id: str) -> float:
        return await self._client._send("DegreeCentrality", {"node_id": node_id})

    async def degree_centrality_all(self) -> list[tuple[str, float]]:
        return await self._client._send("DegreeCentralityAll")

    async def betweenness_centrality(self) -> list[tuple[str, float]]:
        return await self._client._send("BetweennessCentrality")

    async def pagerank(
        self, damping: float = 0.85, iterations: int = 100
    ) -> list[tuple[str, float]]:
        return await self._client._send(
            "PageRank", {"damping": damping, "iterations": iterations}
        )

    async def personalized_pagerank(
        self,
        seed_nodes: list[tuple[str, float]],
        damping: float = 0.85,
        iterations: int = 100,
    ) -> list[tuple[str, float]]:
        return await self._client._send(
            "PersonalizedPageRank",
            {"seed_nodes": seed_nodes, "damping": damping, "iterations": iterations},
        )


class LifecycleClient:
    """CONCEPT:KG-2.6 — Lifecycle and State Management Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def prune(self, max_age_secs: int, min_score: float) -> int:
        return await self._client._send(
            "PruneByLifecycle", {"max_age_secs": max_age_secs, "min_score": min_score}
        )

    async def get_context_view(self, agent_id: str, max_tokens: int = 4096) -> str:
        return await self._client._send(
            "GetContextView", {"agent_id": agent_id, "max_tokens": max_tokens}
        )

    async def batch_update(self, operations: list[dict[str, Any]]) -> Any:
        return await self._client._send(
            "BatchUpdate", {"operations_msgpack": list(msgpack.packb(operations))}
        )

    async def metrics(self) -> dict[str, Any]:
        return await self._client._send("Metrics")

    async def to_msgpack(self) -> bytes:
        return await self._client._send("ToMsgpack")

    async def from_msgpack(self, msgpack_bytes: bytes) -> None:
        await self._client._send("FromMsgpack", {"msgpack": msgpack_bytes})

    async def evict_lru(self, max_nodes: int) -> int:
        """Evict oldest nodes to enforce max_nodes cap. Returns eviction count."""
        return await self._client._send("EvictLRU", {"max_nodes": max_nodes})

    async def decay_sweep(
        self,
        half_life_secs: float = 604_800.0,
        floor: float = 0.0,
        prune: bool = False,
    ) -> dict[str, Any]:
        """CONCEPT:KG-2.16 — Ebbinghaus forgetting-curve decay.

        Decays every node's and edge's belief ``confidence`` by
        ``R = 0.5 ** (Δt / half_life_secs)`` since its last access, persisting the
        result and advancing the access clock so repeated sweeps compound exactly.
        With ``prune=True`` (or a positive ``floor``), items whose decayed
        confidence falls below ``floor`` are removed. The server is the time
        authority. Returns ``{nodes_decayed, edges_decayed, nodes_pruned,
        edges_pruned}``.
        """
        return await self._client._send(
            "DecaySweep",
            {"half_life_secs": half_life_secs, "floor": floor, "prune": prune},
        )

    async def touch_nodes(self, node_ids: list[str]) -> int:
        """Refresh nodes on access (spaced repetition): reset the forgetting clock
        and restore ``confidence = 1.0``. Returns the number of nodes touched."""
        return await self._client._send("TouchNodes", {"node_ids": node_ids})


class ReasoningClient:
    """CONCEPT:KG-2.17 — Compiled Semantic Reasoner Namespace.

    Forward-chaining OWL/RDFS inference executed in the Rust engine. Materialises
    inferred edges and type annotations in-place and returns the inferred triples.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def reason(
        self,
        subclass_relations: list[tuple[str, str]] | None = None,
        subproperty_relations: list[tuple[str, str]] | None = None,
        symmetric_properties: list[str] | None = None,
        transitive_properties: list[str] | None = None,
        inverse_properties: list[tuple[str, str]] | None = None,
        domain_rules: list[tuple[str, str]] | None = None,
        range_rules: list[tuple[str, str]] | None = None,
        property_chains: list[tuple[str, str, str]] | None = None,
    ) -> dict[str, Any]:
        """Run one fixpoint of Datalog reasoning plus optional domain/range and
        property-chain inference over the current graph.

        Every rule set is optional; omitted sets are treated as empty. Returns
        ``{"inferred_count": int, "inferred_triples": [{subject, predicate,
        object, inference_type}, ...]}``. The inferred edges/types are also
        persisted into the graph as a side effect.
        """
        return await self._client._send(
            "RunDatalogReasoning",
            {
                "subclass_relations": [list(t) for t in (subclass_relations or [])],
                "subproperty_relations": [
                    list(t) for t in (subproperty_relations or [])
                ],
                "symmetric_properties": list(symmetric_properties or []),
                "transitive_properties": list(transitive_properties or []),
                "inverse_properties": [list(t) for t in (inverse_properties or [])],
                "domain_rules": [list(t) for t in (domain_rules or [])],
                "range_rules": [list(t) for t in (range_rules or [])],
                "property_chains": [list(t) for t in (property_chains or [])],
            },
        )


class LedgerClient:
    """CONCEPT:KG-2.0 — Ledger Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def get(self) -> list[str]:
        return await self._client._send("GetLedger")

    async def clear(self) -> None:
        await self._client._send("ClearLedger")

    async def apply(self, transactions: list[str]) -> None:
        await self._client._send("ApplyLedger", {"transactions": transactions})


class ChannelsClient:
    """CONCEPT:KG-2.0 — Dynamic Communication Channels Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def create(
        self,
        channel_id: str,
        channel_type: str = "Group",
        creator: str = "",
        initial_members: list[str] | None = None,
    ) -> None:
        await self._client._send(
            "CreateChannel",
            {
                "channel_id": channel_id,
                "channel_type": channel_type,
                "creator": creator,
                "initial_members": initial_members or [],
            },
        )

    async def join(self, channel_id: str, agent_id: str) -> None:
        await self._client._send(
            "JoinChannel", {"channel_id": channel_id, "agent_id": agent_id}
        )

    async def leave(self, channel_id: str, agent_id: str) -> Any:
        return await self._client._send(
            "LeaveChannel", {"channel_id": channel_id, "agent_id": agent_id}
        )

    async def close(
        self,
        channel_id: str,
        summary_embedding: list[float] | None = None,
        topic_metadata: str | None = None,
    ) -> Any:
        return await self._client._send(
            "CloseChannel",
            {
                "channel_id": channel_id,
                "summary_embedding": summary_embedding,
                "topic_metadata": topic_metadata,
            },
        )

    async def send_message(self, channel_id: str, sender: str, payload: str) -> None:
        await self._client._send(
            "SendMessage",
            {"channel_id": channel_id, "sender": sender, "payload": payload},
        )

    async def get_messages(
        self, channel_id: str, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return await self._client._send(
            "GetChannelMessages", {"channel_id": channel_id, "limit": limit}
        )

    async def list(self) -> builtins.list[dict[str, Any]]:
        return await self._client._send("ListChannels")

    async def get_members(self, channel_id: str) -> builtins.list[str]:
        return await self._client._send("GetChannelMembers", {"channel_id": channel_id})


class MultiTenantClient:
    """CONCEPT:KG-2.6 — Multi-Tenant Management Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def create(self, graph_name: str, graph_type: str = "Agent") -> None:
        await self._client._send(
            "CreateGraph", {"graph_name": graph_name, "graph_type": graph_type}
        )

    async def delete(self, graph_name: str) -> None:
        await self._client._send("DeleteGraph", {"graph_name": graph_name})

    async def list(self) -> list[dict[str, str]]:
        return await self._client._send("ListGraphs")


class ConsensusClient:
    """CONCEPT:KG-2.6 — Zero-Trust Consensus Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def register_identity(
        self, agent_id: str, role: str, teams: list[str], signature: str
    ) -> str:
        return await self._client._send(
            "RegisterIdentity",
            {
                "agent_id": agent_id,
                "role": role,
                "teams": teams,
                "signature": signature,
            },
        )

    async def apply_multisig_mutation(
        self, signatures: list[str], threshold: int, mutation_type: str, query: str
    ) -> str:
        return await self._client._send(
            "ApplyMultisigMutation",
            {
                "signatures": signatures,
                "threshold": threshold,
                "mutation_type": mutation_type,
                "query": query,
            },
        )


class FinanceClient:
    """CONCEPT:KG-2.6 — Quantitative Finance Namespace"""

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def optimize_portfolio(
        self,
        expected_returns: list[float],
        cov_matrix: list[list[float]],
        risk_free_rate: float,
        min_weight: float | None = None,
        max_weight: float | None = None,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceOptimizePortfolio",
            {
                "expected_returns": expected_returns,
                "cov_matrix": cov_matrix,
                "risk_free_rate": risk_free_rate,
                "min_weight": min_weight,
                "max_weight": max_weight,
            },
        )

    async def risk_parity(self, cov_matrix: list[list[float]]) -> dict[str, Any]:
        return await self._client._send(
            "FinanceRiskParity",
            {"cov_matrix": cov_matrix},
        )

    async def black_litterman(
        self,
        market_weights: list[float],
        cov_matrix: list[list[float]],
        views: list[float],
        pick_matrix: list[list[float]],
        tau: float,
        risk_aversion: float,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceBlackLitterman",
            {
                "market_weights": market_weights,
                "cov_matrix": cov_matrix,
                "views": views,
                "pick_matrix": pick_matrix,
                "tau": tau,
                "risk_aversion": risk_aversion,
            },
        )

    async def efficient_frontier(
        self,
        expected_returns: list[float],
        cov_matrix: list[list[float]],
        target_return: float,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceEfficientFrontier",
            {
                "expected_returns": expected_returns,
                "cov_matrix": cov_matrix,
                "target_return": target_return,
            },
        )

    # ── Risk metrics ──────────────────────────────────────────────────
    async def var(self, returns: list[float], confidence: float = 0.95) -> float:
        return await self._client._send(
            "FinanceVar", {"returns": returns, "confidence": confidence}
        )

    async def cvar(self, returns: list[float], confidence: float = 0.95) -> float:
        return await self._client._send(
            "FinanceCvar", {"returns": returns, "confidence": confidence}
        )

    async def max_drawdown(self, returns: list[float]) -> float:
        return await self._client._send("FinanceMaxDrawdown", {"returns": returns})

    async def drawdown_series(self, returns: list[float]) -> list[float]:
        return await self._client._send("FinanceDrawdownSeries", {"returns": returns})

    async def downside_deviation(
        self, returns: list[float], target: float = 0.0
    ) -> float:
        return await self._client._send(
            "FinanceDownsideDeviation", {"returns": returns, "target": target}
        )

    async def risk_metrics(
        self, returns: list[float], risk_free_rate: float = 0.0
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceRiskMetrics",
            {"returns": returns, "risk_free_rate": risk_free_rate},
        )

    async def monte_carlo_var(
        self,
        mean: float,
        std_dev: float,
        n_simulations: int = 10000,
        confidence: float = 0.95,
    ) -> float:
        return await self._client._send(
            "FinanceMonteCarloVar",
            {
                "mean": mean,
                "std_dev": std_dev,
                "n_simulations": n_simulations,
                "confidence": confidence,
            },
        )

    async def stress_test(
        self,
        weights: list[float],
        expected_returns: list[float],
        cov_matrix: list[list[float]],
        shock_factors: list[float],
    ) -> list[float]:
        return await self._client._send(
            "FinanceStressTest",
            {
                "weights": weights,
                "expected_returns": expected_returns,
                "cov_matrix": cov_matrix,
                "shock_factors": shock_factors,
            },
        )

    # ── Regime detection (HMM) ────────────────────────────────────────
    async def detect_regimes(
        self,
        observations: list[float],
        n_states: int = 2,
        max_iter: int = 100,
        tol: float = 1e-4,
    ) -> dict[str, Any]:
        return await self._client._send(
            "FinanceDetectRegimes",
            {
                "observations": observations,
                "n_states": n_states,
                "max_iter": max_iter,
                "tol": tol,
            },
        )

    # ── Signals / alpha ───────────────────────────────────────────────
    async def rolling_zscore(self, values: list[float], window: int) -> list[float]:
        return await self._client._send(
            "FinanceRollingZscore", {"values": values, "window": window}
        )

    async def ewma(self, values: list[float], span: int) -> list[float]:
        return await self._client._send("FinanceEwma", {"values": values, "span": span})

    async def signal_decay(self, signal: list[float], half_life: float) -> list[float]:
        return await self._client._send(
            "FinanceSignalDecay", {"signal": signal, "half_life": half_life}
        )

    async def combine_alphas(
        self, signals: list[list[float]], weights: list[float]
    ) -> list[float]:
        return await self._client._send(
            "FinanceCombineAlphas", {"signals": signals, "weights": weights}
        )

    async def cross_sectional_rank(
        self, cross_section: list[list[float]]
    ) -> list[list[float]]:
        return await self._client._send(
            "FinanceCrossSectionalRank", {"cross_section": cross_section}
        )

    async def momentum(self, prices: list[float], lookback: int) -> list[float]:
        return await self._client._send(
            "FinanceMomentum", {"prices": prices, "lookback": lookback}
        )

    async def mean_reversion(self, values: list[float], window: int) -> list[float]:
        return await self._client._send(
            "FinanceMeanReversion", {"values": values, "window": window}
        )

    async def information_coefficient(
        self, signal: list[float], forward_returns: list[float]
    ) -> float:
        return await self._client._send(
            "FinanceInformationCoefficient",
            {"signal": signal, "forward_returns": forward_returns},
        )

    # ── Execution / microstructure ────────────────────────────────────
    async def twap(
        self,
        total_quantity: float,
        n_slices: int,
        start_time: int = 0,
        interval_secs: int = 60,
    ) -> list[tuple[int, float]]:
        return await self._client._send(
            "FinanceTwap",
            {
                "total_quantity": total_quantity,
                "n_slices": n_slices,
                "start_time": start_time,
                "interval_secs": interval_secs,
            },
        )

    async def vwap(
        self,
        total_quantity: float,
        volume_profile: list[float],
        start_time: int = 0,
        interval_secs: int = 60,
    ) -> list[tuple[int, float]]:
        return await self._client._send(
            "FinanceVwap",
            {
                "total_quantity": total_quantity,
                "volume_profile": volume_profile,
                "start_time": start_time,
                "interval_secs": interval_secs,
            },
        )

    async def market_impact(
        self,
        daily_volatility: float,
        order_quantity: float,
        average_daily_volume: float,
        impact_coefficient: float = 0.1,
    ) -> float:
        return await self._client._send(
            "FinanceMarketImpact",
            {
                "daily_volatility": daily_volatility,
                "order_quantity": order_quantity,
                "average_daily_volume": average_daily_volume,
                "impact_coefficient": impact_coefficient,
            },
        )

    async def pairs_trading(
        self, prices_a: list[float], prices_b: list[float], lookback: int
    ) -> list[float]:
        return await self._client._send(
            "FinancePairsTrading",
            {"prices_a": prices_a, "prices_b": prices_b, "lookback": lookback},
        )

    async def match_orders(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Match a limit-order book. Each order: {id, side, price, quantity, timestamp}."""
        return await self._client._send("FinanceMatchOrders", {"orders": orders})

    # ── Market making / microstructure (CONCEPT:KG-2.20f) ──────────────
    async def avellaneda_stoikov(
        self,
        mid: float,
        inventory: float,
        sigma: float,
        gamma: float,
        kappa: float,
        tau: float,
    ) -> dict[str, Any]:
        """Optimal AS quotes around a freely-drifting mid. Returns
        {bid, ask, reservation, half_spread, withdraw}."""
        return await self._client._send(
            "FinanceAvellanedaStoikov",
            {
                "mid": mid,
                "inventory": inventory,
                "sigma": sigma,
                "gamma": gamma,
                "kappa": kappa,
                "tau": tau,
            },
        )

    async def glt_quotes(
        self,
        mid: float,
        inventory: float,
        sigma: float,
        gamma: float,
        kappa: float,
        a: float,
    ) -> dict[str, Any]:
        """Guéant-Lehalle-Fernandez-Tapia closed-form quotes with inventory skew."""
        return await self._client._send(
            "FinanceGltQuotes",
            {
                "mid": mid,
                "inventory": inventory,
                "sigma": sigma,
                "gamma": gamma,
                "kappa": kappa,
                "a": a,
            },
        )

    async def logit_quotes(
        self,
        p_mid: float,
        inventory: float,
        sigma: float,
        gamma: float,
        kappa: float,
        tau: float,
        boundary_m: float = 0.0,
    ) -> dict[str, Any]:
        """Logit-space AS quotes for bounded (0,1) prediction-market prices, with
        a boundary-aware inventory cap. ``withdraw=True`` ⇒ pull quotes."""
        return await self._client._send(
            "FinanceLogitQuotes",
            {
                "p_mid": p_mid,
                "inventory": inventory,
                "sigma": sigma,
                "gamma": gamma,
                "kappa": kappa,
                "tau": tau,
                "boundary_m": boundary_m,
            },
        )

    async def glosten_milgrom_spread(self, alpha: float, p: float) -> float:
        return await self._client._send(
            "FinanceGlostenMilgromSpread", {"alpha": alpha, "p": p}
        )

    async def expected_pnl_rate(
        self,
        delta: float,
        a: float,
        kappa: float,
        alpha: float,
        p: float,
        v_h: float = 1.0,
        v_l: float = 0.0,
    ) -> float:
        return await self._client._send(
            "FinanceExpectedPnlRate",
            {
                "delta": delta,
                "a": a,
                "kappa": kappa,
                "alpha": alpha,
                "p": p,
                "v_h": v_h,
                "v_l": v_l,
            },
        )

    async def breakeven_alpha(
        self, delta: float, p: float, v_h: float = 1.0, v_l: float = 0.0
    ) -> float:
        return await self._client._send(
            "FinanceBreakevenAlpha", {"delta": delta, "p": p, "v_h": v_h, "v_l": v_l}
        )

    async def ofi_series(
        self,
        ts: list[float],
        bid_px: list[float],
        bid_sz: list[float],
        ask_px: list[float],
        ask_sz: list[float],
        window_secs: float = 1.0,
    ) -> list[float]:
        """Cont-Kukanov-Stoikov rolling order-flow imbalance over book events."""
        return await self._client._send(
            "FinanceOfiSeries",
            {
                "ts": ts,
                "bid_px": bid_px,
                "bid_sz": bid_sz,
                "ask_px": ask_px,
                "ask_sz": ask_sz,
                "window_secs": window_secs,
            },
        )

    async def microprice_series(
        self,
        bid_px: list[float],
        bid_sz: list[float],
        ask_px: list[float],
        ask_sz: list[float],
    ) -> list[float]:
        return await self._client._send(
            "FinanceMicropriceSeries",
            {"bid_px": bid_px, "bid_sz": bid_sz, "ask_px": ask_px, "ask_sz": ask_sz},
        )

    async def vpin_pm(
        self,
        buy_vol: list[float],
        sell_vol: list[float],
        p_mean: list[float],
    ) -> float:
        """VPIN toxicity normalised for binary-payoff variance (prediction markets)."""
        return await self._client._send(
            "FinanceVpinPm",
            {"buy_vol": buy_vol, "sell_vol": sell_vol, "p_mean": p_mean},
        )

    async def hawkes_mle(
        self, times: list[float], t_horizon: float, max_iter: int = 200
    ) -> dict[str, Any]:
        """Fit an exponential-kernel Hawkes process. Returns mu/alpha/beta plus
        branching_ratio (>0.95 ⇒ near-critical / crash early-warning)."""
        return await self._client._send(
            "FinanceHawkesMle",
            {"times": times, "t_horizon": t_horizon, "max_iter": max_iter},
        )

    async def hardiman_bouchaud(
        self, times: list[float], t_horizon: float, n_windows: int = 100
    ) -> float:
        """Model-free Hawkes branching ratio from count over-dispersion."""
        return await self._client._send(
            "FinanceHardimanBouchaud",
            {"times": times, "t_horizon": t_horizon, "n_windows": n_windows},
        )

    # ── Kyle insider/stealth surveillance (CONCEPT:KG-2.20k) ───────────
    async def kyle_lambda(
        self, price_changes: list[float], signed_order_flow: list[float]
    ) -> float:
        """Empirical Kyle's λ — price impact (depth) per unit signed net order flow."""
        return await self._client._send(
            "FinanceKyleLambda",
            {"price_changes": price_changes, "signed_order_flow": signed_order_flow},
        )

    async def surveillance_risk(
        self,
        buy_vol: list[float],
        sell_vol: list[float],
        p_mean: list[float],
        signed_flow: list[float],
        price_changes: list[float],
        baseline_sigma: float = 0.0,
    ) -> dict[str, Any]:
        """Kyle insider/stealth-trading surveillance scores (CONCEPT:KG-2.20k).

        Returns ``kyle_lambda``, ``informed_share`` (VPIN α), ``detection_hazard``,
        ``cumulative_suspicion``, ``stealth_ratio`` and ``legal_risk_score`` ∈ [0,1].
        DEFENSIVE use: informed-flow detection + maker adverse-selection protection.
        Pass ``baseline_sigma`` ≤ 0 to use the sample std of ``signed_flow``.
        """
        return await self._client._send(
            "FinanceSurveillanceRisk",
            {
                "buy_vol": buy_vol,
                "sell_vol": sell_vol,
                "p_mean": p_mean,
                "signed_flow": signed_flow,
                "price_changes": price_changes,
                "baseline_sigma": baseline_sigma,
            },
        )

    # ── Position sizing (CONCEPT:KG-2.20f) ─────────────────────────────
    async def kelly_fraction(self, q: float, c: float, fraction: float = 0.25) -> float:
        """Fractional Kelly for a YES contract: f* = (q−c)/(1−c), scaled."""
        return await self._client._send(
            "FinanceKellyFraction", {"q": q, "c": c, "fraction": fraction}
        )

    async def bayesian_kelly(
        self, alpha: float, beta: float, c: float, n_quadrature: int = 50
    ) -> float:
        """Kelly under a Beta(α,β) posterior over the true probability — shrinks
        the bet as posterior variance grows."""
        return await self._client._send(
            "FinanceBayesianKelly",
            {"alpha": alpha, "beta": beta, "c": c, "n_quadrature": n_quadrature},
        )

    async def posterior_credible_interval(
        self, alpha: float, beta: float, level: float = 0.05
    ) -> dict[str, float]:
        return await self._client._send(
            "FinancePosteriorCredibleInterval",
            {"alpha": alpha, "beta": beta, "level": level},
        )

    # ── Backtest validation (CONCEPT:KG-2.20f) ─────────────────────────
    async def purged_cpcv(
        self,
        n_samples: int,
        n_groups: int = 6,
        n_test_groups: int = 2,
        purge_window: int = 0,
        embargo: int = 0,
    ) -> list[dict[str, list[int]]]:
        """Purged combinatorial CV splits — each {train: [...], test: [...]}."""
        return await self._client._send(
            "FinancePurgedCpcv",
            {
                "n_samples": n_samples,
                "n_groups": n_groups,
                "n_test_groups": n_test_groups,
                "purge_window": purge_window,
                "embargo": embargo,
            },
        )

    async def deflated_sharpe(
        self, observed_sr: float, n_trials: int, sr_returns: list[float]
    ) -> float:
        """Probability the observed Sharpe beats zero after deflating for trials
        and non-normality (Bailey & López de Prado). DSR > 0.95 = strong."""
        return await self._client._send(
            "FinanceDeflatedSharpe",
            {
                "observed_sr": observed_sr,
                "n_trials": n_trials,
                "sr_returns": sr_returns,
            },
        )

    async def probability_backtest_overfit(
        self, insample: list[list[float]], oos: list[list[float]]
    ) -> float:
        """PBO — rows = CV splits, cols = strategies. < 0.3 robust; > 0.5 overfit."""
        return await self._client._send(
            "FinanceProbabilityBacktestOverfit",
            {"insample": insample, "oos": oos},
        )

    async def diebold_mariano(
        self, losses_a: list[float], losses_b: list[float], h: int = 1
    ) -> dict[str, Any]:
        """Test of equal predictive accuracy (Newey-West HAC for h>1)."""
        return await self._client._send(
            "FinanceDieboldMariano",
            {"losses_a": losses_a, "losses_b": losses_b, "h": h},
        )

    # ── Forensic accounting (CONCEPT:KG-2.20g) ─────────────────────────
    async def forensic_report(
        self, this_year: dict[str, Any], prior_year: dict[str, Any]
    ) -> dict[str, Any]:
        """Beneish M / Altman Z / Piotroski F / Sloan accruals over two fiscal
        years. Returns scores + flags + verdict (INVESTIGATE | CLEAN). Each year
        dict carries standardized line items (sales, cogs, net_income, cfo, ...)."""
        return await self._client._send(
            "FinanceForensicReport",
            {"this_year": this_year, "prior_year": prior_year},
        )

    # ── State-space / stat-arb (CONCEPT:KG-2.20h) ──────────────────────
    async def kalman_filter_1d(
        self,
        observations: list[float],
        f: float = 1.0,
        q: float = 1e-5,
        h: float = 1.0,
        r: float = 1e-3,
        x0: float = 0.0,
        p0: float = 1.0,
    ) -> dict[str, Any]:
        """Scalar Kalman filter — returns {states, variances} per step."""
        return await self._client._send(
            "FinanceKalmanFilter1d",
            {
                "observations": observations,
                "f": f,
                "q": q,
                "h": h,
                "r": r,
                "x0": x0,
                "p0": p0,
            },
        )

    async def kalman_beta(
        self,
        market_returns: list[float],
        asset_returns: list[float],
        q: float = 1e-5,
        r: float = 1e-3,
        beta0: float = 1.0,
        p0: float = 1.0,
    ) -> dict[str, Any]:
        """Dynamic (time-varying) beta via Kalman filter — {states (betas), variances}.
        OLS gives the average; this gives the current hidden beta with uncertainty."""
        return await self._client._send(
            "FinanceKalmanBeta",
            {
                "market_returns": market_returns,
                "asset_returns": asset_returns,
                "q": q,
                "r": r,
                "beta0": beta0,
                "p0": p0,
            },
        )

    async def kalman_volatility(
        self,
        returns: list[float],
        q: float = 0.1,
        r: float = 1.0,
        log_var0: float | None = None,
        p0: float = 1.0,
        annualization: float = 252.0,
    ) -> list[float]:
        """Kalman volatility tracker (log-variance state) — annualised vol series.
        Tells you what volatility *is* now, not what it was (vs GARCH/EWMA)."""
        return await self._client._send(
            "FinanceKalmanVolatility",
            {
                "returns": returns,
                "q": q,
                "r": r,
                "log_var0": log_var0,
                "p0": p0,
                "annualization": annualization,
            },
        )

    async def adf_test(self, series: list[float], max_lag: int = 1) -> dict[str, Any]:
        """Augmented Dickey-Fuller cointegration/stationarity test — returns
        {statistic, crit_5pct, stationary_5pct, ...}."""
        return await self._client._send(
            "FinanceAdfTest", {"series": series, "max_lag": max_lag}
        )

    async def ou_calibrate(
        self, spread: list[float], dt: float = 1.0
    ) -> dict[str, Any]:
        """Calibrate an Ornstein-Uhlenbeck mean-reversion process from a spread —
        {theta, mu, sigma, half_life, sigma_eq}."""
        return await self._client._send(
            "FinanceOuCalibrate", {"spread": spread, "dt": dt}
        )

    async def ou_optimal_thresholds(
        self,
        theta: float,
        mu: float,
        sigma: float,
        sigma_eq: float,
        cost: float = 0.0,
    ) -> dict[str, Any]:
        """MFPT-optimal OU entry/exit band — {entry_long, entry_short, exit, z,
        expected_return_per_unit_time}."""
        return await self._client._send(
            "FinanceOuOptimalThresholds",
            {
                "theta": theta,
                "mu": mu,
                "sigma": sigma,
                "sigma_eq": sigma_eq,
                "cost": cost,
            },
        )

    async def markov_transition_matrix(
        self, states: list[int], n_states: int
    ) -> list[list[float]]:
        """Laplace-smoothed row-stochastic transition matrix from a state sequence
        (cross-venue lead-lag / regime transitions)."""
        return await self._client._send(
            "FinanceMarkovTransitionMatrix", {"states": states, "n_states": n_states}
        )

    # ── Signal combination / sizing / calibration (CONCEPT:KG-2.20i) ───
    async def order_book_imbalance(
        self, v_bid: list[float], v_ask: list[float]
    ) -> list[float]:
        """Level-1 order-book imbalance series ∈ [−1, 1]."""
        return await self._client._send(
            "FinanceOrderBookImbalance", {"v_bid": v_bid, "v_ask": v_ask}
        )

    async def queue_imbalance(
        self,
        bid_q: list[float],
        ask_q: list[float],
        bid_rate: list[float],
        ask_rate: list[float],
    ) -> dict[str, Any]:
        """Queue-position / time-to-fill signal at the best bid/ask. Returns
        {skew, bid_fill_time, ask_fill_time}; skew = (ask_q−bid_q)/(ask_q+bid_q)
        (positive ⇒ ask queue heavier ⇒ resting bid fills faster)."""
        return await self._client._send(
            "FinanceQueueImbalance",
            {
                "bid_q": bid_q,
                "ask_q": ask_q,
                "bid_rate": bid_rate,
                "ask_rate": ask_rate,
            },
        )

    async def realized_vol_tick(
        self, mid: list[float], window: int = 20
    ) -> list[float]:
        """Tick-level rolling realized volatility of the mid-price (model-free;
        distinct from the kalman_volatility state-space filter)."""
        return await self._client._send(
            "FinanceRealizedVolTick", {"mid": mid, "window": window}
        )

    async def spread_reversion(
        self, bid_px: list[float], ask_px: list[float], window: int = 20
    ) -> dict[str, Any]:
        """Spread mean-reversion feature. Returns {zscore, signal} where the
        rolling z-score of (ask−bid) drives signal = −zscore (wide ⇒ expect
        tighten). Lightweight rolling stats, NOT the OU calibration."""
        return await self._client._send(
            "FinanceSpreadReversion",
            {"bid_px": bid_px, "ask_px": ask_px, "window": window},
        )

    async def information_ratio(self, ic: float, n_independent: float) -> float:
        """Fundamental law of active management: IR = IC · √(N_independent)."""
        return await self._client._send(
            "FinanceInformationRatio", {"ic": ic, "n_independent": n_independent}
        )

    async def effective_independent_n(self, returns_matrix: list[list[float]]) -> float:
        """Effective number of independent signals (eigenvalue participation ratio)
        — correlated signals collapse, exposing the real N in IR = IC·√N."""
        return await self._client._send(
            "FinanceEffectiveIndependentN", {"returns_matrix": returns_matrix}
        )

    async def alpha_combination_engine(
        self, returns_matrix: list[list[float]], lookback: int = 20
    ) -> list[float]:
        """Combine N signals into weights that reward independent edge and penalise
        shared variance (the IR = IC·√N combination engine). Rows = signals."""
        return await self._client._send(
            "FinanceAlphaCombinationEngine",
            {"returns_matrix": returns_matrix, "lookback": lookback},
        )

    async def brier_score(self, forecasts: list[float], outcomes: list[float]) -> float:
        """Brier score of probabilistic forecasts vs binary outcomes (< 0.25 =
        production-grade calibration)."""
        return await self._client._send(
            "FinanceBrierScore", {"forecasts": forecasts, "outcomes": outcomes}
        )

    async def convergence_gate(
        self, strengths: list[float], strong_threshold: float = 0.6, min_agree: int = 5
    ) -> dict[str, Any]:
        """Conviction gate — require ≥min_agree of N signals to STRONGLY agree on a
        direction before trading. Returns {agree, total, fraction, direction, pass}."""
        return await self._client._send(
            "FinanceConvergenceGate",
            {
                "strengths": strengths,
                "strong_threshold": strong_threshold,
                "min_agree": min_agree,
            },
        )

    async def empirical_kelly(
        self,
        p: float,
        b: float,
        historical_returns: list[float],
        n_simulations: int = 10000,
        seed: int = 42,
    ) -> float:
        """Uncertainty-adjusted Kelly: f* · (1 − CV_edge), with CV_edge from a
        seeded bootstrap of the historical returns. Shrinks bets when edge is noisy."""
        return await self._client._send(
            "FinanceEmpiricalKelly",
            {
                "p": p,
                "b": b,
                "historical_returns": historical_returns,
                "n_simulations": n_simulations,
                "seed": seed,
            },
        )

    # ── Derivatives: SABR volatility surface (CONCEPT:KG-2.20j) ─────────
    async def sabr_implied_vol(
        self,
        f: float,
        k: float,
        t: float,
        alpha: float,
        beta: float,
        rho: float,
        nu: float,
    ) -> float:
        """SABR lognormal (Black) implied volatility for one strike (Hagan 2002)."""
        return await self._client._send(
            "FinanceSabrImpliedVol",
            {
                "f": f,
                "k": k,
                "t": t,
                "alpha": alpha,
                "beta": beta,
                "rho": rho,
                "nu": nu,
            },
        )

    async def sabr_smile(
        self,
        f: float,
        strikes: list[float],
        t: float,
        alpha: float,
        beta: float,
        rho: float,
        nu: float,
    ) -> list[float]:
        """SABR implied-vol smile across strikes."""
        return await self._client._send(
            "FinanceSabrSmile",
            {
                "f": f,
                "strikes": strikes,
                "t": t,
                "alpha": alpha,
                "beta": beta,
                "rho": rho,
                "nu": nu,
            },
        )

    async def sabr_calibrate(
        self,
        f: float,
        t: float,
        strikes: list[float],
        market_vols: list[float],
        beta: float = 0.5,
    ) -> dict[str, Any]:
        """Calibrate SABR (α, ρ, ν) to a market smile with β fixed — returns
        {alpha, beta, rho, nu, rmse, converged}."""
        return await self._client._send(
            "FinanceSabrCalibrate",
            {
                "f": f,
                "t": t,
                "strikes": strikes,
                "market_vols": market_vols,
                "beta": beta,
            },
        )


class DataScienceClient:
    """CONCEPT:KG-2.22 — Data Science Primitives Namespace.

    Rust-backed OLS / K-means / PCA / dataset-stats / split. Arrays are shipped
    whole per call (one round-trip) — never loop per row over the wire.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def linear_regression(
        self, x: list[list[float]], y: list[float]
    ) -> dict[str, Any]:
        return await self._client._send("DsLinearRegression", {"x": x, "y": y})

    async def kmeans(
        self, data: list[list[float]], k: int, max_iter: int = 100
    ) -> dict[str, Any]:
        return await self._client._send(
            "DsKMeans", {"data": data, "k": k, "max_iter": max_iter}
        )

    async def pca(self, data: list[list[float]], n_components: int) -> dict[str, Any]:
        return await self._client._send(
            "DsPca", {"data": data, "n_components": n_components}
        )

    async def compute_stats(self, data: list[list[float]]) -> dict[str, Any]:
        return await self._client._send("DsComputeStats", {"data": data})

    async def train_test_split(
        self,
        data: list[list[float]],
        labels: list[float],
        test_ratio: float = 0.2,
        shuffle: bool = True,
        seed: int = 42,
    ) -> dict[str, Any]:
        return await self._client._send(
            "DsTrainTestSplit",
            {
                "data": data,
                "labels": labels,
                "test_ratio": test_ratio,
                "shuffle": shuffle,
                "seed": seed,
            },
        )

    async def fit_estimator(
        self,
        estimator: str,
        x: list[list[float]],
        y: list[float],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Fit a regression estimator (ridge/lasso/elasticnet/decisiontree/
        randomforest/gradientboosting/adaboost/svr). Returns a serializable
        fitted-model blob to pass back to ``predict_estimator``."""
        return await self._client._send(
            "DsFitEstimator",
            {"estimator": estimator, "x": x, "y": y, "params": params or {}},
        )

    async def predict_estimator(
        self, model: dict[str, Any], x: list[list[float]]
    ) -> list[float]:
        """Predict with a model blob returned by ``fit_estimator``."""
        return await self._client._send("DsPredictEstimator", {"model": model, "x": x})

    # ── Training loss / optimizer kernels (CONCEPT:KG-2.22) ──────────────────
    # The Rust performance path for the in-house training substrate (Wave C / C1),
    # mirroring data-science-mcp `trainers/objectives.py`. Batch a step over the
    # wire instead of marshalling per element.

    async def softmax(
        self, logits: list[float], temperature: float = 1.0
    ) -> list[float]:
        """Numerically-stable softmax with temperature."""
        return await self._client._send(
            "DsSoftmax", {"logits": logits, "temperature": temperature}
        )

    async def log_softmax(self, logits: list[float]) -> list[float]:
        """Numerically-stable log-softmax."""
        return await self._client._send("DsLogSoftmax", {"logits": logits})

    async def cross_entropy(
        self, logits: list[list[float]], labels: list[int]
    ) -> dict[str, Any]:
        """Mean categorical cross-entropy → ``{loss, grad}`` (grad = softmax−onehot)."""
        return await self._client._send(
            "DsCrossEntropy", {"logits": logits, "labels": labels}
        )

    async def dpo_loss(
        self,
        policy_chosen: list[float],
        policy_rejected: list[float],
        ref_chosen: list[float],
        ref_rejected: list[float],
        beta: float = 0.1,
    ) -> dict[str, Any]:
        """Bradley-Terry DPO loss → ``{loss, grad_chosen, grad_rejected}``."""
        return await self._client._send(
            "DsDpoLoss",
            {
                "policy_chosen": policy_chosen,
                "policy_rejected": policy_rejected,
                "ref_chosen": ref_chosen,
                "ref_rejected": ref_rejected,
                "beta": beta,
            },
        )

    async def grpo_surrogate(
        self,
        logprob: list[float],
        old_logprob: list[float],
        advantage: list[float],
        clip_eps: float = 0.2,
    ) -> dict[str, Any]:
        """GRPO clipped surrogate (loss to minimise) → ``{loss, grad}``."""
        return await self._client._send(
            "DsGrpoSurrogate",
            {
                "logprob": logprob,
                "old_logprob": old_logprob,
                "advantage": advantage,
                "clip_eps": clip_eps,
            },
        )

    async def kl_divergence(
        self, logprob: list[float], ref_logprob: list[float]
    ) -> float:
        """Schulman k3 low-variance KL estimate (≥0)."""
        return await self._client._send(
            "DsKlDivergence", {"logprob": logprob, "ref_logprob": ref_logprob}
        )

    async def adam_step(
        self,
        params: list[float],
        grads: list[float],
        *,
        lr: float,
        t: int,
        m: list[float] | None = None,
        v: list[float] | None = None,
        beta1: float = 0.9,
        beta2: float = 0.999,
        eps: float = 1e-8,
    ) -> dict[str, Any]:
        """One Adam step with bias correction → ``{params, m, v}``."""
        return await self._client._send(
            "DsAdamStep",
            {
                "params": params,
                "grads": grads,
                "m": m or [],
                "v": v or [],
                "lr": lr,
                "beta1": beta1,
                "beta2": beta2,
                "eps": eps,
                "t": t,
            },
        )

    async def sgd_step(
        self, params: list[float], grads: list[float], lr: float
    ) -> list[float]:
        """One plain SGD step ``params − lr·grads``."""
        return await self._client._send(
            "DsSgdStep", {"params": params, "grads": grads, "lr": lr}
        )


# Per-RPC timeouts (CONCEPT:KG-2.19). A wedged or overloaded engine must never
# hang a caller forever — every request is bounded. Normal CRUD uses the short
# default; known-heavy ops (full-graph parse/scan/algorithms) get a generous
# budget so a legitimately long job is not aborted. Both are overridable per
# client or via env; set the timeout to 0/None to disable (not recommended).
_DEFAULT_RPC_TIMEOUT = float(os.environ.get("GRAPH_SERVICE_RPC_TIMEOUT", "60") or 60)
_HEAVY_RPC_TIMEOUT = float(
    os.environ.get("GRAPH_SERVICE_HEAVY_RPC_TIMEOUT", "1200") or 1200
)
#: Establishing the socket connection must also be bounded — a peer that accepts
#: the connection but never completes the handshake would otherwise hang the
#: caller forever (the connect path is outside the per-RPC read budget).
_CONNECT_TIMEOUT = float(os.environ.get("GRAPH_SERVICE_CONNECT_TIMEOUT", "10") or 10)
#: Flushing a request must be bounded INDEPENDENTLY of (and no longer than) the
#: read budget. A healthy engine drains a local socket in microseconds; a write
#: that backs up means the engine has stopped reading (wedged) — detect that in
#: seconds even for a "heavy" method whose *response* may legitimately take long.
_WRITE_TIMEOUT = float(os.environ.get("GRAPH_SERVICE_WRITE_TIMEOUT", "30") or 30)
#: Methods whose work is O(graph) / batch-sized and may legitimately run long.
_HEAVY_RPC_METHODS = frozenset(
    {
        "ParseFile",
        "ParseFiles",
        "IndexRepository",
        "ParseRepository",
        "CommunityDetection",
        "CommunityDetectEphemeral",
        "ComputeSimilarityEdges",
        "BatchCosineSimilarity",
        "FindSimilarPairs",
        "SpectralCluster",
        "Vf2SubgraphMatch",
        "BetweennessCentrality",
        "PageRank",
        "PersonalizedPagerank",
        "BatchUpdate",
        "FromMsgpack",
        "ToMsgpack",
        "GetTriples",
        "Reconcile",
        "RunDatalogReasoning",
        "GetSubgraph",
        "GetNodes",
        "GetEdges",
        # SQL scans the whole node set (CONCEPT:KG-2.178) — give it the heavy budget.
        "Sql",
    }
)


class QueryClient:
    """CONCEPT:KG-2.178 — Read-only SQL Query Namespace.

    ``SELECT ... FROM nodes WHERE ... LIMIT ...`` over the connection's graph,
    served by the engine's DataFusion surface (requires a server built with the
    ``query`` feature). Schema-on-read: node property keys become columns; a raw
    ``props`` blob column plus ``json_get(props, key)`` /
    ``json_get_f64`` / ``json_get_i64`` UDFs reach fields the inferred schema
    widened or dropped.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def sql(self, query: str) -> list[dict[str, Any]]:
        """Run ``query`` and return a list of row dicts keyed by column name.

        The engine returns ``{"columns": [...], "rows": [<msgpack-blob>, ...]}``
        (a ``Raw`` payload the transport already double-unpacks); each row blob is
        a list of cell values aligned to ``columns``. We zip them into dicts so a
        caller gets ordinary records.
        """
        result = await self._client._send("Sql", {"query": query})
        if not result:
            return []
        columns: list[str] = result.get("columns", [])
        out: list[dict[str, Any]] = []
        for row_blob in result.get("rows", []):
            cells = msgpack.unpackb(bytes(row_blob), raw=False)
            out.append(dict(zip(columns, cells, strict=False)))
        return out


class EpistemicGraphClient:
    """CONCEPT:KG-2.19 — Epistemic Graph Core Client

    Async client for the epistemic-graph Tokio service using Composition.

    Usage::

        client = await EpistemicGraphClient.connect(
            socket_path="/tmp/epistemic-graph.sock",
            auth_secret="my-secret",
            graph_name="agent:planner",
        )
        await client.nodes.add("node1", {"type": "Agent"})
        ranks = await client.analytics.pagerank(damping=0.85, iterations=100)
        await client.close()
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        auth_secret: str,
        graph_name: str,
        agent_id: str | None = None,
        timeout: float | None = _DEFAULT_RPC_TIMEOUT,
        heavy_timeout: float | None = _HEAVY_RPC_TIMEOUT,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._auth_secret = auth_secret
        self._graph_name = graph_name
        # Per-RPC read timeouts (0/None disables). Heavy ops use heavy_timeout.
        self._timeout = timeout if timeout else None
        self._heavy_timeout = heavy_timeout if heavy_timeout else None
        # Caller identity for server-side ACL enforcement (isolation layer).
        # Optional: single-tenant deployments never need it; once identities
        # are registered server-side, requests carry it for check_access().
        self._agent_id = agent_id
        self._request_id = 0
        self._closed = False
        self._lock = asyncio.Lock()
        # How we connected — remembered so a dropped connection can be
        # transparently re-established on the next call (see _reconnect).
        # Populated by connect(); a directly-constructed client cannot self-heal.
        self._socket_path: str | None = None
        self._tcp_addr: str | None = None
        self._connect_timeout: float | None = _CONNECT_TIMEOUT
        # Server capability set, negotiated lazily on first use (see supports());
        # reset on reconnect so a fresh connection re-negotiates.
        self._server_ops: set[str] | None = None

        # Namespaced Sub-Clients (Composition)
        self.nodes = NodeClient(self)
        self.edges = EdgeClient(self)
        self.graph = GraphOperationsClient(self)
        self.analytics = AnalyticsClient(self)
        self.lifecycle = LifecycleClient(self)
        self.reasoning = ReasoningClient(self)
        self.ledger = LedgerClient(self)
        self.channels = ChannelsClient(self)
        self.tenants = MultiTenantClient(self)
        self.consensus = ConsensusClient(self)
        self.finance = FinanceClient(self)
        self.datascience = DataScienceClient(self)
        self.query = QueryClient(self)

    @staticmethod
    async def _open_streams(
        socket_path: str | None,
        tcp_addr: str | None,
        connect_timeout: float | None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, str]:
        """Dial a fresh reader/writer pair to the engine.

        Returns ``(reader, writer, resolved_socket)`` — ``resolved_socket`` is the
        UDS path actually used (so reconnects target the same socket), or ``""``
        for a TCP endpoint. Shared by :meth:`connect` and :meth:`_reconnect`.
        """
        _conn_to = connect_timeout if connect_timeout else None
        if tcp_addr:
            host, port_str = tcp_addr.rsplit(":", 1)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, int(port_str)), _conn_to
                )
            except (asyncio.TimeoutError, TimeoutError) as e:
                raise TimeoutError(
                    f"epistemic-graph connect to {tcp_addr} timed out after {_conn_to}s"
                ) from e
            logger.info("Connected to epistemic-graph service via TCP: %s", tcp_addr)
            return reader, writer, ""

        _socket = socket_path or os.environ.get(
            "GRAPH_SERVICE_SOCKET",
            os.path.join(
                os.environ.get("XDG_RUNTIME_DIR", "/tmp"),  # nosec B108
                "epistemic-graph.sock",
            ),
        )
        if not os.path.exists(_socket):
            _tmp_socket = "/tmp/epistemic-graph.sock"  # nosec B108
            if os.path.exists(_tmp_socket):
                _socket = _tmp_socket
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(_socket), _conn_to
            )
        except (asyncio.TimeoutError, TimeoutError) as e:
            raise TimeoutError(
                f"epistemic-graph connect to {_socket} timed out after {_conn_to}s"
            ) from e
        logger.info("Connected to epistemic-graph service via UDS: %s", _socket)
        return reader, writer, _socket

    @classmethod
    async def connect(
        cls,
        socket_path: str | None = None,
        tcp_addr: str | None = None,
        auth_secret: str | None = None,
        graph_name: str = "__commons__",
        agent_id: str | None = None,
        timeout: float | None = _DEFAULT_RPC_TIMEOUT,
        heavy_timeout: float | None = _HEAVY_RPC_TIMEOUT,
        connect_timeout: float | None = _CONNECT_TIMEOUT,
    ) -> EpistemicGraphClient:
        _secret = auth_secret or os.environ.get("GRAPH_SERVICE_AUTH_SECRET", "")

        reader, writer, resolved_socket = await cls._open_streams(
            socket_path, tcp_addr, connect_timeout
        )

        client = cls(
            reader,
            writer,
            _secret,
            graph_name,
            agent_id=agent_id,
            timeout=timeout,
            heavy_timeout=heavy_timeout,
        )
        # Remember the endpoint so a dropped connection self-heals (KG-2.19).
        client._socket_path = resolved_socket or socket_path
        client._tcp_addr = tcp_addr
        client._connect_timeout = connect_timeout
        return client

    async def _reconnect(self) -> None:
        """Re-establish a dropped connection in place, on the same endpoint.

        A long-lived client's connection can die between calls — engine
        restart, an idle close, or a prior RPC that closed a poisoned stream
        (see ``_send``). Without recovery the client is permanently broken and
        the engine circuit breaker latches OPEN forever. Callers hold no
        reference to the underlying reader/writer, so dialing a fresh stream and
        swapping them in is transparent. Must be called with ``self._lock`` held.
        """
        with contextlib.suppress(Exception):  # discard the poisoned stream
            self._writer.close()
        self._reader, self._writer, _ = await self._open_streams(
            self._socket_path, self._tcp_addr, self._connect_timeout
        )
        self._closed = False
        # Re-negotiate capabilities against the fresh connection.
        self._server_ops = None

    # ── Internal ──────────────────────────────────────────────────────────

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _compute_token(self, request_id: int) -> str:
        if not self._auth_secret:
            return ""
        return hmac.new(
            self._auth_secret.encode(),
            str(request_id).encode(),
            hashlib.sha256,
        ).hexdigest()

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
    ) -> Any:
        req_id = self._next_id()
        request: dict[str, Any] = {
            "id": req_id,
            "graph": graph or self._graph_name,
            "auth_token": self._compute_token(req_id),
            "method": method,
        }
        if self._agent_id is not None:
            request["agent_id"] = self._agent_id
        if params:
            request["params"] = params

        payload = msgpack.packb(request)
        length_prefix = len(payload).to_bytes(4, byteorder="big")

        # Heavy ops (full-graph parse/scan/algorithms) get the longer read budget.
        timeout = self._heavy_timeout if method in _HEAVY_RPC_METHODS else self._timeout
        # The request flush is bounded separately and never longer than the read
        # budget: a slow drain means the engine has stopped reading (wedged), and
        # that is independent of how long its *response* may legitimately take.
        write_timeout = _WRITE_TIMEOUT if _WRITE_TIMEOUT else None
        if timeout is not None:
            write_timeout = min(timeout, write_timeout) if write_timeout else timeout

        async with self._lock:
            # A prior call may have closed a poisoned/dead stream. Re-dial in
            # place so this call succeeds instead of reusing a dead writer —
            # otherwise the engine circuit breaker latches OPEN permanently.
            if self._closed:
                await self._reconnect()
            try:
                self._writer.write(length_prefix)
                self._writer.write(payload)
                await asyncio.wait_for(self._writer.drain(), write_timeout)
                len_buf = await asyncio.wait_for(self._reader.readexactly(4), timeout)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                resp_bytes = await asyncio.wait_for(
                    self._reader.readexactly(msg_len), timeout
                )
            except asyncio.IncompleteReadError as e:
                # Server closed the stream mid-frame — the connection is dead.
                self._closed = True
                with contextlib.suppress(Exception):  # best-effort teardown
                    self._writer.close()
                raise ConnectionError("Connection closed by server") from e
            except (asyncio.TimeoutError, TimeoutError) as e:
                # A write that never drained, or a read that timed out mid-frame,
                # leaves the stream desynced (a late reply would be misread as the
                # NEXT request's response). Treat the timeout as connection-fatal —
                # close so the pool/breaker reconnects on a clean stream rather than
                # reusing a poisoned one.
                self._closed = True
                with contextlib.suppress(Exception):  # best-effort teardown
                    self._writer.close()
                raise TimeoutError(
                    f"epistemic-graph RPC {method!r} timed out (connection closed; "
                    "retry will reconnect)"
                ) from e
            except OSError:
                # Any transport-level error during write/drain/read — broken pipe,
                # connection reset, etc. (all OSError subclasses) — leaves the
                # stream unusable. Mark it closed so the NEXT call reconnects
                # instead of reusing a dead writer (which latched the breaker
                # OPEN forever). Re-raise unchanged; it trips the breaker.
                self._closed = True
                with contextlib.suppress(Exception):  # best-effort teardown
                    self._writer.close()
                raise

        resp = msgpack.unpackb(resp_bytes, raw=False)
        if resp.get("error") is not None:
            err_msg = resp.get("error", "Unknown error")
            raise RuntimeError(err_msg)
        result = resp.get("result")
        # Compact result encoding (engine Phase C-D): heavy algorithm results and
        # node/edge property blobs come back as a top-level MessagePack `bin` (the
        # `Raw`/`PropertiesMsgpack` payloads) — the server skips building a JSON
        # tree. Decode that second layer here so callers get the same structure the
        # old JSON path produced. (Per-call sites that already self-decoded bytes
        # now receive the decoded value and pass it straight through.)
        if isinstance(result, (bytes, bytearray)):
            result = msgpack.unpackb(result, raw=False)
        return result

    # ── Connection Management ─────────────────────────────────────────────

    async def close(self) -> None:
        if not self._closed:
            self._writer.close()
            await self._writer.wait_closed()
            self._closed = True

    async def __aenter__(self) -> EpistemicGraphClient:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.close()

    # ── Service-Level ─────────────────────────────────────────────────────

    async def ping(self) -> str:
        return await self._send("Ping")

    async def health(self) -> dict[str, Any]:
        return await self._send("Health")

    async def supports(self, op: str) -> bool:
        """True if the connected engine advertises protocol op ``op``.

        Capability negotiation (CONCEPT:KG-2.19): the server's ``Health`` response
        carries an ``ops`` list. The probe is cached for the connection's life. An
        older engine that doesn't advertise ``ops`` reports no extra ops, so newer
        callers (e.g. ``ParseFiles``) gracefully fall back to per-item paths.
        """
        ops = getattr(self, "_server_ops", None)
        if ops is None:
            try:
                h = await self.health()
                ops = set(h.get("ops", []) or []) if isinstance(h, dict) else set()
            except Exception:
                ops = set()
            self._server_ops = ops
        return op in ops

    async def checkpoint(self) -> str:
        return await self._send("Checkpoint")

    async def reconcile(self, graph_name: str, json_str: str) -> str:
        return await self._send(
            "Reconcile", {"graph_name": graph_name, "json_str": json_str}
        )

    async def shutdown(self) -> str:
        return await self._send("Shutdown")

    async def apply_mutation(self, event_type: str, query: str) -> str:
        return await self._send(
            "ApplyMutation", {"event_type": event_type, "query": query}
        )


class SyncEpistemicGraphClient:
    """Synchronous wrapper around the async client.

    Warning: If you are upgrading from the legacy flat API, you must update
    your calls to use the namespaced API (e.g. client.nodes.add instead of client.add_node).
    """

    def __init__(
        self,
        async_client: EpistemicGraphClient,
        loop: asyncio.AbstractEventLoop,
        thread: threading.Thread,
    ) -> None:
        self._client = async_client
        self._loop = loop
        self._thread = thread

        # We need to wrap the namespaces synchronously as well
        self.nodes = self._SyncWrapper(self._client.nodes, self._loop)
        self.edges = self._SyncWrapper(self._client.edges, self._loop)
        self.graph = self._SyncWrapper(self._client.graph, self._loop)
        self.analytics = self._SyncWrapper(self._client.analytics, self._loop)
        self.lifecycle = self._SyncWrapper(self._client.lifecycle, self._loop)
        self.reasoning = self._SyncWrapper(self._client.reasoning, self._loop)
        self.ledger = self._SyncWrapper(self._client.ledger, self._loop)
        self.channels = self._SyncWrapper(self._client.channels, self._loop)
        self.tenants = self._SyncWrapper(self._client.tenants, self._loop)
        self.consensus = self._SyncWrapper(self._client.consensus, self._loop)
        self.finance = self._SyncWrapper(self._client.finance, self._loop)
        self.datascience = self._SyncWrapper(self._client.datascience, self._loop)
        self.query = self._SyncWrapper(self._client.query, self._loop)

    def clear(self) -> None:
        """Synchronously clear the graph (used primarily by the test suite teardown)."""
        future = asyncio.run_coroutine_threadsafe(
            self._client._send("ClearGraph"), self._loop
        )
        return future.result()

    class _SyncWrapper:
        def __init__(
            self, async_namespace: Any, loop: asyncio.AbstractEventLoop
        ) -> None:
            self._namespace = async_namespace
            self._loop = loop

        def __getattr__(self, name: str) -> Any:
            attr = getattr(self._namespace, name)
            if inspect.iscoroutinefunction(attr):

                def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                    future = asyncio.run_coroutine_threadsafe(
                        attr(*args, **kwargs), self._loop
                    )
                    return future.result()

                return sync_wrapper
            return attr

    @classmethod
    def connect(cls, **kwargs: Any) -> SyncEpistemicGraphClient:
        import threading

        loop = asyncio.new_event_loop()

        def run_loop() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()

        future = asyncio.run_coroutine_threadsafe(
            EpistemicGraphClient.connect(**kwargs), loop
        )
        async_client = future.result()

        return cls(async_client, loop, thread)

    def close(self) -> None:
        future = asyncio.run_coroutine_threadsafe(self._client.close(), self._loop)
        try:
            future.result(timeout=5)
        except Exception as e:
            logger.debug("Error closing client: %s", e)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2)

    def __enter__(self) -> SyncEpistemicGraphClient:
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._client, name)
        if inspect.iscoroutinefunction(attr):

            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                future = asyncio.run_coroutine_threadsafe(
                    attr(*args, **kwargs), self._loop
                )
                return future.result()

            return sync_wrapper
        return attr
