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


class ResultTooLargeError(RuntimeError):
    """Raised when an unbounded read (e.g. ``nodes.list()`` / ``GetNodes``) would
    return more than the engine's configured node cap
    (``EPISTEMIC_GRAPH_MAX_RESPONSE_NODES``, CONCEPT:KG-2.264).

    The engine refuses to serialize a pathological full-graph dump (which would
    overrun/reset the connection) and instead returns a typed ``RESULT_TOO_LARGE``
    error. Catch this to fall back to a bounded query — ``nodes.list_by_label(
    label, limit)`` or pagination. Subclasses :class:`RuntimeError`, so existing
    ``except RuntimeError`` handlers keep working.
    """


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
        """Dump EVERY node in the graph (unbounded full-graph read).

        On a large graph this is refused by the engine's overload backstop
        (CONCEPT:KG-2.264): if the graph has more than
        ``EPISTEMIC_GRAPH_MAX_RESPONSE_NODES`` nodes (default 50_000), this raises
        :class:`ResultTooLargeError` instead of materializing a gigabyte-scale
        frame that would reset the connection. Use :meth:`list_by_label` (which is
        bounded by ``limit``) or paginate for large graphs.
        """
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

    async def invalidate(
        self,
        source_id: str,
        target_id: str,
        relationship: str,
        invalid_at: int,
        tx_now: int,
    ) -> int:
        """Non-destructively close a contradicted edge's temporal windows (KG-2.251).

        Sets the matching edge's ``valid_until = invalid_at`` and ``tx_to = tx_now``
        instead of deleting it, so an ``AS OF`` before ``invalid_at`` still sees the
        fact. Returns the number of edge blobs updated.
        """
        return await self._client._send(
            "InvalidateEdge",
            {
                "source_id": source_id,
                "target_id": target_id,
                "relationship": relationship,
                "invalid_at": int(invalid_at),
                "tx_now": int(tx_now),
            },
        )

    async def supersede(
        self,
        source_id: str,
        target_id: str,
        prior_source: str,
        prior_target: str,
        prior_relationship: str,
        valid_at: int,
        tx_now: int,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Atomically supersede a prior edge with a new one (KG-2.251).

        Closes the prior edge's validity window AND inserts the new edge under one
        write guard — non-destructive, so the prior edge survives for history. The
        new edge's ``properties`` should carry ``valid_from = valid_at`` and a
        ``supersedes`` provenance pointer.
        """
        await self._client._send(
            "SupersedeEdge",
            {
                "source_id": source_id,
                "target_id": target_id,
                "properties_msgpack": list(msgpack.packb(properties or {})),
                "prior_source": prior_source,
                "prior_target": prior_target,
                "prior_relationship": prior_relationship,
                "valid_at": int(valid_at),
                "tx_now": int(tx_now),
            },
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

    async def observe_screen(
        self,
        png: bytes,
        *,
        session_id: str,
        frame_seq: int = 0,
        prev_frame_id: str = "",
        prev_hash: int = 0,
        elements: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Turn a captured desktop frame into durable graph entities in ONE round-trip
        (CONCEPT:KG-2.185).

        ``png`` is the screenshot bytes (only its dimensions + content hash are kept,
        for frame-diff — the image itself is not persisted). ``elements`` is the AT-SPI
        accessibility tree (``[{role,name,x,y,w,h}, ...]``). Returns a single
        ``ScreenObservationResult``::

            {"nodes": [...], "edges": [...],   # session + frame + UIElement nodes,
             "frame_id": str, "width": int, "height": int,
             "hash": int, "changed": bool, "element_count": int}

        ``edges`` carry ``hasObservation`` (session→frame), ``hasElement``
        (frame→element) and ``succeededBy`` (prev→frame, only when the frame changed).
        Pass the returned ``hash``/``frame_id`` back as ``prev_hash``/``prev_frame_id``
        on the next call to chain the frames.
        """
        blob = msgpack.packb(
            {
                "session_id": session_id,
                "frame_seq": frame_seq,
                "prev_frame_id": prev_frame_id,
                "prev_hash": prev_hash,
                "png": png,
                "elements": elements or [],
            }
        )
        return await self._client._send("ObserveScreen", {"obs_msgpack": blob})

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

    async def resolve_candidates(
        self,
        sim_threshold: float = 0.8,
        merge_threshold: float = 0.92,
        node_type: str | None = None,
    ) -> list[dict]:
        """Native entity-resolution candidate generation (CONCEPT:KG-2.260).

        Composes embedding similarity + clustering server-side into ONE read op and
        returns merge proposals — each ``{canonical, members, score, kind}`` where
        ``kind`` is ``"same_as"`` (mergeable duplicates) or ``"extends"`` (a
        subtype/version link). READ/propose only: nothing is mutated; apply accepted
        proposals via ``batch_update``. This is the escalation tier the
        agent-utilities dedup ladder routes its residual through instead of an
        O(N²) client-side embedding pass.
        """
        return await self._client._send(
            "ResolveCandidates",
            {
                "sim_threshold": sim_threshold,
                "merge_threshold": merge_threshold,
                "node_type": node_type,
            },
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


class ReshardingClient:
    """CONCEPT:EG-038 — M3 catalog-driven resharding admin namespace.

    Drives, over the wire, the M3 ops the engine has building blocks for: online
    single-node resharding (EG-032), the durable tenant catalog (EG-031), and the
    rebalancing planner (EG-035) + its execution (EG-039). All require a durable redb
    engine; a non-redb build returns a "not available in this build" error.
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def reshard(self, graph: str, to_shard: int) -> dict[str, Any]:
        """Online-move ``graph``'s durable rows to ``to_shard`` while the engine runs,
        then flip the catalog route (EG-032). Returns a reshard report (counts +
        ``delta_nodes``/``delta_edges`` = the rows copied under the brief write-pause)."""
        return await self._client._send(
            "Reshard", {"graph": graph, "to_shard": to_shard}
        )

    async def catalog_assign(
        self, graph: str, shard: int, node: int | None = None
    ) -> bool:
        """Populate / assign an explicit catalog placement for ``graph`` (EG-031). Flips
        the ROUTE only — to MOVE the rows too use :meth:`reshard`."""
        return await self._client._send(
            "CatalogAssign", {"graph": graph, "shard": shard, "node": node}
        )

    async def catalog_reassign(self, graph: str, shard: int) -> bool:
        """Re-place ``graph`` onto ``shard``, preserving its node placement (EG-031)."""
        return await self._client._send(
            "CatalogReassign", {"graph": graph, "shard": shard}
        )

    async def catalog_remove(self, graph: str) -> bool:
        """Drop ``graph``'s explicit placement — it reverts to EG-026 FNV-1a routing."""
        return await self._client._send("CatalogRemove", {"graph": graph})

    async def catalog_list(self) -> dict[str, Any]:
        """List every explicit catalog placement ``{graph, shard, node}`` (EG-031)."""
        return await self._client._send("CatalogList")

    async def rebalance_plan(
        self, tolerance: float | None = None, max_moves: int | None = None
    ) -> dict[str, Any]:
        """Compute (do NOT execute) a rebalance plan over live per-shard/per-graph load
        (EG-035). Returns ``{moves: [...], shards: [...]}``."""
        return await self._client._send(
            "RebalancePlan", {"tolerance": tolerance, "max_moves": max_moves}
        )

    async def rebalance_execute(
        self, tolerance: float | None = None, max_moves: int | None = None
    ) -> dict[str, Any]:
        """Compute a rebalance plan AND execute it move-by-move via online resharding
        (EG-039) — online, one graph at a time. Returns ``{executed: [report, ...]}``."""
        return await self._client._send(
            "RebalanceExecute", {"tolerance": tolerance, "max_moves": max_moves}
        )


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
        "ResolveCandidates",
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
        # Cypher MATCH/BFS scans the node set too (CONCEPT:KG-2.179).
        "CypherQuery",
        # A txn commit (CONCEPT:KG-2.180) applies the whole staged write-set under
        # one lock — a large multi-op commit may legitimately take longer.
        "Commit",
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
        return self._rows_to_dicts(result)

    async def cypher(self, query: str) -> list[dict[str, Any]]:
        """Run a Cypher-subset ``query`` and return a list of row dicts keyed by
        RETURN column.

        ``MATCH (a:Label)-[:REL]->(b:Label2) WHERE a.prop = 'x' RETURN a, b LIMIT
        k`` over the connection's graph (CONCEPT:KG-2.179). DEP-FREE on the engine
        side — compiled to the label index / VF2 / BFS, NO DataFusion — so it works
        against a server built with only the ``cypher`` feature (the lean Pi build).

        Supports: node ``:Label`` predicates, typed directed edges
        (``-[:REL]->`` / ``<-[:REL]-``), variable-length paths (``-[:REL*1..3]->``),
        ``WHERE`` equality/comparison on properties, ``RETURN`` of bound variables
        and ``var.prop`` accesses, and ``LIMIT``. The result has the SAME
        ``{"columns": [...], "rows": [<msgpack-blob>, ...]}`` shape as ``sql`` (a
        ``Raw`` payload the transport already double-unpacks); each row blob is a
        list of cell values aligned to ``columns``.
        """
        result = await self._client._send("CypherQuery", {"query": query})
        return self._rows_to_dicts(result)

    async def graphql(self, query: str) -> dict[str, Any]:
        """Run a GraphQL READ ``query`` and return the GraphQL ``{"data": …}`` dict
        (CONCEPT:KG-2.235).

        The query's root fields are node TYPES (labels) with optional ``first``/
        ``limit`` and property-equality arguments and nested EDGE selections, e.g.::

            { Person(name: "Alice", first: 10) { name KNOWS { name } } }

        On the engine side it is compiled to scans + BFS over the SAME ``GraphView``
        the Cypher executor reads (DEP-FREE — no async-graphql / DataFusion), so a
        GraphQL query returns the SAME nodes/fields as the equivalent Cypher query.
        Requires a server built with the ``graphql`` feature. Returns the parsed
        GraphQL JSON (a ``Raw`` payload the transport already double-unpacks).

        Mutations / subscriptions / fragments are not supported (read-only surface);
        the engine returns a clear parse error for them.
        """
        return await self._client._send("GraphQl", {"query": query})

    async def unified(
        self,
        plan: list[dict[str, Any]],
        reorder_filter_selectivity: float | None = None,
    ) -> list[dict[str, Any]]:
        """Run ONE cross-modal plan (CONCEPT:KG-2.208/209) and return ranked rows.

        ``plan`` is an ordered list of operator dicts — a CLOSED algebra over a
        shared ``RowSet`` (ordered ids + optional scores). Each op is the
        externally-tagged form of the engine's ``Op`` enum, e.g.::

            [
                {"Scan": {"label": "Doc"}},
                {"Filter": {"preds": [{"GtNum": {"prop": "year", "n": 2024.0}}]}},
                {"Traverse": {"rel": "CITES", "min": 1, "max": 2}},
                {"Rank": {"query": [1.0, 0.0, 0.0, 0.0]}},
                {"Limit": {"k": 10}},
            ]

        The engine sequences the EXISTING legs over one off-lock snapshot —
        ``Filter`` via real DataFusion, ``Traverse`` via petgraph BFS, ``Rank`` via
        the vector kNN — instead of three siloed round-trips (requires a server
        built with the ``query`` feature). When ``reorder_filter_selectivity`` is
        given (a fraction in ``[0,1]``), the cost model reorders an adjacent
        ``(Filter, Rank)`` pair filter-first vs vector-first by that selectivity
        (CONCEPT:KG-2.209) — the result set is unchanged, only the work differs.

        Returns a list of ``{"id": str, "score": float | None}`` rows, in the plan's
        final order (descending score after a ``Rank``).
        """
        params: dict[str, Any] = {"plan": {"ops": plan}}
        if reorder_filter_selectivity is not None:
            params["reorder_filter_selectivity"] = reorder_filter_selectivity
        result = await self._client._send("UnifiedQuery", params)
        rows = result or []
        return [{"id": id_, "score": score} for id_, score in rows]

    async def uql(
        self,
        text: str,
        reorder_filter_selectivity: float | None = None,
    ) -> list[dict[str, Any]]:
        """Run a UQL TEXT query (CONCEPT:KG-2.214) — the human/agent-writable
        front-end over :meth:`unified`.

        ``text`` is a UQL pipeline that the engine PARSES into the SAME cross-modal
        ``Plan`` AST :meth:`unified` carries, then runs through the IDENTICAL
        executor (no new execution path). One query expresses filter (relational) +
        traverse (graph) + rank (vector) across modalities, e.g.::

            MATCH (:Doc) WHERE year > 2024
              |> TRAVERSE -[:CITES]->{1,2}
              |> RANK BY ~[1.0, 0.0, 0.0, 0.0]
              |> LIMIT 10

        Grammar (this increment): ``MATCH (:Label) [WHERE preds]`` seeds the scan
        (an inline ``WHERE`` is sugar for a ``|> WHERE`` filter stage); pipeline
        stages are ``TRAVERSE -[:REL]->{min,max}`` (or bare ``TRAVERSE REL{min,max}``;
        ``{n}`` = exactly n hops, absent = 1 hop), ``RANK BY ~[v0, v1, …]`` (an inline
        literal query vector), ``LIMIT k``, and a later-stage ``WHERE``. Predicates are
        ``prop > num`` / ``prop < num`` / ``prop = value`` joined by ``AND``; keywords
        are case-insensitive. Requires a server built with the ``query`` feature.

        ``reorder_filter_selectivity`` behaves exactly as in :meth:`unified` — a
        ``[0,1]`` fraction triggering the cost-based (Filter, Rank) reorder
        (CONCEPT:KG-2.209), which never changes the result set.

        On a syntax error the engine returns a clear, caret-annotated parse error
        (raised as the transport's error). Returns the same
        ``{"id": str, "score": float | None}`` rows as :meth:`unified`.
        """
        params: dict[str, Any] = {"text": text}
        if reorder_filter_selectivity is not None:
            params["reorder_filter_selectivity"] = reorder_filter_selectivity
        result = await self._client._send("UnifiedQueryText", params)
        rows = result or []
        return [{"id": id_, "score": score} for id_, score in rows]

    async def register_foreign_source(self, name: str, source: dict[str, Any]) -> str:
        """Register a named EXTERNAL source for query federation (CONCEPT:KG-2.232,
        Lane P), returning the registered name.

        ``source`` is the externally-tagged ``ForeignSourceSpec``: either a REMOTE
        epistemic-graph engine, queried over the engine's own transport::

            {"RemoteEngine": {
                "endpoint": "host:port", "graph": "__commons__",
                "secret": "<remote hmac secret>",
                "uql": "MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2}",
            }}

        or a generic HTTP/JSON API (a pure-Rust rustls client on the engine side)::

            {"HttpJson": {
                "url": "https://api.example.com/papers",
                "json_path": "data",
                "field_map": {"id": "doi", "score": "relevance"},
            }}

        or an EXTERNAL relational-SQL database — Postgres/MySQL (CONCEPT:KG-2.239); the
        engine runs the SQL OUT to the foreign RDBMS over a pure-Rust/rustls ``sqlx``
        client and fuses the rows in-plan (the "engine federates external SQL" half that
        sql-mcp alone cannot give). Requires a server built with ``federation-sql``::

            {"Sql": {
                "dsn": "postgres://user:pw@host:5432/papers",
                "query": "SELECT doi, relevance FROM cited WHERE published > 2023",
                "id_field": "doi",
                "score_field": "relevance",
            }}

        A federated :meth:`unified` / :meth:`uql` plan reads such a source as a
        ``RowSet`` via a ``ForeignScan`` op and composes it with the local
        graph/vector/SQL ops in ONE plan — e.g. JOIN a foreign source with the local
        graph::

            [
                {"Scan": {"label": "Doc"}},
                {"Filter": {"preds": [{"GtNum": {"prop": "year", "n": 2023.0}}]}},
                {"ForeignScan": {"source": {"HttpJson": {...}}, "join": True}},
                {"Rank": {"query": [1.0, 0.0, 0.0, 0.0]}},
                {"Limit": {"k": 10}},
            ]

        A ``ForeignScan`` with ``join`` true intersects the foreign rows with the
        current candidate set (foreign∩local, keyed on id); ``join`` false makes it a
        pure SOURCE that REPLACES the input (like ``Scan``). Requires a server built
        with the ``federation`` feature.
        """
        return await self._client._send(
            "RegisterForeignSource", {"name": name, "source": source}
        )

    @staticmethod
    def _rows_to_dicts(result: Any) -> list[dict[str, Any]]:
        """Zip a ``{columns, rows}`` query result into per-row dicts. Shared by
        ``sql`` and ``cypher`` — both return the identical wire shape."""
        if not result:
            return []
        columns: list[str] = result.get("columns", [])
        out: list[dict[str, Any]] = []
        for row_blob in result.get("rows", []):
            cells = msgpack.unpackb(bytes(row_blob), raw=False)
            out.append(dict(zip(columns, cells, strict=False)))
        return out


class TxnClient:
    """CONCEPT:KG-2.180 — Multi-op OCC ACID Transaction Namespace.

    Optimistic, snapshot-isolation, server-staged transactions. ``begin()``
    returns a server-issued ``txn_id``; the ``add_node``/``remove_node``/
    ``add_edge``/``remove_edge``/``cas`` calls STAGE durable mutations server-side
    (nothing touches the graph until commit), and ``commit()`` validates the OCC
    read-set and applies the whole write-set atomically — returning ``False`` on
    conflict (a true rollback: nothing applied or persisted). ``rollback()``
    discards the staged transaction. Usage::

        txn = await client.txn.begin()
        await client.txn.add_node(txn, "a", {"type": "Doc"})
        await client.txn.add_edge(txn, "a", "b", {})
        ok = await client.txn.commit(txn)   # False ⇒ OCC conflict, retry
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def begin(self, graph: str | None = None) -> str:
        """Open a transaction and return its server-issued ``txn_id``. The target
        graph defaults to the connection's graph; pass ``graph`` to override."""
        params: dict[str, Any] = {}
        if graph is not None:
            params["graph"] = graph
        return await self._client._send("BeginTxn", params, graph=graph)

    async def add_node(
        self,
        txn_id: str,
        node_id: str,
        properties: dict[str, Any] | None = None,
        graph: str | None = None,
    ) -> bool:
        """Stage an add-node. ``graph`` (CONCEPT:KG-2.226) targets a graph OTHER than
        the txn's default — making the txn multi-graph (cross-shard if it spans Raft
        groups, routed through 2PC at commit); omit for the single-graph default."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "node_id": node_id,
            "properties_msgpack": list(msgpack.packb(properties or {})),
        }
        if graph is not None:
            params["graph"] = graph
        return await self._client._send("TxnAddNode", params)

    async def remove_node(
        self, txn_id: str, node_id: str, graph: str | None = None
    ) -> bool:
        params: dict[str, Any] = {"txn_id": txn_id, "node_id": node_id}
        if graph is not None:
            params["graph"] = graph
        return await self._client._send("TxnRemoveNode", params)

    async def add_edge(
        self,
        txn_id: str,
        source_id: str,
        target_id: str,
        properties: dict[str, Any] | None = None,
        graph: str | None = None,
    ) -> bool:
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "source_id": source_id,
            "target_id": target_id,
            "properties_msgpack": list(msgpack.packb(properties or {})),
        }
        if graph is not None:
            params["graph"] = graph
        return await self._client._send("TxnAddEdge", params)

    async def remove_edge(
        self, txn_id: str, source_id: str, target_id: str, graph: str | None = None
    ) -> bool:
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "source_id": source_id,
            "target_id": target_id,
        }
        if graph is not None:
            params["graph"] = graph
        return await self._client._send("TxnRemoveEdge", params)

    async def cas(
        self,
        txn_id: str,
        node_id: str,
        conditions: dict[str, Any],
        updates: dict[str, Any],
        graph: str | None = None,
    ) -> bool:
        """Stage an atomic compare-and-set on ``node_id`` (applied at commit)."""
        params: dict[str, Any] = {
            "txn_id": txn_id,
            "node_id": node_id,
            "conditions_msgpack": list(msgpack.packb(conditions)),
            "updates_msgpack": list(msgpack.packb(updates)),
        }
        if graph is not None:
            params["graph"] = graph
        return await self._client._send("TxnCas", params)

    async def add_embedding(
        self, txn_id: str, node_id: str, embedding: list[float]
    ) -> bool:
        """Stage a VECTOR upsert (CONCEPT:KG-2.225 — cross-modal ACID). The embedding
        lands atomically WITH the txn's graph/property/blob-ref writes in ONE redb
        WriteTransaction at commit (requires the redb persistence backend)."""
        return await self._client._send(
            "TxnAddEmbedding",
            {"txn_id": txn_id, "node_id": node_id, "embedding": embedding},
        )

    async def blob_ref(self, txn_id: str, node_id: str, digest: str) -> bool:
        """Stage a BLOB REFERENCE (CONCEPT:KG-2.225). Records a durable graph-side
        ``__blob__`` link to an already-stored content-addressed blob; lands
        atomically with the node/vector/property at commit."""
        return await self._client._send(
            "TxnBlobRef",
            {"txn_id": txn_id, "node_id": node_id, "digest": digest},
        )

    async def commit(self, txn_id: str) -> bool:
        """Commit the transaction. ``True`` ⇒ applied + persisted; ``False`` ⇒ OCC
        conflict (nothing applied — a true rollback; re-begin and retry)."""
        return await self._client._send("Commit", {"txn_id": txn_id})

    async def rollback(self, txn_id: str) -> bool:
        """Discard the staged transaction (nothing was applied/persisted)."""
        return await self._client._send("Rollback", {"txn_id": txn_id})


class TimeSeriesClient:
    """CONCEPT:KG-2.210/211 — Native Time-Series Namespace.

    Append/scan/query time-partitioned series stored beside the graph (their own
    ``series.redb``), served by a server built with the ``tsdb`` feature. Series are
    keyed by ``series_id`` (independent of the connection's graph). Points are
    ``(ts_ns, [field0, field1, ...])`` — a scalar series is one field per point;
    OHLCV is several. The native primitives (ASOF / gap-fill / windowed aggregate)
    need NO DataFusion, so they work on the lean / Pi build.

    Usage::

        await client.timeseries.append("px", [(0, [100.0]), (1_000_000_000, [101.0])])
        pts  = await client.timeseries.range("px", 0, 2_000_000_000)
        vals = await client.timeseries.asof_join("px", [500_000_000])  # -> [100.0]
        bars = await client.timeseries.window("px", 0, 60_000_000_000, 60_000_000_000, "mean")
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def register_series(
        self,
        series_id: str,
        *,
        entity_id: str | None = None,
        field_names: list[str] | None = None,
        bucket_ns: int = 3_600_000_000_000,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Register a ``:Series`` node in the connection's graph linking the series
        to a KG entity — the series-id registry shape (CONCEPT:KG-2.210).

        The series data itself lives in the time-series store (keyed by
        ``series_id``); this writes a small node into the GRAPH so the series is
        discoverable + linkable from the ontology. Node shape::

            :Series {
              id:          "series:<series_id>",
              type:        "Series",
              series_id:   "<series_id>",
              entity_id:   "<kg-node-id>",     # the entity this series measures
              field_names: ["px", "vol", ...],
              bucket_ns:   <int>,
              ... metadata
            }

        ``entity_id`` is the KG node the series describes (e.g. a ``:Instrument`` or
        ``:Memory``); a downstream caller adds the ``:measures`` edge in its ontology
        layer (agent-utilities owns the OWL mapping)."""
        props: dict[str, Any] = {
            "type": "Series",
            "series_id": series_id,
            "field_names": field_names or [],
            "bucket_ns": int(bucket_ns),
        }
        if entity_id is not None:
            props["entity_id"] = entity_id
        if metadata:
            props.update(metadata)
        await self._client.nodes.add(f"series:{series_id}", props)

    async def append(
        self,
        series_id: str,
        points: list[tuple[int, list[float]]],
        *,
        n_fields: int | None = None,
        bucket_ns: int = 3_600_000_000_000,
        field_names: list[str] | None = None,
    ) -> int:
        """Append a batch of ``(ts_ns, [values])`` points in ONE round-trip. Returns
        the number of points appended. ``bucket_ns``/``field_names`` are used only
        when the series is NEW (default bucket = 1h); ``n_fields`` defaults to the
        width of the first point. Out-of-order / late points are handled."""
        if not points:
            return 0
        nf = n_fields if n_fields is not None else len(points[0][1])
        blob = msgpack.packb(
            [[int(ts), [float(v) for v in vals]] for ts, vals in points]
        )
        return await self._client._send(
            "TsAppend",
            {
                "series_id": series_id,
                "n_fields": nf,
                "bucket_ns": int(bucket_ns),
                "field_names": field_names or [],
                "points_msgpack": blob,
            },
        )

    async def range(
        self, series_id: str, from_ts: int, to_ts: int
    ) -> list[tuple[int, list[float]]]:
        """Scan ``[from_ts, to_ts)`` of a series in ts order. Returns
        ``(ts_ns, [values])`` points (empty for an unknown series)."""
        rows = await self._client._send(
            "TsRange", {"series_id": series_id, "from": int(from_ts), "to": int(to_ts)}
        )
        return [(int(ts), [float(v) for v in vals]) for ts, vals in (rows or [])]

    async def asof_join(
        self, series_id: str, left_ts: list[int], *, tolerance_ns: int | None = None
    ) -> list[float | None]:
        """ASOF join: for each event ts in ``left_ts``, the series' field-0 value as
        of (nearest at-or-before) that time. Results are in the SAME order as
        ``left_ts``; an unmatched / out-of-tolerance event yields ``None``."""
        blob = msgpack.packb([int(t) for t in left_ts])
        return await self._client._send(
            "TsAsofJoin",
            {
                "series_id": series_id,
                "left_ts_msgpack": blob,
                "tolerance": -1 if tolerance_ns is None else int(tolerance_ns),
            },
        )

    async def window(
        self, series_id: str, from_ts: int, to_ts: int, width_ns: int, agg: str = "mean"
    ) -> list[tuple[int, float, int]]:
        """Windowed aggregate over ``[from_ts, to_ts)`` in ``width_ns`` buckets.
        ``agg`` ∈ first/last/min/max/mean/sum/count. Returns
        ``(bucket_start_ns, value, count)`` per non-empty bucket."""
        rows = await self._client._send(
            "TsWindow",
            {
                "series_id": series_id,
                "from": int(from_ts),
                "to": int(to_ts),
                "width": int(width_ns),
                "agg": agg,
            },
        )
        return [(int(b), float(v), int(c)) for b, v, c in (rows or [])]

    async def gap_fill(
        self, series_id: str, from_ts: int, to_ts: int, step_ns: int
    ) -> list[tuple[int, float | None, bool]]:
        """Gap-fill (LOCF) on a fixed grid from ``from_ts`` to ``to_ts`` every
        ``step_ns``. Returns ``(grid_ts_ns, value_or_None, carried_forward)`` —
        ``value`` is ``None`` before the first observation (encoded as NaN on the
        wire); ``carried_forward`` is ``True`` when no real obs landed on that grid ts."""
        rows = await self._client._send(
            "TsGapFill",
            {
                "series_id": series_id,
                "from": int(from_ts),
                "to": int(to_ts),
                "step": int(step_ns),
            },
        )
        out: list[tuple[int, float | None, bool]] = []
        for ts, val, filled in rows or []:
            v = (
                None if isinstance(val, float) and val != val else float(val)
            )  # NaN -> None
            out.append((int(ts), v, bool(filled)))
        return out


class RdfClient:
    """CONCEPT:KG-2.217 / KG-2.218 — Native RDF/SPARQL Namespace.

    The RDF dataset maps onto the SAME property-graph the rest of the engine uses
    (a resource object becomes a typed edge, a literal object a typed property cell
    preserving xsd datatype + ``@lang``, ``rdf:type`` the engine ``type`` label, a
    named graph the connection's graph). Requires a server built with the ``rdf``
    feature (``sparql`` for :meth:`sparql`).
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def add_triples(
        self,
        turtle: str | None = None,
        ntriples: str | None = None,
    ) -> dict[str, int]:
        """Parse Turtle OR N-Triples (exactly one) into the connection's graph.

        Returns a ``LoadReport`` dict ``{triples, multivalue, dropped_multivalue}``.
        ``dropped_multivalue`` is non-zero only when a multi-valued literal predicate
        was seen AND the server has no lossless quad store (no persist dir) — the
        extras beyond the first value are then reported, never silently lost.
        """
        if (turtle is None) == (ntriples is None):
            raise ValueError(
                "add_triples: provide exactly one of `turtle` or `ntriples`"
            )
        return await self._client._send(
            "AddTriples",
            {"turtle": turtle or "", "ntriples": ntriples or ""},
        )

    async def get_triples(self) -> str:
        """Serialize the connection's graph back OUT to N-Triples (datatype/lang
        faithful — the inverse of :meth:`add_triples`)."""
        return await self._client._send("GetRdf")

    async def remove_triples(
        self,
        turtle: str | None = None,
        ntriples: str | None = None,
    ) -> dict[str, int]:
        """Physically RETRACT Turtle OR N-Triples from the connection's graph (CONCEPT:EG-017).

        The inverse of :meth:`add_triples`: parses the document and surgically removes
        each triple (a literal triple drops the property cell; a resource triple removes
        the one matching typed edge). Durable. Returns a count dict. The retract op the
        ontology UNLOAD path + SPARQL ``DELETE DATA`` build on. Requires the ``rdf`` feature.
        """
        if (turtle is None) == (ntriples is None):
            raise ValueError(
                "remove_triples: provide exactly one of `turtle` or `ntriples`"
            )
        return await self._client._send(
            "RemoveTriples",
            {"turtle": turtle or "", "ntriples": ntriples or ""},
        )

    async def drop_named_graph(self, graph: str) -> str:
        """DROP a named RDF graph (CONCEPT:EG-017): physically clear ALL of its RDF
        content (property-graph nodes/edges + the lossless multi-valued-literal quad
        rows) in one op. Durable. The coarse-grained retract used when an ontology owns
        a dedicated named graph; the SPARQL ``DROP/CLEAR GRAPH`` op routes here. The op
        targets the request's graph, so ``graph`` is sent via the request envelope.
        Requires the ``rdf`` feature.
        """
        return await self._client._send("DropNamedGraph", graph=graph)

    async def sparql(
        self,
        query: str,
        base_iri: str = "",
        type_convention: str = "",
    ) -> list[dict[str, str | None]]:
        """Run a SPARQL 1.1 ``SELECT`` over the connection's graph and return a list
        of row dicts keyed by projected variable (``None`` for an unbound OPTIONAL
        variable). Requires a server built with the ``sparql`` feature.

        ``base_iri`` + ``type_convention`` select the LPG→RDF projection vocabulary
        (CONCEPT:KG-2.240). Both default to empty ⇒ the IDENTITY projection (node-type
        and property keys emitted verbatim, no ``rdf:type`` synthesis), preserving the
        prior behavior. A caller that passes ``base_iri`` (e.g. agent-utilities'
        ``http://agent-utilities.dev/ontology#``) + ``type_convention="camel"`` makes
        the engine project the live property graph into that vocabulary, so a by-class
        query (``?s a au:Agent``) resolves natively — the engine, not rdflib, answers.

        The engine returns ``{"vars": [...], "rows": [[cell, ...], ...]}`` (a ``Raw``
        payload the transport already double-unpacks); we zip each row to its vars.
        """
        result = await self._client._send(
            "Sparql",
            {
                "query": query,
                "base_iri": base_iri,
                "type_convention": type_convention,
            },
        )
        if not result:
            return []
        vars_: list[str] = result.get("vars", [])
        rows: list[dict[str, str | None]] = []
        for row in result.get("rows", []):
            rows.append(dict(zip(vars_, row, strict=False)))
        return rows

    async def owl_reason(
        self,
        ontology: str | None = None,
        target_class: str | None = None,
        min_confidence: float = 0.0,
    ) -> dict[str, Any]:
        """Run the native OWL 2 (EL⁺ + RL) reasoner over the connection's graph and
        materialize entailments — confidence-weighted (CONCEPT:KG-2.219 / KG-2.236).
        Classifies the OWL axioms already in the graph (loaded via :meth:`add_triples`)
        plus any extra ``ontology`` Turtle, then returns::

            {
                "subclasses": [[sub, sup], ...],    # the classification hierarchy
                "subclass_conf": [c, ...],          # per-subsumption confidence in [0,1],
                                                    #   aligned index-for-index
                "instances":  [[inst, class], ...], # inferred memberships (incl. ones
                                                    #   reached only through ∃-restrictions
                                                    #   / role chains), conf >= min_confidence
                "instance_conf": [c, ...],          # per-membership confidence in [0,1]
                "consistent": bool,                 # False if a class is unsatisfiable
                "unsatisfiable": [class, ...],
            }

        Axioms may carry an ``eg:confidence`` annotation and facts their per-node
        ``confidence`` (decayed by age on the Ebbinghaus curve); the closure propagates
        them — a derived entailment's confidence is ``axiom_conf x product(premise_conf)``
        (max over alternative derivations). ``min_confidence`` (tau) drops entailments
        below the threshold. ``target_class`` restricts ``instances`` to that class's
        inferred members. Read-only. Requires a server built with the ``owl`` feature.
        """
        return await self._client._send(
            "OwlReason",
            {
                "ontology": ontology or "",
                "target_class": target_class or "",
                "min_confidence": float(min_confidence),
            },
        )

    async def owl_reason_distributed(
        self,
        graphs: list[str],
        ontology: str | None = None,
        target_class: str | None = None,
        min_confidence: float = 0.0,
    ) -> dict[str, Any]:
        """Distributed (cross-shard) confidence-weighted OWL reasoning over the UNION of
        ``graphs`` (CONCEPT:KG-2.236). Gathers each graph/shard's TBox axioms + decayed-
        confidence type facts, runs ONE weighted EL⁺/RL closure over the union (the
        cross-shard union-read seam), and returns the SAME shape as :meth:`owl_reason` —
        provably identical to reasoning over the same axioms in a single graph. The
        single-shard fast path stays :meth:`owl_reason`. Read-only; ``owl`` feature.
        """
        return await self._client._send(
            "OwlReasonDistributed",
            {
                "graphs": list(graphs),
                "ontology": ontology or "",
                "target_class": target_class or "",
                "min_confidence": float(min_confidence),
            },
        )


class StreamingClient:
    """CONCEPT:KG-2.229/230 — Streaming / CDC / subscriptions / triggers.

    A reactive surface over the engine's per-graph durable change record (the
    ledger): every durable mutation emits an ordered, cursor-addressable change into
    a per-graph in-memory feed. From that ONE feed three surfaces are served over the
    SAME framed-MessagePack transport (cursor / long-poll — NO side-channel socket):

      * **CDC feed** (``cdc_read``) — tail the ordered ``CdcEvent`` changes since a
        ``from_seq`` cursor; re-read from ``last["seq"] + 1`` to skip what you've seen.
        The foundation for incremental matviews, mirrors, and external sinks.
      * **Continuous queries** (``register_continuous_query`` / ``read_continuous_query``)
        — a named aggregate (count / sum) maintained INCREMENTALLY on each change.
      * **Subscriptions / triggers** (``watch`` / ``register_trigger`` / ``fired_triggers``)
        — a LISTEN/NOTIFY-style long-poll over a graph/label cursor, plus
        condition→action triggers whose firings are pollable.

    Requires a server built with the ``streaming`` feature (folded into
    pi/node/cluster/full).
    """

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def cdc_read(
        self, graph: str, from_seq: int = 0, *, limit: int = 0
    ) -> list[dict[str, Any]]:
        """Read the ordered CDC feed for ``graph`` from cursor ``from_seq`` (inclusive),
        up to ``limit`` (0 ⇒ a server default). Each event is a dict with ``seq``,
        ``kind`` (AddNode/RemoveNode/UpdateNode/AddEdge/RemoveEdge), ``node_id``,
        ``target_id``, ``label``, and the ``before``/``after`` property blobs (raised as
        ``bytes``; ``had_before``/``had_after`` flag presence). Re-read from
        ``events[-1]["seq"] + 1`` to skip seen. Raises if the cursor is behind the
        retained ring window."""
        return await self._client._send(
            "CdcRead", {"graph": graph, "from_seq": int(from_seq), "limit": int(limit)}
        )

    async def register_continuous_query(
        self, name: str, graph: str, agg: str, *, label: str = "", field: str = ""
    ) -> str:
        """Register (or replace) an incrementally-maintained query ``name`` over
        ``graph``'s CDC feed. ``agg`` is ``"count"`` (live count of matching nodes) or
        ``"sum"`` (running sum of numeric node property ``field``). ``label`` (empty ⇒
        all nodes) filters by node label. The view is SEEDED from the graph's current
        state at registration, then maintained on delta. Returns ``name``."""
        if agg == "count":
            spec_agg: Any = "Count"
        elif agg == "sum":
            if not field:
                raise ValueError("sum continuous query requires a field")
            spec_agg = {"Sum": {"field": field}}
        else:
            raise ValueError(f"unknown agg '{agg}' (expected 'count' or 'sum')")
        spec = {"graph": graph, "label": label, "agg": spec_agg}
        return await self._client._send(
            "RegisterContinuousQuery",
            {"name": name, "spec_msgpack": msgpack.packb(spec)},
        )

    async def read_continuous_query(self, name: str) -> dict[str, Any]:
        """Read the current incrementally-maintained result of continuous query
        ``name`` → ``{"name", "value", "through_seq"}`` (the value + the CDC seq it
        reflects)."""
        return await self._client._send("ReadContinuousQuery", {"name": name})

    async def drop_continuous_query(self, name: str) -> bool:
        """Drop a continuous query. Returns ``True`` if it existed."""
        return await self._client._send("DropContinuousQuery", {"name": name})

    async def watch(
        self, graph: str, from_seq: int = 0, *, label: str = "", timeout_ms: int = 0
    ) -> dict[str, Any]:
        """LISTEN/NOTIFY-style long-poll subscription: return the matching CDC changes
        for ``graph`` since ``from_seq`` (filtered by ``label``, empty ⇒ all). If none
        are pending, block up to ``timeout_ms`` for the first one (0 ⇒ don't block).
        Returns ``{"events": [...], "next_seq": int}`` — pass ``next_seq`` back to keep
        tailing. One Request → one Response; re-issue to continue watching."""
        return await self._client._send(
            "Watch",
            {
                "graph": graph,
                "from_seq": int(from_seq),
                "label": label,
                "timeout_ms": int(timeout_ms),
            },
        )

    async def register_trigger(
        self,
        name: str,
        graph: str,
        op: str,
        *,
        label: str = "",
        action: dict[str, Any] | None = None,
    ) -> str:
        """Register a trigger/reaction: when a CDC change in ``graph`` matches ``label``
        (empty ⇒ any) + ``op`` (``"add"``/``"remove"``/``"update"``/``"any"``), record a
        firing carrying ``action`` (an opaque reaction payload — e.g. a notification
        topic / webhook spec). Poll firings with ``fired_triggers``. Returns ``name``."""
        return await self._client._send(
            "RegisterTrigger",
            {
                "name": name,
                "graph": graph,
                "label": label,
                "op": op,
                "action_msgpack": msgpack.packb(action or {}),
            },
        )

    async def drop_trigger(self, name: str) -> bool:
        """Drop a trigger. Returns ``True`` if it existed."""
        return await self._client._send("DropTrigger", {"name": name})

    async def list_triggers(self, graph: str) -> list[dict[str, Any]]:
        """List the triggers registered on ``graph`` (``name``/``op``/``label``/
        ``fire_count``)."""
        return await self._client._send("ListTriggers", {"graph": graph})

    async def fired_triggers(
        self, graph: str, from_seq: int = 0, *, limit: int = 0
    ) -> list[dict[str, Any]]:
        """Poll the fired-trigger log for ``graph`` from cursor ``from_seq``: the
        reactions that fired, each ``{"fire_seq", "trigger", "change_seq", "node_id",
        "action"}`` (``action`` raised as ``bytes``). Resume from
        ``fired[-1]["fire_seq"] + 1``."""
        return await self._client._send(
            "FiredTriggers",
            {"graph": graph, "from_seq": int(from_seq), "limit": int(limit)},
        )


class BlobClient:
    """CONCEPT:KG-2.206 — Streamed content-addressed BLOB namespace.

    Store / fetch large media (image / audio / video) bytes as a content-addressed,
    deduplicated, refcount-GC'd blob beside the graph. The whole file is never
    resident on either side: an upload streams as N fixed-size chunks sharing ONE
    server-side cursor (each chunk hashed + stored on arrival), a commit assembles
    the manifest → a stable blob digest; a fetch mirrors it (open cursor → pull
    chunks → reassemble). Identical bytes ⇒ identical digest ⇒ ZERO new chunks
    (dedup). Requires a server built with the ``blob`` feature (folded into
    ``node``/``pi-max``/``full``) AND a persist dir.

    The CONTENT lives here keyed by digest (graph-independent); a caller links it
    into the graph with a ``:MediaAsset``/``:Media`` node + a ``blob_ref`` (the
    cross-modal ACID path, CONCEPT:KG-2.225). Usage::

        digest = await client.blob.store(image_bytes)        # content-addressed
        same   = await client.blob.store(image_bytes)        # == digest, deduped
        out    = await client.blob.fetch(digest)             # == image_bytes
        await client.blob.incref(digest)                     # a :Media now refs it
    """

    #: Default chunk size for an upload when the caller passes none. Matches the
    #: engine default; small enough that one chunk is never a large allocation.
    DEFAULT_CHUNK_SIZE = 1 << 20  # 1 MiB

    def __init__(self, client: EpistemicGraphClient) -> None:
        self._client = client

    async def begin(self, chunk_size: int = 0) -> int:
        """Open an upload cursor (server allocates an id). ``chunk_size`` 0 ⇒ engine
        default. Push chunks with :meth:`chunk_put`, finalize with :meth:`commit`."""
        return await self._client._send("BlobBegin", {"chunk_size": int(chunk_size)})

    async def chunk_put(self, cursor: int, data: bytes) -> int:
        """Push one chunk into an open upload cursor (hashed + stored on arrival).
        Returns the running chunk count on the cursor."""
        return await self._client._send(
            "BlobChunkPut", {"cursor": int(cursor), "data": data}
        )

    async def commit(self, cursor: int) -> str:
        """Finalize an upload cursor → store the manifest content-addressed; returns
        the blob digest (the hash of the manifest, a stable content address)."""
        return await self._client._send("BlobCommit", {"cursor": int(cursor)})

    async def store(self, data: bytes, *, chunk_size: int = 0) -> str:
        """Store ``data`` as a content-addressed blob in ONE call (begin → stream
        chunks → commit) and return its digest. Streams in ``chunk_size`` chunks
        (default :attr:`DEFAULT_CHUNK_SIZE`) so a large payload is never re-buffered
        whole server-side. Identical bytes always yield the same digest (dedup)."""
        cs = int(chunk_size) or self.DEFAULT_CHUNK_SIZE
        cursor = await self.begin(cs)
        for off in range(0, len(data), cs):
            await self.chunk_put(cursor, data[off : off + cs])
        return await self.commit(cursor)

    async def fetch_begin(self, digest: str) -> tuple[int, int]:
        """Open a fetch cursor for ``digest``; returns ``(cursor, n_chunks)``."""
        cursor, n = await self._client._send("BlobFetchBegin", {"digest": digest})
        return int(cursor), int(n)

    async def chunk_get(self, cursor: int, idx: int) -> bytes:
        """Pull chunk ``idx`` of an open fetch cursor as raw bytes."""
        out = await self._client._send(
            "BlobChunkGet", {"cursor": int(cursor), "idx": int(idx)}
        )
        return bytes(out)

    async def fetch_end(self, cursor: int) -> bool:
        """Close a fetch cursor (idempotent)."""
        return await self._client._send("BlobFetchEnd", {"cursor": int(cursor)})

    async def fetch(self, digest: str) -> bytes:
        """Fetch a whole blob by digest in ONE call (open → pull every chunk →
        reassemble → close). Returns the exact stored bytes."""
        cursor, n = await self.fetch_begin(digest)
        try:
            chunks = [await self.chunk_get(cursor, i) for i in range(n)]
        finally:
            await self.fetch_end(cursor)
        return b"".join(chunks)

    async def incref(self, digest: str) -> int:
        """Increment a blob's GC refcount (a ``:Media`` node now references it).
        Returns the new count."""
        return await self._client._send("BlobRef", {"digest": digest})

    async def unref(self, digest: str) -> int:
        """Decrement a blob's GC refcount (a reference was removed). Returns the new
        count; a blob at 0 is eligible for the next :meth:`gc`."""
        return await self._client._send("BlobUnref", {"digest": digest})

    async def gc(self) -> tuple[int, int]:
        """Run the refcount mark-and-sweep GC; returns ``(blobs, chunks)`` reclaimed."""
        blobs, chunks = await self._client._send("BlobGc")
        return int(blobs), int(chunks)


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
        # ── CONCEPT:EG-043 — single-connection request PIPELINING (demux) ──
        # The engine (src/server/transport.rs) processes many requests on ONE
        # connection concurrently and writes responses back OUT OF ORDER, each
        # tagged with its `Response.id`. So instead of a lock held across the
        # whole write→round-trip→read (which serialized one connection), the
        # client runs a background reader task that resolves the matching pending
        # future by id. ``_send`` registers a future under the request id, writes
        # the frame, and awaits ONLY its own future — so per-caller ordering is
        # automatic (each await blocks on its own id) while INDEPENDENT concurrent
        # calls pipeline on the one connection. ``_lock`` now guards only the
        # connect/reconnect lifecycle; ``_write_lock`` serializes just the frame
        # write so two callers never interleave bytes on the wire.
        self._lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._reader_task: asyncio.Task[None] | None = None
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
        self.resharding = ReshardingClient(self)
        self.consensus = ConsensusClient(self)
        self.finance = FinanceClient(self)
        self.datascience = DataScienceClient(self)
        self.query = QueryClient(self)
        self.txn = TxnClient(self)
        self.timeseries = TimeSeriesClient(self)
        self.rdf = RdfClient(self)
        self.streaming = StreamingClient(self)
        self.blob = BlobClient(self)

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
        # Tear down the old demux reader and fail any calls still bound to the
        # dead connection (CONCEPT:EG-043) before swapping in the fresh stream.
        self._mark_dead(ConnectionError("connection reset; reconnecting"))
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

    # ── CONCEPT:EG-043 — pipelined connection: reader/demux internals ──────────

    @staticmethod
    def _retrieve_exc(fut: asyncio.Future) -> None:
        # Mark a failed future "retrieved" so a caller that already moved on (e.g.
        # its write raced the reader's EOF and never reached ``await fut``) does not
        # emit a noisy "Future exception was never retrieved" warning.
        if not fut.cancelled():
            with contextlib.suppress(Exception):
                fut.exception()

    def _fail_pending(self, exc: BaseException) -> None:
        """Resolve every in-flight future with ``exc`` (a connection died)."""
        pending = self._pending
        self._pending = {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(exc)
                fut.add_done_callback(self._retrieve_exc)

    def _mark_dead(self, exc: BaseException) -> None:
        """Tear the connection down: stop the reader, fail all in-flight calls.

        Idempotent. Called on any connection-fatal event (EOF, transport error,
        a bounded-timeout, explicit close, reconnect). Marks ``_closed`` so the
        next call self-heals via :meth:`_reconnect`.
        """
        self._closed = True
        task = self._reader_task
        self._reader_task = None
        if task is not None and not task.done():
            task.cancel()
        with contextlib.suppress(Exception):  # best-effort transport teardown
            self._writer.close()
        self._fail_pending(exc)

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        """Background demultiplexer: read frames, resolve futures by ``id``.

        One task per live connection. Responses arrive in ANY order (the engine
        pipelines, CONCEPT:EG-043); each is routed to its caller by the
        ``Response.id`` correlation id the protocol already carries. On EOF /
        transport error every in-flight call is failed so no caller hangs and the
        next call reconnects.
        """
        try:
            while True:
                len_buf = await reader.readexactly(4)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                body = await reader.readexactly(msg_len)
                resp = msgpack.unpackb(body, raw=False)
                fut = self._pending.pop(resp.get("id"), None)
                if fut is None and len(self._pending) == 1:
                    # Single-in-flight fallback (behaves EXACTLY as the pre-pipeline
                    # serial path): a response that doesn't carry a matching id
                    # resolves the sole pending call. With ≥2 calls in flight the
                    # engine's ``Response.id`` is REQUIRED to demux — and the engine
                    # always sends it — so this only ever affects the one-in-flight
                    # case (and tolerant of peers that omit the id).
                    _, fut = self._pending.popitem()
                if fut is not None and not fut.done():
                    fut.set_result(resp)
                # A response with no matching pending future and ≠1 in flight (e.g. a
                # late reply for a timed-out call) is simply dropped — the demux keeps
                # the stream in sync regardless, which is exactly why one
                # slow/timed-out call no longer desyncs the others.
        except asyncio.CancelledError:
            raise
        except (asyncio.IncompleteReadError, OSError):
            self._closed = True
            self._fail_pending(ConnectionError("Connection closed by server"))
        except Exception as e:  # noqa: BLE001 — surface any decode error to callers
            self._closed = True
            self._fail_pending(e)

    async def _ensure_connection(self) -> None:
        """Ensure a live stream + a running reader task (lifecycle lock held)."""
        async with self._lock:
            if self._closed:
                # A prior call closed a poisoned/dead stream. Re-dial in place so
                # this call succeeds instead of reusing a dead writer — otherwise
                # the engine circuit breaker latches OPEN permanently.
                await self._reconnect()
            if self._reader_task is None or self._reader_task.done():
                self._reader_task = asyncio.ensure_future(self._read_loop(self._reader))

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

        # Establish/heal the connection and the background demux reader.
        await self._ensure_connection()

        # Register this call's future under its id BEFORE writing, so the reader
        # can never miss the response (the engine can't reply before it reads).
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            # Serialize ONLY the frame write so two callers never interleave bytes;
            # the round-trip itself is NOT held under any lock — that is what lets
            # independent concurrent calls pipeline on the one connection.
            async with self._write_lock:
                self._writer.write(length_prefix)
                self._writer.write(payload)
                await asyncio.wait_for(self._writer.drain(), write_timeout)
            # Await ONLY our own response; per-caller ordering is automatic.
            resp = await asyncio.wait_for(fut, timeout)
        except (asyncio.TimeoutError, TimeoutError) as e:
            # Bounded per-call timeout. Connection-fatal (parity with the pre-pipeline
            # contract): a wedged engine that stops replying must not strand the
            # connection. Tear it down so the pool/breaker reconnects on a clean
            # stream; the demux already kept the wire in sync, but a timeout still
            # means the peer is unhealthy.
            self._pending.pop(req_id, None)
            self._mark_dead(TimeoutError(f"epistemic-graph RPC {method!r} timed out"))
            raise TimeoutError(
                f"epistemic-graph RPC {method!r} timed out (connection closed; "
                "retry will reconnect)"
            ) from e
        except asyncio.IncompleteReadError as e:
            self._pending.pop(req_id, None)
            self._mark_dead(ConnectionError("Connection closed by server"))
            raise ConnectionError("Connection closed by server") from e
        except OSError as e:
            # Any transport-level error during write/drain — broken pipe, reset,
            # etc. (all OSError subclasses). A ConnectionError raised from our own
            # future (the reader saw EOF) also lands here. Mark dead so the NEXT
            # call reconnects instead of reusing a dead writer (which latched the
            # breaker OPEN forever). Re-raise unchanged; it trips the breaker.
            self._pending.pop(req_id, None)
            self._mark_dead(e)
            raise

        if resp.get("error") is not None:
            err_msg = resp.get("error", "Unknown error")
            # The engine's overload backstop (CONCEPT:KG-2.264) returns a typed
            # RESULT_TOO_LARGE error for an oversize full-graph dump. Surface it as
            # a dedicated, catchable exception (still a RuntimeError subclass) so a
            # caller can fall back to a bounded query without string-matching.
            if isinstance(err_msg, str) and err_msg.startswith("RESULT_TOO_LARGE"):
                raise ResultTooLargeError(err_msg)
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
            # Stop the demux reader and fail any straggler in-flight calls
            # (CONCEPT:EG-043) before tearing the transport down.
            self._mark_dead(ConnectionError("client closed"))
            with contextlib.suppress(Exception):
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

    async def resource_stats(self) -> dict[str, Any]:
        """Return the per-tenant / per-graph resource snapshot (CONCEPT:KG-2.234).

        The autoscale signals an external autoscaler (agent-utilities OS-5.27)
        consumes in ONE round-trip: per-graph + per-tenant resident memory, node/edge
        counts, in-flight admission depth, hibernated-vs-resident counts, and the
        cumulative budget eviction/hibernation totals, plus a process aggregate.
        Requires an engine built ``--features cost`` (pi/node/cluster/full); an engine
        without it returns the "not available in this build" error.
        """
        return await self._send("ResourceStats")

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
        self.resharding = self._SyncWrapper(self._client.resharding, self._loop)
        self.consensus = self._SyncWrapper(self._client.consensus, self._loop)
        self.finance = self._SyncWrapper(self._client.finance, self._loop)
        self.datascience = self._SyncWrapper(self._client.datascience, self._loop)
        self.query = self._SyncWrapper(self._client.query, self._loop)
        self.txn = self._SyncWrapper(self._client.txn, self._loop)
        self.timeseries = self._SyncWrapper(self._client.timeseries, self._loop)
        self.rdf = self._SyncWrapper(self._client.rdf, self._loop)
        self.streaming = self._SyncWrapper(self._client.streaming, self._loop)
        self.blob = self._SyncWrapper(self._client.blob, self._loop)

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
