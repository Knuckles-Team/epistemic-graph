# CONCEPT:KG-2.19 — Epistemic Graph Service Client
#
# Async Python client for the Tokio-based epistemic-graph service.
# Communicates over UDS or TCP using Length-prefixed MessagePack framing
# with HMAC-SHA256 authentication.

from __future__ import annotations

import asyncio
import builtins
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

    async def list(self) -> builtins.list[tuple[str, str]]:
        return await self._client._send("GetNodes")

    async def properties(self, node_id: str) -> dict[str, Any] | None:
        raw_val = await self._client._send("GetNodeProperties", {"node_id": node_id})
        if raw_val is None:
            return None
        if isinstance(raw_val, bytes):
            import msgpack

            return msgpack.unpackb(raw_val, raw=False)
        return raw_val

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
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._auth_secret = auth_secret
        self._graph_name = graph_name
        self._request_id = 0
        self._closed = False
        self._lock = asyncio.Lock()

        # Namespaced Sub-Clients (Composition)
        self.nodes = NodeClient(self)
        self.edges = EdgeClient(self)
        self.graph = GraphOperationsClient(self)
        self.analytics = AnalyticsClient(self)
        self.lifecycle = LifecycleClient(self)
        self.ledger = LedgerClient(self)
        self.channels = ChannelsClient(self)
        self.tenants = MultiTenantClient(self)
        self.consensus = ConsensusClient(self)
        self.finance = FinanceClient(self)

    @classmethod
    async def connect(
        cls,
        socket_path: str | None = None,
        tcp_addr: str | None = None,
        auth_secret: str | None = None,
        graph_name: str = "__bus__",
    ) -> EpistemicGraphClient:
        _secret = auth_secret or os.environ.get("GRAPH_SERVICE_AUTH_SECRET", "")

        if tcp_addr:
            host, port_str = tcp_addr.rsplit(":", 1)
            reader, writer = await asyncio.open_connection(host, int(port_str))
            logger.info("Connected to epistemic-graph service via TCP: %s", tcp_addr)
        else:
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
            reader, writer = await asyncio.open_unix_connection(_socket)
            logger.info("Connected to epistemic-graph service via UDS: %s", _socket)

        return cls(reader, writer, _secret, graph_name)

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
        if params:
            request["params"] = params

        payload = msgpack.packb(request)
        length_prefix = len(payload).to_bytes(4, byteorder="big")

        async with self._lock:
            self._writer.write(length_prefix)
            self._writer.write(payload)
            await self._writer.drain()

            try:
                len_buf = await self._reader.readexactly(4)
                msg_len = int.from_bytes(len_buf, byteorder="big")
                resp_bytes = await self._reader.readexactly(msg_len)
            except asyncio.IncompleteReadError as e:
                raise ConnectionError("Connection closed by server") from e

        resp = msgpack.unpackb(resp_bytes, raw=False)
        if resp.get("error") is not None:
            err_msg = resp.get("error", "Unknown error")
            raise RuntimeError(err_msg)
        return resp.get("result")

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
        self.ledger = self._SyncWrapper(self._client.ledger, self._loop)
        self.channels = self._SyncWrapper(self._client.channels, self._loop)
        self.tenants = self._SyncWrapper(self._client.tenants, self._loop)
        self.consensus = self._SyncWrapper(self._client.consensus, self._loop)
        self.finance = self._SyncWrapper(self._client.finance, self._loop)

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
