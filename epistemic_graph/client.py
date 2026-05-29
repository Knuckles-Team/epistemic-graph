# CONCEPT:KG-2.19 — Epistemic Graph Service Client
#
# Async Python client for the Tokio-based epistemic-graph service.
# Communicates over UDS or TCP using JSON-over-newline framing
# with HMAC-SHA256 authentication.

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import threading
from typing import Any

import msgpack

logger = logging.getLogger(__name__)


class EpistemicGraphClient:
    """Async client for the epistemic-graph Tokio service.

    Usage::

        client = await EpistemicGraphClient.connect(
            socket_path="/tmp/epistemic-graph.sock",
            auth_secret="my-secret",
            graph_name="agent:planner",
        )
        await client.add_node("node1", {"type": "Agent"})
        ranks = await client.pagerank(damping=0.85, iterations=100)
        await client.close()

    Can also be used as an async context manager::

        async with await EpistemicGraphClient.connect(...) as client:
            await client.add_node("node1", {"type": "Agent"})
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

    @classmethod
    async def connect(
        cls,
        socket_path: str | None = None,
        tcp_addr: str | None = None,
        auth_secret: str | None = None,
        graph_name: str = "__bus__",
    ) -> EpistemicGraphClient:
        """Connect to the epistemic-graph service.

        Args:
            socket_path: Path to the Unix Domain Socket. Defaults to
                ``$XDG_RUNTIME_DIR/epistemic-graph.sock`` or ``/tmp/epistemic-graph.sock``.
            tcp_addr: TCP address (``host:port``). Takes precedence over socket_path.
            auth_secret: HMAC-SHA256 shared secret. Falls back to
                ``GRAPH_SERVICE_AUTH_SECRET`` env var.
            graph_name: Default graph to target (e.g., ``agent:planner``).
        """
        _secret = auth_secret or os.environ.get("GRAPH_SERVICE_AUTH_SECRET", "")

        if tcp_addr:
            host, port_str = tcp_addr.rsplit(":", 1)
            reader, writer = await asyncio.open_connection(host, int(port_str))
            logger.info("Connected to epistemic-graph service via TCP: %s", tcp_addr)
        else:
            fallback = os.path.join(
                os.path.expanduser("~"), ".local", "state", "epistemic-graph"
            )
            _socket = socket_path or os.environ.get(
                "GRAPH_SERVICE_SOCKET",
                os.path.join(
                    os.environ.get("XDG_RUNTIME_DIR", fallback),  # nosec B108
                    "epistemic-graph.sock",
                ),
            )
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
        """Send a request and await the response."""
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

        self._writer.write(length_prefix)
        self._writer.write(payload)
        await self._writer.drain()

        try:
            len_buf = await self._reader.readexactly(4)
            msg_len = int.from_bytes(len_buf, byteorder="big")
            resp_bytes = await self._reader.readexactly(msg_len)
        except asyncio.IncompleteReadError as e:
            raise ConnectionError("Connection closed by server") from e

        resp = msgpack.unpackb(resp_bytes)
        if resp.get("error") is not None:
            # Structured error raising
            err_msg = resp.get("error", "Unknown error")
            raise RuntimeError(err_msg)
        return resp.get("result")

    # ── Connection Management ─────────────────────────────────────────────

    async def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            self._writer.close()
            await self._writer.wait_closed()
            self._closed = True

    async def __aenter__(self) -> EpistemicGraphClient:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.close()

    # ── Node CRUD ─────────────────────────────────────────────────────────

    async def add_node(
        self, node_id: str, properties: dict[str, Any] | None = None
    ) -> None:
        await self._send(
            "AddNode",
            {
                "node_id": node_id,
                "properties_json": json.dumps(properties or {}),
            },
        )

    async def remove_node(self, node_id: str) -> None:
        await self._send("RemoveNode", {"node_id": node_id})

    async def has_node(self, node_id: str) -> bool:
        return await self._send("HasNode", {"node_id": node_id})

    async def get_nodes(self) -> list[tuple[str, str]]:
        return await self._send("GetNodes")

    async def get_node_properties(self, node_id: str) -> str | None:
        return await self._send("GetNodeProperties", {"node_id": node_id})

    async def node_count(self) -> int:
        return await self._send("NodeCount")

    async def node_ids(self) -> list[str]:
        return await self._send("NodeIds")

    # ── Edge CRUD ─────────────────────────────────────────────────────────

    async def add_edge(
        self, source_id: str, target_id: str, properties: dict[str, Any] | None = None
    ) -> None:
        await self._send(
            "AddEdge",
            {
                "source_id": source_id,
                "target_id": target_id,
                "properties_json": json.dumps(properties or {}),
            },
        )

    async def remove_edge(self, source_id: str, target_id: str) -> None:
        await self._send("RemoveEdge", {"source_id": source_id, "target_id": target_id})

    async def has_edge(self, source_id: str, target_id: str) -> bool:
        return await self._send(
            "HasEdge", {"source_id": source_id, "target_id": target_id}
        )

    async def get_edges(self) -> list[tuple[str, str, str]]:
        return await self._send("GetEdges")

    async def get_edge_properties(self, source_id: str, target_id: str) -> list[str]:
        return await self._send(
            "GetEdgeProperties", {"source_id": source_id, "target_id": target_id}
        )

    async def clear(self) -> None:
        """Clear the entire graph."""
        await self._send("ClearGraph")

    async def edge_count(self) -> int:
        return await self._send("EdgeCount")

    async def parse_repository(self, root_path: str) -> None:
        await self._send("ParseRepository", {"root_path": root_path})

    async def vf2_subgraph_match(
        self, pattern: EpistemicGraphClient
    ) -> list[dict[str, str]]:
        # The pattern graph has a name we can send to the server
        return await self._send(
            "Vf2SubgraphMatch", {"pattern_graph_name": pattern._graph_name}
        )

    # ── Neighbor Queries ──────────────────────────────────────────────────

    async def in_degree(self, node_id: str) -> int:
        return await self._send("InDegree", {"node_id": node_id})

    async def out_degree(self, node_id: str) -> int:
        return await self._send("OutDegree", {"node_id": node_id})

    async def get_predecessors(self, node_id: str) -> list[str]:
        return await self._send("GetPredecessors", {"node_id": node_id})

    async def get_successors(self, node_id: str) -> list[str]:
        return await self._send("GetSuccessors", {"node_id": node_id})

    async def get_neighbors(self, node_id: str) -> list[str]:
        return await self._send("GetNeighbors", {"node_id": node_id})

    # ── Graph Algorithms ──────────────────────────────────────────────────

    async def topological_sort(self) -> list[str]:
        return await self._send("TopologicalSort")

    async def find_cycle(self) -> list[str] | None:
        return await self._send("FindCycle")

    async def get_shortest_path(
        self, source_id: str, target_id: str
    ) -> list[str] | None:
        return await self._send(
            "GetShortestPath",
            {
                "source_id": source_id,
                "target_id": target_id,
            },
        )

    async def get_blast_radius(self, node_id: str, max_depth: int) -> list[str]:
        return await self._send(
            "GetBlastRadius",
            {
                "node_id": node_id,
                "max_depth": max_depth,
            },
        )

    async def degree_centrality(self, node_id: str) -> float:
        return await self._send("DegreeCentrality", {"node_id": node_id})

    async def degree_centrality_all(self) -> list[tuple[str, float]]:
        return await self._send("DegreeCentralityAll")

    async def betweenness_centrality(self) -> list[tuple[str, float]]:
        return await self._send("BetweennessCentrality")

    async def pagerank(
        self, damping: float = 0.85, iterations: int = 100
    ) -> list[tuple[str, float]]:
        return await self._send(
            "PageRank", {"damping": damping, "iterations": iterations}
        )

    async def personalized_pagerank(
        self,
        seed_nodes: list[tuple[str, float]],
        damping: float = 0.85,
        iterations: int = 100,
    ) -> list[tuple[str, float]]:
        return await self._send(
            "PersonalizedPageRank",
            {
                "seed_nodes": seed_nodes,
                "damping": damping,
                "iterations": iterations,
            },
        )

    async def connected_components(self) -> list[list[str]]:
        return await self._send("ConnectedComponents")

    async def community_detection(self, resolution: float = 1.0) -> list[list[str]]:
        return await self._send("CommunityDetection", {"resolution": resolution})

    async def graph_coloring(self) -> list[tuple[str, int]]:
        return await self._send("GraphColoring")

    async def compute_similarity_edges(self, threshold: float) -> int:
        return await self._send("ComputeSimilarityEdges", {"threshold": threshold})

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def prune_by_lifecycle(self, max_age_secs: int, min_score: float) -> int:
        return await self._send(
            "PruneByLifecycle",
            {
                "max_age_secs": max_age_secs,
                "min_score": min_score,
            },
        )

    async def get_context_view(self, agent_id: str, max_tokens: int = 4096) -> str:
        return await self._send(
            "GetContextView",
            {
                "agent_id": agent_id,
                "max_tokens": max_tokens,
            },
        )

    async def batch_update(self, operations: list[dict[str, Any]]) -> Any:
        return await self._send(
            "BatchUpdate",
            {
                "operations_json": json.dumps(operations),
            },
        )

    async def metrics(self) -> dict[str, Any]:
        return await self._send("Metrics")

    # ── Serialization ─────────────────────────────────────────────────────

    async def to_json(self) -> str:
        return await self._send("ToJson")

    async def from_json(self, json_str: str) -> None:
        await self._send("FromJson", {"json_str": json_str})

    # ── Ledger ────────────────────────────────────────────────────────────

    async def get_ledger(self) -> list[str]:
        return await self._send("GetLedger")

    async def clear_ledger(self) -> None:
        await self._send("ClearLedger")

    async def apply_ledger(self, transactions: list[str]) -> None:
        await self._send("ApplyLedger", {"transactions": transactions})

    # ── Multi-Tenant Graph Management ─────────────────────────────────────

    async def create_graph(self, graph_name: str, graph_type: str = "Agent") -> None:
        await self._send(
            "CreateGraph",
            {
                "graph_name": graph_name,
                "graph_type": graph_type,
            },
        )

    async def delete_graph(self, graph_name: str) -> None:
        await self._send("DeleteGraph", {"graph_name": graph_name})

    async def list_graphs(self) -> list[dict[str, str]]:
        return await self._send("ListGraphs")

    # ── Dynamic Communication Channels ────────────────────────────────────

    async def create_channel(
        self,
        channel_id: str,
        channel_type: str = "Group",
        creator: str = "",
        initial_members: list[str] | None = None,
    ) -> None:
        await self._send(
            "CreateChannel",
            {
                "channel_id": channel_id,
                "channel_type": channel_type,
                "creator": creator,
                "initial_members": initial_members or [],
            },
        )

    async def join_channel(self, channel_id: str, agent_id: str) -> None:
        await self._send(
            "JoinChannel",
            {
                "channel_id": channel_id,
                "agent_id": agent_id,
            },
        )

    async def leave_channel(self, channel_id: str, agent_id: str) -> Any:
        return await self._send(
            "LeaveChannel",
            {
                "channel_id": channel_id,
                "agent_id": agent_id,
            },
        )

    async def close_channel(
        self,
        channel_id: str,
        summary_embedding: list[float] | None = None,
        topic_metadata: str | None = None,
    ) -> Any:
        return await self._send(
            "CloseChannel",
            {
                "channel_id": channel_id,
                "summary_embedding": summary_embedding,
                "topic_metadata": topic_metadata,
            },
        )

    async def send_message(self, channel_id: str, sender: str, payload: str) -> None:
        await self._send(
            "SendMessage",
            {
                "channel_id": channel_id,
                "sender": sender,
                "payload": payload,
            },
        )

    async def get_channel_messages(
        self, channel_id: str, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return await self._send(
            "GetChannelMessages",
            {
                "channel_id": channel_id,
                "limit": limit,
            },
        )

    async def list_channels(self) -> list[dict[str, Any]]:
        return await self._send("ListChannels")

    async def get_channel_members(self, channel_id: str) -> list[str]:
        return await self._send("GetChannelMembers", {"channel_id": channel_id})

    # ── Service-Level ─────────────────────────────────────────────────────

    async def ping(self) -> str:
        return await self._send("Ping")

    async def checkpoint(self) -> str:
        return await self._send("Checkpoint")

    async def reconcile(self, graph_name: str, json_str: str) -> str:
        return await self._send(
            "Reconcile",
            {
                "graph_name": graph_name,
                "json_str": json_str,
            },
        )

    async def shutdown(self) -> str:
        return await self._send("Shutdown")

    async def apply_mutation(self, event_type: str, query: str) -> str:
        return await self._send(
            "ApplyMutation",
            {
                "event_type": event_type,
                "query": query,
            },
        )

    # ── Zero-Trust Consensus ──────────────────────────────────────────────

    async def register_identity(
        self, agent_id: str, role: str, teams: list[str], signature: str
    ) -> str:
        return await self._send(
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
        return await self._send(
            "ApplyMultisigMutation",
            {
                "signatures": signatures,
                "threshold": threshold,
                "mutation_type": mutation_type,
                "query": query,
            },
        )


class SyncEpistemicGraphClient:
    """Synchronous wrapper around the async client for backward compatibility.

    Usage::

        client = SyncEpistemicGraphClient.connect(
            socket_path="/tmp/epistemic-graph.sock",
        )
        client.add_node("node1", {"type": "Agent"})
        client.close()
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
        if asyncio.iscoroutinefunction(attr):

            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                future = asyncio.run_coroutine_threadsafe(
                    attr(*args, **kwargs), self._loop
                )
                return future.result()

            return sync_wrapper
        return attr
