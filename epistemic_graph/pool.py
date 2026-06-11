"""Connection pooling and shard routing for epistemic-graph Tokio service.

Provides an async ConnectionPool to manage multiple connections to a single service endpoint,
and a ShardRouter that distributes graphs across multiple backend endpoints using
Highest Random Weight (HRW) consistent hashing.
"""

import asyncio
import hashlib
import logging

from .client import EpistemicGraphClient

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Async connection pool for EpistemicGraphClient instances."""

    def __init__(
        self,
        endpoint: str,
        auth_secret: str | None = None,
        min_size: int = 1,
        max_size: int = 10,
        agent_id: str | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.auth_secret = auth_secret
        # Optional caller identity forwarded on every request for server-side
        # ACL enforcement (see the server's isolation layer).
        self.agent_id = agent_id
        self.min_size = min_size
        self.max_size = max_size
        self._pool: asyncio.Queue[EpistemicGraphClient] = asyncio.Queue(
            maxsize=max_size
        )
        self._active_connections = 0
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize the pool with min_size connections."""
        async with self._lock:
            for _ in range(self.min_size):
                client = await self._create_client()
                self._pool.put_nowait(client)

    async def _create_client(self) -> EpistemicGraphClient:
        if self.endpoint.startswith("tcp://"):
            tcp_addr = self.endpoint[6:]
            client = await EpistemicGraphClient.connect(
                tcp_addr=tcp_addr, auth_secret=self.auth_secret, agent_id=self.agent_id
            )
        elif self.endpoint.startswith("unix://"):
            socket_path = self.endpoint[7:]
            client = await EpistemicGraphClient.connect(
                socket_path=socket_path,
                auth_secret=self.auth_secret,
                agent_id=self.agent_id,
            )
        else:
            # Default to socket if no scheme provided
            client = await EpistemicGraphClient.connect(
                socket_path=self.endpoint,
                auth_secret=self.auth_secret,
                agent_id=self.agent_id,
            )

        self._active_connections += 1
        return client

    async def acquire(self) -> EpistemicGraphClient:
        """Acquire a healthy connection from the pool."""
        try:
            client = self._pool.get_nowait()
            # Test health on checkout
            try:
                await client.ping()
            except Exception:
                logger.warning("Connection pool found dead connection, reopening...")
                async with self._lock:
                    self._active_connections -= 1
                client = await self._create_client()
            return client
        except asyncio.QueueEmpty:
            async with self._lock:
                if self._active_connections < self.max_size:
                    client = await self._create_client()
                    return client
            # Wait for an available connection
            client = await self._pool.get()
            return client

    def release(self, client: EpistemicGraphClient) -> None:
        """Release a connection back to the pool."""
        try:
            self._pool.put_nowait(client)
        except asyncio.QueueFull:
            # Shouldn't happen if managed correctly, but close excess
            asyncio.create_task(client.close())
            self._active_connections -= 1

    async def close_all(self) -> None:
        """Close all connections in the pool."""
        async with self._lock:
            while not self._pool.empty():
                client = self._pool.get_nowait()
                await client.close()
                self._active_connections -= 1


class ShardRouter:
    """Routes graphs to specific shard endpoints using HRW consistent hashing."""

    def __init__(
        self,
        endpoints: list[str],
        auth_secret: str | None = None,
        min_size: int = 1,
        max_size: int = 10,
        agent_id: str | None = None,
    ) -> None:
        if not endpoints:
            raise ValueError("ShardRouter requires at least one endpoint")
        self.endpoints = endpoints
        self.pools: dict[str, ConnectionPool] = {}
        for ep in endpoints:
            self.pools[ep] = ConnectionPool(
                ep, auth_secret, min_size, max_size, agent_id=agent_id
            )

    async def initialize(self) -> None:
        """Initialize all connection pools."""
        for pool in self.pools.values():
            await pool.initialize()

    def _get_shard_endpoint(self, graph_name: str) -> str:
        # Rendezvous hashing (HRW)
        max_score = -1
        best_endpoint = self.endpoints[0]

        for ep in self.endpoints:
            # Hash endpoint + graph_name
            s = f"{ep}-{graph_name}".encode()
            # MD5 used only for rendezvous/HRW shard selection, never security.
            score = int(hashlib.md5(s, usedforsecurity=False).hexdigest(), 16)  # nosec B324
            if score > max_score:
                max_score = score
                best_endpoint = ep

        return best_endpoint

    async def acquire(self, graph_name: str) -> EpistemicGraphClient:
        """Acquire a client for the specific graph."""
        ep = self._get_shard_endpoint(graph_name)
        client = await self.pools[ep].acquire()
        # Set the target graph for this checkout
        client._graph_name = graph_name
        return client

    def release(self, client: EpistemicGraphClient, graph_name: str) -> None:
        """Release a client back to its corresponding pool."""
        ep = self._get_shard_endpoint(graph_name)
        if ep in self.pools:
            self.pools[ep].release(client)
        else:
            asyncio.create_task(client.close())

    async def close_all(self) -> None:
        """Close all pools in the router."""
        for pool in self.pools.values():
            await pool.close_all()
