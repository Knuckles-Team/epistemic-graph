"""HelixDB adapter: the pinned standalone server over ``POST /v2/query``.

The server runs from its own build of the pinned commit, disk-backed
(``HELIX_DATA_DIR``), and is driven through the pinned revision's Python SDK
(``sdks/python``, async HTTPX client with a reused connection pool). Every write
sets ``X-Helix-Await-Durable`` so an acknowledged write has been flushed, the
same contract as Epistemic Graph's commit-before-ack. The idiomatic Helix
schema is declared up front: a node-equality index on the prefilter property
and the text and vector indexes the ranked workloads search, all activated
before any data is written.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .dataset import Citation, Dataset, Doc

LABEL = "Doc"
LOAD_NODES_PER_BATCH = 500
LOAD_EDGES_PER_BATCH = 1000
START_TIMEOUT_S = 120.0
INDEX_TIMEOUT_S = 300.0


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def rows(result: Any, name: str) -> list[dict[str, Any]]:
    value = result.get(name) if isinstance(result, dict) else None
    if isinstance(value, dict) and "rows" in value:
        value = value["rows"]
    if value is None:
        return []
    return list(value) if isinstance(value, list) else [value]


class HelixEngine:
    name = "helixdb"

    def __init__(self, binary: Path, workdir: Path, cpus: set[int]) -> None:
        self.binary, self.workdir, self.cpus = binary, workdir, cpus
        self.data_dir = workdir / "data"
        self.process: subprocess.Popen[bytes] | None = None
        self.client: Any = None
        self.probe: Any = None
        self.ids: dict[str, int] = {}
        self.url = ""

    @property
    def pid(self) -> int:
        assert self.process is not None
        return self.process.pid

    async def start(self) -> None:
        import httpx
        from helixdb import AsyncClient

        self.data_dir.mkdir(parents=True, exist_ok=True)
        http, grpc = _free_port(), _free_port()
        self.url = f"http://127.0.0.1:{http}"
        env = {
            "HELIX_DATA_DIR": str(self.data_dir),
            "HELIX_HTTP_ADDR": f"127.0.0.1:{http}",
            "HELIX_GRPC_ADDR": f"127.0.0.1:{grpc}",
            "RUST_LOG": "warn",
        }
        self.process = subprocess.Popen(
            [str(self.binary)],
            env={**os.environ, **env},
            stdout=subprocess.DEVNULL,
            stderr=(self.workdir / "server.log").open("ab"),
            preexec_fn=lambda: os.sched_setaffinity(0, self.cpus),
        )
        limits = httpx.Limits(max_connections=64, max_keepalive_connections=64)
        self.client = AsyncClient(self.url, timeout=120.0, limits=limits)
        self.probe = httpx.AsyncClient(limits=limits)
        await self._wait_ready(httpx)

    async def _wait_ready(self, httpx: Any) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + START_TIMEOUT_S
        async with httpx.AsyncClient() as probe:
            while loop.time() < deadline:
                if self.process is not None and self.process.poll() is not None:
                    raise RuntimeError(f"helix exited {self.process.returncode}")
                try:
                    if (await probe.get(f"{self.url}/readyz")).status_code == 200:
                        return
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.1)
        raise TimeoutError("helix never became ready")

    async def stop(self) -> None:
        if self.client is not None:
            await self.client.close()
            await self.probe.aclose()
            self.client = self.probe = None
        if self.process is not None:
            self.process.terminate()
            self.process.wait(timeout=60)
            self.process = None

    async def _write(self, batch: Any) -> Any:
        from helixdb import QueryRequest

        return await self.client.execute(
            QueryRequest.write(batch), await_durability=True
        )

    async def _read(self, name: str, traversal: Any) -> list[dict[str, Any]]:
        from helixdb import QueryRequest, read_batch

        request = QueryRequest.read(
            read_batch().var_as(name, traversal).returning([name])
        )
        return rows(await self.client.query(request), name)

    async def create_indexes(self, dim: int) -> None:
        from helixdb import IndexSpec, VectorDistanceMetric, g, write_batch

        specs = {
            "category": IndexSpec.node_equality(LABEL, "category"),
            "text": IndexSpec.node_text(LABEL, "text", None),
            "vector": IndexSpec.node_vector(
                LABEL, "embedding", dim, VectorDistanceMetric.COSINE, None
            ),
        }
        for name, spec in specs.items():
            result = await self._write(
                write_batch()
                .var_as(name, g().create_index_if_not_exists(spec))
                .returning([name])
            )
            await self._await_index(rows(result, name)[0])

    async def _await_index(self, receipt_value: Any) -> None:
        from helixdb import (
            IndexDdlAlreadyActive,
            IndexOperationSucceeded,
            g,
            parse_index_ddl_receipt,
            parse_index_operation_status,
        )

        receipt = parse_index_ddl_receipt(receipt_value)
        if isinstance(receipt, IndexDdlAlreadyActive):
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + INDEX_TIMEOUT_S
        while loop.time() < deadline:
            found = await self._read(
                "op", g().get_index_operation(receipt.operation_id)
            )
            status = parse_index_operation_status(found[0])
            if isinstance(status, IndexOperationSucceeded):
                return
            await asyncio.sleep(0.2)
        raise TimeoutError(f"index operation {receipt.operation_id} never succeeded")

    @staticmethod
    def _node_properties(doc: Doc) -> dict[str, Any]:
        from helixdb import PropertyValue

        return {
            "key": doc.key,
            "category": doc.category,
            "year": doc.year,
            "text": doc.text,
            "embedding": PropertyValue.f32_array(doc.embedding),
        }

    async def _add_docs(self, docs: Sequence[Doc]) -> None:
        from helixdb import g, write_batch

        batch = write_batch()
        names = [f"n{index}" for index in range(len(docs))]
        for name, doc in zip(names, docs, strict=True):
            traversal = (
                g().add_n(LABEL, self._node_properties(doc)).value_map(["$id", "key"])
            )
            batch = batch.var_as(name, traversal)
        result = await self._write(batch.returning(names))
        for name in names:
            for row in rows(result, name):
                self.ids[str(row["key"])] = int(row["$id"])

    def _edge_batch(self, citations: Sequence[Citation]) -> Any:
        from helixdb import g, write_batch

        batch = write_batch()
        for index, citation in enumerate(citations):
            traversal = (
                g()
                .n(self.ids[citation.source])
                .add_e("CITES", self.ids[citation.target], {"weight": citation.weight})
                .count()
            )
            batch = batch.var_as(f"e{index}", traversal)
        return batch

    async def load(self, dataset: Dataset) -> int:
        await self.create_indexes(dataset.spec.dim)
        for start in range(0, len(dataset.docs), LOAD_NODES_PER_BATCH):
            await self._add_docs(dataset.docs[start : start + LOAD_NODES_PER_BATCH])
        edges = dataset.citations
        for start in range(0, len(edges), LOAD_EDGES_PER_BATCH):
            await self._write(
                self._edge_batch(edges[start : start + LOAD_EDGES_PER_BATCH])
            )
        return len(dataset.docs) + len(edges)

    async def ping(self, _: object) -> object:
        """Transport floor: one keep-alive HTTP round trip to ``/healthz``."""

        return (await self.probe.get(f"{self.url}/healthz")).raise_for_status()

    async def point_get(self, key: str) -> dict[str, Any]:
        from helixdb import g

        found = await self._read(
            "n", g().n(self.ids[key]).value_map(["category", "year"])
        )
        return found[0] if found else {}

    async def _keys(self, traversal: Any) -> list[str]:
        return [
            str(row["key"])
            for row in await self._read("r", traversal.value_map(["key"]))
        ]

    async def one_hop(self, key: str) -> set[str]:
        from helixdb import g

        return set(await self._keys(g().n(self.ids[key]).out("CITES")))

    async def filtered_hop(self, item: tuple[str, int]) -> set[str]:
        from helixdb import Predicate, g

        key, min_year = item
        hop = g().n(self.ids[key]).out("CITES").where(Predicate.gt("year", min_year))
        return set(await self._keys(hop))

    async def txn_batch(self, batch: Sequence[tuple[Doc, str]]) -> None:
        from helixdb import NodeRef, g, write_batch

        request = write_batch()
        for index, (doc, target) in enumerate(batch):
            node = (
                g().add_n(LABEL, self._node_properties(doc)).value_map(["$id", "key"])
            )
            request = request.var_as(f"n{index}", node)
            edge = (
                g()
                .n(NodeRef.var(f"n{index}"))
                .add_e("CITES", self.ids[target], {"weight": 0.5})
            )
            request = request.var_as(f"e{index}", edge.count())
        names = [f"n{index}" for index in range(len(batch))]
        result = await self._write(request.returning(names))
        for name in names:
            for row in rows(result, name):
                self.ids[str(row["key"])] = int(row["$id"])

    def _category(self, group: str) -> Any:
        from helixdb import SourcePredicate, g

        return g().n_with_label_where(LABEL, SourcePredicate.eq("category", group))

    async def text_prefilter(self, item: tuple[str, str, int]) -> list[str]:
        group, term, k = item
        return await self._keys(
            self._category(group).text_search(LABEL, "text", term, k)
        )

    async def vector_prefilter(
        self, item: tuple[str, Sequence[float], int]
    ) -> list[str]:
        group, vector, k = item
        ranked = self._category(group).vector_search(
            LABEL, "embedding", list(vector), k
        )
        return await self._keys(ranked)

    async def mixed(self, item: tuple[str, Sequence[float], int]) -> list[str]:
        from helixdb import g, sub

        key, vector, k = item
        reach = (
            g()
            .n(self.ids[key])
            .union([sub().out("CITES"), sub().out("CITES").out("CITES")])
            .dedup()
        )
        return await self._keys(
            reach.vector_search(LABEL, "embedding", list(vector), k)
        )
