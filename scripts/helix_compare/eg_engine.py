"""Epistemic Graph adapter: the served engine over its UDS transport.

The server runs in its documented durable, authenticated service mode (HMAC
``eg2.`` envelopes, redb authority with Immediate durability, AEAD at rest --
which the durable multi-op path requires) with the OIDC requirement explicitly
opted out, as the test suite does. Reads use the native point methods where
they exist (``GetNodeProperties``, ``GetSuccessors``), an id-anchored Cypher
match for the filtered hop, and UQL for the prefiltered text/vector and mixed
pipelines. Writes go through ``BatchUpdate``: one validated, atomic, durable
transaction per call.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .dataset import Citation, Dataset, Doc

AGENT = "service:helix-compare"
# Fresh per process: the bench server lives only for this run, so nothing is
# ever configured with a reusable credential.
SIGNER_KEY = secrets.token_hex(32)
AUTH_SECRET = secrets.token_hex(32)
AT_REST_KEY = secrets.token_hex(32)
AUDIENCE = "epistemic-graph-bench"
TENANT = "tenant:bench"
POLICY = "policy:bench"
GRAPH = "helixbench"
LOAD_CHUNK = 2000
START_TIMEOUT_S = 120.0
# A restarted engine re-materializes a graph lazily and refuses reads with a
# retryable PARTIAL_MATERIALIZATION until it is complete; the harness waits for
# `valid` (bounded) and reports how long it took and how the cursor moved.
READY_TIMEOUT_S = 1800.0
READY_POLL_S = 0.5


def _context(*, bootstrap: bool = False) -> dict[str, Any]:
    return {
        "principal": AGENT,
        "tenant": TENANT,
        "audience": AUDIENCE,
        "agent_id": AGENT,
        "roles": [] if bootstrap else ["bench"],
        "scopes": ["security:bootstrap"] if bootstrap else ["*"],
        "policy_version": POLICY,
        "delegation": [],
    }


def server_env(workdir: Path) -> dict[str, str]:
    signers = {
        AGENT: {"key": SIGNER_KEY, "allowed_roles": [], "may_grant_system": True}
    }
    return {
        "GRAPH_SERVICE_AUTH_SECRET": AUTH_SECRET,
        "EPISTEMIC_GRAPH_REQUIRE_OIDC": "false",
        "EPISTEMIC_GRAPH_AUDIENCE": AUDIENCE,
        "EPISTEMIC_GRAPH_TENANT": TENANT,
        "EPISTEMIC_GRAPH_POLICY_VERSION": POLICY,
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR": str(workdir / "security"),
        "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON": json.dumps(signers),
        "GRAPH_SERVICE_PERSIST_DIR": str(workdir / "persist"),
        "EPISTEMIC_GRAPH_ENCRYPTION_KEY": AT_REST_KEY,
    }


def node_properties(doc: Doc) -> dict[str, Any]:
    return {
        "type": "Doc",
        "key": doc.key,
        "category": doc.category,
        "year": doc.year,
        "text": doc.text,
    }


def doc_operations(doc: Doc) -> list[dict[str, Any]]:
    return [
        {"op": "add_node", "id": doc.key, "properties": node_properties(doc)},
        {"op": "add_embedding", "id": doc.key, "embedding": list(doc.embedding)},
    ]


def citation_operation(citation: Citation) -> dict[str, Any]:
    return {
        "op": "add_edge",
        "source": citation.source,
        "target": citation.target,
        "properties": {"relationship": "CITES", "weight": citation.weight},
    }


def _quote(value: str) -> str:
    return json.dumps(value)


def _vector(vector: Sequence[float]) -> str:
    return "[" + ", ".join(repr(float(value)) for value in vector) + "]"


def uql_ids(result: Any) -> list[str]:
    """Row ids from either UQL result shape (plain rows or ``{"kind": "rows"}``)."""

    rows = result.get("rows", []) if isinstance(result, dict) else result
    return [row["id"] if isinstance(row, dict) else row[0] for row in rows or []]


def cypher_ids(rows: Sequence[dict[str, Any]], column: str) -> set[str]:
    ids = set()
    for row in rows:
        cell = row[column]
        ids.add(cell["id"] if isinstance(cell, dict) else str(cell))
    return ids


class EgEngine:
    name = "epistemic-graph"

    def __init__(self, binary: Path, workdir: Path, cpus: set[int]) -> None:
        self.binary, self.workdir, self.cpus = binary, workdir, cpus
        self.data_dir = workdir / "persist"
        self.socket = workdir / "engine.sock"
        self.process: subprocess.Popen[bytes] | None = None
        self.client: Any = None
        self._bootstrapped = False
        self.ready_s = 0.0
        self.materialization: list[dict[str, Any]] = []

    @property
    def pid(self) -> int:
        assert self.process is not None
        return self.process.pid

    async def start(self) -> None:
        for directory in (self.data_dir, self.workdir / "security"):
            directory.mkdir(parents=True, exist_ok=True)
        self.socket.unlink(missing_ok=True)
        began = asyncio.get_running_loop().time()
        self.process = subprocess.Popen(
            [str(self.binary), "--socket-path", str(self.socket)],
            env={**os.environ, **server_env(self.workdir)},
            stdout=subprocess.DEVNULL,
            stderr=(self.workdir / "server.log").open("ab"),
            preexec_fn=lambda: os.sched_setaffinity(0, self.cpus),
        )
        await self._wait_for_socket()
        if not self._bootstrapped:
            await self._bootstrap()
        self.client = await self._connect(_context(), GRAPH)
        await self._wait_materialized()
        self.ready_s = round(asyncio.get_running_loop().time() - began, 2)

    async def _wait_for_socket(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + START_TIMEOUT_S
        while not self.socket.exists():
            if self.process is not None and self.process.poll() is not None:
                raise RuntimeError(f"engine exited {self.process.returncode}")
            if loop.time() > deadline:
                raise TimeoutError("engine socket never appeared")
            await asyncio.sleep(0.1)

    async def _graph_entry(self) -> dict[str, Any]:
        admin = await self._connect(_context(), "__commons__")
        try:
            graphs = await admin.tenants.list()
        finally:
            await admin.close()
        return next((g for g in graphs if g.get("name") == GRAPH), {})

    async def _touch(self) -> None:
        """One graph read: a restarted engine opens a graph lazily on first touch
        and pages the rest in off the request path; a refusal here is expected."""

        try:
            await self.client.nodes.count()
        except RuntimeError as error:
            if "PARTIAL_MATERIALIZATION" not in str(error):
                raise

    async def _wait_materialized(self) -> None:
        loop = asyncio.get_running_loop()
        began, last = loop.time(), None
        self.materialization = []
        await self._touch()
        while loop.time() - began < READY_TIMEOUT_S:
            entry = await self._graph_entry()
            if entry.get("valid") is True:
                return
            state = (entry.get("materialization"), entry.get("completeness_cursor"))
            if state != last:
                at = round(loop.time() - began, 2)
                self.materialization.append({"at_s": at, "state": repr(state)})
                last = state
            await asyncio.sleep(READY_POLL_S)
        raise TimeoutError(f"graph never materialized: {self.materialization[-5:]}")

    async def _connect(self, context: dict[str, Any], graph: str) -> Any:
        from epistemic_graph.client import EpistemicGraphClient

        return await EpistemicGraphClient.connect(
            socket_path=str(self.socket),
            auth_secret=AUTH_SECRET,
            graph_name=graph,
            verified_context=context,
        )

    async def _bootstrap(self) -> None:
        bootstrap = await self._connect(_context(bootstrap=True), "__commons__")
        try:
            await bootstrap.consensus.bootstrap_system_identity(
                agent_id=AGENT, signer_id=AGENT, signer_key=SIGNER_KEY
            )
        finally:
            await bootstrap.close()
        admin = await self._connect(_context(), "__commons__")
        try:
            await admin.tenants.create(GRAPH, "Global")
        finally:
            await admin.close()
        self._bootstrapped = True

    async def stop(self) -> None:
        if self.client is not None:
            await self.client.close()
            self.client = None
        if self.process is not None:
            self.process.terminate()
            self.process.wait(timeout=60)
            self.process = None

    async def load(self, dataset: Dataset) -> int:
        operations = [op for doc in dataset.docs for op in doc_operations(doc)]
        operations += [citation_operation(c) for c in dataset.citations]
        for start in range(0, len(operations), LOAD_CHUNK):
            await self.client.lifecycle.batch_update(
                operations[start : start + LOAD_CHUNK]
            )
        return len(operations)

    async def ping(self, _: object) -> object:
        return await self.client.ping()

    async def point_get(self, key: str) -> dict[str, Any]:
        return await self.client.nodes.properties(key) or {}

    async def one_hop(self, key: str) -> set[str]:
        return set(await self.client.nodes.successors(key))

    async def filtered_hop(self, item: tuple[str, int]) -> set[str]:
        key, min_year = item
        rows = await self.client.query.cypher_read(
            f"MATCH (a {{id: {_quote(key)}}})-[:CITES]->(b) "
            f"WHERE b.year > {int(min_year)} RETURN b"
        )
        return cypher_ids(rows, "b")

    async def txn_batch(self, batch: Sequence[tuple[Doc, str]]) -> None:
        operations = [op for doc, _ in batch for op in doc_operations(doc)]
        operations += [
            citation_operation(Citation(doc.key, target, 0.5)) for doc, target in batch
        ]
        await self.client.lifecycle.batch_update(operations)

    async def _uql(self, text: str) -> list[str]:
        return uql_ids(await self.client.query.uql(text))

    async def text_prefilter(self, item: tuple[str, str, int]) -> list[str]:
        group, term, k = item
        return await self._uql(
            f"MATCH (:Doc) WHERE category = {_quote(group)} "
            f"|> TEXT {_quote(term)} |> LIMIT {int(k)}"
        )

    async def vector_prefilter(
        self, item: tuple[str, Sequence[float], int]
    ) -> list[str]:
        group, vector, k = item
        return await self._uql(
            f"MATCH (:Doc) WHERE category = {_quote(group)} "
            f"|> RANK BY ~{_vector(vector)} |> LIMIT {int(k)}"
        )

    async def mixed(self, item: tuple[str, Sequence[float], int]) -> list[str]:
        key, vector, k = item
        return await self._uql(
            f"MATCH (:Doc) WHERE key = {_quote(key)} |> TRAVERSE CITES {{1,2}} "
            f"|> RANK BY ~{_vector(vector)} |> LIMIT {int(k)}"
        )
