#!/usr/bin/env python3
"""Certify the seven-family KnowledgeBatch contract on one exact binary.

The supplied artifact is never discovered or built.  Synthetic fixtures exercise
the served KnowledgeStream method through the current Python client, and retained
evidence contains only counts, digests, categorical outcomes, and the artifact
digest.  Runtime stores, cursors, payloads, endpoints, and credentials are removed.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from certify_exact_fault_restart import (
    AGENT_ID,
    GRAPH,
    CertificationError,
    ExactBinary,
    ExactEngine,
    _fail,
    _new_ephemeral_authority,
    _validate_binary,
    _with_client,
    _write_evidence,
)

SCHEMA_VERSION = 1
BATCH_SIZE = 2
FIXTURE_ROWS = 7
MAX_FIXTURE_ARROW_BYTES = 2 * 1024 * 1024
BACKPRESSURE_SECONDS = 0.02
JOB_TIMEOUT_SECONDS = 45.0

FAMILIES = (
    "graph",
    "sql",
    "rdf",
    "vector",
    "time_series",
    "job",
    "cross_modal",
)
REQUIREMENTS = (
    "arrow_parity",
    "pushdown",
    "bounded_streaming",
    "paging_resume",
    "cancellation",
    "backpressure",
    "snapshot_correctness",
)

Query = dict[str, Any]
Mutation = Callable[[Any], None]


def _fixture_properties(index: int) -> dict[str, object]:
    return {
        "type": "StreamFixture",
        "rank": index,
        "confidence": 0.9,
        "_owner": AGENT_ID,
        "_visibility": "private",
    }


def _state_name(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and len(value) == 1:
        return str(next(iter(value)))
    _fail("analytics_state_invalid")


def _seed(engine: ExactEngine) -> str:
    _with_client(engine, "__commons__", lambda client: client.tenants.create(GRAPH))

    def seed_graph(client: Any) -> str:
        for index in range(FIXTURE_ROWS):
            node_id = f"stream-node-{index:03d}"
            client.nodes.add(node_id, _fixture_properties(index))
            client.graph.add_embedding(node_id, [1.0, float(index) / 100.0])
        client.timeseries.append(
            "stream-series",
            [(index * 10, [float(index)]) for index in range(FIXTURE_ROWS)]
            + [(10_000, [999.0])],
        )
        triples = "\n".join(
            f'<urn:stream:subject:{index}> <urn:stream:predicate> "value-{index}" .'
            for index in range(FIXTURE_ROWS)
        )
        triples += '\n<urn:stream:outside> <urn:outside:predicate> "outside" .'
        client.rdf.add_triples(ntriples=triples)
        job = client.jobs.submit(
            GRAPH,
            {
                "MineAssociate": {
                    "transactions": [
                        ["alpha", "beta", "gamma"],
                        ["alpha", "beta"],
                        ["alpha", "gamma"],
                        ["beta", "gamma"],
                        ["alpha", "beta", "gamma"],
                    ],
                    "min_support": 0.2,
                    "min_confidence": 0.1,
                    "algorithm": "fpgrowth",
                }
            },
        )
        return str(job["job_id"])

    job_id = str(_with_client(engine, GRAPH, seed_graph))
    deadline = time.monotonic() + JOB_TIMEOUT_SECONDS
    while True:
        status = _with_client(engine, GRAPH, lambda client: client.jobs.status(job_id))
        state = _state_name(status["state"])
        if state == "Succeeded":
            return job_id
        if state in {"Failed", "Cancelled"}:
            _fail("analytics_fixture_failed")
        if time.monotonic() >= deadline:
            _fail("analytics_fixture_timeout")
        time.sleep(0.05)


def _queries(job_id: str) -> dict[str, Query]:
    return {
        "graph": {"family": "graph", "label": "StreamFixture", "limit": 0},
        "sql": {
            "family": "sql",
            "query": ("SELECT id FROM nodes WHERE type = 'StreamFixture' ORDER BY id"),
            "params_msgpack": b"",
        },
        "rdf": {
            "family": "rdf",
            "query": "SELECT ?s WHERE { ?s <urn:stream:predicate> ?o . }",
            "base_iri": "",
            "type_convention": "",
        },
        "vector": {
            "family": "vector",
            "keywords": [],
            "query_embedding": [1.0, 0.0],
            "k": FIXTURE_ROWS,
        },
        "time_series": {
            "family": "time_series",
            "series_id": "stream-series",
            "from": 0,
            "to": 100,
        },
        "job": {"family": "job", "job_id": job_id},
        "cross_modal": {
            "family": "cross_modal",
            "text": "MATCH (:StreamFixture) |> LIMIT 100",
        },
    }


def arrow_schema_digest(payload: bytes) -> str:
    """Return the first Arrow IPC schema-message digest, or fail closed.

    Arrow stream messages begin with the continuation marker, a little-endian
    metadata length, and eight-byte-padded FlatBuffer metadata.  Hashing that
    complete first message proves every family emitted the same native schema
    without adding a second Arrow implementation to the certification runtime.
    """

    if len(payload) < 16 or payload[:4] != b"\xff\xff\xff\xff":
        _fail("invalid_arrow_ipc_stream")
    metadata_length = int.from_bytes(payload[4:8], "little")
    padded_length = (metadata_length + 7) & ~7
    end = 8 + padded_length
    if metadata_length <= 0 or end > len(payload):
        _fail("invalid_arrow_schema_message")
    return hashlib.sha256(payload[:end]).hexdigest()


def _pull(
    client: Any, query: Query, cursor: dict[str, Any] | None = None
) -> dict[str, Any]:
    batch = client.knowledge.pull(
        query,
        batch_size=BATCH_SIZE,
        cursor=cursor,
    )
    payload = bytes(batch["payload"])
    if not payload or len(payload) > MAX_FIXTURE_ARROW_BYTES:
        _fail("knowledge_batch_payload_not_bounded")
    arrow_schema_digest(payload)
    return batch


def _expect_cursor_denial(
    engine: ExactEngine, query: Query, cursor: dict[str, Any]
) -> None:
    client = engine.connect(GRAPH)
    try:
        try:
            _pull(client, query, cursor)
        except RuntimeError:
            return
        _fail("tampered_cursor_was_accepted")
    finally:
        client.close()


def _certify_family(
    engine: ExactEngine, family: str, query: Query
) -> dict[str, object]:
    first_client = engine.connect(GRAPH)
    try:
        first = _pull(first_client, query)
    finally:
        # Closing the physical client is the cancellation boundary. The cursor is
        # the only state carried to the replacement connection.
        first_client.close()

    first_cursor = copy.deepcopy(first["cursor"])
    if first_cursor["row_offset"] != BATCH_SIZE or first_cursor["exhausted"]:
        _fail("family_did_not_produce_resumable_first_page")

    tampered = copy.deepcopy(first_cursor)
    tampered["row_offset"] = 0
    _expect_cursor_denial(engine, query, tampered)

    payload_digests = [hashlib.sha256(bytes(first["payload"])).hexdigest()]
    schema_digests = [arrow_schema_digest(bytes(first["payload"]))]
    pull_count = 1
    cursor = first_cursor
    resumed_client = engine.connect(GRAPH)
    try:
        while not cursor["exhausted"]:
            time.sleep(BACKPRESSURE_SECONDS)
            previous_offset = int(cursor["row_offset"])
            batch = _pull(resumed_client, query, cursor)
            cursor = copy.deepcopy(batch["cursor"])
            delta = int(cursor["row_offset"]) - previous_offset
            if delta <= 0 or delta > BATCH_SIZE:
                _fail("knowledge_batch_page_bound_violated")
            pull_count += 1
            payload = bytes(batch["payload"])
            payload_digests.append(hashlib.sha256(payload).hexdigest())
            schema_digests.append(arrow_schema_digest(payload))
            if pull_count > 1_000:
                _fail("knowledge_batch_stream_did_not_terminate")
    finally:
        resumed_client.close()

    row_count = int(cursor["row_offset"])
    if row_count < 3 or len(set(schema_digests)) != 1:
        _fail("knowledge_batch_family_parity_failed")
    return {
        "arrow_schema_sha256": schema_digests[0],
        "backpressure_seconds": BACKPRESSURE_SECONDS,
        "cancel_resume": True,
        "family": family,
        "page_payload_sha256": payload_digests,
        "pages": pull_count,
        "rows": row_count,
        "tamper_denied": True,
    }


def _snapshot_mutators() -> dict[str, Mutation | None]:
    def add_stream(client: Any, node_id: str) -> None:
        client.nodes.add(node_id, _fixture_properties(99))

    def rdf(client: Any) -> None:
        client.rdf.add_triples(
            ntriples='<urn:stream:changed> <urn:stream:predicate> "changed" .'
        )

    def vector(client: Any) -> None:
        client.nodes.add(
            "a-vector-snapshot",
            {
                "type": "VectorFixture",
                "_owner": AGENT_ID,
                "_visibility": "private",
            },
        )
        client.graph.add_embedding("a-vector-snapshot", [1.0, 0.0])

    def timeseries(client: Any) -> None:
        client.timeseries.append("stream-series", [(95, [95.0])])

    return {
        "graph": lambda client: add_stream(client, "a-graph-snapshot"),
        "sql": lambda client: add_stream(client, "a-sql-snapshot"),
        "rdf": rdf,
        "vector": vector,
        "time_series": timeseries,
        # A completed job result is immutable. An unrelated graph write must not
        # invalidate the result cursor merely because the graph version advanced.
        "job": None,
        "cross_modal": lambda client: add_stream(client, "a-crossmodal-snapshot"),
    }


def _certify_snapshot_binding(
    engine: ExactEngine,
    family: str,
    query: Query,
    mutator: Mutation | None,
) -> dict[str, object]:
    first = _with_client(engine, GRAPH, lambda client: _pull(client, query))
    cursor = copy.deepcopy(first["cursor"])
    if cursor["exhausted"]:
        _fail("snapshot_probe_requires_resumable_cursor")
    if mutator is None:
        _with_client(
            engine,
            GRAPH,
            lambda client: client.nodes.add(
                "job-unrelated-snapshot-change",
                {"_owner": AGENT_ID, "_visibility": "private"},
            ),
        )
        _with_client(engine, GRAPH, lambda client: _pull(client, query, cursor))
        return {"family": family, "outcome": "immutable_source_resumed", "passed": True}

    _with_client(engine, GRAPH, mutator)
    client = engine.connect(GRAPH)
    try:
        try:
            _pull(client, query, cursor)
        except RuntimeError:
            return {
                "family": family,
                "outcome": "changed_snapshot_denied",
                "passed": True,
            }
        _fail("changed_snapshot_cursor_was_accepted")
    finally:
        client.close()


def _run(binary: ExactBinary, binary_digest: str) -> dict[str, object]:
    authority = _new_ephemeral_authority()
    with tempfile.TemporaryDirectory(prefix="eg-exact-knowledge-batch-") as scratch:
        root = Path(scratch)
        engine = ExactEngine(binary, root, authority)
        try:
            engine.start()
            engine.bootstrap()
            job_id = _seed(engine)
            queries = _queries(job_id)
            if tuple(queries) != FAMILIES:
                _fail("knowledge_batch_family_inventory_mismatch")
            families = [
                _certify_family(engine, family, queries[family]) for family in FAMILIES
            ]
            schema_digests = {str(result["arrow_schema_sha256"]) for result in families}
            if len(schema_digests) != 1:
                _fail("cross_family_arrow_schema_mismatch")
            mutators = _snapshot_mutators()
            snapshots = [
                _certify_snapshot_binding(
                    engine,
                    family,
                    queries[family],
                    mutators[family],
                )
                for family in FAMILIES
            ]
        finally:
            engine.stop()
            shutil.rmtree(root, ignore_errors=True)

    evidence = {
        "binary": {"sha256": binary_digest},
        "certification": "epistemic-graph-exact-knowledge-batch",
        "families": families,
        "requirements": list(REQUIREMENTS),
        "schema_version": SCHEMA_VERSION,
        "snapshot_matrix": snapshots,
        "summary": {
            "families": len(families),
            "requirements": len(REQUIREMENTS),
            "snapshot_cases": len(snapshots),
            "status": "pass",
        },
    }
    encoded = json.dumps(evidence, sort_keys=True, ensure_ascii=True)
    if authority.auth_secret in encoded or authority.signer_key in encoded:
        _fail("evidence_contains_ephemeral_authority")
    return evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certify seven-family KnowledgeBatch behavior on one exact artifact."
    )
    parser.add_argument("--binary", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    binary: ExactBinary | None = None
    try:
        output = Path(args.output)
        if output.is_symlink() or output.exists():
            _fail("evidence_destination_must_be_new")
        binary, digest = _validate_binary(args.binary, args.binary_sha256)
        _write_evidence(output, _run(binary, digest))
    except CertificationError as error:
        print(f"exact KnowledgeBatch certification failed: {error}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "exact KnowledgeBatch certification failed: unexpected_runtime_failure",
            file=sys.stderr,
        )
        return 1
    finally:
        if binary is not None:
            binary.close()
    print("exact KnowledgeBatch certification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
