#!/usr/bin/env python3
"""Certify commit atomicity and restart completeness against one exact binary.

This is an artifact-certification harness, not a build helper.  It refuses to
discover or compile an engine: the caller supplies both the executable and its
expected SHA-256.  Every case uses an isolated durable store, disables core
dumps, removes all runtime material, and retains only aggregate, synthetic,
path-free evidence.

The process-fault seam is intentionally exact.  A fault is armed for one
authenticated request id, one canonical mutation domain, one commit phase, and
one random 256-bit nonce.  The engine aborts at that boundary; the harness then
restarts the same artifact over the same store and observes authoritative state.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import resource
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from epistemic_graph.client import SyncEpistemicGraphClient  # noqa: E402

SCHEMA_VERSION = 1
AGENT_ID = "service:exact-certifier"
AUDIENCE = "epistemic-graph-certification"
POLICY_VERSION = "policy:certification"
TENANT = "tenant:certification-a"
SECOND_TENANT = "tenant:certification-b"
GRAPH = "certification-graph"
COMMONS = "__commons__"
STARTUP_TIMEOUT_SECONDS = 60.0
SHUTDOWN_TIMEOUT_SECONDS = 15.0
RECOVERY_TIMEOUT_SECONDS = 30.0

EXACT_OPTIONAL_LISTENER_ENV = (
    "EPISTEMIC_GRAPH_PGWIRE_ADDR",
    "EPISTEMIC_GRAPH_MYSQL_ADDR",
    "EPISTEMIC_GRAPH_MSSQL_ADDR",
    "EPISTEMIC_GRAPH_SQLITE_ADDR",
    "EPISTEMIC_GRAPH_BOLT_ADDR",
    "EPISTEMIC_GRAPH_REDIS_ADDR",
    "EPISTEMIC_GRAPH_AMQP_ADDR",
    "EPISTEMIC_GRAPH_MQTT_ADDR",
    "EPISTEMIC_GRAPH_STOMP_ADDR",
)

PHASES = (
    "before_rows",
    "after_rows_before_metadata",
    "before_commit",
    "after_commit_before_ack",
)
PRECOMMIT_PHASES = frozenset(PHASES[:-1])
DOMAINS = (
    "graph_rows",
    "graph_snapshot",
    "rdf_dataset",
    "sql_catalog",
    "blob_store",
    "kv_store",
    "time_series",
    "analytics_job",
    "broker",
    "cross_modal",
    "multi_graph",
    "lifecycle",
    "control_plane",
)


class CertificationError(RuntimeError):
    """A privacy-safe certification failure code."""


def _fail(code: str) -> None:
    raise CertificationError(code)


@dataclass(frozen=True)
class EphemeralAuthority:
    """Process-local certification secrets that are never retained."""

    auth_secret: str
    signer_key: str


@dataclass
class ExactBinary:
    """Private immutable-by-policy copy of the caller's digest-pinned executable."""

    path: Path
    digest: str
    _root: tempfile.TemporaryDirectory[str]

    def close(self) -> None:
        self._root.cleanup()


def _new_ephemeral_authority() -> EphemeralAuthority:
    return EphemeralAuthority(
        auth_secret=secrets.token_urlsafe(48),
        signer_key=secrets.token_urlsafe(48),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_binary(path_text: str, expected_digest: str) -> tuple[ExactBinary, str]:
    if len(expected_digest) != 64 or any(
        byte not in "0123456789abcdef" for byte in expected_digest
    ):
        _fail("invalid_binary_digest")
    path = Path(path_text)
    descriptor: int | None = None
    output: int | None = None
    private_root: tempfile.TemporaryDirectory[str] | None = None
    try:
        path_metadata = os.lstat(path)
        if stat.S_ISLNK(path_metadata.st_mode):
            _fail("binary_must_not_be_symlink")
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
    except OSError:
        if descriptor is not None:
            os.close(descriptor)
        _fail("binary_unavailable")
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o111 == 0:
        os.close(descriptor)
        _fail("binary_not_executable")
    try:
        private_root = tempfile.TemporaryDirectory(prefix="eg-exact-binary-")
        root = Path(private_root.name)
        root.chmod(0o700)
        destination = root / "engine"
        output = os.open(
            destination,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            0o500,
        )
        digest = hashlib.sha256()
        while block := os.read(descriptor, 1024 * 1024):
            digest.update(block)
            view = memoryview(block)
            while view:
                written = os.write(output, view)
                if written <= 0:
                    _fail("binary_copy_failed")
                view = view[written:]
        os.fsync(output)
        os.close(output)
        output = None
        actual = digest.hexdigest()
        if actual != expected_digest or _sha256_file(destination) != expected_digest:
            _fail("binary_digest_mismatch")
        destination.chmod(0o500)
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return ExactBinary(destination, actual, private_root), actual
    except CertificationError:
        if private_root is not None:
            private_root.cleanup()
        raise
    except OSError:
        if private_root is not None:
            private_root.cleanup()
        _fail("binary_copy_failed")
    finally:
        if output is not None:
            os.close(output)
        os.close(descriptor)


def _context(
    tenant: str,
    *,
    bootstrap: bool = False,
    scopes: tuple[str, ...] | None = None,
) -> dict[str, object]:
    return {
        "principal": AGENT_ID,
        "tenant": tenant,
        "audience": AUDIENCE,
        "agent_id": AGENT_ID,
        "roles": [] if bootstrap else ["certifier"],
        "scopes": (
            ["security:bootstrap"]
            if bootstrap
            else list(scopes if scopes is not None else ("*",))
        ),
        "policy_version": POLICY_VERSION,
        "delegation": [],
    }


def _disable_core_dumps() -> None:
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


class ExactEngine:
    """Restart one supplied binary over one isolated durable root."""

    def __init__(
        self, binary: ExactBinary, root: Path, authority: EphemeralAuthority
    ) -> None:
        if not isinstance(binary, ExactBinary):
            _fail("unsealed_exact_binary")
        self.binary = binary
        self.root = root
        self.authority = authority
        self.persist_dir = root / "persist"
        self.security_dir = root / "security"
        self.backup_dir = root / "backups"
        self.home_dir = root / "home"
        self.temporary_dir = root / "temporary"
        self.socket_path = root / "engine.sock"
        self.log_path = root / "engine.log"
        self.process: subprocess.Popen[bytes] | None = None
        self._log: Any | None = None

    def start(
        self,
        *,
        tenant: str = TENANT,
        fault: dict[str, object] | None = None,
        lazy_page_size: int = 1,
        modality_source_limit: int | None = None,
        redb_shards: int = 1,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        if self.process is not None:
            _fail("engine_already_started")
        if isinstance(redb_shards, bool) or not 1 <= redb_shards <= 64:
            _fail("invalid_redb_shard_count")
        for directory in (
            self.persist_dir,
            self.security_dir,
            self.backup_dir,
            self.home_dir,
            self.temporary_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory.chmod(0o700)
        self.socket_path.unlink(missing_ok=True)
        self._log = self.log_path.open("wb")
        if _sha256_file(self.binary.path) != self.binary.digest:
            _fail("sealed_binary_digest_changed")
        env = {
            "HOME": str(self.home_dir),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "TMPDIR": str(self.temporary_dir),
            "TZ": "UTC",
            "GRAPH_SERVICE_AUTH_SECRET": self.authority.auth_secret,
            "EPISTEMIC_GRAPH_AUDIENCE": AUDIENCE,
            "EPISTEMIC_GRAPH_TENANT": tenant,
            "EPISTEMIC_GRAPH_POLICY_VERSION": POLICY_VERSION,
            "EPISTEMIC_GRAPH_SECURITY_STATE_DIR": str(self.security_dir),
            "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON": json.dumps(
                {AGENT_ID: self.authority.signer_key}, separators=(",", ":")
            ),
            "EPISTEMIC_GRAPH_REDB_COMMIT_POLICY": "each",
            "EPISTEMIC_GRAPH_REDB_SHARDS": str(redb_shards),
            "EPISTEMIC_GRAPH_LAZY_OPEN_PAGE_SIZE": str(lazy_page_size),
            "EPISTEMIC_GRAPH_BACKUP_ROOT": str(self.backup_dir),
            "RUST_BACKTRACE": "0",
        }
        if extra_env is not None:
            for name, value in extra_env.items():
                if (
                    name not in EXACT_OPTIONAL_LISTENER_ENV
                    or not isinstance(value, str)
                    or not value
                ):
                    _fail("invalid_exact_engine_environment_override")
            env.update(extra_env)
        if fault is not None:
            env["EPISTEMIC_GRAPH_CERTIFICATION_FAULT"] = json.dumps(
                fault, sort_keys=True, separators=(",", ":")
            )
        if modality_source_limit is not None:
            if (
                isinstance(modality_source_limit, bool)
                or not isinstance(modality_source_limit, int)
                or not 1 <= modality_source_limit <= 256 * 1024 * 1024
            ):
                _fail("invalid_modality_source_limit")
            env["EPISTEMIC_GRAPH_MODALITY_MAX_SOURCE_BYTES"] = str(
                modality_source_limit
            )
        self.process = subprocess.Popen(  # noqa: S603 - exact caller-supplied argv
            [
                str(self.binary.path),
                "--socket-path",
                str(self.socket_path),
                "--persist-dir",
                str(self.persist_dir),
            ],
            cwd=self.root,
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            preexec_fn=_disable_core_dumps,
            start_new_session=True,
        )
        try:
            self._wait_ready()
        except BaseException:
            self.stop()
            raise

    def crash(self) -> None:
        """Kill the exact process group without a graceful shutdown."""

        process = self.process
        if process is None or process.poll() is not None:
            _fail("crash_engine_missing")
        try:
            os.killpg(process.pid, signal.SIGKILL)
            return_code = process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            _fail("crash_engine_kill_failed")
        self._close_log()
        self.process = None
        self.socket_path.unlink(missing_ok=True)
        if return_code != -signal.SIGKILL:
            _fail("crash_engine_was_not_killed")

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            process = self.process
            if process is None or process.poll() is not None:
                _fail("engine_exited_during_startup")
            if self.socket_path.exists():
                try:
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                        probe.settimeout(0.25)
                        probe.connect(str(self.socket_path))
                    return
                except OSError:
                    pass
            time.sleep(0.05)
        _fail("engine_startup_timeout")

    def connect(
        self,
        graph: str = GRAPH,
        *,
        tenant: str = TENANT,
        scopes: tuple[str, ...] | None = None,
    ) -> SyncEpistemicGraphClient:
        return SyncEpistemicGraphClient.connect(
            socket_path=str(self.socket_path),
            auth_secret=self.authority.auth_secret,
            graph_name=graph,
            verified_context=_context(tenant, scopes=scopes),
            timeout=15.0,
            heavy_timeout=30.0,
            connect_timeout=5.0,
        )

    def bootstrap(self, *, tenant: str = TENANT) -> None:
        client = SyncEpistemicGraphClient.connect(
            socket_path=str(self.socket_path),
            auth_secret=self.authority.auth_secret,
            graph_name=COMMONS,
            verified_context=_context(tenant, bootstrap=True),
            timeout=15.0,
            connect_timeout=5.0,
        )
        try:
            client.consensus.bootstrap_system_identity(
                agent_id=AGENT_ID,
                signer_id=AGENT_ID,
                signer_key=self.authority.signer_key,
            )
        finally:
            client.close()

    def wait_for_abort(self, request_id: int) -> None:
        process = self.process
        if process is None:
            _fail("fault_engine_missing")
        try:
            return_code = process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            _fail("fault_did_not_terminate_engine")
        # The certified engine is the process-group leader.  Reap any helper it
        # may have spawned even though the leader has already aborted.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self._close_log()
        if return_code != -signal.SIGABRT:
            _fail("fault_engine_did_not_abort")
        try:
            log = self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            _fail("fault_marker_unavailable")
        marker = "EG_CERTIFICATION_FAULT"
        request_marker = f"request_id={request_id}"
        if marker not in log or request_marker not in log:
            _fail("fault_marker_missing")
        self.process = None
        self.socket_path.unlink(missing_ok=True)

    def stop(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        if process is not None:
            # A child that ignored SIGTERM must not escape when the leader exits
            # first.  A missing group simply means every member is already gone.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.process = None
        self._close_log()
        self.socket_path.unlink(missing_ok=True)

    def _close_log(self) -> None:
        if self._log is not None:
            self._log.close()
            self._log = None


State = dict[str, Any]
Setup = Callable[[ExactEngine, State], None]
Mutation = Callable[[ExactEngine, State], None]
Observe = Callable[[ExactEngine, State], bool]


@dataclass(frozen=True)
class Scenario:
    family: str
    domain: str
    request_id: int
    setup: Setup
    mutate: Mutation
    observe: Observe
    retry_after_restart: bool = False
    warm_graphs: tuple[str, ...] = ()


def _with_client(
    engine: ExactEngine,
    graph: str,
    operation: Callable[[SyncEpistemicGraphClient], Any],
    *,
    tenant: str = TENANT,
) -> Any:
    client = engine.connect(graph, tenant=tenant)
    try:
        return operation(client)
    finally:
        client.close()


def _retry_partial(operation: Callable[[], Any]) -> Any:
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECONDS
    while True:
        try:
            return operation()
        except RuntimeError as error:
            if "PARTIAL_MATERIALIZATION" not in str(error):
                raise
            if time.monotonic() >= deadline:
                _fail("materialization_recovery_timeout")
            time.sleep(0.02)


def _setup_graph(engine: ExactEngine, _state: State) -> None:
    _with_client(engine, COMMONS, lambda client: client.tenants.create(GRAPH))


def _node_present(engine: ExactEngine, node_id: str, graph: str = GRAPH) -> bool:
    return bool(
        _with_client(
            engine,
            graph,
            lambda client: _retry_partial(lambda: client.nodes.has(node_id)),
        )
    )


def _warm_graphs(engine: ExactEngine, graphs: tuple[str, ...]) -> None:
    """Finish lazy recovery on separate connections before the targeted RPC.

    Request ids are connection-local.  Warming on a separate client therefore
    cannot shift the exact id of the mutation client, and read methods cannot
    trigger a mutation-domain fault even when their id numerically matches it.
    """

    for graph in graphs:
        _with_client(
            engine,
            graph,
            lambda client: _retry_partial(client.nodes.count),
        )


def _direct_node_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.nodes.add(
            "direct-node", {"type": "Certification", "marker": "complete"}
        ),
    )


def _ordinary_txn_mutation(engine: ExactEngine, _state: State) -> None:
    def operation(client: SyncEpistemicGraphClient) -> None:
        txn_id = client.txn.begin()
        client.txn.add_node(
            txn_id,
            "transaction-node",
            {"type": "Certification", "marker": "complete"},
        )
        client.txn.commit(txn_id)

    _with_client(engine, GRAPH, operation)


def _sql_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.query.sql(
            "CREATE TABLE exact_certification (key TEXT, value BIGINT)"
        ),
    )


def _sql_present(engine: ExactEngine, _state: State) -> bool:
    def operation(client: SyncEpistemicGraphClient) -> bool:
        try:
            _retry_partial(
                lambda: client.query.sql("SELECT key FROM exact_certification LIMIT 1")
            )
        except RuntimeError as error:
            message = str(error).lower()
            if "table" in message and any(
                token in message
                for token in ("missing", "unknown", "not found", "does not exist")
            ):
                return False
            raise
        return True

    return bool(_with_client(engine, GRAPH, operation))


def _cypher_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.query.cypher_write(
            "CREATE (n:Certification {id: 'cypher-node', marker: 'complete'})"
        ),
    )


def _graphql_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.query.graphql(
            'mutation { createNode(label: "Certification", id: "graphql-node", '
            'props: {marker: "complete"}) { id } }'
        ),
    )


RDF_TRIPLE = '<urn:certification:subject> <urn:certification:predicate> "complete" .'


def _rdf_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.rdf.add_triples(ntriples=RDF_TRIPLE),
    )


def _rdf_present(engine: ExactEngine, _state: State) -> bool:
    value = _with_client(
        engine,
        GRAPH,
        lambda client: _retry_partial(client.rdf.get_rdf),
    )
    return RDF_TRIPLE in str(value)


def _lifecycle_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        COMMONS,
        lambda client: client.tenants.create("certification-lifecycle"),
    )


def _lifecycle_present(engine: ExactEngine, _state: State) -> bool:
    rows = _with_client(engine, COMMONS, lambda client: client.tenants.list())
    for row in rows:
        if not isinstance(row, dict):
            continue
        if any(
            row.get(field) == "certification-lifecycle"
            for field in ("name", "graph", "graph_name")
        ):
            return True
    return False


def _blob_setup(engine: ExactEngine, state: State) -> None:
    def operation(client: SyncEpistemicGraphClient) -> None:
        digest = client.blob.store(b"exact-certification-payload")
        if client.blob.incref(digest) != 1:
            _fail("blob_setup_refcount_invalid")
        state["blob_digest"] = digest

    _with_client(engine, COMMONS, operation)


def _blob_mutation(engine: ExactEngine, state: State) -> None:
    _with_client(
        engine,
        COMMONS,
        lambda client: client.blob.unref(state["blob_digest"]),
    )


def _blob_present(engine: ExactEngine, state: State) -> bool:
    # Refcount has no public read API.  Incrementing once distinguishes the two
    # authoritative states exactly: 2 means the fault rolled back; 1 means unref
    # committed.  The throwaway store is removed immediately after observation.
    count = _with_client(
        engine,
        COMMONS,
        lambda client: client.blob.incref(state["blob_digest"]),
    )
    return int(count) == 1


def _kv_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        COMMONS,
        lambda client: client._send(
            "KvPut",
            {
                "namespace": "certification",
                "key": "atomic-key",
                "value": b"complete",
            },
        ),
    )


def _kv_present(engine: ExactEngine, _state: State) -> bool:
    value = _with_client(
        engine,
        COMMONS,
        lambda client: client._send(
            "KvGet", {"namespace": "certification", "key": "atomic-key"}
        ),
    )
    return value == b"complete"


def _timeseries_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.timeseries.append("atomic-series", [(100, [7.0])]),
    )


def _timeseries_present(engine: ExactEngine, _state: State) -> bool:
    rows = _with_client(
        engine,
        GRAPH,
        lambda client: _retry_partial(
            lambda: client.timeseries.range("atomic-series", 0, 1000)
        ),
    )
    return rows == [(100, [7.0])]


def _job_state_name(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and len(value) == 1:
        return str(next(iter(value)))
    _fail("job_state_invalid")


def _job_setup(engine: ExactEngine, state: State) -> None:
    _setup_graph(engine, state)

    def operation(client: SyncEpistemicGraphClient) -> None:
        job = client.jobs.submit(
            GRAPH,
            {
                "MineAssociate": {
                    "transactions": [["a", "b"], ["a"]],
                    "min_support": 0.5,
                    "min_confidence": 0.5,
                    "algorithm": "fpgrowth",
                }
            },
            required_capabilities=["certification.unassigned"],
        )
        state["job_id"] = job["job_id"]

    _with_client(engine, GRAPH, operation)


def _job_mutation(engine: ExactEngine, state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.jobs.cancel(state["job_id"]),
    )


def _job_present(engine: ExactEngine, state: State) -> bool:
    record = _with_client(
        engine,
        GRAPH,
        lambda client: client.jobs.status(state["job_id"]),
    )
    return _job_state_name(record["state"]) == "Cancelled"


def _broker_setup(engine: ExactEngine, _state: State) -> None:
    _setup_graph(engine, {})

    def operation(client: SyncEpistemicGraphClient) -> None:
        client.broker.declare_exchange("certification-exchange", "direct")
        client.broker.declare_queue("certification-queue")
        client.broker.bind_queue(
            "certification-exchange", "certification-queue", "atomic"
        )

    _with_client(engine, GRAPH, operation)


def _broker_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.broker.publish(
            "certification-exchange", "atomic", b"complete"
        ),
    )


def _broker_present(engine: ExactEngine, _state: State) -> bool:
    message = _with_client(
        engine,
        GRAPH,
        lambda client: _retry_partial(
            lambda: client.broker.consume(
                "certification-queue",
                group="certification-group",
                consumer="certification-consumer",
                now_ms=1,
                lease_ms=1000,
            )
        ),
    )
    if message is None:
        return False
    _, properties = message
    payload = properties.get("payload")
    return payload in (
        b"complete",
        list(b"complete"),
        "complete",
        b"complete".hex(),
    )


def _cross_modal_mutation(engine: ExactEngine, _state: State) -> None:
    def operation(client: SyncEpistemicGraphClient) -> None:
        txn_id = client.txn.begin()
        client.txn.add_node(
            txn_id,
            "cross-modal-node",
            {"type": "Certification", "marker": "complete"},
        )
        client.txn.add_embedding(txn_id, "cross-modal-node", [0.25, 0.75])
        client.txn.add_measurement(txn_id, "cross-modal-series", [(200, [9.0])])
        client.txn.commit(txn_id)

    _with_client(engine, GRAPH, operation)


def _cross_modal_present(engine: ExactEngine, _state: State) -> bool:
    def operation(client: SyncEpistemicGraphClient) -> bool:
        node = _retry_partial(lambda: client.nodes.has("cross-modal-node"))
        ranked = _retry_partial(lambda: client.graph.semantic_search([0.25, 0.75], 5))
        points = _retry_partial(
            lambda: client.timeseries.range("cross-modal-series", 0, 1000)
        )
        return (
            bool(node)
            and any(row[0] == "cross-modal-node" for row in ranked)
            and points == [(200, [9.0])]
        )

    return bool(_with_client(engine, GRAPH, operation))


MULTI_GRAPHS = ("certification-multi-a", "certification-multi-b")


def _multi_graph_setup(engine: ExactEngine, _state: State) -> None:
    def operation(client: SyncEpistemicGraphClient) -> None:
        for graph in MULTI_GRAPHS:
            client.tenants.create(graph)

    _with_client(engine, COMMONS, operation)


def _multi_graph_mutation(engine: ExactEngine, _state: State) -> None:
    batches = {
        MULTI_GRAPHS[0]: [
            {
                "op": "add_node",
                "id": "multi-node-a",
                "properties": {"type": "Certification", "marker": "complete"},
            }
        ],
        MULTI_GRAPHS[1]: [
            {
                "op": "add_node",
                "id": "multi-node-b",
                "properties": {"type": "Certification", "marker": "complete"},
            }
        ],
    }
    _with_client(
        engine,
        COMMONS,
        lambda client: client.lifecycle.multi_graph_batch_update(batches),
    )


def _multi_graph_present(engine: ExactEngine, _state: State) -> bool:
    return _node_present(engine, "multi-node-a", MULTI_GRAPHS[0]) and _node_present(
        engine, "multi-node-b", MULTI_GRAPHS[1]
    )


def _work_item_setup(engine: ExactEngine, _state: State) -> None:
    _setup_graph(engine, {})
    _with_client(
        engine,
        GRAPH,
        lambda client: client.nodes.add(
            "certification-work-item",
            {
                "node_type": "WorkItem",
                "tenant": TENANT,
                "status": "ready",
                "kind": "certification",
                "payload_ref": "payload:sha256:" + ("0" * 64),
                "created_at": 1.0,
                "prio_bucket": 0,
                "attempt": 0,
                "max_attempts": 1,
            },
        ),
    )


def _work_item_mutation(engine: ExactEngine, _state: State) -> None:
    _with_client(
        engine,
        GRAPH,
        lambda client: client.work_items.claim(
            {
                "schema_version": "1",
                "tenant_ref": TENANT,
                "work_item_id": "certification-work-item",
                "queue_ref": None,
                "resource_class": None,
                "fairness_group": None,
                "worker_ref": "worker:certification",
                "now_ms": 1000,
                "lease_ms": 10_000,
                "max_tenant_in_flight": 1,
            }
        ),
    )


def _work_item_present(engine: ExactEngine, _state: State) -> bool:
    properties = _with_client(
        engine,
        GRAPH,
        lambda client: _retry_partial(
            lambda: client.nodes.properties("certification-work-item")
        ),
    )
    return bool(properties and properties.get("status") == "leased")


def _scenarios() -> tuple[Scenario, ...]:
    return (
        Scenario(
            "ordinary_transaction",
            "graph_rows",
            3,
            _setup_graph,
            _ordinary_txn_mutation,
            lambda engine, state: _node_present(engine, "transaction-node"),
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "typed_graph_mutation",
            "graph_snapshot",
            1,
            _setup_graph,
            _direct_node_mutation,
            lambda engine, state: _node_present(engine, "direct-node"),
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "cypher_mutation",
            "graph_snapshot",
            1,
            _setup_graph,
            _cypher_mutation,
            lambda engine, state: _node_present(engine, "cypher-node"),
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "graphql_mutation",
            "graph_snapshot",
            1,
            _setup_graph,
            _graphql_mutation,
            lambda engine, state: _node_present(engine, "graphql-node"),
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "rdf_mutation",
            "rdf_dataset",
            1,
            _setup_graph,
            _rdf_mutation,
            _rdf_present,
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "sql_catalog_mutation",
            "sql_catalog",
            1,
            _setup_graph,
            _sql_mutation,
            _sql_present,
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "blob_mutation",
            "blob_store",
            1,
            _blob_setup,
            _blob_mutation,
            _blob_present,
        ),
        Scenario(
            "kv_mutation",
            "kv_store",
            1,
            lambda engine, state: None,
            _kv_mutation,
            _kv_present,
        ),
        Scenario(
            "time_series_mutation",
            "time_series",
            1,
            _setup_graph,
            _timeseries_mutation,
            _timeseries_present,
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "analytics_job_mutation",
            "analytics_job",
            1,
            _job_setup,
            _job_mutation,
            _job_present,
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "broker_mutation",
            "broker",
            1,
            _broker_setup,
            _broker_mutation,
            _broker_present,
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "cross_modal_transaction",
            "cross_modal",
            5,
            _setup_graph,
            _cross_modal_mutation,
            _cross_modal_present,
            warm_graphs=(GRAPH,),
        ),
        Scenario(
            "multi_graph_saga",
            "multi_graph",
            1,
            _multi_graph_setup,
            _multi_graph_mutation,
            _multi_graph_present,
            retry_after_restart=True,
            warm_graphs=MULTI_GRAPHS,
        ),
        Scenario(
            "graph_lifecycle_mutation",
            "lifecycle",
            1,
            lambda engine, state: None,
            _lifecycle_mutation,
            _lifecycle_present,
        ),
        Scenario(
            "work_item_control_plane",
            "control_plane",
            1,
            _work_item_setup,
            _work_item_mutation,
            _work_item_present,
            warm_graphs=(GRAPH,),
        ),
    )


def _fault_spec(scenario: Scenario, phase: str) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "nonce": os.urandom(32).hex(),
        "request_id": scenario.request_id,
        "domain": scenario.domain,
        "phase": phase,
    }


def _run_fault_case(
    binary: ExactBinary,
    case_root: Path,
    authority: EphemeralAuthority,
    scenario: Scenario,
    phase: str,
) -> dict[str, object]:
    engine = ExactEngine(binary, case_root, authority)
    state: State = {}
    try:
        engine.start()
        engine.bootstrap()
        scenario.setup(engine, state)
        engine.stop()

        engine.start(fault=_fault_spec(scenario, phase))
        _warm_graphs(engine, scenario.warm_graphs)
        returned = False
        try:
            scenario.mutate(engine, state)
            returned = True
        except Exception:
            pass
        if returned:
            _fail("faulted_mutation_returned")
        engine.wait_for_abort(scenario.request_id)

        engine.start()
        if scenario.retry_after_restart:
            # Prepared multi-store coordinators recover through an exact replay:
            # before execution it starts, after children it resumes, and after a
            # lost acknowledgement it returns the terminal stored result.
            _warm_graphs(engine, scenario.warm_graphs)
            scenario.mutate(engine, state)
        present = scenario.observe(engine, state)
        expected_present = (
            scenario.retry_after_restart or phase == "after_commit_before_ack"
        )
        passed = present == expected_present
        if not passed:
            _fail("restart_atomicity_mismatch")
        expected = (
            "complete_after_exact_retry"
            if scenario.retry_after_restart
            else "no_effect"
            if phase in PRECOMMIT_PHASES
            else "complete_effect"
        )
        observed = "complete_effect" if present else "no_effect"
        return {
            "domain": scenario.domain,
            "expected": expected,
            "family": scenario.family,
            "observed": observed,
            "passed": True,
            "phase": phase,
            "recovery": (
                "exact_request_replay"
                if scenario.retry_after_restart
                else "authoritative_restart_read"
            ),
        }
    finally:
        engine.stop()
        shutil.rmtree(case_root, ignore_errors=True)


def _digest_json(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _tenant_timeseries_certification(
    binary: ExactBinary, root: Path, authority: EphemeralAuthority
) -> dict[str, object]:
    engine = ExactEngine(binary, root, authority)
    series = "tenant-qualified-series"
    try:
        engine.start(tenant=TENANT)
        engine.bootstrap(tenant=TENANT)
        _with_client(
            engine,
            COMMONS,
            lambda client: client.tenants.create(GRAPH),
            tenant=TENANT,
        )
        _with_client(
            engine,
            GRAPH,
            lambda client: client.timeseries.append(series, [(100, [11.0])]),
            tenant=TENANT,
        )
        engine.stop()

        engine.start(tenant=SECOND_TENANT)
        _with_client(
            engine,
            GRAPH,
            lambda client: _retry_partial(
                lambda: client.timeseries.append(series, [(100, [22.0])])
            ),
            tenant=SECOND_TENANT,
        )
        engine.stop()

        engine.start(tenant=TENANT)
        first = _with_client(
            engine,
            GRAPH,
            lambda client: _retry_partial(
                lambda: client.timeseries.range(series, 0, 1000)
            ),
            tenant=TENANT,
        )
        engine.stop()

        engine.start(tenant=SECOND_TENANT)
        second = _with_client(
            engine,
            GRAPH,
            lambda client: _retry_partial(
                lambda: client.timeseries.range(series, 0, 1000)
            ),
            tenant=SECOND_TENANT,
        )
        if first != [(100, [11.0])] or second != [(100, [22.0])]:
            _fail("tenant_timeseries_isolation_mismatch")
        return {
            "identical_local_series_ids": 2,
            "isolated_result_digests": [_digest_json(first), _digest_json(second)],
            "passed": True,
            "restart_cycles": 3,
        }
    finally:
        engine.stop()
        shutil.rmtree(root, ignore_errors=True)


def _spatial_rows(client: SyncEpistemicGraphClient) -> list[dict[str, Any]]:
    return client.query.unified(
        [
            {
                "SpatialScan": {
                    "layer": "CertificationCity",
                    "bbox": [0.0, 0.0, 10.0, 10.0],
                }
            }
        ]
    )


def _await_spatial_complete(
    client: SyncEpistemicGraphClient,
) -> tuple[bool, list[dict[str, Any]]]:
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECONDS
    saw_partial = False
    while True:
        try:
            return saw_partial, _spatial_rows(client)
        except RuntimeError as error:
            if "PARTIAL_MATERIALIZATION" not in str(error):
                raise
            saw_partial = True
            if time.monotonic() >= deadline:
                _fail("spatial_materialization_timeout")
            time.sleep(0.02)


def _spatial_certification(
    binary: ExactBinary, root: Path, authority: EphemeralAuthority
) -> dict[str, object]:
    engine = ExactEngine(binary, root, authority)
    inside_ids = [f"inside-{index:03d}" for index in range(96)]
    operations: list[dict[str, Any]] = []
    for index, node_id in enumerate(inside_ids):
        coordinate = float(index % 9) + 0.5
        operations.append(
            {
                "op": "add_node",
                "id": node_id,
                "properties": {
                    "type": "CertificationCity",
                    "geometry": f"POINT ({coordinate} {coordinate})",
                },
            }
        )
    for index in range(32):
        operations.append(
            {
                "op": "add_node",
                "id": f"outside-{index:03d}",
                "properties": {
                    "type": "CertificationCity",
                    "geometry": "POINT (20 20)",
                },
            }
        )
    try:
        engine.start(lazy_page_size=1)
        engine.bootstrap()
        _with_client(engine, COMMONS, lambda client: client.tenants.create(GRAPH))
        _with_client(
            engine,
            GRAPH,
            lambda client: client.lifecycle.batch_update(operations),
        )
        engine.stop()

        observed: list[tuple[bool, list[str]]] = []
        for _ in range(2):
            engine.start(lazy_page_size=1)

            def read(client: SyncEpistemicGraphClient) -> tuple[bool, list[str]]:
                partial, rows = _await_spatial_complete(client)
                ids = sorted(str(row["id"]) for row in rows)
                return partial, ids

            partial, ids = _with_client(engine, GRAPH, read)
            observed.append((partial, ids))
            engine.stop()
        expected = sorted(inside_ids)
        if any(ids != expected for _, ids in observed):
            _fail("spatial_restart_completeness_mismatch")
        if not any(partial for partial, _ in observed):
            _fail("spatial_partial_state_not_visible")
        digest = _digest_json(expected)
        return {
            "expected_hits": len(expected),
            "final_result_digest": digest,
            "lazy_open_cycles": 2,
            "partial_state_observed": True,
            "passed": True,
        }
    finally:
        engine.stop()
        shutil.rmtree(root, ignore_errors=True)


def _run(binary: ExactBinary, binary_digest: str) -> dict[str, object]:
    authority = _new_ephemeral_authority()
    matrix: list[dict[str, object]] = []
    scenarios = _scenarios()
    covered = {scenario.domain for scenario in scenarios}
    if covered != set(DOMAINS):
        _fail("mutation_domain_inventory_incomplete")
    with tempfile.TemporaryDirectory(prefix="eg-exact-certification-") as scratch:
        root = Path(scratch)
        case_number = 0
        for scenario in scenarios:
            for phase in PHASES:
                case_number += 1
                matrix.append(
                    _run_fault_case(
                        binary,
                        root / f"case-{case_number:03d}",
                        authority,
                        scenario,
                        phase,
                    )
                )
        tenant_ts = _tenant_timeseries_certification(
            binary, root / "tenant-ts", authority
        )
        spatial = _spatial_certification(binary, root / "spatial", authority)
    evidence = {
        "binary": {"sha256": binary_digest},
        "certification": "epistemic-graph-exact-fault-restart",
        "commit_phases": list(PHASES),
        "g05": {
            "spatial_restart_lazy_open": spatial,
            "tenant_qualified_time_series": tenant_ts,
        },
        "matrix": matrix,
        "mutation_domains": list(DOMAINS),
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "matrix_cases": len(matrix),
            "passed": len(matrix),
            "status": "pass",
        },
    }
    encoded = json.dumps(evidence, sort_keys=True, ensure_ascii=True)
    if any(
        secret in encoded for secret in (authority.auth_secret, authority.signer_key)
    ):
        _fail("evidence_contains_ephemeral_authority")
    return evidence


def _write_evidence(path: Path, evidence: dict[str, object]) -> None:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        _fail("invalid_evidence_path")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        parent = path.parent.resolve(strict=True)
    except (OSError, RuntimeError):
        _fail("invalid_evidence_parent")
    if parent != path.parent.absolute() or not parent.is_dir():
        _fail("evidence_parent_must_not_use_symlinks")
    body = (
        json.dumps(evidence, sort_keys=True, indent=2, ensure_ascii=True).encode(
            "utf-8"
        )
        + b"\n"
    )
    parent_descriptor: int | None = None
    temporary_descriptor: int | None = None
    temporary = f".{path.name}.{secrets.token_hex(16)}.tmp"
    try:
        parent_descriptor = os.open(
            parent,
            os.O_RDONLY
            | os.O_DIRECTORY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
        )
        temporary_descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=parent_descriptor,
        )
        view = memoryview(body)
        while view:
            written = os.write(temporary_descriptor, view)
            if written <= 0:
                _fail("evidence_write_failed")
            view = view[written:]
        os.fsync(temporary_descriptor)
        os.close(temporary_descriptor)
        temporary_descriptor = None
        try:
            os.link(
                temporary,
                path.name,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
        except FileExistsError:
            _fail("evidence_destination_must_be_new")
        os.unlink(temporary, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    except CertificationError:
        raise
    except OSError as error:
        if error.errno == errno.EEXIST:
            _fail("evidence_destination_must_be_new")
        _fail("evidence_write_failed")
    finally:
        if temporary_descriptor is not None:
            os.close(temporary_descriptor)
        if parent_descriptor is not None:
            try:
                os.unlink(temporary, dir_fd=parent_descriptor)
            except FileNotFoundError:
                pass
            except OSError:
                pass
            os.close(parent_descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certify one exact Epistemic Graph artifact under restart faults."
    )
    parser.add_argument(
        "--binary",
        required=True,
        help="Exact executable to certify; discovery and builds are forbidden.",
    )
    parser.add_argument(
        "--binary-sha256",
        required=True,
        help="Expected lowercase SHA-256 of --binary.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Destination for deterministic, path-free JSON evidence.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    binary: ExactBinary | None = None
    try:
        output = Path(args.output)
        if output.is_symlink() or output.exists():
            _fail("evidence_destination_must_be_new")
        binary, digest = _validate_binary(args.binary, args.binary_sha256)
        evidence = _run(binary, digest)
        _write_evidence(output, evidence)
    except CertificationError as error:
        print(f"exact certification failed: {error}", file=sys.stderr)
        return 1
    except Exception:
        # Runtime exceptions can contain host paths, principals, or raw payloads.
        # Preserve that privacy boundary even on failure; engine logs remain in
        # the throwaway directory and are removed by the enclosing context.
        print("exact certification failed: unexpected_runtime_failure", file=sys.stderr)
        return 1
    finally:
        if binary is not None:
            binary.close()
    print("exact certification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
