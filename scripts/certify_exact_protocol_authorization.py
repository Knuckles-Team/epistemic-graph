#!/usr/bin/env python3
"""Certify generated read isolation and every direct wire on one exact artifact.

This campaign combines two executable negative matrices.  First, a same-tenant peer
and a cross-tenant peer probe the lowest served data paths after an owner writes only
synthetic private rows.  Second, every direct protocol included by the ``full`` tier is
started on an ephemeral loopback port and driven through its native authentication
framing with an invalid credential.  No engine discovery or build is permitted.
"""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import struct
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from certify_exact_fault_restart import (
    AGENT_ID,
    AUDIENCE,
    CERTIFIER_ALLOWED_ROLES,
    POLICY_VERSION,
    SECOND_TENANT,
    TENANT,
    CertificationError,
    ExactBinary,
    ExactEngine,
    _fail,
    _new_ephemeral_authority,
    _validate_binary,
    _with_client,
    _write_evidence,
)

from epistemic_graph.client import SyncEpistemicGraphClient

SCHEMA_VERSION = 1
AUTH_GRAPH = "authorization-matrix"
PEER_ID = "service:exact-peer"
PEER_ROLE = "exact-matrix-reader"
# NE-247: `ExactEngine.start` builds the SCOPED signer registry from
# `CERTIFIER_ALLOWED_ROLES`, and NE-065's `authorize_grant` denies any role
# outside it. Asserting the coupling here means a renamed PEER_ROLE fails at
# import with an obvious message instead of at `register_identity` with the
# deliberately-indistinguishable "signer is not authorized" denial.
assert PEER_ROLE in CERTIFIER_ALLOWED_ROLES, (
    f"{PEER_ROLE!r} must be listed in certify_exact_fault_restart."
    "CERTIFIER_ALLOWED_ROLES or the engine sandbox will refuse to register it"
)
SOCKET_TIMEOUT_SECONDS = 3.0
LISTENER_TIMEOUT_SECONDS = 30.0

WIRE_PROTOCOLS = (
    "native_rpc",
    "postgresql",
    "mysql",
    "mssql",
    "sqlite",
    "bolt",
    "redis",
    "amqp",
    "mqtt",
    "stomp",
)
WIRE_FEATURES = {
    "native_rpc": "server",
    "postgresql": "pgwire",
    "mysql": "mysql-wire",
    "mssql": "mssql-wire",
    "sqlite": "sqlite-wire",
    "bolt": "bolt-wire",
    "redis": "redis-wire",
    "amqp": "amqp-wire",
    "mqtt": "mqtt-wire",
    "stomp": "stomp-wire",
}
LISTENER_ENV = {
    "postgresql": "EPISTEMIC_GRAPH_PGWIRE_ADDR",
    "mysql": "EPISTEMIC_GRAPH_MYSQL_ADDR",
    "mssql": "EPISTEMIC_GRAPH_MSSQL_ADDR",
    "sqlite": "EPISTEMIC_GRAPH_SQLITE_ADDR",
    "bolt": "EPISTEMIC_GRAPH_BOLT_ADDR",
    "redis": "EPISTEMIC_GRAPH_REDIS_ADDR",
    "amqp": "EPISTEMIC_GRAPH_AMQP_ADDR",
    "mqtt": "EPISTEMIC_GRAPH_MQTT_ADDR",
    "stomp": "EPISTEMIC_GRAPH_STOMP_ADDR",
}
DATA_PATHS = (
    "graph",
    "property",
    "union",
    "semantic",
    "topology",
    "rdf",
    "time_series",
    "vector",
    "blob",
    "job",
    "sql",
    "cache",
    "kv",
    "broker",
)


def _peer_context(tenant: str) -> dict[str, object]:
    return {
        "principal": PEER_ID,
        "tenant": tenant,
        "audience": AUDIENCE,
        "agent_id": PEER_ID,
        "roles": [PEER_ROLE],
        "scopes": ["*"],
        "policy_version": POLICY_VERSION,
        "delegation": [],
    }


def _peer_client(engine: ExactEngine, tenant: str = TENANT) -> SyncEpistemicGraphClient:
    return SyncEpistemicGraphClient.connect(
        socket_path=str(engine.socket_path),
        auth_secret=engine.authority.auth_secret,
        graph_name=AUTH_GRAPH,
        verified_context=_peer_context(tenant),
        timeout=15.0,
        heavy_timeout=30.0,
        connect_timeout=5.0,
    )


def _free_address() -> tuple[str, int]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()
    return str(host), int(port)


def _listener_configuration() -> tuple[dict[str, tuple[str, int]], dict[str, str]]:
    addresses: dict[str, tuple[str, int]] = {}
    env: dict[str, str] = {}
    for protocol, name in LISTENER_ENV.items():
        address = _free_address()
        addresses[protocol] = address
        env[name] = f"{address[0]}:{address[1]}"
    return addresses, env


def _wait_for_listeners(addresses: dict[str, tuple[str, int]]) -> None:
    pending = set(addresses)
    deadline = time.monotonic() + LISTENER_TIMEOUT_SECONDS
    while pending and time.monotonic() < deadline:
        for protocol in tuple(pending):
            try:
                with socket.create_connection(addresses[protocol], timeout=0.2):
                    pending.remove(protocol)
            except OSError:
                pass
        if pending:
            time.sleep(0.05)
    if pending:
        _fail("enabled_wire_listener_unavailable")


def _connection(address: tuple[str, int]) -> socket.socket:
    client = socket.create_connection(address, timeout=SOCKET_TIMEOUT_SECONDS)
    client.settimeout(SOCKET_TIMEOUT_SECONDS)
    return client


def _recv_exact(client: socket.socket, length: int) -> bytes:
    body = bytearray()
    while len(body) < length:
        part = client.recv(length - len(body))
        if not part:
            _fail("wire_response_truncated")
        body.extend(part)
    return bytes(body)


def _closed_without_payload(client: socket.socket) -> bool:
    try:
        return client.recv(1) == b""
    except ConnectionResetError:
        return True
    except TimeoutError:
        return False


def _probe_native(engine: ExactEngine) -> bool:
    client = SyncEpistemicGraphClient.connect(
        socket_path=str(engine.socket_path),
        auth_secret="synthetic-invalid-authority",
        graph_name=AUTH_GRAPH,
        verified_context=_peer_context(TENANT),
        timeout=5.0,
        connect_timeout=5.0,
    )
    try:
        try:
            client.nodes.has("private-row")
        except RuntimeError:
            return True
        return False
    finally:
        client.close()


def _pg_message(client: socket.socket) -> tuple[bytes, bytes]:
    kind = _recv_exact(client, 1)
    length = struct.unpack(">I", _recv_exact(client, 4))[0]
    if length < 4 or length > 16 * 1024 * 1024:
        _fail("postgresql_response_invalid")
    return kind, _recv_exact(client, length - 4)


def _probe_postgresql(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        fields = b"user\x00unauthorized\x00database\x00authorization-matrix\x00\x00"
        startup = struct.pack(">II", 8 + len(fields), 196608) + fields
        client.sendall(startup)
        auth_required = False
        for _ in range(8):
            kind, payload = _pg_message(client)
            if kind == b"R" and len(payload) >= 4:
                auth_required = struct.unpack(">I", payload[:4])[0] == 10
                break
            if kind == b"E":
                return True
        if not auth_required:
            return False
        invalid = b"INVALID\x00" + struct.pack(">i", -1)
        client.sendall(b"p" + struct.pack(">I", len(invalid) + 4) + invalid)
        try:
            kind, _ = _pg_message(client)
        except (ConnectionResetError, CertificationError):
            return True
        except TimeoutError:
            return False
        return kind == b"E"


def _mysql_packet(client: socket.socket) -> tuple[int, bytes]:
    header = _recv_exact(client, 4)
    length = int.from_bytes(header[:3], "little")
    if length > 16 * 1024 * 1024:
        _fail("mysql_response_invalid")
    return header[3], _recv_exact(client, length)


def _probe_mysql(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        _sequence, handshake = _mysql_packet(client)
        if not handshake or handshake[0] != 10:
            return False
        capabilities = 0x00000200 | 0x00008000 | 0x00080000 | 0x01000000
        payload = bytearray()
        payload.extend(struct.pack("<I", capabilities))
        payload.extend(struct.pack("<I", 1024 * 1024))
        payload.append(45)
        payload.extend(b"\x00" * 23)
        payload.extend(b"unauthorized\x00")
        payload.append(20)
        payload.extend(b"\x00" * 20)
        payload.extend(b"mysql_native_password\x00")
        header = len(payload).to_bytes(3, "little") + b"\x01"
        client.sendall(header + payload)
        _sequence, response = _mysql_packet(client)
        return bool(response) and response[0] == 0xFF


def _tds_packet(packet_type: int, payload: bytes) -> bytes:
    return (
        bytes((packet_type, 0x01))
        + struct.pack(">H", len(payload) + 8)
        + b"\x00\x00\x01\x00"
        + payload
    )


def _probe_mssql(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        client.sendall(_tds_packet(0x12, b"\xff"))
        header = _recv_exact(client, 8)
        length = struct.unpack(">H", header[2:4])[0]
        if length < 8:
            return False
        _recv_exact(client, length - 8)
        # An empty LOGIN7 payload decodes to no principal and can never authenticate.
        client.sendall(_tds_packet(0x10, b""))
        header = _recv_exact(client, 8)
        length = struct.unpack(">H", header[2:4])[0]
        response = _recv_exact(client, max(0, length - 8))
        return header[0] == 0x04 and bool(response)


def _probe_sqlite(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        request = {
            "id": 1,
            "graph": AUTH_GRAPH,
            "auth_token": "eg2.invalid",
            "agent_id": PEER_ID,
            "sql": "SELECT id FROM nodes",
        }
        client.sendall(json.dumps(request, separators=(",", ":")).encode() + b"\n")
        response = bytearray()
        while b"\n" not in response and len(response) < 64 * 1024:
            response.extend(client.recv(4096))
        decoded = json.loads(bytes(response).splitlines()[0])
        error = decoded.get("error") if isinstance(decoded, dict) else None
        return isinstance(error, dict) and error.get("code") == "28000"


def _probe_bolt(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        client.sendall(
            b"\x60\x60\xb0\x17" + b"\x00\x00\x04\x04" + b"\x00\x00\x00\x00" * 3
        )
        if _recv_exact(client, 4) != b"\x00\x00\x04\x04":
            return False
        # RUN with zero fields is syntactically a PackStream structure.  The auth
        # gate executes before field validation and must return FAILURE (tag 0x7f).
        client.sendall(b"\x00\x02\xb0\x10\x00\x00")
        length = struct.unpack(">H", _recv_exact(client, 2))[0]
        body = _recv_exact(client, length)
        terminator = _recv_exact(client, 2)
        return terminator == b"\x00\x00" and len(body) >= 2 and body[1] == 0x7F


def _probe_redis(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        client.sendall(b"*2\r\n$3\r\nGET\r\n$7\r\nprivate\r\n")
        response = client.recv(4096)
        return response.startswith(b"-NOAUTH")


def _amqp_frame(payload: bytes) -> bytes:
    return b"\x01\x00\x00" + struct.pack(">I", len(payload)) + payload + b"\xce"


def _recv_amqp_frame(client: socket.socket) -> bytes:
    header = _recv_exact(client, 7)
    length = struct.unpack(">I", header[3:7])[0]
    payload = _recv_exact(client, length)
    if _recv_exact(client, 1) != b"\xce":
        _fail("amqp_response_invalid")
    return payload


def _probe_amqp(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        client.sendall(b"AMQP\x00\x00\x09\x01")
        start = _recv_amqp_frame(client)
        if start[:4] != b"\x00\x0a\x00\x0a":
            return False
        response = b"\x00unauthorized\x00invalid"
        args = (
            struct.pack(">I", 0)
            + bytes((5,))
            + b"PLAIN"
            + struct.pack(">I", len(response))
            + response
            + bytes((5,))
            + b"en_US"
        )
        client.sendall(_amqp_frame(b"\x00\x0a\x00\x0b" + args))
        return _closed_without_payload(client)


def _mqtt_string(value: bytes) -> bytes:
    return struct.pack(">H", len(value)) + value


def _probe_mqtt(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        payload = (
            _mqtt_string(b"MQTT")
            + b"\x04\xc2\x00\x00"
            + _mqtt_string(b"exact-probe")
            + _mqtt_string(b"unauthorized")
            + _mqtt_string(b"invalid")
        )
        if len(payload) >= 128:
            _fail("mqtt_probe_payload_invalid")
        client.sendall(b"\x10" + bytes((len(payload),)) + payload)
        response = _recv_exact(client, 4)
        return response[:3] == b"\x20\x02\x00" and response[3] != 0


def _probe_stomp(address: tuple[str, int]) -> bool:
    with _connection(address) as client:
        client.sendall(
            b"CONNECT\naccept-version:1.2\nhost:local\n"
            b"login:unauthorized\npasscode:invalid\n\n\x00"
        )
        return _closed_without_payload(client)


PROBES: dict[str, Callable[[tuple[str, int]], bool]] = {
    "postgresql": _probe_postgresql,
    "mysql": _probe_mysql,
    "mssql": _probe_mssql,
    "sqlite": _probe_sqlite,
    "bolt": _probe_bolt,
    "redis": _probe_redis,
    "amqp": _probe_amqp,
    "mqtt": _probe_mqtt,
    "stomp": _probe_stomp,
}


def _seed_authorization_matrix(engine: ExactEngine) -> dict[str, str]:
    def configure(client: Any) -> None:
        client.rbac.add_role(PEER_ROLE)
        client.rbac.add_grant(PEER_ROLE, {"Graph": AUTH_GRAPH}, "Read", "Allow")
        client.consensus.register_identity(
            PEER_ID,
            "Agent",
            [],
            [PEER_ROLE],
            signer_id=AGENT_ID,
            signer_key=engine.authority.signer_key,
        )
        client.tenants.create(AUTH_GRAPH, "Global")

    _with_client(engine, "__commons__", configure)

    state: dict[str, str] = {}

    def seed(client: Any) -> None:
        private = {
            "type": "PrivateRow",
            "secret_marker": "synthetic",
            "_owner": AGENT_ID,
            "_visibility": "private",
        }
        client.nodes.add("private-row", private)
        client.nodes.add(
            "public-row",
            {"type": "PublicRow", "_visibility": "public"},
        )
        client.edges.add(
            "private-row",
            "public-row",
            {
                "relationship": "LINKS",
                "_owner": AGENT_ID,
                "_visibility": "private",
            },
        )
        client.graph.add_embedding("private-row", [1.0, 0.0])
        client.graph.add_embedding("public-row", [0.0, 1.0])
        client.timeseries.append("private-series", [(1, [7.0])])
        state["blob"] = client.blob.store(b"synthetic-private-blob")
        job = client.jobs.submit(
            AUTH_GRAPH,
            {
                "MineAssociate": {
                    "transactions": [["a", "b"], ["a", "c"], ["a", "b"]],
                    "min_support": 0.2,
                    "min_confidence": 0.1,
                    "algorithm": "fpgrowth",
                }
            },
        )
        state["job"] = str(job["job_id"])
        client._send(
            "KvPut",
            {"namespace": "private-kv", "key": "secret", "value": b"value"},
        )
        client.broker.declare_exchange("private-exchange", "direct")
        client.broker.declare_queue("private-queue")
        client.broker.bind_queue("private-exchange", "private-queue", "private")
        client.broker.publish("private-exchange", "private", b"value")

    _with_client(engine, AUTH_GRAPH, seed)
    return state


def _contains_id(rows: object, node_id: str) -> bool:
    return node_id in json.dumps(rows, sort_keys=True, ensure_ascii=True)


def _expect_runtime_denial(operation: Callable[[], object]) -> bool:
    try:
        operation()
    except RuntimeError:
        return True
    return False


def _data_path_matrix(
    engine: ExactEngine, state: dict[str, str]
) -> list[dict[str, object]]:
    peer = _peer_client(engine)
    try:
        sql = "SELECT id FROM nodes WHERE id = 'private-row'"
        checks: dict[str, bool] = {
            "graph": peer.nodes.has("private-row") is False,
            "property": peer.nodes.properties("private-row") is None,
            "union": not _contains_id(
                peer.nodes.list_by_label_union("PrivateRow", [AUTH_GRAPH]),
                "private-row",
            ),
            "semantic": not _contains_id(
                peer.graph.semantic_search([1.0, 0.0], 10), "private-row"
            ),
            "topology": "private-row" not in peer.nodes.neighbors("public-row"),
            "rdf": not _contains_id(
                peer.rdf.sparql("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"),
                "private-row",
            ),
            "time_series": peer.timeseries.range("private-series", 0, 10) == [],
            "vector": not _contains_id(
                peer.query.unified(
                    [
                        {"Scan": {"label": "PrivateRow"}},
                        {"Rank": {"query": [1.0, 0.0]}},
                        {"Limit": {"k": 10}},
                    ]
                ),
                "private-row",
            ),
            "blob": _expect_runtime_denial(lambda: peer.blob.fetch(state["blob"])),
            "job": _expect_runtime_denial(lambda: peer.jobs.status(state["job"])),
            "sql": peer.query.sql(sql) == [],
            # The owner warms its cache before this call. Actor-keyed caching must
            # still return the peer's empty RLS result on repeated reads.
            "cache": peer.query.sql(sql) == [] and peer.query.sql(sql) == [],
            "kv": peer._send("KvGet", {"namespace": "private-kv", "key": "secret"})
            is None,
            "broker": peer.broker.consume(
                "private-queue",
                group="peer-group",
                consumer="peer-consumer",
                now_ms=1,
            )
            is None,
        }
    finally:
        peer.close()
    if tuple(checks) != DATA_PATHS or not all(checks.values()):
        _fail("generated_data_path_authorization_matrix_failed")
    return [
        {"denial_observed": checks[path], "path": path, "tenant_relation": "same"}
        for path in DATA_PATHS
    ]


def _cross_tenant_probe(engine: ExactEngine) -> dict[str, object]:
    peer = _peer_client(engine, SECOND_TENANT)
    try:
        hidden = peer.nodes.has("private-row") is False
        series_hidden = peer.timeseries.range("private-series", 0, 10) == []
        kv_hidden = (
            peer._send("KvGet", {"namespace": "private-kv", "key": "secret"}) is None
        )
    finally:
        peer.close()
    if not (hidden and series_hidden and kv_hidden):
        _fail("cross_tenant_authorization_matrix_failed")
    return {
        "graph_row_hidden": True,
        "kv_namespace_hidden": True,
        "time_series_namespace_hidden": True,
    }


def _run(binary: ExactBinary, binary_digest: str) -> dict[str, object]:
    authority = _new_ephemeral_authority()
    addresses, listener_env = _listener_configuration()
    with tempfile.TemporaryDirectory(prefix="eg-exact-protocol-auth-") as scratch:
        root = Path(scratch)
        engine = ExactEngine(binary, root, authority)
        try:
            engine.start(extra_env=listener_env)
            engine.bootstrap()
            _wait_for_listeners(addresses)
            state = _seed_authorization_matrix(engine)
            # Warm the owner's SQL result before the peer cache-isolation checks.
            _with_client(
                engine,
                AUTH_GRAPH,
                lambda client: client.query.sql(
                    "SELECT id FROM nodes WHERE id = 'private-row'"
                ),
            )
            data_matrix = _data_path_matrix(engine, state)

            wire_matrix = [
                {
                    "auth_denial_observed": _probe_native(engine),
                    "feature": WIRE_FEATURES["native_rpc"],
                    "protocol": "native_rpc",
                }
            ]
            for protocol in WIRE_PROTOCOLS[1:]:
                passed = PROBES[protocol](addresses[protocol])
                wire_matrix.append(
                    {
                        "auth_denial_observed": passed,
                        "feature": WIRE_FEATURES[protocol],
                        "protocol": protocol,
                    }
                )
            if not all(row["auth_denial_observed"] for row in wire_matrix):
                _fail("enabled_wire_authentication_matrix_failed")

            engine.stop()
            engine.start(tenant=SECOND_TENANT)
            cross_tenant = _cross_tenant_probe(engine)
        finally:
            engine.stop()
            shutil.rmtree(root, ignore_errors=True)

    if tuple(row["protocol"] for row in wire_matrix) != WIRE_PROTOCOLS:
        _fail("enabled_wire_protocol_inventory_mismatch")
    evidence = {
        "binary": {"sha256": binary_digest},
        "certification": "epistemic-graph-exact-protocol-authorization",
        "cross_tenant": cross_tenant,
        "data_path_matrix": data_matrix,
        "data_paths": list(DATA_PATHS),
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "data_path_cases": len(data_matrix),
            "protocol_cases": len(wire_matrix),
            "status": "pass",
        },
        "wire_matrix": wire_matrix,
        "wire_protocols": list(WIRE_PROTOCOLS),
    }
    encoded = json.dumps(evidence, sort_keys=True, ensure_ascii=True)
    if authority.auth_secret in encoded or authority.signer_key in encoded:
        _fail("evidence_contains_ephemeral_authority")
    return evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certify every full-tier direct wire and generated read path."
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
        print(f"exact protocol authorization failed: {error}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "exact protocol authorization failed: unexpected_runtime_failure",
            file=sys.stderr,
        )
        return 1
    finally:
        if binary is not None:
            binary.close()
    print("exact protocol authorization passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
