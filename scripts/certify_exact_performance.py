#!/usr/bin/env python3
"""G-37 exact-binary performance certification for epistemic-graph.

The harness is deliberately a release gate, not a development benchmark.  It
requires an explicit executable and SHA-256 digest, stages those verified bytes
into a private Linux-native work directory, starts one authenticated durable
engine, and evaluates a committed synthetic workload against committed
thresholds.  JSON and Markdown evidence contain digests and abstract hardware
class data only; paths, endpoints, principals, secrets, and source bodies are
never serialized.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import math
import os
import platform
import random
import re
import shutil
import signal
import stat
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import msgpack

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = ROOT / "protocols" / "performance" / "v1" / "dataset.json"
DEFAULT_THRESHOLDS = ROOT / "protocols" / "performance" / "v1" / "thresholds.json"
DEFAULT_SCENARIOS = ROOT / "protocols" / "performance" / "v1" / "scenarios.json"
DEFAULT_SCENARIO_SCHEMA = (
    ROOT / "protocols" / "performance" / "v1" / "scenarios.schema.json"
)
DEFAULT_COMPLEXITY_LEDGER = ROOT / "docs" / "architecture" / "hot-path-complexity.md"

SCHEMA_VERSION = "1"
GATE_ID = "G-37"
MODALITIES = ("document", "image", "audio", "video")
SEGMENT_KINDS = {
    "document": "paragraph",
    "image": "region",
    "audio": "audio_range",
    "video": "video_shot",
}
MAX_RSS_SAMPLES = 100_000
MAX_AUTHORITY_BYTES = 64 * 1024
MAX_CONTRACT_BYTES = 1024 * 1024
MAX_LEDGER_BYTES = 2 * 1024 * 1024
MAX_SCENARIO_OUTPUT_BYTES = 8 * 1024 * 1024
EXPECTED_SCENARIO_COUNT = 30
EXPECTED_LEDGER_ROW_COUNT = 54
PROFILE = {
    "transport": "private_uds",
    "durability": "redb_authoritative",
    "redb_shards": 1,
    "max_inflight": 256,
    "max_inflight_per_graph": 64,
    "reserved_read_slots": 32,
    "rpc_timeout_seconds": 30,
    "heavy_rpc_timeout_seconds": 120,
    "request_authority": "eg2_verified",
    "optional_listeners": "disabled",
}
REQUIRED_OPS = frozenset(
    {
        "ServedModality",
    }
)

METRIC_CONTRACT: dict[str, tuple[str, str]] = {
    "cold_start_ready_ms": ("milliseconds", "maximum"),
    "routing_latency_p50_ms": ("milliseconds", "maximum"),
    "routing_latency_p99_ms": ("milliseconds", "maximum"),
    "routing_throughput_ops_per_second": ("operations_per_second", "minimum"),
    "ingest_batch_latency_p50_ms": ("milliseconds", "maximum"),
    "ingest_batch_latency_p99_ms": ("milliseconds", "maximum"),
    "ingest_throughput_ops_per_second": ("operations_per_second", "minimum"),
    "point_query_latency_p50_ms": ("milliseconds", "maximum"),
    "point_query_latency_p99_ms": ("milliseconds", "maximum"),
    "point_query_throughput_rows_per_second": ("rows_per_second", "minimum"),
    "analytics_query_latency_p99_ms": ("milliseconds", "maximum"),
    "job_submit_latency_p99_ms": ("milliseconds", "maximum"),
    "job_completion_latency_p99_ms": ("milliseconds", "maximum"),
    "job_throughput_jobs_per_second": ("jobs_per_second", "minimum"),
    "modality_capability_latency_p99_ms": ("milliseconds", "maximum"),
    "modality_ingest_latency_p99_ms": ("milliseconds", "maximum"),
    "modality_query_latency_p99_ms": ("milliseconds", "maximum"),
    "modality_throughput_ops_per_second": ("operations_per_second", "minimum"),
    "memory_ready_rss_mib": ("mebibytes", "maximum"),
    "memory_peak_rss_mib": ("mebibytes", "maximum"),
    "memory_growth_rss_mib": ("mebibytes", "maximum"),
}

COMPLEXITY_CONTRACT: dict[str, tuple[str, str]] = {
    "routing_state_growth_ratio": ("ratio", "maximum"),
    "point_query_state_growth_ratio": ("ratio", "maximum"),
    "fixed_batch_ingest_state_growth_ratio": ("ratio", "maximum"),
    "modality_index_growth_ratio": ("ratio", "maximum"),
}

COVERAGE_CONTRACT = frozenset(
    {
        "cold_start",
        "routing",
        "ingest",
        "query",
        "job",
        "modality",
        "memory",
        "hot_path_scenarios",
    }
)

_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_OPAQUE = re.compile(r"^eg:[a-z0-9_-]{1,32}:[0-9a-f]{64}$")
_SAFE_CODE = re.compile(r"^[a-z0-9_.:-]+$")
_SAFE_VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z.+-]{0,63}$")
_ROW_ID = re.compile(r"^G37-HP-[0-9]{3}$")
_SCENARIO_ID = re.compile(r"^g37-s([0-9]{2})-[a-z0-9-]+$")
_DRIVER = re.compile(r"^[a-z][a-z0-9_]{2,63}$")
_CHECK_NAME = re.compile(r"^[a-z][a-z0-9_]{2,63}$")
_IMPLEMENTATION_REF = re.compile(
    r"^(?:src|crates|benches|tests|scripts)/[A-Za-z0-9_./-]+(?:::[A-Za-z0-9_:]+)?$"
)
_PATH_OR_ENDPOINT = re.compile(
    r"(?i)(?:[a-z]:[\\/]|/(?:home|root|mnt|users|tmp|opt|var|run)/|"
    r"https?://|\\\\|\b[^\s@]+@[^\s@]+\.[^\s@]+\b)"
)

PNG_FIXTURE = bytes(
    (
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
        0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
        0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
        0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41,
        0x54, 0x78, 0xDA, 0x63, 0xFC, 0xCF, 0xC0, 0x50,
        0x0F, 0x00, 0x05, 0xFE, 0x02, 0xFE, 0x42, 0x75,
        0x27, 0x59, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45,
        0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    )
)


def _boxed(kind: bytes, body: bytes) -> bytes:
    return struct.pack(">I", len(body) + 8) + kind + body


def _wav_fixture() -> bytes:
    samples = (0, 8_000, 16_000, 8_000, 0, -8_000, -16_000, -8_000)
    pcm = b"".join(struct.pack("<h", sample) for sample in samples)
    return b"".join(
        (
            b"RIFF",
            struct.pack("<I", 36 + len(pcm)),
            b"WAVEfmt ",
            struct.pack("<IHHIIHH", 16, 1, 1, 8, 16, 2, 16),
            b"data",
            struct.pack("<I", len(pcm)),
            pcm,
        )
    )


def _mp4_fixture() -> bytes:
    ftyp = _boxed(b"ftyp", b"isom" + struct.pack(">I", 0) + b"isom")
    mdat = _boxed(b"mdat", bytes((0, 1, 2, 3, 4, 5)))
    sample_offset = len(ftyp) + 8
    mvhd = bytearray(100)
    mvhd[12:16] = struct.pack(">I", 1_000)
    mvhd[16:20] = struct.pack(">I", 1_000)
    tkhd = bytearray(84)
    tkhd[12:16] = struct.pack(">I", 1)
    mdhd = bytearray(24)
    mdhd[12:16] = struct.pack(">I", 1_000)
    mdhd[16:20] = struct.pack(">I", 1_000)
    hdlr = bytearray(24)
    hdlr[8:12] = b"vide"
    sample_body = bytearray(78)
    sample_body[6:8] = struct.pack(">H", 1)
    sample_body[24:26] = struct.pack(">H", 2)
    sample_body[26:28] = struct.pack(">H", 1)
    sample_body[40:42] = struct.pack(">H", 1)
    sample_body[74:76] = struct.pack(">H", 24)
    sample_body[76:78] = struct.pack(">H", 0xFFFF)
    sample_entry = _boxed(b"raw ", bytes(sample_body))
    stbl = b"".join(
        (
            _boxed(b"stsd", struct.pack(">II", 0, 1) + sample_entry),
            _boxed(b"stts", struct.pack(">IIII", 0, 1, 1, 1_000)),
            _boxed(b"stsz", struct.pack(">III", 0, 6, 1)),
            _boxed(b"stco", struct.pack(">III", 0, 1, sample_offset)),
            _boxed(b"stsc", struct.pack(">IIIII", 0, 1, 1, 1, 1)),
        )
    )
    minf = _boxed(b"minf", _boxed(b"stbl", stbl))
    mdia = b"".join((_boxed(b"mdhd", bytes(mdhd)), _boxed(b"hdlr", bytes(hdlr)), minf))
    trak = _boxed(b"trak", _boxed(b"tkhd", bytes(tkhd)) + _boxed(b"mdia", mdia))
    return ftyp + mdat + _boxed(b"moov", _boxed(b"mvhd", bytes(mvhd)) + trak)


class CertificationError(RuntimeError):
    """A fail-closed error represented by a stable, non-sensitive code."""

    def __init__(self, code: str) -> None:
        if not _SAFE_CODE.fullmatch(code):
            code = "invalid_internal_error_code"
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class AuthorityConfig:
    auth_secret: str
    signer_id: str
    signer_key: str
    context: dict[str, Any]

    @property
    def bootstrap_context(self) -> dict[str, Any]:
        return {
            **self.context,
            "roles": [],
            "scopes": ["security:bootstrap"],
            "delegation": [],
        }

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(_canonical_json(self.context)).hexdigest()


@dataclass
class Workload:
    node_operations: list[dict[str, Any]]
    edge_operations: list[dict[str, Any]]
    node_ids: list[str]
    route_partition_ref: str
    job_transactions: list[list[str]]
    modality_sources: dict[str, list[bytes]]
    digest: str


@dataclass(frozen=True)
class ScenarioContracts:
    manifest: dict[str, Any]
    manifest_sha256: str
    schema_sha256: str
    ledger_rows: dict[str, str]
    ledger_sha256: str


@dataclass(frozen=True)
class ScenarioExecution:
    result: dict[str, Any]
    elapsed_ms: float
    peak_rss_bytes: int
    rss_samples: int


@dataclass
class EngineHandle:
    process: subprocess.Popen[bytes]
    socket_path: Path
    log_file: Any

    def stop(self) -> None:
        try:
            if self.process.poll() is None:
                try:
                    os.killpg(self.process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    self.process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(self.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    self.process.wait(timeout=5)
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        finally:
            self.log_file.close()


class RssSampler:
    """Bounded process RSS sampler; PIDs and proc paths never enter evidence."""

    def __init__(self, pid: int) -> None:
        self._pid = pid
        self._samples_kib: list[int] = []
        self._saturated = False
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    @property
    def samples_kib(self) -> list[int]:
        return list(self._samples_kib)

    @property
    def sample_count(self) -> int:
        return len(self._samples_kib)

    def samples_since(self, index: int) -> list[int]:
        return list(self._samples_kib[index:])

    @property
    def saturated(self) -> bool:
        return self._saturated

    @property
    def alive(self) -> bool:
        return self._thread.is_alive()

    def current_kib(self) -> int:
        return _read_rss_kib(self._pid)

    def _run(self) -> None:
        while not self._stop.is_set():
            value = _read_rss_kib(self._pid)
            if value > 0:
                if len(self._samples_kib) >= MAX_RSS_SAMPLES:
                    self._saturated = True
                else:
                    self._samples_kib.append(value)
            self._stop.wait(0.01)


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _exact_keys(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise CertificationError(f"invalid_{label}_schema")
    return value


def _read_bounded_file(
    path: Path, label: str, maximum_bytes: int, *, private: bool = False
) -> bytes:
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= maximum_bytes:
            raise CertificationError(f"invalid_{label}_file")
        if private and (
            metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise CertificationError(f"{label}_file_permissions")
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - total)):
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum_bytes:
                raise CertificationError(f"invalid_{label}_file")
        return b"".join(chunks)
    except CertificationError:
        raise
    except OSError as error:
        raise CertificationError(f"invalid_{label}_file") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_public_json(path: Path, label: str) -> tuple[dict[str, Any], str]:
    try:
        raw = _read_bounded_file(path, label, MAX_CONTRACT_BYTES)
        value = json.loads(raw)
    except CertificationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CertificationError(f"invalid_{label}_file") from error
    if not isinstance(value, dict):
        raise CertificationError(f"invalid_{label}_schema")
    return value, hashlib.sha256(raw).hexdigest()


def _validate_opaque(value: Any, namespace: str, label: str) -> str:
    if not isinstance(value, str) or not value.startswith(f"eg:{namespace}:"):
        raise CertificationError(f"invalid_{label}")
    if not _OPAQUE.fullmatch(value):
        raise CertificationError(f"invalid_{label}")
    return value


def _load_authority(path: Path) -> AuthorityConfig:
    try:
        raw = _read_bounded_file(
            path, "authority", MAX_AUTHORITY_BYTES, private=True
        )
        value = json.loads(raw)
    except CertificationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CertificationError("invalid_authority_file") from error

    data = _exact_keys(
        value,
        {"schema_version", "auth_secret", "signer_id", "signer_key", "context"},
        "authority",
    )
    if data["schema_version"] != SCHEMA_VERSION:
        raise CertificationError("invalid_authority_version")
    auth_secret = data["auth_secret"]
    signer_key = data["signer_key"]
    if not isinstance(auth_secret, str) or not 32 <= len(auth_secret) <= 4096:
        raise CertificationError("invalid_auth_secret")
    if not isinstance(signer_key, str) or not 32 <= len(signer_key) <= 4096:
        raise CertificationError("invalid_signer_key")
    if any(ord(character) < 32 or ord(character) == 127 for character in auth_secret + signer_key):
        raise CertificationError("invalid_secret_material")

    signer_id = _validate_opaque(data["signer_id"], "certifier", "signer_id")
    context = _exact_keys(
        data["context"],
        {
            "principal",
            "tenant",
            "audience",
            "agent_id",
            "roles",
            "scopes",
            "policy_version",
            "delegation",
        },
        "authority_context",
    )
    principal = _validate_opaque(context["principal"], "certifier", "principal")
    agent_id = _validate_opaque(context["agent_id"], "certifier", "agent_id")
    _validate_opaque(context["tenant"], "tenant", "tenant")
    _validate_opaque(context["audience"], "audience", "audience")
    _validate_opaque(context["policy_version"], "policy", "policy_version")
    if principal != agent_id or signer_id != agent_id:
        raise CertificationError("authority_identity_mismatch")
    if context["roles"] != ["certifier"]:
        raise CertificationError("invalid_authority_roles")
    if context["scopes"] != ["kg:admin"]:
        raise CertificationError("invalid_authority_scopes")
    if context["delegation"] != []:
        raise CertificationError("invalid_authority_delegation")
    return AuthorityConfig(auth_secret, signer_id, signer_key, dict(context))


def _load_dataset(path: Path) -> tuple[dict[str, Any], str]:
    value, digest = _read_public_json(path, "dataset_manifest")
    data = _exact_keys(
        value,
        {
            "schema_version",
            "seed",
            "graph_ref",
            "node_count",
            "edge_count",
            "batch_size",
            "scale_points",
            "probe_repetitions",
            "query_batch_size",
            "analytics_query_repetitions",
            "job_count",
            "job_transaction_count",
            "job_items_per_transaction",
            "job_item_cardinality",
            "job_poll_timeout_seconds",
            "modality_capability_repetitions",
            "modality_records_per_kind",
            "modality_scale_points",
            "modality_query_repetitions",
            "expected_workload_sha256",
        },
        "dataset_manifest",
    )
    if data["schema_version"] != SCHEMA_VERSION:
        raise CertificationError("invalid_dataset_version")
    integer_bounds = {
        "seed": (0, (1 << 63) - 1),
        "node_count": (128, 100_000),
        "edge_count": (127, 500_000),
        "batch_size": (16, 4_096),
        "probe_repetitions": (5, 10_000),
        "query_batch_size": (1, 1_000),
        "analytics_query_repetitions": (1, 100),
        "job_count": (1, 100),
        "job_transaction_count": (2, 10_000),
        "job_items_per_transaction": (2, 30),
        "job_item_cardinality": (2, 31),
        "job_poll_timeout_seconds": (1, 300),
        "modality_capability_repetitions": (1, 100),
        "modality_records_per_kind": (1, 1_000),
        "modality_query_repetitions": (1, 1_000),
    }
    for field, (minimum, maximum) in integer_bounds.items():
        item = data[field]
        if isinstance(item, bool) or not isinstance(item, int) or not minimum <= item <= maximum:
            raise CertificationError(f"invalid_dataset_{field}")
    if data["job_items_per_transaction"] > data["job_item_cardinality"]:
        raise CertificationError("invalid_dataset_job_cardinality")
    graph_ref = data["graph_ref"]
    if graph_ref != "g37:synthetic" or _PATH_OR_ENDPOINT.search(graph_ref):
        raise CertificationError("invalid_dataset_graph_ref")
    for field, final in (
        ("scale_points", data["node_count"]),
        ("modality_scale_points", data["modality_records_per_kind"]),
    ):
        points = data[field]
        if (
            not isinstance(points, list)
            or len(points) < 2
            or any(isinstance(item, bool) or not isinstance(item, int) for item in points)
            or points != sorted(set(points))
            or points[-1] != final
            or points[0] <= 0
        ):
            raise CertificationError(f"invalid_dataset_{field}")
    if any(point % data["batch_size"] != 0 for point in data["scale_points"]):
        raise CertificationError("invalid_dataset_scale_alignment")
    expected = data["expected_workload_sha256"]
    if not isinstance(expected, str) or not _HEX_64.fullmatch(expected):
        raise CertificationError("invalid_dataset_workload_digest")
    return data, digest


def _load_thresholds(path: Path) -> tuple[dict[str, Any], str]:
    value, digest = _read_public_json(path, "threshold_manifest")
    data = _exact_keys(
        value,
        {"schema_version", "profile", "metrics", "complexity"},
        "threshold_manifest",
    )
    if data["schema_version"] != SCHEMA_VERSION or data["profile"] != PROFILE:
        raise CertificationError("invalid_threshold_profile")
    for section, contract in (
        ("metrics", METRIC_CONTRACT),
        ("complexity", COMPLEXITY_CONTRACT),
    ):
        values = data[section]
        if not isinstance(values, dict) or set(values) != set(contract):
            raise CertificationError(f"invalid_threshold_{section}_coverage")
        for name, (unit, direction) in contract.items():
            threshold = _exact_keys(values[name], {"unit", direction}, "threshold")
            limit = threshold[direction]
            if (
                threshold["unit"] != unit
                or isinstance(limit, bool)
                or not isinstance(limit, int | float)
                or not math.isfinite(float(limit))
                or float(limit) <= 0
            ):
                raise CertificationError(f"invalid_threshold_{name}")
    return data, digest


def _markdown_section(text: str, heading: str) -> list[str]:
    marker = f"## {heading}"
    lines = text.splitlines()
    try:
        start = lines.index(marker) + 1
    except ValueError as error:
        raise CertificationError("invalid_complexity_ledger_heading") from error
    end = next(
        (index for index in range(start, len(lines)) if lines[index].startswith("## ")),
        len(lines),
    )
    return lines[start:end]


def _load_complexity_ledger(
    path: Path, registry_heading: str, implemented_heading: str, expected_rows: int
) -> tuple[dict[str, str], str]:
    try:
        raw = _read_bounded_file(path, "complexity_ledger", MAX_LEDGER_BYTES)
        text = raw.decode("utf-8")
    except CertificationError:
        raise
    except (OSError, UnicodeError) as error:
        raise CertificationError("invalid_complexity_ledger_file") from error

    registry: list[tuple[str, str]] = []
    for line in _markdown_section(text, registry_heading):
        match = re.fullmatch(r"\| `(G37-HP-[0-9]{3})` \| (.+) \|", line)
        if match:
            registry.append((match.group(1), match.group(2)))
    implemented = [
        line.split("|", 2)[1].strip()
        for line in _markdown_section(text, implemented_heading)
        if line.startswith("| ") and not line.startswith("| Path ")
    ]
    identifiers = [row_id for row_id, _ in registry]
    names = [name for _, name in registry]
    if (
        expected_rows != EXPECTED_LEDGER_ROW_COUNT
        or len(registry) != expected_rows
        or len(set(identifiers)) != expected_rows
        or len(set(names)) != expected_rows
        or any(not _ROW_ID.fullmatch(row_id) for row_id in identifiers)
        or identifiers
        != [f"G37-HP-{index:03d}" for index in range(1, expected_rows + 1)]
        or implemented != names
    ):
        raise CertificationError("invalid_complexity_ledger_coverage")
    return dict(registry), hashlib.sha256(raw).hexdigest()


def _positive_number(value: Any, *, maximum: float | None = None) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, int | float)
        and math.isfinite(float(value))
        and float(value) > 0
        and (maximum is None or float(value) <= maximum)
    )


def _load_scenario_contracts(
    manifest_path: Path,
    schema_path: Path = DEFAULT_SCENARIO_SCHEMA,
    ledger_path: Path = DEFAULT_COMPLEXITY_LEDGER,
) -> ScenarioContracts:
    manifest, manifest_sha256 = _read_public_json(
        manifest_path, "scenario_manifest"
    )
    schema, schema_sha256 = _read_public_json(schema_path, "scenario_schema")
    if (
        schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema"
        or schema.get("$id") != "urn:epistemic-graph:g37:performance-scenarios:v1"
        or schema.get("type") != "object"
        or schema.get("additionalProperties") is not False
    ):
        raise CertificationError("invalid_scenario_schema_contract")

    data = _exact_keys(
        manifest,
        {"schema_version", "manifest_id", "ledger", "probe_protocol", "scenarios"},
        "scenario_manifest",
    )
    if (
        data["schema_version"] != SCHEMA_VERSION
        or data["manifest_id"] != "g37-performance-scenarios-v1"
        or data["probe_protocol"] != "g37.performance-probe.v1"
    ):
        raise CertificationError("invalid_scenario_manifest_version")
    ledger = _exact_keys(
        data["ledger"],
        {"document", "registry_heading", "implemented_heading", "expected_rows"},
        "scenario_ledger",
    )
    if (
        ledger["document"] != "docs/architecture/hot-path-complexity.md"
        or ledger["registry_heading"] != "Implemented row identities"
        or ledger["implemented_heading"] != "Implemented bounds"
        or ledger["expected_rows"] != EXPECTED_LEDGER_ROW_COUNT
    ):
        raise CertificationError("invalid_scenario_ledger_contract")
    ledger_rows, ledger_sha256 = _load_complexity_ledger(
        ledger_path,
        ledger["registry_heading"],
        ledger["implemented_heading"],
        ledger["expected_rows"],
    )

    scenarios = data["scenarios"]
    if not isinstance(scenarios, list) or len(scenarios) != EXPECTED_SCENARIO_COUNT:
        raise CertificationError("invalid_scenario_count")
    seen_scenarios: set[str] = set()
    seen_drivers: set[str] = set()
    seen_rows: set[str] = set()
    for ordinal, untyped_scenario in enumerate(scenarios, start=1):
        scenario = _exact_keys(
            untyped_scenario,
            {
                "scenario_id",
                "driver",
                "scales",
                "repetitions",
                "resource_bounds",
                "implementation_refs",
                "rows",
            },
            "scenario",
        )
        scenario_id = scenario["scenario_id"]
        match = _SCENARIO_ID.fullmatch(scenario_id) if isinstance(scenario_id, str) else None
        if match is None or int(match.group(1)) != ordinal or scenario_id in seen_scenarios:
            raise CertificationError("invalid_scenario_identity")
        seen_scenarios.add(scenario_id)
        driver = scenario["driver"]
        if (
            not isinstance(driver, str)
            or not _DRIVER.fullmatch(driver)
            or driver in seen_drivers
        ):
            raise CertificationError("invalid_scenario_driver")
        seen_drivers.add(driver)
        scales = scenario["scales"]
        if (
            not isinstance(scales, list)
            or not 3 <= len(scales) <= 8
            or scales != sorted(set(scales))
            or any(
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= 1_000_000
                for value in scales
            )
        ):
            raise CertificationError("invalid_scenario_scales")
        repetitions = scenario["repetitions"]
        if (
            isinstance(repetitions, bool)
            or not isinstance(repetitions, int)
            or not 3 <= repetitions <= 1_000
        ):
            raise CertificationError("invalid_scenario_repetitions")
        bounds = _exact_keys(
            scenario["resource_bounds"],
            {"maximum_elapsed_ms", "maximum_peak_rss_mib", "maximum_output_bytes"},
            "scenario_resource_bounds",
        )
        for name, minimum, maximum in (
            ("maximum_elapsed_ms", 100, 600_000),
            ("maximum_peak_rss_mib", 16, 16_384),
            ("maximum_output_bytes", 1_024, MAX_SCENARIO_OUTPUT_BYTES),
        ):
            value = bounds[name]
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise CertificationError("invalid_scenario_resource_bounds")
        references = scenario["implementation_refs"]
        if (
            not isinstance(references, list)
            or not 1 <= len(references) <= 12
            or len(set(references)) != len(references)
        ):
            raise CertificationError("invalid_scenario_implementation_refs")
        for reference in references:
            if not isinstance(reference, str) or not _IMPLEMENTATION_REF.fullmatch(reference):
                raise CertificationError("invalid_scenario_implementation_ref")
            relative_path = reference.split("::", 1)[0]
            source = (ROOT / relative_path).resolve()
            try:
                source.relative_to(ROOT.resolve())
            except ValueError as error:
                raise CertificationError("invalid_scenario_implementation_ref") from error
            if not source.exists():
                raise CertificationError("missing_scenario_implementation_ref")
        rows = scenario["rows"]
        if not isinstance(rows, list) or not 1 <= len(rows) <= 8:
            raise CertificationError("invalid_scenario_rows")
        for untyped_row in rows:
            row = _exact_keys(
                untyped_row,
                {"row_id", "equivalence_checks", "thresholds"},
                "scenario_row",
            )
            row_id = row["row_id"]
            if (
                not isinstance(row_id, str)
                or not _ROW_ID.fullmatch(row_id)
                or row_id not in ledger_rows
                or row_id in seen_rows
            ):
                raise CertificationError("invalid_scenario_row_coverage")
            seen_rows.add(row_id)
            checks = row["equivalence_checks"]
            if (
                not isinstance(checks, list)
                or not 1 <= len(checks) <= 12
                or len(set(checks)) != len(checks)
                or any(
                    not isinstance(check, str) or not _CHECK_NAME.fullmatch(check)
                    for check in checks
                )
            ):
                raise CertificationError("invalid_scenario_equivalence_checks")
            threshold = _exact_keys(
                row["thresholds"],
                {
                    "maximum_work_units",
                    "maximum_work_growth_ratio",
                    "maximum_peak_memory_bytes",
                    "maximum_memory_growth_ratio",
                    "maximum_latency_p99_ms",
                    "maximum_latency_growth_ratio",
                },
                "scenario_thresholds",
            )
            for name, maximum in (
                ("maximum_work_units", 1_000_000_000_000),
                ("maximum_work_growth_ratio", 1_000_000),
                ("maximum_peak_memory_bytes", 17_179_869_184),
                ("maximum_memory_growth_ratio", 1_000_000),
                ("maximum_latency_p99_ms", 600_000),
                ("maximum_latency_growth_ratio", 1_000_000),
            ):
                if not _positive_number(threshold[name], maximum=maximum):
                    raise CertificationError("invalid_scenario_threshold")
    if seen_rows != set(ledger_rows):
        raise CertificationError("invalid_scenario_row_coverage")
    return ScenarioContracts(
        data,
        manifest_sha256,
        schema_sha256,
        ledger_rows,
        ledger_sha256,
    )


def _workload_from_manifest(manifest: dict[str, Any]) -> Workload:
    rng = random.Random(manifest["seed"])  # nosec B311 -- reproducible synthetic data
    node_ids = [f"g37-node-{index:08x}" for index in range(manifest["node_count"])]
    node_operations = [
        {
            "op": "add_node",
            "id": node_id,
            "properties": {
                "kind": "synthetic",
                "ordinal": index,
                "partition": index % 32,
            },
        }
        for index, node_id in enumerate(node_ids)
    ]
    pairs = {(index, index + 1) for index in range(len(node_ids) - 1)}
    while len(pairs) < manifest["edge_count"]:
        source = rng.randrange(len(node_ids))
        target = rng.randrange(len(node_ids))
        if source != target:
            pairs.add((source, target))
    edge_operations = [
        {
            "op": "add_edge",
            "source": node_ids[source],
            "target": node_ids[target],
            "properties": {"kind": "synthetic_link", "ordinal": ordinal},
        }
        for ordinal, (source, target) in enumerate(sorted(pairs))
    ]
    item_refs = [
        f"g37-item-{index:02x}" for index in range(manifest["job_item_cardinality"])
    ]
    job_transactions = [
        sorted(rng.sample(item_refs, manifest["job_items_per_transaction"]))
        for _ in range(manifest["job_transaction_count"])
    ]
    count = manifest["modality_records_per_kind"]
    modality_sources = {
        "document": [
            f"alpha beta gamma delta {index:08x}".encode("ascii")
            for index in range(count)
        ],
        "image": [PNG_FIXTURE for _ in range(count)],
        "audio": [_wav_fixture() for _ in range(count)],
        "video": [_mp4_fixture() for _ in range(count)],
    }
    route_partition_ref = _opaque("partition", manifest["seed"], 0, "routing")
    definition = {
        "node_operations": node_operations,
        "edge_operations": edge_operations,
        "route_partition_ref": route_partition_ref,
        "job_transactions": job_transactions,
        "modality_source_sha256": {
            modality: [hashlib.sha256(source).hexdigest() for source in sources]
            for modality, sources in modality_sources.items()
        },
    }
    digest = hashlib.sha256(_canonical_json(definition)).hexdigest()
    if digest != manifest["expected_workload_sha256"]:
        raise CertificationError("workload_digest_mismatch")
    return Workload(
        node_operations,
        edge_operations,
        node_ids,
        route_partition_ref,
        job_transactions,
        modality_sources,
        digest,
    )


def _validate_work_root(path: Path) -> Path:
    if not sys.platform.startswith("linux") or not path.is_absolute():
        raise CertificationError("work_root_requires_linux_absolute_path")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise CertificationError("invalid_work_root") from error
    if not resolved.is_dir() or resolved.parts[:2] == ("/", "mnt"):
        raise CertificationError("work_root_not_linux_native")
    if not os.access(resolved, os.W_OK | os.X_OK):
        raise CertificationError("work_root_not_writable")
    return resolved


def _validate_output_path(path: Path, suffix: str) -> Path:
    if not path.is_absolute() or path.suffix != suffix or path.exists() or path.is_symlink():
        raise CertificationError("invalid_output_path")
    try:
        parent = path.parent.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise CertificationError("invalid_output_parent") from error
    if not parent.is_dir() or not os.access(parent, os.W_OK | os.X_OK):
        raise CertificationError("invalid_output_parent")
    return parent / path.name


def _stage_binary(source: Path, expected_digest: str, work_dir: Path) -> tuple[Path, int]:
    if not _HEX_64.fullmatch(expected_digest):
        raise CertificationError("invalid_engine_digest")
    try:
        resolved = source.resolve(strict=True)
        descriptor = os.open(resolved, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except (OSError, RuntimeError) as error:
        raise CertificationError("invalid_engine_binary") from error
    destination = work_dir / "engine"
    digest = hashlib.sha256()
    size = 0
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or not metadata.st_mode & 0o111:
            raise CertificationError("invalid_engine_binary")
        output = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o500,
        )
        try:
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(output, view)
                    view = view[written:]
            os.fsync(output)
            os.fchmod(output, 0o500)
        finally:
            os.close(output)
    finally:
        os.close(descriptor)
    if digest.hexdigest() != expected_digest or _sha256_file(destination) != expected_digest:
        raise CertificationError("engine_digest_mismatch")
    return destination, size


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _disable_core_dumps() -> None:
    import resource

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def _spawn_engine(
    binary: Path,
    work_dir: Path,
    authority: AuthorityConfig,
    expected_digest: str,
) -> EngineHandle:
    socket_path = work_dir / "engine.sock"
    persist_dir = work_dir / "store"
    security_dir = work_dir / "security"
    runtime_dir = work_dir / "runtime"
    temporary_dir = work_dir / "temporary"
    home_dir = work_dir / "home"
    for directory in (persist_dir, security_dir, runtime_dir, temporary_dir, home_dir):
        directory.mkdir(mode=0o700)
    log_path = work_dir / "engine.log"
    log_file = log_path.open("xb", buffering=0)
    env = {
        "HOME": str(home_dir),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "RUST_BACKTRACE": "0",
        "TMPDIR": str(temporary_dir),
        "TZ": "UTC",
        "XDG_RUNTIME_DIR": str(runtime_dir),
        "GRAPH_SERVICE_AUTH_SECRET": authority.auth_secret,
        "EPISTEMIC_GRAPH_AUDIENCE": authority.context["audience"],
        "EPISTEMIC_GRAPH_TENANT": authority.context["tenant"],
        "EPISTEMIC_GRAPH_POLICY_VERSION": authority.context["policy_version"],
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR": str(security_dir),
        "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON": json.dumps(
            {authority.signer_id: authority.signer_key}, separators=(",", ":")
        ),
        "EPISTEMIC_GRAPH_REDB_SHARDS": "1",
        "EPISTEMIC_GRAPH_MAX_INFLIGHT": "256",
        "EPISTEMIC_GRAPH_MAX_INFLIGHT_PER_GRAPH": "64",
        "EPISTEMIC_GRAPH_READ_RESERVED": "32",
    }
    try:
        if _sha256_file(binary) != expected_digest:
            raise CertificationError("staged_engine_digest_changed")
        process = subprocess.Popen(  # nosec B603 -- staged bytes are digest-pinned
            [
                str(binary),
                "--socket-path",
                str(socket_path),
                "--persist-dir",
                str(persist_dir),
            ],
            cwd=work_dir,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=log_file,
            close_fds=True,
            preexec_fn=_disable_core_dumps,
            start_new_session=True,
        )
    except OSError as error:
        log_file.close()
        raise CertificationError("engine_spawn_failed") from error
    return EngineHandle(process, socket_path, log_file)


def _private_socket_ready(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise CertificationError("private_socket_inspection_failed") from error
    if (
        not stat.S_ISSOCK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise CertificationError("invalid_private_socket")
    return True


async def _bootstrap_and_connect(
    engine: EngineHandle,
    authority: AuthorityConfig,
    graph_ref: str,
    deadline_seconds: float,
    started_ns: int,
) -> tuple[Any, float, dict[str, Any]]:
    from epistemic_graph.client import EpistemicGraphClient

    deadline = time.monotonic() + deadline_seconds
    bootstrap = None
    while time.monotonic() < deadline:
        if engine.process.poll() is not None:
            raise CertificationError("engine_startup_exit")
        if not _private_socket_ready(engine.socket_path):
            await asyncio.sleep(0.02)
            continue
        try:
            bootstrap = await EpistemicGraphClient.connect(
                socket_path=str(engine.socket_path),
                auth_secret=authority.auth_secret,
                graph_name="__commons__",
                verified_context=authority.bootstrap_context,
                timeout=30.0,
                heavy_timeout=120.0,
                connect_timeout=0.5,
                tls=False,
            )
            break
        except (ConnectionError, FileNotFoundError, OSError, TimeoutError):
            await asyncio.sleep(0.02)
    if bootstrap is None:
        raise CertificationError("engine_startup_timeout")
    try:
        await bootstrap.consensus.bootstrap_system_identity(
            agent_id=authority.context["agent_id"],
            signer_id=authority.signer_id,
            signer_key=authority.signer_key,
        )
    finally:
        await bootstrap.close()

    client = await EpistemicGraphClient.connect(
        socket_path=str(engine.socket_path),
        auth_secret=authority.auth_secret,
        graph_name=graph_ref,
        verified_context=authority.context,
        timeout=30.0,
        heavy_timeout=120.0,
        connect_timeout=2.0,
        tls=False,
    )
    health = await client.health()
    if not isinstance(health, dict):
        await client.close()
        raise CertificationError("invalid_health_response")
    advertised = health.get("ops")
    if not isinstance(advertised, list) or not REQUIRED_OPS.issubset(set(advertised)):
        await client.close()
        raise CertificationError("exact_binary_capability_gap")
    elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
    return client, elapsed_ms, health


def _read_rss_kib(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text(encoding="ascii").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (OSError, UnicodeError, ValueError, IndexError):
        return 0
    return 0


def _scenario_request(
    scenario: dict[str, Any], *, seed: int, workload_sha256: str
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol": "g37.performance-probe.v1",
        "scenario_id": scenario["scenario_id"],
        "driver": scenario["driver"],
        "seed": seed,
        "workload_sha256": workload_sha256,
        "scales": scenario["scales"],
        "repetitions": scenario["repetitions"],
        "rows": [
            {
                "row_id": row["row_id"],
                "equivalence_checks": row["equivalence_checks"],
            }
            for row in scenario["rows"]
        ],
    }


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=5)


def _validate_scenario_probe_result(
    value: Any, scenario: dict[str, Any]
) -> dict[str, Any]:
    result = _exact_keys(
        value,
        {"schema_version", "protocol", "scenario_id", "driver", "rows"},
        "scenario_probe_result",
    )
    if (
        result["schema_version"] != SCHEMA_VERSION
        or result["protocol"] != "g37.performance-probe.v1"
        or result["scenario_id"] != scenario["scenario_id"]
        or result["driver"] != scenario["driver"]
    ):
        raise CertificationError("scenario_probe_identity_mismatch")
    rows = result["rows"]
    expected_rows = scenario["rows"]
    if not isinstance(rows, list) or len(rows) != len(expected_rows):
        raise CertificationError("invalid_scenario_probe_row_coverage")
    for row_result_untyped, row_contract in zip(rows, expected_rows, strict=True):
        row_result = _exact_keys(
            row_result_untyped,
            {"row_id", "scales", "equivalence"},
            "scenario_probe_row",
        )
        if row_result["row_id"] != row_contract["row_id"]:
            raise CertificationError("invalid_scenario_probe_row_coverage")
        equivalence = row_result["equivalence"]
        if (
            not isinstance(equivalence, dict)
            or set(equivalence) != set(row_contract["equivalence_checks"])
            or any(not isinstance(outcome, bool) for outcome in equivalence.values())
        ):
            raise CertificationError("invalid_scenario_probe_equivalence")
        scale_results = row_result["scales"]
        if (
            not isinstance(scale_results, list)
            or len(scale_results) != len(scenario["scales"])
        ):
            raise CertificationError("invalid_scenario_probe_scales")
        all_latency_samples: list[int] = []
        for scale_result_untyped, expected_scale in zip(
            scale_results, scenario["scales"], strict=True
        ):
            scale_result = _exact_keys(
                scale_result_untyped,
                {"scale", "work_units", "memory_bytes", "latency_ns"},
                "scenario_probe_scale",
            )
            if scale_result["scale"] != expected_scale:
                raise CertificationError("invalid_scenario_probe_scales")
            for field in ("work_units", "memory_bytes"):
                measured = scale_result[field]
                if (
                    isinstance(measured, bool)
                    or not isinstance(measured, int)
                    or measured <= 0
                ):
                    raise CertificationError("invalid_scenario_probe_measurement")
            samples = scale_result["latency_ns"]
            if (
                not isinstance(samples, list)
                or len(samples) != scenario["repetitions"]
                or any(
                    isinstance(sample, bool)
                    or not isinstance(sample, int)
                    or sample <= 0
                    for sample in samples
                )
            ):
                raise CertificationError("invalid_scenario_probe_latency")
            all_latency_samples.extend(samples)
        # An exact timing probe cannot legitimately report one repeated literal for
        # every repetition at every scale. Reject the characteristic constant-output
        # shape so a placeholder driver cannot manufacture passing evidence. O(1)
        # algorithms remain valid: their work counters may be constant, but their
        # independently-clocked observations are not a hard-coded scalar.
        if len(set(all_latency_samples)) == 1:
            raise CertificationError("constant_scenario_probe_evidence")
    return result


def _execute_exact_scenario(
    binary: Path,
    work_dir: Path,
    scenario: dict[str, Any],
    *,
    seed: int,
    workload_sha256: str,
) -> ScenarioExecution:
    bounds = scenario["resource_bounds"]
    scratch = work_dir / f"scenario-{scenario['scenario_id']}"
    try:
        scratch.mkdir(mode=0o700)
    except OSError as error:
        raise CertificationError("scenario_scratch_creation_failed") from error
    request = _canonical_json(
        _scenario_request(scenario, seed=seed, workload_sha256=workload_sha256)
    )
    process: subprocess.Popen[bytes] | None = None
    sampler: RssSampler | None = None
    started_ns = time.perf_counter_ns()
    initial_rss = 0
    try:
        process = subprocess.Popen(  # nosec B603 -- the staged executable is digest-pinned
            [
                str(binary),
                "--exact-performance-probe",
                "--exact-performance-probe-root",
                str(scratch),
            ],
            cwd=work_dir,
            env={"LC_ALL": "C", "TZ": "UTC", "RUST_BACKTRACE": "0"},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            preexec_fn=_disable_core_dumps,
            start_new_session=True,
        )
        initial_rss = _read_rss_kib(process.pid)
        sampler = RssSampler(process.pid)
        sampler.start()
        try:
            stdout, _ = process.communicate(
                input=request,
                timeout=bounds["maximum_elapsed_ms"] / 1_000,
            )
        except subprocess.TimeoutExpired as error:
            _terminate_process_group(process)
            raise CertificationError("scenario_probe_timeout") from error
        elapsed_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        sampler.stop()
        samples = sampler.samples_kib
        if sampler.saturated:
            raise CertificationError("scenario_rss_sample_limit_exceeded")
        if process.returncode != 0:
            raise CertificationError("scenario_probe_failed")
        if (
            not stdout
            or len(stdout) > bounds["maximum_output_bytes"]
            or len(stdout) > MAX_SCENARIO_OUTPUT_BYTES
        ):
            raise CertificationError("scenario_probe_output_bound")
        if elapsed_ms > bounds["maximum_elapsed_ms"]:
            raise CertificationError("scenario_elapsed_bound")
        peak_rss_kib = max([initial_rss, *samples])
        if peak_rss_kib <= 0:
            raise CertificationError("scenario_rss_unavailable")
        peak_rss_bytes = peak_rss_kib * 1024
        if peak_rss_bytes > bounds["maximum_peak_rss_mib"] * 1024 * 1024:
            raise CertificationError("scenario_rss_bound")
        try:
            decoded = json.loads(stdout)
        except (UnicodeError, json.JSONDecodeError) as error:
            raise CertificationError("invalid_scenario_probe_output") from error
        result = _validate_scenario_probe_result(decoded, scenario)
        return ScenarioExecution(result, elapsed_ms, peak_rss_bytes, len(samples))
    except OSError as error:
        raise CertificationError("scenario_probe_spawn_failed") from error
    finally:
        if sampler is not None and sampler.alive:
            sampler.stop()
        if process is not None:
            _terminate_process_group(process)
        try:
            shutil.rmtree(scratch)
        except OSError as error:
            raise CertificationError("scenario_scratch_cleanup_failed") from error


def _run_exact_scenarios(
    binary: Path,
    expected_digest: str,
    work_dir: Path,
    contracts: ScenarioContracts,
    *,
    seed: int,
    workload_sha256: str,
) -> dict[str, ScenarioExecution]:
    executions: dict[str, ScenarioExecution] = {}
    for scenario in contracts.manifest["scenarios"]:
        if _sha256_file(binary) != expected_digest:
            raise CertificationError("staged_engine_digest_changed")
        execution = _execute_exact_scenario(
            binary,
            work_dir,
            scenario,
            seed=seed,
            workload_sha256=workload_sha256,
        )
        executions[scenario["scenario_id"]] = execution
    if (
        len(executions) != EXPECTED_SCENARIO_COUNT
        or _sha256_file(binary) != expected_digest
    ):
        raise CertificationError("scenario_execution_coverage_gap")
    return executions


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise CertificationError("missing_latency_samples")
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


def _summary(values: list[float]) -> dict[str, Any]:
    return {
        "samples": len(values),
        "p50_ms": round(_percentile(values, 0.50), 6),
        "p95_ms": round(_percentile(values, 0.95), 6),
        "p99_ms": round(_percentile(values, 0.99), 6),
        "maximum_ms": round(max(values), 6),
    }


async def _timed(call: Callable[[], Awaitable[Any]]) -> tuple[Any, float]:
    started = time.perf_counter_ns()
    result = await call()
    return result, (time.perf_counter_ns() - started) / 1_000_000


async def _repeat_latency(
    call: Callable[[], Awaitable[Any]],
    repetitions: int,
    validator: Callable[[Any], None] | None = None,
) -> list[float]:
    values = []
    for _ in range(repetitions):
        result, elapsed = await _timed(call)
        if validator is not None:
            validator(result)
        values.append(elapsed)
    return values


def _growth_ratio(groups: list[list[float]]) -> float:
    medians = [_percentile(group, 0.50) for group in groups if group]
    if len(medians) < 2 or min(medians) <= 0:
        raise CertificationError("insufficient_complexity_samples")
    return max(medians) / min(medians)


def _chunks(values: list[Any], size: int) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _validate_route(value: Any, tenant_ref: str, partition_ref: str) -> None:
    if (
        not isinstance(value, dict)
        or value.get("authoritative") is not True
        or value.get("tenant_ref") != tenant_ref
        or value.get("partition_ref") != partition_ref
        or not isinstance(value.get("placed"), bool)
        or not isinstance(value.get("stale"), bool)
    ):
        raise CertificationError("invalid_routing_response")


def _validate_batch_result(
    value: Any, *, expected_nodes: int = 0, expected_edges: int = 0
) -> None:
    fields = {
        "added_nodes",
        "upserted_nodes",
        "removed_nodes",
        "added_edges",
        "upserted_edges",
        "removed_edges",
        "added_embeddings",
        "errors",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise CertificationError("invalid_batch_result")
    expected = {
        "added_nodes": expected_nodes,
        "upserted_nodes": 0,
        "removed_nodes": 0,
        "added_edges": expected_edges,
        "upserted_edges": 0,
        "removed_edges": 0,
        "added_embeddings": 0,
        "errors": [],
    }
    if value != expected:
        raise CertificationError("incorrect_batch_result")


def _validate_properties_batch(
    value: Any, node_ids: list[str], ordinal_by_id: dict[str, int]
) -> None:
    if not isinstance(value, dict) or set(value) != set(node_ids):
        raise CertificationError("invalid_point_query_result")
    for node_id in node_ids:
        properties = value[node_id]
        if not isinstance(properties, dict) or properties != {
            "kind": "synthetic",
            "ordinal": ordinal_by_id[node_id],
            "partition": ordinal_by_id[node_id] % 32,
        }:
            raise CertificationError("incorrect_point_query_result")


def _validate_pagerank(value: Any, node_ids: set[str]) -> None:
    if not isinstance(value, list) or len(value) != len(node_ids):
        raise CertificationError("invalid_analytics_result")
    observed: set[str] = set()
    score_sum = 0.0
    for row in value:
        if (
            not isinstance(row, (list, tuple))
            or len(row) != 2
            or not isinstance(row[0], str)
            or isinstance(row[1], bool)
            or not isinstance(row[1], int | float)
            or not math.isfinite(float(row[1]))
            or float(row[1]) < 0
        ):
            raise CertificationError("invalid_analytics_result")
        observed.add(row[0])
        score_sum += float(row[1])
    if observed != node_ids or not 0.999_999 <= score_sum <= 1.000_001:
        raise CertificationError("incorrect_analytics_result")


def _validate_modality_page(value: Any, occurrence_ids: set[str]) -> None:
    if not isinstance(value, dict) or set(value) != {"records", "next"}:
        raise CertificationError("invalid_modality_query_result")
    records = value["records"]
    if not isinstance(records, list) or len(records) != 1:
        raise CertificationError("incorrect_modality_query_result")
    occurrence = records[0].get("occurrence_id") if isinstance(records[0], dict) else None
    if occurrence not in occurrence_ids or value["next"] != occurrence:
        raise CertificationError("incorrect_modality_query_result")


def _opaque(namespace: str, seed: int, index: int, kind: str) -> str:
    token = hashlib.sha256(f"{seed}:{index}:{kind}".encode("ascii")).hexdigest()
    return f"eg:{namespace}:{token}"


def _modality_bundle(
    authority: dict[str, Any],
    modality: str,
    source: bytes,
    seed: int,
    index: int,
) -> tuple[dict[str, Any], str, str]:
    if modality not in MODALITIES:
        raise CertificationError("invalid_modality_fixture")
    def token(namespace: str, kind: str) -> str:
        return _opaque(namespace, seed, index, f"{modality}:{kind}")
    content = hashlib.sha256(source).hexdigest()
    artifact = token("artifact", "artifact")
    occurrence = token("occurrence", "occurrence")
    rendition = token("rendition", "rendition")
    segment = token("segment", "segment")
    feature = token("feature", "feature")
    locus = token("locus", "locus")
    derivation_id = token("derivation", "derivation")
    address: dict[str, object] = {
        "document": {"kind": "character_range", "start": 0, "end": len(source)},
        "image": {
            "kind": "image_region",
            "x": 0.1,
            "y": 0.1,
            "width": 0.2,
            "height": 0.2,
        },
        "audio": {"kind": "audio_range", "start_ms": 0, "end_ms": 500},
        "video": {"kind": "video_time_range", "start_ms": 0, "end_ms": 1_000},
    }[modality]
    derivation = {
        "id": derivation_id,
        "transform_ref": token("transform", "transform"),
        "implementation_ref": token("implementation", "implementation"),
        "version_ref": token("version", "version"),
        "model_ref": None,
        "inputs": [{"kind": "occurrence", "id": occurrence}],
    }
    bundle = {
        "protocol_version": 1,
        "privacy": {
            "scanner_ref": token("scanner", "scanner"),
            "policy_version_ref": token("policyversion", "privacy-policy"),
            "raw_pii_persisted": False,
            "local_identifiers_persisted": False,
        },
        "artifacts": [
            {
                "id": artifact,
                "content_ref": f"eg:content:{content}",
                "modality": modality,
                "schema_ref": token("schema", "artifact-schema"),
                "content_version": 1,
            }
        ],
        "occurrences": [
            {
                "id": occurrence,
                "artifact_id": artifact,
                "source_ref": token("source", "source"),
                "observation_version": 1,
                "policy": {
                    "tenant_ref": authority["tenant_ref"],
                    "access_policy_ref": authority["access_policy_ref"],
                    "classification": "internal",
                    "retention_policy_ref": token("retention", "retention"),
                    "deletion_policy_ref": token("deletion", "deletion"),
                    "legal_hold_ref": None,
                    "purpose_refs": [authority["purpose_ref"]],
                },
            }
        ],
        "renditions": [
            {
                "id": rendition,
                "occurrence_id": occurrence,
                "content_ref": f"eg:content:{content}",
                "modality": modality,
                "schema_ref": token("schema", "rendition-schema"),
                "derivation": derivation,
            }
        ],
        "segments": [
            {
                "id": segment,
                "rendition_id": rendition,
                "parent_segment_id": None,
                "kind": SEGMENT_KINDS[modality],
                "ordinal": 0,
                "schema_ref": token("schema", "segment-schema"),
            }
        ],
        "features": [
            {
                "id": feature,
                "subject": {"kind": "segment", "id": segment},
                "kind": "statistic",
                "value_ref": token("value", "feature-value"),
                "schema_ref": token("schema", "feature-schema"),
                "derivation": derivation,
            }
        ],
        "evidence_loci": [
            {
                "id": locus,
                "subject": {"kind": "segment", "id": segment},
                "address": address,
                "policy_ref": authority["access_policy_ref"],
                "derivation_ref": derivation_id,
            }
        ],
    }
    return bundle, occurrence, token("idempotency", "idempotency")


def _native_modality_query(client: Any, modality: str) -> Awaitable[Any]:
    if modality == "document":
        return client.modalities.search_documents("alpha", limit=1)
    if modality == "image":
        return client.modalities.query_image_region(
            x=0.05, y=0.05, width=0.3, height=0.3, limit=1
        )
    if modality == "audio":
        return client.modalities.query_audio_window(
            start_ms=0, end_ms=1_000, minimum_rms=0.0, limit=1
        )
    if modality == "video":
        return client.modalities.query_video_window(
            start_ms=0, end_ms=1_000, keyframes_only=False, limit=1
        )
    raise CertificationError("invalid_modality_fixture")


def _job_state_name(job: Any) -> str:
    if not isinstance(job, dict):
        raise CertificationError("invalid_job_response")
    state = job.get("state")
    if isinstance(state, str):
        return state
    if isinstance(state, dict) and len(state) == 1:
        return str(next(iter(state)))
    raise CertificationError("invalid_job_state")


async def _measure(
    client: Any,
    manifest: dict[str, Any],
    workload: Workload,
    cold_start_ms: float,
    sampler: RssSampler,
    route_tenant_ref: str,
) -> tuple[dict[str, float], dict[str, float], dict[str, Any], dict[str, Any]]:
    graph_ref = manifest["graph_ref"]
    ready_sample_index = sampler.sample_count
    ready_rss_kib = sampler.current_kib()
    if ready_rss_kib <= 0:
        raise CertificationError("missing_ready_rss")
    await client.tenants.create(graph_ref)

    routing_groups: list[list[float]] = []
    point_query_groups: list[list[float]] = []
    node_ingest_groups: list[list[float]] = []
    ingest_latencies: list[float] = []
    ingested_ops = 0
    query_rows = 0
    query_elapsed_ms = 0.0
    next_operation = 0
    scale_points = set(manifest["scale_points"])
    current_node_ingest_group: list[float] = []
    ordinal_by_id = {
        node_id: index for index, node_id in enumerate(workload.node_ids)
    }
    node_id_set = set(workload.node_ids)
    for chunk in _chunks(workload.node_operations, manifest["batch_size"]):
        result, elapsed = await _timed(
            lambda chunk=chunk: client.lifecycle.batch_update(chunk)
        )
        _validate_batch_result(result, expected_nodes=len(chunk))
        ingest_latencies.append(elapsed)
        ingested_ops += len(chunk)
        next_operation += len(chunk)
        current_node_ingest_group.append(elapsed / len(chunk))
        if next_operation in scale_points:
            node_ingest_groups.append(current_node_ingest_group)
            current_node_ingest_group = []
            routing = await _repeat_latency(
                lambda: client.placement.route(
                    route_tenant_ref, workload.route_partition_ref
                ),
                manifest["probe_repetitions"],
                lambda value: _validate_route(
                    value, route_tenant_ref, workload.route_partition_ref
                ),
            )
            ids = workload.node_ids[: manifest["query_batch_size"]]
            point = await _repeat_latency(
                lambda ids=ids: client.nodes.properties_batch(ids),
                manifest["probe_repetitions"],
                lambda value, ids=ids: _validate_properties_batch(
                    value, ids, ordinal_by_id
                ),
            )
            routing_groups.append(routing)
            point_query_groups.append(point)
            query_rows += len(ids) * len(point)
            query_elapsed_ms += sum(point)
    if (
        next_operation != manifest["node_count"]
        or current_node_ingest_group
        or len(routing_groups) != len(scale_points)
        or len(node_ingest_groups) != len(scale_points)
    ):
        raise CertificationError("node_scale_coverage_gap")

    for chunk in _chunks(workload.edge_operations, manifest["batch_size"]):
        result, elapsed = await _timed(
            lambda chunk=chunk: client.lifecycle.batch_update(chunk)
        )
        _validate_batch_result(result, expected_edges=len(chunk))
        ingest_latencies.append(elapsed)
        ingested_ops += len(chunk)

    node_count, edge_count = await asyncio.gather(
        client.nodes.count(), client.edges.count()
    )
    if node_count != manifest["node_count"] or edge_count != manifest["edge_count"]:
        raise CertificationError("incorrect_ingest_cardinality")

    analytics_latencies = await _repeat_latency(
        lambda: client.analytics.pagerank(damping=0.85, iterations=20),
        manifest["analytics_query_repetitions"],
        lambda value: _validate_pagerank(value, node_id_set),
    )
    routing_latencies = [value for group in routing_groups for value in group]
    point_query_latencies = [value for group in point_query_groups for value in group]

    job_submit_latencies: list[float] = []
    job_completion_latencies: list[float] = []
    for _ in range(manifest["job_count"]):
        started = time.perf_counter_ns()
        job, submit_ms = await _timed(
            lambda: client.jobs.submit(
                graph_ref,
                {
                    "MineAssociate": {
                        "transactions": workload.job_transactions,
                        "min_support": 0.1,
                        "min_confidence": 0.5,
                        "algorithm": "fpgrowth",
                    }
                },
                purpose="g37",
            )
        )
        job_submit_latencies.append(submit_ms)
        if not isinstance(job, dict) or not isinstance(job.get("job_id"), str):
            raise CertificationError("invalid_job_submission")
        deadline = time.monotonic() + manifest["job_poll_timeout_seconds"]
        while True:
            status = await client.jobs.status(job["job_id"])
            state = _job_state_name(status)
            if state == "Succeeded":
                break
            if state in {"Failed", "Cancelled"}:
                raise CertificationError("job_terminal_failure")
            if time.monotonic() >= deadline:
                raise CertificationError("job_completion_timeout")
            await asyncio.sleep(0.01)
        job_completion_latencies.append((time.perf_counter_ns() - started) / 1_000_000)

    modality_capability_latencies: list[float] = []
    capability_modalities = list(MODALITIES)
    for modality in capability_modalities:
        for _ in range(manifest["modality_capability_repetitions"]):
            capabilities, elapsed = await _timed(
                lambda modality=modality: client.modalities.capabilities(modality)
            )
            if capabilities != {
                "component_ready": True,
                "component_pass": 12,
                "component_not_applicable": 0,
                "component_total": 12,
            }:
                raise CertificationError("modality_capability_failure")
            modality_capability_latencies.append(elapsed)

    modality_authority = await client.modalities.authority()
    modality_ingest_by_kind: dict[str, list[float]] = {
        modality: [] for modality in MODALITIES
    }
    modality_query_groups_by_kind: dict[str, list[list[float]]] = {
        modality: [] for modality in MODALITIES
    }
    modality_scale_points = set(manifest["modality_scale_points"])
    for modality in MODALITIES:
        occurrence_ids: set[str] = set()
        sources = workload.modality_sources[modality]
        for index, source in enumerate(sources, start=1):
            global_index = MODALITIES.index(modality) * len(sources) + index
            bundle, occurrence, idempotency = _modality_bundle(
                modality_authority,
                modality,
                source,
                manifest["seed"],
                global_index,
            )
            outcome, elapsed = await _timed(
                lambda modality=modality, bundle=bundle, occurrence=occurrence, idempotency=idempotency, source=source: (
                    client.modalities.ingest(
                        modality,
                        idempotency_ref=idempotency,
                        target_occurrence_id=occurrence,
                        bundle_msgpack=msgpack.packb(bundle, use_bin_type=True),
                        source_bytes=source,
                    )
                )
            )
            if (
                not isinstance(outcome, dict)
                or outcome.get("disposition") != "Applied"
                or outcome.get("observation_version") != 1
            ):
                raise CertificationError("incorrect_modality_ingest_result")
            occurrence_ids.add(occurrence)
            modality_ingest_by_kind[modality].append(elapsed)
            if index in modality_scale_points:
                group = await _repeat_latency(
                    lambda modality=modality: _native_modality_query(client, modality),
                    manifest["modality_query_repetitions"],
                    lambda value, occurrence_ids=occurrence_ids: (
                        _validate_modality_page(value, occurrence_ids)
                    ),
                )
                modality_query_groups_by_kind[modality].append(group)
        if len(modality_query_groups_by_kind[modality]) != len(modality_scale_points):
            raise CertificationError("modality_scale_coverage_gap")
    modality_ingest_latencies = [
        value for modality in MODALITIES for value in modality_ingest_by_kind[modality]
    ]
    modality_query_groups = [
        group
        for modality in MODALITIES
        for group in modality_query_groups_by_kind[modality]
    ]
    modality_query_latencies = [value for group in modality_query_groups for value in group]

    all_samples = sampler.samples_kib
    workload_samples = sampler.samples_since(ready_sample_index)
    if sampler.saturated:
        raise CertificationError("memory_sample_limit_exceeded")
    if not all_samples or not workload_samples:
        raise CertificationError("missing_memory_samples")
    peak_rss_kib = max(all_samples)
    workload_peak_rss_kib = max(ready_rss_kib, *workload_samples)
    ingest_elapsed_ms = sum(ingest_latencies)
    routing_elapsed_ms = sum(routing_latencies)
    job_elapsed_ms = sum(job_completion_latencies)
    modality_elapsed_ms = sum(
        modality_capability_latencies
        + modality_ingest_latencies
        + modality_query_latencies
    )
    modality_ops = (
        len(modality_capability_latencies)
        + len(modality_ingest_latencies)
        + len(modality_query_latencies)
    )
    metrics = {
        "cold_start_ready_ms": cold_start_ms,
        "routing_latency_p50_ms": _percentile(routing_latencies, 0.50),
        "routing_latency_p99_ms": _percentile(routing_latencies, 0.99),
        "routing_throughput_ops_per_second": len(routing_latencies)
        / (routing_elapsed_ms / 1_000),
        "ingest_batch_latency_p50_ms": _percentile(ingest_latencies, 0.50),
        "ingest_batch_latency_p99_ms": _percentile(ingest_latencies, 0.99),
        "ingest_throughput_ops_per_second": ingested_ops / (ingest_elapsed_ms / 1_000),
        "point_query_latency_p50_ms": _percentile(point_query_latencies, 0.50),
        "point_query_latency_p99_ms": _percentile(point_query_latencies, 0.99),
        "point_query_throughput_rows_per_second": query_rows / (query_elapsed_ms / 1_000),
        "analytics_query_latency_p99_ms": _percentile(analytics_latencies, 0.99),
        "job_submit_latency_p99_ms": _percentile(job_submit_latencies, 0.99),
        "job_completion_latency_p99_ms": _percentile(job_completion_latencies, 0.99),
        "job_throughput_jobs_per_second": len(job_completion_latencies)
        / (job_elapsed_ms / 1_000),
        "modality_capability_latency_p99_ms": _percentile(
            modality_capability_latencies, 0.99
        ),
        "modality_ingest_latency_p99_ms": _percentile(modality_ingest_latencies, 0.99),
        "modality_query_latency_p99_ms": _percentile(modality_query_latencies, 0.99),
        "modality_throughput_ops_per_second": modality_ops
        / (modality_elapsed_ms / 1_000),
        "memory_ready_rss_mib": ready_rss_kib / 1024,
        "memory_peak_rss_mib": peak_rss_kib / 1024,
        "memory_growth_rss_mib": max(0, workload_peak_rss_kib - ready_rss_kib)
        / 1024,
    }
    metrics = {name: float(value) for name, value in metrics.items()}
    if set(metrics) != set(METRIC_CONTRACT) or any(
        not math.isfinite(value) or value <= 0
        for name, value in metrics.items()
        if name != "memory_growth_rss_mib"
    ):
        raise CertificationError("metric_coverage_gap")
    modality_growth_by_kind = {
        modality: _growth_ratio(modality_query_groups_by_kind[modality])
        for modality in MODALITIES
    }
    complexity = {
        "routing_state_growth_ratio": _growth_ratio(routing_groups),
        "point_query_state_growth_ratio": _growth_ratio(point_query_groups),
        "fixed_batch_ingest_state_growth_ratio": _growth_ratio(node_ingest_groups),
        "modality_index_growth_ratio": max(modality_growth_by_kind.values()),
    }
    measurements = {
        "routing": _summary(routing_latencies),
        "ingest_batch": _summary(ingest_latencies),
        "point_query": _summary(point_query_latencies),
        "analytics_query": _summary(analytics_latencies),
        "job_submit": _summary(job_submit_latencies),
        "job_completion": _summary(job_completion_latencies),
        "modality_capability": _summary(modality_capability_latencies),
        "modality_ingest": _summary(modality_ingest_latencies),
        "modality_query": _summary(modality_query_latencies),
        "memory_samples": len(all_samples),
        "workload_memory_samples": len(workload_samples),
    }
    coverage = {
        "cold_start": {
            "ready": True,
            "private_socket_verified": True,
            "signed_health": True,
        },
        "routing": {
            "operation": "engine_authoritative_placement_route",
            "scale_points": manifest["scale_points"],
            "samples": len(routing_latencies),
            "results_verified": True,
        },
        "ingest": {
            "operation": "atomic_batch_update",
            "nodes": manifest["node_count"],
            "edges": manifest["edge_count"],
            "batches": len(ingest_latencies),
            "result_counts_verified": True,
            "graph_cardinality_verified": True,
        },
        "query": {
            "operations": ["properties_batch", "pagerank"],
            "point_samples": len(point_query_latencies),
            "analytics_samples": len(analytics_latencies),
            "results_verified": True,
        },
        "job": {
            "operation": "durable_association_job",
            "completed": len(job_completion_latencies),
        },
        "modality": {
            "component_probes": capability_modalities,
            "ingests_by_modality": {
                modality: len(modality_ingest_by_kind[modality])
                for modality in MODALITIES
            },
            "native_query_samples_by_modality": {
                modality: sum(
                    len(group) for group in modality_query_groups_by_kind[modality]
                )
                for modality in MODALITIES
            },
            "index_growth_ratio_by_modality": modality_growth_by_kind,
            "results_verified": True,
        },
        "memory": {
            "rss_samples": len(all_samples),
            "workload_rss_samples": len(workload_samples),
        },
    }
    return metrics, complexity, measurements, coverage


def _evaluate(
    measured: dict[str, float],
    thresholds: dict[str, Any],
    contract: dict[str, tuple[str, str]],
) -> tuple[dict[str, Any], list[str]]:
    results: dict[str, Any] = {}
    failures: list[str] = []
    for name, (unit, direction) in contract.items():
        value = measured.get(name)
        if value is None or not math.isfinite(value):
            failures.append(f"missing:{name}")
            continue
        limit = float(thresholds[name][direction])
        passed = value <= limit if direction == "maximum" else value >= limit
        results[name] = {
            "value": round(value, 6),
            "unit": unit,
            "comparator": "less_than_or_equal" if direction == "maximum" else "greater_than_or_equal",
            "threshold": limit,
            "passed": passed,
        }
        if not passed:
            failures.append(f"threshold:{name}")
    return results, failures


def _scenario_binding(
    *,
    engine_sha256: str,
    dataset_manifest_sha256: str,
    workload_sha256: str,
    threshold_manifest_sha256: str,
    contracts: ScenarioContracts,
    authority_fingerprint: str,
    hardware_class: dict[str, Any],
) -> tuple[dict[str, str], str]:
    binding = {
        "engine_sha256": engine_sha256,
        "dataset_manifest_sha256": dataset_manifest_sha256,
        "workload_sha256": workload_sha256,
        "threshold_manifest_sha256": threshold_manifest_sha256,
        "scenario_manifest_sha256": contracts.manifest_sha256,
        "scenario_schema_sha256": contracts.schema_sha256,
        "complexity_ledger_sha256": contracts.ledger_sha256,
        "authority_context_sha256": authority_fingerprint,
        "hardware_class_sha256": hashlib.sha256(
            _canonical_json(hardware_class)
        ).hexdigest(),
    }
    return binding, hashlib.sha256(_canonical_json(binding)).hexdigest()


def _scale_growth_ratio(values: list[float]) -> float:
    if len(values) < 2 or any(not math.isfinite(value) or value <= 0 for value in values):
        raise CertificationError("invalid_scenario_growth_samples")
    return max(values[1:]) / values[0]


def _scenario_threshold_result(
    value: float, threshold: float, unit: str
) -> dict[str, Any]:
    return {
        "value": round(value, 6),
        "unit": unit,
        "comparator": "less_than_or_equal",
        "threshold": float(threshold),
        "passed": value <= float(threshold),
    }


def _evaluate_scenarios(
    contracts: ScenarioContracts,
    executions: dict[str, ScenarioExecution],
    binding_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    row_evidence: dict[str, Any] = {}
    scenario_evidence: dict[str, Any] = {}
    failures: list[str] = []
    for scenario in contracts.manifest["scenarios"]:
        scenario_id = scenario["scenario_id"]
        execution = executions.get(scenario_id)
        if execution is None:
            failures.append(f"missing_scenario:{scenario_id}")
            for row in scenario["rows"]:
                failures.append(f"missing_scenario_row:{row['row_id']}")
            continue
        scenario_evidence[scenario_id] = {
            "driver": scenario["driver"],
            "row_ids": [row["row_id"] for row in scenario["rows"]],
            "elapsed_ms": round(execution.elapsed_ms, 6),
            "peak_rss_bytes": execution.peak_rss_bytes,
            "rss_samples": execution.rss_samples,
            "resource_bounds": scenario["resource_bounds"],
            "evidence_binding_sha256": binding_sha256,
        }
        output_rows = {
            row["row_id"]: row for row in execution.result["rows"]
        }
        for row_contract in scenario["rows"]:
            row_id = row_contract["row_id"]
            output = output_rows.get(row_id)
            if output is None:
                failures.append(f"missing_scenario_row:{row_id}")
                continue
            work = [float(scale["work_units"]) for scale in output["scales"]]
            memory = [float(scale["memory_bytes"]) for scale in output["scales"]]
            latency_by_scale_ms = [
                [float(sample) / 1_000_000 for sample in scale["latency_ns"]]
                for scale in output["scales"]
            ]
            latency_p99 = [
                _percentile(samples, 0.99) for samples in latency_by_scale_ms
            ]
            threshold = row_contract["thresholds"]
            threshold_results = {
                "work_units": _scenario_threshold_result(
                    max(work), threshold["maximum_work_units"], "work_units"
                ),
                "work_growth_ratio": _scenario_threshold_result(
                    _scale_growth_ratio(work),
                    threshold["maximum_work_growth_ratio"],
                    "ratio",
                ),
                "peak_memory_bytes": _scenario_threshold_result(
                    max(memory),
                    threshold["maximum_peak_memory_bytes"],
                    "bytes",
                ),
                "memory_growth_ratio": _scenario_threshold_result(
                    _scale_growth_ratio(memory),
                    threshold["maximum_memory_growth_ratio"],
                    "ratio",
                ),
                "latency_p99_ms": _scenario_threshold_result(
                    max(latency_p99),
                    threshold["maximum_latency_p99_ms"],
                    "milliseconds",
                ),
                "latency_growth_ratio": _scenario_threshold_result(
                    _scale_growth_ratio(latency_p99),
                    threshold["maximum_latency_growth_ratio"],
                    "ratio",
                ),
            }
            equivalence = dict(output["equivalence"])
            for check, passed in equivalence.items():
                if not passed:
                    failures.append(f"scenario_equivalence:{row_id}:{check}")
            for metric, result in threshold_results.items():
                if not result["passed"]:
                    failures.append(f"scenario_threshold:{row_id}:{metric}")
            row_evidence[row_id] = {
                "ledger_row_name": contracts.ledger_rows[row_id],
                "scenario_id": scenario_id,
                "driver": scenario["driver"],
                "scales": [
                    {
                        "scale": scale_result["scale"],
                        "work_units": scale_result["work_units"],
                        "memory_bytes": scale_result["memory_bytes"],
                        "latency_p99_ms": round(latency, 6),
                        "latency_samples": len(scale_result["latency_ns"]),
                    }
                    for scale_result, latency in zip(
                        output["scales"], latency_p99, strict=True
                    )
                ],
                "equivalence": equivalence,
                "threshold_results": threshold_results,
                "resource_evidence": scenario_evidence[scenario_id],
                "evidence_binding_sha256": binding_sha256,
                "passed": all(equivalence.values())
                and all(result["passed"] for result in threshold_results.values()),
            }
    expected_rows = set(contracts.ledger_rows)
    if set(row_evidence) != expected_rows:
        failures.append("scenario_row_evidence_coverage_gap")
    if set(scenario_evidence) != {
        scenario["scenario_id"] for scenario in contracts.manifest["scenarios"]
    }:
        failures.append("scenario_family_evidence_coverage_gap")
    return row_evidence, scenario_evidence, failures


def _coverage_failures(coverage: dict[str, Any]) -> list[str]:
    failures = [
        f"missing_coverage:{name}"
        for name in sorted(COVERAGE_CONTRACT - set(coverage))
    ]
    if set(coverage) - COVERAGE_CONTRACT:
        failures.append("invalid_coverage:unexpected")
    for name in sorted(COVERAGE_CONTRACT & set(coverage)):
        if not isinstance(coverage[name], dict) or not coverage[name]:
            failures.append(f"invalid_coverage:{name}")
    modality = coverage.get("modality")
    expected_modalities = set(MODALITIES)
    if isinstance(modality, dict):
        expected_fields = {
            "component_probes",
            "ingests_by_modality",
            "native_query_samples_by_modality",
            "index_growth_ratio_by_modality",
            "results_verified",
        }
        ingests = modality.get("ingests_by_modality")
        queries = modality.get("native_query_samples_by_modality")
        growth = modality.get("index_growth_ratio_by_modality")
        if (
            set(modality) != expected_fields
            or modality.get("component_probes") != list(MODALITIES)
            or modality.get("results_verified") is not True
            or not isinstance(ingests, dict)
            or set(ingests) != expected_modalities
            or not isinstance(queries, dict)
            or set(queries) != expected_modalities
            or not isinstance(growth, dict)
            or set(growth) != expected_modalities
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 1
                for value in (
                    *(ingests.values() if isinstance(ingests, dict) else ()),
                    *(queries.values() if isinstance(queries, dict) else ()),
                )
            )
            or any(
                isinstance(value, bool)
                or not isinstance(value, int | float)
                or not math.isfinite(float(value))
                or value <= 0
                for value in (growth.values() if isinstance(growth, dict) else ())
            )
        ):
            failures.append("invalid_coverage:modality_inventory")
    hot_paths = coverage.get("hot_path_scenarios")
    if isinstance(hot_paths, dict):
        if hot_paths != {
            "scenario_families": EXPECTED_SCENARIO_COUNT,
            "ledger_rows": EXPECTED_LEDGER_ROW_COUNT,
            "raw_results_validated": True,
            "exact_binary_subcommands": EXPECTED_SCENARIO_COUNT,
        }:
            failures.append("invalid_coverage:hot_path_scenarios")
    return failures


def _memory_class() -> dict[str, Any]:
    host_memory_bytes = 0
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            if line.startswith("MemTotal:"):
                host_memory_bytes = int(line.split()[1]) * 1024
                break
    except (OSError, UnicodeError, ValueError, IndexError):
        pass
    cgroup_limit = None
    try:
        raw = Path("/sys/fs/cgroup/memory.max").read_text(encoding="ascii").strip()
        if raw != "max":
            cgroup_limit = int(raw)
    except (OSError, UnicodeError, ValueError):
        pass
    effective_candidates = [
        value
        for value in (host_memory_bytes, cgroup_limit)
        if value is not None and value > 0
    ]
    effective = min(effective_candidates) if effective_candidates else 0
    release = platform.release().lower()
    if "microsoft" in release:
        runtime = "wsl2" if "wsl2" in release else "wsl1"
    else:
        runtime = "linux_native"
    if Path("/.dockerenv").exists() or Path("/run/.containerenv").exists():
        runtime = "linux_container"
    available_cpus = len(os.sched_getaffinity(0))
    architecture = platform.machine().lower()
    if (
        not architecture
        or not _SAFE_CODE.fullmatch(architecture)
        or available_cpus <= 0
        or host_memory_bytes <= 0
        or effective <= 0
    ):
        raise CertificationError("hardware_class_unavailable")
    return {
        "architecture": architecture,
        "os_family": "linux",
        "runtime_class": runtime,
        "logical_cpu_count": int(available_cpus),
        "host_memory_gib": round(host_memory_bytes / (1024**3), 2),
        "effective_memory_gib": round(effective / (1024**3), 2),
    }


def _assert_evidence_safe(value: dict[str, Any], authority: AuthorityConfig) -> None:
    encoded = json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True)
    if _PATH_OR_ENDPOINT.search(encoded):
        raise CertificationError("evidence_contains_local_reference")
    for secret in (authority.auth_secret, authority.signer_key):
        if secret in encoded:
            raise CertificationError("evidence_contains_secret")


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# G-37 exact-binary performance certification",
        "",
        f"Status: **{report['status'].upper()}**",
        "",
        "This report contains measured results for one digest-pinned engine on the committed synthetic dataset. No result is extrapolated.",
        "",
        "## Artifact and environment",
        "",
        f"- Engine SHA-256: `{report['exact_artifact']['sha256']}`",
        f"- Dataset SHA-256: `{report['dataset']['workload_sha256']}`",
        f"- Scenario manifest SHA-256: `{report['scenario_contract']['manifest_sha256']}`",
        f"- Complexity ledger SHA-256: `{report['scenario_contract']['complexity_ledger_sha256']}`",
        f"- Evidence binding SHA-256: `{report['scenario_evidence_binding']['binding_sha256']}`",
        f"- Architecture: `{report['hardware_class']['architecture']}`",
        f"- Runtime class: `{report['hardware_class']['runtime_class']}`",
        f"- Logical CPUs: `{report['hardware_class']['logical_cpu_count']}`",
        f"- Effective memory GiB: `{report['hardware_class']['effective_memory_gib']}`",
        "",
        "## Threshold results",
        "",
        "| Metric | Measured | Comparator | Threshold | Unit | Result |",
        "|---|---:|---|---:|---|---|",
    ]
    for name in sorted(report["metric_results"]):
        result = report["metric_results"][name]
        lines.append(
            f"| `{name}` | {result['value']:.6f} | `{result['comparator']}` | "
            f"{result['threshold']:.6f} | `{result['unit']}` | "
            f"{'PASS' if result['passed'] else 'FAIL'} |"
        )
    lines.extend(
        [
            "",
            "## Empirical complexity checks",
            "",
            "| Check | Measured ratio | Maximum ratio | Result |",
            "|---|---:|---:|---|",
        ]
    )
    for name in sorted(report["complexity_results"]):
        result = report["complexity_results"][name]
        lines.append(
            f"| `{name}` | {result['value']:.6f} | {result['threshold']:.6f} | "
            f"{'PASS' if result['passed'] else 'FAIL'} |"
        )
    lines.extend(
        [
            "",
            "## Implemented hot-path row evidence",
            "",
            "| Row | Scenario | Work growth | Memory growth | Latency p99 ms | Result |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row_id in sorted(report["hot_path_row_evidence"]):
        evidence = report["hot_path_row_evidence"][row_id]
        thresholds = evidence["threshold_results"]
        lines.append(
            f"| `{row_id}` | `{evidence['scenario_id']}` | "
            f"{thresholds['work_growth_ratio']['value']:.6f} | "
            f"{thresholds['memory_growth_ratio']['value']:.6f} | "
            f"{thresholds['latency_p99_ms']['value']:.6f} | "
            f"{'PASS' if evidence['passed'] else 'FAIL'} |"
        )
    lines.extend(["", "## Coverage", ""])
    for name in sorted(report["coverage"]):
        lines.append(f"- `{name}`: covered")
    if report["failures"]:
        lines.extend(["", "## Failures", ""])
        lines.extend(f"- `{failure}`" for failure in report["failures"])
    return "\n".join(lines) + "\n"


def _write_new(path: Path, content: bytes) -> None:
    descriptor = None
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    except OSError as error:
        with contextlib.suppress(OSError):
            path.unlink()
        raise CertificationError("evidence_write_failed") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


async def _run(args: argparse.Namespace) -> tuple[dict[str, Any], AuthorityConfig]:
    authority = _load_authority(args.authority_config)
    dataset, dataset_manifest_sha256 = _load_dataset(args.dataset_manifest)
    thresholds, threshold_manifest_sha256 = _load_thresholds(args.thresholds)
    scenario_contracts = _load_scenario_contracts(
        args.scenario_manifest,
        args.scenario_schema,
        args.complexity_ledger,
    )
    workload = _workload_from_manifest(dataset)
    hardware_class = _memory_class()
    work_root = _validate_work_root(args.work_root)
    json_output = _validate_output_path(args.json_output, ".json")
    markdown_output = _validate_output_path(args.markdown_output, ".md")
    if json_output == markdown_output:
        raise CertificationError("duplicate_output_path")

    work_dir = Path(os.path.realpath(Path(work_root) / f"g37-{os.urandom(16).hex()}"))
    try:
        work_dir.mkdir(mode=0o700)
    except OSError as error:
        raise CertificationError("work_directory_creation_failed") from error
    engine = None
    sampler = None
    client = None
    started_at = datetime.now(UTC)
    failure_code = None
    metrics: dict[str, float] = {}
    complexity: dict[str, float] = {}
    measurements: dict[str, Any] = {}
    coverage: dict[str, Any] = {}
    binary_size = 0
    staged_copy_verified = False
    client_digest = ""
    client_version = "unknown"
    bootstrap_verified = False
    scenario_executions: dict[str, ScenarioExecution] = {}
    try:
        binary, binary_size = _stage_binary(args.engine_binary, args.engine_sha256, work_dir)
        staged_copy_verified = True
        scenario_executions = _run_exact_scenarios(
            binary,
            args.engine_sha256,
            work_dir,
            scenario_contracts,
            seed=dataset["seed"],
            workload_sha256=workload.digest,
        )
        spawn_started_ns = time.perf_counter_ns()
        engine = _spawn_engine(binary, work_dir, authority, args.engine_sha256)
        sampler = RssSampler(engine.process.pid)
        sampler.start()
        client, cold_start_ms, _ = await _bootstrap_and_connect(
            engine,
            authority,
            dataset["graph_ref"],
            args.startup_timeout_seconds,
            spawn_started_ns,
        )
        bootstrap_verified = True
        from epistemic_graph import __version__ as package_version
        from epistemic_graph import client as client_module

        client_file = Path(client_module.__file__ or "")
        if not client_file.is_file():
            raise CertificationError("client_source_unavailable")
        client_digest = _sha256_file(client_file)
        client_version = str(package_version)
        if not _SAFE_VERSION.fullmatch(client_version):
            raise CertificationError("invalid_client_version")
        metrics, complexity, measurements, coverage = await _measure(
            client,
            dataset,
            workload,
            cold_start_ms,
            sampler,
            authority.context["tenant"],
        )
        if engine.process.poll() is not None:
            raise CertificationError("exact_engine_exited_during_measurement")
    except CertificationError as error:
        failure_code = error.code
    except Exception as error:  # Fail closed without persisting raw exception text.
        failure_code = f"measurement_error:{type(error).__name__.lower()}"
        if not _SAFE_CODE.fullmatch(failure_code):
            failure_code = "measurement_error:unknown"
    finally:
        cleanup_failed = False
        if client is not None:
            try:
                await client.close()
            except Exception:
                cleanup_failed = True
        if sampler is not None:
            try:
                sampler.stop()
            except Exception:
                cleanup_failed = True
        if engine is not None:
            try:
                engine.stop()
            except Exception:
                cleanup_failed = True
        try:
            shutil.rmtree(work_dir)
        except OSError:
            cleanup_failed = True
    evidence_binding, evidence_binding_sha256 = _scenario_binding(
        engine_sha256=args.engine_sha256,
        dataset_manifest_sha256=dataset_manifest_sha256,
        workload_sha256=workload.digest,
        threshold_manifest_sha256=threshold_manifest_sha256,
        contracts=scenario_contracts,
        authority_fingerprint=authority.fingerprint,
        hardware_class=hardware_class,
    )
    hot_path_row_evidence, scenario_family_evidence, scenario_failures = (
        _evaluate_scenarios(
            scenario_contracts,
            scenario_executions,
            evidence_binding_sha256,
        )
    )
    if len(scenario_executions) == EXPECTED_SCENARIO_COUNT:
        coverage["hot_path_scenarios"] = {
            "scenario_families": EXPECTED_SCENARIO_COUNT,
            "ledger_rows": EXPECTED_LEDGER_ROW_COUNT,
            "raw_results_validated": True,
            "exact_binary_subcommands": EXPECTED_SCENARIO_COUNT,
        }
    metric_results, failures = _evaluate(
        metrics, thresholds["metrics"], METRIC_CONTRACT
    )
    complexity_results, complexity_failures = _evaluate(
        complexity, thresholds["complexity"], COMPLEXITY_CONTRACT
    )
    failures.extend(complexity_failures)
    failures.extend(scenario_failures)
    failures.extend(_coverage_failures(coverage))
    if cleanup_failed:
        failures.append("runtime_cleanup_failed")
    if failure_code is not None:
        failures.insert(0, failure_code)
    failures = sorted(set(failures))
    report = {
        "schema_version": SCHEMA_VERSION,
        "gate": GATE_ID,
        "status": "pass" if not failures else "fail",
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "exact_artifact": {
            "component": "epistemic-graph-server",
            "sha256": args.engine_sha256,
            "size_bytes": binary_size,
            "staged_copy_verified": staged_copy_verified,
        },
        "client_artifact": {
            "component": "epistemic-graph-python-client",
            "version": client_version,
            "source_sha256": client_digest,
        },
        "authority": {
            "protocol": "eg2",
            "configuration_verified": True,
            "context_sha256": authority.fingerprint,
            "bootstrap_verified": bootstrap_verified,
            "secret_material_persisted": False,
        },
        "deployment_profile": PROFILE,
        "dataset": {
            "manifest_sha256": dataset_manifest_sha256,
            "workload_sha256": workload.digest,
            "seed": dataset["seed"],
            "nodes": dataset["node_count"],
            "edges": dataset["edge_count"],
            "jobs": dataset["job_count"],
            "records_per_modality": dataset["modality_records_per_kind"],
        },
        "threshold_manifest_sha256": threshold_manifest_sha256,
        "scenario_contract": {
            "manifest_id": scenario_contracts.manifest["manifest_id"],
            "manifest_sha256": scenario_contracts.manifest_sha256,
            "schema_sha256": scenario_contracts.schema_sha256,
            "complexity_ledger_sha256": scenario_contracts.ledger_sha256,
            "scenario_families": EXPECTED_SCENARIO_COUNT,
            "ledger_rows": EXPECTED_LEDGER_ROW_COUNT,
        },
        "scenario_evidence_binding": {
            **evidence_binding,
            "binding_sha256": evidence_binding_sha256,
        },
        "hardware_class": hardware_class,
        "coverage": coverage,
        "measurements": measurements,
        "metric_results": metric_results,
        "complexity_results": complexity_results,
        "scenario_family_evidence": scenario_family_evidence,
        "hot_path_row_evidence": hot_path_row_evidence,
        "failures": failures,
    }
    _assert_evidence_safe(report, authority)
    markdown = _markdown(report)
    if _PATH_OR_ENDPOINT.search(markdown):
        raise CertificationError("markdown_contains_local_reference")
    encoded_report = (
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False).encode("utf-8")
        + b"\n"
    )
    json_written = False
    try:
        _write_new(json_output, encoded_report)
        json_written = True
        _write_new(markdown_output, markdown.encode("utf-8"))
    except CertificationError:
        if json_written:
            with contextlib.suppress(OSError):
                json_output.unlink()
        raise
    return report, authority


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-binary", type=Path, required=True)
    parser.add_argument("--engine-sha256", required=True)
    parser.add_argument("--authority-config", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    parser.add_argument("--dataset-manifest", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--thresholds", type=Path, default=DEFAULT_THRESHOLDS)
    parser.add_argument("--scenario-manifest", type=Path, default=DEFAULT_SCENARIOS)
    parser.add_argument(
        "--scenario-schema", type=Path, default=DEFAULT_SCENARIO_SCHEMA
    )
    parser.add_argument(
        "--complexity-ledger", type=Path, default=DEFAULT_COMPLEXITY_LEDGER
    )
    parser.add_argument("--startup-timeout-seconds", type=float, default=30.0)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if not math.isfinite(args.startup_timeout_seconds) or not 1 <= args.startup_timeout_seconds <= 300:
        print("G-37 certification failed: invalid_startup_timeout", file=sys.stderr)
        return 2
    try:
        report, _ = asyncio.run(_run(args))
    except CertificationError as error:
        print(f"G-37 certification failed: {error.code}", file=sys.stderr)
        return 2
    print(f"G-37 exact-binary performance certification: {report['status'].upper()}")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
