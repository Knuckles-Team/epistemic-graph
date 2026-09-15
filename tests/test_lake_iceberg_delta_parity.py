"""Read-parity tests for the eg-lake Delta + Iceberg write path (W4.8, CONCEPT:EG-317).

Drives the REAL `eg-lake` production write path directly via the `lake-fixture-export`
Rust binary (`src/bin/lake_fixture_export.rs`) — the identical `LakeTable::materialize`/
`delta_log`/`iceberg`/`iceberg_manifests` calls `src/server/lake::LakeManager` makes
against the blob CAS in the live server, here written straight to a local directory —
and asserts a REAL, unmodified `pyiceberg` reads the Iceberg table back correctly and a
REAL, unmodified `deltalake` reads the Delta table back correctly. No HTTP, no live
server, and no reimplementation of either format on the Python side: the engine writes
the bytes, the reference readers open them.

`pyiceberg[pyarrow]`/`deltalake` are TEST-ONLY dependencies (installed from
``tests/lake-parity-requirements.txt`` in an isolated environment; the project extra
is intentionally empty because uv resolves all workspace extras together). An ordinary
developer pytest run may still SKIP the reference-reader cases when those test-only
dependencies are absent. The dedicated ``tests/run_lake_parity.py`` harness enables
strict mode, where missing readers, OAuth fixture dependencies, or pre-built engine
artifacts fail collection instead of silently producing a green run with no parity
proof.

Run standalone with already-built artifacts (bypass the slow shared-engine
conftest fixture, matching `test_kvcache_connector.py`'s documented pattern)::

    EPISTEMIC_GRAPH_TEST_BINARY=target-isolated/debug/epistemic-graph-server \
    EPISTEMIC_GRAPH_LAKE_FIXTURE_BINARY=target-isolated/debug/lake-fixture-export \
      python3 -m pytest tests/test_lake_iceberg_delta_parity.py --noconftest -q

Without those artifacts an ordinary developer run skips the corresponding
live-binary cases. The strict ``tests/run_lake_parity.py`` gate fails closed.
This ``no_engine`` module never starts a Cargo build of its own.

A18/BUG-222 (see `reports/issue-register.md`): the shared
`server::unauthenticated_carrier_denied`
carrier-check STUB that used to deny EVERY `serve_with_security`-wired auxiliary HTTP
surface unconditionally is fixed — `s3-api`, `sparql-http` (mutations),
`kvcache-server`,
and now the Iceberg-REST catalog surface itself all mint a real `CarrierAuthority` from
their own protocol-native proof (SigV4 / `eg2.` envelope / bearer-JWT / OAuth2 bearer
respectively) and work for an authenticated carrier. The Iceberg-REST catalog protocol's
native mechanism is an OAuth2 bearer token (the spec's own `/v1/oauth/tokens`
convention), verified against a configured Keycloak-compatible JWKS issuer
(`EPISTEMIC_GRAPH_ICEBERG_JWT_*`, `crate::server::oidc::JwtValidator`) — a
validly-signed
bearer whose tenant claim matches the deployment's own configured tenant now mints a
`CarrierAuthority` and is served; an unauthenticated request, or a validly-signed bearer
for a DIFFERENT tenant, both still fail closed (403). `test_iceberg_rest_*` below drive
that acceptance matrix (authenticated plus missing/expired/cross-tenant/
insufficient-scope denial) against the real compiled
server binary over a local RSA keypair + JWKS HTTP server standing in for Keycloak (so
the REAL Rust JWKS/RSA verification path is exercised without a live network
dependency). Same open question as before for `obs`/`federation-search` and SPARQL's own
read (SELECT/CONSTRUCT) leg — unaffected by this fix. The read-parity tests above
deliberately do NOT read table bytes back over `--iceberg-addr` HTTP — they drive
`eg-lake`'s real write path directly instead, which exercises 100% of the same
materialize/render code the live listener's `LakeManager` calls, only skipping the HTTP
hop (now gated by a real carrier check rather than a stub).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from pathlib import Path

import pytest
from conftest import _prebuilt_test_binary

# This module drives its OWN dedicated `lake-fixture-export`/`epistemic-graph-server`
# subprocesses; it does not need the shared session-scoped engine conftest starts for
# the rest of the suite (see pytest.ini's marker doc + tests/conftest.py's check).
pytestmark = pytest.mark.no_engine

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PROCESS_TIMEOUT_S = 120
# GOC-70: redb recovery can legitimately make a cold CI start exceed 30 seconds.
SERVER_STARTUP_TIMEOUT_S = 60
STRICT_PARITY = os.environ.get("EPISTEMIC_GRAPH_LAKE_PARITY_STRICT") == "1"

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.table import StaticTable

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False

try:
    from deltalake import DeltaTable

    DELTALAKE_AVAILABLE = True
except ImportError:
    DELTALAKE_AVAILABLE = False

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

_SKIP_PYICEBERG = pytest.mark.skipif(
    not PYICEBERG_AVAILABLE,
    reason="pyiceberg[pyarrow] is not installed (see "
    "tests/lake-parity-requirements.txt)",
)
_SKIP_DELTALAKE = pytest.mark.skipif(
    not DELTALAKE_AVAILABLE,
    reason="deltalake is not installed (see tests/lake-parity-requirements.txt)",
)


def _configured_executable(variable: str) -> str | None:
    """Return a configured executable path, or ``None`` when it is invalid.

    The strict parity harness supplies both engine binaries explicitly. Keeping
    this check in the test module as well means invoking pytest directly with
    ``EPISTEMIC_GRAPH_LAKE_PARITY_STRICT=1`` cannot accidentally fall back to a
    source rebuild or run against an unrelated artifact.
    """

    configured = str(os.environ.get(variable, "") or "").strip()
    if not configured:
        return None
    path = Path(configured).expanduser().resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        return None
    return str(path)


def _required_executable(executable: str | None, variable: str, purpose: str) -> str:
    """Return the configured binary or stop this parity test explicitly."""

    if executable is not None:
        return executable
    if STRICT_PARITY:
        pytest.fail(f"strict lake parity requires an executable {variable}")
    pytest.skip(
        f"{purpose} requires a prebuilt {variable}; "
        "this no_engine module does not launch Cargo"
    )
    raise AssertionError("pytest.skip returned unexpectedly")


def _require_strict_parity_prerequisites() -> None:
    """Fail closed before collection when the dedicated gate is incomplete."""

    if not STRICT_PARITY:
        return

    missing = []
    if not PYICEBERG_AVAILABLE:
        missing.append("pyiceberg[pyarrow]")
    if not DELTALAKE_AVAILABLE:
        missing.append("deltalake")
    if not PANDAS_AVAILABLE:
        missing.append("pandas")
    if not _OAUTH_DEPS_AVAILABLE:
        missing.append("pyjwt + cryptography")
    if _configured_executable("EPISTEMIC_GRAPH_TEST_BINARY") is None:
        missing.append("EPISTEMIC_GRAPH_TEST_BINARY (executable full server)")
    if _configured_executable("EPISTEMIC_GRAPH_LAKE_FIXTURE_BINARY") is None:
        missing.append(
            "EPISTEMIC_GRAPH_LAKE_FIXTURE_BINARY (executable lake-fixture-export)"
        )
    if missing:
        raise RuntimeError(
            "strict lake parity prerequisites are missing: "
            + ", ".join(missing)
            + "; use tests/run_lake_parity.py with the isolated requirements and "
            "exact full-featured engine binaries"
        )


# --------------------------------------------------------------------------------- #
# Fixture-scale expected data — mirrors src/bin/lake_fixture_export.rs's BATCH_1/
# BATCH_2 constants exactly (two commits: CREATE 3 rows, APPEND 2 rows). The binary's
# own stdout JSON is the actual source of truth used in assertions below; this literal
# copy exists only for a human reading this file to see the fixture at a glance.
# --------------------------------------------------------------------------------- #
EXPECTED_ROW_COUNT = 5


def _run_lake_fixture_export(out_dir: Path) -> dict:
    """Run the real fixture exporter; return its parsed JSON summary.

    Both ordinary and strict invocations use an already-built binary. This
    module is marked ``no_engine`` and must never hide an independent Cargo
    workload inside pytest; the strict harness additionally turns a missing
    artifact into a collection-time failure.
    """

    prebuilt = _required_executable(
        _configured_executable("EPISTEMIC_GRAPH_LAKE_FIXTURE_BINARY"),
        "EPISTEMIC_GRAPH_LAKE_FIXTURE_BINARY",
        "lake read parity",
    )
    proc = subprocess.run(
        [prebuilt, str(out_dir)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=FIXTURE_PROCESS_TIMEOUT_S,
    )
    assert proc.returncode == 0, (
        f"lake-fixture-export failed (exit {proc.returncode}):\n"
        f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    )
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    assert lines, f"lake-fixture-export produced no stdout; stderr={proc.stderr!r}"
    return json.loads(lines[-1])


@pytest.fixture(scope="module")
def lake_fixture(tmp_path_factory) -> dict:
    """Run the real engine write path once per module; every test reads the result."""
    out_dir = tmp_path_factory.mktemp("lake-fixture")
    return _run_lake_fixture_export(out_dir)


def _expected_rows_by_id(fixture: dict) -> dict[int, dict]:
    return {row["id"]: row for row in fixture["rows"]}


def _is_na(value) -> bool:
    if value is None:
        return True
    if PANDAS_AVAILABLE:
        try:
            return bool(pd.isna(value))
        except (TypeError, ValueError):
            return False
    try:
        return bool(value != value)  # NaN is the only value unequal to itself
    except TypeError:
        return False


def _ts_micros(value) -> int:
    """Recover the original micros-since-epoch int from whatever timestamp shape the
    reader handed back (pandas.Timestamp, numpy.datetime64, datetime.datetime — all
    accepted by `pd.Timestamp(...)`). Uses a timedelta subtraction rather than
    `.value` so it is correct regardless of the underlying storage resolution
    (ns/us) pandas chose for the column."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    epoch = pd.Timestamp("1970-01-01", tz="UTC")
    return int((ts - epoch) / pd.Timedelta(microseconds=1))


def _assert_row_matches(row: dict, expected: dict) -> None:
    assert int(row["id"]) == expected["id"]
    if expected["price"] is None:
        assert _is_na(row["price"]), f"id={expected['id']}: price should be NULL"
    else:
        assert row["price"] == pytest.approx(expected["price"]), (
            f"id={expected['id']}: price"
        )
    if expected["symbol"] is None:
        assert _is_na(row["symbol"]), f"id={expected['id']}: symbol should be NULL"
    else:
        assert row["symbol"] == expected["symbol"], f"id={expected['id']}: symbol"
    assert bool(row["active"]) == expected["active"], f"id={expected['id']}: active"
    assert _ts_micros(row["ts"]) == expected["ts_micros"], f"id={expected['id']}: ts"


def _assert_two_commit_snapshot_history(table) -> None:
    """Validate the fixture's complete linked Iceberg snapshot projection."""

    snapshots = list(table.metadata.snapshots)
    current_snapshot = next(
        snapshot
        for snapshot in snapshots
        if snapshot.snapshot_id == table.metadata.current_snapshot_id
    )
    actual_history = (
        [snapshot.snapshot_id for snapshot in snapshots],
        [snapshot.parent_snapshot_id for snapshot in snapshots],
        [entry.snapshot_id for entry in table.metadata.snapshot_log],
        len(table.metadata.metadata_log),
        table.metadata.current_snapshot_id,
        current_snapshot.snapshot_id,
    )
    assert actual_history == ([1, 4], [None, 1], [1, 4], 1, 4, 4), (
        "CREATE and APPEND must form one linked, current snapshot history"
    )


# --------------------------------------------------------------------------------- #
# Iceberg read parity (pyiceberg)
# --------------------------------------------------------------------------------- #
class TestIcebergReadParity:
    """A real, unmodified pyiceberg `StaticTable` opens the engine-written table
    directly off its `metadata.json` — no catalog, no HTTP (`StaticTable.from_metadata`
    is pyiceberg's own documented no-catalog entry point)."""

    @_SKIP_PYICEBERG
    def test_row_count_and_fixture_scale(self, lake_fixture):
        assert lake_fixture["row_count"] == EXPECTED_ROW_COUNT
        assert len(lake_fixture["rows"]) == EXPECTED_ROW_COUNT

    @_SKIP_PYICEBERG
    def test_pyiceberg_schema_matches_written_columns(self, lake_fixture):
        table = StaticTable.from_metadata(lake_fixture["metadata_location"])
        fields = table.schema().fields
        assert [f.name for f in fields] == ["id", "price", "symbol", "active", "ts"]
        by_name = {f.name: f for f in fields}
        assert by_name["id"].required is True, "id is the one NOT NULL column"
        for nullable_col in ("price", "symbol", "active", "ts"):
            assert by_name[nullable_col].required is False

    @_SKIP_PYICEBERG
    def test_parquet_footer_uses_offset_utf8_for_external_readers(self, lake_fixture):
        """Every production data file advertises Arrow ``string``, not
        ``string_view``, so PyIceberg/PyArrow filter and take kernels remain usable."""
        data_files = sorted(Path(lake_fixture["location"]).glob("data/*.parquet"))
        assert len(data_files) == 2, "the two fixture commits must produce two files"
        for data_file in data_files:
            schema = pq.read_schema(data_file)
            symbol_type = schema.field("symbol").type
            assert pa.types.is_string(symbol_type) or pa.types.is_large_string(
                symbol_type
            ), (
                f"{data_file.name}: symbol must use offset-based Arrow UTF-8, "
                f"found {symbol_type}"
            )

    @_SKIP_PYICEBERG
    def test_pyiceberg_reads_all_rows_across_both_commits(self, lake_fixture):
        table = StaticTable.from_metadata(lake_fixture["metadata_location"])
        arrow_table = table.scan().to_arrow()
        assert arrow_table.num_rows == lake_fixture["row_count"]

        df = arrow_table.to_pandas().sort_values("id").reset_index(drop=True)
        expected_by_id = _expected_rows_by_id(lake_fixture)
        for record in df.to_dict("records"):
            _assert_row_matches(record, expected_by_id[int(record["id"])])

    @_SKIP_PYICEBERG
    def test_pyiceberg_sees_both_commits_as_snapshot_history(self, lake_fixture):
        table = StaticTable.from_metadata(lake_fixture["metadata_location"])
        _assert_two_commit_snapshot_history(table)

    @_SKIP_PYICEBERG
    def test_pyiceberg_reads_the_actual_historical_snapshot(self, lake_fixture):
        table = StaticTable.from_metadata(lake_fixture["historical_metadata_location"])
        assert table.metadata.current_snapshot_id == 1
        assert [snapshot.snapshot_id for snapshot in table.metadata.snapshots] == [1]
        rows = table.scan().to_arrow()
        assert rows.num_rows == 3
        assert sorted(rows.column("id").to_pylist()) == [1, 2, 3]

    @_SKIP_PYICEBERG
    @pytest.mark.parametrize(
        "location_key",
        ["tombstone_as_of_metadata_location", "unrelated_as_of_metadata_location"],
    )
    def test_pyiceberg_reads_latest_emitted_snapshot_across_lsn_gaps(
        self, lake_fixture, location_key
    ):
        table = StaticTable.from_metadata(lake_fixture[location_key])
        assert table.metadata.current_snapshot_id == 1
        assert [snapshot.snapshot_id for snapshot in table.metadata.snapshots] == [1]
        rows = table.scan().to_arrow()
        assert rows.num_rows == 3
        assert sorted(rows.column("id").to_pylist()) == [1, 2, 3]

    def test_precreation_history_has_no_metadata_location(self, lake_fixture):
        assert lake_fixture["precreation_metadata_location"] is None
        assert not (
            Path(lake_fixture["location"]) / "metadata/v0.metadata.json"
        ).exists()

    @_SKIP_PYICEBERG
    def test_pyiceberg_row_filter_pushdown_matches_expected_subset(self, lake_fixture):
        """A predicate scan (not just a full table read) returns the right rows —
        exercises the REAL Iceberg Avro manifest stats (EG-350) pyiceberg's planner
        reads for file/row-group pruning, not only that the bytes parse."""
        from pyiceberg.expressions import GreaterThan

        table = StaticTable.from_metadata(lake_fixture["metadata_location"])
        df = table.scan(row_filter=GreaterThan("id", 3)).to_arrow().to_pandas()
        assert sorted(df["id"].tolist()) == [4, 5]
        filtered = df.sort_values("id").reset_index(drop=True)
        assert filtered["symbol"].tolist() == ["GOOG", "AMZN"]


# --------------------------------------------------------------------------------- #
# Delta read parity (deltalake / delta-rs)
# --------------------------------------------------------------------------------- #
class TestDeltaReadParity:
    """A real, unmodified `deltalake.DeltaTable` opens the engine-written
    `_delta_log` directly off the local table directory."""

    @_SKIP_DELTALAKE
    def test_deltalake_version_reflects_both_commits(self, lake_fixture):
        dt = DeltaTable(lake_fixture["location"])
        assert dt.version() == 1, "CREATE=version 0, APPEND=version 1"
        assert len(dt.history()) == 2

    @_SKIP_DELTALAKE
    def test_deltalake_reads_all_rows_across_both_commits(self, lake_fixture):
        dt = DeltaTable(lake_fixture["location"])
        pa_table = dt.to_pyarrow_table()
        assert pa_table.num_rows == lake_fixture["row_count"]

        df = pa_table.to_pandas().sort_values("id").reset_index(drop=True)
        expected_by_id = _expected_rows_by_id(lake_fixture)
        for record in df.to_dict("records"):
            _assert_row_matches(record, expected_by_id[int(record["id"])])

    @_SKIP_DELTALAKE
    def test_deltalake_schema_matches_written_columns(self, lake_fixture):
        dt = DeltaTable(lake_fixture["location"])
        names = [f.name for f in dt.schema().fields]
        assert names == ["id", "price", "symbol", "active", "ts"]


# --------------------------------------------------------------------------------- #
# Live listener smoke test — proves --iceberg-addr actually starts an HTTP surface in
# the real compiled `epistemic-graph-server` binary (the "serves the /iceberg catalog
# when configured" acceptance bar), while being honest about today's known gap rather
# than silently skipping status-code assertions (see the module docstring).
# --------------------------------------------------------------------------------- #
def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --------------------------------------------------------------------------------- #
# BUG-222: a local RSA keypair + JWKS HTTP server standing in for Keycloak, so the
# REAL Rust `oidc::JwtValidator` RSA/JWKS/issuer/audience/expiry verification path
# (`EPISTEMIC_GRAPH_ICEBERG_JWT_*`) is exercised end-to-end without depending on
# network reachability to the live homelab Keycloak from wherever this suite runs.
# The issuer/audience strings below are arbitrary (never resolved over the network —
# only string-compared against the signed token's `iss`/`aud`), independent of the
# `EPISTEMIC_GRAPH_AUDIENCE`/`EPISTEMIC_GRAPH_TENANT` pair the PRIMARY `eg2.` protocol
# uses below (a separate, independently-configured credential namespace).
# --------------------------------------------------------------------------------- #
ICEBERG_OAUTH_ISSUER = "https://iceberg-test-issuer.invalid/realms/test"
ICEBERG_OAUTH_AUDIENCE = "epistemic-graph-iceberg-test"
ICEBERG_TENANT = "tenant:test"  # matches EPISTEMIC_GRAPH_TENANT in iceberg_server's env
ICEBERG_OTHER_TENANT = "tenant:intruder"

try:
    import importlib.util

    import jwt as _pyjwt
    from cryptography.hazmat.primitives import serialization

    # Probed by spec, not imported: nothing at module scope binds this name
    # (the fixture below does its own local `import ... as _rsa` when it
    # actually needs the module), so a real import here would just be an
    # unused-name F401 finding.
    _OAUTH_DEPS_AVAILABLE = (
        importlib.util.find_spec("cryptography.hazmat.primitives.asymmetric.rsa")
        is not None
    )
except ImportError:
    _OAUTH_DEPS_AVAILABLE = False


_require_strict_parity_prerequisites()


_SKIP_OAUTH_DEPS = pytest.mark.skipif(
    not _OAUTH_DEPS_AVAILABLE,
    reason="pyjwt/cryptography are not installed (see "
    "tests/lake-parity-requirements.txt)",
)


@pytest.fixture(scope="module")
def iceberg_oauth_fixture():
    """Start the local JWKS HTTP server + generate the RSA keypair once per module."""
    if not _OAUTH_DEPS_AVAILABLE:
        pytest.skip(
            "pyjwt/cryptography are not installed (see "
            "tests/lake-parity-requirements.txt)"
        )
    import http.server
    import threading

    from cryptography.hazmat.primitives.asymmetric import rsa as _rsa
    from jwt.algorithms import RSAAlgorithm

    key = _rsa.generate_private_key(public_exponent=65537, key_size=2048)
    kid = "iceberg-oauth-test-kid"
    jwk = json.loads(RSAAlgorithm.to_jwk(key.public_key()))
    jwk["kid"] = kid
    jwk["use"] = "sig"
    jwk["alg"] = "RS256"
    jwks_body = json.dumps({"keys": [jwk]}).encode("utf-8")

    class _JwksHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(jwks_body)))
            self.end_headers()
            self.wfile.write(jwks_body)

        def log_message(self, *_args):  # silence stdlib request logging
            pass

    httpd = http.server.HTTPServer(("127.0.0.1", 0), _JwksHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    try:
        yield {
            "jwks_url": f"http://127.0.0.1:{httpd.server_address[1]}/jwks",
            "kid": kid,
            "private_pem": private_pem,
        }
    finally:
        httpd.shutdown()
        thread.join(timeout=5)


def _sign_iceberg_bearer(
    fixture: dict,
    *,
    subject: str,
    tenant: str | None,
    expires_in: int = 300,
    scope: str | None = "kg:read kg:write",
) -> str:
    """Mint a real RS256 bearer over `fixture`'s key — the exact shape a
    Keycloak-issued Iceberg-REST OAuth2 token takes (`sub`/`iss`/`aud`/`exp` +
    `tenant_id` and space-delimited `scope` claims, the claim names
    `oidc::JwtValidator::validate_claims` checks)."""
    payload = {
        "sub": subject,
        "iss": ICEBERG_OAUTH_ISSUER,
        "aud": ICEBERG_OAUTH_AUDIENCE,
        "exp": int(time.time()) + expires_in,
    }
    if tenant is not None:
        payload["tenant_id"] = tenant
    if scope is not None:
        payload["scope"] = scope
    return _pyjwt.encode(
        payload,
        fixture["private_pem"],
        algorithm="RS256",
        headers={"kid": fixture["kid"]},
    )


def _terminate_server(proc: subprocess.Popen[str]) -> None:
    """Stop a fixture server, escalating only when graceful shutdown stalls."""

    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _wait_for_iceberg_server(proc: subprocess.Popen[str], port: int) -> None:
    """Wait for the private Iceberg listener or report its captured failure."""

    deadline = time.monotonic() + SERVER_STARTUP_TIMEOUT_S
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate()
            pytest.fail(
                f"epistemic-graph-server exited early (code {proc.returncode}):\n"
                f"stdout={out}\nstderr={err}"
            )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.5)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.5)

    proc.terminate()
    try:
        out, err = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
    pytest.fail(
        f"--iceberg-addr 127.0.0.1:{port} never accepted a connection "
        f"within {SERVER_STARTUP_TIMEOUT_S}s; process exit={proc.returncode}:\n"
        f"stdout={out}\nstderr={err}"
    )


@pytest.fixture(scope="module")
def iceberg_server(tmp_path_factory, iceberg_oauth_fixture):
    persist_dir = tmp_path_factory.mktemp("lake-iceberg-persist")
    socket_path = str(tmp_path_factory.mktemp("lake-iceberg-sock") / "engine.sock")
    state_dir = str(tmp_path_factory.mktemp("lake-iceberg-security"))
    iceberg_port = _free_tcp_port()
    auth_secret = "test-lake-iceberg-secret"  # sanitizer:ignore — test-only value
    env = {
        **os.environ,
        "GRAPH_SERVICE_AUTH_SECRET": auth_secret,
        "EPISTEMIC_GRAPH_REQUIRE_OIDC": "false",
        "EPISTEMIC_GRAPH_AUDIENCE": "epistemic-graph-test",
        "EPISTEMIC_GRAPH_TENANT": ICEBERG_TENANT,
        "EPISTEMIC_GRAPH_POLICY_VERSION": "policy:test",
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR": state_dir,
        "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON": json.dumps(
            {"service:test-suite": "test-key"}
        ),
        "GRAPH_SERVICE_PERSIST_DIR": str(persist_dir),
        # This fixture validates Iceberg authentication and routing, not sharding.
        # One shard keeps its cold-start cost bounded without changing that surface.
        "EPISTEMIC_GRAPH_REDB_SHARDS": "1",
        "EPISTEMIC_GRAPH_ICEBERG_ADDR": f"127.0.0.1:{iceberg_port}",
        # BUG-222: the Iceberg-REST surface's OWN, independently-configured
        # OAuth2 bearer verifier — points at the local JWKS server above
        # rather than the live Keycloak so this suite has no network
        # dependency.
        "EPISTEMIC_GRAPH_ICEBERG_JWT_ISSUER": ICEBERG_OAUTH_ISSUER,
        "EPISTEMIC_GRAPH_ICEBERG_JWT_AUDIENCE": ICEBERG_OAUTH_AUDIENCE,
        "EPISTEMIC_GRAPH_ICEBERG_JWKS_URL": iceberg_oauth_fixture["jwks_url"],
    }
    # Prefer the shared `EPISTEMIC_GRAPH_TEST_BINARY` (see conftest.py's
    # `_prebuilt_test_binary()`) so this module never pays for its own `cargo
    # build`/`cargo run` compile of the real server binary when a caller
    # already has a matching one -- this is the same `epistemic-graph-server`
    # binary the shared session fixture uses, just launched on a private
    # socket/port so `--iceberg-addr` can be exercised in isolation.
    prebuilt = _required_executable(
        _prebuilt_test_binary(),
        "EPISTEMIC_GRAPH_TEST_BINARY",
        "Iceberg REST parity",
    )
    proc = subprocess.Popen(
        [prebuilt, "--socket-path", socket_path],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_iceberg_server(proc, iceberg_port)
        yield f"127.0.0.1:{iceberg_port}"
    finally:
        _terminate_server(proc)


def _http_request(
    addr: str,
    method: str,
    path: str,
    *,
    headers: dict | None = None,
    body: str | None = None,
) -> tuple[int, str]:
    import http.client

    host, port = addr.split(":")
    conn = http.client.HTTPConnection(host, int(port), timeout=10)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", errors="replace")
    finally:
        conn.close()


def _http_get(addr: str, path: str, headers: dict | None = None) -> tuple[int, str]:
    return _http_request(addr, "GET", path, headers=headers)


def _assert_non_oracular_carrier_denial(status: int, body: str) -> None:
    assert status == 403, f"expected carrier denial (403), got {status}: {body}"
    assert json.loads(body) == {
        "error": {
            "message": "Iceberg carrier request denied",
            "type": "ForbiddenException",
            "code": 403,
        }
    }, "all carrier failures must share one privacy-safe error envelope"


def test_iceberg_rest_listener_responds_when_configured(iceberg_server):
    """The real server binary, built with `full` (which folds `lake`+`lake-rest` in as
    of W4.8), opens a live HTTP listener on `--iceberg-addr` and speaks the
    Iceberg-REST envelope shape — proven here without any dependency on the
    pyjwt/cryptography test-only deps the `test_iceberg_rest_*` auth-outcome
    tests below (BUG-222) need. An unauthenticated request is denied (403), exactly
    like `test_iceberg_rest_unauthenticated_request_is_denied` proves again below.
    """
    status, body = _http_get(iceberg_server, "/v1/config")
    _assert_non_oracular_carrier_denial(status, body)


# --------------------------------------------------------------------------------- #
# BUG-222 acceptance matrix: authenticated (200) plus missing, expired,
# cross-tenant and insufficient-scope (403) denials. The
# `server::unauthenticated_carrier_denied` shared
# fail-closed predicate — untouched by this fix) is exercised here through the
# Iceberg-REST surface's OWN OAuth2 bearer proof (`server::lake::rest::verify_bearer`
# + `server::auth::mint_iceberg_carrier`), driven end-to-end against the real
# compiled server binary rather than a unit-level stand-in.
# --------------------------------------------------------------------------------- #
@_SKIP_OAUTH_DEPS
def test_iceberg_rest_authenticated_bearer_is_allowed(
    iceberg_server, iceberg_oauth_fixture
):
    """A bearer that RSA/JWKS/issuer/audience/expiry-verifies AND asserts this
    deployment's own tenant (`EPISTEMIC_GRAPH_TENANT`) mints a real `CarrierAuthority`
    — the Iceberg-REST catalog surface serves the request instead of denying it."""
    token = _sign_iceberg_bearer(
        iceberg_oauth_fixture, subject="agent:reader", tenant=ICEBERG_TENANT
    )
    status, body = _http_get(
        iceberg_server, "/v1/config", headers={"Authorization": f"Bearer {token}"}
    )
    assert status == 200, (
        f"expected an authenticated, correctly-tenanted bearer to be ALLOWED, "
        f"got {status}: {body}"
    )
    payload = json.loads(body)
    assert "defaults" in payload and "overrides" in payload


@_SKIP_OAUTH_DEPS
def test_iceberg_rest_unauthenticated_request_is_denied(iceberg_server):
    """The fail-closed direction still holds after BUG-222: no bearer at all is
    denied exactly as before this fix — `server::access::unauthenticated_carrier_denied`
    itself was never relaxed, only the caller (`carrier_denied` in
    `server::lake::rest`) that used to hand it a hardcoded `None`."""
    status, body = _http_get(iceberg_server, "/v1/config")
    _assert_non_oracular_carrier_denial(status, body)


@_SKIP_OAUTH_DEPS
def test_iceberg_rest_cross_tenant_bearer_is_denied(
    iceberg_server, iceberg_oauth_fixture
):
    """A validly-signed, unexpired, correctly-issued/audienced bearer for a
    DIFFERENT tenant must still be denied — proves the tenant-match check itself,
    not merely 'is there any Authorization header' (a known-bad input: everything
    about this token verifies except which tenant it asserts)."""
    token = _sign_iceberg_bearer(
        iceberg_oauth_fixture, subject="agent:intruder", tenant=ICEBERG_OTHER_TENANT
    )
    status, body = _http_get(
        iceberg_server, "/v1/config", headers={"Authorization": f"Bearer {token}"}
    )
    _assert_non_oracular_carrier_denial(status, body)


@_SKIP_OAUTH_DEPS
def test_iceberg_rest_expired_bearer_is_denied(iceberg_server, iceberg_oauth_fixture):
    """A correctly signed bearer whose expiry is in the past cannot mint a
    carrier, and its response does not reveal whether the requested resource
    exists."""
    token = _sign_iceberg_bearer(
        iceberg_oauth_fixture,
        subject="agent:expired",
        tenant=ICEBERG_TENANT,
        expires_in=-300,
    )
    status, body = _http_get(
        iceberg_server, "/v1/namespaces", headers={"Authorization": f"Bearer {token}"}
    )
    _assert_non_oracular_carrier_denial(status, body)


@_SKIP_OAUTH_DEPS
def test_iceberg_rest_insufficient_scope_is_denied_without_resource_oracle(
    iceberg_server, iceberg_oauth_fixture
):
    """A validly signed, correctly tenanted write-only carrier cannot perform
    a read, and the scope denial uses the same envelope as invalid carriers
    before the target namespace/table is examined."""
    token = _sign_iceberg_bearer(
        iceberg_oauth_fixture,
        subject="agent:reader",
        tenant=ICEBERG_TENANT,
        scope="kg:write",
    )
    status, body = _http_request(
        iceberg_server,
        "GET",
        "/v1/namespaces",
        headers={"Authorization": f"Bearer {token}"},
    )
    _assert_non_oracular_carrier_denial(status, body)
