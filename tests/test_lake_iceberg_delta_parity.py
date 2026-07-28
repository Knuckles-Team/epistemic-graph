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
is intentionally empty because uv resolves all workspace extras together). Every test
below is SKIPPED, not failed, when they are not installed.

Run standalone (bypass the slow shared-engine conftest fixture, matching
`test_kvcache_connector.py`'s documented pattern)::

    python3 -m pytest tests/test_lake_iceberg_delta_parity.py --noconftest -q

A18 (see `reports/issue-register.md`): the shared `server::unauthenticated_carrier_denied`
carrier-check STUB that used to deny EVERY `serve_with_security`-wired auxiliary HTTP
surface unconditionally is fixed — `s3-api`, `sparql-http` (mutations), and
`kvcache-server` now mint a real `CarrierAuthority` from their own protocol-native proof
(SigV4 / `eg2.` envelope / bearer-JWT respectively) and work for an authenticated
carrier. The production Iceberg-REST listener wiring (`serve_with_security` in
`src/main.rs`) still denies every HTTP request, honestly now: the Iceberg-REST catalog
protocol carries no credential this engine can verify yet (no `eg2.` envelope,
bearer/OAuth2 token, or other proof — the real Iceberg-REST spec's own convention is an
OAuth2 client-credentials bearer token, a deliberate scheme decision NOT made here), so
no `CarrierAuthority` can ever be minted for this surface today — same open question for
`obs`/`federation-search` and SPARQL's own read (SELECT/CONSTRUCT) leg. That is why the
tests here deliberately do NOT read table bytes back over `--iceberg-addr` HTTP — they
drive `eg-lake`'s real write path directly instead, which exercises 100% of the same
materialize/render code the live listener's `LakeManager` calls, only skipping the
gated HTTP hop. `test_iceberg_rest_listener_responds_when_configured` below proves the
listener itself starts and speaks HTTP from the real compiled server binary, and
documents today's 403 (a real, deliberate denial now, not the old stub) rather than
silently ignoring it.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from pathlib import Path

import pytest

# This module drives its OWN dedicated `lake-fixture-export`/`epistemic-graph-server`
# subprocesses; it does not need the shared session-scoped engine conftest starts for
# the rest of the suite (see pytest.ini's marker doc + tests/conftest.py's check).
pytestmark = pytest.mark.no_engine

REPO_ROOT = Path(__file__).resolve().parents[1]
CARGO_BUILD_TIMEOUT_S = 900

try:
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
    reason="pyiceberg[pyarrow] is not installed (see tests/lake-parity-requirements.txt)",
)
_SKIP_DELTALAKE = pytest.mark.skipif(
    not DELTALAKE_AVAILABLE,
    reason="deltalake is not installed (see tests/lake-parity-requirements.txt)",
)


# --------------------------------------------------------------------------------- #
# Fixture-scale expected data — mirrors src/bin/lake_fixture_export.rs's BATCH_1/
# BATCH_2 constants exactly (two commits: CREATE 3 rows, APPEND 2 rows). The binary's
# own stdout JSON is the actual source of truth used in assertions below; this literal
# copy exists only for a human reading this file to see the fixture at a glance.
# --------------------------------------------------------------------------------- #
EXPECTED_ROW_COUNT = 5


def _run_lake_fixture_export(out_dir: Path) -> dict:
    """Build+run the real `lake-fixture-export` binary; return its parsed JSON summary."""
    proc = subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--features",
            "full",
            "--bin",
            "lake-fixture-export",
            "--",
            str(out_dir),
        ],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=CARGO_BUILD_TIMEOUT_S,
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
        snapshots = list(table.metadata.snapshots)
        assert len(snapshots) == 2, (
            "the CREATE and the APPEND must both be real Iceberg snapshots"
        )
        assert table.metadata.current_snapshot_id == snapshots[-1].snapshot_id

    @_SKIP_PYICEBERG
    def test_pyiceberg_row_filter_pushdown_matches_expected_subset(self, lake_fixture):
        """A predicate scan (not just a full table read) returns the right rows —
        exercises the REAL Iceberg Avro manifest stats (EG-350) pyiceberg's planner
        reads for file/row-group pruning, not only that the bytes parse."""
        from pyiceberg.expressions import GreaterThan

        table = StaticTable.from_metadata(lake_fixture["metadata_location"])
        df = table.scan(row_filter=GreaterThan("id", 3)).to_arrow().to_pandas()
        assert sorted(df["id"].tolist()) == [4, 5]


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


@pytest.fixture(scope="module")
def iceberg_server(tmp_path_factory):
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
        "EPISTEMIC_GRAPH_TENANT": "tenant:test",
        "EPISTEMIC_GRAPH_POLICY_VERSION": "policy:test",
        "EPISTEMIC_GRAPH_SECURITY_STATE_DIR": state_dir,
        "EPISTEMIC_GRAPH_SIGNER_KEYS_JSON": json.dumps(
            {"service:test-suite": "test-key"}
        ),
        "GRAPH_SERVICE_PERSIST_DIR": str(persist_dir),
        "EPISTEMIC_GRAPH_ICEBERG_ADDR": f"127.0.0.1:{iceberg_port}",
    }
    proc = subprocess.Popen(
        [
            "cargo",
            "run",
            "--quiet",
            "--features",
            "full",
            "--bin",
            "epistemic-graph-server",
            "--",
            "--socket-path",
            socket_path,
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + CARGO_BUILD_TIMEOUT_S
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                out, err = proc.communicate()
                pytest.fail(
                    f"epistemic-graph-server exited early (code {proc.returncode}):\n"
                    f"stdout={out}\nstderr={err}"
                )
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.settimeout(0.5)
                if probe.connect_ex(("127.0.0.1", iceberg_port)) == 0:
                    break
            time.sleep(0.5)
        else:
            pytest.fail(
                f"--iceberg-addr 127.0.0.1:{iceberg_port} never accepted a connection"
            )
        yield f"127.0.0.1:{iceberg_port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _http_get(addr: str, path: str) -> tuple[int, str]:
    import http.client

    host, port = addr.split(":")
    conn = http.client.HTTPConnection(host, int(port), timeout=10)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read().decode("utf-8", errors="replace")
    finally:
        conn.close()


def test_iceberg_rest_listener_responds_when_configured(iceberg_server):
    """The real server binary, built with `full` (which folds `lake`+`lake-rest` in as
    of W4.8), opens a live HTTP listener on `--iceberg-addr` and speaks the
    Iceberg-REST envelope shape.

    A18 (see the module docstring + reports/issue-register.md): the OLD
    cross-cutting `server::unauthenticated_carrier_denied` stub that used to deny
    EVERY `serve_with_security`-wired auxiliary surface unconditionally is fixed.
    This surface still returns 403 for this plain, unauthenticated request, but
    now for a real, deliberate reason: the Iceberg-REST catalog protocol carries
    no credential this engine can verify yet (no `eg2.` envelope, bearer/OAuth2
    token, or other proof), so no `CarrierAuthority` can be minted here — a
    genuine open scheme decision (see `reports/issue-register.md`, A18), not a
    stub. This assertion pins that CURRENT, documented, correctly-fail-closed
    behavior; if/when a carrier scheme is decided and implemented for THIS
    surface specifically, this test's status-code assertion should be updated
    (and the JSON body asserted for an authenticated request) as a deliberate,
    reviewed change rather than an unnoticed regression in either direction.
    """
    status, body = _http_get(iceberg_server, "/v1/config")
    assert status == 403, (
        f"expected the current fail-closed carrier gate (403), got {status}: {body}"
    )
    payload = json.loads(body)
    assert payload["error"]["type"] == "ForbiddenException"
