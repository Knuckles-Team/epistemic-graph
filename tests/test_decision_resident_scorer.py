"""Compiled-engine R090.2 acceptance; Python only drives authenticated RPCs.

Run against a prebuilt full binary with MEDULLA_TEST_BINARY. The test
never builds or substitutes an in-process Python scorer. Linux strace records
all engine descendants; any inference subprocess, outgoing connection, or GPU
open fails acceptance. R090.3 resource benchmarking remains separate.
"""

from __future__ import annotations

import contextlib
import os
import re
import secrets
import shutil
import signal
import subprocess
from pathlib import Path

import pytest
from _decision_resident_fixture import fit_and_publish, scores
from conftest import (
    TEST_AGENT_ID,
    TEST_SIGNER_KEY,
    bootstrap_context,
    find_server_binary,
    request_context,
    server_timeout,
    spawn_server,
    strict_server_env,
    wait_for_server,
)
from pydantic import BaseModel

from medulla.client import SyncMedullaClient

SECRET = secrets.token_hex(32)


@contextlib.contextmanager
def engine(tmp_path, run, *, bootstrap):
    binary = find_server_binary()
    assert binary, (
        "supply a prebuilt full MEDULLA_TEST_BINARY; no cold build is attempted"
    )
    tracer = shutil.which("strace")
    assert tracer, "Linux strace is required to verify the native process boundary"
    socket = str(tmp_path / "engine.sock")
    Path(socket).unlink(missing_ok=True)
    persist = tmp_path / "persist"
    persist.mkdir(exist_ok=True)
    empty_path = tmp_path / "empty-path"
    empty_path.mkdir(exist_ok=True)
    env = strict_server_env(
        str(tmp_path / "security"), auth_secret=SECRET, persist_dir=str(persist)
    )
    env.update(
        PATH=str(empty_path),
        HOME=str(tmp_path),
        MEDULLA_ENCRYPTION_KEY="resident-test-encryption-key",
        CUDA_VISIBLE_DEVICES="",
        HIP_VISIBLE_DEVICES="",
        ROCR_VISIBLE_DEVICES="",
    )
    trace = tmp_path / f"{run}.trace"
    log = tmp_path / f"{run}.log"
    # Keep strace as a waitable parent. It exits only after all traced tasks
    # finish and the output is flushed. EXITKILL also prevents orphaned engine
    # children if readiness or shutdown times out and the tracer is killed.
    process = spawn_server(
        [
            tracer,
            "--kill-on-exit",
            "-f",
            "-s",
            "4096",
            "-o",
            str(trace),
            "-e",
            "trace=execve,execveat,connect,open,openat,openat2",
            str(Path(binary).resolve()),
            "--socket-path",
            socket,
        ],
        env=env,
        log_path=log,
    )
    try:
        wait_for_server(process, socket, name="resident engine", log_path=log)
        if bootstrap:
            with SyncMedullaClient.connect(
                socket_path=socket,
                auth_secret=SECRET,
                verified_context=bootstrap_context(),
            ) as client:
                client.consensus.bootstrap_system_identity(
                    agent_id=TEST_AGENT_ID,
                    signer_id=TEST_AGENT_ID,
                    signer_key=TEST_SIGNER_KEY,
                )
        # DecisionFit/DecisionEval run synchronously in a debug engine under
        # strace -f; the client's 60 s default RPC budget is a wall-clock race on
        # a loaded host. Use the same explicit ceiling as engine start/stop.
        with SyncMedullaClient.connect(
            socket_path=socket,
            auth_secret=SECRET,
            verified_context=request_context(),
            timeout=server_timeout(),
        ) as client:
            try:
                yield client
            except Exception as error:
                # Only closed diagnostic tokens leave the synthetic fixture.
                # Raw native logs can contain request values and stay private.
                add_scorer_diagnostic_note(error, log)
                raise
    finally:
        stop_traced_engine(process)
    assert_no_external_runtime(trace.read_text())


SCORER_STAGES = frozenset(
    {
        "store_open",
        "worker_join",
        "task_plan",
        "candidates",
        "pinned_inputs",
        "policy",
        "outcome_rates",
        "execute",
        "belief_slices",
        "binding",
        "seal",
    }
)
SCORER_REASONS = frozenset(
    {
        "persist_directory_missing",
        "owner_manifest_changed",
        "read_manifest_changed",
        "scope_unbound",
        "scope_identity_mismatch",
        "store_root_mismatch",
        "serving_grant_mismatch",
        "read_capability_mismatch",
        "table_outside_layout",
        "scoped_accessor_required",
        "control_scope_required",
        "component_row_decode",
        "component_revision_bound",
        "component_byte_bound",
        "worker_join_failed",
        "mutation_ledger_corrupt",
        "component_pin_mismatch",
        "head_invalid",
        "feature_schema_invalid",
        "unclassified",
    }
)


def add_scorer_diagnostic_note(error, log):
    # Python 3.10 has no BaseException.add_note; never mask its original error.
    add_note = getattr(error, "add_note", None)
    if callable(add_note):
        add_note(scorer_diagnostics(log))


def scorer_diagnostics(log):
    """Export fixed allowlisted tokens only, never raw text or log paths."""
    try:
        # Bound memory even if an unrelated failure produces excessive output.
        with log.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            stream.seek(max(0, stream.tell() - 65536))
            text = stream.read(65536).decode("utf-8", errors="replace")
    except OSError:
        return "resident scorer diagnostic: log unavailable"
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)
    found = set()
    for line in text.splitlines():
        if "resident scorer stage refused" not in line:
            continue
        stage = re.search(r'\bstage="?([a-z_]+)"?(?:\s|$)', line)
        reason = re.search(r'\breason="?([a-z_]+)"?(?:\s|$)', line)
        if (
            stage
            and reason
            and stage[1] in SCORER_STAGES
            and reason[1] in SCORER_REASONS
        ):
            found.add((stage[1], reason[1]))
    summary = "; ".join(f"{stage}/{reason}" for stage, reason in sorted(found))
    return "resident scorer diagnostic: " + (summary or "no classified event")


@pytest.mark.no_engine
def test_scorer_diagnostics_never_export_raw_log_values(tmp_path):
    log = tmp_path / "private.log"
    log.write_text(
        "secret=do-not-export /private/path\n"
        'resident scorer stage refused stage="pinned_inputs" '
        'reason="component_row_decode" secret=do-not-export\n'
        'resident scorer stage refused stage="secret" reason="private"\n'
    )
    assert scorer_diagnostics(log) == (
        "resident scorer diagnostic: pinned_inputs/component_row_decode"
    )
    log.write_text("unknown private refusal secret=do-not-export")
    assert scorer_diagnostics(log) == "resident scorer diagnostic: no classified event"

    class LegacyException:
        pass

    add_scorer_diagnostic_note(LegacyException(), log)
    error = RuntimeError("original error")
    add_scorer_diagnostic_note(error, log)
    assert str(error) == "original error"
    if hasattr(error, "__notes__"):
        assert error.__notes__ == ["resident scorer diagnostic: no classified event"]
    log.unlink()
    assert scorer_diagnostics(log) == "resident scorer diagnostic: log unavailable"


def stop_traced_engine(process):
    """Gracefully stop the direct engine child, then reap the tracing parent."""
    if process.poll() is not None:
        return
    children = Path(f"/proc/{process.pid}/task/{process.pid}/children")
    try:
        for child in children.read_text().split():
            with contextlib.suppress(ProcessLookupError):
                os.kill(int(child), signal.SIGTERM)
        process.wait(timeout=server_timeout())
    except (OSError, subprocess.TimeoutExpired):
        process.kill()  # EXITKILL terminates every remaining traced task.
        process.wait(timeout=server_timeout())
        raise AssertionError("native engine/tracer did not shut down cleanly") from None


def assert_no_external_runtime(trace):
    assert not re.search(
        r"libpython|/python\d|/site-packages/|/dist-packages/", trace
    ), "engine loaded an embedded Python runtime or standard library"
    lines = trace.splitlines()
    executions = [line for line in lines if "execve(" in line or "execveat(" in line]
    assert len(executions) == 1, f"only the compiled engine may execute: {executions}"
    assert "medulla-server" in executions[0]
    assert not any("connect(" in line for line in lines), (
        "engine attempted an outgoing IPC/network connection"
    )
    assert not any(
        device in trace for device in ("/dev/nvidia", "/dev/dri/", "/dev/kfd")
    ), "engine accessed a GPU"


@pytest.mark.exact_artifact
@pytest.mark.spec("MEDULLA-DECISION-ENGINE-R090.2")
def test_compiled_engine_scores_without_external_runtime(tmp_path):
    with engine(tmp_path, "first", bootstrap=True) as client:
        asked = fit_and_publish(client)
        recorded = scores(client, asked)
        assert scores(client, asked) == recorded
    # Reopen the durable head in a separate compiled-engine process. Compare
    # fixed-point bytes, excluding timestamps/nonces unrelated to scoring.
    with engine(tmp_path, "restart", bootstrap=False) as client:
        assert scores(client, asked) == recorded


@pytest.mark.no_engine
def test_wire_fixture_matches_generated_request_contracts():
    from _decision_resident_fixture import (
        DIGEST,
        dataset,
        job_requests,
        publication,
        request,
        schema_body,
    )

    from medulla.generated import models

    schema = {
        "component_id": "schema",
        "kind": "feature_schema",
        "definition_digest": DIGEST,
    }
    data = dataset({"candidate_ids": ["a", "b", "c"], "values": [1, 2, 3, 4, 5, 6]})
    payloads: list[tuple[type[BaseModel], dict]] = [
        (
            models.MethodAgentComponentParams,
            publication("schema", "feature_schema", schema_body()),
        ),
        (models.MethodDecideParams, {"request": request(schema)}),
        (models.MethodDecisionFitParams, job_requests(schema, data)),
        (
            models.MethodDecisionEvalParams,
            job_requests(schema, data, {"draft_sha256": "1" * 64, "draft_length": 10}),
        ),
    ]
    for model, value in payloads:
        model.model_validate(value)


@pytest.mark.no_engine
@pytest.mark.parametrize(
    "violation",
    [
        '2 execve("/usr/bin/python3", [], []) = 0',
        '2 execveat(3, "sidecar", [], [], 0) = 0',
        "2 connect(4, {sa_family=AF_INET}, 16) = -1 ECONNREFUSED",
        '2 openat(AT_FDCWD, "/dev/nvidia0", O_RDWR) = 3',
        '2 openat(AT_FDCWD, "/dev/dri/renderD128", O_RDWR) = 3',
        '2 openat(AT_FDCWD, "/dev/kfd", O_RDWR) = 3',
        '2 openat(AT_FDCWD, "/usr/lib/libpython3.12.so", O_RDONLY) = 3',
        '2 openat(AT_FDCWD, "/usr/lib/python3.12/os.py", O_RDONLY) = 3',
        '2 openat2(AT_FDCWD, "/usr/lib/python312.zip", {}, 24) = 3',
        '2 open("/opt/site-packages/torch/__init__.py", O_RDONLY) = 3',
    ],
)
def test_runtime_trace_rejects_external_dependency_attempts(violation):
    trace = '1 execve("/tmp/medulla-server", [], []) = 0\n'
    assert_no_external_runtime(trace)
    with pytest.raises(AssertionError):
        assert_no_external_runtime(trace + violation)


@pytest.mark.no_engine
@pytest.mark.parametrize("timeout", [False, True])
def test_tracer_shutdown_is_bounded_and_reaped(monkeypatch, timeout):
    events: list[object] = []

    class Process:
        pid = 123
        waits = 0

        def poll(self):
            return None

        def wait(self, *, timeout):
            self.waits += 1
            events.append("wait")
            if self.waits == 1 and fail_wait:
                raise subprocess.TimeoutExpired("strace", timeout)
            return 0

        def kill(self):
            events.append("kill-tracer")

    fail_wait = timeout
    monkeypatch.setattr(Path, "read_text", lambda self: "456")
    monkeypatch.setattr(os, "kill", lambda pid, sig: events.append((pid, sig)))
    process = Process()
    if timeout:
        with pytest.raises(AssertionError, match="shut down cleanly"):
            stop_traced_engine(process)
        assert events == [(456, signal.SIGTERM), "wait", "kill-tracer", "wait"]
    else:
        stop_traced_engine(process)
        assert events == [(456, signal.SIGTERM), "wait"]
