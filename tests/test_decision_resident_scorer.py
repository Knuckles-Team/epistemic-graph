"""Compiled-engine R090.2 acceptance; Python only drives authenticated RPCs.

Run against a prebuilt full binary with EPISTEMIC_GRAPH_TEST_BINARY. The test
never builds or substitutes an in-process Python scorer. Linux strace records
all engine descendants; any inference subprocess, outgoing connection, or GPU
open fails acceptance. R090.3 resource benchmarking remains separate.
"""

from __future__ import annotations

import contextlib
import shutil
from pathlib import Path

import pytest
from _decision_resident_fixture import fit_and_publish, scores
from conftest import (
    TEST_AGENT_ID,
    TEST_SIGNER_KEY,
    bootstrap_context,
    find_server_binary,
    request_context,
    spawn_server,
    stop_server,
    strict_server_env,
    wait_for_server,
)

from epistemic_graph.client import SyncEpistemicGraphClient

SECRET = "resident-scorer-fixture-secret"


@contextlib.contextmanager
def engine(tmp_path, run, *, bootstrap):
    binary = find_server_binary()
    assert binary, (
        "supply a prebuilt full EPISTEMIC_GRAPH_TEST_BINARY; no cold build is attempted"
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
        EPISTEMIC_GRAPH_ENCRYPTION_KEY="resident-test-encryption-key",
        CUDA_VISIBLE_DEVICES="",
        HIP_VISIBLE_DEVICES="",
        ROCR_VISIBLE_DEVICES="",
    )
    trace = tmp_path / f"{run}.trace"
    log = tmp_path / f"{run}.log"
    # -D leaves the engine as our child; bounded stop_server terminates it,
    # while the detached tracer follows all engine threads and subprocesses.
    process = spawn_server(
        [
            tracer,
            "-D",
            "-f",
            "-s",
            "4096",
            "-o",
            str(trace),
            "-e",
            "trace=execve,execveat,connect,openat",
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
            with SyncEpistemicGraphClient.connect(
                socket_path=socket,
                auth_secret=SECRET,
                verified_context=bootstrap_context(),
            ) as client:
                client.consensus.bootstrap_system_identity(
                    agent_id=TEST_AGENT_ID,
                    signer_id=TEST_AGENT_ID,
                    signer_key=TEST_SIGNER_KEY,
                )
        with SyncEpistemicGraphClient.connect(
            socket_path=socket, auth_secret=SECRET, verified_context=request_context()
        ) as client:
            yield client
    finally:
        stop_server(process, name="resident engine", log_path=log)
    assert_no_external_runtime(trace.read_text())


def assert_no_external_runtime(trace):
    lines = trace.splitlines()
    executions = [line for line in lines if "execve(" in line or "execveat(" in line]
    assert len(executions) == 1, f"only the compiled engine may execute: {executions}"
    assert "epistemic-graph-server" in executions[0]
    assert not any("connect(" in line for line in lines), (
        "engine attempted an outgoing IPC/network connection"
    )
    assert not any(
        device in trace for device in ("/dev/nvidia", "/dev/dri/", "/dev/kfd")
    ), "engine accessed a GPU"


@pytest.mark.exact_artifact
@pytest.mark.spec("EG-DECISION-ENGINE-R090.2")
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

    from epistemic_graph.generated import models

    schema = {
        "component_id": "schema",
        "kind": "feature_schema",
        "definition_digest": DIGEST,
    }
    data = dataset({"candidate_ids": ["a", "b", "c"], "values": [1, 2, 3, 4, 5, 6]})
    payloads = [
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
    ],
)
def test_runtime_trace_rejects_external_dependency_attempts(violation):
    trace = '1 execve("/tmp/epistemic-graph-server", [], []) = 0\n'
    assert_no_external_runtime(trace)
    with pytest.raises(AssertionError):
        assert_no_external_runtime(trace + violation)
