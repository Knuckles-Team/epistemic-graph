"""Tests for the measurement harness lite twin (``scripts/measurement_lite.py``).

Mirrors a subset of the incident proofs in `agent-utilities`'
`agent_utilities/measurement/` test suite -- the subset this repo's own
gates actually use. Each test names the incident it reproduces.

Pure static/subprocess/git/systemd-run tests -- never touches the native
`epistemic-graph` engine -- so the whole module is marked ``no_engine``
(see ``tests/conftest.py``'s ``start_epistemic_graph_server`` fixture) to
keep a `pytest tests/test_measurement_lite.py` run from triggering a full
`cargo build --features full` of the shared engine.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from measurement_lite import (  # noqa: E402
    TOO_LOADED_TO_MEASURE,
    TooLoadedToMeasureError,
    check_load,
    env_fingerprint,
    files_deleted_by_merge,
    gate_or_raise,
    poll,
    run,
    run_background,
)


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True
    )


# --- B: load gating (incident 4 / danger-zone note) -------------------------


def test_incident_4_high_load_refused_not_reported(monkeypatch):
    monkeypatch.setattr("os.getloadavg", lambda: (15.82, 12.0, 9.0))
    result = check_load(threshold=10.0)
    assert result.status == TOO_LOADED_TO_MEASURE
    assert not result.ok


def test_incident_4_low_load_allowed(monkeypatch):
    monkeypatch.setattr("os.getloadavg", lambda: (4.03, 3.5, 3.0))
    result = check_load(threshold=10.0)
    assert result.ok


def test_gate_or_raise_aborts_with_exception_not_a_falsy_pass(monkeypatch):
    monkeypatch.setattr("os.getloadavg", lambda: (62.0, 58.0, 40.0))
    with pytest.raises(TooLoadedToMeasureError):
        gate_or_raise(threshold=36.0)


# --- sccache/RUSTC_WRAPPER incident (7) -- this repo's own incident --------


def test_incident_7_env_fingerprint_reveals_rustc_wrapper_drift(monkeypatch):
    monkeypatch.setenv("RUSTC_WRAPPER", "sccache")
    local = env_fingerprint()
    monkeypatch.delenv("RUSTC_WRAPPER", raising=False)
    ci = env_fingerprint()
    assert local["RUSTC_WRAPPER"] == "sccache"
    assert ci["RUSTC_WRAPPER"] is None
    assert local != ci


# --- D: exit-code correctness (incident 1) ----------------------------------


def test_incident_1_run_captures_real_exit_code_not_a_pipeline_stage():
    result = run([sys.executable, "-c", "import sys; sys.exit(17)"])
    assert result.returncode == 17
    assert not result.ok


def test_run_rejects_shell_string():
    with pytest.raises(TypeError):
        run("cargo test | tail -25")  # type: ignore[arg-type]


# --- E: merged-tree helper (incident 2) -------------------------------------


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q", "-b", "main")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "t")
    (r / "A.rs").write_text("// a\n")
    _git(r, "add", "A.rs")
    _git(r, "commit", "-q", "-m", "init")
    _git(r, "branch", "feature")
    (r / "B.rs").write_text("// b\n")
    _git(r, "add", "B.rs")
    _git(r, "commit", "-q", "-m", "main gains B.rs")
    _git(r, "checkout", "-q", "feature")
    (r / "C.rs").write_text("// c\n")
    _git(r, "add", "C.rs")
    _git(r, "commit", "-q", "-m", "feature adds C.rs")
    _git(r, "checkout", "-q", "main")
    return r


def test_incident_2_merge_tree_does_not_falsely_report_a_deletion(repo: Path):
    naive = _git(
        repo, "diff", "--diff-filter=D", "--name-only", "main..feature"
    ).stdout.split()
    assert "B.rs" in naive, "the naive two-dot diff must show the false alarm first"

    real_deletions = files_deleted_by_merge(repo, "main", "feature")
    assert "B.rs" not in real_deletions
    assert real_deletions == set()


def test_merge_tree_still_catches_a_real_deletion(repo: Path):
    _git(repo, "checkout", "-q", "-b", "deleter", "feature")
    (repo / "A.rs").unlink()
    _git(repo, "add", "A.rs")
    _git(repo, "commit", "-q", "-m", "actually delete A.rs")
    assert files_deleted_by_merge(repo, "main", "deleter") == {"A.rs"}


# --- G: background-run wrapper (journal-vs-file incident) -------------------


def test_background_redirect_wraps_the_whole_multi_stage_command(monkeypatch, tmp_path):
    captured = {}

    class FakeCompleted:
        returncode = 0

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        return FakeCompleted()

    monkeypatch.setattr("measurement_lite.subprocess.run", fake_run)
    result = run_background(
        "echo one && echo two", unit_name="eg-test-unit", log_dir=tmp_path
    )
    inner_cmd = captured["argv"][-1]
    assert inner_cmd.startswith("{ echo one && echo two ; } >")
    assert str(result.log_path) in inner_cmd


@pytest.mark.skipif(
    __import__("shutil").which("systemd-run") is None, reason="no systemd-run on PATH"
)
def test_background_end_to_end_lands_in_file(tmp_path):
    unit = f"eg-measurement-lite-test-{int(time.time())}"
    result = run_background(
        "echo eg-canary && sleep 0.2", unit_name=unit, log_dir=tmp_path
    )
    assert result.launch_returncode == 0
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        status = subprocess.run(
            ["systemctl", "--user", "is-active", unit], capture_output=True, text=True
        ).stdout.strip()
        if status != "active":
            break
        time.sleep(0.2)
    assert "eg-canary" in poll(result.log_path)


def test_poll_missing_file_returns_empty():
    assert poll(Path("/nonexistent/path/does-not-exist.log")) == ""
