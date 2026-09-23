"""EH-376: a signal-killed `cargo test` binary gets per-test verdicts (nextest).

`scripts/cargo_test_rescue.py` wraps every `cargo test` step of the release
workflow (so hosted CI, the landing gate and local runs share it). These tests
drive it with a fake `cargo` on PATH that prints cargo's real crash output and
records how nextest was invoked -- no Rust build.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "cargo_test_rescue.py"
pytestmark = pytest.mark.no_engine

# Verbatim shape of cargo's output when a libtest binary dies on SIGABRT
# (235fffb28, `cargo test -p epistemic-graph --features full`), with the colour
# CI's CARGO_TERM_COLOR=always adds.
ERROR = "\x1b[1m\x1b[91merror\x1b[0m: test failed, to rerun pass "
LIB_CRASH = (
    "thread 'server::bolt_wire::tests::bolt_atomic' (1) has overflowed its stack\n"
    f"{ERROR}`-p epistemic-graph --lib`\n\nCaused by:\n"
    "  process didn't exit successfully: `/t/deps/epistemic_graph-1d4e` "
    "(signal: 6, SIGABRT: process abort signal)\n"
)
IT_FAILURE = (
    f"{ERROR}`-p epistemic-graph --test graphql_crossmodal_durable`\n\n"
    "Caused by:\n"
    "  process didn't exit successfully: `/t/deps/graphql_crossmodal_durable-9f` "
    "(exit status: 101)\n"
)
CRASH = "running 1750 tests\n" + LIB_CRASH + IT_FAILURE

NEXTEST = """        PASS [   0.030s] (   1/3) epistemic-graph server::a::ok_test
     SIGABRT [  11.700s] (   2/3) epistemic-graph server::bolt_wire::tests::bolt_atomic
        FAIL [   0.040s] (   3/3) epistemic-graph server::c::bad
     Summary [  12.000s] 3 tests run: 1 passed, 2 failed, 0 skipped
     SIGABRT [  11.700s] (   2/3) epistemic-graph server::bolt_wire::tests::bolt_atomic
        FAIL [   0.040s] (   3/3) epistemic-graph server::c::bad
"""


def _load():
    spec = importlib.util.spec_from_file_location("cargo_test_rescue", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fake_cargo(tmp_path):
    """A `cargo` whose `test` output and exit code are chosen per test."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (tmp_path / "nextest.out").write_text(NEXTEST, encoding="utf-8")
    script = bindir / "cargo"
    script.write_text(
        "#!/bin/sh\n"
        'echo "$*" >> "$FAKE_LOG"\n'
        'if [ "$1" = nextest ]; then cat "$FAKE_DIR/nextest.out"; exit 100; fi\n'
        'cat "$FAKE_DIR/test.out"; exit "$(cat "$FAKE_DIR/test.rc")"\n',
        encoding="utf-8",
    )
    script.chmod(0o755)
    nextest = bindir / "cargo-nextest"
    nextest.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    nextest.chmod(0o755)

    def run(test_output: str, test_rc: int, *command: str):
        (tmp_path / "test.out").write_text(test_output, encoding="utf-8")
        (tmp_path / "test.rc").write_text(str(test_rc), encoding="utf-8")
        log = tmp_path / "calls.log"
        log.write_text("", encoding="utf-8")
        env = dict(
            os.environ,
            PATH=f"{bindir}{os.pathsep}{os.environ['PATH']}",
            FAKE_LOG=str(log),
            FAKE_DIR=str(tmp_path),
        )
        env.pop("CI", None)
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), *command],
            capture_output=True,
            text=True,
            env=env,
        )
        return proc, log.read_text(encoding="utf-8").splitlines()

    return run


FACADE = ("cargo", "test", "-p", "epistemic-graph", "--features", "full")


def test_green_run_costs_nothing_extra(fake_cargo):
    proc, calls = fake_cargo("test result: ok.\n", 0, *FACADE, "--no-fail-fast")
    assert proc.returncode == 0
    assert calls == ["test -p epistemic-graph --features full --no-fail-fast"]


def test_ordinary_failure_is_not_rerun(fake_cargo):
    proc, calls = fake_cargo(IT_FAILURE, 101, *FACADE, "--no-fail-fast")
    assert proc.returncode == 101
    assert len(calls) == 1
    assert "cargo-test-rescue" not in proc.stdout


def test_signal_killed_binary_gets_per_test_verdicts_and_still_fails(fake_cargo):
    proc, calls = fake_cargo(CRASH, 101, *FACADE, "--no-fail-fast")
    assert proc.returncode == 101
    # Only the SIGABRT target is re-run; the exit-101 target already has verdicts.
    assert calls[1:] == [
        "nextest run --features full -p epistemic-graph --lib --no-fail-fast"
    ]
    assert (
        "RESCUE-VERDICT SIGABRT  epistemic-graph server::bolt_wire::tests::bolt_atomic"
        in proc.stdout
    )
    assert "RESCUE-VERDICT FAIL     epistemic-graph server::c::bad" in proc.stdout
    assert "3 test(s) with a verdict, 1 passed, 2 failed" in proc.stdout
    assert "gate FAILS: 1 target(s) died on a signal" in proc.stdout


def test_filters_and_libtest_flags_carry_over_to_nextest():
    rescue = _load()
    selection = rescue.parse_selection(
        [
            *FACADE,
            "--no-fail-fast",
            "bounded_memory",
            "--",
            "--ignored",
            "--test-threads=1",
        ]
    )
    assert rescue.nextest_command(selection, "-p epistemic-graph --lib") == [
        "cargo",
        "nextest",
        "run",
        "--features",
        "full",
        "-p",
        "epistemic-graph",
        "--lib",
        "--run-ignored",
        "only",
        "--test-threads",
        "1",
        "--no-fail-fast",
        "bounded_memory",
    ]
    raft = rescue.parse_selection(
        [
            "cargo",
            "test",
            "--locked",
            "-p",
            "epistemic-graph",
            "--features",
            "cluster,harness,calvin",
            "--lib",
            "--no-fail-fast",
            "--",
            "raft::",
        ]
    )
    assert raft.filters == ("raft::",) and "--lib" not in raft.cargo_flags
    exact = rescue.parse_selection([*FACADE, "--", "a::b", "c::d", "--exact"])
    assert rescue.nextest_command(exact, "-p epistemic-graph --lib")[-2:] == [
        "-E",
        "test(=a::b) | test(=c::d)",
    ]


def test_untranslatable_libtest_argument_fails_closed():
    rescue = _load()
    with pytest.raises(rescue.RescueError, match="--format"):
        rescue.parse_selection([*FACADE, "--", "--format", "json"])
    with pytest.raises(rescue.RescueError):
        rescue.parse_selection(["cargo", "build"])


def test_every_release_cargo_test_step_goes_through_the_rescue():
    import yaml

    doc = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    bare = [
        (job_id, step.get("name"))
        for job_id, job in doc["jobs"].items()
        for step in (job or {}).get("steps", []) or []
        for line in str(step.get("run", "")).splitlines()
        if line.strip().startswith("cargo test")
    ]
    assert bare == [], "wrap with `python3 scripts/cargo_test_rescue.py cargo test`"
