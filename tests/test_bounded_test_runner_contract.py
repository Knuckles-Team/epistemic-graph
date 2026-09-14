"""Static contract checks for the bounded all-feature test lifecycle.

These checks intentionally inspect source only.  They do not start cargo,
spawn a process, send a signal, or exercise the full feature matrix; the R820
operator run owns that one expensive validation.  Keeping the lifecycle
invariants here prevents a future edit from silently restoring an unbounded
runner or an unscoped process kill.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "bounded_test_runner.py"
ALL_FEATURE_GATE = ROOT / "scripts" / "all_features_validation_gate.sh"
CONSTRAINED_GATE = ROOT / "scripts" / "constrained_parallelism_gate.sh"


def test_runner_has_bounded_attribution_and_containment_contract():
    source = RUNNER.read_text(encoding="utf-8")
    for required in (
        "start_new_session=True",
        "os.getpgid",
        "os.killpg",
        "signal.SIGTERM",
        "signal.SIGKILL",
        "suite_timeout_seconds",
        "test_timeout_seconds",
        '"Threads:"',
        '"/proc/{pid}/fd"',
        "MAX_DIAGNOSTIC_FDS",
        "fd_count_capped",
        'os.scandir("/proc")',
        '"timeout_detected"',
        '"term_sent"',
        '"kill_sent"',
        '"reaped"',
        '"containment_incomplete"',
        '"returncode"',
        '"test_elapsed_s"',
        "partial_progress_name",
        "_observe_partial_progress",
    ):
        assert required in source, f"lifecycle contract token missing: {required}"


def test_r820_gate_preserves_all_feature_selection_and_no_fail_fast():
    source = ALL_FEATURE_GATE.read_text(encoding="utf-8")
    assert "bounded_test_runner.py" in source
    assert "--workspace --all-features --no-fail-fast" in source
    assert "r820-all-features-workspace" in source
    assert "EG_ALL_FEATURE_SUITE_TIMEOUT" in source
    assert "EG_ALL_FEATURE_TEST_TIMEOUT" in source


def test_constrained_gate_routes_every_test_phase_through_runner():
    source = CONSTRAINED_GATE.read_text(encoding="utf-8")
    assert "bounded_test() {" in source
    assert source.count("bounded_test ") >= 4
    assert "timeout -k" not in source
    assert "EG_CONSTRAINED_TEST_TIMEOUT" in source
    assert "EG_CONSTRAINED_TERM_GRACE" in source
    assert "EG_CONSTRAINED_KILL_GRACE" in source


def test_constrained_gate_kafka_proofs_are_exact_and_short_bounded():
    source = CONSTRAINED_GATE.read_text(encoding="utf-8")
    assert "sink::tests::raw_kafka_producer_publish_only_enqueues_locally" in source
    assert "sink::tests::kafka_cdc_sink_emit_only_enqueues_locally" in source
    assert "KAFKA_TEST_TIMEOUT_SECS=10" in source
    assert "bounded_kafka_test() {" in source
    assert '--test-timeout "$KAFKA_TEST_TIMEOUT_SECS"' in source
