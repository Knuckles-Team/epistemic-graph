"""Contract tests for scripts/check_p2_analytics_reasoning_architecture.py.

Two holes this file closes, both of the "a gate reports more coverage than it
has" class:

1. The publication-barrier assertion used to slice the job store between
   ``pub fn succeed`` and ``pub fn fail``. ``succeed`` has not existed since
   eg-jobs was split, so ``str.find`` returned -1, the slice was EMPTY, and the
   assertion could never fire — a permissive ``Running -> Succeeded`` arm passed
   the gate. Every offset-based contract now treats an absent marker as fatal,
   and the tests below plant the exact known-bad that used to slip through.

2. ``_REQUIRED_MARKERS`` is a table. Deleting a whole row would silently stop
   checking a source without failing anything, so its shape is pinned here.

These are pure-text tests against the gate's own helpers: they never read the
repository tree, so they cost milliseconds rather than a module-tree expansion.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Pure/static test -- never needs the shared native engine.
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _gate():
    path = ROOT / "scripts" / "check_p2_analytics_reasoning_architecture.py"
    spec = importlib.util.spec_from_file_location("eg_p2_analytics_gate", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


#: A job store whose two publication-completion methods both require Publishing.
_CLEAN_STORE = """
impl JobStore {
    pub fn complete_publication_fenced(&self, job_id: &str) -> Result<AnalyticsJob> {
        let (result_ref, checkpoint) = match &job.state {
            JobState::Publishing { result_ref, checkpoint } => (result_ref, checkpoint),
            other => return Err(invalid_transition(other)),
        };
        Ok(job)
    }

    /// A doc comment that mentions JobState::Running { checkpoint } on purpose:
    /// it belongs to the NEXT method and must never be read as this one's body.
    pub fn complete_publication_prepared(&self, job_id: &str) -> Result<AnalyticsJob> {
        match &job.state {
            JobState::Publishing { .. } => {}
            _ => return Err(invalid_transition(&job, "requires Publishing")),
        }
        Ok(job)
    }
}
"""


def test_clean_publication_barrier_reports_nothing() -> None:
    gate = _gate()
    assert gate._publication_barrier_failures(_CLEAN_STORE) == []


@pytest.mark.parametrize(
    "signature",
    [
        "pub fn complete_publication_fenced(",
        "pub fn complete_publication_prepared(",
    ],
)
def test_permissive_running_arm_fails_the_barrier(signature: str) -> None:
    gate = _gate()
    permissive = _CLEAN_STORE.replace(
        signature + "&self, job_id: &str) -> Result<AnalyticsJob> {",
        signature + "&self, job_id: &str) -> Result<AnalyticsJob> {\n"
        "        let x = match &job.state {\n"
        "            JobState::Running { checkpoint } => checkpoint,\n"
        "        };",
        1,
    )
    assert permissive != _CLEAN_STORE
    failures = gate._publication_barrier_failures(permissive)
    assert len(failures) == 1
    assert "still permits Running -> Succeeded" in failures[0]


@pytest.mark.parametrize(
    "signature",
    [
        "pub fn complete_publication_fenced(",
        "pub fn complete_publication_prepared(",
    ],
)
def test_a_missing_completion_entry_point_is_fatal(signature: str) -> None:
    """An absent symbol must FAIL, never leave an empty slice that cannot fire."""
    gate = _gate()
    renamed = _CLEAN_STORE.replace(signature, "pub fn renamed_away(", 1)
    failures = gate._publication_barrier_failures(renamed)
    assert len(failures) == 1
    assert "cannot locate" in failures[0]
    assert "publication barrier cannot be checked" in failures[0]


def test_ordered_markers_accepts_the_declared_order() -> None:
    gate = _gate()
    assert (
        gate._ordered_markers("first() second()", "p.rs", "first(", "second(", "V")
        == []
    )


def test_ordered_markers_reports_a_swapped_order() -> None:
    gate = _gate()
    assert gate._ordered_markers(
        "second() first()", "p.rs", "first(", "second(", "V"
    ) == ["V"]


@pytest.mark.parametrize("present", ["first(", "second("])
def test_ordered_markers_fails_closed_on_an_absent_marker(present: str) -> None:
    """`str.find` returns -1 for an absent marker and would make the comparison
    vacuous; the gate must report instead of silently passing."""
    gate = _gate()
    failures = gate._ordered_markers(present, "p.rs", "first(", "second(", "V")
    assert len(failures) == 1
    assert "ordering contract" in failures[0]


def test_required_marker_table_shape_is_pinned() -> None:
    """Deleting a whole row would silently stop checking a source."""
    gate = _gate()
    table = gate._REQUIRED_MARKERS
    assert len(table) == 11, f"marker table row count changed: {sorted(table)}"
    assert sum(len(markers) for markers in table.values()) == 82
    assert sorted(table) == sorted(
        [
            "Cargo.toml",
            "crates/eg-epistemic/src/incremental.rs",
            "crates/eg-jobs/src/model.rs",
            "crates/eg-jobs/src/store.rs",
            "crates/eg-types/src/jobs.rs",
            "src/raft/mod.rs",
            "src/server/authority_context.rs",
            "src/server/dispatch.rs",
            "src/server/handlers/jobs.rs",
            "src/server/handlers/query.rs",
            "src/server/reasoning_projection.rs",
        ]
    )
    assert all(markers and all(markers) for markers in table.values())
