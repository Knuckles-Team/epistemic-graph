"""Static ownership checks for the native WorkItem/development-lane routes."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HANDLERS = ROOT / "src" / "server" / "handlers"

pytestmark = pytest.mark.no_engine

WORK_ITEM_METHODS = {
    "ClaimWorkItem",
    "RenewWorkItemLease",
    "CommitWorkItemResult",
    "CancelWorkItem",
    "DeferWorkItem",
    "CasWorkItemMetadata",
}
DEVELOPMENT_LANE_WRITES = {
    "ReserveDevelopmentLane",
    "RenewDevelopmentLane",
    "ObserveDevelopmentLane",
    "FinishDevelopmentLane",
    "CleanupDevelopmentLane",
    "UpdateDevelopmentLaneQuota",
}
DEVELOPMENT_LANE_READS = {"QueryDevelopmentLane", "DevelopmentLaneStatus"}


def _owned_methods(path: Path) -> set[str]:
    return set(re.findall(r"Method::([A-Za-z0-9_]+)", path.read_text()))


def test_work_item_handler_explicitly_owns_six_transitions() -> None:
    source = (HANDLERS / "work_item.rs").read_text()
    assert _owned_methods(HANDLERS / "work_item.rs") == WORK_ITEM_METHODS
    assert "mutation_batch::commit_work_item" in source
    assert "_ =>" not in source
    assert source.count("other => return Err(other)") == 1


def test_development_lane_handler_has_six_explicit_writes_and_two_reads() -> None:
    source = (HANDLERS / "development_lane.rs").read_text()
    assert _owned_methods(HANDLERS / "development_lane.rs") == (
        DEVELOPMENT_LANE_WRITES | DEVELOPMENT_LANE_READS
    )
    assert "commit_development_lane" in source
    assert "_ =>" not in source
    assert source.count("other => return Err(other)") == 1


def test_dispatch_routes_to_handlers_without_domain_classifiers() -> None:
    split_pipeline = ROOT / "src" / "server" / "dispatch" / "graph_pipeline.rs"
    dispatch_path = (
        split_pipeline
        if split_pipeline.exists()
        else ROOT / "src" / "server" / "dispatch.rs"
    )
    dispatch = dispatch_path.read_text()
    # The native WorkItem/DevelopmentLane routes live in the pipeline's
    # declared child modules (e.g. `graph_pipeline/native_routes.rs`).
    if dispatch_path == split_pipeline:
        dispatch += "\n".join(
            path.read_text()
            for path in sorted((split_pipeline.parent / "graph_pipeline").glob("*.rs"))
        )
    split_governance = (
        ROOT / "src" / "server" / "dispatch" / "graph_pipeline" / "work_governance.rs"
    )
    routing_sources = dispatch
    if split_governance.exists():
        routing_sources += split_governance.read_text()
    mutation_batch_paths = [ROOT / "src" / "server" / "mutation_batch.rs"]
    mutation_batch_paths.extend(
        sorted((ROOT / "src" / "server" / "mutation_batch").glob("*.rs"))
    )
    mutation_batch = "\n".join(path.read_text() for path in mutation_batch_paths)
    handler_modules = (HANDLERS / "mod.rs").read_text()

    assert "handlers::work_item::try_handle" in dispatch
    assert "handlers::development_lane::try_handle" in dispatch
    assert "is_work_item_mutation_method(&method)" not in dispatch
    assert "is_development_lane_method" not in dispatch
    assert "dispatch_op_workitem_mutation" not in routing_sources
    assert "dispatch_op_development_lane" not in routing_sources
    assert "is_development_lane_method" not in mutation_batch
    assert "mod work_item;" in handler_modules
    assert "mod development_lane;" in handler_modules


def test_deleted_lane_classifier_is_absent_from_authority_evidence() -> None:
    evidence_paths = (
        ROOT / "src" / "raft" / "mod.rs",
        ROOT / "src" / "server" / "access.rs",
        ROOT / "src" / "server" / "mutation.rs",
        ROOT / "crates" / "eg-capabilities" / "tests" / "consistency" / "divergence.rs",
    )
    for path in evidence_paths:
        assert "is_development_lane_method" not in path.read_text(), path
