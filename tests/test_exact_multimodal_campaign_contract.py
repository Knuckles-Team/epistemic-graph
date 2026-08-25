"""Static contract tests for the G-14 exact multimodal campaign."""

from __future__ import annotations

import ast
from pathlib import Path
import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "scripts" / "certify_exact_multimodal.py"
WRAPPER = ROOT / "tests" / "test_exact_release_campaigns.py"


def _literal(tree: ast.AST, name: str) -> object:
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"missing literal {name}")


def test_multimodal_campaign_inventory_is_current_and_complete() -> None:
    source = HARNESS.read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert _literal(tree, "MODALITIES") == ("document", "image", "audio", "video")
    assert _literal(tree, "EXACT_BEHAVIOR_DIMENSIONS") == (
        "artifact_identity_and_exact_round_trip",
        "atomic_stream_ingest_replay_and_delete",
        "native_storage_index_and_stats",
        "restart_restore_migration_and_index_backfill",
        "typed_query_selectivity_and_paging",
        "fault_atomicity_and_durable_event_outbox",
        "tenant_classification_and_authority",
        "provenance_evidence_and_lineage",
        "lifecycle_retention_and_tombstone_collection",
        "malformed_input_rejection",
        "per_modality_resource_bounds",
        "four_modality_performance_binding",
    )
    assert '"component_pass": 12' in source
    assert '"component_not_applicable": 0' in source
    assert "tenant=SECOND_TENANT" in source
    assert "lazy_page_size=1" in source
    assert "raw_modality_source_was_persisted" in source
    assert "ingest_stream" in source
    assert "collect_tombstones" in source
    assert "FAULT_PHASES" in source


def test_multimodal_campaign_binds_only_the_supplied_artifact() -> None:
    source = HARNESS.read_text(encoding="utf-8")

    for required in (
        'parser.add_argument("--binary", required=True)',
        'parser.add_argument("--binary-sha256", required=True)',
        'parser.add_argument("--performance-evidence", required=True)',
        'parser.add_argument("--performance-evidence-sha256", required=True)',
        "_validate_binary(args.binary, args.binary_sha256)",
        '"sealed_copy_verified": True',
        "_load_performance_evidence(",
        "evidence_destination_must_be_new",
    ):
        assert required in source
    for forbidden in (
        "cargo build",
        "cargo run",
        "target/debug",
        "target/release",
        "resolve_engine_binary",
        "shutil.which",
    ):
        assert forbidden not in source


def test_serial_exact_wrapper_requires_multimodal_summary() -> None:
    source = WRAPPER.read_text(encoding="utf-8")

    assert "certify_exact_multimodal.py" in source
    assert '"modalities": 4' in source
    assert '"dimensions_per_modality": 12' in source
    assert '"fault_cases": 16' in source
    assert "EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE" in source
    assert "EPISTEMIC_GRAPH_PERFORMANCE_EVIDENCE_SHA256" in source
