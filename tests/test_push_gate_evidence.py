"""Focused admissibility fixtures for the pre-push evidence contract.

These tests exercise only the bounded, pure predicate.  They intentionally do
not invoke Cargo, a workflow, or the private on-disk cache; the heavy producer
and all mandatory workflow coverage remain owned by the parent gate.
"""

from __future__ import annotations

from scripts.push_gate_evidence import (
    EvidenceStore,
    Selection,
    SUBSET_PROOFS,
    _digest,
)
import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


def _success(selection: Selection) -> dict[str, object]:
    return {
        "status": "success",
        "exitCode": 0,
        "elapsedSeconds": 1.0,
        "resultDigest": _digest(
            {
                "selection": selection.payload(),
                "exitCode": 0,
                "status": "success",
            }
        ),
    }


def _document(selection: Selection, *, status: str = "complete") -> dict[str, object]:
    key = selection.selection_digest
    return {
        "status": status,
        "plan": {key: selection.payload()},
        "results": {key: _success(selection)},
    }


def test_exact_reuse_requires_complete_success_and_identical_selection() -> None:
    selection = Selection.from_argv(
        "fixture-exact",
        ["cargo", "test", "-p", "eg-core", "--all-features"],
        kind="cargo",
        environment={"CARGO_TARGET_DIR": "/var/tmp/eg", "CARGO_BUILD_JOBS": "2"},
    )
    document = _document(selection)

    assert EvidenceStore._admissible(document, selection)
    assert not EvidenceStore._admissible(
        _document(selection, status="running"), selection
    )

    failed = _document(selection)
    failed["results"][selection.selection_digest]["status"] = "failed"  # type: ignore[index]
    assert not EvidenceStore._admissible(failed, selection)

    tampered = _document(selection)
    tampered["results"][selection.selection_digest]["resultDigest"] = (
        "sha256:tampered"
    )  # type: ignore[index]
    assert not EvidenceStore._admissible(tampered, selection)

    different_environment = Selection.from_argv(
        "fixture-exact",
        selection.argv,
        kind="cargo",
        environment={"CARGO_TARGET_DIR": "/var/tmp/other", "CARGO_BUILD_JOBS": "2"},
    )
    assert not EvidenceStore._admissible(document, different_environment)


def test_subset_reuse_is_only_the_declared_clippy_proof() -> None:
    proof = SUBSET_PROOFS["cargo-clippy-full"]
    environment = {"CARGO_TARGET_DIR": "/var/tmp/eg", "CARGO_BUILD_JOBS": "2"}
    requested = Selection.from_argv(
        "cargo-clippy-full",
        proof["requested_argv"],
        kind="cargo",
        environment=environment,
    )
    provider = Selection.from_argv(
        "advisory-provider",
        proof["provider_argv"],
        kind="cargo",
        environment=environment,
    )
    document = _document(provider)

    assert EvidenceStore._admissible(document, requested)

    unrelated = Selection.from_argv(
        "unapproved-subset",
        proof["requested_argv"],
        kind="cargo",
        environment=environment,
    )
    assert not EvidenceStore._admissible(document, unrelated)
