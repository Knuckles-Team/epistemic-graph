"""Tests for two ``scripts/push_gate_evidence.py`` internals that the existing
``test_push_gate_evidence.py`` suite deliberately does not cover:

  * ``_extract_cargo`` -- the cargo argv token classifier, now decomposed into
    ``_CARGO_PAIR_FLAGS``/``_append_cargo_pair_value``. Exercised only
    indirectly (via ``Selection.from_argv``) by the existing suite, and never
    asserted on there.
  * ``EvidenceStore.begin_or_resume`` -- the existing suite's own docstring
    says it "intentionally does not invoke ... the private on-disk cache";
    now decomposed into ``_ensure_cache_directory``,
    ``_marker_matches_invocation``, ``EvidenceStore._resume_from_marker``, and
    ``EvidenceStore._start_new_invocation``. These tests isolate it from the
    real (shared, multi-worktree) repository cache by monkeypatching
    ``_git_directory`` to a private ``tmp_path``, so they never touch the
    shared ``.git`` common directory other concurrent lanes use.
"""

from __future__ import annotations

import json
from pathlib import Path

import push_gate_evidence
import pytest
from push_gate_evidence import EvidenceStore, _extract_cargo

# Pure/static tests (the EvidenceStore fixtures below isolate the private
# cache to tmp_path) -- never needs the shared native engine.
pytestmark = pytest.mark.no_engine


# ── _extract_cargo ──────────────────────────────────────────────────────────


def test_extract_cargo_short_and_long_package_flags():
    packages, _, _ = _extract_cargo(["-p", "eg-core", "--package", "eg-server"])
    assert packages == ("eg-core", "eg-server")


def test_extract_cargo_package_equals_form():
    packages, _, _ = _extract_cargo(["--package=eg-core"])
    assert packages == ("eg-core",)


def test_extract_cargo_features_split_on_comma_and_space():
    _, features, _ = _extract_cargo(["--features", "a,b c"])
    assert features == ("a", "b", "c")


def test_extract_cargo_features_equals_form_and_short_flag():
    _, features, _ = _extract_cargo(["--features=x,y", "-F", "z"])
    assert features == ("x", "y", "z")


def test_extract_cargo_target_test_bin_pair_forms_keep_flag_prefix():
    _, _, targets = _extract_cargo(
        ["--target", "x86_64-unknown-linux-gnu", "--test", "smoke", "--bin", "eg"]
    )
    assert targets == (
        "--target=x86_64-unknown-linux-gnu",
        "--test=smoke",
        "--bin=eg",
    )


def test_extract_cargo_target_test_bin_equals_forms_pass_through_whole_token():
    _, _, targets = _extract_cargo(
        ["--target=x86_64-unknown-linux-gnu", "--test=smoke", "--bin=eg"]
    )
    assert targets == (
        "--target=x86_64-unknown-linux-gnu",
        "--test=smoke",
        "--bin=eg",
    )


def test_extract_cargo_trailing_flag_with_no_value_is_ignored():
    # "-p" is the last token -- no next value exists, so it contributes
    # nothing (this is the original loop's "index + 1 < len(argv)" guard).
    packages, features, targets = _extract_cargo(["cargo", "test", "-p"])
    assert packages == ()
    assert features == ()
    assert targets == ()


def test_extract_cargo_packages_are_sorted_features_sorted_targets_ordered():
    packages, features, _ = _extract_cargo(
        ["-p", "zeta", "-p", "alpha", "--features", "zz aa"]
    )
    assert packages == ("alpha", "zeta")
    assert features == ("aa", "zz")


def test_extract_cargo_unrelated_tokens_are_ignored():
    packages, features, targets = _extract_cargo(["cargo", "build", "--release"])
    assert (packages, features, targets) == ((), (), ())


# ── EvidenceStore.begin_or_resume ───────────────────────────────────────────


@pytest.fixture
def isolated_git_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the private evidence cache at a throwaway directory instead of
    this (shared, multi-worktree) repository's real `.git` common dir."""

    fake_git_dir = tmp_path / "fake-gitdir"
    fake_git_dir.mkdir()
    monkeypatch.setattr(push_gate_evidence, "_git_directory", lambda: fake_git_dir)
    return fake_git_dir


def test_begin_or_resume_starts_a_fresh_invocation(isolated_git_directory: Path):
    store = EvidenceStore.begin_or_resume()

    cache_dir = isolated_git_directory / push_gate_evidence.CACHE_DIRECTORY
    assert store.directory == cache_dir
    assert store.evidence_path.exists()
    marker = json.loads((cache_dir / "current.json").read_text(encoding="utf-8"))
    assert marker["invocationId"] == store.invocation_id
    evidence = json.loads(store.evidence_path.read_text(encoding="utf-8"))
    assert evidence["status"] == "running"


def test_begin_or_resume_marker_matches_but_resume_falls_through_on_bad_evidence(
    isolated_git_directory: Path,
):
    """Pins EXISTING behavior, not desired behavior.

    ``_write_evidence``'s digest is computed over the document BEFORE
    ``contentDigest`` is added, but ``_verify_document`` recomputes it over
    the document AFTER, via ``_signed_core`` (which strips only
    ``signature``, not ``contentDigest``) -- so ``_load_evidence`` fails
    self-verification on every store, even one that just wrote its own
    evidence and never resumed at all. That pre-existing mismatch (in
    ``_write_evidence``/``_verify_document``/``_signed_core`` -- none of
    which this lane's decomposition touches) is confirmed present in the
    unrefactored code at HEAD too, so ``begin_or_resume`` always falls
    through to ``_start_new_invocation`` in practice. This test exists so a
    change that accidentally starts making resume succeed -- or fail for a
    NEW reason -- shows up here rather than only in the marker-matching
    logic this lane's decomposition (``_marker_matches_invocation``) exists
    to prove.
    """

    first = EvidenceStore.begin_or_resume()

    cache_dir = isolated_git_directory / push_gate_evidence.CACHE_DIRECTORY
    marker = json.loads((cache_dir / "current.json").read_text(encoding="utf-8"))
    assert push_gate_evidence._marker_matches_invocation(
        marker,
        parent_identity=first.parent_identity,
        source=first.source,
        context=first.context,
    ), "marker should still match this same-process invocation"

    second = EvidenceStore.begin_or_resume()

    assert second.invocation_id != first.invocation_id


def test_begin_or_resume_starts_a_new_invocation_when_marker_is_absent(
    isolated_git_directory: Path,
):
    first = EvidenceStore.begin_or_resume()
    # Simulate a fresh invocation with no marker on disk at all.
    (
        isolated_git_directory / push_gate_evidence.CACHE_DIRECTORY / "current.json"
    ).unlink()

    second = EvidenceStore.begin_or_resume()

    assert second.invocation_id != first.invocation_id


def test_begin_or_resume_starts_a_new_invocation_when_marker_is_corrupt(
    isolated_git_directory: Path,
):
    first = EvidenceStore.begin_or_resume()
    marker_path = (
        isolated_git_directory / push_gate_evidence.CACHE_DIRECTORY / "current.json"
    )
    marker_path.write_text("not json", encoding="utf-8")

    second = EvidenceStore.begin_or_resume()

    assert second.invocation_id != first.invocation_id
