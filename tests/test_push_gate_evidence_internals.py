"""Tests for two ``scripts/push_gate_evidence.py`` internals that the existing
``test_push_gate_evidence.py`` suite deliberately does not cover:

  * ``_extract_cargo`` -- the cargo argv token classifier, now decomposed into
    ``_CARGO_PAIR_FLAGS``/``_append_cargo_pair_value``. Exercised only
    indirectly (via ``Selection.from_argv``) by the existing suite, and never
    asserted on there.
  * ``EvidenceStore.begin_or_resume`` -- the existing suite's own docstring
    says it "intentionally does not invoke ... the private on-disk cache";
    now decomposed into ``_ensure_cache_directory``,
    ``_marker_matches_invocation``, ``EvidenceStore._store_from_verified_marker``,
    and ``EvidenceStore._start_new_invocation``. ``EvidenceStore.current`` is
    covered too -- it was never covered by any prior test, and now shares
    ``_store_from_verified_marker`` with ``begin_or_resume`` (see below).
    These tests isolate the private cache to a per-test ``tmp_path`` by
    monkeypatching ``_git_directory``, so they never touch the real (shared,
    multi-worktree) repository's own cache other concurrent lanes use.

These tests also caught and drove the fix for a real defect: ``_write_evidence``
signed a core that excluded ``contentDigest`` while ``_verify_document``
verified against a core that included it, so a store's own fresh evidence
never passed its own self-check and ``begin_or_resume`` never actually
resumed. ``_write_evidence``/``_verify_document`` now agree (both sign the
document minus ``signature``), proven below by an actual resume, a
tampered-digest rejection, and a forged-content rejection that holds even
when the forger recomputes the unkeyed content digest.

Fixing that also surfaced a real jscpd (clone-gate) finding: ``current``'s
signature-check-and-build-store tail duplicated ``_resume_from_marker``'s
body almost verbatim. Both now share one extracted classmethod,
``_store_from_verified_marker``; ``current``'s own tests below cover the
same resume/tamper scenarios to prove that extraction changed nothing
observable.
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


def test_begin_or_resume_actually_resumes_the_same_invocation(
    isolated_git_directory: Path,
):
    """A same-process second call must resume, not silently start fresh.

    Fixes a real defect this lane's tests found: ``_write_evidence`` used to
    compute its digest/signature over the document BEFORE ``contentDigest``
    was added, while ``_verify_document`` recomputed over the document
    AFTER -- so a store's own freshly-written evidence never passed its own
    ``_load_evidence`` self-check, and ``begin_or_resume`` silently fell
    through to a new invocation on every call. ``_write_evidence`` now signs
    a core that includes ``contentDigest`` (matching what
    ``_verify_document`` has always expected), so resume works.
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

    assert second.invocation_id == first.invocation_id
    assert second.evidence_path == first.evidence_path
    # The resumed store's own evidence must still self-verify.
    assert second._load_evidence()["status"] == "running"


def test_begin_or_resume_rejects_a_tampered_content_digest(
    isolated_git_directory: Path,
):
    first = EvidenceStore.begin_or_resume()
    raw = json.loads(first.evidence_path.read_text(encoding="utf-8"))
    raw["contentDigest"] = "sha256:" + "0" * 64
    first.evidence_path.write_text(json.dumps(raw), encoding="utf-8")

    second = EvidenceStore.begin_or_resume()

    # Tampered evidence must not verify -- begin_or_resume falls through to
    # a fresh invocation rather than trusting it.
    assert second.invocation_id != first.invocation_id


def test_begin_or_resume_rejects_forged_content_with_a_recomputed_digest(
    isolated_git_directory: Path,
):
    """Forged ``results`` with a correctly recomputed ``contentDigest`` must
    still be rejected. The digest is unkeyed, so anyone can recompute it; the
    rejection comes from the HMAC signature authenticating the content."""

    first = EvidenceStore.begin_or_resume()
    raw = json.loads(first.evidence_path.read_text(encoding="utf-8"))
    raw["results"] = {"forged": "data"}
    raw["contentDigest"] = push_gate_evidence._digest(
        {k: v for k, v in raw.items() if k not in ("contentDigest", "signature")}
    )
    first.evidence_path.write_text(json.dumps(raw), encoding="utf-8")

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


# ── EvidenceStore.current ───────────────────────────────────────────────────
# Not covered by any prior test; now shares _store_from_verified_marker with
# begin_or_resume (see the module docstring for why).


def test_current_returns_none_before_any_invocation_started(
    isolated_git_directory: Path,
):
    assert EvidenceStore.current() is None


def test_current_returns_the_active_invocation(isolated_git_directory: Path):
    started = EvidenceStore.begin_or_resume()

    found = EvidenceStore.current()

    assert found is not None
    assert found.invocation_id == started.invocation_id
    assert found.evidence_path == started.evidence_path
    assert found._load_evidence()["status"] == "running"


def test_current_returns_none_when_marker_signature_is_tampered(
    isolated_git_directory: Path,
):
    EvidenceStore.begin_or_resume()
    marker_path = (
        isolated_git_directory / push_gate_evidence.CACHE_DIRECTORY / "current.json"
    )
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    marker["signature"] = "0" * len(marker["signature"])
    marker_path.write_text(json.dumps(marker), encoding="utf-8")

    assert EvidenceStore.current() is None


def test_current_returns_none_when_evidence_content_digest_is_tampered(
    isolated_git_directory: Path,
):
    started = EvidenceStore.begin_or_resume()
    raw = json.loads(started.evidence_path.read_text(encoding="utf-8"))
    raw["contentDigest"] = "sha256:" + "0" * 64
    started.evidence_path.write_text(json.dumps(raw), encoding="utf-8")

    assert EvidenceStore.current() is None
