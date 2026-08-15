"""Meta-tests: prove scripts/ci_gate_replica.py's anti-drift guards actually fire.

The whole point of parsing every workflow file under .github/workflows/ at run
time (see that script's module docstring) instead of hand-copying step lists
is that drift can never be silently invisible — --consistency-check must fail
loudly instead. These tests prove that claim rather than merely asserting it
in a comment, for all three classes of drift this script now guards against:

  1. a NEW JOB appearing in a known, registered workflow file
  2. a NEW WORKFLOW FILE appearing under .github/workflows/ with no registry entry
  3. a .cargo/config.toml change that adds an external-binary build dependency
     (rustc-wrapper/linker/runner) no workflow file ever installs — the exact
     shape of the incident that shipped commit 652f91c and broke every GitHub
     CI job for ~24s each (sccache not on the runner's PATH; cargo hard-errors
     rather than falling back).

It also proves the previously-uncovered advisory.yml is now actually replayed
(GAP 1: a 10-way feature-build matrix was invisible to this script before),
and that the GAP 3 build-affecting file-set predicate used by `--skip-safe`
is correct.
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "ci_gate_replica.py"

# Static tests of a Python script that parses YAML/TOML and shells out to
# `bash -c` for its OWN (mocked-away, in these tests) subprocess calls --
# never touches the compiled engine. Without this marker every test here
# would pay the shared session fixture's `cargo build --features full` +
# server-start cost (conftest.py) for nothing.
pytestmark = pytest.mark.no_engine


def _load_module():
    spec = importlib.util.spec_from_file_location("ci_gate_replica", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_real_workflows_pass_consistency_check():
    m = _load_module()
    assert m.consistency_check(verbose=False) is True


def test_injected_new_job_in_release_yml_fails_consistency_check():
    m = _load_module()
    release_doc = m.load_workflow(m.WORKFLOWS_DIR / "release.yml")
    release_doc["jobs"]["totally-new-release-job"] = {
        "runs-on": "ubuntu-latest",
        "steps": [{"name": "do a new blocking thing", "run": "echo hi"}],
    }
    ok = m.consistency_check(verbose=False, workflow_docs={"release.yml": release_doc})
    assert ok is False


def test_removing_a_configured_job_fails_consistency_check():
    """The opposite drift direction: a spec naming a job that no longer
    exists in the workflow (renamed or deleted) must also fail, not
    silently pass with less coverage than the config claims."""
    m = _load_module()
    release_doc = m.load_workflow(m.WORKFLOWS_DIR / "release.yml")
    assert "gates" in m.WORKFLOW_REGISTRY["release.yml"].executable_jobs
    del release_doc["jobs"]["gates"]
    ok = m.consistency_check(verbose=False, workflow_docs={"release.yml": release_doc})
    assert ok is False


def test_unregistered_workflow_file_fails_consistency_check(tmp_path):
    """GAP 1's core anti-drift claim: a brand new workflow file (advisory.yml
    was exactly this, once) must fail the check until it is registered, not
    be silently invisible the way advisory.yml was before this fix."""
    m = _load_module()
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    for fname in m.WORKFLOW_REGISTRY:
        shutil.copy(m.WORKFLOWS_DIR / fname, workflows_dir / fname)
    (workflows_dir / "newly-added.yml").write_text(
        "name: New\non:\n  push: {}\njobs:\n  x:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n"
    )
    ok = m.consistency_check(verbose=False, workflows_dir=workflows_dir)
    assert ok is False


def test_registered_workflow_missing_from_disk_fails_consistency_check(tmp_path):
    """Opposite direction: WORKFLOW_REGISTRY names a file no longer present."""
    m = _load_module()
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    shutil.copy(m.WORKFLOWS_DIR / "release.yml", workflows_dir / "release.yml")
    # advisory.yml deliberately not copied here.
    ok = m.consistency_check(verbose=False, workflows_dir=workflows_dir)
    assert ok is False


def test_gates_job_run_steps_include_the_numeric_kernel_parity_chain():
    """Regression guard for the exact drift class this script was built to
    close: a hand-copied step list previously omitted the eg-numeric kernel
    build + numpy-parity gate steps from the `gates` job. Parsing must always
    surface them as RUN steps."""
    m = _load_module()
    doc = m.load_workflow(m.WORKFLOWS_DIR / "release.yml")
    plan, _, _ = m.build_plan_for_workflow(m.WORKFLOW_REGISTRY["release.yml"], doc)
    gates_run_names = {p["name"] for p in plan if p["job"] == "gates" and p["mode"] == "RUN"}
    for expected in (
        "Build eg-numeric Surface-A wheel (feature python)",
        "numpy-parity gate (compiled kernel vs numpy)",
        "Audit numeric wheel for retained build identity",
    ):
        assert expected in gates_run_names, f"missing from parsed gates RUN plan: {expected!r}"


def test_advisory_feature_matrix_is_now_covered_and_expanded_per_leg():
    """GAP 1's headline example: advisory.yml's feature-build matrix (10
    feature combinations) used to be entirely unreplicated. It must now show
    up as 10 distinct, individually-runnable RUN rows with the real matrix
    values substituted in (not a literal, unresolved '${{ matrix... }}')."""
    m = _load_module()
    doc = m.load_workflow(m.WORKFLOWS_DIR / "advisory.yml")
    plan, _, _ = m.build_plan_for_workflow(m.WORKFLOW_REGISTRY["advisory.yml"], doc)
    matrix_rows = [p for p in plan if p["job"].startswith("feature-matrix#") and p["mode"] == "RUN" and p["name"].startswith("Build ")]
    assert len(matrix_rows) == 10, f"expected 10 feature-matrix legs, got {len(matrix_rows)}: {[r['job'] for r in matrix_rows]}"
    for row in matrix_rows:
        assert "${{" not in row["detail"], f"unresolved matrix expression leaked into {row['job']}: {row['detail']!r}"
    flags_by_leg = {r["job"]: r["detail"] for r in matrix_rows}
    assert flags_by_leg["feature-matrix#full"] == "cargo build --no-default-features --features full"
    assert flags_by_leg["feature-matrix#crate-eg-types"] == "cargo build -p eg-types --all-features"


def test_advisory_benchmarks_job_is_now_covered():
    m = _load_module()
    doc = m.load_workflow(m.WORKFLOWS_DIR / "advisory.yml")
    plan, _, _ = m.build_plan_for_workflow(m.WORKFLOW_REGISTRY["advisory.yml"], doc)
    bench_run_names = {p["name"] for p in plan if p["job"] == "benchmarks" and p["mode"] == "RUN"}
    assert "Run hybrid-query benches (latency + recall@k)" in bench_run_names
    assert "Perf/recall regression gate" in bench_run_names
    # advisory.yml never blocks the pre-push gate.
    assert m.WORKFLOW_REGISTRY["advisory.yml"].blocking is False
    assert m.WORKFLOW_REGISTRY["release.yml"].blocking is True


def test_reusable_workflow_call_job_is_reported_not_silently_dropped():
    """advisory.yml's `pages` job is a job-level `uses:` reusable-workflow
    call with no `steps:` at all -- it must still produce a plan row (as
    SKIP_LOUD with a reason), never zero rows."""
    m = _load_module()
    doc = m.load_workflow(m.WORKFLOWS_DIR / "advisory.yml")
    plan, _, _ = m.build_plan_for_workflow(m.WORKFLOW_REGISTRY["advisory.yml"], doc)
    pages_rows = [p for p in plan if p["job"] == "pages"]
    assert pages_rows, "pages job produced zero plan rows -- silently dropped"
    assert all(p["mode"] == "SKIP_LOUD" for p in pages_rows)


# ─────────────────────────────────────────────────────────────────────────
# GAP 2 — external build-tool dependency check. Proves the check FAILS on a
# known-bad reintroduction of the incident's exact .cargo/config.toml lines,
# naming the binary, and PASSES once they are absent (the real, current
# repo state, post-revert).
# ─────────────────────────────────────────────────────────────────────────


def test_build_tool_dependency_check_passes_on_real_cargo_config():
    m = _load_module()
    problems = m.check_build_tool_dependencies(
        m.CARGO_CONFIG_PATH,
        {
            "release.yml": (m.WORKFLOWS_DIR / "release.yml").read_text(encoding="utf-8"),
            "advisory.yml": (m.WORKFLOWS_DIR / "advisory.yml").read_text(encoding="utf-8"),
        },
    )
    assert problems == []


def test_build_tool_dependency_check_fails_on_reintroduced_sccache_wrapper(tmp_path):
    """Known-bad proof: re-introduce commit 652f91c's exact
    `rustc-wrapper = "sccache"` line into a SCRATCH copy of .cargo/config.toml
    (never touching the real repo file) and confirm the check fails and
    names 'sccache' by binary name, without any hard-coded string match on
    'sccache' in the checker itself."""
    m = _load_module()
    bad_config = tmp_path / "config.toml"
    bad_config.write_text(
        m.CARGO_CONFIG_PATH.read_text(encoding="utf-8")
        + '\n[build]\nrustc-wrapper = "sccache"\n'
    )
    problems = m.check_build_tool_dependencies(
        bad_config,
        {
            "release.yml": (m.WORKFLOWS_DIR / "release.yml").read_text(encoding="utf-8"),
            "advisory.yml": (m.WORKFLOWS_DIR / "advisory.yml").read_text(encoding="utf-8"),
        },
    )
    assert any("sccache" in p for p in problems), f"expected an sccache finding, got: {problems}"


def test_build_tool_dependency_check_passes_when_binary_is_actually_installed():
    """The check must not simply reject any rustc-wrapper -- only one the
    workflow never installs. A wrapper name that DOES appear in the
    workflow text (an explicit install step) must pass."""
    m = _load_module()
    fake_config_text = '[build]\nrustc-wrapper = "totally-fake-wrapper-xyz"\n'
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as f:
        f.write(fake_config_text)
        fake_path = Path(f.name)
    try:
        workflow_with_install = (
            "jobs:\n  gates:\n    steps:\n"
            "      - run: apt-get install -y totally-fake-wrapper-xyz\n"
            "      - run: cargo test --workspace\n"
        )
        problems = m.check_build_tool_dependencies(fake_path, {"release.yml": workflow_with_install})
        assert problems == []
    finally:
        fake_path.unlink()


def test_find_required_build_binaries_returns_empty_when_no_cargo_config(tmp_path):
    m = _load_module()
    assert m.find_required_build_binaries(tmp_path / "does-not-exist.toml") == []


# ─────────────────────────────────────────────────────────────────────────
# GAP 3 — the authoritative build-affecting file-set predicate.
# ─────────────────────────────────────────────────────────────────────────


def test_build_affecting_predicate_catches_the_actual_incident_diff():
    """The incident: commit 652f91c changed ONLY .cargo/config.toml. The
    file-set that judged skipping ci-gate-replica safe (zero .rs/Cargo.toml/
    Cargo.lock changes) missed it. Prove the authoritative predicate now
    catches exactly this diff shape."""
    from ci_gate_replica import is_build_affecting

    assert is_build_affecting(".cargo/config.toml") is True


def test_build_affecting_predicate_matches_expected_patterns():
    from ci_gate_replica import is_build_affecting

    for path in (
        "crates/eg-core/src/lib.rs",
        "Cargo.toml",
        "crates/eg-numeric/Cargo.toml",
        "Cargo.lock",
        ".cargo/config.toml",
        ".cargo/nested/whatever.toml",
        "rust-toolchain",
        "rust-toolchain.toml",
        "build.rs",
        ".github/workflows/release.yml",
        ".pre-commit-config.yaml",
    ):
        assert is_build_affecting(path) is True, f"expected {path!r} to be build-affecting"


def test_build_affecting_predicate_excludes_unrelated_paths():
    from ci_gate_replica import is_build_affecting

    for path in (
        "docs/architecture/overview.md",
        "README.md",
        "scripts/some_python_gate.py",
        "epistemic_graph/client.py",
    ):
        assert is_build_affecting(path) is False, f"expected {path!r} to NOT be build-affecting"


# ─────────────────────────────────────────────────────────────────────────
# GAP 4 — local-host external-toolchain detection for a RUN step's own
# cargo invocation. Mirror image of GAP 2 above: GAP 2 fires when the REPO's
# build config needs a binary no CI RUNNER installs (always a real defect);
# this fires when a step needs a binary THIS DEV HOST doesn't have (never a
# defect — classified NOT_VALIDATED_LOCALLY, not FAILED). Both directions
# proven below, plus the "must not create a false skip" guard for the
# feature this task's own problem statement assumed was toolchain-gated
# (gpu-cuda) but, per cudarc 0.17.8's own build.rs and an empirical
# `cargo check --features gpu-cuda`, is not.
# ─────────────────────────────────────────────────────────────────────────


def test_expand_features_resolves_full_extras_to_ros2_rmw_transitively():
    m = _load_module()
    table = m._load_cargo_features()
    resolved = m._expand_features({"full-extras"}, table, all_features=False)
    for expected in ("full-extras", "full", "gpu-cuda", "ros2-bridge", "ros2-dds", "ros2-rmw"):
        assert expected in resolved, f"{expected!r} missing from full-extras closure: {sorted(resolved)}"


def test_all_features_flag_seeds_the_closure_with_every_feature():
    m = _load_module()
    table = m._load_cargo_features()
    resolved = m._expand_features(set(), table, all_features=True)
    assert "ros2-rmw" in resolved
    assert resolved == set(table) | m._expand_features(set(table), table, all_features=False)


def test_extract_requested_features_handles_space_and_equals_and_all_features():
    m = _load_module()
    assert m._extract_requested_features(
        "cargo build --no-default-features --features full-extras"
    ) == ({"full-extras"}, False)
    assert m._extract_requested_features("cargo build --features=a,b -p foo") == ({"a", "b"}, False)
    assert m._extract_requested_features(
        "cargo clippy --workspace --all-features --all-targets -- -D warnings"
    ) == (set(), True)
    assert m._extract_requested_features("echo not a cargo invocation at all") == (set(), False)


def test_check_toolchain_requirements_flags_missing_cmake_for_ros2_rmw(monkeypatch):
    """Direction 1: tool genuinely absent -> a named, loud reason, not a
    silent skip and not an attempted run."""
    m = _load_module()
    table = m._load_cargo_features()
    monkeypatch.setattr(m.shutil, "which", lambda tool: None)
    reason = m.check_toolchain_requirements("cargo build --no-default-features --features full-extras", table)
    assert reason is not None
    assert "cmake" in reason
    assert "ros2-rmw" in reason


def test_check_toolchain_requirements_passes_when_tool_present(monkeypatch):
    """Direction 2: same feature, tool now resolvable on PATH (a stub is
    enough -- only presence is checked, matching how a real CI runner or a
    different dev host would already have it) -> no longer flagged, the
    step is left as an ordinary RUN to be attempted."""
    m = _load_module()
    table = m._load_cargo_features()
    monkeypatch.setattr(m.shutil, "which", lambda tool: f"/usr/bin/{tool}")
    reason = m.check_toolchain_requirements("cargo build --no-default-features --features full-extras", table)
    assert reason is None


def test_check_toolchain_requirements_does_not_false_positive_on_gpu_cuda(monkeypatch):
    """Regression guard for the exact wrong premise this task's own problem
    statement started from: gpu-cuda must NOT be treated as nvcc-gated. If
    it ever were, a step that actually compiles fine here would be silently
    downgraded from a counted RUN to an uncounted NOT_VALIDATED_LOCALLY --
    the coverage-regression failure mode this whole script exists to
    prevent. Simulates nvcc (and every other tool) being ABSENT to prove
    gpu-cuda alone never trips any entry."""
    m = _load_module()
    table = m._load_cargo_features()
    monkeypatch.setattr(m.shutil, "which", lambda tool: None)
    reason = m.check_toolchain_requirements("cargo build --no-default-features --features gpu-cuda", table)
    assert reason is None, f"gpu-cuda must never be toolchain-gated here, got: {reason!r}"
    reason = m.check_toolchain_requirements("cargo build --no-default-features --features ros2-dds", table)
    assert reason is None
    reason = m.check_toolchain_requirements("cargo build --no-default-features --features ros2-bridge", table)
    assert reason is None


def test_check_toolchain_requirements_ignores_non_cargo_text():
    m = _load_module()
    table = m._load_cargo_features()
    assert m.check_toolchain_requirements("python3 scripts/bench_gate.py", table) is None


def test_build_plan_reclassifies_run_step_when_required_tool_missing(monkeypatch):
    """End-to-end: build_plan_for_workflow (not just the helper function)
    turns the real advisory.yml full-extras leg from RUN into
    NOT_VALIDATED_LOCALLY-bound TOOLCHAIN_MISSING when cmake is absent, and
    leaves every OTHER matrix leg (that doesn't transitively need ros2-rmw)
    alone as RUN -- proving this is NOT a `job == "feature-matrix"`
    special-case: legs of the very same job are treated differently based
    purely on what their own command line requests."""
    m = _load_module()
    doc = m.load_workflow(m.WORKFLOWS_DIR / "advisory.yml")
    table = m._load_cargo_features()
    monkeypatch.setattr(m.shutil, "which", lambda tool: None)
    plan, _, _ = m.build_plan_for_workflow(m.WORKFLOW_REGISTRY["advisory.yml"], doc, feature_table=table)
    by_job = {p["job"]: p for p in plan if p["job"].startswith("feature-matrix#") and p["name"].startswith("Build ")}
    assert by_job["feature-matrix#full-extras"]["mode"] == "TOOLCHAIN_MISSING"
    assert "cmake" in by_job["feature-matrix#full-extras"]["detail"]
    assert by_job["feature-matrix#full"]["mode"] == "RUN"
    assert by_job["feature-matrix#slim-lib"]["mode"] == "RUN"
    assert by_job["feature-matrix#default"]["mode"] == "RUN"


def test_build_plan_leaves_full_extras_as_run_when_toolchain_present():
    """Direction 2 at the build_plan_for_workflow level, using whatever this
    test host's REAL PATH provides (no monkeypatch) -- proves the mechanism
    does not default to pessimistic/always-skip; it actually checks."""
    m = _load_module()
    doc = m.load_workflow(m.WORKFLOWS_DIR / "advisory.yml")
    table = m._load_cargo_features()
    if m.shutil.which("cmake") is None or m.shutil.which("cc") is None:
        pytest.skip("this test host itself lacks cmake/cc; nothing to prove here")
    plan, _, _ = m.build_plan_for_workflow(m.WORKFLOW_REGISTRY["advisory.yml"], doc, feature_table=table)
    full_extras = next(p for p in plan if p["job"] == "feature-matrix#full-extras" and p["name"] == "Build full-extras")
    assert full_extras["mode"] == "RUN"


def test_consistency_check_unaffected_by_toolchain_detection():
    """--consistency-check must still pass on the real repo state and still
    fail on an unclassified job regardless of GAP 4 -- the two mechanisms
    are orthogonal (job/workflow coverage vs. per-step local tool
    availability)."""
    m = _load_module()
    assert m.consistency_check(verbose=False) is True
    release_doc = m.load_workflow(m.WORKFLOWS_DIR / "release.yml")
    release_doc["jobs"]["another-new-job"] = {"runs-on": "ubuntu-latest", "steps": [{"run": "echo hi"}]}
    assert m.consistency_check(verbose=False, workflow_docs={"release.yml": release_doc}) is False
