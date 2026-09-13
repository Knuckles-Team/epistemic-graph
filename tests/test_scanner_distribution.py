"""Static checks for the scanner distribution and architecture wiring.

These tests intentionally inspect contracts and workflow text only.  They do
not install or execute a scanner (the scanner-quality job is the execution
surface); that keeps the wiring regression tests safe on hosts without the
optional native tools.
"""

from __future__ import annotations

import configparser
import importlib.util
import sys
from pathlib import Path

import pytest
import tomllib
import yaml

REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _hooks() -> dict[str, dict]:
    document = yaml.safe_load((REPO / ".pre-commit-config.yaml").read_text())
    return {
        hook["id"]: hook
        for repo in document["repos"]
        for hook in repo.get("hooks", [])
        if "id" in hook
    }


def _workflow(filename: str = "release.yml") -> dict:
    return yaml.safe_load((REPO / ".github/workflows" / filename).read_text())


def _ci_replica():
    path = REPO / "scripts/ci_gate_replica.py"
    spec = importlib.util.spec_from_file_location("eg_ci_gate_replica", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_scanner_contract_versions_and_native_policy_files_exist():
    document = tomllib.loads((REPO / "pyproject.toml").read_text())
    scanners = document["tool"]["epistemic_graph"]["scanners"]
    assert {
        key: scanners[key]
        for key in (
            "cccc_version",
            "kiss_version",
            "dupehound_version",
            "jscpd_version",
            "dependency_cruiser_version",
            "import_linter_version",
            "arch_lint_version",
            "cargo_deny_version",
        )
    } == {
        "cccc_version": "1.6.0",
        "kiss_version": "0.4.10",
        "dupehound_version": "0.1.2",
        "jscpd_version": "5.0.16",
        "dependency_cruiser_version": "18.2.0",
        "import_linter_version": "2.13",
        "arch_lint_version": "0.5.0",
        "cargo_deny_version": "0.20.2",
    }
    assert (REPO / ".importlinter").is_file()
    assert (REPO / "arch-lint.toml").is_file()
    assert (REPO / "clients/js/.dependency-cruiser.cjs").is_file()
    exclusions = scanners["jscpd_exclusions"]
    assert any("cache" in pattern for pattern in exclusions)
    assert any("fixture" in pattern for pattern in exclusions)
    assert scanners["jscpd_formats"]["bash"] == ["sh", "bash"]


def test_precommit_has_staged_differential_census_and_architecture_profiles():
    hooks = _hooks()
    assert hooks["dupehound-changed-functions"]["stages"] == ["pre-commit"]
    assert hooks["kiss-changed-rust"]["stages"] == ["pre-commit"]
    assert hooks["jscpd-differential"]["stages"] == ["pre-push", "manual"]
    assert hooks["jscpd-census"]["stages"] == ["pre-push", "manual"]
    assert hooks["cccc-census"]["stages"] == ["pre-push", "manual"]
    assert hooks["kiss-census"]["stages"] == ["pre-push", "manual"]
    assert "validate_cccc_census.py" in hooks["cccc-census"]["entry"]
    for hook_id in (
        "import-linter-architecture",
        "dependency-cruiser-architecture",
        "rust-arch-lint",
    ):
        assert hooks[hook_id]["stages"] == ["pre-commit", "pre-push", "manual"]
    assert hooks["rust-arch-lint"]["entry"] == "python3 scripts/check_rust_arch_lint.py"

    # Distribution is deliberately separated from hooks: a hook may resolve a
    # binary, but it must not invoke a package manager or network installer.
    hook_text = "\n".join(str(hook.get("entry", "")) for hook in hooks.values())
    assert "cargo install" not in hook_text
    assert "pip install" not in hook_text
    assert "npm install" not in hook_text
    assert "npx " not in hook_text


def test_goc70_complete_plan_has_one_pre_push_execution_and_manual_hook():
    hooks = _hooks()
    lint_steps = _workflow()["jobs"]["lint-and-architecture"]["steps"]
    constrained = [
        step
        for step in lint_steps
        if step.get("run") == "bash scripts/constrained_parallelism_gate.sh"
    ]
    assert len(constrained) == 1
    assert "env" not in constrained[0]
    direct_hooks = [
        hook
        for hook in hooks.values()
        if "scripts/constrained_parallelism_gate.sh" in hook.get("entry", "")
    ]
    assert direct_hooks == [hooks["constrained-parallelism"]]
    assert hooks["ci-gate-replica"]["stages"] == ["pre-push", "manual"]
    assert hooks["constrained-parallelism"]["stages"] == ["manual"]


def test_release_scanner_job_is_full_history_blocking_and_pinned():
    document = _workflow()
    jobs = document["jobs"]
    scanner = jobs["scanner-quality"]
    checkout = next(
        step
        for step in scanner["steps"]
        if step.get("uses", "").startswith("actions/checkout@")
    )
    assert checkout["with"]["fetch-depth"] == 0
    assert checkout["with"]["persist-credentials"] is False
    assert "continue-on-error" not in scanner
    assert "scanner-quality" in jobs["build"]["needs"]

    all_runs = "\n".join(str(step["run"]) for step in scanner["steps"] if "run" in step)
    workflow_source = (REPO / ".github/workflows/release.yml").read_text(
        encoding="utf-8"
    )
    for command in (
        "cccc-cli",
        "kiss-ai",
        "dupehound",
        "arch-lint-cli",
        'cargo install --locked --version "$cargo_deny_version" --root '
        '"$scanner_root/cargo-deny" cargo-deny',
        'test "$(cargo-deny --version)" = "cargo-deny $cargo_deny_version"',
        "import-linter==2.13",
        "jscpd@5.0.16",
        "dependency-cruiser@18.2.0",
        "check_duplication.py enforce --base-ref",
        "check_duplication.py census",
        "validate_cccc_census.py",
        "cccc --no-config --min 0",
        "kiss check --config .kiss/kiss.toml --lang rust",
        "lint-imports --config .importlinter --no-cache",
        "depcruise --validate --config .dependency-cruiser.cjs",
        "python3 scripts/check_rust_arch_lint.py",
    ):
        assert command in all_runs, f"scanner-quality is missing {command!r}"
    assert "arch-lint check" not in all_runs
    assert (
        _hooks()["rust-arch-lint"]["entry"] == "python3 scripts/check_rust_arch_lint.py"
    )
    assert "cargo-deny 0.20.2" not in all_runs
    assert all_runs.count("load_contract().cargo_deny_version") == 2
    assert "disclosed as non-hermetic" in workflow_source

    advisory_steps = [
        step
        for step in scanner["steps"]
        if step.get("run") == "bash scripts/check_cargo_advisories.sh"
    ]
    assert len(advisory_steps) == 1
    assert "continue-on-error" not in advisory_steps[0]

    base_step = next(
        step
        for step in scanner["steps"]
        if step.get("name") == "Resolve scanner base commit"
    )
    assert "github.event.pull_request.base.sha" in str(base_step["env"])
    assert "github.event.before" in str(base_step["env"])
    assert checkout["with"]["fetch-depth"] == 0


def test_ci_uses_central_exact_python_version():
    assert (REPO / ".python-version").read_text(encoding="utf-8") == "3.12.13\n"
    registered = _ci_replica().WORKFLOW_REGISTRY
    setup_steps = [
        (filename, step)
        for filename in registered
        for job in _workflow(filename)["jobs"].values()
        for step in job.get("steps", [])
        if step.get("uses", "").startswith("actions/setup-python@")
    ]
    assert len(setup_steps) == 6
    assert {filename for filename, _ in setup_steps} == set(registered)
    assert all(
        step.get("with", {}).get("python-version-file") == ".python-version"
        and "python-version" not in step.get("with", {})
        for _, step in setup_steps
    )

    root_hygiene = (REPO / "scripts/check_root_hygiene.py").read_text(encoding="utf-8")
    assert '".python-version"' in root_hygiene


def test_advisory_gate_wires_exact_cargo_deny_version_check():
    advisory_hook = _hooks()["cargo-deny-advisories"]
    assert advisory_hook["files"] == (
        r"^(Cargo\.lock|Cargo\.toml|crates/.*/Cargo\.toml|deny\.toml|"
        r"\.cargo-audit-allow\.txt|pyproject\.toml|scripts/scanner_contract\.py|"
        r"scripts/check_cargo_advisories\.sh)$"
    )
    precommit_source = (REPO / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    assert "Not yet mirrored into rust-ci.yml" not in precommit_source
    assert "pre-commit-only per" not in precommit_source
    assert "blocking release `scanner-quality` job" in precommit_source

    gate = (REPO / "scripts/check_cargo_advisories.sh").read_text(encoding="utf-8")
    assert "load_contract().cargo_deny_version" in gate
    assert '!= "cargo-deny $EXPECTED_CARGO_DENY_VERSION"' in gate
    assert '"$CARGO_DENY_BIN" check advisories' in gate
    assert (
        "cargo install --locked --version $EXPECTED_CARGO_DENY_VERSION cargo-deny"
        in gate
    )


def test_ci_replica_classifies_scanner_job_and_scanner_files_as_build_affecting():
    module = _ci_replica()
    spec = module.WORKFLOW_REGISTRY["release.yml"]
    assert "scanner-quality" in spec.job_skip_reasons
    for path in (
        "pyproject.toml",
        ".python-version",
        ".kiss/kiss.toml",
        ".importlinter",
        "arch-lint.toml",
        "clients/js/.dependency-cruiser.cjs",
        "clients/js/package.json",
        "scripts/scanner_contract.py",
        "scripts/validate_cccc_census.py",
    ):
        assert module.is_build_affecting(path), path


def test_native_architecture_configs_are_explicit_and_scoped():
    import_linter = configparser.ConfigParser()
    import_linter.read(REPO / ".importlinter")
    assert import_linter["importlinter"]["root_package"] == "epistemic_graph"
    contracts = [
        section
        for section in import_linter.sections()
        if section.startswith("importlinter:contract:")
    ]
    assert contracts
    assert all(import_linter[section]["type"] == "forbidden" for section in contracts)

    arch = tomllib.loads((REPO / "arch-lint.toml").read_text(encoding="utf-8"))
    assert "preset" not in arch
    assert arch["fail_on"] == "error"
    assert arch["analyzer"]["root"] == "."
    assert arch["analyzer"]["exclude"] == ["**/target/**", "**/target-*/**"]
    assert arch["rules"]["no-unwrap-expect"]["enabled"] is False
    assert {
        name for name, settings in arch["rules"].items() if settings["enabled"]
    } == {
        "no-sync-io",
        "no-error-swallowing",
        "handler-complexity",
        "require-thiserror",
        "require-tracing",
        "tracing-env-init",
        "no-silent-result-drop",
    }
    assert arch["rules"]["no-sync-io"]["severity"] == "error"
    assert all(
        settings["severity"] == "warning"
        for name, settings in arch["rules"].items()
        if name not in {"no-unwrap-expect", "no-sync-io"}
    )
    dependency_cruiser = (REPO / "clients/js/.dependency-cruiser.cjs").read_text()
    assert 'name: "no-circular"' in dependency_cruiser
    assert 'name: "no-unresolved"' in dependency_cruiser
