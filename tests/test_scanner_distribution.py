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


def _workflow() -> dict:
    return yaml.safe_load((REPO / ".github/workflows/release.yml").read_text())


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
        )
    } == {
        "cccc_version": "1.6.0",
        "kiss_version": "0.4.10",
        "dupehound_version": "0.1.2",
        "jscpd_version": "5.0.16",
        "dependency_cruiser_version": "18.2.0",
        "import_linter_version": "2.13",
        "arch_lint_version": "0.5.0",
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

    # Distribution is deliberately separated from hooks: a hook may resolve a
    # binary, but it must not invoke a package manager or network installer.
    hook_text = "\n".join(str(hook.get("entry", "")) for hook in hooks.values())
    assert "cargo install" not in hook_text
    assert "pip install" not in hook_text
    assert "npm install" not in hook_text
    assert "npx " not in hook_text


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
    for command in (
        "cccc-cli",
        "kiss-ai",
        "dupehound",
        "arch-lint-cli",
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
        "arch-lint check --format json",
    ):
        assert command in all_runs, f"scanner-quality is missing {command!r}"

    base_step = next(
        step
        for step in scanner["steps"]
        if step.get("name") == "Resolve scanner base commit"
    )
    assert "github.event.pull_request.base.sha" in str(base_step["env"])
    assert "github.event.before" in str(base_step["env"])
    assert checkout["with"]["fetch-depth"] == 0


def test_ci_replica_classifies_scanner_job_and_scanner_files_as_build_affecting():
    module = _ci_replica()
    spec = module.WORKFLOW_REGISTRY["release.yml"]
    assert "scanner-quality" in spec.job_skip_reasons
    for path in (
        "pyproject.toml",
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
    assert arch["preset"] == "minimal"
    assert arch["fail_on"] == "error"
    assert arch["analyzer"]["root"] == "."
    assert "**/target/**" in arch["analyzer"]["exclude"]
    assert arch["rules"]["no-unwrap-expect"]["enabled"] is False
    dependency_cruiser = (REPO / "clients/js/.dependency-cruiser.cjs").read_text()
    assert 'name: "no-circular"' in dependency_cruiser
    assert 'name: "no-unresolved"' in dependency_cruiser
