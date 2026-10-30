"""Static checks for the scanner distribution and architecture wiring.

These tests intentionally inspect contracts and workflow text only.  They do
not install or execute a scanner (the scanner-quality job is the execution
surface); that keeps the wiring regression tests safe on hosts without the
optional native tools.
"""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest
import tomllib
import yaml

REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine
CENTRAL_LIGHT_GATES = (
    "Knuckles-Team/pipelines/.github/workflows/medulla_light_gates.yml@main"
)
CENTRAL_RUNNER = (
    "${{ vars.ARBORA_TRUSTED_CI_RUNNER == 'true' && "
    "'\"arbora-ci-trusted\"' || '\"ubuntu-latest\"' }}"
)


def _hooks() -> dict[str, dict]:
    document = yaml.safe_load((REPO / ".config" / "pre-commit.yaml").read_text())
    return {
        hook["id"]: hook
        for repo in document["repos"]
        for hook in repo.get("hooks", [])
        if "id" in hook
    }


def _workflow(filename: str = "ci.yml") -> dict:
    return yaml.safe_load((REPO / ".github/workflows" / filename).read_text())


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
        "kiss_version": "0.4.12",
        "dupehound_version": "0.1.2",
        "jscpd_version": "5.0.16",
        "dependency_cruiser_version": "18.2.0",
        "import_linter_version": "2.13",
        "arch_lint_version": "0.5.0",
        "cargo_deny_version": "0.20.2",
    }
    assert (REPO / ".config" / "importlinter.ini").is_file()
    assert (REPO / ".config" / "arch-lint.toml").is_file()
    assert (REPO / "clients/js/.dependency-cruiser.cjs").is_file()
    exclusions = scanners["jscpd_exclusions"]
    assert any("cache" in pattern for pattern in exclusions)
    assert any("fixture" in pattern for pattern in exclusions)
    assert scanners["jscpd_formats"]["bash"] == ["sh", "bash"]


def test_precommit_hooks_never_install_tools():
    """Hook stages are the config's own business; this pins one real rule."""

    hooks = _hooks()
    # Distribution is deliberately separated from hooks: a hook may resolve a
    # binary, but it must not invoke a package manager or network installer.
    hook_text = "\n".join(str(hook.get("entry", "")) for hook in hooks.values())
    assert "cargo install" not in hook_text
    assert "pip install" not in hook_text
    assert "npm install" not in hook_text
    assert "npx " not in hook_text


def test_goc70_is_manual_only_until_execution_is_bounded():
    hooks = _hooks()
    gates_steps = [
        step for job in _workflow()["jobs"].values() for step in job.get("steps", [])
    ]
    constrained = [
        step
        for step in gates_steps
        if step.get("run") == "bash scripts/constrained_parallelism_gate.sh"
    ]
    # Hosted GOC-70 spent 20 minutes compiling dependencies and timed out before
    # the constrained runtime test started. Keep its explicit pre-commit hook
    # available to developers, but don't let that cold build delay CI/publishing.
    assert constrained == []
    direct_hooks = [
        hook
        for hook in hooks.values()
        if "scripts/constrained_parallelism_gate.sh" in hook.get("entry", "")
    ]
    assert direct_hooks == [hooks["constrained-parallelism"]]
    assert hooks["constrained-parallelism"]["stages"] == ["manual"]


def _assert_scanner_job_removed(jobs):
    """The advisory hosted scanner job was removed to bound pull-request CI
    (EG-CONTRACT-R053.7). Every scanner stays in the manual hook profile that
    scripts/ci_parity.sh runs, with tools provisioned by install_scanners.sh."""
    assert "scanner-quality" not in jobs
    hooks = _hooks()
    for hook_id, command in {
        "jscpd-differential": "check_duplication.py enforce --base-ref",
        "jscpd-census": "check_duplication.py census",
        "cccc-census": "validate_cccc_census.py",
        "kiss-census": "python3 scripts/check_kiss_census.py",
        "import-linter-architecture": "lint-imports",
        "dependency-cruiser-architecture": "depcruise",
        "rust-arch-lint": "python3 scripts/check_rust_arch_lint.py",
    }.items():
        assert command in str(hooks[hook_id]["entry"]), hook_id
        assert hooks[hook_id]["stages"] == ["manual"], hook_id
    assert "--hook-stage manual" in (REPO / "scripts/ci_parity.sh").read_text()


def _assert_security_and_build_jobs(jobs):
    security = jobs["security"]
    assert "continue-on-error" not in security
    assert "security" in jobs["engine-main-wheel"]["needs"]
    deny_install = next(
        step
        for step in security["steps"]
        if step.get("name") == "Install pinned cargo-deny"
    )
    assert deny_install["env"]["CARGO_DENY_VERSION"] == (
        "${{ steps.cargo-deny-cache-key.outputs.version }}"
    )
    assert 'version="$CARGO_DENY_VERSION"' in deny_install["run"]
    advisory_steps = [
        step
        for step in security["steps"]
        if step.get("run") == "bash scripts/check_cargo_advisories.sh"
    ]
    assert len(advisory_steps) == 1
    assert "continue-on-error" not in advisory_steps[0]
    release_jobs = _workflow("release.yml")["jobs"]
    assert (
        release_jobs["build"]["continue-on-error"] == "${{ matrix.optional || false }}"
    )

    return security


def _assert_runtime_contract_jobs(jobs, security):
    # Runtime and spec checks are owned by the immutable central reusable
    # workflow; native jobs read scope from the local `changes` job.
    runtime_contracts = jobs["light-gates"]
    assert runtime_contracts == {
        "if": "github.event_name != 'pull_request'",
        "uses": CENTRAL_LIGHT_GATES,
        "with": {"runner": CENTRAL_RUNNER},
    }
    assert "lint-and-architecture" not in jobs
    assert "spec-gates" not in jobs
    assert "python3 scripts/check_universal_read_rls.py" in {
        step.get("run") for step in security["steps"]
    }


def test_spec_status_gates_are_delegated_to_the_central_workflow():
    """The central workflow owns full-history checkout and spec-status steps."""
    assert _workflow()["jobs"]["light-gates"] == {
        "if": "github.event_name != 'pull_request'",
        "uses": CENTRAL_LIGHT_GATES,
        "with": {"runner": CENTRAL_RUNNER},
    }


def test_scanners_run_in_the_manual_profile_not_hosted_ci():
    jobs = _workflow()["jobs"]
    _assert_scanner_job_removed(jobs)
    security = _assert_security_and_build_jobs(jobs)
    _assert_runtime_contract_jobs(jobs, security)


def test_ci_uses_central_exact_python_version():
    # A minor-version pin: every hosted runner (Linux, Windows, macOS) resolves
    # its newest 3.14. An exact patch pin failed setup-python wherever that
    # patch had not been built for the platform yet.
    assert (REPO / ".python-version").read_text(encoding="utf-8") == "3.14\n"
    workflows = sorted(path.name for path in (REPO / ".github/workflows").glob("*.yml"))
    setup_steps = [
        (filename, name, step)
        for filename in workflows
        for name, job in _workflow(filename)["jobs"].items()
        for step in job.get("steps", [])
        if step.get("uses", "").startswith("actions/setup-python@")
    ]
    assert setup_steps
    for filename, name, step in setup_steps:
        _assert_python_source(filename, name, step)

    root_hygiene = (REPO / "scripts/check_root_hygiene.py").read_text(encoding="utf-8")
    assert '".python-version"' in root_hygiene


def _assert_python_source(filename: str, name: str, step: dict) -> None:
    settings = step.get("with", {})
    assert "python-version" not in settings
    if (filename, name) == ("release-publication-recovery.yml", "recover"):
        assert settings.get("python-version-file") == ".frozen-source/.python-version"
        _assert_frozen_python_checkout(_workflow(filename)["jobs"][name]["steps"], step)
    else:
        assert settings.get("python-version-file") == ".python-version"


def _assert_frozen_python_checkout(steps: list[dict], setup: dict) -> None:
    frozen = [
        item for item in steps if item.get("with", {}).get("path") == ".frozen-source"
    ]
    assert len(frozen) == 1
    assert frozen[0]["uses"].startswith("actions/checkout@")
    assert frozen[0]["with"]["ref"] == "7f5179650d87e6f763b129cfeea6dfb4f8b0273b"
    assert frozen[0]["with"]["persist-credentials"] is False
    assert steps.index(frozen[0]) < steps.index(setup)


def test_advisory_gate_wires_exact_cargo_deny_version_check():
    advisory_hook = _hooks()["cargo-deny-advisories"]
    assert advisory_hook["files"] == (
        r"^(Cargo\.lock|Cargo\.toml|crates/.*/Cargo\.toml|\.config/deny\.toml|"
        r"\.config/cargo-audit-allow\.txt|pyproject\.toml|scripts/scanner_contract\.py|"
        r"scripts/check_cargo_advisories\.sh)$"
    )
    precommit_source = (REPO / ".config" / "pre-commit.yaml").read_text(
        encoding="utf-8"
    )
    assert "Not yet mirrored into rust-ci.yml" not in precommit_source
    assert "pre-commit-only per" not in precommit_source

    gate = (REPO / "scripts/check_cargo_advisories.sh").read_text(encoding="utf-8")
    assert "load_contract().cargo_deny_version" in gate
    assert '!= "cargo-deny $EXPECTED_CARGO_DENY_VERSION"' in gate
    assert '"$CARGO_DENY_BIN" --config "$DENY_TOML" check advisories' in gate
    assert (
        "cargo install --locked --version $EXPECTED_CARGO_DENY_VERSION cargo-deny"
        in gate
    )


def test_native_architecture_configs_are_explicit_and_scoped():
    import_linter = configparser.ConfigParser()
    import_linter.read(REPO / ".config" / "importlinter.ini")
    assert import_linter["importlinter"]["root_package"] == "epistemic_graph"
    contracts = [
        section
        for section in import_linter.sections()
        if section.startswith("importlinter:contract:")
    ]
    assert contracts
    assert all(import_linter[section]["type"] == "forbidden" for section in contracts)

    arch = tomllib.loads(
        (REPO / ".config" / "arch-lint.toml").read_text(encoding="utf-8")
    )
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


def _fake_scanner(path: Path, version: str, *, probe_ok: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'#!/bin/sh\nif [ "$1" = --version ]; then echo "{version}"; exit 0; fi\n'
        f"exit {0 if probe_ok else 1}\n"
    )
    path.chmod(0o755)


def _scanner_cache(root: Path) -> dict[str, str]:
    versions = {
        "cccc/bin/cccc": "cccc 1.6.0",
        "kiss/bin/kiss": "kiss 0.4.12",
        "dupehound/bin/dupehound": "dupehound 0.1.2",
        "arch-lint/bin/arch-lint": "arch-lint 0.5.0",
        "npm/node_modules/.bin/jscpd": "cpd 5.0.16",
        "npm/node_modules/.bin/depcruise": "18.2.0",
    }
    for relative, version in versions.items():
        _fake_scanner(root / relative, version)
    return versions


def _run_scanner_installer(root: Path, *, verify: bool = True, env=None):
    import subprocess

    args = ["bash", str(REPO / "scripts/install_scanners.sh")]
    if verify:
        args.append("--verify")
    return subprocess.run(
        [*args, str(root)], capture_output=True, text=True, env=env, timeout=15
    )


def test_scanner_cache_accepts_complete_tools_and_rejects_missing(tmp_path):
    versions = _scanner_cache(tmp_path)
    assert _run_scanner_installer(tmp_path).returncode == 0
    for relative, version in versions.items():
        (tmp_path / relative).unlink()
        assert _run_scanner_installer(tmp_path).returncode != 0, relative
        _fake_scanner(tmp_path / relative, version)


def test_scanner_cache_rejects_wrong_version_and_upstream_kiss(tmp_path):
    versions = _scanner_cache(tmp_path)
    for relative, version in versions.items():
        _fake_scanner(tmp_path / relative, "wrong version")
        assert _run_scanner_installer(tmp_path).returncode != 0, relative
        _fake_scanner(tmp_path / relative, version)
    _fake_scanner(tmp_path / "kiss/bin/kiss", "kiss 0.4.12", probe_ok=False)
    assert _run_scanner_installer(tmp_path).returncode != 0


@pytest.mark.parametrize("relative", [False, True])
def test_scanner_installer_repairs_partial_cache_and_propagates_failure(
    tmp_path, monkeypatch, relative
):
    import os

    monkeypatch.chdir(tmp_path)
    root = Path("relative cache") if relative else tmp_path / "cache"
    _scanner_cache(root)
    missing = root / "cccc/bin/cccc"
    tools = tmp_path / "tools"
    tools.mkdir()
    cargo = tools / "cargo"
    cargo.write_text("#!/bin/sh\nexit 42\n")
    cargo.chmod(0o755)
    env = {**os.environ, "PATH": f"{tools}:{os.environ['PATH']}"}
    assert _run_scanner_installer(root, verify=False, env=env).returncode == 0
    missing.unlink()
    assert _run_scanner_installer(root, verify=False, env=env).returncode == 42
    assert _run_scanner_installer(root).returncode != 0
    cargo.write_text("#!/bin/sh\nexit 0\n")
    assert _run_scanner_installer(root, verify=False, env=env).returncode != 0
    cargo.write_text(
        '#!/bin/sh\n[ "$1" = install ] || exit 2\n'
        'case " $* " in *" --force "*) ;; *) exit 3 ;; esac\n'
        'case " $* " in *" --config net.git-fetch-with-cli=true "*) ;;\n'
        "  *) exit 4 ;; esac\n"
        'while [ "$1" != --root ]; do shift; done\nshift\n'
        'mkdir -p "$1/bin"\n'
        'printf \'#!/bin/sh\\necho "cccc 1.6.0"\\n\' > "$1/bin/cccc"\n'
        'chmod +x "$1/bin/cccc"\n'
    )
    assert _run_scanner_installer(root, verify=False, env=env).returncode == 0
    assert _run_scanner_installer(root).returncode == 0
    _fake_scanner(missing, "cccc 0.0.0")
    assert _run_scanner_installer(root, verify=False, env=env).returncode == 0
    assert _run_scanner_installer(root).returncode == 0


def test_scanner_installer_stdout_contains_only_bootstrap_path_entries(tmp_path):
    _scanner_cache(tmp_path)
    result = _run_scanner_installer(tmp_path, verify=False)
    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        str(tmp_path / directory)
        for directory in (
            "cccc/bin",
            "kiss/bin",
            "dupehound/bin",
            "arch-lint/bin",
            "npm/node_modules/.bin",
        )
    ]
    assert "kiss fork probe:" in result.stderr
    verified = _run_scanner_installer(tmp_path)
    assert verified.returncode == 0
    assert verified.stdout == ""
    assert "kiss fork probe:" in verified.stderr


def test_scanner_installer_supports_relative_roots_without_verify_creation(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    root = Path("relative cache")
    missing = _run_scanner_installer(root)
    assert missing.returncode != 0
    assert not root.exists()
    _scanner_cache(root)
    for verify in (True, False):
        result = _run_scanner_installer(root, verify=verify)
        assert result.returncode == 0, result.stderr
        if not verify:
            assert all(Path(line).is_absolute() for line in result.stdout.splitlines())
