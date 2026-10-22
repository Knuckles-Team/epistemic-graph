"""Keep fast documentation PR checks conservative and publication exhaustive."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import tomllib
import yaml

from scripts import ci_change_scope as scope

pytestmark = pytest.mark.no_engine
ROOT = Path(__file__).resolve().parents[1]


def raw(
    path: str, old: str = "100644", new: str = "100644", status: str = "M"
) -> bytes:
    return f":{old} {new} abc def {status}\0{path}\0".encode()


@pytest.mark.parametrize(
    "path",
    ["README.md", "docs/guide.md", "specs/demo/spec.md", "specs/demo/status.json"],
)
def test_documentation_only_diff(path: str) -> None:
    assert scope.documentation_diff(raw(path))
    assert scope.documentation_diff(raw(path, new="000000", status="D"))
    assert scope.documentation_diff(raw(path, old="000000", status="A"))


@pytest.mark.parametrize(
    "path",
    [
        "src/lib.rs",
        "Cargo.lock",
        ".config/pre-commit.yaml",
        ".github/workflows/ci.yml",
        "docs/plugin.py",
        "specs/build.rs",
        ".specify/scripts/setup.sh",
        "docs/data.json",
        "crates/runtime/fixture.md",
        "architecture/component-registry.yml",
    ],
)
def test_unknown_or_executable_content_runs_code(path: str) -> None:
    assert not scope.documentation_diff(raw(path))


@pytest.mark.parametrize("mode", ["100755", "120000", "160000"])
def test_executable_symlink_and_submodule_modes_run_code(mode: str) -> None:
    assert not scope.documentation_diff(raw("docs/guide.md", old=mode))
    assert not scope.documentation_diff(raw("docs/guide.md", new=mode))


def test_code_renamed_to_docs_still_runs_code() -> None:
    # --no-renames exposes both the removed source and new destination.
    assert not scope.documentation_diff(
        raw("src/old.rs", new="000000", status="D")
        + raw("docs/new.md", old="000000", status="A")
    )


@pytest.mark.parametrize("data", [b"", b"broken", b"\0", b":bad\0docs/a.md\0"])
def test_incomplete_diff_runs_code(data: bytes) -> None:
    assert not scope.documentation_diff(data)


@pytest.mark.parametrize("event", ["push", "workflow_dispatch", "merge_group", ""])
def test_non_pr_events_always_run_full_validation(event: str) -> None:
    assert scope.requires_code(event, "a" * 40, "b" * 40)


def test_invalid_or_missing_history_runs_code(monkeypatch: pytest.MonkeyPatch) -> None:
    assert scope.requires_code("pull_request", "", "b" * 40)

    def missing(*args: object, **kwargs: object) -> None:
        raise subprocess.CalledProcessError(128, "git")

    monkeypatch.setattr(scope.subprocess, "run", missing)
    assert scope.requires_code("pull_request", "a" * 40, "b" * 40)


def test_complete_git_diff_has_no_api_file_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def diff(
        command: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[bytes]:
        assert command == [
            "git",
            "diff",
            "--raw",
            "--no-renames",
            "-z",
            f"{'a' * 40}...{'b' * 40}",
        ]
        return subprocess.CompletedProcess(
            command, 0, raw("docs/a.md") * 301 + raw("src/a.rs")
        )

    monkeypatch.setattr(scope.subprocess, "run", diff)
    assert scope.requires_code("pull_request", "a" * 40, "b" * 40)


def test_workflow_keeps_security_and_spec_checks_and_full_publication() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    security = {step.get("name"): step for step in jobs["security"]["steps"]}
    for name in (
        "Secret-history scan",
        "Universal served-read RLS and carrier-ownership contract",
    ):
        assert "if" not in security[name]
    for name in ("Install pinned cargo-deny", "Rust dependency advisory gate"):
        assert security[name]["if"] == "needs.changes.outputs.code != 'false'"
    assert jobs["security"]["if"] == "always()"
    assert "if" not in jobs["spec-gates"]
    lint = {step.get("name"): step for step in jobs["lint-and-architecture"]["steps"]}
    hooks = lint["pre-commit (commit stage, PR diff or full publication)"]
    assert '--from-ref "$BASE" --to-ref "$HEAD"' in hooks["run"]
    assert "--all-files" in hooks["run"]
    assert hooks["env"]["SKIP"] == "lane-guard,cargo-deny-advisories"
    assert lint["rustfmt"]["run"] == "cargo fmt --all -- --check"
    assert "if" not in lint["rustfmt"]
    assert "CI change-scope regression tests" in lint


def test_pr_runtime_gates_do_not_wait_for_self_hosted_runners() -> None:
    # 2026-10-14 policy (plan.md): ordinary native acceptance stays on hosted
    # runners for every event so constrained user hosts carry no native load.
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    for name in (
        "gates",
        "gates-plan-fuzz",
        "gates-wheel",
        "gates-facade",
        "gates-whisper",
    ):
        assert jobs[name]["runs-on"] == "ubuntu-latest"
        assert jobs[name]["if"] == "needs.changes.outputs.code == 'true'"


@pytest.mark.parametrize("job_name", ["gates-crates", "gates-storage"])
def test_workspace_gates_use_hosted_runner_with_required_headers(job_name: str) -> None:
    """EG-CONTRACT-R035: prerequisite installation must work on every event."""
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    job = jobs[job_name]
    assert job["runs-on"] == "ubuntu-latest"
    assert job["needs"] == "changes"
    assert job["if"] == "needs.changes.outputs.code == 'true'"
    assert job["timeout-minutes"] == (
        "${{ github.event_name == 'pull_request' && 10 || 60 }}"
    )
    installs = [
        step
        for step in job["steps"]
        if step.get("name") == "Install librdkafka/Cyrus-SASL build headers"
    ]
    assert len(installs) == 1
    assert "if" not in installs[0]
    assert installs[0]["run"] == (
        "set -euo pipefail\n"
        "sudo apt-get update\n"
        "sudo apt-get install -y --no-install-recommends "
        "libsasl2-dev libcurl4-openssl-dev\n"
    )


# spec: EG-CONTRACT-R031
# spec: EG-CONTRACT-R031.1
def test_lint_installs_manifest_rustfmt_before_all_formatting_steps() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    steps = workflow["jobs"]["lint-and-architecture"]["steps"]
    install = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Install manifest Rust toolchain and rustfmt"
    )
    setup = steps[install]
    assert "if" not in setup
    assert setup["run"] == (
        "rustup toolchain install --profile minimal --component rustfmt "
        "&& rustup show active-toolchain"
    )
    for index, step in enumerate(steps):
        command = step.get("run", "")
        if "pre-commit run" in command or "check_rustfmt_scope.py" in command:
            assert install < index
    rustfmt = next(step for step in steps if step.get("name") == "rustfmt")
    assert rustfmt["run"] == "cargo fmt --all -- --check"


@pytest.fixture
def local_gate_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from scripts import source_gate_scope as gate

    monkeypatch.chdir(tmp_path)
    for key in (
        "CI",
        "GITHUB_ACTIONS",
        "GITHUB_EVENT_NAME",
        "PRE_COMMIT_FROM_REF",
        "PRE_COMMIT_TO_REF",
    ):
        monkeypatch.delenv(key, raising=False)

    def git(*args: str) -> str:
        return (
            subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )

    git("init", "-q")
    git("config", "user.email", "fixture@example.invalid")
    git("config", "user.name", "Fixture")
    for name in (gate.SKILL, "docs/guide.md", "src/lib.rs"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("original\n")
    git("add", ".")
    git("commit", "-qm", "fixture")
    return gate, git


@pytest.mark.parametrize("mode", ["100755", "120000", "160000"])
def test_normalized_document_modes_keep_real_git_ranges_conservative(
    local_gate_repo, mode: str
) -> None:
    _, git = local_gate_repo
    path = "docs/guide.md"
    git("update-index", "--chmod=+x", path)
    git("commit", "-qm", "executable documentation baseline")
    executable = git("rev-parse", "HEAD")
    git("update-index", "--chmod=-x", path)
    git("commit", "-qm", "normalize documentation mode")
    normalized = git("rev-parse", "HEAD")
    assert git("rev-parse", f"{executable}:{path}") == git(
        "rev-parse", f"{normalized}:{path}"
    )
    assert scope.requires_code("pull_request", executable, normalized)

    Path(path).write_text("updated prose\n")
    git("add", path)
    git("commit", "-qm", "ordinary documentation edit")
    prose = git("rev-parse", "HEAD")
    assert not scope.requires_code("pull_request", normalized, prose)
    assert scope.requires_code("pull_request", executable, prose)

    if mode == "100755":
        git("update-index", "--chmod=+x", path)
    elif mode == "120000":
        Path(path).unlink()
        Path(path).symlink_to("elsewhere")
        git("add", path)
    else:
        git("update-index", "--add", "--cacheinfo", f"160000,{prose},{path}")
    git("commit", "-qm", "non-document mode requires full CI")
    head = git("rev-parse", "HEAD")
    assert git("ls-tree", head, path).split()[0] == mode
    assert scope.requires_code("pull_request", prose, head)


def test_local_scope_reuses_parser_without_broadening_ci(local_gate_repo) -> None:
    gate, git = local_gate_repo
    Path(gate.SKILL).write_text("updated\n")
    git("add", gate.SKILL)
    assert gate.can_skip("staged", [gate.SKILL])
    assert not scope.documentation_diff(raw(gate.SKILL))
    assert scope.documentation_diff(raw(gate.SKILL), gate.local_document)
    assert not gate.can_skip("staged", [gate.SKILL, "src/lib.rs"])
    assert not gate.can_skip("staged", [])
    assert not gate.can_skip("all-files", [gate.SKILL])
    assert not gate.can_skip("manual", [gate.SKILL])
    assert not gate.can_skip("staged", git("ls-files").splitlines())


@pytest.mark.parametrize(
    "change",
    [
        "delete-rust",
        "rename-rust",
        "delete-doc",
        "executable",
        "symlink",
        "mixed",
        "dirty",
        "config",
        "helper",
    ],
)
def test_local_scope_fails_closed_on_real_git_changes(local_gate_repo, change) -> None:
    gate, git = local_gate_repo
    Path(gate.SKILL).write_text("updated\n")
    git("add", gate.SKILL)
    if change in {"delete-rust", "rename-rust"}:
        git("rm", "src/lib.rs")
        if change == "rename-rust":
            Path("docs/moved.md").write_text("original\n")
            git("add", "docs/moved.md")
    elif change == "delete-doc":
        git("rm", "docs/guide.md")
    elif change == "executable":
        git("update-index", "--chmod=+x", gate.SKILL)
    elif change == "symlink":
        Path(gate.SKILL).unlink()
        Path(gate.SKILL).symlink_to("elsewhere")
        git("add", gate.SKILL)
    elif change == "dirty":
        Path(gate.SKILL).write_text("unstaged\n")
    else:
        name = {
            "mixed": "src/lib.rs",
            "config": "Cargo.toml",
            "helper": "scripts/rust_lexer.py",
        }[change]
        Path(name).parent.mkdir(parents=True, exist_ok=True)
        Path(name).write_text("changed\n")
        git("add", name)
    assert not gate.can_skip("staged", [gate.SKILL])


@pytest.mark.parametrize("mode", ["staged", "push"])
def test_explicit_range_requires_exact_clean_tip_and_available_history(
    local_gate_repo, monkeypatch, mode
) -> None:
    gate, git = local_gate_repo
    base = git("rev-parse", "HEAD")
    Path("docs/guide.md").write_text("updated\n")
    git("add", "docs/guide.md")
    git("commit", "-qm", "docs")
    head = git("rev-parse", "HEAD")
    monkeypatch.setenv("PRE_COMMIT_FROM_REF", base)
    monkeypatch.setenv("PRE_COMMIT_TO_REF", head)
    assert gate.can_skip(mode, ["docs/guide.md"])
    monkeypatch.setenv("CI", "false")
    assert not gate.can_skip(mode, ["docs/guide.md"])
    monkeypatch.delenv("CI")
    for invalid in ("", "0" * 40, base):
        monkeypatch.setenv("PRE_COMMIT_TO_REF", invalid)
        assert not gate.can_skip(mode, ["docs/guide.md"])
    monkeypatch.setenv("PRE_COMMIT_TO_REF", head)
    monkeypatch.setenv("PRE_COMMIT_FROM_REF", "f" * 40)
    assert not gate.can_skip(mode, ["docs/guide.md"])
    monkeypatch.setenv("PRE_COMMIT_FROM_REF", head)
    assert not gate.can_skip(mode, ["docs/guide.md"])
    monkeypatch.setenv("PRE_COMMIT_FROM_REF", base)
    Path("src/lib.rs").write_text("dirty\n")
    assert not gate.can_skip(mode, ["docs/guide.md"])
    git("add", "src/lib.rs")
    assert not gate.can_skip(mode, ["docs/guide.md"])


def test_local_scope_absent_repository_runs_full(tmp_path, monkeypatch) -> None:
    from scripts import source_gate_scope as gate

    monkeypatch.chdir(tmp_path)
    assert not gate.can_skip("staged", [gate.SKILL])


@pytest.mark.parametrize(
    "name", ["contract-method-reachability", "durable-table-registration"]
)
def test_original_scanner_failures_propagate(name, monkeypatch) -> None:
    from scripts import source_gate_scope as gate

    monkeypatch.setattr("sys.argv", ["gate", "--scope", "manual", "--scanner", name])
    called = []

    def run(command):
        called.append(command)
        return 17

    monkeypatch.setattr(gate.subprocess, "call", run)
    assert gate.main() == 17
    assert Path(called[0][1]).name == gate.SCANNERS[name]


def test_source_hook_modes_keep_manual_and_full_coverage() -> None:
    config = yaml.safe_load((ROOT / ".config/pre-commit.yaml").read_text())
    hooks = {hook["id"]: hook for repo in config["repos"] for hook in repo["hooks"]}
    for name in ("contract-method-reachability", "durable-table-registration"):
        for suffix, stage, mode in (
            ("", "pre-commit", "staged"),
            ("-push", "pre-push", "push"),
        ):
            hook = hooks[name + suffix]
            assert f"--scope {mode}" in hook["entry"]
            assert hook["stages"] == [stage]
            assert (
                hook["always_run"] and hook["pass_filenames"] and hook["require_serial"]
            )
        manual = hooks[name + "-manual"]
        assert "source_gate_scope" not in manual["entry"]
        assert manual["stages"] == ["manual"]
        assert manual["always_run"] and not manual["pass_filenames"]


@pytest.mark.parametrize("flag", ["--assume-unchanged", "--skip-worktree"])
def test_hidden_index_state_forces_full_scan(local_gate_repo, flag) -> None:
    gate, git = local_gate_repo
    Path(gate.SKILL).write_text("updated\n")
    git("add", gate.SKILL)
    git("update-index", flag, "src/lib.rs")
    assert not gate.can_skip("staged", [gate.SKILL])


def test_ignored_executable_mode_forces_full_scan(local_gate_repo) -> None:
    gate, git = local_gate_repo
    Path(gate.SKILL).write_text("updated\n")
    git("add", gate.SKILL)
    git("config", "core.filemode", "false")
    Path(gate.SKILL).chmod(0o755)
    assert not gate.can_skip("staged", [gate.SKILL])


@pytest.mark.parametrize("present", ["PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF"])
def test_partial_range_never_falls_back_to_staged_docs(
    local_gate_repo, monkeypatch, present
):
    gate, git = local_gate_repo
    Path(gate.SKILL).write_text("updated\n")
    git("add", gate.SKILL)
    assert gate.can_skip("staged", [gate.SKILL])
    monkeypatch.setenv(present, git("rev-parse", "HEAD"))
    assert not gate.can_skip("staged", [gate.SKILL])


def test_frozen_commit_stage_entry_uses_complete_range(local_gate_repo, monkeypatch):
    gate, git = local_gate_repo
    base = git("rev-parse", "HEAD")
    Path(gate.SKILL).write_text("updated\n")
    git("add", gate.SKILL)
    git("commit", "-qm", "frozen docs")
    head = git("rev-parse", "HEAD")
    assert not gate.can_skip("staged", [gate.SKILL])
    monkeypatch.setenv("PRE_COMMIT_FROM_REF", base)
    monkeypatch.setenv("PRE_COMMIT_TO_REF", head)
    monkeypatch.setenv("PYTHONPATH", str(ROOT))
    hooks = yaml.safe_load((ROOT / ".config/pre-commit.yaml").read_text())["repos"]
    for hook in (h for repo in hooks for h in repo["hooks"]):
        if hook["id"] not in gate.SCANNERS:
            continue
        command = [sys.executable, *hook["entry"].split()[1:], gate.SKILL]
        result = subprocess.run(command, capture_output=True, text=True, env=os.environ)
        assert result.returncode == 0, result.stderr
        assert "scan unnecessary" in result.stdout
    assert not git("status", "--porcelain")


# spec: EG-CONTRACT-R035
def test_generated_contract_pytest_uses_test_interpreter_and_lock() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    steps = jobs["gates"]["steps"]
    install = next(
        step
        for step in steps
        if step.get("name") == "Install locked pytest for generated-client checks"
    )
    command = install["run"]
    assert command.startswith('uv pip install --python "$(command -v python3)" ')
    packages = tomllib.loads((ROOT / "uv.lock").read_text())["package"]
    versions = {
        package["name"]: package["version"]
        for package in packages
        if "version" in package
    }
    expected = {
        f"{name}=={versions[name]}"
        for name in ("pytest", "iniconfig", "packaging", "pluggy", "pygments")
    }
    assert set(command.split('" ', 1)[1].split()) == expected
    consumer = next(
        step for step in steps if step.get("name") == "Generated Python client contract"
    )
    assert consumer["run"] == "python3 tests/test_generated_client_contract.py"
    assert steps.index(install) < steps.index(consumer)


@pytest.mark.parametrize(
    ("job_name", "steps_digest"),
    [
        (
            "gates-wheel",
            "fb183be132512966d0c14feb1139851019b5f1e96518485c2c0b6dbe0452aba9",
        ),
        (
            "gates-whisper",
            "6a4943c685f7d0a3b4e27fd57ab2f3e3b7c0616cf2c156e294461a7786dce2cb",
        ),
    ],
)
def test_split_native_acceptance_preserves_commands_and_release_dependencies(
    job_name: str,
    steps_digest: str,
) -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    job = jobs[job_name]
    assert job["needs"] == "changes"
    assert job["if"] == jobs["gates"]["if"]
    assert job["runs-on"] == jobs["gates"]["runs-on"]
    assert job["timeout-minutes"] == jobs["gates"]["timeout-minutes"]
    assert job["steps"][:5] == jobs["gates"]["steps"][:5]
    # Snapshot the unchanged step mappings, including every command and its env.
    acceptance_steps = job["steps"][7 if job_name == "gates-wheel" else 5 :]
    actual = json.dumps(acceptance_steps, sort_keys=True).encode()
    assert hashlib.sha256(actual).hexdigest() == steps_digest
    for consumer in ("engine-main-wheel",):
        assert job_name in jobs[consumer]["needs"]
    original = "gates" if job_name == "gates-wheel" else "gates-facade"
    original_names = {step.get("name") for step in jobs[original]["steps"]}
    assert not original_names & {step["name"] for step in acceptance_steps}


def test_wheel_gate_inherits_locked_pytest_before_parity_dependencies() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    original = next(
        step
        for step in jobs["gates"]["steps"]
        if step.get("name") == "Install locked pytest for generated-client checks"
    )
    steps = jobs["gates-wheel"]["steps"]
    locked = next(
        step
        for step in steps
        if step.get("name") == "Install locked pytest for generated-client checks"
    )
    # Reuse the exact step whose interpreter and uv.lock pins are checked above.
    assert locked is original
    parity = next(
        step for step in steps if step.get("name") == "Install maturin + parity deps"
    )
    assert steps.index(locked) == 5
    assert steps.index(locked) < steps.index(parity)


def test_wheel_acceptance_uses_only_the_external_virtualenv(tmp_path: Path) -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    job = jobs["gates-wheel"]
    assert "EG_GATES_WHEEL_VENV" not in job.get("env", {})
    initialize = job["steps"][6]
    assert initialize["name"] == "Initialize external wheel virtualenv path"
    environment_file = tmp_path / "github-env"
    runner_temp = tmp_path / "runner temp"
    subprocess.run(
        ["bash", "-e", "-c", initialize["run"]],
        env={
            **os.environ,
            "RUNNER_TEMP": str(runner_temp),
            "GITHUB_ENV": str(environment_file),
        },
        check=True,
    )
    assert environment_file.read_text() == (
        f"EG_GATES_WHEEL_VENV={runner_temp}/.gates-wheel-venv\n"
    )
    steps = {step.get("name"): step.get("run", "") for step in job["steps"]}
    install = steps[
        "Install the folded epistemic-graph wheel into a clean throwaway venv"
    ]
    assert 'python -m venv "$EG_GATES_WHEEL_VENV"' in install
    assert install.count('"$EG_GATES_WHEEL_VENV/bin/pip"') == 2
    consumers = {
        "numpy-parity gate (compiled kernel vs numpy)",
        "numpy-free boundary contract (NE-249)",
        "engine kernel smoke test (import epistemic_graph.engine)",
        "installed-wheel no-source-tree-import check (EG-CONTRACT-R002.3)",
    }
    actual = {
        name
        for name, command in steps.items()
        if '"$EG_GATES_WHEEL_VENV/bin/python"' in command
    }
    assert actual == consumers
    for name, command in steps.items():
        if name != initialize["name"]:
            assert ".gates-wheel-venv" not in command


@pytest.mark.parametrize("context", ["runner", "env", "steps", "job"])
def test_job_environment_uses_only_allowed_expression_contexts(context: str) -> None:
    # These runtime contexts are available to steps, not job-level env.
    # actionlint validates the complete workflow expression grammar separately.
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    for name, job in jobs.items():
        for value in job.get("env", {}).values():
            if isinstance(value, str) and "${{" in value:
                assert f"{context}." not in value, name
                assert f"{context}[" not in value, name
