"""Keep fast documentation PR checks conservative and publication exhaustive."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
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
        ".github/workflows/release.yml",
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
    jobs = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())["jobs"]
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
    assert 'check_rustfmt_scope.py --base "$BASE"' in lint["rustfmt"]["run"]
    assert "cargo fmt --all --check" in lint["rustfmt"]["run"]
    assert "CI change-scope regression tests" in lint


def test_pr_runtime_gates_do_not_wait_for_self_hosted_runners() -> None:
    jobs = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())["jobs"]
    selector = (
        "${{ github.event_name == 'push' && "
        "fromJSON('[\"self-hosted\", \"homelab\"]') || 'ubuntu-latest' }}"
    )
    for name in ("gates", "gates-facade"):
        assert jobs[name]["runs-on"] == selector
        assert jobs[name]["if"] == "needs.changes.outputs.code == 'true'"


def test_workspace_gates_use_hosted_runner_with_required_headers() -> None:
    """EG-CONTRACT-R035: prerequisite installation must work on every event."""
    jobs = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())["jobs"]
    job = jobs["gates-crates"]
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
def test_lint_installs_manifest_rustfmt_before_all_formatting_steps() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
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
    assert "scripts/check_rustfmt_scope.py --base" in rustfmt["run"]
    assert "cargo fmt --all --check" in rustfmt["run"]
