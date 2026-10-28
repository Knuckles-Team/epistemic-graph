"""Mocked safety checks for the trusted PR auto-merge workflow."""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
SHA = "a" * 40
REPO_NAME = "Knuckles-Team/medulla"
pytestmark = pytest.mark.no_engine


def _module():
    spec = importlib.util.spec_from_file_location(
        "medulla_auto_merge", REPO / "scripts" / "auto_merge.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pull_request() -> dict[str, Any]:
    return {
        "number": 42,
        "state": "open",
        "draft": False,
        "base": {"ref": "main", "repo": {"full_name": REPO_NAME}},
        "head": {"sha": SHA},
    }


def _ci_run(
    *,
    run_id: int = 1,
    conclusion: str | None = "success",
    status: str = "completed",
    head_sha: str = SHA,
) -> dict[str, Any]:
    return {
        "id": run_id,
        "name": "CI required",
        "status": status,
        "conclusion": conclusion,
        "head_sha": head_sha,
    }


CHECK_RUNS_PATH = (
    f"/repos/{REPO_NAME}/commits/{SHA}/check-runs?check_name=CI%20required&per_page=100"
)


class ApiMock:
    def __init__(self, *, check_runs: list[dict[str, Any]], required: Any):
        self.check_runs = check_runs
        self.required = required
        self.calls: list[str] = []

    def __call__(self, path: str) -> Any:
        self.calls.append(path)
        if path == f"/repos/{REPO_NAME}/pulls/42":
            return _pull_request()
        if path == f"/repos/{REPO_NAME}/branches/main":
            return {
                "name": "main",
                "protected": True,
                "protection": {
                    "enabled": True,
                    "required_status_checks": self.required,
                },
            }
        if path == CHECK_RUNS_PATH:
            return {"total_count": len(self.check_runs), "check_runs": self.check_runs}
        if path == f"/repos/{REPO_NAME}/commits/{SHA}/pulls?per_page=100":
            return [_pull_request()]
        raise AssertionError(f"unexpected API path: {path}")


REQUIRED = {"enforcement_level": "non_admins", "contexts": ["CI required"]}


def _gate(module, api, *, event_name="pull_request_target", event=None):
    merges: list[tuple[int, str]] = []
    logs: list[str] = []
    module.run_gate(
        event_name=event_name,
        event={"number": 42} if event is None else event,
        repo=REPO_NAME,
        api_get=api,
        merge=lambda number, sha: merges.append((number, sha)),
        log=logs.append,
    )
    return merges, logs


def test_pull_request_trigger_requires_rule_and_ci_required_success():
    module = _module()
    api = ApiMock(check_runs=[_ci_run()], required=REQUIRED)

    merges, logs = _gate(module, api)

    assert merges == [(42, SHA)]
    assert api.calls == [
        f"/repos/{REPO_NAME}/pulls/42",
        f"/repos/{REPO_NAME}/branches/main",
        CHECK_RUNS_PATH,
    ]
    assert any("required status-check contexts" in line for line in logs)
    assert any("CI required success" in line for line in logs)


def test_source_review_is_not_required():
    module = _module()
    assert module.REQUIRED_CONTEXTS == {"CI required"}
    api = ApiMock(check_runs=[_ci_run()], required=REQUIRED)

    merges, _ = _gate(module, api)

    assert merges == [(42, SHA)]


def test_missing_required_rule_fails_closed_without_merge():
    module = _module()
    api = ApiMock(
        check_runs=[],
        required={"enforcement_level": "non_admins", "contexts": ["Other"]},
    )

    with pytest.raises(module.GateNotReady):
        _gate(module, api)
    assert api.calls == [
        f"/repos/{REPO_NAME}/pulls/42",
        f"/repos/{REPO_NAME}/branches/main",
    ]


@pytest.mark.parametrize(
    "runs",
    [
        [],
        [_ci_run(conclusion="failure")],
        [_ci_run(status="in_progress", conclusion=None)],
        [_ci_run(head_sha="b" * 40)],
    ],
)
def test_ci_required_not_green_for_this_head_does_not_arm(runs):
    module = _module()
    api = ApiMock(check_runs=runs, required=REQUIRED)
    merges: list[tuple[int, str]] = []

    with pytest.raises(module.GateNotReady):
        module.run_gate(
            event_name="pull_request_target",
            event={"number": 42},
            repo=REPO_NAME,
            api_get=api,
            merge=lambda number, sha: merges.append((number, sha)),
        )
    assert merges == []


def test_latest_check_run_decides():
    module = _module()
    api = ApiMock(
        check_runs=[_ci_run(run_id=20, conclusion="failure"), _ci_run(run_id=21)],
        required=REQUIRED,
    )
    assert _gate(module, api)[0] == [(42, SHA)]

    api = ApiMock(
        check_runs=[_ci_run(run_id=20), _ci_run(run_id=21, conclusion="failure")],
        required=REQUIRED,
    )
    with pytest.raises(module.GateNotReady):
        _gate(module, api)


def test_check_run_event_arms_the_matching_open_pr():
    module = _module()
    api = ApiMock(check_runs=[_ci_run()], required=REQUIRED)
    event = {
        "check_run": {"name": "CI required", "conclusion": "success", "head_sha": SHA}
    }

    merges, _ = _gate(module, api, event_name="check_run", event=event)

    assert merges == [(42, SHA)]


@pytest.mark.parametrize(
    "check_run",
    [
        {"name": "Source review", "conclusion": "success", "head_sha": SHA},
        {"name": "CI required", "conclusion": "failure", "head_sha": SHA},
    ],
)
def test_check_run_event_ignores_other_or_failed_runs(check_run):
    module = _module()
    api = ApiMock(check_runs=[_ci_run()], required=REQUIRED)

    with pytest.raises(module.GateNotReady):
        _gate(module, api, event_name="check_run", event={"check_run": check_run})
    assert api.calls == []


def test_merge_command_is_head_pinned(monkeypatch):
    module = _module()
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(
        command: list[str], *, check: bool, env: dict[str, str]
    ) -> subprocess.CompletedProcess[str]:
        calls.append((command, env))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    module._gh_merge(42, SHA, repo=REPO_NAME, token="token")

    assert calls[0][0] == [
        "gh",
        "pr",
        "merge",
        "--auto",
        "--merge",
        "--match-head-commit",
        SHA,
        "42",
    ]
    assert calls[0][1]["GH_REPO"] == REPO_NAME
    assert calls[0][1]["GH_TOKEN"] == "token"


def test_empty_merge_token_fails_closed():
    module = _module()

    with pytest.raises(module.ApiFailure, match="MERGE_TOKEN is empty"):
        module._gh_merge(42, SHA, repo=REPO_NAME, token="")


def test_workflow_uses_trusted_events_without_checkout():
    source = (REPO / ".github" / "workflows" / "auto_merge.yml").read_text()
    workflow = yaml.safe_load(source)
    events = workflow.get("on", workflow.get(True))

    assert set(events["pull_request_target"]["types"]) == {
        "opened",
        "reopened",
        "ready_for_review",
        "synchronize",
    }
    assert "main" in events["pull_request_target"]["branches"]
    assert events["check_run"]["types"] == ["completed"]
    assert "status" not in events
    assert "Source review" not in source
    assert "actions/checkout@" not in source
    assert "github.event.pull_request.head.ref" not in source
    assert "github.event.pull_request.base.ref" not in source
    assert "github.event.pull_request.title" not in source
    assert "github.workflow_sha" in source
    assert "contents: write" in source
    assert "pull-requests: write" in source
    assert "checks: read" in source
    assert "MERGE_TOKEN: ${{ secrets.GIT_TOKEN }}" in source
    script = (REPO / "scripts" / "auto_merge.py").read_text()
    assert 'token=os.environ["GITHUB_TOKEN"]' in script
    assert 'token=os.environ["MERGE_TOKEN"]' in script
