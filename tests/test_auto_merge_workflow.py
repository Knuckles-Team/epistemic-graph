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


class ApiMock:
    def __init__(
        self,
        *,
        statuses: list[dict[str, Any]],
        required: Any,
        status_sha: str = SHA,
    ):
        self.statuses = statuses
        self.required = required
        self.status_sha = status_sha
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
        if path == f"/repos/{REPO_NAME}/commits/{SHA}/status?per_page=100":
            return {"sha": self.status_sha, "statuses": self.statuses}
        if path == f"/repos/{REPO_NAME}/commits/{SHA}/pulls?per_page=100":
            return [_pull_request()]
        raise AssertionError(f"unexpected API path: {path}")


def test_pull_request_trigger_requires_rule_and_exact_source_receipt():
    module = _module()
    api = ApiMock(
        statuses=[
            {
                "context": "Source review",
                "state": "success",
                "updated_at": "2026-10-27T17:00:00Z",
            }
        ],
        required={
            "enforcement_level": "non_admins",
            "contexts": ["CI required", "Source review"],
        },
    )
    merges: list[tuple[int, str]] = []
    logs: list[str] = []

    module.run_gate(
        event_name="pull_request_target",
        event={"number": 42},
        repo=REPO_NAME,
        api_get=api,
        merge=lambda number, sha: merges.append((number, sha)),
        log=logs.append,
    )

    assert merges == [(42, SHA)]
    assert api.calls == [
        f"/repos/{REPO_NAME}/pulls/42",
        f"/repos/{REPO_NAME}/branches/main",
        f"/repos/{REPO_NAME}/commits/{SHA}/status?per_page=100",
    ]
    assert any("required status-check contexts" in line for line in logs)
    assert any("Source review success receipt" in line for line in logs)


def test_missing_required_rule_fails_closed_without_merge():
    module = _module()
    api = ApiMock(
        statuses=[],
        required={"enforcement_level": "non_admins", "contexts": ["CI required"]},
    )
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
    assert api.calls == [
        f"/repos/{REPO_NAME}/pulls/42",
        f"/repos/{REPO_NAME}/branches/main",
    ]


def test_status_event_and_receipt_must_match_same_head():
    module = _module()
    wrong_sha = "b" * 40
    api = ApiMock(
        statuses=[
            {
                "context": "Source review",
                "state": "success",
                "updated_at": "2026-10-27T17:00:00Z",
            }
        ],
        required={
            "enforcement_level": "non_admins",
            "checks": [
                {"context": "CI required", "app_id": -1},
                {"context": "Source review", "app_id": -1},
            ],
        },
        status_sha=wrong_sha,
    )
    merges: list[tuple[int, str]] = []

    with pytest.raises(module.GateNotReady):
        module.run_gate(
            event_name="status",
            event={"context": "Source review", "state": "success", "sha": SHA},
            repo=REPO_NAME,
            api_get=api,
            merge=lambda number, sha: merges.append((number, sha)),
        )
    assert merges == []


def test_same_timestamp_uses_newest_first_api_status():
    module = _module()
    api = ApiMock(
        statuses=[
            {
                "id": 20,
                "context": "Source review",
                "state": "success",
                "updated_at": "2026-10-27T17:00:00Z",
            },
            {
                "id": 19,
                "context": "Source review",
                "state": "failure",
                "updated_at": "2026-10-27T17:00:00Z",
            },
        ],
        required={
            "enforcement_level": "non_admins",
            "contexts": ["CI required", "Source review"],
        },
    )
    merges: list[tuple[int, str]] = []

    module.run_gate(
        event_name="pull_request_target",
        event={"number": 42},
        repo=REPO_NAME,
        api_get=api,
        merge=lambda number, sha: merges.append((number, sha)),
    )

    assert merges == [(42, SHA)]


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
    assert "status" in events
    assert "actions/checkout@" not in source
    assert "github.event.pull_request.head.ref" not in source
    assert "github.event.pull_request.base.ref" not in source
    assert "github.event.pull_request.title" not in source
    assert "github.workflow_sha" in source
    assert "contents: write" in source
    assert "pull-requests: write" in source
    assert "statuses: read" in source
    assert "MERGE_TOKEN: ${{ secrets.GIT_TOKEN }}" in source
    script = (REPO / "scripts" / "auto_merge.py").read_text()
    assert 'token=os.environ["GITHUB_TOKEN"]' in script
    assert 'token=os.environ["MERGE_TOKEN"]' in script
