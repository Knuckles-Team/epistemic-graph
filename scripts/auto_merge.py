#!/usr/bin/env python3
"""Fail-closed, trusted-workflow gate for green pull requests.

The workflow fetches this file at github.workflow_sha without checking out
the pull request. All repository state used for the decision is read back
from GitHub's API, and the merge command receives only a validated PR number
and commit SHA.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

CI_REQUIRED_CONTEXT = "CI required"
BASE_BRANCH = "main"
REQUIRED_CONTEXTS = frozenset({CI_REQUIRED_CONTEXT})
SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")

ApiGet = Callable[[str], Any]
Merge = Callable[[int, str], None]
Log = Callable[[str], None]


class GateNotReady(Exception):
    """A safe no-op: the PR is not eligible to arm auto-merge yet."""


class ApiFailure(Exception):
    """The API could not establish the safety preconditions."""


def _sha(value: object) -> str:
    if not isinstance(value, str) or SHA_RE.fullmatch(value) is None:
        raise GateNotReady("the candidate head is not a full commit SHA")
    return value.lower()


def _repo(value: object) -> str:
    if not isinstance(value, str) or REPO_RE.fullmatch(value) is None:
        raise ApiFailure("GITHUB_REPOSITORY is invalid")
    return value


def _positive_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise GateNotReady("the pull request number is invalid")
    return value


def github_get(path: str, *, api_url: str, token: str) -> Any:
    """Perform one authenticated GET; callers pass only fixed/validated paths."""

    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        raise ApiFailure(
            f"GitHub GET {path} returned HTTP {exc.code}: {detail}"
        ) from exc
    except (OSError, ValueError) as exc:
        raise ApiFailure(f"GitHub GET {path} failed: {exc}") from exc


def _get(api_get: ApiGet, path: str, log: Log) -> Any:
    log(f"GET {path}")
    return api_get(path)


def _pull_request(
    *,
    repo: str,
    number: int,
    api_get: ApiGet,
    log: Log,
) -> tuple[int, str]:
    payload = _get(api_get, f"/repos/{repo}/pulls/{number}", log)
    if not isinstance(payload, Mapping):
        raise ApiFailure("pull request API returned a non-object")
    if payload.get("state") != "open" or payload.get("draft") is not False:
        raise GateNotReady("pull request is closed or still a draft")
    base = payload.get("base")
    head = payload.get("head")
    if not isinstance(base, Mapping) or base.get("ref") != BASE_BRANCH:
        raise GateNotReady("pull request base is not main")
    base_repo = base.get("repo")
    if not isinstance(base_repo, Mapping) or base_repo.get("full_name") != repo:
        raise GateNotReady("pull request targets a different repository")
    if not isinstance(head, Mapping):
        raise GateNotReady("pull request head is missing")
    return number, _sha(head.get("sha"))


def _candidate_pr(
    *,
    event_name: str,
    event: Mapping[str, Any],
    repo: str,
    api_get: ApiGet,
    log: Log,
) -> tuple[int, str]:
    if event_name == "pull_request_target":
        return _pull_request(
            repo=repo,
            number=_positive_int(event.get("number")),
            api_get=api_get,
            log=log,
        )
    if event_name != "check_run":
        raise GateNotReady(f"event {event_name!r} is not an auto-merge trigger")
    check_run = event.get("check_run")
    if (
        not isinstance(check_run, Mapping)
        or check_run.get("name") != CI_REQUIRED_CONTEXT
        or check_run.get("conclusion") != "success"
    ):
        raise GateNotReady("check_run event is not a successful CI required")
    head_sha = _sha(check_run.get("head_sha"))
    pulls = _get(
        api_get,
        f"/repos/{repo}/commits/{head_sha}/pulls?per_page=100",
        log,
    )
    if not isinstance(pulls, list):
        raise ApiFailure("commit pull request API returned a non-list")
    matches = [
        pull
        for pull in pulls
        if isinstance(pull, Mapping)
        and pull.get("state") == "open"
        and pull.get("draft") is False
        and isinstance(pull.get("base"), Mapping)
        and pull["base"].get("ref") == BASE_BRANCH
        and isinstance(pull.get("head"), Mapping)
        and _same_sha(pull["head"].get("sha"), head_sha)
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("number"), int):
        raise GateNotReady("the check-run SHA does not identify one open main PR")
    return _pull_request(
        repo=repo,
        number=matches[0]["number"],
        api_get=api_get,
        log=log,
    )


def _same_sha(value: object, expected: str) -> bool:
    return isinstance(value, str) and value.lower() == expected


def _required_contexts(payload: object) -> set[str]:
    if not isinstance(payload, Mapping):
        raise ApiFailure("required-status-check API returned a non-object")
    contexts = {
        value for value in payload.get("contexts", []) if isinstance(value, str)
    }
    checks = payload.get("checks", [])
    if isinstance(checks, list):
        for check in checks:
            if isinstance(check, Mapping) and isinstance(check.get("context"), str):
                contexts.add(check["context"])
    return contexts


def _check_required_rule(*, repo: str, api_get: ApiGet, log: Log) -> None:
    path = f"/repos/{repo}/branches/{BASE_BRANCH}"
    try:
        payload = _get(api_get, path, log)
    except ApiFailure as exc:
        if "HTTP 404" in str(exc):
            raise GateNotReady(
                "main has no readable branch protection summary"
            ) from exc
        raise
    if not isinstance(payload, Mapping):
        raise ApiFailure("branch API returned a non-object")
    if payload.get("protected") is not True:
        raise GateNotReady("main is not protected")
    protection = payload.get("protection")
    if not isinstance(protection, Mapping):
        raise GateNotReady("main has no readable protection details")
    required = protection.get("required_status_checks")
    if not isinstance(required, Mapping):
        raise GateNotReady("main has no required-status-check rule")
    enforcement = required.get("enforcement_level")
    if not isinstance(enforcement, str) or enforcement == "off":
        raise GateNotReady("main required-status-check enforcement is disabled")
    contexts = _required_contexts(required)
    log(f"required status-check contexts: {sorted(contexts)!r}")
    missing = REQUIRED_CONTEXTS - contexts
    if missing:
        raise GateNotReady(
            f"required contexts {sorted(missing)!r} are absent; "
            f"observed {sorted(contexts)!r}"
        )


def _ci_required_passed(*, repo: str, head_sha: str, api_get: ApiGet, log: Log) -> None:
    path = (
        f"/repos/{repo}/commits/{head_sha}/check-runs"
        f"?check_name=CI%20required&per_page=100"
    )
    payload = _get(api_get, path, log)
    if not isinstance(payload, Mapping):
        raise ApiFailure("check-runs API returned a non-object")
    runs = payload.get("check_runs")
    if not isinstance(runs, list):
        raise ApiFailure("check-runs API returned no check_runs list")
    matches = [
        run
        for run in runs
        if isinstance(run, Mapping)
        and run.get("name") == CI_REQUIRED_CONTEXT
        and _same_sha(run.get("head_sha"), head_sha)
        and isinstance(run.get("id"), int)
    ]
    if not matches:
        raise GateNotReady("no CI required check run exists for this head")
    latest = max(matches, key=lambda run: run["id"])
    if latest.get("status") != "completed" or latest.get("conclusion") != "success":
        raise GateNotReady("the latest CI required check run is not success")
    log(f"CI required success matches head {head_sha}")


def run_gate(
    *,
    event_name: str,
    event: Mapping[str, Any],
    repo: str,
    api_get: ApiGet,
    merge: Merge,
    log: Log = print,
) -> None:
    number, head_sha = _candidate_pr(
        event_name=event_name,
        event=event,
        repo=repo,
        api_get=api_get,
        log=log,
    )
    _check_required_rule(repo=repo, api_get=api_get, log=log)
    _ci_required_passed(
        repo=repo,
        head_sha=head_sha,
        api_get=api_get,
        log=log,
    )
    log(f"arming auto-merge for PR {number} at exact head {head_sha}")
    merge(number, head_sha)


def _gh_merge(number: int, head_sha: str, *, repo: str, token: str) -> None:
    if not token:
        raise ApiFailure("MERGE_TOKEN is empty")
    command = [
        "gh",
        "pr",
        "merge",
        "--auto",
        "--merge",
        "--match-head-commit",
        head_sha,
        str(number),
    ]
    environment = os.environ.copy()
    environment.update({"GH_REPO": repo, "GH_TOKEN": token})
    subprocess.run(command, check=True, env=environment)


def main() -> int:
    try:
        event_path = os.environ["GITHUB_EVENT_PATH"]
        with open(event_path, encoding="utf-8") as stream:
            event = json.load(stream)
        run_gate(
            event_name=os.environ["GITHUB_EVENT_NAME"],
            event=event,
            repo=_repo(os.environ["GITHUB_REPOSITORY"]),
            api_get=lambda path: github_get(
                path,
                api_url=os.environ["GITHUB_API_URL"],
                token=os.environ["GITHUB_TOKEN"],
            ),
            merge=lambda number, sha: _gh_merge(
                number,
                sha,
                repo=os.environ["GITHUB_REPOSITORY"],
                token=os.environ["MERGE_TOKEN"],
            ),
        )
    except GateNotReady as exc:
        print(f"auto-merge not armed: {exc}")
    except (ApiFailure, KeyError, json.JSONDecodeError) as exc:
        print(f"auto-merge gate failed closed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
