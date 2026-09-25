"""Normalized `ChangeRequest`/`Build` read models for GitHub and GitLab.

CONCEPT:EG.enterprise.normalized-read-models

BUG-012 (GOC-27-W01/W02/W03): the WebUI's GitHub/GitLab cards
(the enterprise browser view) render fields the
backend never populates (`GithubPr.checks`, `GithubWorkflow.run_number`) and
never supply the repository selector `get_github_prs`
(`agent-webui/agent/agent_webui/api_extensions.py`) requires. Root cause:
there was no single normalized shape both providers validated into and no
typed failure when a provider payload drifted from what the adapter assumed
— vendor dicts flowed straight from the MCP tool call to the WebUI response.

This module is the fix for the *shape* half of that: one `ChangeRequest`
model that a GitHub pull request and a GitLab merge request both normalize
into, and one `Build` model that a GitHub Actions run and a GitLab pipeline
both normalize into. Each adapter first validates the raw provider payload
against a strict `_...Raw` pydantic model naming exactly the upstream fields
it depends on; a renamed, removed, or retyped field fails *there*, as a
typed `SchemaMismatchError` with the field path in the reason, never as a
silently-missing value that reaches the WebUI as `undefined`.

Deliberately narrow: this covers the two providers and two models BUG-012's
evidence names, not the full twelve-model registry GOC-27's mission
describes (`WorkItemList`, `Issue`, `Deployment`, `Incident`, ...). That
broader registry, the versioning/compatibility-matrix machinery, and a
generated TypeScript client are GOC-27 lane scope beyond one bug fix — see
the BUG-012 handoff note for what is deliberately left for that lane.

Every model here is a genuine third shape: no field name or type is
GitHub's or GitLab's verbatim. `ChangeRequestData.source_branch` exists
because GitHub calls it `head.ref` and GitLab calls it `source_branch`;
`status` is a normalized bucket derived from each vendor's own state
machine, not a passthrough of either vendor's literal enum.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

__all__ = [
    "Build",
    "BuildData",
    "ChangeRequest",
    "ChangeRequestData",
    "Freshness",
    "SchemaMismatchError",
    "SourceRef",
    "build_from_github_workflow_run",
    "build_from_gitlab_pipeline",
    "change_request_from_github_pull_request",
    "change_request_from_gitlab_merge_request",
]


class SchemaMismatchError(ValueError):
    """Raised when a provider payload no longer matches the shape a
    normalizer depends on.

    This is the typed, diagnosable failure BUG-012's acceptance gate
    requires in place of a silently-missing/undefined field reaching the
    WebUI: ``model``/``source_system`` say which normalized model and
    vendor were being built, ``reason`` carries pydantic's exact
    field-path diagnosis, and ``raw_keys`` is the top-level key set of the
    payload that failed, to make root-causing a real drift fast without
    re-logging the (possibly sensitive) payload itself.
    """

    def __init__(
        self,
        *,
        model: str,
        source_system: str,
        reason: str,
        raw_keys: list[str] | None = None,
    ) -> None:
        self.model = model
        self.source_system = source_system
        self.reason = reason
        self.raw_keys = raw_keys or []
        super().__init__(
            f"{source_system} payload does not match the schema {model} normalization requires: {reason}"
        )


class SourceRef(BaseModel):
    """Where this normalized record came from."""

    model_config = ConfigDict(frozen=True)

    system: Literal["github", "gitlab"]
    record_id: str
    url: str | None = None


class Freshness(BaseModel):
    model_config = ConfigDict(frozen=True)

    observed_at: datetime


class ChangeRequestData(BaseModel):
    """The provider-neutral shape a GitHub PR and a GitLab MR both map into.

    Field names are deliberately neither vendor's: GitHub has no
    `source_branch` (it is `head.ref`) and GitLab has no `checks`
    (`status` folds review/CI state the same way GitHub does its `state`).
    """

    model_config = ConfigDict(frozen=True)

    number: int
    title: str
    author: str | None
    source_branch: str | None
    target_branch: str
    status: Literal["open", "closed", "merged", "unknown"]


class ChangeRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal["enterprise.read-model.v1"] = "enterprise.read-model.v1"
    model: Literal["ChangeRequest"] = "ChangeRequest"
    source: SourceRef
    freshness: Freshness
    data: ChangeRequestData
    extensions: dict[str, Any] = Field(default_factory=dict)


class BuildData(BaseModel):
    """The provider-neutral shape a GitHub Actions run and a GitLab
    pipeline both map into.

    `run_number` is optional because GitLab pipelines expose no per-project
    sequential run number (only a global `id`); it is never fabricated.
    """

    model_config = ConfigDict(frozen=True)

    run_number: int | None
    name: str | None
    status: Literal["queued", "running", "success", "failed", "canceled", "unknown"]


class Build(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal["enterprise.read-model.v1"] = "enterprise.read-model.v1"
    model: Literal["Build"] = "Build"
    source: SourceRef
    freshness: Freshness
    data: BuildData
    extensions: dict[str, Any] = Field(default_factory=dict)


# --------------------------------------------------------------------------
# Provider-raw validators. Each names exactly the upstream fields its
# adapter depends on; pydantic's strict typing turns a rename/removal/
# retype into a `ValidationError` at this boundary, not a `.get(..., None)`
# that reaches the WebUI as a missing value. `extra="ignore"` on purpose --
# fields we don't map are not our concern here (they're a vendor-extension
# candidate for the WebUI's bounded `extensions` display, not a schema
# violation).
# --------------------------------------------------------------------------


class _GitHubPullRequestRaw(BaseModel):
    model_config = ConfigDict(extra="ignore")

    number: int
    title: str
    state: str
    user: dict[str, Any] | None = None
    head: dict[str, Any] | None = None
    html_url: str | None = None
    merged_at: str | None = None


class _GitHubWorkflowRunRaw(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    run_number: int
    name: str | None = None
    status: str
    conclusion: str | None = None
    html_url: str | None = None


class _GitLabMergeRequestRaw(BaseModel):
    model_config = ConfigDict(extra="ignore")

    iid: int
    project_id: int
    title: str
    state: str
    author: dict[str, Any] | None = None
    target_branch: str
    source_branch: str | None = None
    web_url: str | None = None


class _GitLabPipelineRaw(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    project_id: int
    ref: str
    status: str
    web_url: str | None = None


_GITHUB_PR_STATUS: dict[str, Literal["open", "closed", "merged", "unknown"]] = {
    "open": "open",
    "closed": "closed",
}
_GITLAB_MR_STATUS: dict[str, Literal["open", "closed", "merged", "unknown"]] = {
    "opened": "open",
    "closed": "closed",
    "merged": "merged",
    "locked": "closed",
}
_GITHUB_RUN_CONCLUSION: dict[
    str, Literal["queued", "running", "success", "failed", "canceled", "unknown"]
] = {
    "success": "success",
    "failure": "failed",
    "cancelled": "canceled",
    "timed_out": "failed",
    "action_required": "failed",
    "neutral": "success",
    "skipped": "canceled",
    "stale": "canceled",
}
_GITHUB_RUN_STATUS: dict[
    str, Literal["queued", "running", "success", "failed", "canceled", "unknown"]
] = {
    "queued": "queued",
    "waiting": "queued",
    "in_progress": "running",
    "requested": "queued",
    "pending": "queued",
}
_GITLAB_PIPELINE_STATUS: dict[
    str, Literal["queued", "running", "success", "failed", "canceled", "unknown"]
] = {
    "created": "queued",
    "waiting_for_resource": "queued",
    "preparing": "queued",
    "pending": "queued",
    "running": "running",
    "success": "success",
    "failed": "failed",
    "canceled": "canceled",
    "canceling": "canceled",
    "skipped": "canceled",
    "manual": "queued",
    "scheduled": "queued",
}


def _now() -> datetime:
    return datetime.now(UTC)


def change_request_from_github_pull_request(raw: dict[str, Any]) -> ChangeRequest:
    """Normalize one raw GitHub `pulls`/`list` item into a `ChangeRequest`.

    Raises `SchemaMismatchError` if `raw` no longer has the shape this
    adapter depends on (see `_GitHubPullRequestRaw`).
    """
    try:
        parsed = _GitHubPullRequestRaw.model_validate(raw)
    except ValidationError as exc:
        raise SchemaMismatchError(
            model="ChangeRequest",
            source_system="github",
            reason=str(exc),
            raw_keys=sorted(raw.keys()) if isinstance(raw, dict) else None,
        ) from exc

    status: Literal["open", "closed", "merged", "unknown"]
    if parsed.merged_at:
        status = "merged"
    else:
        status = _GITHUB_PR_STATUS.get(parsed.state, "unknown")

    # `base` (the target branch) is not in `_GitHubPullRequestRaw` because a
    # missing target branch should not fail the whole normalization the way
    # a missing `number`/`title`/`state` does -- it degrades to "" instead.
    target_branch = (
        (raw.get("base") or {}).get("ref") or "" if isinstance(raw, dict) else ""
    )

    return ChangeRequest(
        source=SourceRef(
            system="github", record_id=str(parsed.number), url=parsed.html_url
        ),
        freshness=Freshness(observed_at=_now()),
        data=ChangeRequestData(
            number=parsed.number,
            title=parsed.title,
            author=(parsed.user or {}).get("login"),
            source_branch=(parsed.head or {}).get("ref"),
            target_branch=target_branch,
            status=status,
        ),
        extensions={},
    )


def change_request_from_gitlab_merge_request(raw: dict[str, Any]) -> ChangeRequest:
    """Normalize one raw GitLab `merge_requests` item into a `ChangeRequest`.

    Raises `SchemaMismatchError` if `raw` no longer has the shape this
    adapter depends on (see `_GitLabMergeRequestRaw`).
    """
    try:
        parsed = _GitLabMergeRequestRaw.model_validate(raw)
    except ValidationError as exc:
        raise SchemaMismatchError(
            model="ChangeRequest",
            source_system="gitlab",
            reason=str(exc),
            raw_keys=sorted(raw.keys()) if isinstance(raw, dict) else None,
        ) from exc

    return ChangeRequest(
        source=SourceRef(
            system="gitlab",
            record_id=f"{parsed.project_id}:{parsed.iid}",
            url=parsed.web_url,
        ),
        freshness=Freshness(observed_at=_now()),
        data=ChangeRequestData(
            number=parsed.iid,
            title=parsed.title,
            author=(parsed.author or {}).get("username"),
            source_branch=parsed.source_branch,
            target_branch=parsed.target_branch,
            status=_GITLAB_MR_STATUS.get(parsed.state, "unknown"),
        ),
        extensions={"project_id": parsed.project_id},
    )


def build_from_github_workflow_run(raw: dict[str, Any]) -> Build:
    """Normalize one raw GitHub Actions `list_runs` item into a `Build`.

    Raises `SchemaMismatchError` if `raw` no longer has the shape this
    adapter depends on (see `_GitHubWorkflowRunRaw`).
    """
    try:
        parsed = _GitHubWorkflowRunRaw.model_validate(raw)
    except ValidationError as exc:
        raise SchemaMismatchError(
            model="Build",
            source_system="github",
            reason=str(exc),
            raw_keys=sorted(raw.keys()) if isinstance(raw, dict) else None,
        ) from exc

    if parsed.conclusion:
        status = _GITHUB_RUN_CONCLUSION.get(parsed.conclusion, "unknown")
    else:
        status = _GITHUB_RUN_STATUS.get(parsed.status, "unknown")

    return Build(
        source=SourceRef(
            system="github", record_id=str(parsed.id), url=parsed.html_url
        ),
        freshness=Freshness(observed_at=_now()),
        data=BuildData(run_number=parsed.run_number, name=parsed.name, status=status),
        extensions={},
    )


def build_from_gitlab_pipeline(raw: dict[str, Any]) -> Build:
    """Normalize one raw GitLab `pipelines` item into a `Build`.

    Raises `SchemaMismatchError` if `raw` no longer has the shape this
    adapter depends on (see `_GitLabPipelineRaw`).
    """
    try:
        parsed = _GitLabPipelineRaw.model_validate(raw)
    except ValidationError as exc:
        raise SchemaMismatchError(
            model="Build",
            source_system="gitlab",
            reason=str(exc),
            raw_keys=sorted(raw.keys()) if isinstance(raw, dict) else None,
        ) from exc

    return Build(
        source=SourceRef(system="gitlab", record_id=str(parsed.id), url=parsed.web_url),
        freshness=Freshness(observed_at=_now()),
        data=BuildData(
            run_number=None,
            name=parsed.ref,
            status=_GITLAB_PIPELINE_STATUS.get(parsed.status, "unknown"),
        ),
        extensions={"project_id": parsed.project_id, "vendor_status": parsed.status},
    )
