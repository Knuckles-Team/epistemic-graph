"""BUG-012 (GOC-27-W01/W02/W03): GitHub/GitLab normalized read-model tests.

Positive cases run the adapters against real, captured API responses (see
`tests/fixtures/enterprise/github_gitlab/MANIFEST.md` for provenance/digests
— never a hand-written approximation). Negative cases run them against a
`*.drifted.json` sibling — a real record with exactly one field
renamed/removed/retyped — and assert the adapter fails typed and
diagnosable (`SchemaMismatchError`) instead of producing a normalized
record with a silently-missing field.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from epistemic_graph.enterprise_read_models import (
    SchemaMismatchError,
    build_from_github_workflow_run,
    build_from_gitlab_pipeline,
    change_request_from_github_pull_request,
    change_request_from_gitlab_merge_request,
)

FIXTURES = Path(__file__).parent / "fixtures" / "enterprise" / "github_gitlab"


def _load(name: str) -> list[dict]:
    return json.loads((FIXTURES / name).read_text())


# --------------------------------------------------------------------------
# Positive: real captured payloads normalize cleanly and match the source
# values exactly (no guessed/omitted field).
# --------------------------------------------------------------------------


def test_change_request_from_real_github_pull_request():
    raw = _load("github_pulls_list.raw.json")[0]
    cr = change_request_from_github_pull_request(raw)

    assert cr.model == "ChangeRequest"
    assert cr.source.system == "github"
    assert cr.data.number == raw["number"]
    assert cr.data.title == raw["title"]
    assert cr.data.author == raw["user"]["login"]
    assert cr.data.source_branch == raw["head"]["ref"]
    assert cr.data.target_branch == raw["base"]["ref"]
    # merged_at is set on this fixture -> normalized status must be "merged",
    # not a passthrough of GitHub's own "closed" state.
    assert raw["merged_at"]
    assert cr.data.status == "merged"


def test_build_from_real_github_workflow_run():
    raw = _load("github_actions_runs_list.raw.json")[0]
    build = build_from_github_workflow_run(raw)

    assert build.model == "Build"
    assert build.source.system == "github"
    # The exact field BUG-012 found missing end-to-end: the adapter must
    # carry it through even though the WebUI backend adapter (separately
    # fixed) had been dropping it.
    assert build.data.run_number == raw["run_number"]
    assert build.data.name == raw["name"]


def test_change_request_from_real_gitlab_merge_request():
    raw = _load("gitlab_merge_requests_list.raw.json")[0]
    cr = change_request_from_gitlab_merge_request(raw)

    assert cr.model == "ChangeRequest"
    assert cr.source.system == "gitlab"
    assert cr.data.number == raw["iid"]
    assert cr.data.title == raw["title"]
    assert cr.data.author == raw["author"]["username"]
    assert cr.data.target_branch == raw["target_branch"]
    assert cr.extensions["project_id"] == raw["project_id"]
    assert raw["state"] == "opened"
    assert cr.data.status == "open"


def test_build_from_real_gitlab_pipeline():
    raw = _load("gitlab_pipelines_list.raw.json")[0]
    build = build_from_gitlab_pipeline(raw)

    assert build.model == "Build"
    assert build.source.system == "gitlab"
    # GitLab pipelines carry no per-project sequential run number -- never
    # fabricate one.
    assert build.data.run_number is None
    assert build.extensions["project_id"] == raw["project_id"]
    assert build.extensions["vendor_status"] == raw["status"]


# --------------------------------------------------------------------------
# Known-bad proof: a drifted provider field must fail typed and
# diagnosable, not reach the WebUI as a missing/undefined value.
# --------------------------------------------------------------------------


def test_github_pull_request_drift_fails_typed_not_silent():
    """`state` renamed to `pr_state` (simulating a vendor field rename)."""
    raw = _load("github_pulls_list.drifted.json")[0]
    assert "state" not in raw
    assert "pr_state" in raw

    with pytest.raises(SchemaMismatchError) as exc_info:
        change_request_from_github_pull_request(raw)

    err = exc_info.value
    assert err.model == "ChangeRequest"
    assert err.source_system == "github"
    assert "state" in err.reason
    assert "pr_state" in err.raw_keys


def test_github_workflow_run_missing_optional_field_degrades_not_crashes():
    """`conclusion` dropped entirely (simulating an in-progress-run shape).

    `conclusion` is optional on the raw shape (a genuinely running workflow
    has none) -- this drift must NOT raise. It proves the normalizer only
    treats the fields the model declares required as fatal; `run_number`
    (the field BUG-012 actually found missing end-to-end) still comes
    through untouched.
    """
    raw = _load("github_actions_runs_list.drifted.json")[0]
    assert "conclusion" not in raw

    build = build_from_github_workflow_run(raw)
    assert build.data.run_number == raw["run_number"]


def test_gitlab_merge_request_drift_fails_typed_not_silent():
    """`iid` removed (simulating GitLab's id/iid ambiguity being mis-mapped)."""
    raw = _load("gitlab_merge_requests_list.drifted.json")[0]
    assert "iid" not in raw

    with pytest.raises(SchemaMismatchError) as exc_info:
        change_request_from_gitlab_merge_request(raw)

    err = exc_info.value
    assert err.model == "ChangeRequest"
    assert err.source_system == "gitlab"
    assert "iid" in err.reason


def test_gitlab_pipeline_drift_fails_typed_not_silent():
    """`status` retyped from a string to an object (simulating a v5 shape change)."""
    raw = _load("gitlab_pipelines_list.drifted.json")[0]
    assert isinstance(raw["status"], dict)

    with pytest.raises(SchemaMismatchError) as exc_info:
        build_from_gitlab_pipeline(raw)

    err = exc_info.value
    assert err.model == "Build"
    assert err.source_system == "gitlab"
    assert "status" in err.reason


def test_malformed_non_dict_payload_fails_typed():
    """A wrong-shape (non-dict) payload must not raise an unhandled
    AttributeError/TypeError deep inside the adapter -- it is still a typed
    `SchemaMismatchError`."""
    with pytest.raises(SchemaMismatchError):
        change_request_from_github_pull_request([])  # type: ignore[arg-type]
