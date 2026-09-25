# BUG-012 fixture provenance (GitHub / GitLab)

Captured for `GOC-27-W01` (BUG-012 — GitHub/GitLab contract mismatch). Every
`*.raw.json` file in this directory is an **unmodified, real API response**
(only pretty-printed and newline-terminated by `gh`/`python3 -m json.tool`
and this repo's `end-of-file-fixer` pre-commit hook), not a hand-written
approximation. Capture method and digest are recorded here so a future
recapture can prove drift against a known baseline; the digest is of the
file as committed (post `end-of-file-fixer`), not the byte-for-byte curl/gh
output.

| File | Source | Method | Captured | sha256 (as committed) |
|---|---|---|---|---|
| `github_pulls_list.raw.json` | `GET /repos/Knuckles-Team/agent-webui/pulls?state=all&per_page=2` (real GitHub REST API v3, matches `github_pulls`/`list` tool the `github-mcp` server wraps) | `gh api repos/Knuckles-Team/agent-webui/pulls -f state=all -f per_page=2` (authenticated `gh` CLI) | 2026-08-16 | `7fd0091b7168066778d0736ddcd2fb25f139c8762313ab969ae0926f2a1c548a` |
| `github_actions_runs_list.raw.json` | `GET /repos/Knuckles-Team/agent-webui/actions/runs?per_page=2` → `.workflow_runs[]` (real GitHub REST API v3, matches `github_actions`/`list_runs`) — `head_commit.author`/`committer` `name`/`email` sanitized post-capture (real committer identity -> `"Test Committer"` / `test-committer@example.invalid`, flagged by this repo's `guardrail-tracked-privacy` hook); every other field, and the field shape itself, is untouched | `gh api repos/Knuckles-Team/agent-webui/actions/runs -f per_page=2` | 2026-08-16 | `dbb9c5d7d298fb63b5171a08e2db8435bda6eac6cd4e07323aa0ef6e778786a6` |
| `gitlab_merge_requests_list.raw.json` | `GET /api/v4/projects/gitlab-org%2Fgitlab/merge_requests?state=opened&per_page=2` (real, public, unauthenticated GitLab.com REST API v4 — this workspace has no GitLab token/CLI reachable, so a public project on the real gitlab.com instance stands in for the private instance the `gitlab-mcp` server targets; the wire shape is identical, it is the same API) | `curl -s https://gitlab.com/api/v4/projects/gitlab-org%2Fgitlab/merge_requests?state=opened\&per_page=2` | 2026-08-16 | `7a5f5f2075a61e4b074d69448cf65a48450d50fa356e688f8b46fd9d91ee474b` |
| `gitlab_pipelines_list.raw.json` | `GET /api/v4/projects/278964/pipelines?per_page=2` (same real gitlab.com instance, same caveat as above) | `curl -s https://gitlab.com/api/v4/projects/278964/pipelines?per_page=2` | 2026-08-16 | `03e868ba1bdf1745b6ac7d3eea5f5a5111c5b8381896776d53df401acf2880a9` |

`*.drifted.json` files alongside these are **synthetic negative fixtures**:
a real captured record with exactly one field renamed/removed/retyped, used
only to prove the normalizer fails typed and diagnosable on drift (never
used as a positive/"real" fixture). Each has a comment sibling
`*.drifted.md` naming the exact mutation.

## Concrete mismatches this capture proves (BUG-012)

1. **GitHub — no repository selector supplied by the UI.** `EcosystemView.tsx`
   calls `fetch('/api/enhanced/ecosystem/github/prs')` with no `?repo=`.
   `get_github_prs` (`agent/agent_webui/api_extensions.py`) requires an
   explicit `owner/name` selector (or the `GITHUB_REPO` env fallback); absent
   either, it returns `{status: 'needs_input', prs: [], workflows: []}` — no
   crash, but the UI never gives an operator a way to supply the selector the
   backend contract requires. This is the literal defect BD-012 names
   ("required repository selector is omitted").
2. **GitHub — `GithubPr.checks` has no data source.** `EcosystemView.tsx`'s
   `GithubPr` interface declares (and renders, `pr.checks` badge) a `checks`
   field. Neither the real GitHub API response captured here nor
   `get_github_prs`'s PR mapping (`id/title/author/branch/status` only)
   produce any `checks` value — it renders as an empty badge for every PR,
   always, with no code path that could ever fill it in.
3. **GitHub — `GithubWorkflow.run_number` is dropped in the backend adapter,
   not missing upstream.** The real GitHub Actions run object captured here
   *does* carry `run_number` (see `github_actions_runs_list.raw.json`), but
   `get_github_prs`'s run mapping only extracts `id/name/status/conclusion`
   and never copies `run_number` through — so the field the frontend
   declares and renders is silently dropped one hop before it reaches the
   UI. This is the cleanest instance of the ledger's "fields not returned by
   backend schemas" phrasing: the field exists in the source, the adapter
   just forgot to map it.
4. **GitLab — MR mapping is correct; the frontend drops two real fields.**
   `get_gitlab_mrs`'s MR mapping (`id←iid, project_id, title,
   author←author.username, target_branch, status←state, web_url`) matches
   the real GitLab API shape captured here exactly. But `GitlabMr` in
   `EcosystemView.tsx` only types/uses `id, title, author, target_branch,
   status` — `project_id` and `web_url` are silently discarded, even though
   the normalized envelope this lane defines requires a `source.url`.
5. **GitLab — pipeline mapping is correct; `duration` was already fixed.**
   The real pipelines list captured here confirms GitLab's `/pipelines` list
   endpoint has no `duration` field — `GitlabPipeline.duration?` is already
   correctly optional (pre-existing fix, not new). `project_id` is returned
   by the backend but not typed/used on the frontend (same class of gap as
   MRs, lower severity since pipelines are already grouped by project in the
   UI's data flow).
6. **Zero test coverage.** `EcosystemView.test.tsx` has no fixture or
   assertion at all covering the GitHub/GitLab cards — none of the above
   would be caught by CI today.
