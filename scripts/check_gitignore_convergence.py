#!/usr/bin/env python3
"""Assert this repo's .gitignore carries the fleet-shared REQUIRED set.

Ported identically (CX-HYG-01) across agent-utilities, epistemic-graph, and
agent-webui -- ``REQUIRED`` below is the SAME literal set in all three copies
on purpose: every rule in it exists because ONE of these three repos got
burned by exactly the thing it excludes, and the other two are not
categorically immune just because they have not been burned yet (a JS-less
repo can still grow a `dist-*/` the day it gains a build step; a Rust-less
repo already inherited `target/` from the generic-Python-template boilerplate
without anyone deciding it needed a Rust build). Convergence is the point --
if one repo's rule genuinely does not apply, that is a conversation to change
``REQUIRED`` here (and in the other two copies), not a reason to special-case
this repo's own ``.gitignore``.

Two independent checks:

1. NEGATIVE (``.gitignore`` protects the future): every token in ``REQUIRED``
   must appear as its own line in ``.gitignore``, ignoring a purely
   presentational leading/trailing ``/`` -- gitignore's own trailing slash
   only narrows a pattern to directory-only matches; omitting it is strictly
   BROADER (matches a same-named file too), never a gap, so ``.venv`` legally
   satisfies a required ``.venv/`` and is not a finding. A leading ``/``
   anchors a pattern to the repo root; both anchored and unanchored forms
   satisfy the requirement, since dropping the anchor only widens the match.
2. POSITIVE (``.gitignore`` cannot un-track what already slipped through):
   ``git ls-files`` must not already contain a tracked ``dist/``, ``build/``,
   or ``target/`` (or a same-family variant like ``dist-primary/`` /
   ``target-isolated/``) -- a gitignore rule only stops FUTURE adds; if the
   directory is already tracked, the rule is decorative. The check is
   anchored to a path SEGMENT (``^(dist|build|target)(-[^/]*)?/``), not a bare
   prefix: an unanchored ``^(dist|build|target)`` also matches legitimate
   root files like ``build.rs``/``build_backend.py`` and would make this gate
   cry wolf on its very first run in every repo that owns one of those files
   (BUG-043's lesson -- a gate that fires on real files gets disabled).

Reads the tracked file set via ``git ls-files``, matching
``check_root_hygiene.py``'s own rationale for never walking the filesystem.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _git_subprocess_env import (  # noqa: E402
    sanitized_git_env,
    strip_inherited_git_repository_env,
)

# NE-059-class hardening (see check_root_hygiene.py / check_tracked_privacy.py
# for the full rationale): strip inherited GIT_DIR/GIT_INDEX_FILE at import
# time, and pass an explicit sanitized env= at the one call site below.
strip_inherited_git_repository_env()

REPO_ROOT = Path(__file__).resolve().parent.parent
GITIGNORE_PATH = REPO_ROOT / ".gitignore"

# The fleet-shared minimum. Each entry exists because a real incident showed
# its absence costs something -- see the module docstring for the mechanism
# ("REQUIRED must stay shared" is the point, not a suggestion):
#   dist/, dist-*/, sdist/    -- Python/JS build output; dist-*/ is the FAMILY
#                                 match (agent-utilities' dist-primary/ +
#                                 dist-reproduction/, epistemic-graph's
#                                 dist-gates/, BUG-CX-017 family -- `dist/`
#                                 alone does not cover a purpose-suffixed dir)
#   build/, target/, target-*/ -- Rust/generic build output; target-*/ is the
#                                 per-worktree isolated build dir every repo's
#                                 worktree-per-lane convention writes
#   .venv/                     -- the uv-managed virtualenv
#   .mypy_cache/, .pytest_cache/, .ruff_cache/, .hypothesis/, .pytest_tmp/
#                               -- Python tool caches / transient test dirs
#   node_modules/               -- JS/TS dependency tree (npm/pnpm)
#   .kissconfig                 -- `kiss check`'s self-calibrating config; a
#                                 ratchet that installs itself the moment it
#                                 is NOT gitignored, twin to
#                                 check_root_hygiene.py's FORBIDDEN_ANYWHERE
#   reports/, scratch/          -- generated analysis output / AI-agent
#                                 scratch space; belongs in the workspace-level
#                                 reports/, never in a package's commit history
REQUIRED: frozenset[str] = frozenset(
    {
        "dist/",
        "dist-*/",
        "sdist/",
        "build/",
        "target/",
        "target-*/",
        ".venv/",
        ".mypy_cache/",
        ".pytest_cache/",
        ".ruff_cache/",
        ".hypothesis/",
        ".pytest_tmp/",
        "node_modules/",
        ".kissconfig",
        "reports/",
        "scratch/",
    }
)

# Anchored to a path SEGMENT boundary -- see the module docstring's point 2.
_TRACKED_BUILD_OUTPUT_RE = re.compile(r"^(dist|build|target)(-[^/]*)?/")


def _normalize(token: str) -> str:
    """Strip a purely presentational leading/trailing '/'. See docstring
    point 1 for why this is a widening, never a loosening, of the check."""
    return token.strip("/")


def _tracked_paths() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "-z"],
        capture_output=True,
        check=True,
        text=True,
        env=sanitized_git_env(),
    ).stdout
    return [p for p in out.split("\0") if p]


def _gitignore_tokens() -> set[str]:
    if not GITIGNORE_PATH.exists():
        return set()
    tokens: set[str] = set()
    for line in GITIGNORE_PATH.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        tokens.add(_normalize(stripped))
    return tokens


def main() -> int:
    problems: list[str] = []

    gitignore_tokens = _gitignore_tokens()
    missing = sorted(
        req for req in REQUIRED if _normalize(req) not in gitignore_tokens
    )
    if missing:
        problems.append(
            "Missing from .gitignore (fleet-shared REQUIRED set):\n"
            + "\n".join(f"    {m}" for m in missing)
        )

    tracked = _tracked_paths()
    tracked_build_output = sorted(
        p for p in tracked if _TRACKED_BUILD_OUTPUT_RE.match(p)
    )
    if tracked_build_output:
        problems.append(
            "Build-output paths are TRACKED (a .gitignore rule cannot "
            "un-track what already slipped through -- delete these and let "
            "the .gitignore rule do its job going forward):\n"
            + "\n".join(f"    {p}" for p in tracked_build_output)
        )

    if not problems:
        print(
            f"gitignore convergence: clean ({len(REQUIRED)} required entries "
            f"present, 0 tracked build-output paths)"
        )
        return 0

    print("FAIL: .gitignore convergence violations.\n")
    for p in problems:
        print(p)
        print()
    return 1


if __name__ == "__main__":
    sys.exit(main())
