#!/usr/bin/env python3
"""BUG-180 (sibling of D-LGI-1): strip git-repository-redirecting env vars
before any script that shells out to ``git`` runs as a real git hook.

``tests/conftest.py``'s ``_strip_inherited_git_repository_env()`` already
fixed this class for the pytest session: a real ``git commit``/``git push``
exports ``GIT_DIR``/``GIT_INDEX_FILE``/``GIT_WORK_TREE`` (and siblings) into
every hook it runs, and ``git -C <other-dir> ...`` does **not** override them
-- ``-C`` only changes the working directory; the repository these env vars
name still wins over path-based discovery. That fix only protects code
reached via ``pytest`` (i.e. after ``tests/conftest.py`` has been imported).

It does **not** protect a gate script invoked *directly* as its own
``language: system`` pre-commit hook -- e.g. ``scripts/check_wiring.py`` and
``scripts/check_current_only_contract.py``, both of which shell out to
``git -C <root> ls-files`` to prefer the git-tracked file set over a raw
``rglob`` walk (BUG-043's rationale: a raw walk also picks up gitignored,
generated build output). Confirmed live 2026-08-15 (BUG-180): running
``scripts/check_liveness.py`` (which imports ``check_wiring.py``'s resolver)
under a real ``git commit`` reported a false liveness regression --
``orphan_modules: 4 -> 196``, ``dead_definitions: 527 -> 579`` -- with ZERO
source changes. Reproduced deterministically outside git entirely by setting
``GIT_DIR``/``GIT_INDEX_FILE`` to the worktree's real gitdir/index by hand:
identical 196/579. Root cause: ``_tracked_or_walked``'s ``git -C str(root)
ls-files`` call passes no explicit ``env=``, so it inherits the outer git
commit's ``GIT_DIR``/``GIT_INDEX_FILE``, which win over ``-C``'s path -- the
call resolves against the WRONG repository/index, returns an empty or wrong
tracked-file list, and the function silently falls back to the untracked
``rglob`` walk, which sweeps in ``.venv``, build output, and other
gitignored content the ratchet was never meant to see.

This is the same defect class as D-LGI-1, in a **different, wider** blast
radius: ~30 ``scripts/*.py`` gate scripts independently reimplement the same
"prefer git ls-files, fall back to a walk" pattern, each with its own
unprotected ``subprocess.run(["git", ...])`` call (see BUG-180 in
``plans/graph-os-completion-program/BUG-LEDGER.md`` for the full audit list).
This module is the reusable primitive so each site can adopt one fix instead
of copy-pasting the strip logic N times; it currently guards the two sites
proven to break a real gate (``check_wiring.py``, ``check_current_only_contract.py``).
The remaining sites are the same class and not yet migrated -- tracked as
open follow-up in BUG-180, not silently left unrecorded.
"""

from __future__ import annotations

import os

#: Mirrors ``tests/conftest.py``'s ``_DANGEROUS_GIT_ENV_VARS`` exactly (kept as
#: an independent copy rather than an import: this module must stay import-safe
#: from a bare ``python3 scripts/<gate>.py`` invocation with no ``tests/``
#: package on ``sys.path``, and ``tests/conftest.py`` is pytest-session-scoped
#: infrastructure, not a library other scripts should depend on).
_DANGEROUS_GIT_ENV_VARS = (
    "GIT_DIR",
    "GIT_INDEX_FILE",
    "GIT_WORK_TREE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CEILING_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_NAMESPACE",
)

#: GOC-71's quieter sibling leak (see ``tests/conftest.py`` for the full
#: rationale): ``GIT_AUTHOR_*``/``GIT_COMMITTER_*``/``GIT_CONFIG*`` mis-author
#: or mis-configure a git call made without its own explicit override.
_DANGEROUS_GIT_ENV_PREFIXES = ("GIT_AUTHOR_", "GIT_COMMITTER_", "GIT_CONFIG")


def strip_inherited_git_repository_env() -> None:
    """Process-wide chokepoint: pop the dangerous vars from ``os.environ``
    once, at module import time, so every subsequent ``subprocess.run(["git",
    ...])`` in this process -- including ones that never pass their own
    ``env=`` -- is safe. Call this at the top of any gate script that shells
    out to git, immediately after the stdlib imports. Does nothing when these
    vars were never set (the common case outside a real ``git commit``)."""
    for name in _DANGEROUS_GIT_ENV_VARS:
        os.environ.pop(name, None)
    for name in list(os.environ):
        if name.startswith(_DANGEROUS_GIT_ENV_PREFIXES):
            os.environ.pop(name, None)


def sanitized_git_env(base: dict[str, str] | None = None) -> dict[str, str]:
    """Return a copy of ``base`` (default ``os.environ``) with the dangerous
    vars stripped, for a call site that wants to pass an explicit ``env=``
    rather than mutate the whole process's environment."""
    env = dict(base if base is not None else os.environ)
    for name in list(env):
        if name in _DANGEROUS_GIT_ENV_VARS or name.startswith(
            _DANGEROUS_GIT_ENV_PREFIXES
        ):
            env.pop(name, None)
    return env
