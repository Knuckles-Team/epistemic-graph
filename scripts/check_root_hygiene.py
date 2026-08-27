#!/usr/bin/env python3
"""Repository-root hygiene gate.

The repo root is the first thing a reader of a public GitHub project sees, and
it is where scratch files accumulate fastest: a one-off proof artifact from a
CI experiment, a scratch note, a directory that belongs to the workspace rather
than to this package. Two such files (``runner_proof2.md``,
``reports_runner_proof.md`` -- both of which literally said "safe to delete" in
their own body) survived long enough to be published, and a stray ``plans/``
directory carried workspace-level program content into a package repo.

This gate enforces an **allowlist**, not a denylist. A denylist only catches the
junk somebody already thought of; an allowlist means a genuinely new root entry
has to be justified once, deliberately, by someone editing this file (for a
dotfile) or the sibling ``.repo-layout.toml`` manifest (for everything else).

It reads the **tracked** file set (``git ls-files``), never the filesystem --
walking the filesystem makes a gate fire on build output and gitignored
artifacts, which is how gates earn a reputation for crying wolf and get
disabled (see BUG-043, where exactly that happened to the workspace_helpers
chokepoint gate).

Ported verbatim (engine + docstrings) from agent-utilities' original of this
gate (guardrail check-root-hygiene), which already passes green on
agent-utilities' own ``main`` -- reused rather than reinvented per this
fleet's Extend-Before-Invent convention (see MEMORY
``gate-tool-published-before-gate-adopted``). CX-HYG-01 generalized the
original from a hardcoded ``ALLOWED_DIRS``/``ALLOWED_FILES`` pair into a
manifest-driven engine precisely so this port needs ZERO logic changes: the
only thing that differs between repos is ``ALLOWED_DOTFILES`` immediately
below (this repo's own tracked dot-files: ``.cargo-audit-allow.txt`` and
``.vulture_ignore`` instead of agent-utilities' ``.security-audit-allow.txt``)
and the contents of this repo's own ``.repo-layout.toml``. Two holes this
engine closed relative to the pre-CX-HYG-01 original, both of which let a
real tracked artifact sit unchallenged at a repo root:

1. Dotfiles used to be allowed as an unenumerated CLASS ("anything starting
   with '.' is conventional config"). That is what let a tracked
   ``.liveness_baseline.json`` -- a self-writing ratchet, not a convention --
   sit at agent-utilities' root unchallenged. Dotfiles are now split: a
   conventional, self-describing dot-FILE (``.gitignore``,
   ``.pre-commit-config.yaml``, ...) is enumerated in ``ALLOWED_DOTFILES``
   below; a dot-DIRECTORY (``.github``, ``.security``, ...) is repo-specific
   enough to need a stated reason, so it is declared in the manifest's
   ``[dirs]`` table like any other directory.
2. ``FORBIDDEN_ANYWHERE`` -- some tool-written files are ratchets that
   INSTALL THEMSELVES the moment a gate or developer runs the tool that
   writes them (``kiss check`` -> ``.kissconfig``; the retired
   ``check_complexity.py --write`` -> ``.complexity-baseline.json``). Being on
   an allowlist never rescues one of these; they are rejected anywhere in the
   tracked tree, not just at the root, because a self-calibrating baseline is
   exactly the ratchet shape this ecosystem has banned project-wide (see
   MEMORY "NO RATCHETS -- expose tech debt").
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _git_subprocess_env import (  # noqa: E402
    sanitized_git_env,
    strip_inherited_git_repository_env,
)

# NE-059-class hardening (see check_tracked_privacy.py's identical fix): a
# real `git commit`/`git push` exports GIT_DIR/GIT_INDEX_FILE into every hook
# subprocess it runs, and `git -C <root> ls-files` does NOT override them --
# those env vars win over -C's path-based repository discovery. Strip once,
# process-wide, at import time, AND pass an explicit sanitized env= at the one
# call site below, so this gate can never silently resolve against the wrong
# repository (or a poisoned index) and report a false-clean root.
strip_inherited_git_repository_env()

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / ".repo-layout.toml"

# Dot-FILES that are conventional and self-describing -- their name alone
# tells a reader what tool owns them, so (unlike a directory, and unlike the
# non-dot files in the manifest) they don't need a stated reason. Each of
# these is derived from what is actually tracked at this repo's root; adding
# a new one here is the deliberate-justification act for this class, same as
# editing the manifest is for everything else.
ALLOWED_DOTFILES: frozenset[str] = frozenset(
    {
        # The manifest THIS GATE READS. A bootstrap omission: without it the
        # gate fails on its own config file, so wiring it as a blocking hook
        # would have bricked every commit in the repo. Caught by the known-bad
        # proof, not by review.
        ".repo-layout.toml",
        ".bumpversion.cfg",  # release version bump config (bump2version)
        ".cargo-audit-allow.txt",  # risk-accepted RUSTSEC/OSV ledger (cargo-deny gate)
        ".codespellignore",  # codespell false-positive word list
        ".dockerignore",  # Docker build-context exclusions
        ".env.example",  # non-secret catalog of explicit process-env keys
        ".gitattributes",  # git attributes (line endings, diff drivers, ...)
        ".gitignore",  # git exclusion patterns
        ".mergequeue.yaml",  # merge-queue config
        ".pre-commit-config.yaml",  # pre-commit hook config
        ".vulture_ignore",  # vulture dead-code false-positive whitelist
    }
)

# Self-installing config files a tool writes to calibrate itself against its
# own past runs -- a ratchet that hides findings by construction the moment it
# is tracked, anywhere in the tree, not just at the root:
#   .kissconfig               -- `kiss check` writes a self-calibrating config
#   .complexity-baseline.json -- check_complexity.py's retired `--write` wrote
#                                 a self-calibrating complexity baseline
# Being declared in the manifest never rescues one of these; see the module
# docstring.
FORBIDDEN_ANYWHERE: frozenset[str] = frozenset(
    {".kissconfig", ".complexity-baseline.json"}
)


def _tracked_paths() -> list[str]:
    """All tracked paths, repo-root-relative, via ``git ls-files``.

    Resolves the repo root from this script's own location rather than
    trusting the caller's cwd (a hook can be invoked from anywhere), and passes
    a sanitized ``env=`` so an inherited GIT_DIR/GIT_INDEX_FILE from an outer
    ``git commit`` can never redirect ``-C``'s resolution to a different
    repository or a stale index (see the module docstring / NE-059).
    """
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "-z"],
        capture_output=True,
        check=True,
        text=True,
        env=sanitized_git_env(),
    ).stdout
    return [p for p in out.split("\0") if p]


def tracked_root_entries(paths: list[str]) -> tuple[set[str], set[str]]:
    """Return (root_files, root_dirs) from the tracked path list."""
    files: set[str] = set()
    dirs: set[str] = set()
    for path in paths:
        head, sep, _ = path.partition("/")
        if sep:
            dirs.add(head)
        else:
            files.add(head)
    return files, dirs


class ManifestError(Exception):
    """The manifest itself is missing or malformed -- not a hygiene finding,
    a setup error. Reported distinctly so a reader doesn't mistake a typo'd
    TOML table for a stray root file."""


def load_manifest() -> tuple[dict[str, str], dict[str, str]]:
    """Load ``.repo-layout.toml``'s ``[dirs]``/``[files]`` tables.

    Every value must be a non-empty string reason -- an empty or missing
    reason defeats the point of the manifest (see its own header: "a manifest
    full of misc is worse than no manifest").
    """
    if not MANIFEST_PATH.exists():
        raise ManifestError(
            f"{MANIFEST_PATH} does not exist. Every repo this gate runs in "
            "needs a .repo-layout.toml declaring its tracked root entries "
            "(see check_root_hygiene.py's module docstring)."
        )
    try:
        data = tomllib.loads(MANIFEST_PATH.read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ManifestError(f"{MANIFEST_PATH} is not valid TOML: {exc}") from exc

    dirs = data.get("dirs", {})
    files = data.get("files", {})

    for table_name, table in (("dirs", dirs), ("files", files)):
        if not isinstance(table, dict):
            raise ManifestError(f"{MANIFEST_PATH}: [{table_name}] must be a table")
        for name, reason in table.items():
            if not isinstance(reason, str) or not reason.strip():
                raise ManifestError(
                    f"{MANIFEST_PATH}: [{table_name}].{name} needs a non-empty "
                    "string reason, not a placeholder"
                )

    return dict(dirs), dict(files)


def main() -> int:
    try:
        declared_dirs, declared_files = load_manifest()
    except ManifestError as exc:
        print(f"FAIL: {exc}")
        return 1

    paths = _tracked_paths()
    root_files, root_dirs = tracked_root_entries(paths)

    # FORBIDDEN_ANYWHERE: scan the WHOLE tracked tree, not just the root --
    # these ratchets install themselves wherever the tool that writes them is
    # run, and being nested somewhere plausible-looking is not a defense.
    forbidden_hits = sorted(
        p for p in paths if Path(p).name in FORBIDDEN_ANYWHERE
    )

    undeclared_dirs = sorted(d for d in root_dirs if d not in declared_dirs)
    stale_dirs = sorted(d for d in declared_dirs if d not in root_dirs)

    undeclared_dotfiles = sorted(
        f
        for f in root_files
        if f.startswith(".") and f not in ALLOWED_DOTFILES and f not in declared_files
    )
    undeclared_files = sorted(
        f for f in root_files if not f.startswith(".") and f not in declared_files
    )
    # A manifest [files] entry only ever justifies a NON-dot file (dot-files
    # go through ALLOWED_DOTFILES instead) -- a dot-file accidentally declared
    # in the manifest would be silently ignored by the check above, which
    # would hide a class mismatch rather than report it.
    misfiled_dotfiles = sorted(f for f in declared_files if f.startswith("."))
    stale_files = sorted(
        f
        for f in declared_files
        if f not in root_files and f not in misfiled_dotfiles
    )

    ok = not (
        forbidden_hits
        or undeclared_dirs
        or stale_dirs
        or undeclared_dotfiles
        or undeclared_files
        or misfiled_dotfiles
        or stale_files
    )

    if ok:
        print(
            f"root hygiene: clean ({len(root_files)} root files, "
            f"{len(root_dirs)} root dirs, {len(declared_dirs)} declared dirs, "
            f"{len(declared_files)} declared files)"
        )
        return 0

    print("FAIL: repository-root hygiene violations.\n")

    if forbidden_hits:
        print("  Self-installing ratchet config tracked anywhere in the tree:")
        for p in forbidden_hits:
            print(f"    FORBIDDEN  {p}")
        print(
            "    -> delete it and stop tracking it; add its name to .gitignore\n"
            "       if it is not already there (check_gitignore_convergence.py\n"
            "       enforces that for the shared REQUIRED set).\n"
        )

    for d in undeclared_dirs:
        print(f"  DIR   {d}/  (undeclared)")
    for f in undeclared_files:
        print(f"  FILE  {f}  (undeclared)")
    for f in undeclared_dotfiles:
        print(f"  DOTFILE  {f}  (not in ALLOWED_DOTFILES or the manifest)")
    for f in misfiled_dotfiles:
        print(f"  FILE  {f}  (a dot-file was declared in [files]; add it to")
        print("           ALLOWED_DOTFILES in check_root_hygiene.py instead)")
    for d in stale_dirs:
        print(f"  DIR   {d}/  (declared in {MANIFEST_PATH.name} but no longer tracked)")
    for f in stale_files:
        print(f"  FILE  {f}  (declared in {MANIFEST_PATH.name} but no longer tracked)")

    if undeclared_dirs or undeclared_files or undeclared_dotfiles:
        print(
            "\nPick the one that is true for each undeclared entry:\n"
            "  * it is scratch/proof output   -> delete it (it should never have been committed)\n"
            "  * it belongs to the workspace  -> move it to ${WORKSPACE_ROOT}/, not this package\n"
            "  * it belongs inside a package  -> move it under the package source dir or scripts/\n"
            "  * it genuinely belongs at root -> add a one-line reason to .repo-layout.toml\n"
            "    (dirs/files) or, for a conventional self-describing dot-file, to\n"
            "    ALLOWED_DOTFILES in scripts/check_root_hygiene.py\n"
        )
    if stale_dirs or stale_files:
        print(
            "\nA declared .repo-layout.toml entry no longer exists in the tracked tree.\n"
            "Remove it from the manifest -- a stale entry is exactly the fiction this\n"
            "manifest exists to prevent (see its own header).\n"
        )

    return 1


if __name__ == "__main__":
    sys.exit(main())
