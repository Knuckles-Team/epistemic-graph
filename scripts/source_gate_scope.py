#!/usr/bin/env python3
"""Avoid irrelevant local source scans only with complete, verified Git evidence."""

from __future__ import annotations

import argparse
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

from scripts.ci_change_scope import documentation_diff, documentation_path

SKILL = "epistemic_graph/skills/epistemic-graph-development/SKILL.md"
SCANNERS = {
    "contract-method-reachability": "check_contract_method_reachability.py",
    "durable-table-registration": "check_durable_table_registration.py",
}


def local_document(path: str) -> bool:
    return documentation_path(path) or path == SKILL


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL)


def verified_range() -> list[str]:
    """Require a complete immutable range and its clean checked-out tip."""
    if git("status", "--porcelain", "-z", "--untracked-files=no"):
        raise ValueError("tracked worktree is dirty")
    base = os.environ.get("PRE_COMMIT_FROM_REF", "")
    head = os.environ.get("PRE_COMMIT_TO_REF", "")
    for ref in (base, head):
        if not re.fullmatch(r"[0-9a-f]{40}", ref):
            raise ValueError("unknown range ref")
        git("rev-parse", "--verify", f"{ref}^{{commit}}")
    if git("rev-parse", "HEAD").decode().strip() != head:
        raise ValueError("checkout is not range tip")
    return [base, head]


def verified_diff(scope: str) -> bytes:
    """Explicit commit ranges share push validation; ordinary commits use index."""
    if scope not in {"staged", "push"}:
        raise ValueError("full scan mode")
    has_range = any(
        key in os.environ for key in ("PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF")
    )
    if scope == "push" or has_range:
        refs = verified_range()
    else:
        git("rev-parse", "--verify", "HEAD^{commit}")
        if git("diff", "--raw", "--no-renames", "--ignore-submodules=none", "-z"):
            raise ValueError("index/worktree disagree")
        refs = ["--cached", "HEAD"]
    return git("diff", "--raw", "--no-renames", "-z", *refs)


def ordinary_documents(filenames: list[str]) -> bool:
    for name in filenames:
        if not local_document(name):
            return False
        mode = Path(name).lstat().st_mode
        if not stat.S_ISREG(mode) or mode & 0o111:
            return False
    return bool(filenames)


def matching_documents(raw: bytes, filenames: list[str]) -> bool:
    if not documentation_diff(raw, local_document):
        return False
    fields = raw.split(b"\0")
    if any(fields[i].split()[-1] == b"D" for i in range(0, len(fields) - 1, 2)):
        return False
    paths = {fields[i].decode() for i in range(1, len(fields) - 1, 2)}
    supplied = set(filenames)
    tracked = set(git("ls-files", "-z").decode().rstrip("\0").split("\0"))
    return paths == supplied and supplied != tracked


def can_skip(scope: str, filenames: list[str]) -> bool:
    if any(key in os.environ for key in ("CI", "GITHUB_ACTIONS", "GITHUB_EVENT_NAME")):
        return False
    try:
        if not ordinary_documents(filenames):
            return False
        flags = git("ls-files", "-v", "-z").split(b"\0")
        if any(item and not item.startswith(b"H ") for item in flags):
            return False
        raw = verified_diff(scope)
        return matching_documents(raw, filenames)
    except (OSError, subprocess.CalledProcessError, ValueError, UnicodeError):
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope", required=True, choices=("staged", "push", "manual", "all-files")
    )
    parser.add_argument("--scanner", required=True, choices=SCANNERS)
    parser.add_argument("filenames", nargs="*")
    args = parser.parse_args()
    if can_skip(args.scope, args.filenames):
        print(f"{args.scanner}: verified local docs-only change; scan unnecessary")
        return 0
    scanner = Path(__file__).with_name(SCANNERS[args.scanner])
    return subprocess.call([sys.executable, str(scanner)])


if __name__ == "__main__":
    raise SystemExit(main())
