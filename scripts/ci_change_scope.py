#!/usr/bin/env python3
"""Conservatively identify documentation-only PRs; other events run full CI."""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Callable
from pathlib import PurePosixPath

ROOT_DOCUMENTS = frozenset(
    {"README.md", "CHANGELOG.md", "CONTRIBUTING.md", "AGENTS.md", "CLAUDE.md"}
)


def documentation_path(path: str) -> bool:
    """Only non-executable prose and generated spec status can bypass builds."""
    parts = PurePosixPath(path).parts
    if path in ROOT_DOCUMENTS:
        return True
    if len(parts) < 2 or parts[0] not in {"docs", "specs", ".specify", "architecture"}:
        return False
    return path.endswith(".md") or (
        len(parts) == 3 and parts[0] == "specs" and parts[2] == "status.json"
    )


def documentation_diff(
    raw: bytes, path_predicate: Callable[[str], bool] = documentation_path
) -> bool:
    """Read --raw -z --no-renames, retaining deletions and executable modes."""
    fields = raw.split(b"\0")
    if fields[-1] != b"" or len(fields) < 3 or len(fields) % 2 != 1:
        return False
    for index in range(0, len(fields) - 1, 2):
        metadata = fields[index].split()
        if len(metadata) != 5 or not metadata[0].startswith(b":"):
            return False
        modes = {metadata[0][1:], metadata[1]}
        if not modes <= {b"100644", b"000000"} or b"100644" not in modes:
            return False
        if metadata[4] not in {b"A", b"M", b"D"}:
            return False
        try:
            path = fields[index + 1].decode("utf-8")
        except UnicodeDecodeError:
            return False
        if not path_predicate(path):
            return False
    return True


def requires_code(event: str, base: str, head: str) -> bool:
    """Unknown refs, unavailable history and empty comparisons run full CI."""
    if event != "pull_request" or not all(
        re.fullmatch(r"[0-9a-f]{40}", ref) for ref in (base, head)
    ):
        return True
    try:
        result = subprocess.run(
            ["git", "diff", "--raw", "--no-renames", "-z", f"{base}...{head}"],
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return True
    return not documentation_diff(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True)
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    args = parser.parse_args()
    print(f"code={str(requires_code(args.event, args.base, args.head)).lower()}")


if __name__ == "__main__":
    main()
