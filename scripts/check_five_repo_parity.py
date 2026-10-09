"""CLI entry point for EG-CONTRACT-R008.2: validate the resolved checkout.

Wraps the typed contract in ``scripts/repo_root_parity.py`` (EG-CONTRACT-R008.1)
behind a small CLI: given a repository root argument, resolve it and the actual
checkout directory this script is running from, and fail closed (non-zero exit,
no traceback) if they diverge instead of silently comparing against an
unintended checkout.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from scripts.repo_root_parity import RepoRootMismatchError, verify_resolved_checkout


def check_five_repo_parity(requested_root: Path, actual_root: Path) -> int:
    """Return 0 if ``actual_root`` resolves to ``requested_root``, else 1."""
    try:
        verify_resolved_checkout(requested_root, actual_root)
    except RepoRootMismatchError as exc:
        print(f"check_five_repo_parity: {exc}", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "repo_root",
        type=Path,
        help="The repository root this check is expected to run against.",
    )
    args = parser.parse_args(argv)
    actual_root = Path(__file__).resolve().parent.parent
    return check_five_repo_parity(args.repo_root, actual_root)


if __name__ == "__main__":
    raise SystemExit(main())
