#!/usr/bin/env python3
"""Prove an installed kiss is the pinned fork build, not the crates.io 0.4.10.

EG runs kiss 0.4.10 rule semantics from ``KISS_FORK_GIT`` at ``KISS_FORK_REV``:
the 0.4.10 release commit plus the inline-module resolution fix (upstream PR
dsweet99/kiss#48). Without the fix, kiss aborts the census with ``missing
module foreign_tenancy declared from src/server/mod.rs``. Both builds print
``kiss 0.4.10``, so ``--version`` cannot tell them apart; this probe runs a
four-file crate that declares ``mod helper;`` inside an inline
``mod tests { .. }`` block of a non-root file, which only the fork analyzes.

The release workflow installs exactly this rev (tests/test_scanner_distribution.py
asserts the two agree). Usage: ``python3 scripts/kiss_fork.py [KISS_BIN]``.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

KISS_FORK_GIT = "https://github.com/Knucklessg1/kiss"
KISS_FORK_REV = "4d05b0ee01c58318f199848b4d799b12d470d1bd"

PROBE_CRATE = {
    "Cargo.toml": (
        '[package]\nname = "inline_mod_probe"\nversion = "0.1.0"\nedition = "2024"\n'
    ),
    "src/lib.rs": "pub mod a;\n",
    "src/a.rs": "pub fn a() {}\n\n#[cfg(test)]\nmod tests {\n    mod helper;\n}\n",
    "src/a/tests/helper.rs": "#[test]\nfn h() {}\n",
}


class NotForkBuild(RuntimeError):
    """The binary is not the pinned fork build (or could not run the probe)."""


def _write_probe(root: Path) -> None:
    for relative, text in PROBE_CRATE.items():
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")


def require_fork_build(kiss_bin: str, env: dict[str, str] | None = None) -> None:
    """Raise ``NotForkBuild`` unless ``kiss_bin`` resolves inline-module children."""

    with tempfile.TemporaryDirectory(prefix="kiss-fork-probe-") as tmp:
        _write_probe(Path(tmp))
        try:
            result = subprocess.run(
                [kiss_bin, "check", "--lang", "rust", "."],
                cwd=tmp,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
            raise NotForkBuild(f"could not run the kiss fork probe: {exc}") from exc
    output = f"{result.stdout or ''}{result.stderr or ''}"
    if result.returncode != 0 or "missing module" in output:
        raise NotForkBuild(
            f"{kiss_bin} is not the pinned kiss fork build "
            f"({KISS_FORK_GIT} @ {KISS_FORK_REV}); the inline-module probe "
            f"exited {result.returncode}: {output.strip()[-300:]}"
        )


def main(argv: list[str]) -> int:
    kiss_bin = argv[0] if argv else os.environ.get("KISS_BIN", "kiss")
    try:
        require_fork_build(kiss_bin)
    except NotForkBuild as exc:
        print(f"kiss fork probe: {exc}", file=sys.stderr)
        return 2
    print(f"kiss fork probe: {kiss_bin} is the pinned fork build @ {KISS_FORK_REV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
