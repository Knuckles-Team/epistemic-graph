#!/usr/bin/env python3
"""Refuse a ``.config/kiss.toml`` key that kiss would silently drop its table for.

kiss 0.4.12 validates ``[global]`` and ``[test]`` against fixed allow-lists and,
on ANY unknown key, discards the whole table: ``[global]`` with no output at
all, ``[test]`` with one stderr line nobody reads. The census and the staged
hook then run with kiss's defaults -- e.g. ``docs_allowed = []`` fires ``doc``
on every ``///`` -- while looking configured. This is how the pre-0.4.11
``[global] orphan_module_enabled`` key (renamed to ``[test] orphan_detection``)
turned a ~500-finding census into ~84k. A pre-0.4.11 ``[gate]`` table is
refused whole. Usage: ``python3 scripts/kiss_config_keys.py [CONFIG]``; exit 0
when every key is known, 2 otherwise.
"""

from __future__ import annotations

import sys
from pathlib import Path

import tomllib

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / ".config" / "kiss.toml"

#: kiss 0.4.12 ``GLOBAL_KEYS`` (src/gate_config/mod.rs) and
#: ``TEST_SECTION_KEYS`` (src/test_toml.rs).
KNOWN_KEYS = {
    "global": frozenset(
        {
            "min_similarity",
            "duplication_enabled",
            "comment_removal_enabled",
            "docs_allowed",
            "orphan_allowed",
        }
    ),
    "test": frozenset(
        {
            "main_branch",
            "num_jobs",
            "num_jobs_pytest",
            "num_jobs_llvm_cov",
            "watch_settle_seconds",
            "pytest_plugins",
            "ignore",
            "test_coverage_threshold",
            "test_coverage_scope",
            "orphan_detection",
            "max_unit_test_seconds",
            "max_num_tests",
            "cache",
        }
    ),
}
RENAMED_GATE = "[gate] (renamed to [global]/[test] in kiss 0.4.11)"


class UnknownKissKey(RuntimeError):
    """The config holds a key kiss would drop its whole table for."""


def unknown_keys(document: dict) -> list[str]:
    """``table.key`` for every key kiss 0.4.12 would silently drop its table for."""

    unknown = [
        f"{table}.{key}"
        for table, known in KNOWN_KEYS.items()
        for key in sorted(document.get(table) or {})
        if key not in known
    ]
    if "gate" in document:
        unknown.append(RENAMED_GATE)
    return unknown


def require_known_keys(config: Path) -> None:
    """Raise ``UnknownKissKey`` unless every table key in ``config`` is known."""

    try:
        document = tomllib.loads(config.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise UnknownKissKey(f"cannot read {config}: {exc}") from exc
    unknown = unknown_keys(document)
    if unknown:
        raise UnknownKissKey(
            f"{config} has key(s) kiss 0.4.12 does not know: {', '.join(unknown)}; "
            "kiss would silently drop the whole table and run on its defaults"
        )


def main(argv: list[str]) -> int:
    config = Path(argv[0]) if argv else DEFAULT_CONFIG
    try:
        require_known_keys(config)
    except UnknownKissKey as exc:
        print(f"kiss config keys: {exc}", file=sys.stderr)
        return 2
    print(f"kiss config keys: every key in {config} is known to kiss 0.4.12")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
