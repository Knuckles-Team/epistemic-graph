"""EG-CONTRACT-R022: the jscpd duplication gate must eliminate a new clone
pair by consolidating the shared implementation, never by a scanner
suppression comment or a baseline-exception file.

Two properties prove this:

1. The gate has no baseline/suppression CLI surface at all -- only
   ``census`` (advisory) and ``enforce`` (fails on any NEW pair versus a
   base ref), with no flag that accepts a list of pairs to ignore.
2. A cwd-local jscpd config file is exactly the mechanism that could silently
   change clone-matching semantics (e.g. widening ``--ignore`` or dropping a
   format) out of band from a reviewed diff. ``guard_ambient_config`` fails
   the run closed instead of silently honoring it.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest
from check_duplication import _parser, guard_ambient_config

pytestmark = pytest.mark.no_engine


@pytest.mark.spec("EG-CONTRACT-R022")
def test_gate_cli_has_no_baseline_or_suppression_flag() -> None:
    parser = _parser()
    subparsers_action = next(
        action
        for action in parser._actions  # type: ignore[attr-defined]
        if isinstance(action, argparse._SubParsersAction)
    )
    enforce = subparsers_action.choices["enforce"]
    flag_names = {
        option for action in enforce._actions for option in action.option_strings
    }
    assert flag_names == {"-h", "--help", "--base-ref", "--diff"}


@pytest.mark.spec("EG-CONTRACT-R022")
def test_ambient_suppression_config_fails_closed(tmp_path: Path) -> None:
    (tmp_path / ".jscpd.json").write_text("{}", encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        guard_ambient_config(tmp_path)
    assert excinfo.value.code == 2
