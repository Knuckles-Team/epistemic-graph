"""EG-CONTRACT-R043: the full-range secret-history gate reports zero hits.

``scripts/security/check_secret_history.py`` is the absolute, forward-looking
secret-history gate: no baseline allowlist file, the only exemptions being the
inline ``# sanitizer:ignore`` marker on the offending line and the digest-
pinned ``secret_history_reviewed_exceptions.toml`` register. This test runs
the gate over its full commit range -- every not-yet-pushed commit, the
complete accumulated change set compared to origin/main -- and requires zero
credential-shaped hits, which is the acceptance statement for the row: test
fixtures (and every other tracked change) carry no credential-shaped
literal.
"""

from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _gate():
    path = ROOT / "scripts" / "security" / "check_secret_history.py"
    spec = importlib.util.spec_from_file_location("eg_secret_history_gate", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ref_exists(ref: str) -> bool:
    proc = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", "--end-of-options", ref],
        cwd=str(ROOT),
        capture_output=True,
        check=False,
    )
    return proc.returncode == 0


def _full_range_base(gate) -> str:
    """The gate's own default full range: origin/main, when the ref exists
    (local checkouts, push-to-main runs). A PR merge-commit checkout
    (actions/checkout of refs/pull/N/merge) carries no origin/main ref; there
    the main-side parent of the merge commit is the full-range base of the
    not-yet-pushed change set. Neither is available is an environment
    failure, never a silently empty scan (D-MW-9)."""
    if _ref_exists("origin/main"):
        return "origin/main"
    if _ref_exists("HEAD^2"):
        return "HEAD^2"
    pytest.fail(
        "cannot resolve the full scan range: no origin/main ref and HEAD is "
        "not a merge commit; refusing to guess rather than scan nothing"
    )


@pytest.mark.spec("EG-CONTRACT-R043")
def test_full_range_secret_history_scan_reports_zero_credential_hits() -> None:
    gate = _gate()
    base = _full_range_base(gate)
    rc, result = gate.check(ROOT, base)
    assert result.get("ok") is True, result.get("error", "gate reported not ok")
    assert result["credentialHits"] == []
    assert result["range"] == f"{base}..HEAD"
    assert rc == 0
