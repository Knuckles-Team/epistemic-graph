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


def _fetch_origin_main() -> None:
    """Best-effort restore of the main-side ref (D-MW-9: the gate itself
    refuses to guess a range). PR checkouts that omit it — the python-suite
    job's head-branch checkout is one — still carry the origin remote, so
    an explicit fetch of main restores the gate's full-range base. It is a
    no-op where the ref already exists (the caller only invokes it when the
    ref is absent) and degrades to a failed attempt, checked by the caller,
    in an offline environment. Shallow checkouts need --unshallow: a plain
    fetch of main stops at the clone's shallow boundary, which makes
    origin/main look like a history whose only reachable commit is its tip
    and silently widens the scanned range to most of main's history
    (observed 2026-10-16: 1.9M added lines, 5 pre-existing main-side
    fixture hits)."""
    shallow = (
        subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=str(ROOT),
            capture_output=True,
            check=False,
        ).stdout.strip()
        == b"true"
    )
    args = ["git", "fetch", "--quiet"]
    if shallow:
        args.append("--unshallow")
    args += ["origin", "main"]
    subprocess.run(
        args,
        cwd=str(ROOT),
        capture_output=True,
        check=False,
        timeout=600,
    )


def _full_range_base(gate) -> str:
    """The gate's full commit-range base, per the row's acceptance
    statement ("the full commit-range secret-history scan reports zero
    credential hits"). origin/main when the ref exists (local checkouts,
    push-to-main runs); checkouts that omit it fetch it explicitly (a
    shallow head-branch checkout still carries the origin remote); a PR
    merge-commit checkout (actions/checkout of refs/pull/N/merge) can fall
    back to the main-side parent of the merge commit. If neither resolves,
    the environment cannot honour the acceptance, so the test fails loudly
    rather than scanning nothing or a guessed range (D-MW-9)."""
    if not _ref_exists("origin/main"):
        _fetch_origin_main()
    for base in ("origin/main", "HEAD^2"):
        if _ref_exists(base):
            return base
    raise RuntimeError(
        "cannot resolve the full scan range: origin/main unavailable "
        "(ref absent and fetch failed) and HEAD is not a merge commit; "
        "refusing to guess rather than scan nothing (D-MW-9)"
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
