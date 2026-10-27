"""Static invariants for the security job's cargo-deny cache."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _security_steps() -> list[dict[str, Any]]:
    workflow = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text())
    return workflow["jobs"]["security"]["steps"]


def _step(steps: list[dict[str, Any]], name: str) -> dict[str, Any]:
    return next(step for step in steps if step.get("name") == name)


def test_cargo_deny_cache_is_versioned_verified_and_main_only():
    steps = _security_steps()
    resolve = _step(steps, "Resolve pinned cargo-deny cache key")
    restore = _step(steps, "Restore pinned cargo-deny")
    install = _step(steps, "Install pinned cargo-deny")
    verify = _step(steps, "Verify pinned cargo-deny")
    save = _step(steps, "Save pinned cargo-deny")
    advisory = _step(steps, "Rust dependency advisory gate")

    resolve_run = resolve["run"]
    assert "load_contract().cargo_deny_version" in resolve_run
    assert "rustc -vV" in resolve_run
    assert "ldd --version" in resolve_run
    assert "$RUNNER_OS" in resolve_run
    assert "$RUNNER_ARCH" in resolve_run
    assert "GITHUB_OUTPUT" in resolve_run

    assert restore["uses"] == (
        "actions/cache/restore@55cc8345863c7cc4c66a329aec7e433d2d1c52a9"
    )
    assert restore["with"]["path"] == ("${{ runner.temp }}/epistemic-graph-cargo-deny")
    assert restore["with"]["key"] == ("${{ steps.cargo-deny-cache-key.outputs.key }}")

    install_run = install["run"]
    assert (
        'cargo install --locked --version "$version" --root "$root" cargo-deny'
        in install_run
    )
    assert 'actual="$("$binary" --version || true)"' in install_run
    assert 'rm -rf "$root"' in install_run
    assert install["env"]["CARGO_DENY_VERSION"] == (
        "${{ steps.cargo-deny-cache-key.outputs.version }}"
    )

    verify_run = verify["run"]
    assert 'test "$("$root/bin/cargo-deny" --version)"' in verify_run
    assert "verified=true" in verify_run

    save_if = save["if"]
    assert "github.event_name == 'push'" in save_if
    assert "github.ref == 'refs/heads/main'" in save_if
    assert "steps.cargo-deny-cache.outputs.cache-hit != 'true'" in save_if
    assert "steps.cargo-deny-verified.outputs.verified == 'true'" in save_if
    assert save["uses"] == (
        "actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9"
    )
    assert save["with"]["key"] == (
        "${{ steps.cargo-deny-cache.outputs.cache-primary-key }}"
    )

    assert advisory["run"] == "bash scripts/check_cargo_advisories.sh"
    assert "continue-on-error" not in advisory
