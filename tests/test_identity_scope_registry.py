"""EG-IDENTITY-R007: the engine contract registers the identity and loop scopes."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "identity:admin": ("admin", "engine"),
    "loops:read": ("user", "graph-os"),
    "loops:control": ("admin", "graph-os"),
}


def _scopes(path: str) -> dict[str, dict]:
    document = json.loads((ROOT / path).read_text())
    return {entry["scope"]: entry for entry in document["scopes"]}


@pytest.mark.spec("EG-IDENTITY-R007")
@pytest.mark.parametrize(
    "path", ["contract/scopes.json", "epistemic_graph/contract/scopes.json"]
)
def test_identity_and_loop_scopes_are_registered_in_both_contract_copies(path) -> None:
    registry = _scopes(path)
    for scope, (klass, owner) in EXPECTED.items():
        assert registry[scope]["class"] == klass, scope
        assert registry[scope]["owner"] == owner, scope
    # An unregistered scope is absent: the registry is closed, not open-ended.
    assert "made:up" not in registry
    assert "identity:root" not in registry


@pytest.mark.spec("EG-IDENTITY-R007")
def test_packaged_scope_mirror_is_identical_to_the_generated_contract() -> None:
    assert (ROOT / "contract/scopes.json").read_bytes() == (
        ROOT / "epistemic_graph/contract/scopes.json"
    ).read_bytes()
