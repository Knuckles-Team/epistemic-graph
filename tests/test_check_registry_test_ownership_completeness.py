"""EG-CONTRACT-R023.1: every tracked `tests/` file has a recognized owner in
this repository's real `architecture/component-registry.yml`, so the
pre-commit test-ownership hook (part of EG-CONTRACT-R023's full clean run)
cannot fail on an unclaimed file.

Unlike `tests/test_check_registry_test_ownership.py` (which proves the
checker's logic against synthetic registries/trees), this binds the spec by
running the real checker against the real, currently tracked repository
state.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from check_registry_test_ownership import unowned_test_files

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


@pytest.mark.spec("EG-CONTRACT-R023.1")
def test_current_tracked_tests_tree_has_no_unowned_files() -> None:
    registry_path = ROOT / "architecture" / "component-registry.yml"
    _files, unowned = unowned_test_files(ROOT, registry_path)
    assert unowned == []


@pytest.mark.spec("EG-CONTRACT-R023.1", "EG-DURABLE-KERNEL-R014.2")
def test_cluster_load_harness_has_exact_binary_composition_owner() -> None:
    document = yaml.safe_load(
        (ROOT / "architecture/component-registry.yml").read_text()
    )
    path = "tests/test_validate_raft_cluster.py"
    owners = [
        component["component_id"]
        for component in document["components"]
        if path in component.get("test_roots", [])
    ]
    assert owners == ["eg.binary-composition"]
