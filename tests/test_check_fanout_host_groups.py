"""EG-CONTRACT-R011.1: the land-gate fan-out host-group manifest must be
structurally valid -- one warm target and a headroom reservation per group,
no host claimed by more than one group -- before any fan-out work is
dispatched.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from check_fanout_host_groups import load_manifest, validate_groups

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _write_manifest(tmp_path: Path, groups: list[dict]) -> Path:
    path = tmp_path / "fanout-host-groups.yml"
    path.write_text(yaml.safe_dump({"groups": groups}), encoding="utf-8")
    return path


@pytest.mark.spec("EG-CONTRACT-R011.1")
def test_disjoint_fully_specified_manifest_is_valid(tmp_path: Path) -> None:
    manifest = _write_manifest(
        tmp_path,
        [
            {
                "name": "group-a",
                "warm_target": "warm-a",
                "headroom_reservation": 0.1,
                "hosts": ["host-a1", "host-a2"],
            },
            {
                "name": "group-b",
                "warm_target": "warm-b",
                "headroom_reservation": 0.2,
                "hosts": ["host-b1"],
            },
        ],
    )
    document = load_manifest(manifest)
    assert validate_groups(document["groups"]) == []


@pytest.mark.spec("EG-CONTRACT-R011.1")
def test_overlapping_host_assignment_is_rejected(tmp_path: Path) -> None:
    manifest = _write_manifest(
        tmp_path,
        [
            {
                "name": "group-a",
                "warm_target": "warm-a",
                "headroom_reservation": 0.1,
                "hosts": ["shared-host"],
            },
            {
                "name": "group-b",
                "warm_target": "warm-b",
                "headroom_reservation": 0.2,
                "hosts": ["shared-host"],
            },
        ],
    )
    document = load_manifest(manifest)
    problems = validate_groups(document["groups"])
    assert any(
        "shared-host" in problem and "claimed by both" in problem
        for problem in problems
    )


@pytest.mark.spec("EG-CONTRACT-R011.1")
def test_missing_headroom_reservation_is_rejected(tmp_path: Path) -> None:
    manifest = _write_manifest(
        tmp_path,
        [
            {
                "name": "group-a",
                "warm_target": "warm-a",
                "hosts": ["host-a1"],
            }
        ],
    )
    document = load_manifest(manifest)
    problems = validate_groups(document["groups"])
    assert any("headroom_reservation" in problem for problem in problems)


@pytest.mark.spec("EG-CONTRACT-R011.1")
def test_repository_default_manifest_is_valid() -> None:
    default = ROOT / ".config" / "fanout-host-groups.yml"
    document = load_manifest(default)
    assert validate_groups(document["groups"]) == []
