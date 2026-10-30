"""EG-T-12.4: real entry-point evidence rows resolve against source."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
TEST_SPEC = ROOT / "specs" / "public-engine-contract-and-release" / "test-spec.md"
REFUSED_EVIDENCE = (".md", ".svg", "status.json", ".toml", ".yml", ".yaml")


def _rows() -> list[tuple[str, str, str]]:
    text = TEST_SPEC.read_text(encoding="utf-8")
    section = text.split("### EG-T-12.4 real entry-point evidence", 1)[1]
    rows = []
    for line in section.splitlines()[1:]:
        if line.startswith("#"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 3 and cells[1].startswith("`src/"):
            rows.append((cells[0], cells[1].strip("`"), cells[2]))
    return rows


def _defines(path: Path, symbol: str) -> bool:
    pattern = rf"\bfn {re.escape(symbol)}\b|\bdef {re.escape(symbol)}\b"
    return path.is_file() and re.search(pattern, path.read_text(encoding="utf-8")) is not None


# spec: EG-CONTRACT-R052.4
@pytest.mark.spec("EG-CONTRACT-R052.4")
def test_entry_point_evidence_resolves_and_refuses_non_execution_proof() -> None:
    rows = _rows()
    assert {name for name, _, _ in rows} >= {"Served mutation", "Planned query"}
    for name, anchor, evidence in rows:
        path, symbol = anchor.split("::")
        assert _defines(ROOT / path, symbol), f"{name}: {anchor} does not resolve"
        if evidence.startswith("missing:"):
            assert len(evidence) > len("missing:") + 10, f"{name}: missing evidence needs a reason"
            continue
        test_path, test_name = evidence.strip("`").split("::")
        assert not test_path.endswith(REFUSED_EVIDENCE), f"{name}: {test_path} is not execution proof"
        assert "test" in test_path, f"{name}: evidence is not a test file"
        assert _defines(ROOT / test_path, test_name), f"{name}: {evidence} does not resolve"
        assert symbol in (ROOT / test_path).read_text(encoding="utf-8"), f"{name}: test does not enter {symbol}"
