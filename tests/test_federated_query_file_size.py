"""File-size bounds for the federated query split refactors."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
LIMIT = 900


def _lines(relative: str) -> int:
    return len((ROOT / relative).read_text(encoding="utf-8").splitlines())


@pytest.mark.spec("EG-FEDERATED-QUERY-R057")
def test_sql_provider_module_is_within_file_size_limit() -> None:
    assert _lines("crates/eg-query/src/sql/providers.rs") <= LIMIT


@pytest.mark.spec("EG-FEDERATED-QUERY-R066")
def test_sparql_evaluator_modules_are_within_file_size_limit() -> None:
    files = [Path("crates/eg-rdf/src/sparql.rs")]
    files += sorted((ROOT / "crates/eg-rdf/src/sparql").rglob("*.rs"))
    oversized = {str(p): n for p in files if (n := _lines(str(p))) > LIMIT}
    assert not oversized, oversized
