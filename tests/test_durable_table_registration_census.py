"""The durable-table gate resolves both literal and shared-macro shard censuses."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine


def _gate():
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    sys.path.insert(0, str(scripts))
    spec = importlib.util.spec_from_file_location(
        "check_durable_table_registration",
        scripts / "check_durable_table_registration.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_MACRO = """
macro_rules! graph_shard_table_names {
    ($($enrichment:expr),* $(,)?) => { &[
        "nodes",
        $($enrichment,)*
        "edges",
    ] };
}
pub(crate) const GRAPH_SHARD_TABLES: &[&str] = graph_shard_table_names!(
    "repository_enrichment_budgets",
);
"""


def test_shared_macro_census_resolves_common_and_inserted_tables() -> None:
    gate = _gate()
    assert gate._graph_shard_census(_MACRO) == {
        "nodes",
        "edges",
        "repository_enrichment_budgets",
    }


def test_shared_macro_census_fails_closed_when_slot_or_literal_is_missing() -> None:
    gate = _gate()
    with pytest.raises(gate.GateError, match="slot"):
        gate._graph_shard_census(_MACRO.replace("$($enrichment,)*", ""))
    with pytest.raises(gate.GateError, match="literal-only"):
        gate._graph_shard_census(
            _MACRO.replace('"repository_enrichment_budgets",', "other_table(),")
        )


def test_direct_literal_census_still_resolves() -> None:
    gate = _gate()
    source = 'pub(crate) const GRAPH_SHARD_TABLES: &[&str] = &["nodes", "edges"] ;'
    assert gate._graph_shard_census(source) == {"nodes", "edges"}
