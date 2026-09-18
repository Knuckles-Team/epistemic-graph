"""Golden and adversarial checks for the current method-policy registry."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pytest

from scripts.method_policy_inventory import (
    EXPECTED_CFG_ROWS,
    EXPECTED_DOMAIN_MODULES,
    EXPECTED_METHOD_POLICY_ROWS,
    MethodPolicyInventoryError,
    load_capability_sources,
    parse_method_policy_table,
)

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/method_policy_registry_v1.json"


def _rows():
    return parse_method_policy_table(load_capability_sources(ROOT))


def _policy_digest(rows) -> str:
    payload = []
    for row in rows:
        value = asdict(row)
        value.pop("domain")
        value.pop("cfg_feature")
        payload.append(value)
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def test_domain_registry_matches_current_golden() -> None:
    rows = _rows()
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))

    assert fixture["schema"] == "eg-method-policy-registry/v1"
    assert fixture["method_count"] == EXPECTED_METHOD_POLICY_ROWS == len(rows)
    assert fixture["domain_order"] == list(EXPECTED_DOMAIN_MODULES)
    assert fixture["cfg_rows"] == [
        {"name": row.name, "feature": row.cfg_feature}
        for row in rows
        if row.cfg_feature is not None
    ]
    names = "\n".join(row.name for row in rows).encode()
    assert fixture["order_sha256"] == hashlib.sha256(names).hexdigest()
    assert fixture["policy_sha256"] == _policy_digest(rows)


def test_registry_is_unique_complete_and_domain_ordered() -> None:
    rows = _rows()
    names = [row.name for row in rows]

    assert len(names) == len(set(names)) == EXPECTED_METHOD_POLICY_ROWS
    assert names[0] == "CreateGraph"
    assert names[-1] == "MutationOutbox"
    assert {"ParseFile", "ParseFiles"} <= set(names)
    assert (
        tuple(
            (row.name, row.cfg_feature) for row in rows if row.cfg_feature is not None
        )
        == EXPECTED_CFG_ROWS
    )


def test_generated_ledger_uses_registry_order_and_policy_values() -> None:
    rows = _rows()
    ledger = (ROOT / "docs/capabilities.generated.md").read_text(encoding="utf-8")
    table = [line for line in ledger.splitlines() if line.startswith("| `")]

    assert len(table) == len(rows)
    for row, line in zip(rows, table, strict=True):
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        conditional = (
            "runtime-conditional" in row.note or "conservative upper bound" in row.note
        )
        mutates = "~true" if conditional and row.mutates else str(row.mutates).lower()
        assert cells == [
            f"`{row.name}`",
            mutates,
            row.durability_domain,
            f"`{row.authz_action}`",
            str(row.idempotent).lower(),
            str(row.audited).lower(),
            str(row.emits_cdc).lower(),
            row.txn_participation,
            row.note,
        ]


def test_duplicate_domain_declaration_fails_closed() -> None:
    source = load_capability_sources(ROOT)
    duplicate = source.replace(
        '("ParseFiles", spec(make_policy',
        '("ParseFile", spec(make_policy',
        1,
    )
    assert duplicate != source, "the planted duplicate must actually change the source"

    with pytest.raises(
        MethodPolicyInventoryError,
        match="duplicate method-policy declaration: ParseFile",
    ):
        parse_method_policy_table(duplicate)


def test_missing_domain_declaration_fails_closed() -> None:
    source = load_capability_sources(ROOT)
    missing = "\n".join(
        line
        for line in source.splitlines()
        if '("ParseFiles", spec(make_policy' not in line
    )
    assert missing != source, "the planted deletion must actually change the source"

    # Derived, not literal. This assertion carried a hard-coded "407 rows
    # instead of 408" that had already rotted five methods behind the registry
    # -- so the test that is supposed to prove a DELETED row fails closed was
    # itself failing for an unrelated reason, and would have gone on masking a
    # real regression. The expectation is the registry's own count minus the
    # one row this test deletes.
    expected = EXPECTED_METHOD_POLICY_ROWS
    with pytest.raises(
        MethodPolicyInventoryError,
        match=f"{expected - 1} rows instead of {expected}",
    ):
        parse_method_policy_table(missing)


def test_malformed_domain_declaration_fails_closed() -> None:
    source = load_capability_sources(ROOT)
    malformed = source.replace(
        '("ParseFile", spec(make_policy',
        '("ParseFile", spec(alternate_policy',
        1,
    )
    assert malformed != source, (
        "the planted malformation must actually change the source"
    )

    with pytest.raises(
        MethodPolicyInventoryError, match="unparsed ingestion policy row"
    ):
        parse_method_policy_table(malformed)


def test_second_ordering_projection_fails_closed() -> None:
    source = load_capability_sources(ROOT)
    second_projection = source.replace(
        "mod domains;",
        "mod domains;\npub const ALL_METHODS: &[()] = &[];",
        1,
    )

    with pytest.raises(MethodPolicyInventoryError, match="flat policy authority"):
        parse_method_policy_table(second_projection)


def test_domain_registry_reordering_fails_closed() -> None:
    source = load_capability_sources(ROOT)
    reordered = source.replace(
        '    ("cluster", cluster::ROWS),\n    ("compute", compute::ROWS),',
        '    ("compute", compute::ROWS),\n    ("cluster", cluster::ROWS),',
        1,
    )

    with pytest.raises(MethodPolicyInventoryError, match="domain registry differs"):
        parse_method_policy_table(reordered)


def test_no_retired_flat_policy_surface_exists() -> None:
    capability_source = (ROOT / "crates/eg-capabilities/src/lib.rs").read_text(
        encoding="utf-8"
    )
    registry_source = (ROOT / "crates/eg-capabilities/src/domains/mod.rs").read_text(
        encoding="utf-8"
    )
    scanner_source = (ROOT / "scripts/method_policy_inventory.py").read_text(
        encoding="utf-8"
    )

    assert not (ROOT / "crates/eg-capabilities/src/dispatch.rs").exists()
    assert "ALL_METHODS" not in capability_source
    assert "historical" not in capability_source.lower()
    assert "compatib" not in capability_source.lower()
    assert "flat-inventory" not in registry_source
    assert "historical-order" not in scanner_source
    assert "method_policy_hierarchy_v1" not in scanner_source
