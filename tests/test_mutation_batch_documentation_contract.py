"""Static parity gate for the product-v1 MutationBatch documentation."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _contract_source() -> str:
    return "\n".join(
        _read(path)
        for path in (
            "crates/eg-types/src/mutation_batch.rs",
            "crates/eg-types/src/mutation_batch/model/batch.rs",
            "crates/eg-types/src/mutation_batch/model/identity.rs",
            "crates/eg-types/src/mutation_batch/model/records.rs",
        )
    )


def _store_source() -> str:
    # crates/eg-mutation-store is deleted. Its `_v1` table definitions and
    # MUTATION_STORE_SCHEMA_VERSION const (formerly lib.rs +
    # store/identity.rs) now live in eg-storage/src/tables.rs and
    # eg-storage/src/physical/incarnation.rs (renamed STORAGE_KERNEL_SCHEMA_
    # VERSION, bumped 1 -> 2 by b90e42a7); the `_v3` retired-prototype
    # candidate family (formerly store/identity.rs's RETIRED_PROTOTYPE_TABLES)
    # now lives in eg-storage/src/physical/root.rs.
    return "\n".join(
        _read(path)
        for path in (
            "crates/eg-storage/src/tables.rs",
            "crates/eg-storage/src/physical/incarnation.rs",
            "crates/eg-storage/src/physical/root.rs",
        )
    )


def _validate(document: str, contract: str, store: str) -> None:
    assert "pub const MUTATION_BATCH_VERSION: u16 = 1;" in contract
    # Renamed STORAGE_KERNEL_SCHEMA_VERSION and bumped 1 -> 2 by b90e42a7.
    assert "pub const STORAGE_KERNEL_SCHEMA_VERSION: u16 = 2;" in store
    for marker in (
        "pub struct MutationScopeIdentity",
        "pub enum MutationScope",
        "pub enum VersionExpectation",
        "pub enum CommittedVersion",
    ):
        assert marker in contract

    for marker in (
        "product `MutationBatch` v1",
        "`MutationScopeIdentity`",
        "`VersionExpectation`",
        "`CommittedVersion`",
        "v1 mutation-store tables",
        "exact `*_v3` candidate table family",
        "quarantined",
        "never translated",
    ):
        assert marker in document

    for table in (
        "mutation_store_root",
        "mutation_scope_bindings",
        "mutation_batches",
        "mutation_idempotency",
        "mutation_versions",
        "mutation_fences",
        "mutation_outbox",
        "mutation_private_payloads",
    ):
        assert f'TableDefinition::new("{table}_v1")' in store
        assert f'"{table}_v3"' in store

    for retired in (
        "`MutationBatch` v2",
        "pre-v2",
        "`expected_graph_version`",
        "`version_scope`",
        "`source_graph_version`",
        "migration path",
        "compatibility reader",
        "legacy reader",
    ):
        assert retired not in document


def test_document_runtime_and_store_share_product_v1_contract() -> None:
    _validate(
        _read("docs/architecture/mutation_batch.md"),
        _contract_source(),
        _store_source(),
    )


def test_gate_rejects_document_version_and_flat_shape_drift() -> None:
    document = _read("docs/architecture/mutation_batch.md")
    contract = _contract_source()
    store = _store_source()

    with pytest.raises(AssertionError):
        _validate(
            document.replace("product `MutationBatch` v1", "`MutationBatch` v2"),
            contract,
            store,
        )
    with pytest.raises(AssertionError):
        _validate(document + "\n`version_scope`\n", contract, store)
