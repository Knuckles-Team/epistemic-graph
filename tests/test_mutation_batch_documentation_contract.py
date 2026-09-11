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
    # crates/eg-mutation-store is deleted. The live bare/ledger table
    # definitions now live in eg-storage/src/tables.rs and
    # eg-transaction/src/tables.rs. The physical storage-kernel schema marker
    # lives in eg-storage/src/physical/incarnation.rs (v2), while the `_v3`
    # retired-prototype candidate family lives in physical/root.rs.
    return "\n".join(
        _read(path)
        for path in (
            "crates/eg-storage/src/tables.rs",
            "crates/eg-transaction/src/tables.rs",
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
        "schema-v2 storage-kernel table set",
        "`mutation_store_root`",
        "`ledger_batches`",
        "`mutation_replay_nonces`",
        "live table",
        "retired candidate",
        "exact `*_v3` candidate table family",
        "quarantined",
        "never translated",
    ):
        assert marker in document

    live_tables = (
        "mutation_store_root",
        "mutation_scope_bindings",
        "mutation_owner_manifest",
        "ledger_batches",
        "ledger_maintenance",
        "ledger_versions",
        "ledger_fences",
        "ledger_outbox",
        "mutation_outbox_topic_index",
        "ledger_private_payloads",
        "mutation_outbox_consumers",
        "mutation_outbox_deliveries",
        "mutation_outbox_cursors",
        "mutation_outbox_claim_cursors",
        "mutation_outbox_fairness",
        "mutation_replay_nonces",
        "mutation_replay_operations",
        "mutation_classes",
    )
    for table in live_tables:
        assert f'TableDefinition::new("{table}")' in store

    retired_candidates = (
        ("mutation_store_root", "mutation_store_root_v3"),
        ("mutation_scope_bindings", "mutation_scope_bindings_v3"),
        ("ledger_batches", "mutation_batches_v3"),
        ("ledger idempotency/replay", "mutation_idempotency_v3"),
        ("ledger_versions", "mutation_versions_v3"),
        ("ledger_fences", "mutation_fences_v3"),
        ("ledger_outbox", "mutation_outbox_v3"),
        ("ledger_private_payloads", "mutation_private_payloads_v3"),
    )
    for _, retired in retired_candidates:
        assert f'"{retired}"' in store

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


def test_gate_rejects_live_table_name_drift() -> None:
    document = _read("docs/architecture/mutation_batch.md")
    contract = _contract_source()
    store = _store_source()

    broken_store = store.replace(
        'TableDefinition::new("mutation_store_root")',
        'TableDefinition::new("mutation_store_root_removed")',
        1,
    )
    with pytest.raises(AssertionError):
        _validate(document, contract, broken_store)


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
