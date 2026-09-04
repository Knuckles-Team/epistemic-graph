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
    return "\n".join(
        _read(path)
        for path in (
            "crates/eg-mutation-store/src/lib.rs",
            "crates/eg-mutation-store/src/store/identity.rs",
        )
    )


def _validate(document: str, contract: str, store: str) -> None:
    assert "pub const MUTATION_BATCH_VERSION: u16 = 1;" in contract
    assert "pub const MUTATION_STORE_SCHEMA_VERSION: u16 = 1;" in store
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
