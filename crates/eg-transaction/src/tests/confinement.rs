//! A capability serves exactly one scope.
//!
//! One physical owner file deliberately serves many scopes
//! (`one_physical_root_serves_multiple_scopes_without_cross_tenant_aliasing`),
//! and `OwnerLayout::accepts` gates on the mutation *domain*, never on tenant.
//! Key non-collision is therefore not confinement: these cases prove that a
//! handle for tenant A cannot reach, purge, retire or read tenant B's rows.

use super::*;
use crate::admitted::AdmittedMutation;
use crate::tables::BATCHES;
use eg_storage::ledger_scope_key;

struct Tenants {
    fixture: Fixture,
    a: OwnedStoreHandle<LedgerOnlyOwner>,
    b: OwnedStoreHandle<LedgerOnlyOwner>,
    identity_a: MutationScopeIdentity,
    identity_b: MutationScopeIdentity,
}

/// Two tenants bound to one physical file, each with one committed batch.
fn two_tenants(path: &Path) -> Tenants {
    let identity_a = native_identity("tenant-a", "incarnation:blob:a");
    let identity_b = native_identity("tenant-b", "incarnation:blob:b");
    let (fixture, a) = ledger_fixture(path, identity_a.clone());
    let b = fixture.bind::<LedgerOnlyOwner>(
        &verifier("tenant-b", OwnerLayout::LedgerOnly),
        identity_b.clone(),
    );
    apply_batch(&fixture, &a, &batch(identity_a.clone(), "batch-a"));
    apply_batch(&fixture, &b, &batch(identity_b.clone(), "batch-b"));
    Tenants {
        fixture,
        a,
        b,
        identity_a,
        identity_b,
    }
}

#[test]
fn a_capability_cannot_purge_or_retire_another_tenants_scope() {
    let dir = tempfile::tempdir().unwrap();
    let tenants = two_tenants(&dir.path().join("shared.redb"));

    // Holding A's handle, name B's identity.
    assert!(tenants
        .fixture
        .mutations
        .purge_scope(&tenants.a, &tenants.identity_b)
        .unwrap_err()
        .contains("does not serve this scope"));

    // The direct capability method takes no scope at all, so B's binding and
    // version row cannot even be named through A's write.
    let write = AdmittedMutation::open(tenants.fixture.mutations_authority(), &tenants.a).unwrap();
    write.retire_scope_binding().unwrap();
    write.abort().unwrap();

    // B is intact: its receipt, its version row and its binding all survive.
    let read_b = tenants.fixture.kernel.read_scope(&tenants.b).unwrap();
    assert!(read_ledger(&read_b, "batch-b").unwrap().is_some());
    assert_eq!(version(&read_b).unwrap(), 1);

    // And A purging its own scope still works.
    tenants
        .fixture
        .mutations
        .purge_scope(&tenants.a, &tenants.identity_a)
        .unwrap();
    let read_b = tenants.fixture.kernel.read_scope(&tenants.b).unwrap();
    assert!(read_ledger(&read_b, "batch-b").unwrap().is_some());
}

#[test]
fn a_scoped_read_cannot_address_another_tenants_rows() {
    let dir = tempfile::tempdir().unwrap();
    let tenants = two_tenants(&dir.path().join("shared.redb"));
    let read_a = tenants.fixture.kernel.read_scope(&tenants.a).unwrap();
    let key_a = ledger_scope_key(&tenants.identity_a);
    let key_b = ledger_scope_key(&tenants.identity_b);
    let batches = read_a.scoped_table(BATCHES).unwrap();

    assert_eq!(batches.scope_key(), key_a.as_str());
    assert!(batches.get((key_a.as_str(), "batch-a")).unwrap().is_some());
    match batches.get((key_b.as_str(), "batch-b")) {
        Ok(_) => panic!("a scoped read must not address another scope"),
        Err(error) => assert!(error.contains("another scope's rows"), "{error}"),
    }
    match batches.range_inclusive((key_b.as_str(), ""), (key_b.as_str(), "~")) {
        Ok(_) => panic!("a scoped range must not address another scope"),
        Err(error) => assert!(error.contains("another scope's rows"), "{error}"),
    }

    // The typed readers only ever see A's own rows.
    assert!(read_ledger(&read_a, "batch-b").unwrap().is_none());
    assert_eq!(crate::read::read_batches(&read_a).unwrap().len(), 1);
}
