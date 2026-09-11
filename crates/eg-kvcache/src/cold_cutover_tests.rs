//! Cutover tests for the kernel-owned durable cold tier (RF-RULING-004/005).
//!
//! In their own file so the source probe below does not scan the literals it
//! searches for.

use super::tests::{open_test_cold_store, TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF};
use super::*;

/// The kernel path is the ONLY writer. A raw redb handle reappearing here would
/// be a second physical authority and would still work, so no behaviour test
/// could see it.
#[test]
fn the_cold_store_reaches_redb_only_through_kernel_capabilities() {
    let source = include_str!("cold.rs");
    // Prose may name the retired paths to say why they are gone; code may not
    // use them.
    let code = source
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("//") && !trimmed.starts_with("*")
        })
        .collect::<String>();
    for forbidden in [
        "Database::create",
        "Database::open",
        "begin_write()",
        "begin_read()",
    ] {
        assert!(
            !code.contains(forbidden),
            "the cold store must not reach redb through `{forbidden}`"
        );
    }
    let _ = (TEST_PRINCIPAL, TEST_PROOF, TestScopeVerifier);
}

/// P0-1: a demoted page must not carry a version expectation taken outside the
/// write lock, or any interleaved commit fails it closed.
#[test]
fn a_put_survives_a_commit_that_interleaves_before_it() {
    let dir = std::env::temp_dir().join(format!(
        "eg185-interleave-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("cold.redb");
    let mut store = open_test_cold_store(&path).unwrap();

    // Two writes in sequence: the second's expectation would be stale if it were
    // read before the write lock, because the first moved the scope version.
    store.put(&"a".to_string(), b"one").unwrap();
    store
        .put(&"b".to_string(), b"two")
        .expect("a put survives the version the previous put moved");
    store
        .remove(&"a".to_string())
        .expect("a remove survives it too");
    assert_eq!(
        store.get(&"b".to_string()).unwrap().as_deref(),
        Some(&b"two"[..])
    );
    assert!(store.get(&"a".to_string()).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

/// RF-RULING-005 in the ledger: a demoted page carries no caller identity, so it
/// commits as maintenance.
#[test]
fn a_demoted_page_records_the_maintenance_class() {
    let dir = std::env::temp_dir().join(format!(
        "eg185-class-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("cold.redb");
    let mut store = open_test_cold_store(&path).unwrap();
    store.put(&"a".to_string(), b"one").unwrap();

    let read = store.kernel.read_scope(&store.owner).unwrap();
    let mut seen = 0;
    for record in eg_transaction::read_batches(&read).unwrap() {
        assert_eq!(
            eg_transaction::read_class(&read, &record.batch.batch_id)
                .unwrap()
                .expect("every committed batch carries exactly one class row"),
            eg_storage::MutationClass::Maintenance,
        );
        seen += 1;
    }
    assert_eq!(seen, 1, "exactly the one demoted page");
    drop(read);
    let _ = std::fs::remove_dir_all(&dir);
}
