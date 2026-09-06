//! Unit tests for the KV store's owner-write paths.

use super::*;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    crate::test_support::temp_dir("eg-kv", tag)
}

/// put → get → scan → delete → cas round-trip over the durable store.
#[test]
fn kv_roundtrip_put_get_scan_delete_cas() {
    let dir = tmp_dir("rt");
    let store = KvStore::open(Some(dir.to_str().unwrap())).unwrap();
    assert!(store.is_durable());

    // put → get
    store.put("ns", "a", b"alpha".to_vec()).unwrap();
    store.put("ns", "ab", b"alphabet".to_vec()).unwrap();
    store.put("ns", "b", b"bravo".to_vec()).unwrap();
    store.put("other", "a", b"x".to_vec()).unwrap();
    assert_eq!(
        store.get("ns", "a").unwrap().as_deref(),
        Some(&b"alpha"[..])
    );
    assert_eq!(store.get("ns", "missing").unwrap(), None);

    // scan(prefix) is namespace-bounded + prefix-bounded + ordered.
    let hits = store.scan("ns", "a", 0).unwrap();
    assert_eq!(
        hits,
        vec![
            ("a".to_string(), b"alpha".to_vec()),
            ("ab".to_string(), b"alphabet".to_vec()),
        ],
        "prefix 'a' in 'ns' matches a, ab — not b, not the other namespace"
    );
    // Empty prefix → whole namespace; limit caps.
    assert_eq!(store.scan("ns", "", 2).unwrap().len(), 2);
    assert_eq!(store.scan("ns", "", 0).unwrap().len(), 3);

    // delete
    assert!(store.delete("ns", "a").unwrap());
    assert!(!store.delete("ns", "a").unwrap());
    assert_eq!(store.get("ns", "a").unwrap(), None);

    // cas: wrong expected fails, right expected swaps; absent-expected create.
    assert!(!store
        .cas("ns", "b", Some(b"WRONG"), Some(b"new".to_vec()))
        .unwrap());
    assert_eq!(
        store.get("ns", "b").unwrap().as_deref(),
        Some(&b"bravo"[..])
    );
    assert!(store
        .cas("ns", "b", Some(b"bravo"), Some(b"BRAVO".to_vec()))
        .unwrap());
    assert_eq!(
        store.get("ns", "b").unwrap().as_deref(),
        Some(&b"BRAVO"[..])
    );
    // create-if-absent: expected None on a non-existent key.
    assert!(store.cas("ns", "fresh", None, Some(b"v".to_vec())).unwrap());
    assert_eq!(
        store.get("ns", "fresh").unwrap().as_deref(),
        Some(&b"v"[..])
    );
    // cas-delete: expected current, new None.
    assert!(store.cas("ns", "fresh", Some(b"v"), None).unwrap());
    assert_eq!(store.get("ns", "fresh").unwrap(), None);

    let _ = std::fs::remove_dir_all(&dir);
}

/// A durable put survives a store close + reopen (persistence across reopen).
#[test]
fn kv_persists_across_reopen() {
    let dir = tmp_dir("reopen");
    {
        let store = KvStore::open(Some(dir.to_str().unwrap())).unwrap();
        store.put("cfg", "version", b"42".to_vec()).unwrap();
    }
    // Reopen the SAME file — the value is still there.
    let store = KvStore::open(Some(dir.to_str().unwrap())).unwrap();
    assert_eq!(
        store.get("cfg", "version").unwrap().as_deref(),
        Some(&b"42"[..]),
        "durable KV value must survive reopen"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The in-memory backend (no persist dir) honors the same contract (ephemeral).
#[test]
fn kv_in_memory_roundtrip() {
    let store = KvStore::open(None).unwrap();
    assert!(!store.is_durable());
    store.put("ns", "k", b"v".to_vec()).unwrap();
    assert_eq!(store.get("ns", "k").unwrap().as_deref(), Some(&b"v"[..]));
    assert!(store
        .cas("ns", "k", Some(b"v"), Some(b"v2".to_vec()))
        .unwrap());
    assert_eq!(store.scan("ns", "", 0).unwrap().len(), 1);
    assert!(store.delete("ns", "k").unwrap());
}

/// REVIEW-root-c2-6bee2377 P0-1, the served-write half.
///
/// Both writers observe the same scope version, so under the retired
/// `{event}:v{version}` batch id they built BYTE-IDENTICAL maintenance batches;
/// `commit::begin` checks idempotency before the version expectation, so the
/// loser replayed the winner's record and `put` returned `Ok(())` for a write
/// that never happened — a success ack on the S3 and Redis wire surfaces.
/// Every key must be present with its own value.
#[test]
fn concurrent_puts_all_land() {
    let dir = tmp_dir("concurrent-put");
    let store = Arc::new(KvStore::open(Some(dir.to_str().unwrap())).unwrap());
    let writers = 8;
    std::thread::scope(|scope| {
        for index in 0..writers {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                store
                    .put("ns", &format!("k{index}"), format!("v{index}").into_bytes())
                    .expect("a concurrent put must not fail");
            });
        }
    });
    for index in 0..writers {
        assert_eq!(
            store.get("ns", &format!("k{index}")).unwrap(),
            Some(format!("v{index}").into_bytes()),
            "put of k{index} was swallowed"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The same race on `cas`, where the swallowed write is worse: the loser
/// returned the WINNER's `swapped` bool, so a compare-and-swap reported success
/// without swapping. Exactly one contender may win a race for one key, and the
/// stored value must be that winner's.
#[test]
fn concurrent_cas_has_exactly_one_winner() {
    let dir = tmp_dir("concurrent-cas");
    let store = Arc::new(KvStore::open(Some(dir.to_str().unwrap())).unwrap());
    store.put("ns", "k", b"base".to_vec()).unwrap();
    let contenders = 8;
    let winners: Vec<usize> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..contenders)
            .map(|index| {
                let store = Arc::clone(&store);
                scope.spawn(move || {
                    store
                        .cas("ns", "k", Some(b"base"), Some(format!("w{index}").into_bytes()))
                        .expect("a concurrent cas must not fail")
                })
            })
            .collect();
        handles
            .into_iter()
            .enumerate()
            .filter_map(|(index, handle)| handle.join().unwrap().then_some(index))
            .collect()
    });
    assert_eq!(
        winners.len(),
        1,
        "compare-and-swap on one key must have exactly one winner, got {winners:?}"
    );
    assert_eq!(
        store.get("ns", "k").unwrap(),
        Some(format!("w{}", winners[0]).into_bytes()),
        "the stored value must be the winner's"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
