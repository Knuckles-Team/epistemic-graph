//! Test-only helpers shared by crate unit-test modules.

use std::path::PathBuf;

/// Return a unique process-local temporary directory path and clear any stale
/// path from a previous interrupted test run.
///
/// The process-local sequence prevents collisions even when concurrent calls
/// observe the same system-clock tick. Callers retain their prefix and tag.
pub(crate) fn temp_dir(prefix: &str, tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "{prefix}-{tag}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Decode a successful raw typed server response in route-level tests.
#[cfg(feature = "server")]
pub(crate) fn decode_raw_response<T: serde::de::DeserializeOwned>(
    label: &str,
    response: crate::protocol::Response,
) -> T {
    assert!(
        response.error.is_none(),
        "{label} was refused: {:?}",
        response.error
    );
    let Some(crate::protocol::ResultPayload::Raw(bytes)) = response.result else {
        panic!("{label} did not return a raw typed result");
    };
    rmp_serde::from_slice(&bytes).expect("decode raw typed server response")
}

#[test]
fn concurrent_temporary_paths_are_unique() {
    let paths = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    (0..256)
                        .map(|_| temp_dir("eg-path", "parallel"))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let unique: std::collections::HashSet<_> = paths.iter().collect();
    assert_eq!(unique.len(), paths.len());
}
