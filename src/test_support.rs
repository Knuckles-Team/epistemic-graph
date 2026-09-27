//! Test-only helpers shared by crate unit-test modules.

use std::path::PathBuf;

/// Return a unique process-local temporary directory path and clear any stale
/// path from a previous interrupted test run.
///
/// Callers supply their historical prefix and tag, so moving ownership of the
/// uniqueness/cleanup mechanics does not change the paths or lifecycle of the
/// tests that use it.
pub(crate) fn temp_dir(prefix: &str, tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "{prefix}-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
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
