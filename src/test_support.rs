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
