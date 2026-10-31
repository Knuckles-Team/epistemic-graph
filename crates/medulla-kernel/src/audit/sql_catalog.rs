//! Audit lines for SQL-catalog owner operations.

use crate::protocol::Method;

/// SQL-catalog owner operations. Transfer paths are logical operator-provisioned
/// names and are kept out of the chain so audit records never persist filesystem
/// details; a typed SQL source batch is audited by its canonical digest only, so
/// source descriptors, mapping content and row cells never enter the audit line.
pub(super) fn audit_line(method: &Method) -> Option<String> {
    match method {
        #[cfg(feature = "sqlite-file")]
        Method::ImportSqliteFile { .. } => Some("IMPORT_SQLITE_FILE".to_string()),
        #[cfg(feature = "sqlite-file")]
        Method::ExportSqliteFile { .. } => Some("EXPORT_SQLITE_FILE".to_string()),
        #[cfg(feature = "query")]
        Method::SqlSourceBatch { batch } => batch.canonical_digests().ok().map(|digests| {
            format!(
                "SQL_SOURCE_BATCH_MUTATION|sha256:{}",
                digests.batch_digest.to_hex()
            )
        }),
        _ => None,
    }
}
