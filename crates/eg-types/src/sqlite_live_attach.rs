//! EG-UNIFIED-DATA-PLANE-R015.2.1 — adapter skeleton for the SQLite live
//! attach: a typed attach config, its mapping to the read-only driver open
//! config, and the refusal path before any file is opened. Real WAL-frame
//! tailing / watermark polling against a live, concurrently-written file is
//! EG-UNIFIED-DATA-PLANE-R015.2.2+.

use crate::sqlite_attached_catalog::SqliteCaptureMode;
use serde::{Deserialize, Serialize};

/// How this adapter opens the live application's SQLite file. This slice
/// only ever attaches read-only: a writable attach is out of scope — the
/// adapter never mutates the source it is attached to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteOpenMode {
    /// Asserts the file never changes underneath the adapter (SQLite's
    /// `immutable=1`). Incompatible with declaring a change-capture mode.
    ReadOnlyImmutable,
    /// The lock-safe read-only mode that tolerates a concurrent writer
    /// (SQLite's `mode=ro` without `immutable`), required for live capture.
    ReadOnlyLockSafe,
}

/// Typed config for attaching one live SQLite file read-only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteAttachConfig {
    pub file_path: String,
    pub open_mode: SqliteOpenMode,
    pub capture_mode: SqliteCaptureMode,
}

/// The driver-facing open config this adapter builds from a validated
/// [`SqliteAttachConfig`]: a `file:` URI plus the explicit read-only flag
/// the later connect slice (EG-UNIFIED-DATA-PLANE-R015.2.2+) hands to the
/// actual SQLite driver. Kept as our own type so this crate links no
/// SQLite driver itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteDriverOpenConfig {
    pub uri: String,
    pub read_only: bool,
}

/// Why a [`SqliteAttachConfig`] was refused before any file was opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqliteAttachRefusal {
    EmptyFilePath,
    /// `ReadOnlyImmutable` promises the file never changes underneath the
    /// adapter, which directly contradicts declaring a change-capture mode
    /// against it: a live, changing source needs the lock-safe open mode
    /// instead.
    ImmutableModeWithChangeCapture,
}

impl SqliteAttachConfig {
    /// Validates this config and, if well-formed, maps it to the driver
    /// open config the later connect slice hands to the SQLite driver.
    /// Performs NO I/O: a refusal here never opens the file.
    pub fn to_driver_open_config(&self) -> Result<SqliteDriverOpenConfig, SqliteAttachRefusal> {
        if self.file_path.trim().is_empty() {
            return Err(SqliteAttachRefusal::EmptyFilePath);
        }
        if self.open_mode == SqliteOpenMode::ReadOnlyImmutable {
            return Err(SqliteAttachRefusal::ImmutableModeWithChangeCapture);
        }
        Ok(SqliteDriverOpenConfig {
            uri: format!("file:{}?mode=ro", self.file_path),
            read_only: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid(capture_mode: SqliteCaptureMode) -> SqliteAttachConfig {
        SqliteAttachConfig {
            file_path: "/var/lib/app/live.sqlite3".to_string(),
            open_mode: SqliteOpenMode::ReadOnlyLockSafe,
            capture_mode,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.2.1
    #[test]
    fn lock_safe_wal_rowid_tailing_maps_to_a_driver_open_config() {
        let config = valid(SqliteCaptureMode::WalRowidTailing);
        let driver = config
            .to_driver_open_config()
            .expect("well-formed config maps");
        assert_eq!(driver.uri, "file:/var/lib/app/live.sqlite3?mode=ro");
        assert!(driver.read_only);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.2.1
    #[test]
    fn lock_safe_watermark_polling_maps_to_a_driver_open_config() {
        let config = valid(SqliteCaptureMode::WatermarkPolling);
        assert!(config.to_driver_open_config().is_ok());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.2.1
    #[test]
    fn refuses_empty_file_path_before_any_open_attempt() {
        let mut config = valid(SqliteCaptureMode::WatermarkPolling);
        config.file_path = "   ".to_string();
        assert_eq!(
            config.to_driver_open_config(),
            Err(SqliteAttachRefusal::EmptyFilePath)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.2.1
    #[test]
    fn refuses_immutable_mode_with_wal_rowid_tailing() {
        let mut config = valid(SqliteCaptureMode::WalRowidTailing);
        config.open_mode = SqliteOpenMode::ReadOnlyImmutable;
        assert_eq!(
            config.to_driver_open_config(),
            Err(SqliteAttachRefusal::ImmutableModeWithChangeCapture)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R015.2.1
    #[test]
    fn refuses_immutable_mode_with_watermark_polling() {
        let mut config = valid(SqliteCaptureMode::WatermarkPolling);
        config.open_mode = SqliteOpenMode::ReadOnlyImmutable;
        assert_eq!(
            config.to_driver_open_config(),
            Err(SqliteAttachRefusal::ImmutableModeWithChangeCapture)
        );
    }
}
