use super::{filesystem::hex_sha256, *};

pub(super) fn validate_relative_basename(value: &str) -> Result<(), String> {
    let path = Path::new(value);
    let mut components = path.components();
    if value.is_empty()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err("direct-state generation path is not one relative basename".into());
    }
    Ok(())
}

pub(super) fn validate_distinct_authority_paths(first: &Path, second: &Path) -> Result<(), String> {
    let first_name = first
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "direct-state authority name is not portable".to_string())?;
    let second_name = second
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "direct-state authority name is not portable".to_string())?;
    validate_relative_basename(first_name)?;
    validate_relative_basename(second_name)?;
    let first_parent = first
        .parent()
        .ok_or_else(|| "direct-state authority has no parent directory".to_string())?;
    let second_parent = second
        .parent()
        .ok_or_else(|| "direct-state authority has no parent directory".to_string())?;
    if std::fs::canonicalize(first_parent).map_err(|error| error.to_string())?
        != std::fs::canonicalize(second_parent).map_err(|error| error.to_string())?
    {
        return Err("direct-state durable authorities must share one directory".into());
    }
    if first_name == second_name {
        return Err("direct-state durable authorities must use distinct paths".into());
    }
    Ok(())
}

pub const DIRECT_STATE_SCHEMA_VERSION: u16 = 1;
pub const MAX_DIRECT_STATE_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_DIRECT_STATE_CHUNK_ITEMS: usize = 100_000;
pub const MAX_DIRECT_STATE_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_DIRECT_STATE_OWNER_TABLES: usize = 256;
pub const MAX_DIRECT_STATE_OWNER_NAME_BYTES: usize = 256;
pub(super) const MAX_DIRECT_STATE_GC_ENTRIES: usize = 4_096;
pub const DEFAULT_MAX_DIRECT_STATE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
pub const HARD_MAX_DIRECT_STATE_BYTES: u64 = 512 * 1024 * 1024 * 1024;

/// Canonical order is also the install/recovery order.  It must never depend on
/// provider registration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum DirectStateDomain {
    ClusterControl,
    Blob,
    KeyValue,
    TimeSeries,
    AnalyticsJobs,
    Statecharts,
    SqliteCatalog,
}

impl DirectStateDomain {
    pub const ALL: [Self; 7] = [
        Self::ClusterControl,
        Self::Blob,
        Self::KeyValue,
        Self::TimeSeries,
        Self::AnalyticsJobs,
        Self::Statecharts,
        Self::SqliteCatalog,
    ];

    const NAMES: [&'static str; 7] = [
        "cluster_control",
        "blob",
        "key_value",
        "time_series",
        "analytics_jobs",
        "statecharts",
        "sqlite_catalog",
    ];

    pub const fn as_str(self) -> &'static str {
        Self::NAMES[self as usize]
    }
}

/// Path/inode-independent closed layout identity.  The table names are the
/// complete current owner set (including mutation-ledger-private tables when the
/// store is mutation-backed), sorted lexicographically with no duplicates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateOwnerManifest {
    pub schema_version: u16,
    pub domain: DirectStateDomain,
    pub authority_kind: DirectStateAuthorityKind,
    pub store_schema_version: u16,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectStateAuthorityKind {
    PlainRedb,
    MutationStore,
}

impl DirectStateOwnerManifest {
    pub fn sha256(&self) -> Result<String, String> {
        if self.schema_version != DIRECT_STATE_SCHEMA_VERSION || self.store_schema_version == 0 {
            return Err("unsupported direct-state owner manifest schema".into());
        }
        if self.tables.is_empty() || self.tables.len() > MAX_DIRECT_STATE_OWNER_TABLES {
            return Err("direct-state owner table count is outside bounds".into());
        }
        validate_owner_tables(&self.tables)?;
        let bytes = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("encode direct-state owner manifest: {error}"))?;
        if bytes.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
            return Err("direct-state owner manifest exceeds its byte bound".into());
        }
        Ok(hex_sha256(&bytes))
    }
}

fn validate_owner_tables(tables: &[String]) -> Result<(), String> {
    let mut previous: Option<&str> = None;
    for table in tables {
        if table.is_empty()
            || table.len() > MAX_DIRECT_STATE_OWNER_NAME_BYTES
            || !table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            || previous.is_some_and(|name| name >= table.as_str())
        {
            return Err("direct-state owner tables are invalid, duplicated, or unordered".into());
        }
        previous = Some(table);
    }
    Ok(())
}

/// Sidecar stores are global in the current physical format and consequently have
/// one explicit consensus owner.  A future partitioned format must introduce a new
/// scope variant and schema; it must not silently reinterpret these bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DirectStateScope {
    DefaultGlobal {
        control_group: GroupId,
        control_applied_index: u64,
        authority_epoch: u64,
    },
}

impl DirectStateScope {
    pub(super) fn control_group(&self) -> GroupId {
        match self {
            Self::DefaultGlobal { control_group, .. } => *control_group,
        }
    }

    pub(super) fn authority_epoch(&self) -> u64 {
        match self {
            Self::DefaultGlobal {
                authority_epoch, ..
            } => *authority_epoch,
        }
    }

    pub(super) fn control_applied_index(&self) -> u64 {
        match self {
            Self::DefaultGlobal {
                control_applied_index,
                ..
            } => *control_applied_index,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::DefaultGlobal { control_group, .. } if *control_group != DEFAULT_GROUP => {
                Err("global direct state must be owned by the default Raft group".to_string())
            }
            Self::DefaultGlobal {
                authority_epoch: 0, ..
            } => Err("global direct-state authority epoch must be nonzero".to_string()),
            Self::DefaultGlobal { .. } => Ok(()),
        }
    }

    pub fn validate_exact(
        &self,
        control_applied_index: u64,
        authority_epoch: u64,
    ) -> Result<(), String> {
        self.validate()?;
        match self {
            Self::DefaultGlobal {
                control_applied_index: actual_index,
                authority_epoch: actual_epoch,
                ..
            } if *actual_index == control_applied_index && *actual_epoch == authority_epoch => {
                Ok(())
            }
            Self::DefaultGlobal { .. } => {
                Err("direct-state scope does not match the enclosing snapshot fence".into())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectStateSectionManifest {
    pub schema_version: u16,
    pub domain: DirectStateDomain,
    pub scope: DirectStateScope,
    /// One capture invocation across the exact closed domain set. This prevents
    /// transport or recovery from mixing individually valid sections.
    pub capture_set_sha256: String,
    /// Identity of the exact source generation pinned by that invocation.
    pub source_generation_sha256: String,
    /// Digest of the canonical workspace/store owner manifest expected after open.
    pub owner_manifest_sha256: String,
    pub logical_bytes: u64,
    pub chunk_count: u64,
    /// SHA-256 of the concatenated chunk payload bytes in ordinal order.
    pub content_sha256: String,
}

impl DirectStateSectionManifest {
    pub fn validate(&self, configured_max_bytes: u64) -> Result<(), String> {
        if self.schema_version != DIRECT_STATE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported direct-state manifest schema {}",
                self.schema_version
            ));
        }
        self.scope.validate()?;
        validate_sha256("capture set", &self.capture_set_sha256)?;
        validate_sha256("source generation", &self.source_generation_sha256)?;
        validate_sha256("owner manifest", &self.owner_manifest_sha256)?;
        validate_sha256("direct-state content", &self.content_sha256)?;
        validate_section_bounds(self, configured_max_bytes)?;
        let encoded = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("encode direct-state manifest: {error}"))?;
        if encoded.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
            return Err("direct-state manifest exceeds its byte bound".to_string());
        }
        Ok(())
    }

    pub fn sha256(&self) -> Result<String, String> {
        let encoded = rmp_serde::to_vec_named(self)
            .map_err(|error| format!("encode direct-state manifest: {error}"))?;
        if encoded.len() > MAX_DIRECT_STATE_MANIFEST_BYTES {
            return Err("direct-state manifest exceeds its byte bound".to_string());
        }
        Ok(hex_sha256(&encoded))
    }
}

fn validate_section_bounds(
    manifest: &DirectStateSectionManifest,
    configured_max_bytes: u64,
) -> Result<(), String> {
    if configured_max_bytes > HARD_MAX_DIRECT_STATE_BYTES {
        return Err("configured direct-state bound exceeds the hard limit".to_string());
    }
    if manifest.logical_bytes > configured_max_bytes {
        return Err("direct-state section exceeds the configured byte bound".to_string());
    }
    let maximum_chunks = configured_max_bytes
        .saturating_add(MAX_DIRECT_STATE_CHUNK_BYTES as u64 - 1)
        / MAX_DIRECT_STATE_CHUNK_BYTES as u64;
    if manifest.chunk_count > maximum_chunks {
        return Err("direct-state section exceeds the configured chunk bound".to_string());
    }
    if manifest.logical_bytes == 0 && manifest.chunk_count != 0 {
        return Err("empty direct-state section declares chunks".to_string());
    }
    if manifest.logical_bytes != 0 && manifest.chunk_count == 0 {
        return Err("non-empty direct-state section declares no chunks".to_string());
    }
    Ok(())
}

pub(super) fn validate_aggregate_section_bounds<'a>(
    manifests: impl IntoIterator<Item = &'a DirectStateSectionManifest>,
    configured_max_bytes: u64,
) -> Result<(), String> {
    if configured_max_bytes == 0 || configured_max_bytes > HARD_MAX_DIRECT_STATE_BYTES {
        return Err("configured direct-state aggregate bound is outside limits".into());
    }
    let mut total_bytes = 0_u64;
    let mut total_chunks = 0_u64;
    let mut sections = 0_u64;
    for manifest in manifests {
        manifest.validate(configured_max_bytes)?;
        sections += 1;
        if sections > DirectStateDomain::ALL.len() as u64 {
            return Err("direct-state generation exceeds the closed domain set".into());
        }
        total_bytes = total_bytes
            .checked_add(manifest.logical_bytes)
            .ok_or_else(|| "direct-state aggregate byte count overflow".to_string())?;
        total_chunks = total_chunks
            .checked_add(manifest.chunk_count)
            .ok_or_else(|| "direct-state aggregate chunk count overflow".to_string())?;
    }
    if total_bytes > configured_max_bytes {
        return Err("direct-state generation exceeds the configured aggregate byte bound".into());
    }
    // Chunk framing restarts at every section, so `sections` partial chunks are
    // unavoidable on top of the whole chunks the byte ceiling pays for. Deriving the
    // ceiling from bytes alone rejected every closed-domain generation whose total
    // was smaller than one chunk. `sections` is itself bounded above.
    let maximum_chunks =
        sections.saturating_add(configured_max_bytes / MAX_DIRECT_STATE_CHUNK_BYTES as u64);
    if total_chunks > maximum_chunks {
        return Err("direct-state generation exceeds the configured aggregate chunk bound".into());
    }
    Ok(())
}

pub(super) fn validate_sha256(label: &str, value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("{label} digest is not canonical lowercase SHA-256"));
    }
    Ok(())
}
