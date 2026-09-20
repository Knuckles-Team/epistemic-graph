use serde::{Deserialize, Serialize};

use super::{CommittedVersion, MutationBatch, MutationOutboxIntent, MutationScopeIdentity};

/// Durable terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationBatchStatus {
    Prepared,
    Committed,
    Aborted,
}

/// Durable status/result record used for retry reconciliation after restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatchRecord {
    pub batch: MutationBatch,
    /// Self-checking envelope copy; it must exactly equal `batch.identity`.
    pub identity: MutationScopeIdentity,
    pub status: MutationBatchStatus,
    pub committed_version: CommittedVersion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_msgpack: Option<Vec<u8>>,
    pub committed_at_ms: u64,
}

/// One durable outbox row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationOutboxRecord {
    pub schema_version: u16,
    pub batch_id: String,
    pub ordinal: u32,
    pub identity: MutationScopeIdentity,
    pub committed_version: CommittedVersion,
    /// The scope version after the batch committed. Legacy rows may omit
    /// this field and are rejected when their original order is unverifiable.
    #[serde(default)]
    pub commit_sequence: Option<u64>,
    pub intent: MutationOutboxIntent,
    pub created_at_ms: u64,
}

/// Durable lease over an outbox row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationOutboxLease {
    pub record: MutationOutboxRecord,
    pub consumer: String,
    pub lease_epoch: u64,
    /// EXCLUSIVE upper bound, mirroring `eg_transaction::outbox::rows::
    /// OutboxDelivery::lease_until_ms` (the durable row this lease is
    /// issued over): valid while `now_ms < lease_until_ms`, expired once
    /// `now_ms >= lease_until_ms`. `now_ms == lease_until_ms` is already
    /// expired, not still valid.
    pub lease_until_ms: u64,
    pub attempt: u32,
}

/// Monotonic projection watermark updated in the same transaction as outbox ack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationProjectionCursor {
    pub schema_version: u16,
    pub projection: String,
    pub identity: MutationScopeIdentity,
    pub batch_id: String,
    pub outbox_ordinal: u32,
    pub committed_version: CommittedVersion,
    pub advanced_at_ms: u64,
}

/// Result returned by a persistence commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationBatchCommit {
    pub record: MutationBatchRecord,
    /// Self-checking envelope copy; it must equal both record identities.
    pub identity: MutationScopeIdentity,
    pub replayed: bool,
}

/// The outbox header that carries the verified caller on every compiled batch.
pub const MUTATION_ACTOR_HEADER: &str = "actor";

impl MutationBatchRecord {
    /// The verified CALLER that committed this record -- the one owner accessor.
    ///
    /// RF-RULING-004's application note makes the batch's serving principal the
    /// committing ledger's own requirement for every domain, so it can no longer
    /// answer "whose operation was this": comparing it would compare the engine
    /// against itself and pass for any caller. Caller attribution has exactly one
    /// home, the outbox `actor` header, which `compile.rs` writes unconditionally
    /// on every batch it builds.
    ///
    /// This exists as ONE checked accessor because the alternative already
    /// happened: three private, divergent re-derivations of it grew in
    /// `wire/mod.rs`, `handlers/query.rs` and `handlers/sqlite_file.rs`, and a
    /// fourth site (`sql_catalog_acl.rs`) simply had none -- the cross-actor
    /// replay-ownership hole the M1 review raised as a P1. A missing header is an
    /// `Err`, never `None`: "this record names no owner" must fail closed, not
    /// read as "this record matches every owner".
    pub fn committing_actor(&self) -> Result<&str, String> {
        self.batch
            .outbox
            .iter()
            .find_map(|intent| intent.headers.get(MUTATION_ACTOR_HEADER))
            .map(String::as_str)
            .filter(|actor| !actor.is_empty())
            .ok_or_else(|| "committed MutationBatch carries no actor attribution".to_string())
    }

    /// The TENANT whose authority committed this record.
    ///
    /// Read from the envelope's preserved `AuthorityContext`, NOT from
    /// `batch.identity.tenant()` -- for exactly the reason `committing_actor`
    /// above does not read `serving_principal`. RF-RULING-004's application note
    /// makes a graph-scoped batch's `identity` the SHARD's own scope: the durable
    /// bind path (`redb_store::shard::bind_caller_batch`) rewrites
    /// `bound.identity = graph_scope_identity(graph_fname)` before the record is
    /// stored, whose tenant is the reserved `GRAPH_SHARD_TENANT` sentinel a
    /// caller is explicitly forbidden to use. Comparing THAT against a caller's
    /// tenant compares the engine against itself and can never match.
    ///
    /// `bind_caller_batch` does not touch the envelope's authority, so the
    /// caller's real tenant is still there, exactly as compiled. A missing
    /// operation envelope is an `Err`, never `None`: a record that names no
    /// committing tenant must fail closed, not read as "matches every tenant".
    pub fn committing_tenant(&self) -> Result<&str, String> {
        self.batch
            .envelope
            .operation()
            .map(|operation| operation.authority.tenant.as_str())
            .filter(|tenant| !tenant.is_empty())
            .ok_or_else(|| "committed MutationBatch carries no tenant authority".to_string())
    }

    pub fn validate_identity(&self) -> Result<(), String> {
        self.batch.validate_identity()?;
        require_same_identity(
            &self.identity,
            &self.batch.identity,
            "mutation batch record identity does not match its batch",
        )
    }
}

impl MutationOutboxRecord {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.identity.validate_digest()
    }
}

impl MutationProjectionCursor {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.identity.validate_digest()
    }
}

impl MutationBatchCommit {
    pub fn validate_identity(&self) -> Result<(), String> {
        self.record.validate_identity()?;
        require_same_identity(
            &self.identity,
            &self.record.identity,
            "mutation batch commit identity does not match its record",
        )
    }
}

fn require_same_identity(
    envelope: &MutationScopeIdentity,
    nested: &MutationScopeIdentity,
    mismatch: &str,
) -> Result<(), String> {
    envelope.validate_digest()?;
    if envelope == nested {
        Ok(())
    } else {
        Err(mismatch.to_string())
    }
}
