//! Ledger, serialization, native stores, time series, blobs, KV, and SQLite.

use crate::{DurabilityDomain, TxnParticipation};

use super::{make_policy, PolicyFlags, PolicyRow};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("ToMsgpack", make_policy(false, DurabilityDomain::None, "graph:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("FromMsgpack", make_policy(true, DurabilityDomain::GraphRedb, "graph:admin", PolicyFlags { idempotent: false, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "state-backed MutationBatch commits the imported authoritative image"),
    ("ClearLedger", make_policy(true, DurabilityDomain::GraphRedb, "ledger:admin", PolicyFlags { idempotent: true, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "state-backed MutationBatch"),
    ("ApplyLedger", make_policy(true, DurabilityDomain::GraphRedb, "ledger:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "state-backed MutationBatch"),
    ("Backup", make_policy(false, DurabilityDomain::None, "admin:backup", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "reads a consistent snapshot out to a bundle; does not mutate the live graph"),
    ("Restore", make_policy(true, DurabilityDomain::ControlRedb, "admin:backup", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), "prepared/committed admin MutationBatch saga"),
    ("TsAppend", make_policy(true, DurabilityDomain::SeriesRedb, "timeseries:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "graph ACL + placement policy precede the tenant/graph/series-scoped series.redb write"),
    ("TsRange", make_policy(false, DurabilityDomain::None, "timeseries:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("TsAsofJoin", make_policy(false, DurabilityDomain::None, "timeseries:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("TsWindow", make_policy(false, DurabilityDomain::None, "timeseries:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("TsGapFill", make_policy(false, DurabilityDomain::None, "timeseries:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("TsEvict", make_policy(true, DurabilityDomain::SeriesRedb, "timeseries:write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "content-idempotent unlike TsAppend: re-evicting an already-past cutoff is a safe no-op (see SeriesStore::evict_before)"),
    ("TsDeleteSeries", make_policy(true, DurabilityDomain::SeriesRedb, "timeseries:write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "content-idempotent unlike TsAppend: re-deleting an already-gone series is a safe no-op (see SeriesStore::delete_series)"),
    ("TsListSeries", make_policy(false, DurabilityDomain::None, "timeseries:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("BlobBegin", make_policy(true, DurabilityDomain::BlobRedb, "blob:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op"),
    ("BlobChunkPut", make_policy(true, DurabilityDomain::BlobRedb, "blob:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "durable via its own blob.redb (group-committed Immediate); self-routes before dispatch_graph_op"),
    ("BlobCommit", make_policy(true, DurabilityDomain::BlobRedb, "blob:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op"),
    ("BlobFetchBegin", make_policy(false, DurabilityDomain::None, "blob:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("BlobChunkGet", make_policy(false, DurabilityDomain::None, "blob:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("BlobFetchEnd", make_policy(false, DurabilityDomain::None, "blob:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("BlobRef", make_policy(true, DurabilityDomain::BlobRedb, "blob:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "refcount increment; idempotent-ish but re-invocation adds another ref, so not idempotent; durable via blob.redb"),
    ("BlobUnref", make_policy(true, DurabilityDomain::BlobRedb, "blob:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "durable via blob.redb"),
    ("BlobGc", make_policy(true, DurabilityDomain::BlobRedb, "blob:admin", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "durable via blob.redb"),
    ("KvGet", make_policy(false, DurabilityDomain::None, "kv:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("KvPut", make_policy(true, DurabilityDomain::KvRedb, "kv:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch"),
    ("KvDelete", make_policy(true, DurabilityDomain::KvRedb, "kv:write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "durable via its own kv.redb (redb::Durability::Immediate); self-routes before dispatch_graph_op"),
    ("KvScan", make_policy(false, DurabilityDomain::None, "kv:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("KvCas", make_policy(true, DurabilityDomain::KvRedb, "kv:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch"),
    ("ImportSqliteFile", make_policy(true, DurabilityDomain::ControlRedb, "admin:sqlite-file", PolicyFlags { idempotent: true, audited: true, emits_cdc: false }, TxnParticipation::Atomic), "native SQL-catalog MutationBatch; logical transfer name is excluded from the durable receipt"),
    ("ExportSqliteFile", make_policy(false, DurabilityDomain::None, "admin:sqlite-file", PolicyFlags { idempotent: true, audited: true, emits_cdc: false }, TxnParticipation::Snapshot), "operator-provisioned transfer root; logical filenames only"),
];
