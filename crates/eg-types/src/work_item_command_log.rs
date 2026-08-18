//! GOC-19 — the WorkItem submission command-log admission core, built on
//! GOC-03's [`crate::commit_descriptor::CommitDescriptorV1`] currency.
//!
//! # Scope and honesty about what this module is NOT
//!
//! `agent-utilities` `orchestration/work_item.py::_submit_work_item` currently
//! performs a *read-before-create* sequence against the engine — a separate
//! `get_work_item` idempotency probe, a separate `tenant_in_flight_count`
//! quota read, a per-dependency `get_work_item` loop, then `add_node`/
//! `create_node_if_absent`, then per-dependency `_link` calls, then a final
//! `_reconcile_dependency_readiness` pass — none of which race-proof against
//! a concurrent duplicate submit (confirmed 2026-08-16 against this repo's
//! `main`: no `protocol::Method::SubmitWorkItem` variant exists anywhere in
//! `crates/eg-types/src/protocol.rs`, and the only submission-time engine
//! guard, `redb_store/work_item_capability.rs::validate_submission_properties`,
//! rejects a generic `AddNode`/`BatchUpdate` from smuggling in active
//! lease/authority fields — it does not perform quota, dependency, or
//! idempotency-key admission at all; those stay wholly client-side).
//!
//! This module is the **admission-decision core** a future
//! `Method::SubmitWorkItem` redb-apply handler would call from *inside* the
//! same durable write transaction that creates the WorkItem row, indexes its
//! dependency edges, and appends the outbox record — mirroring exactly how
//! `crates/eg-types/src/lake_catalog.rs`/`commit_descriptor.rs` themselves
//! shipped as additive, unconditional, pure-data/logic modules ahead of their
//! wire-protocol wiring (see this crate's `lib.rs` doc comment on
//! `lake_catalog`: "adds no `protocol::Method` variant... not yet
//! implemented"). Wiring a `Method::SubmitWorkItem` variant, an
//! `eg-capabilities` policy entry, `mutation_batch.rs` request handling, and
//! the redb apply/outbox/index integration remains **unstarted** — that is
//! real follow-on engine work, not something this module can honestly claim
//! to deliver by itself. What this module DOES deliver, with tests proving
//! it against known-bad inputs:
//!
//! 1. Tenant-scoped idempotency: replaying `(tenant_ref, idempotency_key)`
//!    with the same `mutation_digest` returns the ORIGINAL record — never a
//!    second WorkItem admission — even if the retried request nominally
//!    proposes a different `commit_id`/`work_item_id` (invariant 5 in the
//!    lane doc).
//! 2. Idempotency-key reuse with a DIFFERENT `mutation_digest` is an explicit
//!    conflict, never silently treated as a replay.
//! 3. Fencing: a command whose `authority_epoch` is behind, or whose
//!    `commit_seq`/`fencing_token` does not represent forward progress for
//!    its `authority_ref`, is rejected — mirroring the exact "strictly
//!    greater fencing token on a newer sequence" rule
//!    [`crate::commit_descriptor::ProjectionCursorV1::advance`] already
//!    enforces and already has passing tests for.
//!
//! See `plans/graph-os-completion-program/lanes/GOC-19-atomic-workitem-command-log.md`.

use std::collections::BTreeMap;

use crate::commit_descriptor::CommitDescriptorV1;

/// One admitted (or replayed) WorkItem submission command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItemCommandRecord {
    /// The GOC-03 commit currency this command was admitted under.
    pub descriptor: CommitDescriptorV1,
    /// The WorkItem this command created (or, for a replay, the WorkItem the
    /// ORIGINAL admission created — never a second id).
    pub work_item_id: String,
    /// This command log's own monotonically increasing cursor position,
    /// independent of `commit_seq` (which is per-`authority_ref`). Backs the
    /// lane doc's `list_commands(cursor, tenant)` keyset pagination.
    pub command_sequence: u64,
}

/// Outcome of [`WorkItemCommandLog::submit`]. Every non-`Created`/`Replayed`
/// arm is a typed rejection — never a best-effort partial admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkItemCommandOutcome {
    /// A new command was admitted; a WorkItem is expected to be created.
    Created(WorkItemCommandRecord),
    /// `(tenant_ref, idempotency_key)` was already admitted with an identical
    /// `mutation_digest`; this is the ORIGINAL record, no second WorkItem was
    /// created.
    Replayed(WorkItemCommandRecord),
    /// `(tenant_ref, idempotency_key)` was already admitted with a
    /// DIFFERENT `mutation_digest` — the caller reused a retry key for a
    /// different logical command. Carries the commit_id already on file so
    /// the caller can surface it in the error.
    IdempotencyConflict { existing_commit_id: String },
    /// `authority_epoch` is behind the highest epoch this log has observed
    /// for `authority_ref` — a stale writer whose view of leadership is out
    /// of date.
    StaleAuthorityEpoch { observed_epoch: u64 },
    /// `commit_seq` does not exceed the highest sequence already observed
    /// for `authority_ref` (replay/rollback of an old sequence number).
    StaleCommitSeq { observed_commit_seq: u64 },
    /// `commit_seq` advanced but `fencing_token` did not strictly increase —
    /// the mechanical form of "a stale writer/reader that only knows an old
    /// `commit_seq` [must not] act as though it holds current authority"
    /// (`CommitDescriptorV1::fencing_token` doc).
    StaleFencingToken { observed_fencing_token: u64 },
    /// The descriptor itself failed [`CommitDescriptorV1::validate`]; never
    /// admitted, regardless of idempotency/fencing state.
    InvalidDescriptor(String),
}

impl WorkItemCommandOutcome {
    /// The record a caller should treat as authoritative, if any was
    /// admitted or already existed (`Created`/`Replayed`). `None` for every
    /// rejection arm.
    pub fn record(&self) -> Option<&WorkItemCommandRecord> {
        match self {
            WorkItemCommandOutcome::Created(record) | WorkItemCommandOutcome::Replayed(record) => {
                Some(record)
            }
            _ => None,
        }
    }

    /// Whether this outcome represents a NEW admission (as opposed to a
    /// replay or a rejection) — the lane doc's `created` boolean.
    pub fn created(&self) -> bool {
        matches!(self, WorkItemCommandOutcome::Created(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AuthorityWatermark {
    authority_epoch: u64,
    commit_seq: u64,
    fencing_token: u64,
}

/// The tenant-scoped idempotency index + per-authority fencing watermark a
/// `SubmitWorkItem` admission decision is checked against. Reference/testable
/// admission logic — holds no lock, performs no I/O, and is not itself wired
/// to `redb`; a durable apply handler owns persistence and would replay this
/// same decision function against durably-loaded state on every call.
#[derive(Debug, Default)]
pub struct WorkItemCommandLog {
    by_key: BTreeMap<(String, String), WorkItemCommandRecord>,
    watermarks: BTreeMap<String, AuthorityWatermark>,
    next_command_sequence: u64,
}

impl WorkItemCommandLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of DISTINCT `(tenant_ref, idempotency_key)` commands admitted.
    /// A replay or a rejection never changes this count — the direct
    /// mechanical proof that a replayed command does not double-apply.
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub fn get(&self, tenant_ref: &str, idempotency_key: &str) -> Option<&WorkItemCommandRecord> {
        self.by_key
            .get(&(tenant_ref.to_string(), idempotency_key.to_string()))
    }

    /// Decide whether `descriptor` (proposing `work_item_id`) may be
    /// admitted. Structural validation, tenant-scoped idempotency, and
    /// per-authority fencing are checked in that order; the first failure
    /// wins and nothing is recorded.
    pub fn submit(
        &mut self,
        descriptor: CommitDescriptorV1,
        work_item_id: String,
    ) -> WorkItemCommandOutcome {
        if let Err(error) = descriptor.validate() {
            return WorkItemCommandOutcome::InvalidDescriptor(error);
        }

        let key = (
            descriptor.tenant_ref.clone(),
            descriptor.idempotency_key.clone(),
        );
        if let Some(existing) = self.by_key.get(&key) {
            return if existing.descriptor.mutation_digest == descriptor.mutation_digest {
                WorkItemCommandOutcome::Replayed(existing.clone())
            } else {
                WorkItemCommandOutcome::IdempotencyConflict {
                    existing_commit_id: existing.descriptor.commit_id.clone(),
                }
            };
        }

        if let Some(watermark) = self.watermarks.get(&descriptor.authority_ref) {
            if descriptor.authority_epoch < watermark.authority_epoch {
                return WorkItemCommandOutcome::StaleAuthorityEpoch {
                    observed_epoch: watermark.authority_epoch,
                };
            }
            if descriptor.commit_seq <= watermark.commit_seq {
                return WorkItemCommandOutcome::StaleCommitSeq {
                    observed_commit_seq: watermark.commit_seq,
                };
            }
            if descriptor.fencing_token <= watermark.fencing_token {
                return WorkItemCommandOutcome::StaleFencingToken {
                    observed_fencing_token: watermark.fencing_token,
                };
            }
        }

        self.watermarks.insert(
            descriptor.authority_ref.clone(),
            AuthorityWatermark {
                authority_epoch: descriptor.authority_epoch,
                commit_seq: descriptor.commit_seq,
                fencing_token: descriptor.fencing_token,
            },
        );
        self.next_command_sequence += 1;
        let record = WorkItemCommandRecord {
            descriptor,
            work_item_id,
            command_sequence: self.next_command_sequence,
        };
        self.by_key.insert(key, record.clone());
        WorkItemCommandOutcome::Created(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_descriptor::{
        CommitParticipantDomain, CommitStatus, COMMIT_DESCRIPTOR_VERSION,
    };

    fn hex_digest(byte: u8) -> String {
        (0..32).map(|_| format!("{byte:02x}")).collect()
    }

    fn descriptor(
        commit_id: &str,
        idempotency_key: &str,
        tenant_ref: &str,
        authority_ref: &str,
        authority_epoch: u64,
        commit_seq: u64,
        fencing_token: u64,
        mutation_digest_byte: u8,
    ) -> CommitDescriptorV1 {
        let mut participants = BTreeMap::new();
        participants.insert(CommitParticipantDomain::Graph, hex_digest(0xAB));
        CommitDescriptorV1 {
            schema_version: COMMIT_DESCRIPTOR_VERSION,
            commit_id: commit_id.to_string(),
            txn_id: format!("txn-{commit_id}"),
            tenant_ref: tenant_ref.to_string(),
            principal_ref: format!("principal:sha256:{}", hex_digest(0x11)),
            authority_ref: authority_ref.to_string(),
            authority_epoch,
            source_graph_version: 10,
            target_graph_version: 11,
            commit_seq,
            fencing_token,
            mutation_digest: hex_digest(mutation_digest_byte),
            idempotency_key: idempotency_key.to_string(),
            participant_digests: participants,
            policy_digest: hex_digest(0x33),
            status: CommitStatus::Committed,
            prepared_at_ms: 100,
            decided_at_ms: Some(101),
            diagnostic_ref: None,
        }
    }

    // ---- PASS-on-good: a fresh submission is admitted ----

    #[test]
    fn submit_admits_a_new_command() {
        let mut log = WorkItemCommandLog::new();
        let d = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 1, 1, 100, 0x22);
        let outcome = log.submit(d, "wi-1".to_string());
        assert!(outcome.created(), "expected Created, got {outcome:?}");
        assert_eq!(log.len(), 1);
    }

    // ---- KNOWN-BAD: replaying the same idempotency key must not double-apply ----

    #[test]
    fn replay_same_key_same_digest_returns_original_without_double_apply() {
        let mut log = WorkItemCommandLog::new();
        let first = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 1, 1, 100, 0x22);
        let outcome1 = log.submit(first, "wi-1".to_string());
        assert!(outcome1.created());
        assert_eq!(log.len(), 1);

        // Retry proposes a DIFFERENT commit_id / work_item_id (as a caller
        // that lost the original response and re-submitted might) but the
        // same (tenant_ref, idempotency_key, mutation_digest).
        let retry = descriptor("commit-B", "idem-1", "tenant-1", "shard-0", 1, 1, 100, 0x22);
        let outcome2 = log.submit(retry, "wi-2-should-not-exist".to_string());

        assert_eq!(log.len(), 1, "a replay must not create a second command");
        match outcome2 {
            WorkItemCommandOutcome::Replayed(record) => {
                assert_eq!(
                    record.descriptor.commit_id, "commit-A",
                    "replay must return the ORIGINAL commit_id, not the retried one"
                );
                assert_eq!(
                    record.work_item_id, "wi-1",
                    "replay must return the ORIGINAL WorkItem id"
                );
            }
            other => panic!("expected Replayed, got {other:?}"),
        }
    }

    // ---- KNOWN-BAD: idempotency-key reuse with different content is a conflict, never a silent replay ----

    #[test]
    fn same_key_different_digest_is_an_explicit_conflict_not_a_replay() {
        let mut log = WorkItemCommandLog::new();
        let first = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 1, 1, 100, 0x22);
        log.submit(first, "wi-1".to_string());

        let different = descriptor("commit-C", "idem-1", "tenant-1", "shard-0", 1, 2, 200, 0x99);
        let outcome = log.submit(different, "wi-3".to_string());

        assert_eq!(
            log.len(),
            1,
            "a conflicting reuse must not create a second command"
        );
        match outcome {
            WorkItemCommandOutcome::IdempotencyConflict { existing_commit_id } => {
                assert_eq!(existing_commit_id, "commit-A");
            }
            other => panic!("expected IdempotencyConflict, got {other:?}"),
        }
    }

    // ---- KNOWN-BAD: a stale authority_epoch is rejected ----

    #[test]
    fn stale_authority_epoch_is_rejected() {
        let mut log = WorkItemCommandLog::new();
        let first = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 2, 5, 500, 0x22);
        assert!(log.submit(first, "wi-1".to_string()).created());

        // A different idempotency key so this is not merely the replay path.
        let stale = descriptor("commit-D", "idem-2", "tenant-1", "shard-0", 1, 6, 600, 0x33);
        let outcome = log.submit(stale, "wi-4".to_string());

        assert_eq!(
            log.len(),
            1,
            "a stale-epoch submission must not be admitted"
        );
        match outcome {
            WorkItemCommandOutcome::StaleAuthorityEpoch { observed_epoch } => {
                assert_eq!(observed_epoch, 2);
            }
            other => panic!("expected StaleAuthorityEpoch, got {other:?}"),
        }
    }

    // ---- KNOWN-BAD: a non-advancing commit_seq at the same epoch is rejected ----

    #[test]
    fn non_advancing_commit_seq_is_rejected() {
        let mut log = WorkItemCommandLog::new();
        let first = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 2, 5, 500, 0x22);
        assert!(log.submit(first, "wi-1".to_string()).created());

        let replayed_seq = descriptor("commit-E", "idem-2", "tenant-1", "shard-0", 2, 5, 600, 0x33);
        let outcome = log.submit(replayed_seq, "wi-5".to_string());

        assert_eq!(log.len(), 1);
        match outcome {
            WorkItemCommandOutcome::StaleCommitSeq {
                observed_commit_seq,
            } => {
                assert_eq!(observed_commit_seq, 5);
            }
            other => panic!("expected StaleCommitSeq, got {other:?}"),
        }
    }

    // ---- KNOWN-BAD: commit_seq advances but fencing_token does not — rejected ----

    #[test]
    fn stale_fencing_token_on_newer_sequence_is_rejected() {
        let mut log = WorkItemCommandLog::new();
        let first = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 2, 5, 500, 0x22);
        assert!(log.submit(first, "wi-1".to_string()).created());

        // commit_seq strictly advances (6 > 5) but fencing_token does not
        // (400 <= 500) — a stale writer that only knows an old fencing token.
        let stale_fence = descriptor("commit-F", "idem-2", "tenant-1", "shard-0", 2, 6, 400, 0x33);
        let outcome = log.submit(stale_fence, "wi-6".to_string());

        assert_eq!(log.len(), 1);
        match outcome {
            WorkItemCommandOutcome::StaleFencingToken {
                observed_fencing_token,
            } => {
                assert_eq!(observed_fencing_token, 500);
            }
            other => panic!("expected StaleFencingToken, got {other:?}"),
        }
    }

    // ---- PASS-on-good: forward progress (higher commit_seq AND higher fencing_token) is admitted ----

    #[test]
    fn forward_progress_is_admitted_and_advances_the_watermark() {
        let mut log = WorkItemCommandLog::new();
        let first = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 2, 5, 500, 0x22);
        assert!(log.submit(first, "wi-1".to_string()).created());

        let second = descriptor("commit-G", "idem-2", "tenant-1", "shard-0", 2, 6, 600, 0x33);
        let outcome = log.submit(second, "wi-2".to_string());
        assert!(outcome.created(), "expected Created, got {outcome:?}");
        assert_eq!(log.len(), 2);

        let third = descriptor("commit-H", "idem-3", "tenant-1", "shard-0", 3, 7, 700, 0x44);
        let outcome3 = log.submit(third, "wi-3".to_string());
        assert!(
            outcome3.created(),
            "a higher epoch with forward progress must be admitted"
        );
        assert_eq!(log.len(), 3);
    }

    // ---- PASS-on-good: the idempotency index is tenant-scoped ----

    #[test]
    fn idempotency_index_is_tenant_scoped() {
        let mut log = WorkItemCommandLog::new();
        let tenant_a = descriptor(
            "commit-A",
            "shared-key",
            "tenant-a",
            "shard-0",
            1,
            1,
            100,
            0x22,
        );
        let tenant_b = descriptor(
            "commit-B",
            "shared-key",
            "tenant-b",
            "shard-0",
            1,
            2,
            200,
            0x22,
        );

        assert!(log.submit(tenant_a, "wi-a".to_string()).created());
        // Same idempotency_key string, different tenant: must be an
        // independent admission, not a replay/conflict of tenant-a's command.
        let outcome_b = log.submit(tenant_b, "wi-b".to_string());
        assert!(
            outcome_b.created(),
            "expected Created for a different tenant, got {outcome_b:?}"
        );
        assert_eq!(log.len(), 2);
        assert_eq!(
            log.get("tenant-a", "shared-key").unwrap().work_item_id,
            "wi-a"
        );
        assert_eq!(
            log.get("tenant-b", "shared-key").unwrap().work_item_id,
            "wi-b"
        );
    }

    // ---- KNOWN-BAD: a structurally invalid descriptor is rejected before any admission bookkeeping ----

    #[test]
    fn structurally_invalid_descriptor_is_rejected() {
        let mut log = WorkItemCommandLog::new();
        let mut invalid = descriptor("commit-A", "idem-1", "tenant-1", "shard-0", 1, 1, 100, 0x22);
        invalid.commit_seq = 0; // CommitDescriptorV1::validate() rejects this
        let outcome = log.submit(invalid, "wi-1".to_string());
        assert!(log.is_empty());
        match outcome {
            WorkItemCommandOutcome::InvalidDescriptor(message) => {
                assert!(message.contains("commit_seq"), "got: {message}");
            }
            other => panic!("expected InvalidDescriptor, got {other:?}"),
        }
    }
}
