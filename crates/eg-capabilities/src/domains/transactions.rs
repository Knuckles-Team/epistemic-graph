//! Batch, change-envelope, multisig, and transaction-protocol capabilities.

use crate::{DurabilityDomain, TxnParticipation};

use super::{make_policy, PolicyFlags, PolicyRow};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("BatchUpdate", make_policy(true, DurabilityDomain::GraphRedb, "node:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::Atomic), ""),
    ("MultiGraphBatchUpdate", make_policy(true, DurabilityDomain::ControlRedb, "node:write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), "durable parent coordinator with per-graph MutationBatch children"),
    ("ApplyChangeEnvelope", make_policy(true, DurabilityDomain::GraphRedb, "ingest:write", PolicyFlags { idempotent: true, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "Engine-native object/material/governance/version/cursor/outbox commit; verified context is mandatory"),
    ("ApplyChangeEnvelopes", make_policy(true, DurabilityDomain::GraphRedb, "ingest:write", PolicyFlags { idempotent: true, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "Batch envelope coordinator: one coalesced graph transaction per shard-partition; same policy class as ApplyChangeEnvelope"),
    ("ApplyMultisigMutation", make_policy(true, DurabilityDomain::GraphRedb, "security:admin", PolicyFlags { idempotent: true, audited: true, emits_cdc: true }, TxnParticipation::Saga), "threshold validation translates into the graph MutationBatch gateway"),
    ("BeginTxn", make_policy(true, DurabilityDomain::ControlRedb, "txn:control", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native transaction staging authority"),
    ("TxnAddNode", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native staging; Commit owns graph publication"),
    ("TxnRemoveNode", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native staging; Commit owns graph publication"),
    ("TxnAddEdge", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native staging; Commit owns graph publication"),
    ("TxnRemoveEdge", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native staging; Commit owns graph publication"),
    ("TxnCas", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native staging; Commit owns graph publication"),
    ("TxnAddEmbedding", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native cross-modal staging"),
    ("TxnBlobRef", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native cross-modal staging"),
    ("TxnAddMeasurement", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native cross-modal staging"),
    ("TxnAxiom", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native cross-modal staging"),
    ("TxnConstruct", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native cross-modal staging"),
    ("TxnPlanWriteback", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native cross-modal staging"),
    ("TxnMaterializeBelief", make_policy(true, DurabilityDomain::ControlRedb, "txn:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native cross-modal staging"),
    ("Commit", make_policy(true, DurabilityDomain::ControlRedb, "txn:control", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), "named parent receipt plus atomic graph/cross-modal child batches"),
    ("Rollback", make_policy(true, DurabilityDomain::ControlRedb, "txn:control", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Saga), "encrypted Raft-native transaction staging removal"),
];
