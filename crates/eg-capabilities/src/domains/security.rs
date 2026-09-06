//! Audit, identity, RBAC, and policy-export capabilities.

use crate::{DurabilityDomain, TxnParticipation};

use super::{make_policy, PolicyFlags, PolicyRow};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("GetLedger", make_policy(false, DurabilityDomain::None, "ledger:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("AuditVerify", make_policy(false, DurabilityDomain::None, "security:audit", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("AuditProveInclusion", make_policy(false, DurabilityDomain::None, "security:audit", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "provenance anchoring: Merkle inclusion proof for one node against a prior PROVENANCE_ANCHOR audit-chain entry"),
    ("RegisterIdentity", make_policy(true, DurabilityDomain::ControlRedb, "security:admin", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX"),
    ("RbacAdmin", make_policy(true, DurabilityDomain::ControlRedb, "security:admin", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata"),
    ("GetIdentity", make_policy(false, DurabilityDomain::None, "security:admin", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "identity read-back closing the RegisterIdentity blind-upsert gap: None means unregistered/unknown, Some(identity) with empty roles means registered-and-confirmed-empty -- gated security:admin like RegisterIdentity/RbacAdmin so it grants no caller new privilege"),
];
