//! Audit, identity, RBAC, and policy-export capabilities.

use crate::{DurabilityDomain, Stability, TxnParticipation};

use super::{make_policy, spec, PolicyFlags, PolicyRow, PYTHON};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("GetLedger", spec(make_policy(false, DurabilityDomain::None, "ledger:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("AuditVerify", spec(make_policy(false, DurabilityDomain::None, "security:audit", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("AuditProveInclusion", spec(make_policy(false, DurabilityDomain::None, "security:audit", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), "provenance anchoring: Merkle inclusion proof for one node against a prior PROVENANCE_ANCHOR audit-chain entry"),
    ("RegisterIdentity", spec(make_policy(true, DurabilityDomain::ControlRedb, "security:admin", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), PYTHON, Stability::Stable), "RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX"),
    ("RbacAdmin", spec(make_policy(true, DurabilityDomain::ControlRedb, "security:admin", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Atomic), PYTHON, Stability::Stable), "runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata"),
    ("GetIdentity", spec(make_policy(false, DurabilityDomain::None, "security:admin", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), "identity read-back closing the RegisterIdentity blind-upsert gap: None means unregistered/unknown, Some(identity) with empty roles means registered-and-confirmed-empty -- gated security:admin like RegisterIdentity/RbacAdmin so it grants no caller new privilege"),
];
