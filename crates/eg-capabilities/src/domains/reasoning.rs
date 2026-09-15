//! Datalog, RDF, OWL, SHACL, SHEX, and reasoning projection capabilities.

use crate::{DurabilityDomain, Stability, TxnParticipation};

use super::{make_policy, spec, PolicyFlags, PolicyRow, NO_CONSUMER, PYTHON};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("RunDatalogReasoning", spec(make_policy(true, DurabilityDomain::GraphRedb, "reasoning:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: true }, TxnParticipation::Atomic), PYTHON, Stability::Stable), "state-backed MutationBatch commits inferred facts"),
    ("GetRdf", spec(make_policy(false, DurabilityDomain::None, "rdf:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("Sparql", spec(make_policy(false, DurabilityDomain::None, "sparql:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("SparqlVirtual", spec(make_policy(false, DurabilityDomain::None, "sparql:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("OwlReason", spec(make_policy(false, DurabilityDomain::None, "owl:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("OwlReasonDistributed", spec(make_policy(false, DurabilityDomain::None, "owl:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("OwlExplain", spec(make_policy(false, DurabilityDomain::None, "owl:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("RunRules", spec(make_policy(false, DurabilityDomain::None, "reasoning:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), NO_CONSUMER, Stability::Internal), "READ-ONLY (EG-P0-2/L11 handler audit): handle_run_rules reasons over an off-lock analysis_snapshot and returns inferred triples, no writeback -- unlike its sibling RunDatalogReasoning which materialises in-place. Corrected from a prior mutates=true semantic guess; now agrees with access.rs (never a write there)"),
    ("ShaclValidate", spec(make_policy(false, DurabilityDomain::None, "validation:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), PYTHON, Stability::Stable), ""),
    ("IcvConfigure", spec(make_policy(true, DurabilityDomain::GraphRedb, "security:admin", PolicyFlags { idempotent: true, audited: true, emits_cdc: true }, TxnParticipation::Atomic), PYTHON, Stability::Stable), "state-backed MutationBatch"),
    ("ShexValidate", spec(make_policy(false, DurabilityDomain::None, "validation:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), NO_CONSUMER, Stability::Internal), ""),
];
