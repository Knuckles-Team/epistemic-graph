//! Datalog, RDF, OWL, SHACL, SHEX, and reasoning projection capabilities.

use crate::{DurabilityDomain, TxnParticipation};

use super::{make_policy, PolicyFlags, PolicyRow};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("RunDatalogReasoning", make_policy(true, DurabilityDomain::GraphRedb, "reasoning:write", PolicyFlags { idempotent: false, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "state-backed MutationBatch commits inferred facts"),
    ("GetRdf", make_policy(false, DurabilityDomain::None, "rdf:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("Sparql", make_policy(false, DurabilityDomain::None, "sparql:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("SparqlVirtual", make_policy(false, DurabilityDomain::None, "sparql:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("OwlReason", make_policy(false, DurabilityDomain::None, "owl:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("OwlReasonDistributed", make_policy(false, DurabilityDomain::None, "owl:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("OwlExplain", make_policy(false, DurabilityDomain::None, "owl:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("RunRules", make_policy(false, DurabilityDomain::None, "reasoning:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "READ-ONLY (EG-P0-2/L11 handler audit): handle_run_rules reasons over an off-lock analysis_snapshot and returns inferred triples, no writeback -- unlike its sibling RunDatalogReasoning which materialises in-place. Corrected from a prior mutates=true semantic guess; now agrees with access.rs (never a write there)"),
    ("ShaclValidate", make_policy(false, DurabilityDomain::None, "validation:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("IcvConfigure", make_policy(true, DurabilityDomain::GraphRedb, "security:admin", PolicyFlags { idempotent: true, audited: true, emits_cdc: true }, TxnParticipation::Atomic), "state-backed MutationBatch"),
    ("ShexValidate", make_policy(false, DurabilityDomain::None, "validation:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
];
