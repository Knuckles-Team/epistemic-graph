//! Query, provenance, materialization, and read-projection capabilities.

use crate::{DurabilityDomain, TxnParticipation};

use super::{make_policy, PolicyFlags, PolicyRow};

pub(crate) const ROWS: &[PolicyRow] = &[
    ("GetContextView", make_policy(false, DurabilityDomain::None, "node:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("GetChangeEnvelope", make_policy(false, DurabilityDomain::None, "ingest:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "Verified tenant-scoped reconciliation read"),
    ("GetContentVersion", make_policy(false, DurabilityDomain::None, "ingest:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "Typed content versions are never compared lexically"),
    ("GetChangeCursor", make_policy(false, DurabilityDomain::None, "ingest:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "Typed source cursors are tenant/graph/partition scoped"),
    ("Sql", make_policy(true, DurabilityDomain::GraphRedb, "query:sql", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::Atomic), "runtime-conditional; graph DML uses staged graph state while table/catalog writes atomically commit SQL rows plus MutationBatch status/fence/idempotency/outbox"),
    ("CypherQuery", make_policy(true, DurabilityDomain::GraphRedb, "query:cypher", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::Atomic), "runtime-conditional; writes execute against a staged graph and publish only after durable MutationBatch commit"),
    ("GraphQl", make_policy(true, DurabilityDomain::GraphRedb, "query:graphql", PolicyFlags { idempotent: false, audited: true, emits_cdc: false }, TxnParticipation::Atomic), "runtime-conditional; ordinary writes stage through MutationBatch and cross-modal commit atomically includes universal status/fence/idempotency/outbox"),
    #[cfg(feature = "knowledge-batch")]
    ("KnowledgeStream", make_policy(false, DurabilityDomain::None, "query:stream", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "one RequestContext/RLS/placement-bound stream with the sole native Arrow IPC projection for all seven query families"),
    ("UnifiedQuery", make_policy(false, DurabilityDomain::None, "query:unified", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("UnifiedQueryText", make_policy(false, DurabilityDomain::None, "query:unified", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("ExplainPlan", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("ExplainProvenance", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("ExplainProvenanceByIds", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of ExplainProvenance, same policy profile"),
    ("ExplainPolicy", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("ExplainBelief", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("EpistemicStatus", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "L53 (EPI-P3-5) acceptance capstone; handler additionally gated `epistemic-tms`"),
    ("WhatChanged", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "L53 (EPI-P3-5) bitemporal diff; handler additionally gated `epistemic-tms`"),
    ("RecomputeMaterialization", make_policy(true, DurabilityDomain::ReasoningProjection, "reasoning:write", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Atomic), "fenced recompute/writeback resolves provenance from the authoritative graph and fsyncs the per-graph projection"),
    ("MaterializationStatus", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "read-only status from the durable per-graph incremental reasoning authority"),
    ("StaleMaterializations", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "bulk opaque stale references from the durable per-graph incremental reasoning authority"),
    ("ResolveConflict", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "EPI-P3-7 (gap-fill) standalone Dung argumentation (grounded/preferred/stable) conflict resolution over a BeliefGraph snapshot; handler additionally gated `epistemic-tms`"),
    ("ExplainEvidence", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "CONCEPT:EG-X1 multimodal-citation resolver; handler additionally gated `evidence-graph`"),
    ("CausalEstimate", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "EPI-P3-3/P3-6 do-calculus intervention OR observational conditioning (selected by `mode`) over a request-carried SCM; handler additionally gated `epistemic-causal`"),
    ("CausalCounterfactual", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "EPI-P3-6 Pearl point-counterfactual over a request-carried SCM + a fully-observed unit; handler additionally gated `epistemic-causal`"),
    ("RankByProvenance", make_policy(false, DurabilityDomain::None, "explain:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), "EPI-P3-3 provenance-aware retrieval ranking; handler additionally gated `epistemic-causal`"),
    ("NlQuery", make_policy(false, DurabilityDomain::None, "query:nl", PolicyFlags { idempotent: false, audited: false, emits_cdc: false }, TxnParticipation::Snapshot), ""),
    ("TxnUnifiedQuery", make_policy(false, DurabilityDomain::None, "txn:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), ""),
    ("TxnUnifiedQueryText", make_policy(false, DurabilityDomain::None, "txn:read", PolicyFlags { idempotent: true, audited: false, emits_cdc: false }, TxnParticipation::Saga), ""),
];
