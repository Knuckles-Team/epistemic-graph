//! The federation optimizer (CONCEPT:EG-KG.query.query-federation, EH-563; design:
//! `plans/refactor/architecture/FEDERATION-OPTIMIZER-DESIGN.md`).
//!
//! A foreign leaf (`Op::ForeignScan`, `Op::Foreign`) no longer means "fetch the whole
//! source, then evaluate locally". The optimizer lowers it to a [`RemoteRequest`] that
//! carries only what the source can evaluate — declared by its [`SourceCapabilities`] — and
//! keeps the rest as the local residual:
//!
//!  * **bind join** — a `ForeignScan { join: true }` over a non-empty local candidate set
//!    sends the local ids as batched key lookups (AIMD-sized batches, learned fallback)
//!    instead of fetching the source; the local intersect still decides the result;
//!  * **LIMIT propagation** — a source op immediately followed by `Limit k` asks the source
//!    for `k` rows (refilling from a full fetch when de-duplication leaves fewer);
//!  * **pagination** — a paged HTTP source is read to its end (or the limit/budget), never
//!    silently truncated to page one.
//!
//! Every query runs under a [`FederationBudget`] (typed refusal, never partial rows) and
//! records a [`FragmentTrace`] per remote fragment for EXPLAIN/PROFILE. Pushed requests only
//! ever NARROW what is fetched from sources the caller's own registry resolved.
//! `EPISTEMIC_GRAPH_FEDERATION_OPT=0` restores the naive full-fetch path (the equivalence
//! oracle).

mod budget;
mod cache;
mod capability;
mod engine;
mod http;
mod limiter;
pub(crate) mod oq2;
mod remote;
mod run;
mod session;
mod sql;
mod stats;
mod strategy;
mod trace;

#[cfg(test)]
mod bounds_tests;
#[cfg(test)]
mod engine_peer_tests;
#[cfg(test)]
mod tests;

pub use budget::{FederationBudget, BUDGET_EXCEEDED, REQUIRES_KEYS, RESULT_INCOMPLETE};
pub use cache::{FragmentCacheScope, SourceWatermark};
pub use capability::{
    FullFetch, KeyLookup, LimitPushdown, PageRequest, Paging, RemoteRequest, SourceCapabilities,
    SourceRate,
};
pub use oq2::{target_capabilities as oq2_target_capabilities, Oq2ReadMode, Oq2TargetCapabilities};
pub use session::FederationSession;
pub use stats::{stats_snapshot, SourceStats};
pub(crate) use trace::redacted_label;
pub use trace::{render_trace, EstimateProvenance, FetchStrategy, FragmentTrace};

pub(crate) use run::{foreign_named, foreign_scan};

use crate::exec::PlanCtx;
use crate::federation::ForeignSourceRegistry;
use crate::rowset::RowSet;
use eg_types::wire::ForeignSourceSpec;

/// Is the federation optimizer active? True unless `EPISTEMIC_GRAPH_FEDERATION_OPT=0`.
pub fn enabled() -> bool {
    !matches!(
        std::env::var("EPISTEMIC_GRAPH_FEDERATION_OPT")
            .ok()
            .as_deref(),
        Some("0")
    )
}

/// The plan-only source identity for EXPLAIN. This uses the same fingerprint as
/// execution while omitting registered names, URLs, DSNs and credentials.
pub(crate) fn explain_source(op: &crate::algebra::Op) -> Option<String> {
    let identity = match op {
        crate::algebra::Op::Foreign { name } => {
            remote::Identity::of_spec(&ForeignSourceSpec::Named { name: name.clone() }, None)
        }
        crate::algebra::Op::ForeignScan { source, .. } => remote::Identity::of_spec(source, None),
        _ => return None,
    };
    Some(trace::redacted_label(&identity.label))
}

/// Record the LIMIT hints of the plan about to run on the ctx's session, if one is bound.
pub(crate) fn prepare(ctx: &PlanCtx, ops: &[crate::algebra::Op]) {
    if let Some(session) = ctx.federation {
        session.prepare(ops);
    }
}

/// Resolve a foreign [`ForeignSourceSpec`] to ALL of its rows through the single leaf-source
/// seam (CONCEPT:EG-KG.query.symmetric-foreign-scan) — the naive full fetch. A `Named` spec
/// resolves through the `registry` on the `PlanCtx` (a clean typed error if none is
/// attached); every self-describing spec resolves via [`crate::federation::source_for`].
pub fn foreign_source_rows(
    spec: &ForeignSourceSpec,
    registry: Option<&ForeignSourceRegistry>,
) -> Result<RowSet, String> {
    match spec {
        ForeignSourceSpec::Named { name } => {
            let registry = registry.ok_or_else(|| {
                format!(
                    "federation: Op::ForeignScan names foreign source '{name}' but no \
                     ForeignSourceRegistry is attached to the PlanCtx \
                     (CONCEPT:EG-KG.query.symmetric-foreign-scan)"
                )
            })?;
            registry.resolve(name)
        }
        other => crate::federation::source_for(other).fetch(),
    }
}

/// Fuse a foreign RowSet with the local candidate set the SAME way for EVERY foreign kind:
/// `join=false` ⇒ the foreign rows REPLACE the input (a pure source); `join=true` ⇒
/// intersect keyed on id, preserving the input's order (a foreign∩local JOIN). An empty
/// input under `join=true` keeps the source behaviour (EG-405: an empty intermediate
/// re-seeds).
pub(crate) fn fuse_foreign(input: RowSet, foreign: RowSet, join: bool) -> RowSet {
    if join && !input.is_empty() {
        let keep = foreign.id_set();
        input.intersect_keep_order(&keep)
    } else {
        foreign
    }
}
