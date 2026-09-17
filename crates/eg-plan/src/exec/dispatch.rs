//! Op-dispatch tiers behind `apply`, split out of `exec.rs` (KISS file budget)
//! purely so they do not grow the parent file past its aggregate line/function
//! caps. `Op` itself, and every arms behavior, is unchanged from when this was
//! `apply`'s own single match; each tier is exactly the arms that theme always had.

use super::*;

/// The one error every op-dispatch tier below (and [`super::apply`]'s own trailing arm)
/// falls back to when the `Op` variant it was handed exists on the wire
/// (`eg-types/query`'s full contract) but this build did not compile in the eg-plan
/// feature that runs it. That fallback is structurally required, not merely a metric
/// dodge.
pub(super) const UNSUPPORTED_MODALITY_OP: &str =
    "plan operator requires a modality feature not enabled in this build";

/// Is `op` one of the 7 `epistemic` ops (`EvidenceFor`/`Contradicts`/`SupportedBy`/
/// `BeliefAsOf`/`SourceReliability`/`ConfidenceOp`/`ExplainBelief`)? Used by
/// [`crate::optimizer`]'s reorder-exclusion check, which — unlike [`apply`]'s own
/// routing arm just below — is a plain boolean test, not an exhaustive match, so it can
/// delegate here instead of listing the variants itself.
#[cfg(feature = "epistemic")]
pub(crate) fn is_epistemic_op(op: &Op) -> bool {
    matches!(
        op,
        Op::ExplainBelief { .. }
            | Op::ConfidenceOp {}
            | Op::SourceReliability { .. }
            | Op::BeliefAsOf { .. }
            | Op::SupportedBy { .. }
            | Op::Contradicts { .. }
            | Op::EvidenceFor { .. }
    )
}

/// Dispatch one physical [`Op`] to its executor (CONCEPT:EG-KG.query.exec-arm-dispatch).
/// Grouped by theme into the tier functions below purely to keep each match's own arm
/// count under the complexity cap — [`Op`] itself, and every arm's behavior, is
/// unchanged; each tier is exactly the arms that theme always had.
pub(crate) fn apply(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        Op::Scan { .. }
        | Op::Filter { .. }
        | Op::Traverse { .. }
        | Op::Rank { .. }
        | Op::RankEmbed { .. }
        | Op::RankNodeDistance { .. }
        | Op::RankMentions {}
        | Op::RankMmr { .. }
        // TIME/FEDERATION source-narrowing ops that ship unconditionally: `AsOf`/`Window`/
        // `WindowAgg` push their own modality-availability check down into the physical fn
        // (Lane B, CONCEPT:EG-KG.query.driver-modality-fn) rather than a `#[cfg]` arm here;
        // `Foreign`/`Limit` have no feature gate at all.
        | Op::AsOf { .. }
        | Op::Window { .. }
        | Op::WindowAgg { .. }
        | Op::Foreign { .. }
        | Op::Limit { .. } => apply_core_ops(op, input, ctx),

        #[cfg(any(feature = "text", feature = "owl"))]
        Op::RankText { .. } | Op::FuseRrf { .. } | Op::Reason { .. } | Op::SparqlBgp { .. } => {
            apply_text_and_owl(op, input, ctx)
        }

        #[cfg(any(
            feature = "wasm-udf",
            feature = "federation",
            feature = "probabilistic",
            feature = "stream"
        ))]
        Op::Udf { .. } | Op::ForeignScan { .. } | Op::Probabilistic { .. } | Op::Cep { .. } => {
            apply_single_feature_ops(op, input, ctx)
        }

        #[cfg(any(feature = "geo", feature = "tensor"))]
        Op::SpatialScan { .. }
        | Op::Reproject { .. }
        | Op::SpatialOp { .. }
        | Op::TensorScan { .. }
        | Op::TensorOp { .. } => apply_geo_and_tensor(op, input, ctx),

        // SOURCE/FILTER/TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2):
        // evidence for/against a claim, belief-confidence re-scoring and time-pinning, source
        // reliability, and justification-tree flattening. Gated behind `epistemic`; the
        // variants only exist when eg-types/epistemic is on (the tensor/geo/probabilistic
        // gating precedent). Named explicitly (not routed through `is_epistemic_op` above,
        // which a match guard can't use) because a guarded wildcard arm does not count
        // toward exhaustiveness: under `--all-features` the trailing catch-all below is
        // itself compiled out, so every variant here MUST be an unconditional pattern or
        // the match stops being exhaustive.
        #[cfg(feature = "epistemic")]
        Op::EvidenceFor { .. }
        | Op::Contradicts { .. }
        | Op::SupportedBy { .. }
        | Op::BeliefAsOf { .. }
        | Op::SourceReliability { .. }
        | Op::ConfidenceOp {}
        | Op::ExplainBelief { .. } => apply_epistemic(op, input, ctx),

        // SOURCE/FUSE (time-series, CONCEPT:EG-KG.query.multi-rate-sensor-stream /
        // native-time-series): multi-rate sensor fusion (ASOF-aligned or clock-resampled) and
        // native eg-tsdb series scan. Gated behind `timeseries` (the eg-plan→eg-tsdb edge
        // `Op::Window` already opens).
        #[cfg(feature = "timeseries")]
        Op::SensorFuse { .. } | Op::SensorAlign { .. } | Op::TsScan { .. } => {
            apply_timeseries(op, input, ctx)
        }

        // `eg-types/query` (which gates this whole module) surfaces the FULL `Op` wire
        // contract, so modality ops whose executor lives behind an eg-plan feature the
        // current build did not enable (`geo`/`owl`/`federation`/`wasm-udf`/`stream`/
        // `epistemic`) still EXIST here. Their handler crates are absent, so decode them
        // to a typed "not built in this configuration" error rather than a non-exhaustive
        // match. Every one of those features is in the shipped `full` build, so this arm
        // is compiled out there (no handler is ever shadowed / left unreachable).
        #[cfg(not(all(
            feature = "geo",
            feature = "owl",
            feature = "federation",
            feature = "wasm-udf",
            feature = "stream",
            feature = "epistemic"
        )))]
        // Whether this arm is reachable depends on a feature resolution this crate
        // cannot see: `Op`'s variants are gated in the crate that DEFINES it, while
        // the handler arms above are gated on THIS crate's features. When cargo
        // unifies those identically (e.g. a query-only build, where the unhandled
        // variants do not exist either) every value is already matched and the arm
        // is dead; when a dependent enables the Op variants without enabling the
        // matching handler features here, it is the only thing standing between a
        // caller and a non-exhaustive match. That second case is real — it is why
        // the arm exists — so it must stay, and the lint has to be silenced for the
        // first. Narrowing the `cfg` cannot express this: it would have to name the
        // OTHER crate's resolved features.
        #[allow(unreachable_patterns)]
        _ => Err(UNSUPPORTED_MODALITY_OP.into()),
    }
}

/// SOURCE (`Scan`) and FILTER (`Filter`/`Traverse`), the RANK family, and TIME
/// (`AsOf`/`Window`/`WindowAgg`) + FEDERATION marker (`Foreign`) + `Limit` — every
/// always-on tier (none of the three groups carries any `#[cfg]` of its own; the
/// `text`-gated lexical `RankText` lives in [`apply_text_and_owl`]). All three share
/// one top-level `apply` arm purely to keep its own arm count down; each group's own
/// checks live in [`apply_ranking`] / [`apply_temporal_and_limit`] so this dispatcher's
/// own complexity stays minimal.
pub(super) fn apply_core_ops(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        Op::Scan { label } => Ok(scan_label(ctx.view, label)),
        Op::Filter { preds } => filter_op(ctx, preds, input),
        Op::Traverse { rel, min, max } => Ok(traverse_op(ctx, rel, *min, *max, input)),
        Op::Rank { .. }
        | Op::RankEmbed { .. }
        | Op::RankNodeDistance { .. }
        | Op::RankMentions {}
        | Op::RankMmr { .. } => apply_ranking(op, input, ctx),
        Op::AsOf { .. }
        | Op::Window { .. }
        | Op::WindowAgg { .. }
        | Op::Foreign { .. }
        | Op::Limit { .. } => apply_temporal_and_limit(op, input, ctx),
        _ => unreachable!("apply routed a non core Op here"),
    }
}

/// RANK family — vector, text-embed, node-distance, mention-count and MMR re-ranking.
fn apply_ranking(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        Op::Rank { query } => rank_op(ctx, query, input),
        Op::RankEmbed { text } => rank_embed_op(ctx, text, input),
        Op::RankNodeDistance { center } => Ok(rank_node_distance(ctx.view, input, center)),
        Op::RankMentions {} => Ok(rank_mentions(ctx.view, input)),
        Op::RankMmr { lambda, k } => Ok(rank_mmr(ctx.semantic, input, *lambda, *k)),
        _ => unreachable!("apply_core_ops routed a non ranking Op here"),
    }
}

/// RANK (lexical BM25) + FUSE (RRF) under `text`; OWL classification + BGP source under
/// `owl`. Two independent feature gates share one tier because each contributes only
/// two arms; every arm keeps its own `#[cfg]` exactly as it had at the top level.
pub(super) fn apply_text_and_owl(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        #[cfg(feature = "text")]
        Op::RankText { query } => Ok(rank_text(ctx, &input, query)),
        #[cfg(feature = "text")]
        Op::FuseRrf { branches, k } => fuse_rrf(ctx, &input, branches, *k),
        #[cfg(feature = "owl")]
        Op::Reason {
            target_class,
            ontology,
        } => reason_op(ctx.view, ctx.decay, input, target_class, ontology),
        #[cfg(feature = "owl")]
        Op::SparqlBgp { query, var } => sparql_source(ctx.view, query, var),
        // `input` may go unused here when a build enables neither `text` nor `owl` (the
        // routing arm in `apply` still exists under `any(text, owl)`); bind it explicitly
        // so the parameter is never reported unused in that configuration.
        #[allow(unreachable_patterns)]
        _ => {
            let _ = input;
            Err(UNSUPPORTED_MODALITY_OP.into())
        }
    }
}

/// Four independently-gated single-arm ops (`wasm-udf`/`federation`/`probabilistic`/
/// `stream`) sharing one tier since none is more than one arm on its own.
pub(super) fn apply_single_feature_ops(
    op: &Op,
    input: RowSet,
    ctx: &PlanCtx,
) -> Result<RowSet, String> {
    match op {
        #[cfg(feature = "wasm-udf")]
        Op::Udf { id } => udf_transform(ctx, &input, id),
        #[cfg(feature = "federation")]
        Op::ForeignScan { source, join } => foreign_scan(input, source, *join, ctx),
        #[cfg(feature = "probabilistic")]
        Op::Probabilistic { query } => Ok(probabilistic_op(ctx.view, input, query)),
        #[cfg(feature = "stream")]
        Op::Cep { pattern } => Ok(cep_op(ctx.view, input, pattern)),
        // See `apply_text_and_owl`'s fallback for why `input` is bound explicitly here.
        #[allow(unreachable_patterns)]
        _ => {
            let _ = input;
            Err(UNSUPPORTED_MODALITY_OP.into())
        }
    }
}

/// TIME (`AsOf`/`Window`/`WindowAgg`), FEDERATION marker (`Foreign`) and `Limit` — the
/// always-on temporal/row-budget tier.
fn apply_temporal_and_limit(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        // TIME — `AS OF [TX] @<ts>` is a real RowSet-narrowing temporal filter
        // (CONCEPT:AU-KG.compute.kg-2): drop rows whose fact is not live at `ts` on the chosen
        // timeline. Dep-free blob scan (no DataFusion), so it runs in the Pi tier.
        Op::AsOf { ts, axis } => Ok(as_of_filter(ctx.view, input, *ts, *axis)),
        // TIME (`WINDOW <dur>`, CONCEPT:EG-KG.query.streaming-execution) — a REAL windowed
        // aggregate via eg-tsdb's Pi-path `time_bucket`; [`window_op`] itself either runs the
        // native aggregate (`timeseries`) or passes the rows through (no feature).
        Op::Window { secs } => Ok(window_op(ctx, input, *secs)),
        // TIME (`WINDOW <dur> <agg>`, CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers)
        // — the selectable-aggregate sibling; [`window_agg_op`] resolves `agg` the same way.
        Op::WindowAgg { secs, agg } => Ok(window_agg_op(ctx, input, *secs, agg)),
        // FEDERATION (`FOREIGN "<name>"`, CONCEPT:EG-KG.query.sparql-completeness / EG-073) —
        // the name MARKER the UQL clause lowers to; resolves through the ctx registry
        // (`federation` build) or errors cleanly; foreign-source intent is never discarded.
        Op::Foreign { name } => foreign_named(name, input, ctx),
        Op::Limit { k } => Ok(input.limit(*k)),
        _ => unreachable!("apply routed a non temporal/limit Op here"),
    }
}

/// SOURCE/TRANSFORM (spatial, `geo`) — bbox scan, CRS reproject, constructive op; plus
/// SOURCE/TRANSFORM (tensor, `tensor`) — layer scan and per-row tensor op. Two
/// independent gates share one tier for the same reason as [`apply_text_and_owl`].
pub(super) fn apply_geo_and_tensor(
    op: &Op,
    input: RowSet,
    ctx: &PlanCtx,
) -> Result<RowSet, String> {
    match op {
        #[cfg(feature = "geo")]
        Op::SpatialScan { layer, bbox } => Ok(spatial_scan(ctx, layer, *bbox)),
        #[cfg(feature = "geo")]
        Op::Reproject { to_epsg, from_epsg } => {
            Ok(spatial_reproject(ctx.view, input, *to_epsg, *from_epsg))
        }
        #[cfg(feature = "geo")]
        Op::SpatialOp { kind } => Ok(spatial_op(ctx.view, input, kind)),
        #[cfg(feature = "tensor")]
        Op::TensorScan { layer } => Ok(tensor_scan(ctx.view, layer)),
        #[cfg(feature = "tensor")]
        Op::TensorOp { kind } => tensor_op(ctx, input, kind),
        // See `apply_text_and_owl`'s fallback for why `input` is bound explicitly here.
        #[allow(unreachable_patterns)]
        _ => {
            let _ = input;
            Err(UNSUPPORTED_MODALITY_OP.into())
        }
    }
}

/// SOURCE/FILTER/TRANSFORM (epistemic, CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) —
/// evidence for/against a claim or the claims a node supports, belief-confidence
/// time-pinning and per-row re-scoring, source-reliability re-weighting, and
/// justification-tree flattening. Every arm shares the single `epistemic` gate, so the
/// tier is exhaustive over its own scope with no fallback needed.
#[cfg(feature = "epistemic")]
pub(super) fn apply_epistemic(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    match op {
        Op::EvidenceFor { claim_id } => Ok(evidence_for_op(ctx.view, input, claim_id)),
        Op::Contradicts { node_id } => Ok(contradicts_op(ctx.view, input, node_id)),
        Op::SupportedBy { node_id } => Ok(supported_by_op(ctx.view, input, node_id)),
        Op::BeliefAsOf { ts } => Ok(belief_as_of_op(ctx, input, *ts)),
        Op::SourceReliability { source_id } => Ok(source_reliability_op(ctx, input, source_id)),
        Op::ConfidenceOp {} => Ok(confidence_op(ctx, input)),
        Op::ExplainBelief { node_id } => Ok(explain_belief_op(ctx, input, node_id)),
        _ => unreachable!("apply routed a non epistemic Op here"),
    }
}

/// FUSE (multimodal sensor fusion, CONCEPT:EG-KG.query.multi-rate-sensor-stream) — ASOF-
/// aligned (`SensorFuse`) or declared-clock-resampled (`SensorAlign`) stream fusion; plus
/// SOURCE (native time-series, CONCEPT:EG-KG.query.native-time-series) — `TsScan` off the
/// ctx-attached `SeriesStore`. Every arm shares the single `timeseries` gate.
#[cfg(feature = "timeseries")]
pub(super) fn apply_timeseries(op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    // Every arm here is a SOURCE that resolves its rows off the snapshot and replaces
    // `input` outright, so the incoming candidate set is intentionally dropped, not read.
    drop(input);
    match op {
        Op::SensorFuse {
            streams,
            tolerance_ns,
        } => Ok(sensor_fuse_op(ctx.view, streams, *tolerance_ns)),
        Op::SensorAlign {
            streams,
            clock,
            tolerance_ns,
        } => Ok(sensor_align_op(ctx.view, streams, clock, *tolerance_ns)),
        Op::TsScan { series, from, to } => Ok(tsdb_scan_op(
            ctx.tsdb,
            ctx.tsdb_tenant,
            ctx.tsdb_graph,
            ctx.staged_series,
            series,
            *from,
            *to,
        )),
        _ => unreachable!("apply routed a non timeseries Op here"),
    }
}
