//! Variant KINDS of the plan algebra (EH-370): a fieldless mirror of every [`Op`] and
//! [`Pred`] variant compiled into this build, plus the complete list of them.
//!
//! The list and the enum come from ONE macro invocation, so they cannot disagree; the
//! [`op_kind`]/[`pred_kind`] matches are exhaustive with no catch-all, so a new wire
//! variant is a compile error until it has a kind — and, through `uql_sample_op`/
//! `uql_sample_pred` (test support), a sample the UQL round-trip walk exercises. That is
//! what makes "every variant is parseable and printable" an executable contract rather
//! than a hand-maintained checklist.

use super::*;

macro_rules! kinds {
    ($(#[$doc:meta])* $kind:ident, $all:ident; $($(#[$cfg:meta])* $name:ident),+ $(,)?) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $kind {
            $($(#[$cfg])* $name,)+
        }

        impl $kind {
            /// Every kind compiled into this build, in declaration order.
            pub fn $all() -> Vec<$kind> {
                let mut all = Vec::new();
                $($(#[$cfg])* all.push($kind::$name);)+
                all
            }

            /// The variant's name (identical to the wire variant name).
            pub fn name(self) -> &'static str {
                match self {
                    $($(#[$cfg])* $kind::$name => stringify!($name),)+
                }
            }
        }
    };
}

kinds! {
    /// One [`Op`] variant, without its fields.
    OpKind, all;
    Scan, ScanAll, Filter, Traverse, Expand, Rank, RankEmbed, RankNodeDistance, RankMentions,
    RankMmr,
    #[cfg(feature = "text")] RankText,
    #[cfg(feature = "text")] FuseRrf,
    #[cfg(feature = "owl-plan")] Reason,
    #[cfg(feature = "owl-plan")] SparqlBgp,
    #[cfg(feature = "owl-plan")] ValidateShape,
    #[cfg(feature = "wasm-udf")] Udf,
    #[cfg(feature = "federation")] ForeignScan,
    AsOf, Window, WindowAgg, Foreign,
    #[cfg(feature = "geo")] SpatialScan,
    #[cfg(feature = "geo")] Reproject,
    #[cfg(feature = "geo")] SpatialOp,
    #[cfg(feature = "tensor")] TensorScan,
    #[cfg(feature = "tensor")] TensorOp,
    #[cfg(feature = "stream")] Cep,
    #[cfg(feature = "timeseries")] SensorFuse,
    #[cfg(feature = "timeseries")] SensorAlign,
    #[cfg(feature = "timeseries")] TsScan,
    #[cfg(feature = "probabilistic")] Probabilistic,
    #[cfg(feature = "epistemic")] EvidenceFor,
    #[cfg(feature = "epistemic")] Contradicts,
    #[cfg(feature = "epistemic")] SupportedBy,
    #[cfg(feature = "epistemic")] BeliefAsOf,
    #[cfg(feature = "epistemic")] SourceReliability,
    #[cfg(feature = "epistemic")] ConfidenceOp,
    #[cfg(feature = "epistemic")] ExplainBelief,
    Limit, Project,
}

kinds! {
    /// One [`Pred`] variant, without its fields.
    PredKind, all;
    Eq, GtNum, LtNum, Cmp, In, Between, IsNull, And, Or, Not, JsonPath,
    #[cfg(feature = "geo")] SpatialWithin,
    #[cfg(feature = "geo")] SpatialDWithin,
    #[cfg(feature = "geo")] SpatialContains,
    #[cfg(feature = "geo")] SpatialCovers,
    #[cfg(feature = "geo")] SpatialTouches,
    #[cfg(feature = "geo")] SpatialCrosses,
    #[cfg(feature = "geo")] SpatialOverlaps,
    #[cfg(feature = "geo")] SpatialEquals,
    #[cfg(feature = "geo")] SpatialDisjoint,
}

/// The kind of `op`.
pub fn op_kind(op: &Op) -> OpKind {
    match op {
        Op::Scan { .. } => OpKind::Scan,
        Op::ScanAll {} => OpKind::ScanAll,
        Op::Filter { .. } => OpKind::Filter,
        Op::Traverse { .. } => OpKind::Traverse,
        Op::Expand { .. } => OpKind::Expand,
        Op::Rank { .. } => OpKind::Rank,
        Op::RankEmbed { .. } => OpKind::RankEmbed,
        Op::RankNodeDistance { .. } => OpKind::RankNodeDistance,
        Op::RankMentions {} => OpKind::RankMentions,
        Op::RankMmr { .. } => OpKind::RankMmr,
        #[cfg(feature = "text")]
        Op::RankText { .. } => OpKind::RankText,
        #[cfg(feature = "text")]
        Op::FuseRrf { .. } => OpKind::FuseRrf,
        #[cfg(feature = "owl-plan")]
        Op::Reason { .. } => OpKind::Reason,
        #[cfg(feature = "owl-plan")]
        Op::SparqlBgp { .. } => OpKind::SparqlBgp,
        #[cfg(feature = "owl-plan")]
        Op::ValidateShape { .. } => OpKind::ValidateShape,
        #[cfg(feature = "wasm-udf")]
        Op::Udf { .. } => OpKind::Udf,
        #[cfg(feature = "federation")]
        Op::ForeignScan { .. } => OpKind::ForeignScan,
        Op::AsOf { .. } => OpKind::AsOf,
        Op::Window { .. } => OpKind::Window,
        Op::WindowAgg { .. } => OpKind::WindowAgg,
        Op::Foreign { .. } => OpKind::Foreign,
        #[cfg(feature = "geo")]
        Op::SpatialScan { .. } => OpKind::SpatialScan,
        #[cfg(feature = "geo")]
        Op::Reproject { .. } => OpKind::Reproject,
        #[cfg(feature = "geo")]
        Op::SpatialOp { .. } => OpKind::SpatialOp,
        #[cfg(feature = "tensor")]
        Op::TensorScan { .. } => OpKind::TensorScan,
        #[cfg(feature = "tensor")]
        Op::TensorOp { .. } => OpKind::TensorOp,
        #[cfg(feature = "stream")]
        Op::Cep { .. } => OpKind::Cep,
        #[cfg(feature = "timeseries")]
        Op::SensorFuse { .. } => OpKind::SensorFuse,
        #[cfg(feature = "timeseries")]
        Op::SensorAlign { .. } => OpKind::SensorAlign,
        #[cfg(feature = "timeseries")]
        Op::TsScan { .. } => OpKind::TsScan,
        #[cfg(feature = "probabilistic")]
        Op::Probabilistic { .. } => OpKind::Probabilistic,
        #[cfg(feature = "epistemic")]
        Op::EvidenceFor { .. } => OpKind::EvidenceFor,
        #[cfg(feature = "epistemic")]
        Op::Contradicts { .. } => OpKind::Contradicts,
        #[cfg(feature = "epistemic")]
        Op::SupportedBy { .. } => OpKind::SupportedBy,
        #[cfg(feature = "epistemic")]
        Op::BeliefAsOf { .. } => OpKind::BeliefAsOf,
        #[cfg(feature = "epistemic")]
        Op::SourceReliability { .. } => OpKind::SourceReliability,
        #[cfg(feature = "epistemic")]
        Op::ConfidenceOp {} => OpKind::ConfidenceOp,
        #[cfg(feature = "epistemic")]
        Op::ExplainBelief { .. } => OpKind::ExplainBelief,
        Op::Limit { .. } => OpKind::Limit,
        Op::Project { .. } => OpKind::Project,
    }
}

/// The kind of `pred`.
pub fn pred_kind(pred: &Pred) -> PredKind {
    match pred {
        Pred::Eq { .. } => PredKind::Eq,
        Pred::GtNum { .. } => PredKind::GtNum,
        Pred::LtNum { .. } => PredKind::LtNum,
        Pred::Cmp { .. } => PredKind::Cmp,
        Pred::In { .. } => PredKind::In,
        Pred::Between { .. } => PredKind::Between,
        Pred::IsNull { .. } => PredKind::IsNull,
        Pred::And { .. } => PredKind::And,
        Pred::Or { .. } => PredKind::Or,
        Pred::Not { .. } => PredKind::Not,
        Pred::JsonPath { .. } => PredKind::JsonPath,
        #[cfg(feature = "geo")]
        Pred::SpatialWithin { .. } => PredKind::SpatialWithin,
        #[cfg(feature = "geo")]
        Pred::SpatialDWithin { .. } => PredKind::SpatialDWithin,
        #[cfg(feature = "geo")]
        Pred::SpatialContains { .. } => PredKind::SpatialContains,
        #[cfg(feature = "geo")]
        Pred::SpatialCovers { .. } => PredKind::SpatialCovers,
        #[cfg(feature = "geo")]
        Pred::SpatialTouches { .. } => PredKind::SpatialTouches,
        #[cfg(feature = "geo")]
        Pred::SpatialCrosses { .. } => PredKind::SpatialCrosses,
        #[cfg(feature = "geo")]
        Pred::SpatialOverlaps { .. } => PredKind::SpatialOverlaps,
        #[cfg(feature = "geo")]
        Pred::SpatialEquals { .. } => PredKind::SpatialEquals,
        #[cfg(feature = "geo")]
        Pred::SpatialDisjoint { .. } => PredKind::SpatialDisjoint,
    }
}
