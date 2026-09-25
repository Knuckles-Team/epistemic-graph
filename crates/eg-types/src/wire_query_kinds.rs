//! Variant KINDS of the plan algebra (UQL-06): a fieldless mirror of every [`Op`] and
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
                vec![$($(#[$cfg])* $kind::$name,)+]
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
    Scan, ScanAll, Filter, Traverse, Expand, Propagate, Rank, RankEmbed, RankNodeDistance, RankMentions,
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
    #[cfg(feature = "timeseries")] Derive,
    #[cfg(feature = "timeseries")] Skill,
    #[cfg(feature = "timeseries")] Motif,
    #[cfg(feature = "timeseries")] Events,
    #[cfg(feature = "probabilistic")] Probabilistic,
    #[cfg(feature = "epistemic")] EvidenceFor,
    #[cfg(feature = "epistemic")] Contradicts,
    #[cfg(feature = "epistemic")] SupportedBy,
    #[cfg(feature = "epistemic")] BeliefAsOf,
    #[cfg(feature = "epistemic")] SourceReliability,
    #[cfg(feature = "epistemic")] ConfidenceOp,
    #[cfg(feature = "epistemic")] ExplainBelief,
    Attribute, DecisionScan, Limit, Project,
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
        Op::Propagate { .. } => OpKind::Propagate,
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
        #[cfg(feature = "timeseries")]
        Op::Derive { .. } => OpKind::Derive,
        #[cfg(feature = "timeseries")]
        Op::Skill { .. } => OpKind::Skill,
        #[cfg(feature = "timeseries")]
        Op::Motif { .. } => OpKind::Motif,
        #[cfg(feature = "timeseries")]
        Op::Events { .. } => OpKind::Events,
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
        Op::Attribute { .. } => OpKind::Attribute,
        Op::DecisionScan { .. } => OpKind::DecisionScan,
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

impl OpKind {
    /// The named score channel a stage of this kind writes (UQL-08): scoring stages record
    /// their score under a fixed name so several coexist in one result (`RETURN
    /// similarity, belief`); `None` for stages that do not score.
    pub fn score_channel(self) -> Option<&'static str> {
        match self {
            OpKind::Rank | OpKind::RankEmbed => Some("similarity"),
            OpKind::RankNodeDistance => Some("node_distance"),
            OpKind::RankMentions => Some("mentions"),
            OpKind::RankMmr => Some("mmr"),
            OpKind::Propagate => Some("impact"),
            #[cfg(feature = "text")]
            OpKind::RankText => Some("text"),
            #[cfg(feature = "text")]
            OpKind::FuseRrf => Some("fused"),
            #[cfg(feature = "owl-plan")]
            OpKind::Reason => Some("reason"),
            OpKind::Window | OpKind::WindowAgg => Some("window"),
            OpKind::Attribute => Some("attribution"),
            #[cfg(feature = "timeseries")]
            OpKind::TsScan | OpKind::SensorFuse | OpKind::SensorAlign => Some("value"),
            #[cfg(feature = "probabilistic")]
            OpKind::Probabilistic => Some("probability"),
            #[cfg(feature = "epistemic")]
            OpKind::BeliefAsOf
            | OpKind::SourceReliability
            | OpKind::ConfidenceOp
            | OpKind::ExplainBelief => Some("belief"),
            OpKind::Scan
            | OpKind::ScanAll
            | OpKind::Filter
            | OpKind::Traverse
            | OpKind::Expand
            | OpKind::AsOf
            | OpKind::Foreign
            | OpKind::DecisionScan
            | OpKind::Limit
            | OpKind::Project => None,
            #[cfg(feature = "owl-plan")]
            OpKind::SparqlBgp | OpKind::ValidateShape => None,
            #[cfg(feature = "wasm-udf")]
            OpKind::Udf => None,
            #[cfg(feature = "federation")]
            OpKind::ForeignScan => None,
            #[cfg(feature = "geo")]
            OpKind::SpatialScan | OpKind::Reproject | OpKind::SpatialOp => None,
            #[cfg(feature = "tensor")]
            OpKind::TensorScan | OpKind::TensorOp => None,
            #[cfg(feature = "stream")]
            OpKind::Cep => None,
            // DERIVE and SKILL write named value channels, not a fixed score channel.
            #[cfg(feature = "timeseries")]
            OpKind::Derive | OpKind::Skill | OpKind::Motif | OpKind::Events => None,
            #[cfg(feature = "epistemic")]
            OpKind::EvidenceFor | OpKind::Contradicts | OpKind::SupportedBy => None,
        }
    }

    /// How a stage of this kind bears on a `WITH PROOF` row proof (EH-448).
    pub fn proof_role(self) -> ProofRole {
        match self {
            #[cfg(feature = "owl-plan")]
            OpKind::SparqlBgp | OpKind::Reason => ProofRole::Certifies,
            OpKind::Rank
            | OpKind::RankEmbed
            | OpKind::RankNodeDistance
            | OpKind::RankMentions
            | OpKind::RankMmr
            | OpKind::Attribute
            | OpKind::Limit
            | OpKind::Project => ProofRole::Neutral,
            #[cfg(feature = "text")]
            OpKind::RankText => ProofRole::Neutral,
            #[cfg(feature = "epistemic")]
            OpKind::SourceReliability | OpKind::ConfidenceOp => ProofRole::Neutral,
            #[cfg(feature = "timeseries")]
            OpKind::Derive => ProofRole::Neutral,
            OpKind::Scan
            | OpKind::ScanAll
            | OpKind::Filter
            | OpKind::Traverse
            | OpKind::Expand
            | OpKind::Propagate
            | OpKind::AsOf
            | OpKind::Window
            | OpKind::WindowAgg
            | OpKind::Foreign
            | OpKind::DecisionScan => ProofRole::Unproved,
            // A FUSE's branches decide it; eg-plan inspects them (`FuseRrf` is Neutral
            // exactly when every branch op is).
            #[cfg(feature = "text")]
            OpKind::FuseRrf => ProofRole::Unproved,
            #[cfg(feature = "owl-plan")]
            OpKind::ValidateShape => ProofRole::Unproved,
            #[cfg(feature = "wasm-udf")]
            OpKind::Udf => ProofRole::Unproved,
            #[cfg(feature = "federation")]
            OpKind::ForeignScan => ProofRole::Unproved,
            #[cfg(feature = "geo")]
            OpKind::SpatialScan | OpKind::Reproject | OpKind::SpatialOp => ProofRole::Unproved,
            #[cfg(feature = "tensor")]
            OpKind::TensorScan | OpKind::TensorOp => ProofRole::Unproved,
            #[cfg(feature = "stream")]
            OpKind::Cep => ProofRole::Unproved,
            #[cfg(feature = "timeseries")]
            OpKind::SensorFuse
            | OpKind::SensorAlign
            | OpKind::TsScan
            | OpKind::Skill
            | OpKind::Motif
            | OpKind::Events => ProofRole::Unproved,
            #[cfg(feature = "probabilistic")]
            OpKind::Probabilistic => ProofRole::Unproved,
            #[cfg(feature = "epistemic")]
            OpKind::EvidenceFor
            | OpKind::Contradicts
            | OpKind::SupportedBy
            | OpKind::BeliefAsOf
            | OpKind::ExplainBelief => ProofRole::Unproved,
        }
    }

    /// The SECONDARY channels a stage of this kind writes beside its score channel:
    /// an `ATTRIBUTE` stage's CI half-width (EH-523) and a `SOURCE RELIABILITY` stage's
    /// learned-reliability interval (EH-525). Empty for every other kind.
    pub fn extra_channels(self) -> &'static [&'static str] {
        if self == OpKind::Attribute {
            return ATTRIBUTION_EXTRA_CHANNELS;
        }
        #[cfg(feature = "epistemic")]
        if self == OpKind::SourceReliability {
            return RELIABILITY_EXTRA_CHANNELS;
        }
        &[]
    }

    /// Every score channel name this build can produce, sorted and de-duplicated.
    pub fn score_channels() -> Vec<&'static str> {
        let mut names: Vec<&'static str> = OpKind::all()
            .into_iter()
            .flat_map(|kind| {
                kind.score_channel()
                    .into_iter()
                    .chain(kind.extra_channels().iter().copied())
            })
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }
}

/// `ATTRIBUTE`'s secondary channel: the sampled-Shapley CI half-width.
pub const ATTRIBUTION_EXTRA_CHANNELS: &[&str] = &["attribution_ci"];
/// `SOURCE RELIABILITY`'s secondary channels: the learned reliability's credible interval.
#[cfg(feature = "epistemic")]
pub const RELIABILITY_EXTRA_CHANNELS: &[&str] = &["reliability_lo", "reliability_hi"];

/// How one stage bears on a `WITH PROOF` row proof (EH-448).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProofRole {
    /// It admits rows AND can prove each admission (SPARQL witness, OWL membership).
    Certifies,
    /// It only orders, scores or cuts rows it was given — nothing to prove.
    Neutral,
    /// It admits or produces rows but carries no proof.
    Unproved,
}
