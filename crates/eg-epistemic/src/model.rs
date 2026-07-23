//! Epistemic model types — all VIEW/compute values, none persisted.

use serde::{Deserialize, Serialize};

/// How one node's confidence bears on another it is linked to.
///
/// Classified from the edge's canonical `relationship` (see [`classify_relationship`]).
/// A support edge raises the target's belief; a contradiction or attack lowers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeKind {
    /// Evidence FOR the target (`SUPPORTS`, `SUPPORTS_BELIEF`, `HAS_EVIDENCE`).
    Supports,
    /// Evidence AGAINST the target (`CONTRADICTS`, `CONTRADICTS_BELIEF`).
    Contradicts,
    /// An argument that defeats the target (`ATTACKS`) — weighted like a
    /// contradiction but scaled by [`AuthorityPolicy::attack_multiplier`].
    Attacks,
}

/// Map an edge `relationship` string to an [`EdgeKind`], or `None` if the edge
/// is epistemically neutral (an ordinary structural edge that does not bear on belief).
///
/// The vocabulary mirrors the control-plane `RegistryEdgeType` names so a
/// `SUPPORTS`/`CONTRADICTS` edge written by `agent-utilities` is understood verbatim.
pub fn classify_relationship(relationship: &str) -> Option<EdgeKind> {
    match relationship.to_ascii_uppercase().as_str() {
        "SUPPORTS" | "SUPPORTS_BELIEF" | "HAS_EVIDENCE" | "CORROBORATES" => {
            Some(EdgeKind::Supports)
        }
        "CONTRADICTS" | "CONTRADICTS_BELIEF" | "REFUTES" => Some(EdgeKind::Contradicts),
        "ATTACKS" | "DEFEATS" | "UNDERCUTS" => Some(EdgeKind::Attacks),
        _ => None,
    }
}

/// Derive the `"epistemic:contested"`/`"epistemic:corroborated"`/`"epistemic:asserted"`
/// classification from a belief's own evidence-kind counts — the SAME rule
/// `contract.rs`'s `ModalityContract::policy_labels` uses for [`BeliefState`], extracted
/// here as a plain, unconditional function (no `contract` feature needed) so any caller
/// that already has these counts (an [`BeliefState`], an `eg_epistemic::query::EpistemicStatus`,
/// or a wire-projected result) can derive the same label without re-running belief
/// propagation. `contradicting`/`attacking` counts take priority (any contradiction or
/// attack makes a claim "contested" regardless of how much support it also has);
/// `supporting > 1` is "corroborated" (more than one independent supporter); otherwise
/// "asserted" (a bare, uncorroborated claim).
pub fn classify_policy_labels(
    supporting: usize,
    contradicting: usize,
    attacking: usize,
) -> Vec<String> {
    let label = if contradicting > 0 || attacking > 0 {
        "epistemic:contested"
    } else if supporting > 1 {
        "epistemic:corroborated"
    } else {
        "epistemic:asserted"
    };
    vec![label.to_string()]
}

/// Which time axis a [`BeliefState`] was pinned at — reuses the engine's bitemporal
/// distinction: `Valid` = "when it was true in the world", `Transaction` = "when the
/// engine believed it". A `BELIEF AS OF` query pins `Transaction`; `VALID AS OF` pins
/// `Valid` (both lower to the existing `Op::AsOf`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeAxis {
    Valid,
    Transaction,
}

/// The confidence-weighting policy applied to evidence during propagation. A pure
/// function of source reliability, staleness, and corroboration — configured per
/// tenant, never persisted on the graph.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuthorityPolicy {
    /// Global reliability multiplier applied to every piece of evidence `[0, 1]`.
    pub source_reliability: f64,
    /// Extra weight on an `ATTACKS` edge relative to a plain `CONTRADICTS` (defeaters
    /// hit harder than mere counter-evidence). `1.0` = same as a contradiction.
    pub attack_multiplier: f64,
    /// Pseudo-count strength of the prior when seeding the `Beta` belief from a node's
    /// own stored confidence. Higher = the stored confidence is harder for evidence to
    /// move; lower = evidence dominates quickly.
    pub prior_strength: f64,
}

impl Default for AuthorityPolicy {
    fn default() -> Self {
        Self {
            source_reliability: 1.0,
            attack_multiplier: 1.5,
            prior_strength: 2.0,
        }
    }
}

impl AuthorityPolicy {
    /// The Bernoulli mass a single edge of `kind` contributes, given the propagated
    /// belief of its source node. Supports become "successes", contradictions/attacks
    /// become "failures" fed to the conjugate update.
    pub fn edge_mass(&self, kind: EdgeKind, source_belief: f64) -> f64 {
        let base = self.source_reliability.clamp(0.0, 1.0) * source_belief.clamp(0.0, 1.0);
        match kind {
            EdgeKind::Supports | EdgeKind::Contradicts => base,
            EdgeKind::Attacks => base * self.attack_multiplier.max(0.0),
        }
    }
}

/// One node's stored bitemporal window (CONCEPT:EG-KG.epistemic.bitemporal-reasoning,
/// EPI-P3-5): mirrors `eg_types::NodeData`'s own `valid_from`/`valid_until`/`tx_from`/
/// `tx_to` fields (KG-2.249/2.250) — **valid time** ("when it's true in the world") and
/// **transaction time** ("when the engine recorded it") are independent axes, exactly
/// the distinction [`TimeAxis`] already names for a single-axis `AS OF` pin. This
/// struct is the two-axis PAIR, so a belief can be asked about on both simultaneously
/// (`BeliefGraph::at_instant`) rather than one axis at a time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BiTemporalRecord {
    /// When this fact became true in the world (valid time, inclusive lower bound).
    /// `None` mirrors the engine's own `live_at` convention (`eg-plan::exec::live_at`):
    /// absent ⇒ treated as having always existed (lower bound `0`).
    pub valid_from: Option<u64>,
    /// When this fact stopped being true in the world (valid time, exclusive upper
    /// bound). `None` ⇒ still valid / open-ended.
    pub valid_until: Option<u64>,
    /// When the engine recorded this fact (transaction time, inclusive lower bound).
    pub tx_from: Option<u64>,
    /// When the engine's record of this fact was superseded (transaction time,
    /// exclusive upper bound). `None` ⇒ still the current record.
    pub tx_to: Option<u64>,
}

impl BiTemporalRecord {
    /// Whether this record is live on the VALID axis at `ts` — the exact semantics
    /// `eg-plan::exec::live_at` uses for `Op::AsOf { axis: Valid }` (absent `from` ⇒ `0`,
    /// absent `until` ⇒ unbounded), kept in lockstep so a belief queried here agrees
    /// with the plan-level `AS OF` filter over the same stored fields.
    pub fn live_at_valid(&self, ts: u64) -> bool {
        self.valid_from.unwrap_or(0) <= ts && self.valid_until.is_none_or(|u| ts < u)
    }

    /// Whether this record is live on the TRANSACTION axis at `ts` — mirrors
    /// [`Self::live_at_valid`] over `tx_from`/`tx_to`.
    pub fn live_at_tx(&self, ts: u64) -> bool {
        self.tx_from.unwrap_or(0) <= ts && self.tx_to.is_none_or(|u| ts < u)
    }

    /// Whether this record is live on **both** axes at once — a `None` component means
    /// "don't filter that axis", so `live_at(Some(v), None)` degrades to a pure
    /// valid-time check and `live_at(None, None)` is vacuously `true` (no pin at all).
    pub fn live_at(&self, valid_ts: Option<u64>, tx_ts: Option<u64>) -> bool {
        valid_ts.is_none_or(|t| self.live_at_valid(t)) && tx_ts.is_none_or(|t| self.live_at_tx(t))
    }
}

/// A computed snapshot of what the engine believes about one node, given its evidence
/// neighbourhood. Derived, never stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BeliefState {
    pub node_id: String,
    /// The propagated posterior belief `[0, 1]` — distinct from the node's stored
    /// `NodeData.confidence` (which is the decay-carrying prior this computation seeds
    /// from). NEVER written back without an explicit materialize op.
    pub confidence: f64,
    /// Ids of nodes supporting this one (incoming `Supports` edges).
    pub supporting: Vec<String>,
    /// Ids of nodes contradicting this one (incoming `Contradicts` edges).
    pub contradicting: Vec<String>,
    /// Ids of nodes attacking this one (incoming `Attacks` edges).
    pub attacking: Vec<String>,
    /// The bitemporal instant this belief was pinned at, if the caller composed an
    /// `AS OF` before propagating; `None` for "as of now".
    pub as_of: Option<(TimeAxis, u64)>,
    /// A calibrated uncertainty interval around [`Self::confidence`] (EPI-P3-3) —
    /// `Some` whenever the posterior is a REAL Bayesian update (the node has at
    /// least one supporting/contradicting/attacking edge, so `confidence` is a
    /// Beta posterior mean, not a bare prior copy); `None` when there is no
    /// evidence to calibrate (an honest absence, not a placeholder null — the
    /// SAME "no fabricated signal" posture the `EG-P3-1` writeback-lineage
    /// `calibration: null` slot documents for a claim with no computed signal).
    pub calibration: Option<Calibration>,
    /// This claim's OWN stored bitemporal window (EPI-P3-5) — "when it's valid" +
    /// "when it was recorded", read straight off `BeliefGraph::temporal`. Distinct from
    /// `as_of` (which records a caller-composed `AS OF` PIN, i.e. the query instant);
    /// this is the claim's own data. `None` when the underlying graph carried no
    /// bitemporal metadata for this node (e.g. a hand-built test fixture).
    #[serde(default)]
    pub bitemporal: Option<BiTemporalRecord>,
}

/// A calibrated interval around a propagated belief (EPI-P3-3): the credible
/// interval of the SAME conjugate `Beta` posterior
/// [`crate::propagate::propagate_confidence`] derives its point `confidence`
/// from — i.e. this is not a second, invented uncertainty model, it is the
/// interval half of the exact distribution whose mean is already reported.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Calibration {
    /// Central credible interval `(lower, upper) ⊆ [0, 1]` at [`Self::level`].
    pub interval: (f64, f64),
    /// The probability mass the interval covers (e.g. `0.95`).
    pub level: f64,
    /// How many supporting/contradicting/attacking edges fed the posterior —
    /// the "source count" signal a reliability-weighted score would scale by
    /// (more corroborating/refuting edges ⇒ a narrower, better-calibrated
    /// interval, all else equal).
    pub evidence_count: usize,
}

/// The inference rule that produced a [`ProofNode`] — the epistemic analogue of an
/// OWL completion-rule name. Deliberately parallel to `eg_rdf::owl::ProofNode` (they
/// explain different graphs: OWL subsumption vs. epistemic support/attack).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JustRule {
    /// A directly asserted node (a leaf — its confidence is its stored prior).
    Asserted,
    /// Confidence raised by a supporting premise.
    DerivedSupport,
    /// Confidence lowered by a contradicting/attacking premise.
    DerivedContradiction,
    /// The conjugate Bayesian update that combined the premises into the posterior.
    BayesianUpdate,
}

/// One node in a justification proof tree: the claim, the rule that justified it, its
/// computed confidence, and the premises that fed it. Built by reconstruction from the
/// propagation walk — no re-derivation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProofNode {
    pub claim: String,
    pub rule: JustRule,
    pub confidence: f64,
    pub premises: Vec<ProofNode>,
}

/// A rooted justification graph — the answer to `EXPLAIN BELIEF <id>`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JustificationGraph {
    pub root: ProofNode,
}

#[cfg(test)]
mod bitemporal_record_tests {
    use super::*;

    #[test]
    fn absent_bounds_default_to_always_live() {
        let rec = BiTemporalRecord::default();
        assert!(rec.live_at_valid(0));
        assert!(rec.live_at_valid(u64::MAX));
        assert!(rec.live_at_tx(0));
        assert!(rec.live_at(Some(12345), Some(67890)));
    }

    #[test]
    fn valid_window_excludes_before_from_and_at_or_after_until() {
        let rec = BiTemporalRecord {
            valid_from: Some(100),
            valid_until: Some(200),
            tx_from: None,
            tx_to: None,
        };
        assert!(!rec.live_at_valid(99));
        assert!(rec.live_at_valid(100));
        assert!(rec.live_at_valid(199));
        assert!(!rec.live_at_valid(200));
    }

    #[test]
    fn live_at_none_axis_is_not_filtered() {
        let rec = BiTemporalRecord {
            valid_from: Some(100),
            valid_until: Some(200),
            tx_from: Some(1000),
            tx_to: Some(2000),
        };
        // Only the valid axis is checked when tx_ts is None.
        assert!(rec.live_at(Some(150), None));
        // Only the tx axis is checked when valid_ts is None.
        assert!(rec.live_at(None, Some(1500)));
        // Both axes must hold when both are given.
        assert!(!rec.live_at(Some(150), Some(2500)));
        assert!(rec.live_at(Some(150), Some(1500)));
    }
}
