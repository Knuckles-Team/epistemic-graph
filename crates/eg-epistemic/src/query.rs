//! CONCEPT:EG-KG.epistemic.bitemporal-reasoning — EPI-P3-5: the Phase-3 ACCEPTANCE
//! query surface. `eg-epistemic` already answers "how confident should we be in `id`"
//! ([`crate::propagate_confidence`]) and "what is the proof" ([`crate::explain_belief`]);
//! EPI-P3-2 ([`crate::recompute`]) added the live dependency-driven recompute/truth-
//! maintenance half. This module is the missing QUERYABLE layer Phase-3
//! acceptance criterion names verbatim: an agent must be able to answer, via ONE typed
//! query, "what do we believe, why, on exactly which evidence, under whose authority, at
//! what time, with what uncertainty, and WHAT WOULD INVALIDATE IT" — [`epistemic_status`]
//! is that one query; [`why`], [`why_not`], [`what_changed`], and
//! [`what_evidence_would_change_this`] are its four addressable facets, each also
//! callable standalone.
//!
//! Nothing here re-derives belief math: every function is a thin composition over
//! [`crate::propagate_confidence`]/[`crate::explain_belief`] (the always-on core) and
//! [`crate::tms`]'s grounded/preferred extensions + [`tms::remove_nodes`] (the
//! paraconsistent argumentation layer) — this module ADDS an acceptance criterion (what
//! counts as "believed"), a diagnostic (why NOT), a temporal diff, and a counterfactual
//! search, wired onto machinery that already exists.
//!
//! ## What counts as "believed"
//! [`is_believed`] is a DUAL criterion, deliberately combining both halves of the crate:
//! skeptical (grounded) acceptance from the qualitative argumentation layer (`tms`,
//! X2) — "cannot be rationally denied" — AND the propagated Bayesian confidence meeting
//! [`BELIEF_THRESHOLD`] — "the evidence actually supports it, quantitatively". A claim
//! with zero attackers is trivially grounded-accepted regardless of how little (or how
//! weak) its supporting evidence is (Dung's `F(S)` is vacuously satisfied over an empty
//! attacker set — see `tms::grounded_labels`), so the confidence half is what lets
//! [`why_not`] answer "not enough evidence" for an unattacked-but-unsupported claim,
//! which the argumentation layer alone can never say.
//!
//! ## Bitemporal
//! Every function here takes a [`crate::BeliefGraph`] that the CALLER may have already
//! narrowed via [`crate::BeliefGraph::at_instant`] (EPI-P3-5's bitemporal-graph
//! addition) — so "what did we believe about `claim` as of transaction-time T, valid as
//! of time V" is simply `epistemic_status(&bg.at_instant(Some(v), Some(t)), claim,
//! policy)`. [`what_changed`] is the one function that does its OWN bitemporal
//! narrowing internally (it needs two graphs, one per instant, to diff).

use std::collections::{BTreeSet, HashSet};

use eg_types::Distribution;

use crate::adapter::BeliefGraph;
use crate::model::{AuthorityPolicy, BiTemporalRecord, JustificationGraph, ProofNode};
use crate::propagate::{belief_distribution, explain_belief, propagate_confidence};
use crate::tms::{self, remove_nodes};

/// The minimum propagated posterior confidence a claim needs — ON TOP OF grounded
/// (skeptical) argumentation acceptance — to count as genuinely [`is_believed`]. `0.5`
/// is the natural "more likely true than not" cut; a claim sitting exactly at the
/// uninformative default prior ([`crate::propagate::DEFAULT_PRIOR`] via
/// [`AuthorityPolicy`]'s midpoint) is deliberately NOT believed by default — maximal
/// ignorance is not belief.
pub const BELIEF_THRESHOLD: f64 = 0.5;

/// Above this many DIRECT evidence ids, [`what_evidence_would_change_this`] skips
/// searching pairs/triples (still tries every SINGLE id) and returns whatever singleton
/// flip it found, logging instead of paying combinatorial cost — mirrors
/// [`tms::MAX_PREFERRED_ARGUMENTS`]'s "bound, then log, never silently truncate" rule.
pub const MAX_FLIP_CANDIDATES: usize = 20;

/// The dual acceptance criterion this module standardizes on (see module docs):
/// skeptically (grounded) accepted per the paraconsistent argumentation layer AND
/// propagated confidence at/above [`BELIEF_THRESHOLD`].
pub fn is_believed(bg: &BeliefGraph, claim: &str, policy: &AuthorityPolicy) -> bool {
    tms::is_skeptically_accepted(bg, claim)
        && propagate_confidence(bg, claim, policy).confidence >= BELIEF_THRESHOLD
}

/// **why**(claim) — the proof. A named alias over [`crate::explain_belief`] (reused
/// verbatim, never re-derived) so the four EPI-P3-5 ops read as one symmetric family:
/// `why` / [`why_not`] / [`what_changed`] / [`what_evidence_would_change_this`].
pub fn why(bg: &BeliefGraph, claim: &str, policy: &AuthorityPolicy) -> JustificationGraph {
    explain_belief(bg, claim, policy)
}

/// The reason a claim is NOT [`is_believed`], plus the id(s) responsible.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WhyNotReason {
    /// `claim` names no node this graph has ever heard of — no prior, no edge in or
    /// out. There is nothing to believe because there is nothing asserted.
    Unknown,
    /// Grounded-accepted (nothing rationally defeats it) but the propagated confidence
    /// still falls below [`BELIEF_THRESHOLD`] — i.e. genuinely too little (or too weak)
    /// supporting evidence, the case the argumentation layer alone cannot express (see
    /// module docs).
    InsufficientConfidence,
    /// Defeated outright: at least one of `claim`'s (bipolar-augmented) attackers is
    /// ITSELF grounded-accepted, so `claim` is provably OUT, not merely undecided.
    Contradicted { blockers: Vec<String> },
    /// Caught in an unresolved (even-length or otherwise self-reinstating) attack
    /// cycle, or a direct symmetric conflict with no other evidence either way — the
    /// paraconsistent "neither in nor out" outcome ([`tms`] module docs). `competing`
    /// lists the attacker(s) equally stuck UNDECIDED, i.e. what `claim` is contending
    /// with, not a single decisive blocker.
    Undecided { competing: Vec<String> },
}

/// The full **why-not** answer for one claim.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WhyNot {
    pub claim: String,
    pub reason: WhyNotReason,
    /// The propagated confidence at the time of the query, for transparency even when
    /// the reason is purely argumentation-level (`Contradicted`/`Undecided`).
    pub confidence: f64,
}

/// **why_not**(claim) — why is `claim` NOT believed (per [`is_believed`])? Returns
/// `None` when `claim` IS believed (why-not does not apply) — mirrors [`crate::tms`]'s
/// "a query, not an error" convention for a no-op change event.
pub fn why_not(bg: &BeliefGraph, claim: &str, policy: &AuthorityPolicy) -> Option<WhyNot> {
    if is_believed(bg, claim, policy) {
        return None;
    }
    let confidence = propagate_confidence(bg, claim, policy).confidence;

    if !tms::arguments(bg).contains(claim) {
        return Some(WhyNot {
            claim: claim.to_string(),
            reason: WhyNotReason::Unknown,
            confidence,
        });
    }

    let labels = tms::grounded_extension(bg);
    if !labels.contains(claim) {
        // Genuinely NOT grounded-accepted: either provably defeated (some attacker is
        // itself grounded-IN) or stuck UNDECIDED (no attacker is grounded-IN either, so
        // neither side of the conflict can be rationally preferred — a cycle or
        // symmetric contradiction; see `tms` module docs on paraconsistency).
        let attackers = tms::augmented_attackers(bg, claim);
        let blockers: Vec<String> = attackers
            .iter()
            .filter(|a| labels.contains(*a))
            .cloned()
            .collect();
        return Some(WhyNot {
            claim: claim.to_string(),
            reason: if !blockers.is_empty() {
                WhyNotReason::Contradicted { blockers }
            } else {
                WhyNotReason::Undecided {
                    competing: attackers.into_iter().collect(),
                }
            },
            confidence,
        });
    }

    // `claim` IS grounded-accepted (nothing defeats it, possibly vacuously — zero
    // attackers is trivially "all attackers OUT"), so the ONLY way `is_believed` still
    // failed above is the quantitative confidence gate: too little (or too weak)
    // supporting evidence, the case argumentation semantics alone cannot express.
    Some(WhyNot {
        claim: claim.to_string(),
        reason: WhyNotReason::InsufficientConfidence,
        confidence,
    })
}

/// One claim whose believed-status and/or confidence differ between two transaction
/// times, plus a rendered reason.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChangedBelief {
    pub id: String,
    pub believed_before: bool,
    pub believed_after: bool,
    pub confidence_before: f64,
    pub confidence_after: f64,
    /// Evidence ids present in the `tx_to` snapshot's incoming evidence but not
    /// `tx_from`'s — what showed up.
    pub evidence_added: Vec<String>,
    /// Evidence ids present in `tx_from` but not `tx_to` — what disappeared (retracted,
    /// or simply not yet recorded at `tx_from`).
    pub evidence_removed: Vec<String>,
    /// A short human-readable summary of the above, for direct display.
    pub reason: String,
}

/// **what_changed**(tx_from, tx_to) — between two transaction times, which beliefs
/// changed and why? Narrows `bg` to each instant via [`BeliefGraph::at_instant`]
/// (transaction axis only — valid time is left unfiltered, i.e. "everything valid as of
/// now, as the engine believed it at each of the two transaction times"), unions the
/// claim ids live in EITHER snapshot, and reports every one whose believed-status or
/// confidence differs.
pub fn what_changed(
    bg: &BeliefGraph,
    tx_from: u64,
    tx_to: u64,
    policy: &AuthorityPolicy,
) -> Vec<ChangedBelief> {
    let before = bg.at_instant(None, Some(tx_from));
    let after = bg.at_instant(None, Some(tx_to));

    let before_ids = tms::arguments(&before);
    let after_ids = tms::arguments(&after);
    let all_ids: BTreeSet<&String> = before_ids.union(&after_ids).collect();

    let mut changed = Vec::new();
    for id in all_ids {
        let in_before = before_ids.contains(id);
        let in_after = after_ids.contains(id);

        let (believed_before, confidence_before, supports_before) = if in_before {
            let bs = propagate_confidence(&before, id, policy);
            (
                is_believed(&before, id, policy),
                bs.confidence,
                bs.supporting,
            )
        } else {
            (false, 0.0, Vec::new())
        };
        let (believed_after, confidence_after, supports_after) = if in_after {
            let bs = propagate_confidence(&after, id, policy);
            (
                is_believed(&after, id, policy),
                bs.confidence,
                bs.supporting,
            )
        } else {
            (false, 0.0, Vec::new())
        };

        let confidence_delta = (confidence_after - confidence_before).abs();
        if believed_before == believed_after && confidence_delta < 1e-9 && in_before == in_after {
            continue;
        }

        let before_set: BTreeSet<&String> = supports_before.iter().collect();
        let after_set: BTreeSet<&String> = supports_after.iter().collect();
        let evidence_added: Vec<String> = after_set
            .difference(&before_set)
            .map(|s| s.to_string())
            .collect();
        let evidence_removed: Vec<String> = before_set
            .difference(&after_set)
            .map(|s| s.to_string())
            .collect();

        let reason = if !in_before && in_after {
            format!("{id}: first recorded between tx {tx_from} and {tx_to}")
        } else if in_before && !in_after {
            format!("{id}: no longer recorded as of tx {tx_to} (was live at tx {tx_from})")
        } else if believed_before != believed_after {
            format!(
                "{id}: belief flipped {believed_before} -> {believed_after} (confidence {confidence_before:.3} -> {confidence_after:.3})"
            )
        } else {
            format!(
                "{id}: confidence changed {confidence_before:.3} -> {confidence_after:.3} (belief unchanged: {believed_after})"
            )
        };

        changed.push(ChangedBelief {
            id: id.clone(),
            believed_before,
            believed_after,
            confidence_before,
            confidence_after,
            evidence_added,
            evidence_removed,
            reason,
        });
    }
    changed
}

/// The minimal evidence set whose retraction flips a claim's [`is_believed`] status —
/// the "counterfactual evidence" the Phase-3 acceptance query names
/// "what-would-invalidate-this".
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MinimalFlipSet {
    pub claim: String,
    pub believed_now: bool,
    /// The smallest evidence id set found whose combined retraction flips
    /// `claim`'s [`is_believed`] status (grounded acceptance; see module docs — the
    /// qualitative half of the dual criterion, which is what dependency-directed
    /// retraction reasons about).
    pub evidence_ids: BTreeSet<String>,
    /// What `claim`'s grounded status becomes once `evidence_ids` is withdrawn.
    pub believed_after: bool,
}

/// Flatten a [`ProofNode`] tree (pre-order, deduplicated) into the ids of every
/// premise it rests on, EXCLUDING the root itself — the justification-graph-derived
/// candidate pool [`what_evidence_would_change_this`] searches (both the direct
/// evidence AND whatever those in turn rest on, e.g. an attacker's own defeater —
/// exactly the depth `tms::retract`'s dependency-directed cascade reaches).
fn flatten_premises(node: &ProofNode, seen: &mut HashSet<String>, out: &mut Vec<String>) {
    for premise in &node.premises {
        if seen.insert(premise.claim.clone()) {
            out.push(premise.claim.clone());
        }
        flatten_premises(premise, seen, out);
    }
}

/// **what_evidence_would_change_this**(claim) — search `claim`'s justification graph
/// (every id [`why`]'s proof tree rests on — direct evidence AND whatever THOSE rest
/// on, e.g. an attacker's own defeater; see [`flatten_premises`]) for the smallest
/// subset whose combined removal ([`tms::remove_nodes`]) flips `claim`'s grounded
/// acceptance ([`tms::grounded_extension`]) — the same dependency-directed-retraction
/// idea [`tms::retract`] runs for a single id, generalized to a SEARCH over candidate
/// sets. Tries every single id first (cheapest), then pairs, up to
/// `min(candidates.len(), 3)` — bounded exactly like [`tms::preferred_extensions`] is
/// bounded, since combination search is combinatorial. Returns `None` if no flip is
/// found within that bound (not "no evidence exists" — the search is intentionally
/// shallow; see [`MAX_FLIP_CANDIDATES`]).
pub fn what_evidence_would_change_this(
    bg: &BeliefGraph,
    claim: &str,
    policy: &AuthorityPolicy,
) -> Option<MinimalFlipSet> {
    let believed_now = tms::is_skeptically_accepted(bg, claim);

    let proof = why(bg, claim, policy);
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    flatten_premises(&proof.root, &mut seen, &mut candidates);

    if candidates.len() > MAX_FLIP_CANDIDATES {
        tracing::warn!(
            claim,
            candidates = candidates.len(),
            bound = MAX_FLIP_CANDIDATES,
            "eg-epistemic query: what_evidence_would_change_this truncating candidate \
             evidence set ({} > {MAX_FLIP_CANDIDATES}); result may miss a larger minimal \
             flip set",
            candidates.len()
        );
        candidates.truncate(MAX_FLIP_CANDIDATES);
    }

    let max_k = candidates.len().min(3);
    for k in 1..=max_k {
        if let Some(found) = search_flip_at_size(bg, claim, &candidates, k, believed_now) {
            return Some(found);
        }
    }
    None
}

/// Try every size-`k` combination of `candidates`, returning the first whose combined
/// retraction flips `claim`'s grounded status away from `believed_now`. Simple
/// recursive combination enumeration — `k` is capped at 3 by the caller, so this never
/// runs away.
fn search_flip_at_size(
    bg: &BeliefGraph,
    claim: &str,
    candidates: &[String],
    k: usize,
    believed_now: bool,
) -> Option<MinimalFlipSet> {
    fn combinations<'a>(
        items: &'a [String],
        k: usize,
        start: usize,
        current: &mut Vec<&'a str>,
        out: &mut Vec<Vec<&'a str>>,
    ) {
        if current.len() == k {
            out.push(current.clone());
            return;
        }
        for i in start..items.len() {
            current.push(items[i].as_str());
            combinations(items, k, i + 1, current, out);
            current.pop();
        }
    }

    let mut combos = Vec::new();
    combinations(candidates, k, 0, &mut Vec::new(), &mut combos);

    for combo in combos {
        let removed = remove_nodes(bg, combo.iter().copied());
        let believed_after = tms::is_skeptically_accepted(&removed, claim);
        if believed_after != believed_now {
            return Some(MinimalFlipSet {
                claim: claim.to_string(),
                believed_now,
                evidence_ids: combo.iter().map(|s| s.to_string()).collect(),
                believed_after,
            });
        }
    }
    None
}

/// The Phase-3 ACCEPTANCE query (EPI-P3-5): everything an agent needs to answer, for
/// one claim, "what do we believe, why, on exactly which evidence, under whose
/// authority, at what time, with what uncertainty, and what would invalidate it" — in
/// ONE typed call. Every facet is computed by an existing, reused function; this struct
/// is purely the composition.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EpistemicStatus {
    pub claim: String,
    /// "what do we believe" — [`is_believed`] (grounded acceptance AND confidence
    /// above [`BELIEF_THRESHOLD`]).
    pub believed: bool,
    pub confidence: f64,
    /// "with what uncertainty" — the posterior `Beta`'s variance
    /// ([`crate::belief_distribution`]); `0.0` for a degenerate/leaf claim with no
    /// competing evidence at all is a legitimate (low-uncertainty-because-unquestioned)
    /// answer, not a missing value.
    pub uncertainty: f64,
    /// "why" — the proof tree.
    pub proof: JustificationGraph,
    /// Populated iff `!believed` — "why NOT", the diagnostic.
    pub why_not: Option<WhyNot>,
    /// "on exactly which evidence" — the supporting ids (`BeliefState::supporting`).
    pub evidence: Vec<String>,
    pub contradicting: Vec<String>,
    pub attacking: Vec<String>,
    /// "under whose authority" — the [`AuthorityPolicy`] this whole status was computed
    /// under (source reliability / attack multiplier / prior strength). Not a
    /// fabricated per-source "authority" field: `BeliefGraph` is deliberately type-
    /// agnostic over claim/evidence/source (see crate module docs), so the one real,
    /// non-fabricated "authority" concept available at this layer is the policy that
    /// weighted every edge above.
    pub authority: AuthorityPolicy,
    /// "at what time" — valid axis: this claim's own stored `(valid_from, valid_until)`.
    pub valid_time: Option<(Option<u64>, Option<u64>)>,
    /// "at what time" — transaction axis: this claim's own stored `(tx_from, tx_to)`.
    pub tx_time: Option<(Option<u64>, Option<u64>)>,
    /// "what would invalidate it" — the minimal counterfactual evidence set.
    pub what_would_invalidate: Option<MinimalFlipSet>,
}

/// Build the unified [`EpistemicStatus`] for `claim` over `bg` under `policy`. If the
/// caller wants a specific bitemporal instant, narrow `bg` with
/// [`BeliefGraph::at_instant`] first — this function reasons purely over whatever graph
/// it is given.
pub fn epistemic_status(
    bg: &BeliefGraph,
    claim: &str,
    policy: &AuthorityPolicy,
) -> EpistemicStatus {
    let belief = propagate_confidence(bg, claim, policy);
    let believed = is_believed(bg, claim, policy);
    let proof = why(bg, claim, policy);
    let why_not = if believed {
        None
    } else {
        why_not(bg, claim, policy)
    };
    let uncertainty = distribution_variance(&belief_distribution(bg, claim, policy));
    let what_would_invalidate = what_evidence_would_change_this(bg, claim, policy);

    let (valid_time, tx_time) = match bg.temporal.get(claim) {
        Some(BiTemporalRecord {
            valid_from,
            valid_until,
            tx_from,
            tx_to,
        }) => (Some((*valid_from, *valid_until)), Some((*tx_from, *tx_to))),
        None => (None, None),
    };

    EpistemicStatus {
        claim: claim.to_string(),
        believed,
        confidence: belief.confidence,
        uncertainty,
        proof,
        why_not,
        evidence: belief.supporting,
        contradicting: belief.contradicting,
        attacking: belief.attacking,
        authority: *policy,
        valid_time,
        tx_time,
        what_would_invalidate,
    }
}

fn distribution_variance(d: &Distribution) -> f64 {
    d.variance()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EdgeKind;

    // --- why -----------------------------------------------------------------------

    #[test]
    fn why_returns_the_proof_tree() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.5), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        );
        let policy = AuthorityPolicy::default();
        let proof = why(&bg, "claim", &policy);
        assert_eq!(proof.root.claim, "claim");
        assert_eq!(proof.root.premises.len(), 1);
        assert_eq!(proof.root.premises[0].claim, "evidence");
    }

    // --- why_not ---------------------------------------------------------------------

    #[test]
    fn why_not_is_none_for_a_believed_claim() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.9), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        );
        let policy = AuthorityPolicy::default();
        assert!(is_believed(&bg, "claim", &policy));
        assert_eq!(why_not(&bg, "claim", &policy), None);
    }

    #[test]
    fn why_not_unknown_for_a_claim_absent_from_the_graph() {
        let bg = BeliefGraph::from_parts([("other", 0.9)], Vec::<(&str, &str, EdgeKind)>::new());
        let policy = AuthorityPolicy::default();
        let wn = why_not(&bg, "nonexistent", &policy).expect("must not be believed");
        assert_eq!(wn.reason, WhyNotReason::Unknown);
    }

    #[test]
    fn why_not_insufficient_confidence_for_an_unattacked_weak_prior() {
        // No attackers at all ⇒ vacuously grounded-accepted; the only way it's
        // unbelieved is the confidence gate (default prior 0.3 < BELIEF_THRESHOLD).
        let bg = BeliefGraph::from_parts([("claim", 0.3)], Vec::<(&str, &str, EdgeKind)>::new());
        let policy = AuthorityPolicy::default();
        assert!(tms::is_skeptically_accepted(&bg, "claim"));
        let wn = why_not(&bg, "claim", &policy).expect("must not be believed");
        assert_eq!(wn.reason, WhyNotReason::InsufficientConfidence);
    }

    #[test]
    fn why_not_contradicted_names_the_blocking_attacker() {
        // `attacker` is unattacked (grounded-IN) and attacks `claim` ⇒ `claim` is
        // grounded-OUT: a decisive contradiction, not merely undecided.
        let bg = BeliefGraph::from_parts(
            [("claim", 0.9), ("attacker", 0.9)],
            [("attacker", "claim", EdgeKind::Attacks)],
        );
        let policy = AuthorityPolicy::default();
        assert!(!tms::is_skeptically_accepted(&bg, "claim"));
        let wn = why_not(&bg, "claim", &policy).expect("must not be believed");
        match wn.reason {
            WhyNotReason::Contradicted { blockers } => {
                assert_eq!(blockers, vec!["attacker".to_string()]);
            }
            other => panic!("expected Contradicted, got {other:?}"),
        }
    }

    #[test]
    fn why_not_undecided_for_a_symmetric_conflict() {
        // p <-> not_p, symmetric, no other evidence: both stay UNDECIDED (paraconsistent).
        let bg = BeliefGraph::from_parts(
            [("p", 0.6), ("not_p", 0.6)],
            [
                ("not_p", "p", EdgeKind::Contradicts),
                ("p", "not_p", EdgeKind::Contradicts),
            ],
        );
        let policy = AuthorityPolicy::default();
        assert!(!tms::is_skeptically_accepted(&bg, "p"));
        let wn = why_not(&bg, "p", &policy).expect("must not be believed");
        match wn.reason {
            WhyNotReason::Undecided { competing } => {
                assert_eq!(competing, vec!["not_p".to_string()]);
            }
            other => panic!("expected Undecided, got {other:?}"),
        }
    }

    // --- what_changed ------------------------------------------------------------------

    #[test]
    fn what_changed_reports_a_belief_that_flips_between_two_tx_times() {
        // `attacker` is recorded (tx_from=100) AFTER `claim` (tx_from=0): as of tx=50,
        // only claim exists and is believed; as of tx=200, attacker also exists and
        // defeats claim.
        let bg = BeliefGraph::from_parts(
            [("claim", 0.9), ("attacker", 0.9)],
            [("attacker", "claim", EdgeKind::Attacks)],
        )
        .with_temporal(
            "attacker",
            BiTemporalRecord {
                valid_from: Some(100),
                valid_until: None,
                tx_from: Some(100),
                tx_to: None,
            },
        );
        let policy = AuthorityPolicy::default();

        let changed = what_changed(&bg, 50, 200, &policy);
        let claim_change = changed
            .iter()
            .find(|c| c.id == "claim")
            .expect("claim's belief must be reported as changed");
        assert!(claim_change.believed_before);
        assert!(!claim_change.believed_after);
        assert!(claim_change.reason.contains("flipped"));

        // `attacker` itself newly appears between the two instants.
        let attacker_change = changed
            .iter()
            .find(|c| c.id == "attacker")
            .expect("attacker's appearance must be reported");
        assert!(!attacker_change.believed_before || attacker_change.confidence_before == 0.0);
        assert!(attacker_change.reason.contains("first recorded"));
    }

    #[test]
    fn what_changed_is_empty_when_nothing_changed() {
        let bg = BeliefGraph::from_parts([("solo", 0.9)], Vec::<(&str, &str, EdgeKind)>::new());
        let policy = AuthorityPolicy::default();
        assert_eq!(what_changed(&bg, 10, 20, &policy), Vec::new());
    }

    // --- what_evidence_would_change_this -----------------------------------------------

    #[test]
    fn what_evidence_would_change_this_finds_the_minimal_flipping_evidence() {
        // ev attacks x, x attacks c (mirrors tms::tests::retract_cascades_through_justification):
        // c is grounded-accepted only because ev defeats x. Withdrawing ev alone flips c.
        let bg = BeliefGraph::from_parts(
            [("ev", 0.9), ("x", 0.5), ("c", 0.5)],
            [
                ("ev", "x", EdgeKind::Attacks),
                ("x", "c", EdgeKind::Attacks),
            ],
        );
        let policy = AuthorityPolicy::default();
        assert!(tms::is_skeptically_accepted(&bg, "c"));

        let flip = what_evidence_would_change_this(&bg, "c", &policy)
            .expect("a minimal flipping set must exist");
        assert!(flip.believed_now);
        assert!(!flip.believed_after);
        // Removing `x` alone does NOT flip `c` (x's attack was already neutralized by
        // ev, so c stays grounded-IN vacuously with x gone too) — the minimal flip is
        // one level DEEPER: withdrawing `ev` itself reinstates x, which then defeats c.
        // This is exactly `tms::retract`'s cascade (see
        // `tms::tests::retract_cascades_through_justification`), reached here via
        // search over the justification-graph candidate pool rather than a single
        // named id.
        assert_eq!(flip.evidence_ids, ["ev".to_string()].into_iter().collect());
    }

    #[test]
    fn what_evidence_would_change_this_none_for_an_unattacked_claim() {
        let bg = BeliefGraph::from_parts([("solo", 0.9)], Vec::<(&str, &str, EdgeKind)>::new());
        let policy = AuthorityPolicy::default();
        assert_eq!(what_evidence_would_change_this(&bg, "solo", &policy), None);
    }

    // --- epistemic_status (the unified acceptance query) --------------------------------

    #[test]
    fn epistemic_status_returns_every_facet_for_a_believed_claim() {
        let bg = BeliefGraph::from_parts(
            [("claim", 0.9), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        )
        .with_temporal(
            "claim",
            BiTemporalRecord {
                valid_from: Some(10),
                valid_until: None,
                tx_from: Some(20),
                tx_to: None,
            },
        );
        let policy = AuthorityPolicy::default();
        let status = epistemic_status(&bg, "claim", &policy);

        assert!(status.believed);
        assert!(status.confidence > 0.5);
        assert_eq!(status.proof.root.claim, "claim");
        assert_eq!(status.why_not, None);
        assert_eq!(status.evidence, vec!["evidence".to_string()]);
        assert_eq!(status.authority, policy);
        assert_eq!(status.valid_time, Some((Some(10), None)));
        assert_eq!(status.tx_time, Some((Some(20), None)));
        assert!(status.uncertainty >= 0.0);
        // `evidence` is unattacked, so no counterfactual flip within the search bound.
        assert_eq!(status.what_would_invalidate, None);
    }

    #[test]
    fn epistemic_status_populates_why_not_and_invalidation_for_an_unbelieved_claim() {
        let bg = BeliefGraph::from_parts(
            [("ev", 0.9), ("x", 0.5), ("c", 0.5)],
            [
                ("ev", "x", EdgeKind::Attacks),
                ("x", "c", EdgeKind::Attacks),
            ],
        );
        let policy = AuthorityPolicy::default();

        // `c` IS grounded-accepted (ev defeats its only attacker x), but the propagated
        // Bayesian confidence still nets below BELIEF_THRESHOLD once x's own (weakened
        // but nonzero) belief is folded in — so the DUAL criterion `is_believed` says
        // not believed, and the reason is the quantitative one, not an argumentation
        // defeat.
        let status = epistemic_status(&bg, "c", &policy);
        assert!(!status.believed);
        assert!(tms::is_skeptically_accepted(&bg, "c"));
        assert!(status.confidence < BELIEF_THRESHOLD);
        assert_eq!(
            status.why_not,
            Some(WhyNot {
                claim: "c".to_string(),
                reason: WhyNotReason::InsufficientConfidence,
                confidence: status.confidence,
            })
        );
        // What would invalidate `c`'s (argumentation-level) grounded acceptance:
        // withdrawing `ev` reinstates `x`, which then defeats `c`.
        let invalidate = status
            .what_would_invalidate
            .expect("a minimal flipping set must exist for c");
        assert_eq!(
            invalidate.evidence_ids,
            ["ev".to_string()].into_iter().collect()
        );

        // `x` is a clean argumentation-level defeat: defeated outright by `ev`.
        let status_x = epistemic_status(&bg, "x", &policy);
        assert!(!status_x.believed);
        match status_x.why_not.expect("x must have a why_not").reason {
            WhyNotReason::Contradicted { blockers } => {
                assert_eq!(blockers, vec!["ev".to_string()]);
            }
            other => panic!("expected Contradicted, got {other:?}"),
        }
    }
}
