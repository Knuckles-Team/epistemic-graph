//! Defect triage as a decision (EG-SDLC-R004): the record the engine decides
//! for one defect, and the pure ranking that feeds it.
//!
//! The decision names severity, suspected symbols, an owner and a duplicate
//! link, or abstains with a typed reason. Nothing here reads a clock or a float;
//! equal inputs produce equal records.

use serde::{Deserialize, Serialize};

use crate::lifecycle::Severity;

/// Why a triage abstained.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriageAbstainReason {
    NoSymbolMatch,
    NoOwner,
    InsufficientEvidence,
}

/// The triage of one defect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriageDecision {
    pub defect_id: String,
    pub severity: Option<Severity>,
    pub suspected_symbols: Vec<String>,
    pub owner_principal: Option<String>,
    pub duplicate_of: Option<String>,
    pub abstain_reason: Option<TriageAbstainReason>,
    pub evidence_refs: Vec<String>,
}

/// A code symbol named by a symptom frame, with its blame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuspectInput {
    pub symbol_id: String,
    /// The changeset that last touched the symbol.
    pub blame_changeset: String,
    /// Blame recency: the time of that changeset.
    pub blame_at: u64,
}

/// Rank suspects: symbols last changed by the bisect culprit first, then the
/// most recently changed, then by symbol id so equal inputs rank equally.
pub fn rank_suspects(inputs: &[SuspectInput], culprit: Option<&str>) -> Vec<String> {
    let mut ranked: Vec<&SuspectInput> = inputs.iter().collect();
    ranked.sort_by(|a, b| {
        let culprit_a = culprit == Some(a.blame_changeset.as_str());
        let culprit_b = culprit == Some(b.blame_changeset.as_str());
        culprit_b
            .cmp(&culprit_a)
            .then(b.blame_at.cmp(&a.blame_at))
            .then_with(|| a.symbol_id.cmp(&b.symbol_id))
    });
    let mut seen = Vec::new();
    for suspect in ranked {
        if !seen.contains(&suspect.symbol_id) {
            seen.push(suspect.symbol_id.clone());
        }
    }
    seen
}

/// Pick the owner from path ownership records first, then recent authors.
pub fn pick_owner(path_owners: &[String], recent_authors: &[String]) -> Option<String> {
    path_owners
        .first()
        .or_else(|| recent_authors.first())
        .cloned()
}

/// Decide a triage from ranked evidence. It abstains, with the typed reason,
/// when the evidence cannot support a decision.
pub fn decide_triage(
    defect_id: &str,
    severity: Severity,
    suspects: Vec<String>,
    owner: Option<String>,
    duplicate_of: Option<String>,
    evidence_refs: Vec<String>,
) -> TriageDecision {
    let abstain = if evidence_refs.is_empty() {
        Some(TriageAbstainReason::InsufficientEvidence)
    } else if suspects.is_empty() {
        Some(TriageAbstainReason::NoSymbolMatch)
    } else if owner.is_none() {
        Some(TriageAbstainReason::NoOwner)
    } else {
        None
    };
    TriageDecision {
        defect_id: defect_id.to_owned(),
        severity: abstain.is_none().then_some(severity),
        suspected_symbols: suspects,
        owner_principal: owner,
        duplicate_of,
        abstain_reason: abstain,
        evidence_refs,
    }
}

impl TriageDecision {
    /// The `affects` edges (defect to symbol) this decision sets. The
    /// `introducedBy` edge comes from the `introduced.bisect` derivation rule.
    pub fn affects_edges(&self) -> Vec<(String, String)> {
        self.suspected_symbols
            .iter()
            .map(|symbol| (self.defect_id.clone(), symbol.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn suspect(symbol: &str, changeset: &str, at: u64) -> SuspectInput {
        SuspectInput {
            symbol_id: symbol.into(),
            blame_changeset: changeset.into(),
            blame_at: at,
        }
    }

    // spec: EG-SDLC-R004.1
    #[test]
    fn decision_round_trips() {
        let decision = decide_triage(
            "d1",
            Severity::High,
            vec!["sym::a".into()],
            Some("alice".into()),
            Some("d0".into()),
            vec!["gv-1".into()],
        );
        let json = serde_json::to_string(&decision).unwrap();
        assert_eq!(
            serde_json::from_str::<TriageDecision>(&json).unwrap(),
            decision
        );

        let abstained = decide_triage("d2", Severity::Low, vec![], None, None, vec!["x".into()]);
        assert_eq!(
            abstained.abstain_reason,
            Some(TriageAbstainReason::NoSymbolMatch)
        );
        assert_eq!(abstained.severity, None);
        let json = serde_json::to_string(&abstained).unwrap();
        assert!(json.contains("no_symbol_match"));
        assert!(
            serde_json::from_str::<TriageDecision>(&json.replace("no_symbol_match", "shrug"))
                .is_err()
        );

        let ownerless = decide_triage(
            "d3",
            Severity::Low,
            vec!["s".into()],
            None,
            None,
            vec!["x".into()],
        );
        assert_eq!(ownerless.abstain_reason, Some(TriageAbstainReason::NoOwner));
        let thin = decide_triage(
            "d4",
            Severity::Low,
            vec!["s".into()],
            Some("o".into()),
            None,
            vec![],
        );
        assert_eq!(
            thin.abstain_reason,
            Some(TriageAbstainReason::InsufficientEvidence)
        );
    }

    // spec: EG-SDLC-R004.2
    #[test]
    fn culprit_outranks_blame() {
        let inputs = [
            suspect("recent", "c9", 900),
            suspect("older", "c5", 500),
            suspect("culprit_sym", "c2", 200),
            suspect("recent", "c9", 900),
        ];
        assert_eq!(
            rank_suspects(&inputs, Some("c2")),
            ["culprit_sym", "recent", "older"]
        );
        assert_eq!(
            rank_suspects(&inputs, None),
            ["recent", "older", "culprit_sym"]
        );
        assert_eq!(
            rank_suspects(&inputs, Some("c2")),
            rank_suspects(&inputs, Some("c2"))
        );

        let owners = ["team".to_string()];
        let authors = ["bob".to_string()];
        assert_eq!(pick_owner(&owners, &authors).as_deref(), Some("team"));
        assert_eq!(pick_owner(&[], &authors).as_deref(), Some("bob"));
        assert_eq!(pick_owner(&[], &[]), None);

        let decision = decide_triage(
            "d1",
            Severity::Medium,
            rank_suspects(&inputs, Some("c2")),
            pick_owner(&owners, &authors),
            None,
            vec!["culprit:c2".into()],
        );
        assert_eq!(
            decision.affects_edges()[0],
            ("d1".to_string(), "culprit_sym".to_string())
        );
    }
}
