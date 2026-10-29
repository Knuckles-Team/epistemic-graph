//! Cross-application entity resolution over [`EntityLinkProposal`]
//! (EG-UNIFIED-DATA-PLANE-R030.2, R030.3, R030.700).
//!
//! * Blocking groups records by a normalized-name key so only records in
//!   one block are compared, and field similarity scores each pair.
//! * Fellegi-Sunter scoring sums per-field log2 likelihood weights from
//!   declared `m`/`u` probabilities and classifies the pair by two
//!   thresholds. Every result is an unapproved proposal; only
//!   [`approve`] records an approver.
//! * Identity evidence carries its kind. A high-entropy token never merges
//!   on name alone, and a version variant links without merging.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::entity_link_proposal::{EntityLinkProposal, EntityRef, MatchClass};

/// One record offered for resolution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionRecord {
    pub entity: EntityRef,
    pub name: String,
    pub fields: BTreeMap<String, String>,
}

/// Lowercase, keep alphanumerics, collapse whitespace.
pub fn normalize_name(name: &str) -> String {
    name.split_whitespace()
        .map(|word| {
            word.chars()
                .filter(|c| c.is_alphanumeric())
                .flat_map(char::to_lowercase)
                .collect::<String>()
        })
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The blocking key: the sorted normalized name tokens' first three
/// characters each.
pub fn blocking_key(name: &str) -> String {
    let mut tokens: Vec<String> = normalize_name(name)
        .split(' ')
        .map(|t| t.chars().take(3).collect())
        .collect();
    tokens.sort();
    tokens.join("|")
}

/// Token Jaccard similarity of two strings after normalization.
pub fn token_similarity(a: &str, b: &str) -> f64 {
    let a: std::collections::BTreeSet<String> =
        normalize_name(a).split(' ').map(str::to_string).collect();
    let b: std::collections::BTreeSet<String> =
        normalize_name(b).split(' ').map(str::to_string).collect();
    let union = a.union(&b).count();
    if union == 0 {
        return 0.0;
    }
    a.intersection(&b).count() as f64 / union as f64
}

/// A field's agreement probabilities: `m` among true matches, `u` among
/// non-matches.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldWeight {
    pub field: String,
    pub m: f64,
    pub u: f64,
    /// Agreement holds at or above this similarity.
    pub agree_at: f64,
}

/// The Fellegi-Sunter model: per-field weights and the two thresholds.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FellegiSunterModel {
    pub fields: Vec<FieldWeight>,
    pub upper: f64,
    pub lower: f64,
}

impl FellegiSunterModel {
    /// Sum of log2(m/u) on agreement and log2((1-m)/(1-u)) on disagreement.
    /// A field absent on either side contributes nothing.
    pub fn score(&self, left: &ResolutionRecord, right: &ResolutionRecord) -> f64 {
        self.fields
            .iter()
            .filter_map(|w| {
                let (a, b) = (left.fields.get(&w.field)?, right.fields.get(&w.field)?);
                let agree = token_similarity(a, b) >= w.agree_at;
                Some(if agree {
                    (w.m / w.u).log2()
                } else {
                    ((1.0 - w.m) / (1.0 - w.u)).log2()
                })
            })
            .sum()
    }

    pub fn classify(&self, score: f64) -> MatchClass {
        if score >= self.upper {
            MatchClass::Match
        } else if score <= self.lower {
            MatchClass::NonMatch
        } else {
            MatchClass::Ambiguous
        }
    }
}

/// Generate unapproved proposals: compare only records of different
/// applications within one block, and score each pair.
pub fn propose_links(
    records: &[ResolutionRecord],
    model: &FellegiSunterModel,
) -> Vec<EntityLinkProposal> {
    let mut blocks: BTreeMap<String, Vec<&ResolutionRecord>> = BTreeMap::new();
    for record in records {
        blocks
            .entry(blocking_key(&record.name))
            .or_default()
            .push(record);
    }
    let mut proposals = Vec::new();
    for members in blocks.values() {
        for (i, left) in members.iter().enumerate() {
            for right in &members[i + 1..] {
                if left.entity.application == right.entity.application {
                    continue;
                }
                let score = model.score(left, right);
                let range = (model.upper - model.lower).abs().max(f64::EPSILON);
                let scaled = ((score - model.lower) / range).clamp(0.0, 1.0);
                proposals.push(EntityLinkProposal {
                    left: left.entity.clone(),
                    right: right.entity.clone(),
                    match_class: model.classify(score),
                    match_score_milli: (scaled * 1000.0).round() as u32,
                    approved_by: None,
                });
            }
        }
    }
    proposals
}

/// A refused approval.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalRefusal {
    /// `APPROVER_REQUIRED`
    ApproverRequired,
    /// `ALREADY_APPROVED`
    AlreadyApproved,
}

impl ApprovalRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ApproverRequired => "APPROVER_REQUIRED",
            Self::AlreadyApproved => "ALREADY_APPROVED",
        }
    }
}

/// Record a human approval on a proposal. The proposal still asserts
/// `owl:sameAs` only when it is also classified `Match`.
pub fn approve(
    proposal: &EntityLinkProposal,
    approver: &str,
) -> Result<EntityLinkProposal, ApprovalRefusal> {
    if approver.trim().is_empty() {
        return Err(ApprovalRefusal::ApproverRequired);
    }
    if proposal.approved_by.is_some() {
        return Err(ApprovalRefusal::AlreadyApproved);
    }
    Ok(EntityLinkProposal {
        approved_by: Some(approver.to_string()),
        ..proposal.clone()
    })
}

/// The kind of one piece of identity evidence for a candidate link.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum IdentityEvidence {
    /// The same stable key (email, external id) on both sides.
    ExactKey { key: String },
    /// Equal names after normalization.
    NormalizedName { name: String },
    /// The same base name with a different version suffix.
    VersionVariant {
        base: String,
        left: String,
        right: String,
    },
    /// A name-like token whose entropy marks it as opaque.
    EntropyQualifiedToken { token: String, bits_per_char: f64 },
    /// Embedding similarity of the two records.
    Embedding { cosine: f64 },
}

/// A merge decision for a candidate link; always a proposal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeDecision {
    /// Propose merging the two into one entity.
    ProposeMerge,
    /// Propose a typed link; the entities stay distinct.
    ProposeLink,
    /// No proposal.
    NoMerge,
}

/// Above this many bits per character, a name is an opaque token.
pub const HIGH_ENTROPY_BITS_PER_CHAR: f64 = 3.5;

/// Shannon entropy of `text` in bits per character.
pub fn entropy_bits_per_char(text: &str) -> f64 {
    let mut counts: BTreeMap<char, usize> = BTreeMap::new();
    let mut total = 0usize;
    for c in text.chars() {
        *counts.entry(c).or_default() += 1;
        total += 1;
    }
    if total == 0 {
        return 0.0;
    }
    counts
        .values()
        .map(|n| {
            let p = *n as f64 / total as f64;
            -p * p.log2()
        })
        .sum()
}

/// Split a trailing version suffix (`v2`, `2.1`, `-3`) from a name.
pub fn version_base(name: &str) -> (String, Option<String>) {
    let normalized = normalize_name(name);
    let mut parts: Vec<&str> = normalized.split(' ').collect();
    if let Some(last) = parts.last() {
        let digits = last.trim_start_matches('v');
        if parts.len() > 1 && !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
            let version = last.to_string();
            parts.pop();
            return (parts.join(" "), Some(version));
        }
    }
    (normalized, None)
}

/// Collect the identity evidence between two names and optional keys.
pub fn identity_evidence(
    left_name: &str,
    right_name: &str,
    left_key: Option<&str>,
    right_key: Option<&str>,
) -> Vec<IdentityEvidence> {
    let mut evidence = Vec::new();
    if let (Some(a), Some(b)) = (left_key, right_key) {
        if !a.is_empty() && a == b {
            evidence.push(IdentityEvidence::ExactKey { key: a.to_string() });
        }
    }
    let (left_base, left_version) = version_base(left_name);
    let (right_base, right_version) = version_base(right_name);
    if left_base == right_base && left_version != right_version {
        evidence.push(IdentityEvidence::VersionVariant {
            base: left_base,
            left: left_version.unwrap_or_default(),
            right: right_version.unwrap_or_default(),
        });
    } else if normalize_name(left_name) == normalize_name(right_name) {
        let name = normalize_name(left_name);
        let bits = entropy_bits_per_char(&name);
        if bits >= HIGH_ENTROPY_BITS_PER_CHAR {
            evidence.push(IdentityEvidence::EntropyQualifiedToken {
                token: name,
                bits_per_char: bits,
            });
        } else {
            evidence.push(IdentityEvidence::NormalizedName { name });
        }
    }
    evidence
}

/// Decide from evidence: an exact key or a strong embedding with a name
/// match proposes a merge; a version variant proposes a link; an opaque
/// token alone never merges.
pub fn decide_merge(evidence: &[IdentityEvidence]) -> MergeDecision {
    let has = |f: fn(&IdentityEvidence) -> bool| evidence.iter().any(f);
    if has(|e| matches!(e, IdentityEvidence::VersionVariant { .. })) {
        return MergeDecision::ProposeLink;
    }
    if has(|e| matches!(e, IdentityEvidence::ExactKey { .. })) {
        return MergeDecision::ProposeMerge;
    }
    let name = has(|e| matches!(e, IdentityEvidence::NormalizedName { .. }));
    let embedding = has(|e| matches!(e, IdentityEvidence::Embedding { cosine } if *cosine >= 0.9));
    if name && embedding {
        MergeDecision::ProposeMerge
    } else if name {
        MergeDecision::ProposeLink
    } else {
        MergeDecision::NoMerge
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_link_proposal::{AttachedApplication, CannotAssert};

    fn record(app: AttachedApplication, id: &str, name: &str, birth: &str) -> ResolutionRecord {
        ResolutionRecord {
            entity: EntityRef {
                application: app,
                entity_id: id.to_string(),
            },
            name: name.to_string(),
            fields: BTreeMap::from([
                ("name".to_string(), name.to_string()),
                ("birth_year".to_string(), birth.to_string()),
            ]),
        }
    }

    fn model() -> FellegiSunterModel {
        FellegiSunterModel {
            fields: vec![
                FieldWeight {
                    field: "name".to_string(),
                    m: 0.95,
                    u: 0.05,
                    agree_at: 0.99,
                },
                FieldWeight {
                    field: "birth_year".to_string(),
                    m: 0.9,
                    u: 0.1,
                    agree_at: 0.99,
                },
            ],
            upper: 6.0,
            lower: 0.0,
        }
    }

    fn fixture() -> Vec<ResolutionRecord> {
        use AttachedApplication::{Firefly, Gramps, Immich, Twenty};
        vec![
            // true match: same name and year
            record(Gramps, "g1", "Ada Lovelace", "1815"),
            record(Immich, "i1", "ada  LOVELACE", "1815"),
            // ambiguous: same name, different year
            record(Twenty, "t1", "John Smith", "1970"),
            record(Firefly, "f1", "John Smith", "1985"),
            // false match: different block entirely
            record(Gramps, "g2", "Grace Hopper", "1906"),
            record(Twenty, "t2", "Alan Turing", "1912"),
        ]
    }

    // spec: EG-UNIFIED-DATA-PLANE-R030.2
    #[test]
    fn blocking_and_similarity_generate_unapproved_candidates() {
        assert_eq!(blocking_key("Ada Lovelace"), blocking_key("lovelace, ADA"));
        assert!((token_similarity("Ada Lovelace", "ada lovelace") - 1.0).abs() < 1e-12);
        let proposals = propose_links(&fixture(), &model());
        assert_eq!(
            proposals.len(),
            2,
            "only same-block, cross-app pairs compare"
        );
        for proposal in &proposals {
            assert_eq!(proposal.approved_by, None);
            assert!(proposal.assert_same_as().is_err());
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R030.3
    #[test]
    fn fellegi_sunter_classifies_and_approval_gates_assertion() {
        let proposals = propose_links(&fixture(), &model());
        let by_left = |id: &str| {
            proposals
                .iter()
                .find(|p| p.left.entity_id == id || p.right.entity_id == id)
                .unwrap()
                .clone()
        };
        let ada = by_left("g1");
        let smith = by_left("t1");
        assert_eq!(ada.match_class, MatchClass::Match);
        assert_eq!(smith.match_class, MatchClass::Ambiguous);
        let expected = (0.95f64 / 0.05).log2() + (0.9f64 / 0.1).log2();
        let score = model().score(&fixture()[0], &fixture()[1]);
        assert!((score - expected).abs() < 1e-9);

        assert_eq!(ada.assert_same_as(), Err(CannotAssert::NotApproved));
        let approved = approve(&ada, "alice").unwrap();
        assert!(approved.assert_same_as().is_ok());
        let ambiguous = approve(&smith, "alice").unwrap();
        assert_eq!(ambiguous.assert_same_as(), Err(CannotAssert::NotAMatch));
        assert_eq!(approve(&ada, " ").unwrap_err().code(), "APPROVER_REQUIRED");
        assert_eq!(
            approve(&approved, "bob").unwrap_err().code(),
            "ALREADY_APPROVED"
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R030.700
    #[test]
    fn evidence_kinds_and_merge_decisions() {
        let opaque = identity_evidence("x7Qp9ZkL2mWv", "x7Qp9ZkL2mWv", None, None);
        assert!(matches!(
            opaque[0],
            IdentityEvidence::EntropyQualifiedToken { .. }
        ));
        assert_eq!(decide_merge(&opaque), MergeDecision::NoMerge);

        let variant = identity_evidence("Report Builder v2", "Report Builder v3", None, None);
        assert!(matches!(
            variant[0],
            IdentityEvidence::VersionVariant { .. }
        ));
        assert_eq!(decide_merge(&variant), MergeDecision::ProposeLink);

        let keyed = identity_evidence("Ada", "A. Lovelace", Some("ada@x.org"), Some("ada@x.org"));
        assert_eq!(decide_merge(&keyed), MergeDecision::ProposeMerge);

        let mut named = identity_evidence("Ada Lovelace", "ada lovelace", None, None);
        assert_eq!(decide_merge(&named), MergeDecision::ProposeLink);
        named.push(IdentityEvidence::Embedding { cosine: 0.95 });
        assert_eq!(decide_merge(&named), MergeDecision::ProposeMerge);

        let json = serde_json::to_string(&named).unwrap();
        assert_eq!(
            serde_json::from_str::<Vec<IdentityEvidence>>(&json).unwrap(),
            named
        );
    }
}
