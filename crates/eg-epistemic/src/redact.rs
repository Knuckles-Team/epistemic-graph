//! Policy-aware proof redaction + selective disclosure (CONCEPT:EG-KG.epistemic.epistemic-substrate, EPI-P3-4).
//!
//! [`explain_belief`](crate::propagate::explain_belief) answers "why does the engine
//! believe X" with a full justification proof tree — every premise's id, inference
//! rule, and confidence. That tree is a first-class read surface (KG-2.134, and
//! EPI-P3-1's `proof_ids` column), so it must respect the SAME per-agent Row-Level
//! Security every other read path enforces (`eg_core::isolation`,
//! CONCEPT:EG-KG.sharding.row-level-security): an agent who cannot see a given
//! evidence NODE via [`IsolationLayer::filter_view`]/[`IsolationLayer::can_see_row`]
//! must not learn its identity by asking `EXPLAIN BELIEF` instead.
//!
//! Unlike `filter_view` (which DROPS an invisible node from a `GraphView` entirely),
//! a proof tree cannot simply drop a node without losing the argument's shape — the
//! whole point of an explanation is "this claim is supported by N premises of these
//! kinds, with this confidence". So this module REDACTS in place instead of
//! dropping: an invisible node's `claim` (its id/content) is replaced with a
//! redaction marker, while its inference `rule`, `confidence`, and `premises`
//! (recursively redacted the same way) are preserved — the actor learns the
//! argument's SHAPE without learning forbidden content. This mirrors `filter_view`'s
//! own per-node (not per-subtree) granularity: exactly the rows RLS would hide from a
//! `GraphView` are the nodes redacted here, no more and no less.
//!
//! No second permission model: every visibility decision below is
//! [`IsolationLayer::can_see_row`] evaluated against the same
//! [`RowVisibility`](eg_core::isolation::RowVisibility) `filter_view` reads off the
//! node's property blob ([`BeliefGraph::node_visibility`], populated by
//! [`BeliefGraph::from_graph_view`]).
//!
//! ## Selective disclosure levels
//!
//! Three levels, chosen automatically from the actor's own access — never
//! caller-selectable, since a caller could otherwise simply request a higher level
//! than their access earns:
//!
//! * [`DisclosureLevel::Full`] — every node in the tree is visible to the actor (or
//!   the isolation layer has no rules registered at all — single-tenant back-compat,
//!   the same condition [`IsolationLayer::has_rules`] already special-cases
//!   elsewhere). Redaction is a structural no-op: the rendered tree is identical to
//!   [`explain_belief`](crate::propagate::explain_belief)'s.
//! * [`DisclosureLevel::Skeleton`] — the root claim itself is visible, but one or
//!   more premises are not: those are individually redacted in place as described
//!   above.
//! * [`DisclosureLevel::ExistenceOnly`] — the root claim ITSELF is not visible to the
//!   actor. No structure is rendered at all — not even a redacted skeleton, which
//!   would still leak the shape of an argument the actor has no right to see. The
//!   actor learns only the coarse [`ExistenceSignal`] (supported / contradicted /
//!   uncertain), never the raw posterior float (a leak of structural detail in its
//!   own right at this tier).

use eg_core::isolation::{IsolationLayer, RowVisibility};
use serde::{Deserialize, Serialize};

use crate::adapter::BeliefGraph;
use crate::model::{AuthorityPolicy, JustRule, ProofNode};
use crate::propagate::explain_belief;

/// The visibility a node with NO captured RLS metadata defaults to — mirrors
/// `filter_view`'s own rule: "a node present in topology but with NO property blob is
/// unowned ⇒ visible".
fn unowned_visible() -> RowVisibility {
    RowVisibility {
        owner: None,
        public: true,
        grants: Vec::new(),
        tagged: false,
    }
}

/// Selective-disclosure level for a redacted proof (see module docs). Derived from
/// the actor's own access; never requested by the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DisclosureLevel {
    Full,
    Skeleton,
    ExistenceOnly,
}

impl DisclosureLevel {
    /// Total ordering by how much is HIDDEN: `Full` (0) < `Skeleton` (1) <
    /// `ExistenceOnly` (2). Used only by [`explain_belief_redacted_capped`] to compare
    /// a caller-requested cap against the actor's earned level — never to grant more
    /// than the actor earns, only to let a caller ask for a STRICTER view.
    fn strictness(self) -> u8 {
        match self {
            DisclosureLevel::Full => 0,
            DisclosureLevel::Skeleton => 1,
            DisclosureLevel::ExistenceOnly => 2,
        }
    }
}

/// The coarse "is this claim believed at all" signal surfaced at every disclosure
/// level (including [`DisclosureLevel::ExistenceOnly`], where it is the ONLY thing
/// surfaced). Deliberately not the raw float confidence — a float is itself
/// structural detail this module does not leak below `Full`/`Skeleton`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExistenceSignal {
    Supported,
    Contradicted,
    Uncertain,
}

impl ExistenceSignal {
    fn from_confidence(confidence: f64) -> Self {
        if confidence > 0.5 {
            ExistenceSignal::Supported
        } else if confidence < 0.5 {
            ExistenceSignal::Contradicted
        } else {
            ExistenceSignal::Uncertain
        }
    }
}

/// One node of a redacted proof tree — structurally parallel to
/// [`ProofNode`](crate::model::ProofNode), except `claim` is masked when the actor
/// cannot see the underlying graph node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RedactedProofNode {
    /// `Some(id)` when the actor may see this node's identity; `None` when redacted
    /// — see `redaction_label` for what is shown in its place. NEVER populated with
    /// the id (or any other content) of a node the actor cannot see.
    pub claim: Option<String>,
    /// Present ONLY when `claim` is `None` — a fixed classification label, never the
    /// id or any other content of the redacted node.
    pub redaction_label: Option<String>,
    /// Preserved even when the node is redacted — the inference type is part of the
    /// argument's SHAPE, not its content.
    pub rule: JustRule,
    /// Preserved even when the node is redacted — the posterior confidence is part
    /// of the argument's shape (it is a computed number, not the evidence's
    /// identity/content) and callers rely on it matching `explain_belief` exactly.
    pub confidence: f64,
    /// Always present, always the true premise count — "supported by N premises" is
    /// exactly the shape a redacted node is still allowed to reveal. Each premise is
    /// independently redacted (an invisible node's own children may still be
    /// individually visible, matching `filter_view`'s per-node granularity).
    pub premises: Vec<RedactedProofNode>,
}

impl RedactedProofNode {
    /// Whether THIS node (not its premises) is redacted.
    pub fn is_redacted(&self) -> bool {
        self.claim.is_none()
    }

    /// Whether this node OR any node in its subtree is redacted.
    pub fn has_redactions(&self) -> bool {
        self.is_redacted() || self.premises.iter().any(RedactedProofNode::has_redactions)
    }
}

/// The result of a policy-aware `EXPLAIN BELIEF` — see module docs for the three
/// levels.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RedactedJustificationGraph {
    pub level: DisclosureLevel,
    /// Always populated — the coarse signal available even at `ExistenceOnly`.
    pub existence: ExistenceSignal,
    /// `Some` at `Full`/`Skeleton`; `None` at `ExistenceOnly` (no structure rendered
    /// at all — see module docs).
    pub root: Option<RedactedProofNode>,
}

impl RedactedJustificationGraph {
    /// `true` if any node in the rendered tree is redacted; always `false` when
    /// `root` is `None` (there is no tree to redact).
    pub fn has_redactions(&self) -> bool {
        self.root
            .as_ref()
            .map(RedactedProofNode::has_redactions)
            .unwrap_or(false)
    }
}

fn node_visible(bg: &BeliefGraph, id: &str, isolation: &IsolationLayer, actor_id: &str) -> bool {
    let default_vis;
    let vis = match bg.node_visibility.get(id) {
        Some(v) => v,
        None => {
            default_vis = unowned_visible();
            &default_vis
        }
    };
    isolation.can_see_row(actor_id, vis)
}

fn redact_node(
    node: &ProofNode,
    bg: &BeliefGraph,
    isolation: &IsolationLayer,
    actor_id: &str,
) -> RedactedProofNode {
    let visible = node_visible(bg, &node.claim, isolation, actor_id);
    let premises = node
        .premises
        .iter()
        .map(|p| redact_node(p, bg, isolation, actor_id))
        .collect();
    if visible {
        RedactedProofNode {
            claim: Some(node.claim.clone()),
            redaction_label: None,
            rule: node.rule,
            confidence: node.confidence,
            premises,
        }
    } else {
        RedactedProofNode {
            claim: None,
            redaction_label: Some("redacted:restricted".to_string()),
            rule: node.rule,
            confidence: node.confidence,
            premises,
        }
    }
}

/// An unredacted rendering — used only when there is nothing to check against (no
/// rules registered at all).
fn unredacted(node: &ProofNode) -> RedactedProofNode {
    RedactedProofNode {
        claim: Some(node.claim.clone()),
        redaction_label: None,
        rule: node.rule,
        confidence: node.confidence,
        premises: node.premises.iter().map(unredacted).collect(),
    }
}

/// Produce the policy-aware, selectively-disclosed proof for `seed` as seen by
/// `actor_id` — the redaction-aware sibling of
/// [`explain_belief`](crate::propagate::explain_belief). Reuses `isolation`'s
/// existing [`IsolationLayer::can_see_row`] access check (the SAME one
/// [`IsolationLayer::filter_view`] enforces on every other read path) per proof-tree
/// node; invents no second permission model. See module docs for the three
/// disclosure levels.
pub fn explain_belief_redacted(
    bg: &BeliefGraph,
    seed: &str,
    policy: &AuthorityPolicy,
    isolation: &IsolationLayer,
    actor_id: &str,
) -> RedactedJustificationGraph {
    let full = explain_belief(bg, seed, policy);
    let existence = ExistenceSignal::from_confidence(full.root.confidence);

    // Single-tenant back-compat: no registered identities ⇒ no RLS to enforce at all
    // (the same no-op condition `IsolationLayer::filter_view`/`check_access` apply).
    if !isolation.has_rules() {
        return RedactedJustificationGraph {
            level: DisclosureLevel::Full,
            existence,
            root: Some(unredacted(&full.root)),
        };
    }

    if !node_visible(bg, seed, isolation, actor_id) {
        // The claim itself is invisible to the actor: no structure at all — a
        // redacted skeleton would still leak the argument's shape for a claim the
        // actor has no right to see.
        return RedactedJustificationGraph {
            level: DisclosureLevel::ExistenceOnly,
            existence,
            root: None,
        };
    }

    let redacted_root = redact_node(&full.root, bg, isolation, actor_id);
    let level = if redacted_root.has_redactions() {
        DisclosureLevel::Skeleton
    } else {
        DisclosureLevel::Full
    };
    RedactedJustificationGraph {
        level,
        existence,
        root: Some(redacted_root),
    }
}

/// [`explain_belief_redacted`] plus an OPTIONAL caller-requested `cap` (L51, the
/// `Method::ExplainBelief::disclosure_level` wiring): a caller may ask for a STRICTER
/// view than their own RLS access earns (e.g. a privacy-conscious display always
/// wants `ExistenceOnly`, even for an actor who could see the full proof), but can
/// NEVER loosen what `explain_belief_redacted` computes from their actual access —
/// `cap` only ever narrows, matching [`DisclosureLevel::strictness`]'s ordering.
///
/// `cap: None` (or a `cap` no stricter than the earned level) is a byte-for-byte
/// passthrough of [`explain_belief_redacted`]'s result — this is what keeps the
/// default (`disclosure_level: None` on the wire) path unchanged.
///
/// A requested `ExistenceOnly` cap always downgrades cleanly (no structure to hide
/// beyond dropping the tree entirely, exactly like a genuinely-invisible root). A
/// requested `Skeleton` cap over an earned `Full` result is a LABEL-only downgrade:
/// since the actor's own access hid nothing, there is nothing further to redact in
/// place — the caller learns the tree is labeled `Skeleton` (so it does not read the
/// absence of redaction as a promise of `Full` access) but sees the same content.
pub fn explain_belief_redacted_capped(
    bg: &BeliefGraph,
    seed: &str,
    policy: &AuthorityPolicy,
    isolation: &IsolationLayer,
    actor_id: &str,
    cap: Option<DisclosureLevel>,
) -> RedactedJustificationGraph {
    let earned = explain_belief_redacted(bg, seed, policy, isolation, actor_id);
    let Some(cap) = cap else {
        return earned;
    };
    if cap.strictness() <= earned.level.strictness() {
        // No stricter than what the actor already earned — nothing to narrow.
        return earned;
    }
    match cap {
        DisclosureLevel::ExistenceOnly => RedactedJustificationGraph {
            level: DisclosureLevel::ExistenceOnly,
            existence: earned.existence,
            root: None,
        },
        // Only reachable when `earned.level == Full` (see the strictness guard
        // above) — relabel only, see this fn's doc comment.
        DisclosureLevel::Skeleton => RedactedJustificationGraph {
            level: DisclosureLevel::Skeleton,
            ..earned
        },
        DisclosureLevel::Full => earned, // unreachable: Full is never stricter than anything
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EdgeKind;
    use eg_core::isolation::{AgentIdentity, AgentRole};

    fn owned_private(owner: &str) -> RowVisibility {
        RowVisibility {
            owner: Some(owner.to_string()),
            public: false,
            grants: Vec::new(),
            tagged: true,
        }
    }

    fn public() -> RowVisibility {
        RowVisibility {
            owner: None,
            public: true,
            grants: Vec::new(),
            tagged: true,
        }
    }

    fn layer_with(agents: &[(&str, AgentRole)]) -> IsolationLayer {
        let mut layer = IsolationLayer::new();
        for (id, role) in agents {
            layer.register_agent(AgentIdentity {
                agent_id: id.to_string(),
                role: role.clone(),
                teams: vec![],
                roles: vec![],
            });
        }
        layer
    }

    // claim <- evidence (public) <- secret (private, owned by "owner")
    fn bg_with_secret_leaf() -> BeliefGraph {
        BeliefGraph::from_parts(
            [("claim", 0.5), ("evidence", 0.6), ("secret", 0.95)],
            [
                ("evidence", "claim", EdgeKind::Supports),
                ("secret", "evidence", EdgeKind::Supports),
            ],
        )
        .with_visibility([
            ("claim", public()),
            ("evidence", public()),
            ("secret", owned_private("owner")),
        ])
    }

    #[test]
    fn no_rules_registered_is_full_back_compat() {
        let bg = bg_with_secret_leaf();
        let layer = IsolationLayer::new(); // no identities ⇒ no-op, matches filter_view
        let j =
            explain_belief_redacted(&bg, "claim", &AuthorityPolicy::default(), &layer, "anyone");
        assert_eq!(j.level, DisclosureLevel::Full);
        assert!(!j.has_redactions());
        let root = j.root.unwrap();
        assert_eq!(root.claim.as_deref(), Some("claim"));
    }

    #[test]
    fn owner_sees_everything_full() {
        let bg = bg_with_secret_leaf();
        let layer = layer_with(&[("owner", AgentRole::Agent), ("stranger", AgentRole::Agent)]);
        let j = explain_belief_redacted(&bg, "claim", &AuthorityPolicy::default(), &layer, "owner");
        assert_eq!(j.level, DisclosureLevel::Full);
        assert!(!j.has_redactions());
    }

    #[test]
    fn stranger_gets_skeleton_with_evidence_hidden_but_shape_preserved() {
        let bg = bg_with_secret_leaf();
        let layer = layer_with(&[("owner", AgentRole::Agent), ("stranger", AgentRole::Agent)]);
        let full = explain_belief(&bg, "claim", &AuthorityPolicy::default());
        let j = explain_belief_redacted(
            &bg,
            "claim",
            &AuthorityPolicy::default(),
            &layer,
            "stranger",
        );
        assert_eq!(j.level, DisclosureLevel::Skeleton);
        assert!(j.has_redactions());

        let root = j.root.unwrap();
        // Root claim itself is visible (public).
        assert_eq!(root.claim.as_deref(), Some("claim"));
        assert_eq!(root.rule, full.root.rule);
        // Confidence (structure) preserved exactly, even though a descendant is hidden.
        assert!((root.confidence - full.root.confidence).abs() < 1e-12);
        // Support-count preserved: still exactly one premise.
        assert_eq!(root.premises.len(), full.root.premises.len());

        // "evidence" is public, so it stays visible...
        let evidence_node = &root.premises[0];
        assert_eq!(evidence_node.claim.as_deref(), Some("evidence"));
        assert!((evidence_node.confidence - full.root.premises[0].confidence).abs() < 1e-12);
        assert_eq!(
            evidence_node.premises.len(),
            full.root.premises[0].premises.len()
        );

        // ...but ITS premise "secret" is private and owned by someone else: redacted.
        let secret_node = &evidence_node.premises[0];
        assert!(secret_node.is_redacted());
        assert_eq!(secret_node.claim, None);
        assert!(secret_node.redaction_label.is_some());
        // The redaction label never contains the hidden node's id.
        assert!(!secret_node
            .redaction_label
            .as_ref()
            .unwrap()
            .contains("secret"));
        // Skeleton (rule + confidence + leaf-ness) is preserved even for the redacted node.
        let secret_full = &full.root.premises[0].premises[0];
        assert_eq!(secret_node.rule, secret_full.rule);
        assert!((secret_node.confidence - secret_full.confidence).abs() < 1e-12);
        assert_eq!(secret_node.premises.len(), secret_full.premises.len());
    }

    #[test]
    fn root_invisible_yields_existence_only_no_structure() {
        // Root claim itself is private and owned by someone else.
        let bg = BeliefGraph::from_parts(
            [("claim", 0.9), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        )
        .with_visibility([("claim", owned_private("owner")), ("evidence", public())]);
        let layer = layer_with(&[("owner", AgentRole::Agent), ("stranger", AgentRole::Agent)]);
        let j = explain_belief_redacted(
            &bg,
            "claim",
            &AuthorityPolicy::default(),
            &layer,
            "stranger",
        );
        assert_eq!(j.level, DisclosureLevel::ExistenceOnly);
        assert!(j.root.is_none());
        // Confidence is high (0.9 prior + support) ⇒ Supported, but no float is exposed.
        assert_eq!(j.existence, ExistenceSignal::Supported);
        assert!(!j.has_redactions());
    }

    #[test]
    fn existence_signal_thresholds() {
        assert_eq!(
            ExistenceSignal::from_confidence(0.9),
            ExistenceSignal::Supported
        );
        assert_eq!(
            ExistenceSignal::from_confidence(0.1),
            ExistenceSignal::Contradicted
        );
        assert_eq!(
            ExistenceSignal::from_confidence(0.5),
            ExistenceSignal::Uncertain
        );
    }

    #[test]
    fn system_role_bypasses_redaction_like_every_other_read_path() {
        // System sees everything, exactly as `can_see_row` documents.
        let bg = bg_with_secret_leaf();
        let mut layer = layer_with(&[("owner", AgentRole::Agent)]);
        layer.register_agent(AgentIdentity {
            agent_id: "root".to_string(),
            role: AgentRole::System,
            teams: vec![],
            roles: vec![],
        });
        let j = explain_belief_redacted(&bg, "claim", &AuthorityPolicy::default(), &layer, "root");
        assert_eq!(j.level, DisclosureLevel::Full);
        assert!(!j.has_redactions());
    }

    // --- explain_belief_redacted_capped (L51: the ExplainBelief wire cap) -------------

    #[test]
    fn cap_none_is_a_passthrough() {
        let bg = bg_with_secret_leaf();
        let layer = layer_with(&[("owner", AgentRole::Agent), ("stranger", AgentRole::Agent)]);
        let earned = explain_belief_redacted(&bg, "claim", &AuthorityPolicy::default(), &layer, "stranger");
        let capped = explain_belief_redacted_capped(
            &bg,
            "claim",
            &AuthorityPolicy::default(),
            &layer,
            "stranger",
            None,
        );
        assert_eq!(earned, capped);
    }

    #[test]
    fn cap_cannot_loosen_a_stricter_earned_level() {
        // `stranger` earns Skeleton (see `stranger_gets_skeleton_...` above); asking for
        // a LOOSER cap (`Full`) must NOT grant more than RLS actually earned.
        let bg = bg_with_secret_leaf();
        let layer = layer_with(&[("owner", AgentRole::Agent), ("stranger", AgentRole::Agent)]);
        let capped = explain_belief_redacted_capped(
            &bg,
            "claim",
            &AuthorityPolicy::default(),
            &layer,
            "stranger",
            Some(DisclosureLevel::Full),
        );
        assert_eq!(capped.level, DisclosureLevel::Skeleton);
        assert!(capped.has_redactions());
    }

    #[test]
    fn cap_can_request_a_stricter_existence_only_view_over_a_full_actor() {
        // `owner` earns Full (sees everything) but requests ExistenceOnly for a
        // privacy-conscious display — the cap narrows even a fully-earned view.
        let bg = bg_with_secret_leaf();
        let layer = layer_with(&[("owner", AgentRole::Agent), ("stranger", AgentRole::Agent)]);
        let capped = explain_belief_redacted_capped(
            &bg,
            "claim",
            &AuthorityPolicy::default(),
            &layer,
            "owner",
            Some(DisclosureLevel::ExistenceOnly),
        );
        assert_eq!(capped.level, DisclosureLevel::ExistenceOnly);
        assert!(capped.root.is_none());
        // The coarse existence signal is still surfaced even at the capped level.
        assert_eq!(capped.existence, ExistenceSignal::Supported);
    }

    #[test]
    fn cap_skeleton_over_a_full_earned_result_relabels_without_hiding_more() {
        // No secret leaf here: `stranger` earns Full over an all-public graph, but
        // requests Skeleton — nothing further to redact, so content is unchanged, only
        // the label narrows (documented behavior, see fn doc comment).
        let bg = BeliefGraph::from_parts(
            [("claim", 0.9), ("evidence", 0.9)],
            [("evidence", "claim", EdgeKind::Supports)],
        )
        .with_visibility([("claim", public()), ("evidence", public())]);
        let layer = layer_with(&[("stranger", AgentRole::Agent)]);
        let capped = explain_belief_redacted_capped(
            &bg,
            "claim",
            &AuthorityPolicy::default(),
            &layer,
            "stranger",
            Some(DisclosureLevel::Skeleton),
        );
        assert_eq!(capped.level, DisclosureLevel::Skeleton);
        let root = capped.root.unwrap();
        assert_eq!(root.claim.as_deref(), Some("claim"));
        assert!(!root.is_redacted());
    }

    #[test]
    fn manager_sees_subordinates_private_evidence() {
        let bg = bg_with_secret_leaf();
        let mut layer = IsolationLayer::new();
        layer.register_agent(AgentIdentity {
            agent_id: "owner".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "boss".to_string(),
            role: AgentRole::Manager {
                subordinates: vec!["owner".to_string()],
            },
            teams: vec![],
            roles: vec![],
        });
        let j = explain_belief_redacted(&bg, "claim", &AuthorityPolicy::default(), &layer, "boss");
        assert_eq!(j.level, DisclosureLevel::Full);
        assert!(!j.has_redactions());
    }
}
