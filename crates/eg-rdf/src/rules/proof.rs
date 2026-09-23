//! Proof trees for rule derivations (EH-197).
//!
//! The forward-chaining engine records, for every fact whose confidence came from a
//! rule, the rule and the ground body facts it fired over. [`RuleDerivations`] turns
//! that record into a recursive [`RuleProofNode`] tree — the rule-side twin of the OWL
//! reasoner's `ProofNodeWire` — bottoming out at asserted facts. A proof is bounded:
//! it stops at [`MAX_PROOF_DEPTH`] levels and [`MAX_PROOF_NODES`] nodes, and never
//! re-enters a fact already on its own path, marking the cut node `truncated`.

use std::collections::HashMap;

pub use eg_types::rdf_report::RuleProofNode;

/// A ground fact key: `(predicate, canonical-args)`.
pub(super) type FactKey = (String, Vec<String>);

/// Rule label of a proof leaf: a fact whose confidence is its own assertion.
pub const ASSERTED_RULE: &str = "asserted";
/// Deepest proof level expanded before a node is cut.
pub const MAX_PROOF_DEPTH: usize = 32;
/// Most nodes one proof tree holds before remaining premises are cut.
pub const MAX_PROOF_NODES: usize = 512;

/// The derivation that set one fact's current confidence.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Justification {
    pub(super) rule: String,
    pub(super) premises: Vec<FactKey>,
}

/// Every fact's final confidence and, for derived ones, the justifying derivation.
#[derive(Clone, Debug, Default)]
pub struct RuleDerivations {
    conf: HashMap<FactKey, f64>,
    justification: HashMap<FactKey, Justification>,
}

/// The mutable state of one proof-tree construction.
struct ProofBuild<'a> {
    derivations: &'a RuleDerivations,
    path: Vec<&'a FactKey>,
    nodes: usize,
}

impl RuleDerivations {
    pub(super) fn new(
        conf: HashMap<FactKey, f64>,
        justification: HashMap<FactKey, Justification>,
    ) -> Self {
        Self {
            conf,
            justification,
        }
    }

    /// The proof tree of the canonical fact `pred(args…)`, or `None` when the fact
    /// does not hold.
    pub fn proof(&self, pred: &str, args: &[String]) -> Option<RuleProofNode> {
        let (key, _) = self
            .conf
            .get_key_value(&(pred.to_string(), args.to_vec()))?;
        let mut build = ProofBuild {
            derivations: self,
            path: Vec::new(),
            nodes: 0,
        };
        Some(build.node(key))
    }
}

impl<'a> ProofBuild<'a> {
    fn node(&mut self, key: &'a FactKey) -> RuleProofNode {
        self.nodes += 1;
        let mut node = RuleProofNode {
            predicate: key.0.clone(),
            args: key.1.clone(),
            rule: ASSERTED_RULE.to_string(),
            confidence: self.derivations.conf.get(key).copied().unwrap_or(1.0),
            premises: Vec::new(),
            truncated: false,
        };
        let derivations = self.derivations;
        let Some(justification) = derivations.justification.get(key) else {
            return node;
        };
        node.rule = justification.rule.clone();
        if !self.may_expand(key) {
            node.truncated = true;
            return node;
        }
        self.path.push(key);
        for premise in &justification.premises {
            if self.nodes >= MAX_PROOF_NODES {
                node.truncated = true;
                break;
            }
            node.premises.push(self.node(premise));
        }
        self.path.pop();
        node
    }

    /// Whether `key`'s premises may be expanded: within the depth budget and not
    /// already being proved further up this branch (a `sameAs` merge can fold a
    /// premise onto its own conclusion).
    fn may_expand(&self, key: &FactKey) -> bool {
        self.path.len() < MAX_PROOF_DEPTH && !self.path.contains(&key)
    }
}
