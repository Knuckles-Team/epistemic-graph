//! The named, versioned derivation rules (EG-SDLC-R001.2). Each rule is a pure
//! function of one changeset and the code graph view; the same inputs always
//! produce the same edges.

use std::collections::BTreeMap;

use eg_types::lifecycle::{release_range, sha256_hex, LifecycleEdgeKind};
use eg_types::source_changeset::Changeset;
use serde::{Deserialize, Serialize};

/// Version of every rule in this pass; a rule change bumps it.
pub const RULE_VERSION: u32 = 1;

pub const RULE_CHANGES: &str = "changes.span_overlap";
pub const RULE_CITES: &str = "cites.trailer";
pub const RULE_IMPLEMENTS: &str = "implements.cited_change";
pub const RULE_VERIFIED: &str = "verified.binding";
pub const RULE_EXERCISES: &str = "exercises.call_graph";
pub const RULE_CONTAINS: &str = "contains.release_range";
pub const RULE_INTRODUCED: &str = "introduced.bisect";
pub const RULE_FIXED: &str = "fixed.trailer";

/// A half-open byte range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

impl Span {
    fn overlaps(self, other: Span) -> bool {
        let insertion = self.start == self.end;
        if insertion {
            other.start <= self.start && self.start < other.end
        } else {
            self.start < other.end && other.start < self.end
        }
    }
}

/// A code symbol with its byte span.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolFact {
    pub id: String,
    pub path: String,
    pub span: Span,
}

/// A test binding marker found in the tree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestBinding {
    pub requirement: String,
    pub test: String,
}

/// A call from a test to a symbol.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestCall {
    pub test: String,
    pub symbol: String,
}

/// A release head and the head of the release before it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseHead {
    pub release: String,
    pub head: String,
    pub previous_head: Option<String>,
}

/// The bisect culprit of a defect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Culprit {
    pub defect: String,
    pub changeset: String,
}

/// The code graph as one changeset sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeGraphView {
    pub symbols: Vec<SymbolFact>,
    /// Changed byte spans per path, at the parent and the new revision.
    pub changed_spans: BTreeMap<String, Vec<Span>>,
    pub on_default_branch: bool,
    pub test_bindings: Vec<TestBinding>,
    pub test_calls: Vec<TestCall>,
    /// The default-branch chain, oldest first, including this changeset.
    pub chain: Vec<String>,
    pub releases: Vec<ReleaseHead>,
    pub culprits: Vec<Culprit>,
}

/// One edge a rule derived, with its provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedEdge {
    pub kind: LifecycleEdgeKind,
    pub from: String,
    pub to: String,
    pub rule: String,
    pub rule_version: u32,
    pub input_digests: Vec<String>,
}

struct Emitter {
    changeset_digest: String,
    out: Vec<DerivedEdge>,
}

impl Emitter {
    fn emit(&mut self, kind: LifecycleEdgeKind, rule: &str, from: &str, to: &str, fact: &str) {
        self.out.push(DerivedEdge {
            kind,
            from: from.to_owned(),
            to: to.to_owned(),
            rule: rule.to_owned(),
            rule_version: RULE_VERSION,
            input_digests: vec![self.changeset_digest.clone(), sha256_hex(fact.as_bytes())],
        });
    }
}

fn changed_symbols<'a>(changeset: &Changeset, view: &'a CodeGraphView) -> Vec<&'a SymbolFact> {
    let touched = |symbol: &&SymbolFact| {
        let in_changeset = changeset
            .changed_paths
            .iter()
            .any(|p| p.path == symbol.path);
        in_changeset
            && view
                .changed_spans
                .get(&symbol.path)
                .is_some_and(|spans| spans.iter().any(|s| s.overlaps(symbol.span)))
    };
    view.symbols.iter().filter(touched).collect()
}

fn symbol_fact(symbol: &SymbolFact) -> String {
    format!(
        "symbol:{}:{}:{}-{}",
        symbol.id, symbol.path, symbol.span.start, symbol.span.end
    )
}

fn rules_over_changes(changeset: &Changeset, view: &CodeGraphView, em: &mut Emitter) {
    let changed = changed_symbols(changeset, view);
    for symbol in &changed {
        let fact = symbol_fact(symbol);
        em.emit(
            LifecycleEdgeKind::Changes,
            RULE_CHANGES,
            &changeset.id,
            &symbol.id,
            &fact,
        );
    }
    for requirement in &changeset.trailers.spec_ids {
        let fact = format!("spec:{requirement}");
        em.emit(
            LifecycleEdgeKind::CitesRequirement,
            RULE_CITES,
            &changeset.id,
            requirement,
            &fact,
        );
        if view.on_default_branch {
            for symbol in &changed {
                let fact = format!("{}|{fact}", symbol_fact(symbol));
                em.emit(
                    LifecycleEdgeKind::Implements,
                    RULE_IMPLEMENTS,
                    &symbol.id,
                    requirement,
                    &fact,
                );
            }
        }
    }
}

fn rules_over_tree(view: &CodeGraphView, em: &mut Emitter) {
    for binding in &view.test_bindings {
        let fact = format!("binding:{}:{}", binding.requirement, binding.test);
        em.emit(
            LifecycleEdgeKind::VerifiedBy,
            RULE_VERIFIED,
            &binding.requirement,
            &binding.test,
            &fact,
        );
    }
    for call in &view.test_calls {
        let fact = format!("call:{}:{}", call.test, call.symbol);
        em.emit(
            LifecycleEdgeKind::Exercises,
            RULE_EXERCISES,
            &call.test,
            &call.symbol,
            &fact,
        );
    }
}

fn rules_over_history(changeset: &Changeset, view: &CodeGraphView, em: &mut Emitter) {
    for release in &view.releases {
        let range = release_range(&view.chain, release.previous_head.as_deref(), &release.head);
        if range.is_ok_and(|ids| ids.contains(&changeset.id)) {
            let fact = format!("release:{}:{}", release.release, release.head);
            em.emit(
                LifecycleEdgeKind::Contains,
                RULE_CONTAINS,
                &release.release,
                &changeset.id,
                &fact,
            );
        }
    }
    for culprit in view.culprits.iter().filter(|c| c.changeset == changeset.id) {
        let fact = format!("culprit:{}", culprit.defect);
        em.emit(
            LifecycleEdgeKind::IntroducedBy,
            RULE_INTRODUCED,
            &culprit.defect,
            &changeset.id,
            &fact,
        );
    }
    for defect in &changeset.trailers.fixes {
        let fact = format!("fixes:{defect}");
        em.emit(
            LifecycleEdgeKind::FixedBy,
            RULE_FIXED,
            defect,
            &changeset.id,
            &fact,
        );
    }
}

/// Apply the eight named rules to one accepted changeset. The result is sorted
/// and free of duplicates, so equal inputs give byte-equal output.
pub fn derive_lifecycle_edges(changeset: &Changeset, view: &CodeGraphView) -> Vec<DerivedEdge> {
    let digest = serde_json::to_vec(changeset).unwrap_or_default();
    let mut em = Emitter {
        changeset_digest: sha256_hex(&digest),
        out: Vec::new(),
    };
    rules_over_changes(changeset, view, &mut em);
    rules_over_tree(view, &mut em);
    rules_over_history(changeset, view, &mut em);
    let mut edges = em.out;
    edges.sort_by(|a, b| (a.kind, &a.from, &a.to).cmp(&(b.kind, &b.from, &b.to)));
    edges.dedup_by(|a, b| (a.kind, &a.from, &a.to) == (b.kind, &b.from, &b.to));
    edges
}
