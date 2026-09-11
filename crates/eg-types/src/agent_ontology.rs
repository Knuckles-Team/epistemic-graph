//! The agent ontology EG ships with (RF-ADR-008).
//!
//! An AI context engine has to be able to answer *"what does an agent trying to
//! do XYZ need?"* — and that question is only answerable natively if the
//! vocabulary is native. A component tagged with an arbitrary string can be
//! matched by exact equality and nothing else: `web-search` would not satisfy a
//! need for `retrieval`, and every generalization would have to be re-derived by
//! a model on every query.
//!
//! So EG bakes in a controlled vocabulary with two relations:
//!
//! * **broader** — a subsumption hierarchy over capabilities and modalities, so
//!   a component providing `eg:capability/web-search` satisfies a requirement
//!   for `eg:capability/retrieval` without anything having to say so.
//! * **requires** — from a task to the capabilities that task needs, so
//!   "an agent doing research" resolves to a capability set *in the graph*
//!   rather than in a prompt.
//!
//! Together those turn the question into a traversal:
//!
//! ```text
//! task term -> required capabilities -> (subsumption) -> components that
//! provide them -> the agents/toolsets those components belong to
//! ```
//!
//! # Why a baked-in vocabulary rather than a published ontology
//!
//! Published ontologies remain supported — [`crate::agent_component`] can
//! classify against any [`crate::agent_component::AgentComponentKind::Ontology`]
//! component. This one is different in kind: it is the vocabulary EG's own
//! selection and impact queries are written against, so it has to exist before
//! any tenant has published anything, be identical across every deployment, and
//! be stable enough that a stored classification does not rot. A tenant
//! vocabulary can extend it by declaring a `broader` term from it; it cannot
//! replace it.
//!
//! The table below is deliberately small. A taxonomy nobody can hold in their
//! head gets misclassified, and a wrong classification is worse than a coarse
//! one: it makes the wrong component look applicable.

/// One term in the native vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OntologyTerm {
    pub iri: &'static str,
    pub label: &'static str,
    /// The immediately more general term, or `None` for a root.
    pub broader: Option<&'static str>,
    /// For a task term: the capabilities an agent doing it needs. Empty for
    /// capability and modality terms, which are what tasks point AT.
    pub requires: &'static [&'static str],
}

/// How deep the hierarchy may be walked before we call it malformed.
///
/// The table is a compile-time constant and
/// [`tests::the_native_ontology_is_acyclic_and_resolvable`] proves it is a
/// forest, so this bound should be unreachable. It exists because an
/// unbounded walk over a structure that is *assumed* acyclic is the same class
/// of defect as an unbounded wait: correct until the assumption is not.
const MAX_DEPTH: usize = 16;

pub const CAPABILITY_ROOT: &str = "eg:capability";
pub const TASK_ROOT: &str = "eg:task";
pub const MODALITY_ROOT: &str = "eg:modality";

/// The native vocabulary.
pub const AGENT_ONTOLOGY: &[OntologyTerm] = &[
    // ── Capabilities: what a component can DO ───────────────────────────────
    t(CAPABILITY_ROOT, "capability", None),
    t("eg:capability/retrieval", "retrieval", Some(CAPABILITY_ROOT)),
    t("eg:capability/retrieval/web-search", "web search", Some("eg:capability/retrieval")),
    t("eg:capability/retrieval/vector-search", "vector search", Some("eg:capability/retrieval")),
    t("eg:capability/retrieval/graph-query", "graph query", Some("eg:capability/retrieval")),
    t("eg:capability/retrieval/sql-query", "sql query", Some("eg:capability/retrieval")),
    t("eg:capability/retrieval/document-read", "document read", Some("eg:capability/retrieval")),
    t("eg:capability/generation", "generation", Some(CAPABILITY_ROOT)),
    t("eg:capability/generation/text", "text generation", Some("eg:capability/generation")),
    t("eg:capability/generation/code", "code generation", Some("eg:capability/generation")),
    t("eg:capability/generation/image", "image generation", Some("eg:capability/generation")),
    t("eg:capability/generation/speech", "speech synthesis", Some("eg:capability/generation")),
    t("eg:capability/analysis", "analysis", Some(CAPABILITY_ROOT)),
    t("eg:capability/analysis/summarize", "summarize", Some("eg:capability/analysis")),
    t("eg:capability/analysis/classify", "classify", Some("eg:capability/analysis")),
    t("eg:capability/analysis/extract", "extract", Some("eg:capability/analysis")),
    t("eg:capability/analysis/compare", "compare", Some("eg:capability/analysis")),
    t("eg:capability/analysis/evaluate", "evaluate", Some("eg:capability/analysis")),
    t("eg:capability/reasoning", "reasoning", Some(CAPABILITY_ROOT)),
    t("eg:capability/reasoning/plan", "plan", Some("eg:capability/reasoning")),
    t("eg:capability/reasoning/decompose", "decompose", Some("eg:capability/reasoning")),
    t("eg:capability/reasoning/verify", "verify", Some("eg:capability/reasoning")),
    t("eg:capability/reasoning/critique", "critique", Some("eg:capability/reasoning")),
    t("eg:capability/memory", "memory", Some(CAPABILITY_ROOT)),
    t("eg:capability/memory/store", "store", Some("eg:capability/memory")),
    t("eg:capability/memory/recall", "recall", Some("eg:capability/memory")),
    // `action` is the side-effecting branch. Keeping it a distinct subtree is
    // what makes "is this graph read-only?" a subsumption query rather than an
    // audit: nothing under `action` is safe to run speculatively.
    t("eg:capability/action", "action", Some(CAPABILITY_ROOT)),
    t("eg:capability/action/file-write", "file write", Some("eg:capability/action")),
    t("eg:capability/action/http-request", "http request", Some("eg:capability/action")),
    t("eg:capability/action/process-exec", "process execution", Some("eg:capability/action")),
    t("eg:capability/action/message-send", "message send", Some("eg:capability/action")),
    t("eg:capability/action/schedule", "schedule", Some("eg:capability/action")),
    // ── Modalities: what a component can handle ─────────────────────────────
    t(MODALITY_ROOT, "modality", None),
    t("eg:modality/text", "text", Some(MODALITY_ROOT)),
    t("eg:modality/image", "image", Some(MODALITY_ROOT)),
    t("eg:modality/audio", "audio", Some(MODALITY_ROOT)),
    t("eg:modality/video", "video", Some(MODALITY_ROOT)),
    t("eg:modality/structured", "structured data", Some(MODALITY_ROOT)),
    // ── Tasks: what an agent is FOR, and what that needs ────────────────────
    t(TASK_ROOT, "task", None),
    task(
        "eg:task/research",
        "research",
        &[
            "eg:capability/retrieval",
            "eg:capability/analysis/summarize",
            "eg:capability/reasoning/plan",
        ],
    ),
    task(
        "eg:task/implement",
        "implement",
        &[
            "eg:capability/generation/code",
            "eg:capability/retrieval/document-read",
            "eg:capability/action/file-write",
        ],
    ),
    task(
        "eg:task/review",
        "review",
        &[
            "eg:capability/analysis/evaluate",
            "eg:capability/reasoning/critique",
            "eg:capability/retrieval/document-read",
        ],
    ),
    task(
        "eg:task/operate",
        "operate",
        &[
            "eg:capability/action",
            "eg:capability/retrieval/graph-query",
            "eg:capability/reasoning/verify",
        ],
    ),
    task(
        "eg:task/communicate",
        "communicate",
        &[
            "eg:capability/generation/text",
            "eg:capability/analysis/summarize",
            "eg:capability/action/message-send",
        ],
    ),
];

const fn t(iri: &'static str, label: &'static str, broader: Option<&'static str>) -> OntologyTerm {
    OntologyTerm {
        iri,
        label,
        broader,
        requires: &[],
    }
}

const fn task(
    iri: &'static str,
    label: &'static str,
    requires: &'static [&'static str],
) -> OntologyTerm {
    OntologyTerm {
        iri,
        label,
        broader: Some(TASK_ROOT),
        requires,
    }
}

/// Resolve one term, or `None` if it is not in the native vocabulary.
pub fn term(iri: &str) -> Option<&'static OntologyTerm> {
    AGENT_ONTOLOGY.iter().find(|entry| entry.iri == iri)
}

pub fn is_native(iri: &str) -> bool {
    term(iri).is_some()
}

/// Whether `iri` is `ancestor` or is subsumed by it.
///
/// This is the relation that makes classification useful: a component that
/// provides `eg:capability/retrieval/web-search` satisfies a requirement for
/// `eg:capability/retrieval` without anyone having written that down.
///
/// A term outside the native vocabulary subsumes nothing but itself — a tenant
/// vocabulary extends this one by declaring a native `broader`, and until it
/// does, EG will not silently infer a relationship it was never told about.
pub fn is_a(iri: &str, ancestor: &str) -> bool {
    if iri == ancestor {
        return true;
    }
    let mut current = term(iri);
    for _ in 0..MAX_DEPTH {
        let Some(entry) = current else { return false };
        let Some(broader) = entry.broader else {
            return false;
        };
        if broader == ancestor {
            return true;
        }
        current = term(broader);
    }
    false
}

/// Every ancestor of `iri`, nearest first, excluding itself.
pub fn ancestors(iri: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    let mut current = term(iri);
    for _ in 0..MAX_DEPTH {
        let Some(entry) = current else { break };
        let Some(broader) = entry.broader else { break };
        out.push(broader);
        current = term(broader);
    }
    out
}

/// The capabilities an agent doing `task_iri` needs.
///
/// The native half of *"what does an agent trying to do XYZ need?"*: a task
/// resolves to a capability set from the graph, and those capabilities then
/// match components by subsumption. Returns empty for a term that is not a
/// task.
pub fn capabilities_for_task(task_iri: &str) -> &'static [&'static str] {
    term(task_iri).map(|entry| entry.requires).unwrap_or(&[])
}

/// Whether a component providing `provided` satisfies a need for `required`.
///
/// Directional on purpose: a component that provides the SPECIFIC
/// `web-search` satisfies a need for the GENERAL `retrieval`, but a component
/// that only claims the general `retrieval` does not satisfy a need for the
/// specific `web-search` — claiming a parent is not evidence of any particular
/// child, and treating it as such is how an optimizer picks a component that
/// cannot do the job.
pub fn satisfies(provided: &str, required: &str) -> bool {
    is_a(provided, required)
}

/// Every native term under `root`, including `root` itself.
pub fn descendants(root: &str) -> Vec<&'static str> {
    AGENT_ONTOLOGY
        .iter()
        .filter(|entry| is_a(entry.iri, root))
        .map(|entry| entry.iri)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn the_native_ontology_is_acyclic_and_resolvable() {
        // The table is hand-written, so the invariants every query below
        // assumes are proved here rather than trusted: unique IRIs, every
        // `broader` and every `requires` resolves, and no term is its own
        // ancestor.
        let mut seen = BTreeSet::new();
        for entry in AGENT_ONTOLOGY {
            assert!(seen.insert(entry.iri), "duplicate term {}", entry.iri);
            assert!(!entry.iri.is_empty() && !entry.label.is_empty());
            if let Some(broader) = entry.broader {
                assert!(is_native(broader), "{} has unresolvable broader {broader}", entry.iri);
            }
            for required in entry.requires {
                assert!(
                    is_native(required),
                    "{} requires unresolvable {required}",
                    entry.iri
                );
                assert!(
                    is_a(required, CAPABILITY_ROOT),
                    "{} requires {required}, which is not a capability",
                    entry.iri
                );
            }
        }
        for entry in AGENT_ONTOLOGY {
            // Walking must terminate strictly below the bound; reaching it
            // would mean a cycle.
            let chain = ancestors(entry.iri);
            assert!(chain.len() < MAX_DEPTH, "{} chain too deep", entry.iri);
            assert!(
                !chain.contains(&entry.iri),
                "{} is its own ancestor",
                entry.iri
            );
        }
    }

    #[test]
    fn every_term_roots_in_a_declared_root() {
        for entry in AGENT_ONTOLOGY {
            let roots = [CAPABILITY_ROOT, TASK_ROOT, MODALITY_ROOT];
            assert!(
                roots.iter().any(|root| is_a(entry.iri, root)),
                "{} is not under any declared root",
                entry.iri
            );
        }
    }

    #[test]
    fn subsumption_generalizes_but_does_not_specialize() {
        assert!(satisfies("eg:capability/retrieval/web-search", "eg:capability/retrieval"));
        assert!(satisfies("eg:capability/retrieval/web-search", CAPABILITY_ROOT));
        assert!(satisfies("eg:capability/retrieval", "eg:capability/retrieval"));
        // The direction that must NOT hold: claiming the parent is not
        // evidence of any particular child.
        assert!(!satisfies("eg:capability/retrieval", "eg:capability/retrieval/web-search"));
        // Nor across sibling branches.
        assert!(!satisfies("eg:capability/generation/code", "eg:capability/retrieval"));
    }

    #[test]
    fn a_foreign_term_subsumes_only_itself() {
        // A tenant vocabulary extends the native one by declaring a native
        // `broader`. Until it does, EG must not infer a relationship it was
        // never told about.
        assert!(satisfies("acme:capability/proprietary", "acme:capability/proprietary"));
        assert!(!satisfies("acme:capability/proprietary", CAPABILITY_ROOT));
        assert!(ancestors("acme:capability/proprietary").is_empty());
    }

    #[test]
    fn a_task_resolves_to_the_capabilities_it_needs() {
        let needs = capabilities_for_task("eg:task/research");
        assert!(!needs.is_empty());
        assert!(needs.contains(&"eg:capability/retrieval"));
        // And the whole point: a component providing the SPECIFIC capability
        // answers the task's GENERAL need.
        assert!(needs
            .iter()
            .any(|required| satisfies("eg:capability/retrieval/vector-search", required)));
    }

    #[test]
    fn a_non_task_term_needs_nothing() {
        assert!(capabilities_for_task("eg:capability/retrieval").is_empty());
        assert!(capabilities_for_task("nonsense").is_empty());
    }

    #[test]
    fn the_side_effecting_branch_is_identifiable_by_subsumption() {
        // "Is this graph read-only?" must be answerable as a query, not an
        // audit: every side-effecting capability lives under one root.
        for action in descendants("eg:capability/action") {
            assert!(satisfies(action, "eg:capability/action"), "{action}");
        }
        for safe in [
            "eg:capability/retrieval/web-search",
            "eg:capability/analysis/summarize",
            "eg:capability/reasoning/plan",
            "eg:capability/memory/recall",
        ] {
            assert!(!satisfies(safe, "eg:capability/action"), "{safe}");
        }
        // `operate` is the task that legitimately needs the action branch --
        // proving the classification is load-bearing, not decorative.
        assert!(capabilities_for_task("eg:task/operate")
            .iter()
            .any(|required| is_a(required, "eg:capability/action")));
    }

    #[test]
    fn descendants_include_the_root_and_nothing_from_a_sibling() {
        let retrieval = descendants("eg:capability/retrieval");
        assert!(retrieval.contains(&"eg:capability/retrieval"));
        assert!(retrieval.contains(&"eg:capability/retrieval/web-search"));
        assert!(!retrieval.contains(&"eg:capability/generation/code"));
    }
}
