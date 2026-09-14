//! rules.rs — user-defined custom rules + instance-level (ABox) RL/Datalog reasoning
//! with OWL equality (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog).
//!
//! `owl.rs` is a TBox/concept-level EL⁺ completion. It reasons about CLASSES — the
//! subsumption hierarchy and class consistency — but it cannot take a user's ad-hoc
//! rule (`parent(x,y) ∧ parent(y,z) → grandparent(x,z)`) nor merge individuals under
//! `owl:FunctionalProperty` / `owl:sameAs`. Those are **ABox**, instance-level
//! inferences. This module adds the second Stardog-parity gap: a small
//! forward-chaining **Datalog / SWRL** rule engine that
//!
//!   * parses user rules in a pragmatic SWRL-ish / Datalog syntax into the SAME
//!     internal atom representation the built-in OWL-RL rules use;
//!   * derives the built-in OWL 2 RL property rules (subPropertyOf, domain, range,
//!     symmetric, inverse, property-chains/transitive, functional &
//!     inverse-functional → `owl:sameAs`, named subClassOf) directly from a parsed
//!     [`crate::owl::Ontology`];
//!   * runs custom + built-in rules in ONE forward-chaining fixpoint with
//!     **confidence propagation** (a derived fact's confidence is `rule_conf × ∏
//!     body-fact confidences`, MAX across alternative derivations — the same
//!     noisy-OR discipline the EL reasoner uses);
//!   * handles **equality**: `owl:sameAs` (asserted or derived through a functional
//!     key) merges individuals via union-find + congruence (facts of equal
//!     individuals are unified), and a `sameAs` over an `owl:differentFrom` pair is a
//!     clash → instance-level inconsistency.
//!
//! A [`RuleSet`] is registrable/listable/removable so rules can be supplied at
//! runtime; [`run_rule_reasoning`] / [`run_rule_reasoning_on_view`] are the
//! server-op-ready entry points (a thin pass-through wraps them as an op — see the
//! module note in `lib.rs`).
//!
//! ## Rule syntax
//!
//! ```text
//!   [name:] body  ->  head   [@ conf]
//! ```
//! * The implication is `->`, `=>`, `⇒`, or Datalog `:-` (with `head :- body`).
//! * `body` and `head` are conjunctions of atoms joined by `,`, `^`, `∧`, or `&`.
//! * An atom is `pred(t1, t2, …)` — unary `C(x)` is class membership, binary
//!   `r(x,y)` a property edge.
//! * A **variable** starts with `?` (e.g. `?x`) OR is a bare identifier (`x`, `y`);
//!   a **constant** is an IRI in angle brackets `<http://…>` or a quoted literal
//!   `"…"`. (So in `parent(x, <http://ex/bob>)`, `x` is a variable and the IRI a
//!   constant.)
//! * The optional `@ 0.8` suffix is the rule's confidence (default `1.0`).
//! * The optional leading `name:` registers the rule under that name (else an
//!   auto-name is assigned).
//!
//! Example: `gp: parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.9`.
//!
//! ## SWRL / RuleML atoms + built-ins (CONCEPT:EG-KG.ontology.concept-3)
//!
//! The same surface ALSO parses SWRL-style atoms — the existing forms ARE the SWRL
//! atom types: a unary `C(x)` is a SWRL **ClassAtom**, a binary `p(x,y)` over an object
//! property is an **IndividualPropertyAtom**, and a binary `p(x,v)` whose second term is
//! a literal is a **DatavaluedPropertyAtom**. The genuinely new shape is the SWRL
//! **BuiltInAtom**, written `swrlb:<name>(arg, …)` (or the full
//! `<http://www.w3.org/2003/11/swrlb#name>` IRI). A built-in atom is NOT matched against
//! stored facts — it is EVALUATED against the current binding at rule-firing time:
//!
//!   * **comparison** built-ins (`equal`, `notEqual`, `lessThan`, `lessThanOrEqual`,
//!     `greaterThan`, `greaterThanOrEqual`, plus string `contains` / `startsWith` /
//!     `endsWith`) act as FILTERS — every argument must already be bound; the binding
//!     survives only if the comparison holds;
//!   * **math** built-ins (`add`, `subtract`, `multiply`, `divide`) and **string**
//!     producers (`stringConcat`, `stringLength`, `upperCase`, `lowerCase`) follow the
//!     SWRL convention that the FIRST argument is the RESULT: if it is an unbound
//!     variable it is BOUND to the computed value, otherwise it is checked for equality.
//!
//! So `age(?p, ?a) ^ swrlb:greaterThanOrEqual(?a, 18) -> Adult(?p)` keeps only adults,
//! and `age(?p, ?a) ^ swrlb:add(?n, ?a, 1) -> nextAge(?p, ?n)` binds `?n = ?a + 1`. A
//! built-in output variable counts toward head-var range-safety (it appears in the body
//! atom), so the safety check is unchanged. Bare numeric tokens (`18`) parse as literal
//! constants so they can feed built-ins; all pre-existing rule strings parse unchanged.

use std::collections::BTreeSet;

#[cfg(feature = "rdf")]
use oxrdf::Triple;
use serde::{Deserialize, Serialize};

use crate::owl::Ontology;

mod builtins;
mod engine;
mod owl_rules;
mod syntax;

pub use owl_rules::builtin_rules;
use syntax::{iri, is_meta_class, is_schema_pred, RDF_TYPE};
pub use syntax::{Atom, RTerm, Rule, RuleSet};

// ── Top-level entry points + the server-op request/response shape ─────────────

/// The materialised result of a rule-reasoning run (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog).
#[derive(Clone, Debug, Default)]
pub struct RuleReasonResult {
    /// Every fact (asserted + derived) as `(predicate, args, confidence)`.
    pub facts: Vec<(String, Vec<String>, f64)>,
    /// Only the DERIVED (newly inferred) facts.
    pub derived: Vec<(String, Vec<String>, f64)>,
    /// `owl:sameAs` equalities `(representative, merged-individual)`.
    pub same_as: Vec<(String, String)>,
    /// Instance-level consistency (`false` ⇒ a `sameAs`/`differentFrom` clash).
    pub consistent: bool,
    /// Human-readable clash descriptions.
    pub conflicts: Vec<String>,
}

impl RuleReasonResult {
    /// Confidence of a specific ground fact, or `None` if absent.
    pub fn fact_confidence(&self, pred: &str, args: &[&str]) -> Option<f64> {
        self.facts.iter().find_map(|(p, a, c)| {
            (p == pred && a.iter().map(String::as_str).eq(args.iter().copied())).then_some(*c)
        })
    }

    /// Does the (canonicalised) fact hold?
    pub fn holds(&self, pred: &str, args: &[&str]) -> bool {
        self.fact_confidence(pred, args).is_some()
    }
}

/// Run forward-chaining reasoning over a set of ground facts + an [`Ontology`] (its
/// built-in OWL-RL rules) + a user [`RuleSet`], in ONE confidence-propagating fixpoint.
///
/// `facts` are `(predicate, args, confidence)` ground atoms; `asserted_same_as` and
/// `asserted_different_from` seed the equality/inequality relations.

pub fn reason_facts(
    ont: &Ontology,
    facts: &[(String, Vec<String>, f64)],
    custom: &RuleSet,
) -> RuleReasonResult {
    engine::reason_facts(ont, facts, custom)
}

#[cfg(feature = "rdf")]
pub fn reason_triples(triples: &[Triple], ont: &Ontology, custom: &RuleSet) -> RuleReasonResult {
    let facts = facts_from_triples(triples);
    reason_facts(ont, &facts, custom)
}

/// Extract ABox ground facts from a triple stream: a `rdf:type` triple with a non-meta
/// object becomes a unary class-membership fact `C(s)`; any other non-schema triple
/// becomes a binary edge fact `p(s,o)`. Confidence defaults to `1.0`.
#[cfg(feature = "rdf")]
pub fn facts_from_triples(triples: &[Triple]) -> Vec<(String, Vec<String>, f64)> {
    let mut out = Vec::new();
    for t in triples {
        let p = t.predicate.as_str();
        if is_schema_pred(p) {
            continue;
        }
        let s = match &t.subject {
            oxrdf::NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
            oxrdf::NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
            #[allow(unreachable_patterns)]
            _ => continue,
        };
        let o = crate::owl::term_key(&t.object);
        if p == RDF_TYPE {
            if is_meta_class(&o) {
                continue;
            }
            out.push((o, vec![s], 1.0)); // C(s)
        } else {
            out.push((format!("<{p}>"), vec![s, o], 1.0)); // p(s,o)
        }
    }
    out
}

// ── Server-op-ready request / response (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog) ───────────────────────

/// A runtime rule-reasoning request — the parameterised-rules path a server op
/// (`RunRules`, or the extended `RunDatalogReasoning`) passes straight through. The
/// engine is fully in-crate; wiring the op is a one-liner in the protocol/exec crates
/// (deferred here to keep the change inside `eg-rdf`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuleReasonRequest {
    /// Optional Turtle carrying the TBox axioms AND/OR ABox facts to reason over.
    #[serde(default)]
    pub ontology_ttl: String,
    /// User rule strings (SWRL-ish / Datalog syntax — see module docs).
    #[serde(default)]
    pub rules: Vec<String>,
    /// When set, restrict the returned facts to this predicate (IRI or bare name).
    #[serde(default)]
    pub query_predicate: Option<String>,
    /// Drop facts whose confidence is below this threshold.
    #[serde(default)]
    pub min_confidence: f64,
    /// When true, return only the DERIVED facts (omit the asserted base).
    #[serde(default)]
    pub derived_only: bool,
}

/// The rule-reasoning response and its facts -- the `RunRules` wire body, owned by eg-types.
pub use eg_types::rdf_report::{RuleFact, RuleReasonResponse};

/// Execute a [`RuleReasonRequest`]: parse the Turtle into an ontology + ABox facts,
/// register the custom rules, run the fixpoint, and project a filtered response. This
/// is the function a server `RunRules` op calls (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog).
#[cfg(feature = "rdf")]
pub fn run_rule_reasoning(req: &RuleReasonRequest) -> Result<RuleReasonResponse, String> {
    let triples = if req.ontology_ttl.trim().is_empty() {
        Vec::new()
    } else {
        crate::mapping::parse_turtle(&req.ontology_ttl)
            .map_err(|e| format!("turtle parse error: {e}"))?
    };
    let ont = crate::owl::parse_ontology(&triples);
    let mut ruleset = RuleSet::new();
    for r in &req.rules {
        ruleset.add_str(r)?;
    }
    let registered = ruleset.names();
    let result = reason_triples(&triples, &ont, &ruleset);
    Ok(project_response(result, req, registered))
}

/// Reason over a live [`eg_core::graph::GraphView`] (its folded TBox axioms + asserted
/// facts) plus runtime rules — the GraphView entry a `Reason`/`RunRules` op uses when no
/// explicit ontology document is supplied (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog).
#[cfg(feature = "rdf")]
pub fn run_rule_reasoning_on_view(
    view: &eg_core::graph::GraphView,
    req: &RuleReasonRequest,
) -> Result<RuleReasonResponse, String> {
    let mut triples = crate::owl::tbox_triples_from_view(view);
    if !req.ontology_ttl.trim().is_empty() {
        triples.extend(
            crate::mapping::parse_turtle(&req.ontology_ttl)
                .map_err(|e| format!("turtle parse error: {e}"))?,
        );
    }
    let ont = crate::owl::parse_ontology(&triples);
    let mut ruleset = RuleSet::new();
    for r in &req.rules {
        ruleset.add_str(r)?;
    }
    let registered = ruleset.names();
    let result = reason_triples(&triples, &ont, &ruleset);
    Ok(project_response(result, req, registered))
}

#[cfg(feature = "rdf")]
fn project_response(
    result: RuleReasonResult,
    req: &RuleReasonRequest,
    registered: Vec<String>,
) -> RuleReasonResponse {
    let want_pred = req.query_predicate.as_ref().map(|p| {
        if p.starts_with('<') || p.starts_with('"') {
            p.clone()
        } else if p.starts_with("http") {
            iri(p)
        } else {
            p.clone()
        }
    });
    let src: &Vec<(String, Vec<String>, f64)> = if req.derived_only {
        &result.derived
    } else {
        &result.facts
    };
    let derived_set: BTreeSet<(&String, &Vec<String>)> =
        result.derived.iter().map(|(p, a, _)| (p, a)).collect();
    let facts = src
        .iter()
        .filter(|(p, _, c)| {
            *c >= req.min_confidence && want_pred.as_ref().map(|w| w == p).unwrap_or(true)
        })
        .map(|(p, a, c)| RuleFact {
            predicate: p.clone(),
            args: a.clone(),
            confidence: *c,
            derived: derived_set.contains(&(p, a)),
        })
        .collect();
    RuleReasonResponse {
        facts,
        same_as: result.same_as,
        consistent: result.consistent,
        conflicts: result.conflicts,
        registered_rules: registered,
    }
}

#[cfg(test)]
mod tests;
