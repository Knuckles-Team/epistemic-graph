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

use std::collections::{BTreeSet, HashMap};

#[cfg(feature = "rdf")]
use oxrdf::{Term, Triple};
use serde::{Deserialize, Serialize};

use crate::owl::Ontology;

// IRIs this module needs (declared locally — owl.rs keeps its vocab private).
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";

/// Canonical `<iri>` form (matches owl.rs / mapping node-id convention).
fn iri(s: &str) -> String {
    format!("<{s}>")
}

/// OWL/RDFS meta-classes whose `rdf:type` assertions are TBox declarations, NOT ABox
/// individual typing — skipped when loading ground facts.
fn is_meta_class(o: &str) -> bool {
    const META: &[&str] = &[
        "http://www.w3.org/2002/07/owl#Class",
        "http://www.w3.org/2002/07/owl#Restriction",
        "http://www.w3.org/2002/07/owl#ObjectProperty",
        "http://www.w3.org/2002/07/owl#DatatypeProperty",
        "http://www.w3.org/2002/07/owl#AnnotationProperty",
        "http://www.w3.org/2002/07/owl#TransitiveProperty",
        "http://www.w3.org/2002/07/owl#SymmetricProperty",
        "http://www.w3.org/2002/07/owl#FunctionalProperty",
        "http://www.w3.org/2002/07/owl#InverseFunctionalProperty",
        "http://www.w3.org/2002/07/owl#Ontology",
        "http://www.w3.org/2002/07/owl#NamedIndividual",
    ];
    let bare = o.trim_start_matches('<').trim_end_matches('>');
    META.contains(&bare)
}

/// Schema predicates whose triples define the TBox, not ABox facts — skipped when
/// loading ground facts (the rule engine consumes them via the built-in rules instead).
fn is_schema_pred(p: &str) -> bool {
    const SCHEMA: &[&str] = &[
        "http://www.w3.org/2000/01/rdf-schema#subClassOf",
        "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
        "http://www.w3.org/2000/01/rdf-schema#domain",
        "http://www.w3.org/2000/01/rdf-schema#range",
        "http://www.w3.org/2002/07/owl#equivalentClass",
        "http://www.w3.org/2002/07/owl#equivalentProperty",
        "http://www.w3.org/2002/07/owl#propertyChainAxiom",
        "http://www.w3.org/2002/07/owl#inverseOf",
        "http://www.w3.org/2002/07/owl#disjointWith",
        "http://www.w3.org/2002/07/owl#intersectionOf",
        "http://www.w3.org/2002/07/owl#unionOf",
        "http://www.w3.org/2002/07/owl#onProperty",
        "http://www.w3.org/2002/07/owl#someValuesFrom",
        "http://www.w3.org/2002/07/owl#allValuesFrom",
        "http://www.w3.org/2002/07/owl#hasValue",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
        "http://epistemic-graph/owl#confidence",
    ];
    SCHEMA.contains(&p)
}

/// Local name of a predicate id (after the last `#` or `/`, sans angle brackets).
fn local_name(p: &str) -> &str {
    let bare = p.trim_start_matches('<').trim_end_matches('>');
    bare.rsplit(['#', '/'])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(bare)
}

/// Whether a rule's body predicate matches a stored fact predicate. An exact match
/// always works; additionally a BARE rule predicate (e.g. `parent`) matches any IRI
/// fact predicate with that local name (e.g. `<http://ex/parent>`), so a user can write
/// rules in short form against graph-loaded IRI facts (EG-021).
fn pred_matches(rule_pred: &str, fact_pred: &str) -> bool {
    if rule_pred == fact_pred {
        return true;
    }
    if rule_pred.starts_with('<') {
        return false; // an explicit IRI rule predicate must match exactly
    }
    local_name(fact_pred) == rule_pred
}

/// True when `pred` is the `owl:sameAs` equality predicate (IRI or short alias).
fn is_same_as(pred: &str) -> bool {
    let bare = pred.trim_start_matches('<').trim_end_matches('>');
    bare == OWL_SAME_AS || pred == "sameAs" || pred == "owl:sameAs"
}

// ── Rule AST ─────────────────────────────────────────────────────────────────

/// A term in a rule atom: a variable (bound during the join) or a ground constant.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RTerm {
    Var(String),
    Const(String),
}

/// An atom `pred(t1, t2, …)`. Unary ⇒ class membership; binary ⇒ a property edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Atom {
    pub pred: String,
    pub args: Vec<RTerm>,
}

/// A Horn rule `IF body THEN head`. `head` may be a conjunction (each head atom is
/// derived). `conf ∈ [0,1]` is the rule's own confidence.
#[derive(Clone, Debug, PartialEq)]
pub struct Rule {
    pub name: String,
    pub body: Vec<Atom>,
    pub head: Vec<Atom>,
    pub conf: f64,
}

impl Rule {
    /// Parse one rule from the pragmatic SWRL-ish / Datalog syntax (see module docs).
    pub fn parse(src: &str) -> Result<Rule, String> {
        let mut s = src.trim().to_string();
        if s.is_empty() {
            return Err("empty rule".into());
        }
        // Optional trailing confidence `@0.8`.
        let mut conf = 1.0;
        if let Some(pos) = s.rfind('@') {
            // Only treat as a confidence suffix if what follows parses as a float.
            let tail = s[pos + 1..].trim();
            if let Ok(c) = tail.parse::<f64>() {
                conf = c.clamp(0.0, 1.0);
                s.truncate(pos);
                s = s.trim().to_string();
            }
        }
        // Optional leading `name:` — but NOT the `:-` Datalog operator and not an IRI
        // (which contains `:`). The name is a bare identifier before the FIRST `:` that
        // is not part of `:-`.
        let mut name = String::new();
        if let Some(colon) = s.find(':') {
            let is_datalog = s[colon..].starts_with(":-");
            let head_part = &s[..colon];
            let looks_like_name = !head_part.contains('(')
                && !head_part.contains('<')
                && !head_part.trim().is_empty();
            if !is_datalog && looks_like_name {
                name = head_part.trim().to_string();
                s = s[colon + 1..].trim().to_string();
            }
        }
        // Split on the implication operator.
        let (body_src, head_src, reversed) = split_implication(&s)?;
        let body_atoms = parse_atom_list(body_src)?;
        let head_atoms = parse_atom_list(head_src)?;
        let (body, head) = if reversed {
            (head_atoms, body_atoms) // `head :- body`
        } else {
            (body_atoms, head_atoms)
        };
        if head.is_empty() {
            return Err(format!("rule has no head: {src}"));
        }
        if name.is_empty() {
            name = default_rule_name(&head);
        }
        // Safety: every head variable must appear in the body (range-restricted).
        let body_vars: BTreeSet<&String> = body
            .iter()
            .flat_map(|a| a.args.iter())
            .filter_map(|t| match t {
                RTerm::Var(v) => Some(v),
                _ => None,
            })
            .collect();
        for h in &head {
            for t in &h.args {
                if let RTerm::Var(v) = t {
                    if !body_vars.contains(v) {
                        return Err(format!("unsafe rule: head var ?{v} not in body: {src}"));
                    }
                }
            }
        }
        Ok(Rule {
            name,
            body,
            head,
            conf,
        })
    }
}

/// Split a rule string on its implication operator, returning `(body, head, reversed)`
/// where `reversed` is set for the Datalog `head :- body` form.
fn split_implication(s: &str) -> Result<(&str, &str, bool), String> {
    for (op, reversed) in [("->", false), ("=>", false), ("⇒", false), (":-", true)] {
        if let Some(pos) = s.find(op) {
            let left = s[..pos].trim();
            let right = s[pos + op.len()..].trim();
            return Ok((left, right, reversed));
        }
    }
    Err(format!("rule has no implication (-> / => / :-): {s}"))
}

/// Parse a conjunction of atoms separated by `,` `^` `∧` `&` (commas INSIDE an atom's
/// argument parens are not separators).
fn parse_atom_list(src: &str) -> Result<Vec<Atom>, String> {
    let mut atoms = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for c in src.chars() {
        match c {
            '(' => {
                depth += 1;
                cur.push(c);
            }
            ')' => {
                depth -= 1;
                cur.push(c);
            }
            // separators only at top level
            ',' | '^' | '&' if depth == 0 => {
                if !cur.trim().is_empty() {
                    atoms.push(parse_atom(cur.trim())?);
                }
                cur.clear();
            }
            // '∧' is multi-byte; handle via match on char
            '∧' if depth == 0 => {
                if !cur.trim().is_empty() {
                    atoms.push(parse_atom(cur.trim())?);
                }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        atoms.push(parse_atom(cur.trim())?);
    }
    Ok(atoms)
}

/// Parse a single atom `pred(t1, t2, …)`.
fn parse_atom(src: &str) -> Result<Atom, String> {
    let open = src
        .find('(')
        .ok_or_else(|| format!("atom missing '(': {src}"))?;
    if !src.ends_with(')') {
        return Err(format!("atom missing ')': {src}"));
    }
    let pred_raw = src[..open].trim();
    let pred = normalize_pred(pred_raw);
    let inner = &src[open + 1..src.len() - 1];
    let args = inner
        .split(',')
        .map(|a| parse_term(a.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    if args.is_empty() {
        return Err(format!("atom has no arguments: {src}"));
    }
    Ok(Atom { pred, args })
}

/// Normalise a predicate name: an IRI `<...>` stays as-is; a bare name is kept verbatim
/// (rules over bare predicate names are fine — they just must match the fact predicate
/// names, which for graph-loaded facts are canonical `<iri>`s).
fn normalize_pred(p: &str) -> String {
    p.to_string()
}

/// Parse a term: `?x` or a bare identifier ⇒ variable; `<iri>` or `"literal"` ⇒ const.
fn parse_term(t: &str) -> Result<RTerm, String> {
    if t.is_empty() {
        return Err("empty term".into());
    }
    if let Some(v) = t.strip_prefix('?') {
        return Ok(RTerm::Var(v.to_string()));
    }
    if t.starts_with('<') && t.ends_with('>') {
        return Ok(RTerm::Const(t.to_string()));
    }
    if t.starts_with('"') {
        return Ok(RTerm::Const(t.trim_matches('"').to_string()));
    }
    // A bare numeric token is a literal constant (CONCEPT:EG-KG.ontology.concept-3): so a SWRL built-in
    // argument can be written `swrlb:greaterThan(?age, 18)` without quoting. Without this
    // `18` would parse as an (unbindable) variable named "18". Non-numeric bare tokens
    // keep the existing variable semantics, so all pre-existing rules parse unchanged.
    if t.parse::<f64>().is_ok() {
        return Ok(RTerm::Const(t.to_string()));
    }
    // A bare identifier is a variable (e.g. `x`, `y`, `z`).
    Ok(RTerm::Var(t.to_string()))
}

fn default_rule_name(head: &[Atom]) -> String {
    head.first()
        .map(|a| {
            format!(
                "rule:{}",
                a.pred.trim_start_matches('<').trim_end_matches('>')
            )
        })
        .unwrap_or_else(|| "rule".into())
}

// ── RuleSet (registration surface) ───────────────────────────────────────────

/// A registrable set of user rules. Supports add / list / remove so rules can be
/// supplied, inspected, and retracted at runtime.
#[derive(Clone, Debug, Default)]
pub struct RuleSet {
    rules: Vec<Rule>,
}

impl RuleSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a pre-built rule (replacing any existing rule of the same name).
    pub fn register(&mut self, rule: Rule) {
        self.remove(&rule.name);
        self.rules.push(rule);
    }

    /// Parse + register one rule from source. Returns its (possibly auto-) name.
    pub fn add_str(&mut self, src: &str) -> Result<String, String> {
        let rule = Rule::parse(src)?;
        let name = rule.name.clone();
        self.register(rule);
        Ok(name)
    }

    /// Parse + register many rules — one per line or `.`-terminated statement; blank
    /// lines and `//`/`#` comments are skipped. Returns each registered name.
    pub fn add_many(&mut self, text: &str) -> Result<Vec<String>, String> {
        let mut names = Vec::new();
        for raw in text.split(['\n', '.']) {
            let line = raw.trim();
            if line.is_empty() || line.starts_with("//") || line.starts_with('#') {
                continue;
            }
            names.push(self.add_str(line)?);
        }
        Ok(names)
    }

    /// Remove a rule by name; returns whether one was removed.
    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.rules.len();
        self.rules.retain(|r| r.name != name);
        self.rules.len() != before
    }

    /// The registered rule names (registration order).
    pub fn names(&self) -> Vec<String> {
        self.rules.iter().map(|r| r.name.clone()).collect()
    }

    pub fn rules(&self) -> &[Rule] {
        &self.rules
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }
}

// ── SWRL built-in library (CONCEPT:EG-KG.ontology.concept-3) ───────────────────────────────────

/// The SWRL built-ins namespace.
const SWRLB_NS: &str = "http://www.w3.org/2003/11/swrlb#";

/// If `pred` names a SWRL built-in (`swrlb:name` short form OR the full
/// `<http://www.w3.org/2003/11/swrlb#name>` IRI), return its bare `name`; else `None`.
/// Gating on the `swrlb` prefix/IRI keeps recognition disjoint from ordinary fact
/// predicates, so non-built-in atoms are matched against facts exactly as before.
fn swrl_builtin_name(pred: &str) -> Option<&str> {
    if let Some(n) = pred.strip_prefix("swrlb:") {
        return Some(n);
    }
    let bare = pred.trim_start_matches('<').trim_end_matches('>');
    bare.strip_prefix(SWRLB_NS)
}

/// Evaluate a SWRL built-in atom against the current `binding` (CONCEPT:EG-KG.ontology.concept-3).
///
/// Returns `None` when the built-in does NOT hold for this binding (the join path dies);
/// `Some(extra)` when it holds, where `extra` carries any output variable the built-in
/// freshly bound (the SWRL convention: math / string producers write their first
/// argument). Comparison built-ins return `Some(vec![])`. An unknown built-in name or a
/// missing/unbound required argument yields `None` (sound — it simply never fires).
fn eval_builtin(
    name: &str,
    args: &[RTerm],
    binding: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    // Resolve the i-th argument to its current string value (None ⇒ unbound variable).
    let val = |i: usize| -> Option<String> {
        args.get(i).and_then(|a| match a {
            RTerm::Const(c) => Some(c.clone()),
            RTerm::Var(v) => binding.get(v).cloned(),
        })
    };
    let num = |i: usize| -> Option<f64> { val(i).and_then(|s| s.parse::<f64>().ok()) };

    match name {
        // ── comparisons (filters; every argument must be bound) ──────────────
        "equal" | "notEqual" => {
            let (a, b) = (val(0)?, val(1)?);
            // Numeric when BOTH parse as numbers (so 18 == 18.0), else string equality.
            let eq = match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => (x - y).abs() < 1e-9,
                _ => a == b,
            };
            ((name == "equal") == eq).then(Vec::new)
        }
        "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" => {
            let (a, b) = (num(0)?, num(1)?);
            let ok = match name {
                "lessThan" => a < b,
                "lessThanOrEqual" => a <= b,
                "greaterThan" => a > b,
                _ => a >= b,
            };
            ok.then(Vec::new)
        }
        "contains" | "startsWith" | "endsWith" => {
            let (a, b) = (val(0)?, val(1)?);
            let ok = match name {
                "contains" => a.contains(&b),
                "startsWith" => a.starts_with(&b),
                _ => a.ends_with(&b),
            };
            ok.then(Vec::new)
        }
        // ── math (first argument = result) ───────────────────────────────────
        "add" | "subtract" | "multiply" | "divide" => {
            // Inputs are args[1..]; all must be bound + numeric.
            let inputs: Vec<f64> = (1..args.len()).map(num).collect::<Option<_>>()?;
            if inputs.is_empty() {
                return None;
            }
            let result = match name {
                "add" => inputs.iter().sum(),
                "multiply" => inputs.iter().product(),
                "subtract" => {
                    if inputs.len() != 2 {
                        return None;
                    }
                    inputs[0] - inputs[1]
                }
                _ => {
                    if inputs.len() != 2 || inputs[1] == 0.0 {
                        return None;
                    }
                    inputs[0] / inputs[1]
                }
            };
            bind_or_check_num(args.first()?, result, binding)
        }
        // ── string producers (first argument = result) ──────────────────────
        "stringConcat" => {
            let parts: Vec<String> = (1..args.len()).map(val).collect::<Option<_>>()?;
            bind_or_check_str(args.first()?, parts.concat(), binding)
        }
        "stringLength" => {
            let s = val(1)?;
            bind_or_check_num(args.first()?, s.chars().count() as f64, binding)
        }
        "upperCase" => bind_or_check_str(args.first()?, val(1)?.to_uppercase(), binding),
        "lowerCase" => bind_or_check_str(args.first()?, val(1)?.to_lowercase(), binding),
        _ => None,
    }
}

/// Bind a producer built-in's result `out` to a numeric `value` (when an unbound
/// variable), or verify equality (numeric) when it is already bound / a constant.
fn bind_or_check_num(
    out: &RTerm,
    value: f64,
    binding: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    let check = |s: &str| matches!(s.parse::<f64>(), Ok(b) if (b - value).abs() < 1e-9);
    match out {
        RTerm::Var(v) => match binding.get(v) {
            None => Some(vec![(v.clone(), fmt_num(value))]),
            Some(bound) => check(bound).then(Vec::new),
        },
        RTerm::Const(c) => check(c).then(Vec::new),
    }
}

/// Bind a producer built-in's result `out` to a string `value`, or verify equality.
fn bind_or_check_str(
    out: &RTerm,
    value: String,
    binding: &HashMap<String, String>,
) -> Option<Vec<(String, String)>> {
    match out {
        RTerm::Var(v) => match binding.get(v) {
            None => Some(vec![(v.clone(), value)]),
            Some(bound) => (bound == &value).then(Vec::new),
        },
        RTerm::Const(c) => (c == &value).then(Vec::new),
    }
}

/// Render a built-in numeric result: an integral value as a plain integer (`19`, not
/// `19.0`) so it round-trips with literal facts; otherwise the default float form.
fn fmt_num(f: f64) -> String {
    if f.is_finite() && f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        format!("{f}")
    }
}

// ── The fact base + forward-chaining engine ──────────────────────────────────

/// A ground fact key: `(predicate, canonical-args)`.
type FactKey = (String, Vec<String>);

/// The forward-chaining engine state: the ground-fact base, per-fact confidence, the
/// `owl:sameAs` union-find, and the derivation bookkeeping.
#[derive(Default)]
struct Engine {
    /// pred → set of canonical argument tuples.
    by_pred: HashMap<String, BTreeSet<Vec<String>>>,
    /// per-fact confidence in `[0,1]`.
    conf: HashMap<FactKey, f64>,
    /// facts that were DERIVED (not asserted) — for the result's `derived` set.
    derived: BTreeSet<FactKey>,
    /// the rule label that first derived each fact.
    rule_of: HashMap<FactKey, String>,
    /// union-find parent map for `owl:sameAs` equality.
    uf: HashMap<String, String>,
    /// derived/asserted `sameAs` pairs (canonical reps).
    same_pairs: BTreeSet<(String, String)>,
    /// asserted `owl:differentFrom` pairs.
    diff_pairs: Vec<(String, String)>,
    /// instance-level clashes (a `sameAs` over a `differentFrom` pair).
    conflicts: Vec<String>,
}

impl Engine {
    /// Equality representative of `x` (read-only union-find find, no compression).
    fn rep(&self, x: &str) -> String {
        let mut cur = x.to_string();
        let mut guard = 0;
        while let Some(p) = self.uf.get(&cur) {
            if p == &cur || guard > 10_000 {
                break;
            }
            cur = p.clone();
            guard += 1;
        }
        cur
    }

    /// Union two individuals under `owl:sameAs`. Returns whether anything merged. The
    /// lexicographically smaller id is kept as the representative (stable output).
    fn union(&mut self, a: &str, b: &str) -> bool {
        let (ra, rb) = (self.rep(a), self.rep(b));
        if ra == rb {
            return false;
        }
        let (root, child) = if ra <= rb { (ra, rb) } else { (rb, ra) };
        self.uf.insert(child.clone(), root.clone());
        self.same_pairs.insert((root, child));
        true
    }

    /// Add (or raise the confidence of) a ground fact. Returns whether membership was
    /// added OR the confidence rose. `derived` marks an inferred (vs asserted) fact.
    fn add_fact(
        &mut self,
        pred: &str,
        args: &[String],
        conf: f64,
        label: &str,
        derived: bool,
    ) -> bool {
        let cargs: Vec<String> = args.iter().map(|a| self.rep(a)).collect();
        let key = (pred.to_string(), cargs.clone());
        let added = self
            .by_pred
            .entry(pred.to_string())
            .or_default()
            .insert(cargs);
        let prev = self.conf.get(&key).copied().unwrap_or(0.0);
        let combined = prev.max(conf.clamp(0.0, 1.0));
        let raised = combined > prev + 1e-9;
        if added || raised {
            self.conf.insert(key.clone(), combined);
        }
        if added {
            if derived {
                self.derived.insert(key.clone());
            }
            self.rule_of.entry(key).or_insert_with(|| label.to_string());
        }
        added || raised
    }

    /// Re-canonicalise every fact through the current union-find (congruence closure):
    /// facts of now-equal individuals collapse onto their representative, MAX-merging
    /// confidence. Run after a round that performed any union so subsequent rounds see
    /// the merged ABox.
    fn canonicalize(&mut self) {
        let mut new_by_pred: HashMap<String, BTreeSet<Vec<String>>> = HashMap::new();
        let mut new_conf: HashMap<FactKey, f64> = HashMap::new();
        let mut new_derived: BTreeSet<FactKey> = BTreeSet::new();
        let mut new_rule_of: HashMap<FactKey, String> = HashMap::new();
        for (pred, tuples) in &self.by_pred {
            for t in tuples {
                let ct: Vec<String> = t.iter().map(|a| self.rep(a)).collect();
                let old_key = (pred.clone(), t.clone());
                let new_key = (pred.clone(), ct.clone());
                new_by_pred.entry(pred.clone()).or_default().insert(ct);
                let c = self.conf.get(&old_key).copied().unwrap_or(1.0);
                let slot = new_conf.entry(new_key.clone()).or_insert(0.0);
                if c > *slot {
                    *slot = c;
                }
                if self.derived.contains(&old_key) {
                    new_derived.insert(new_key.clone());
                }
                if let Some(lbl) = self.rule_of.get(&old_key) {
                    new_rule_of.entry(new_key).or_insert_with(|| lbl.clone());
                }
            }
        }
        self.by_pred = new_by_pred;
        self.conf = new_conf;
        self.derived = new_derived;
        self.rule_of = new_rule_of;
    }

    /// Detect clashes: a `differentFrom` pair forced equal is an instance inconsistency.
    fn check_conflicts(&mut self) {
        self.conflicts.clear();
        for (a, b) in &self.diff_pairs {
            if self.rep(a) == self.rep(b) {
                self.conflicts.push(format!(
                    "owl:sameAs derived over owl:differentFrom pair: {a} = {b}"
                ));
            }
        }
    }

    /// Evaluate a rule body, producing every satisfying `(binding, body-confidence)`.
    fn eval_body(&self, body: &[Atom]) -> Vec<(HashMap<String, String>, f64)> {
        let mut out = Vec::new();
        self.eval_at(body, 0, &mut HashMap::new(), 1.0, &mut out);
        out
    }

    fn eval_at(
        &self,
        body: &[Atom],
        idx: usize,
        binding: &mut HashMap<String, String>,
        conf_acc: f64,
        out: &mut Vec<(HashMap<String, String>, f64)>,
    ) {
        if idx == body.len() {
            out.push((binding.clone(), conf_acc));
            return;
        }
        let atom = &body[idx];
        // SWRL built-in atom (CONCEPT:EG-KG.ontology.concept-3): evaluated against the current binding
        // rather than matched against stored facts. A comparison acts as a filter; a
        // math / string producer binds its (first-argument) result variable. Built-ins
        // are deterministic constraints (confidence 1.0, so `conf_acc` is unchanged).
        if let Some(bn) = swrl_builtin_name(&atom.pred) {
            if let Some(extra) = eval_builtin(bn, &atom.args, binding) {
                let mut newly_bound: Vec<String> = Vec::new();
                for (v, val) in extra {
                    binding.insert(v.clone(), val);
                    newly_bound.push(v);
                }
                self.eval_at(body, idx + 1, binding, conf_acc, out);
                for v in newly_bound {
                    binding.remove(&v);
                }
            }
            return;
        }
        // Gather every stored predicate this body atom matches (exact, or a bare rule
        // predicate matching an IRI fact predicate by local name).
        let matched: Vec<&String> = self
            .by_pred
            .keys()
            .filter(|fp| pred_matches(&atom.pred, fp))
            .collect();
        for fp in matched {
            let Some(tuples) = self.by_pred.get(fp) else {
                continue;
            };
            for tuple in tuples {
                if tuple.len() != atom.args.len() {
                    continue;
                }
                let mut newly_bound: Vec<String> = Vec::new();
                let mut ok = true;
                for (arg, val) in atom.args.iter().zip(tuple.iter()) {
                    match arg {
                        RTerm::Const(c) => {
                            if self.rep(c) != *val {
                                ok = false;
                                break;
                            }
                        }
                        RTerm::Var(v) => match binding.get(v) {
                            Some(bound) => {
                                if bound != val {
                                    ok = false;
                                    break;
                                }
                            }
                            None => {
                                binding.insert(v.clone(), val.clone());
                                newly_bound.push(v.clone());
                            }
                        },
                    }
                }
                if ok {
                    let fconf = self
                        .conf
                        .get(&(fp.clone(), tuple.clone()))
                        .copied()
                        .unwrap_or(1.0);
                    self.eval_at(body, idx + 1, binding, conf_acc * fconf, out);
                }
                for v in newly_bound {
                    binding.remove(&v);
                }
            }
        }
    }

    /// Apply one rule, deriving its head facts; returns whether anything changed.
    fn apply_rule(&mut self, rule: &Rule) -> bool {
        let mut changed = false;
        let solutions = self.eval_body(&rule.body);
        for (binding, body_conf) in solutions {
            let conf = (rule.conf * body_conf).clamp(0.0, 1.0);
            for head in &rule.head {
                // Instantiate the head atom; every head var is range-restricted so bound.
                let mut args = Vec::with_capacity(head.args.len());
                let mut ok = true;
                for t in &head.args {
                    match t {
                        RTerm::Const(c) => args.push(self.rep(c)),
                        RTerm::Var(v) => match binding.get(v) {
                            Some(b) => args.push(b.clone()),
                            None => {
                                ok = false;
                                break;
                            }
                        },
                    }
                }
                if !ok {
                    continue;
                }
                if is_same_as(&head.pred) && args.len() == 2 {
                    if self.union(&args[0], &args[1]) {
                        changed = true;
                    }
                } else if self.add_fact(&head.pred, &args, conf, &rule.name, true) {
                    changed = true;
                }
            }
        }
        changed
    }

    /// Run all rules to a fixpoint with congruence re-canonicalisation between rounds.
    fn run(&mut self, rules: &[Rule]) {
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 10_000 {
                break;
            }
            let mut changed = false;
            let pre_unions = self.same_pairs.len();
            for rule in rules {
                if self.apply_rule(rule) {
                    changed = true;
                }
            }
            if self.same_pairs.len() != pre_unions {
                self.canonicalize();
                changed = true;
            }
            if !changed {
                break;
            }
        }
        self.check_conflicts();
    }
}

// ── Built-in OWL 2 RL rules derived from a parsed Ontology ────────────────────

/// Build the instance-level (ABox) OWL 2 RL rules implied by a parsed [`Ontology`]:
/// subClassOf, subPropertyOf, domain, range, symmetric, inverse, property-chains /
/// transitive, and functional / inverse-functional → `owl:sameAs`. These run in the
/// SAME fixpoint as the user's custom rules, so a custom rule can build on (and feed)
/// OWL-inferred facts (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog).
pub fn builtin_rules(ont: &Ontology) -> Vec<Rule> {
    let x = || RTerm::Var("x".into());
    let y = || RTerm::Var("y".into());
    let z = || RTerm::Var("z".into());
    let mut rules: Vec<Rule> = Vec::new();
    let mut n = 0usize;
    let mut named = |prefix: &str| {
        n += 1;
        format!("builtin:{prefix}:{n}")
    };

    // Named subClassOf (single + conjunctive LHS, named RHS): B(x) [^ …] -> C(x).
    for g in &ont.gcis {
        use crate::owl::Concept;
        let rhs = match &g.rhs {
            Concept::Named(c) => c.clone(),
            Concept::Some(..) => continue, // existential ⇒ TBox EL completion, not ABox
        };
        let mut body = Vec::new();
        let mut all_named = true;
        for c in &g.lhs {
            match c {
                Concept::Named(b) => body.push(Atom {
                    pred: b.clone(),
                    args: vec![x()],
                }),
                Concept::Some(..) => {
                    all_named = false;
                    break;
                }
            }
        }
        if all_named && !body.is_empty() {
            rules.push(Rule {
                name: named("subclass"),
                body,
                head: vec![Atom {
                    pred: rhs,
                    args: vec![x()],
                }],
                conf: g.conf,
            });
        }
    }

    // subPropertyOf (incl. equivalentProperty's two directions): r(x,y) -> s(x,y).
    for (r, s, _label, conf) in &ont.sub_roles {
        rules.push(Rule {
            name: named("subprop"),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![x(), y()],
            }],
            head: vec![Atom {
                pred: s.clone(),
                args: vec![x(), y()],
            }],
            conf: *conf,
        });
    }

    // domain(r,D): r(x,y) -> D(x).  range(r,D): r(x,y) -> D(y).
    for (r, d) in &ont.domains {
        rules.push(Rule {
            name: named("domain"),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![x(), y()],
            }],
            head: vec![Atom {
                pred: d.clone(),
                args: vec![x()],
            }],
            conf: 1.0,
        });
    }
    for (r, d) in &ont.ranges {
        rules.push(Rule {
            name: named("range"),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![x(), y()],
            }],
            head: vec![Atom {
                pred: d.clone(),
                args: vec![y()],
            }],
            conf: 1.0,
        });
    }

    // symmetric r: r(x,y) -> r(y,x).
    for r in &ont.symmetric {
        rules.push(Rule {
            name: named("symmetric"),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![x(), y()],
            }],
            head: vec![Atom {
                pred: r.clone(),
                args: vec![y(), x()],
            }],
            conf: 1.0,
        });
    }

    // inverse (p1,p2): p1(x,y) -> p2(y,x) and p2(x,y) -> p1(y,x).
    for (p1, p2) in &ont.inverses {
        rules.push(Rule {
            name: named("inverse"),
            body: vec![Atom {
                pred: p1.clone(),
                args: vec![x(), y()],
            }],
            head: vec![Atom {
                pred: p2.clone(),
                args: vec![y(), x()],
            }],
            conf: 1.0,
        });
        rules.push(Rule {
            name: named("inverse"),
            body: vec![Atom {
                pred: p2.clone(),
                args: vec![x(), y()],
            }],
            head: vec![Atom {
                pred: p1.clone(),
                args: vec![y(), x()],
            }],
            conf: 1.0,
        });
    }

    // property chains (covers transitive r∘r⊑r): r1(x,y) ^ r2(y,z) -> s(x,z).
    for ch in &ont.chains {
        if ch.chain.len() == 2 {
            rules.push(Rule {
                name: named("chain"),
                body: vec![
                    Atom {
                        pred: ch.chain[0].clone(),
                        args: vec![x(), y()],
                    },
                    Atom {
                        pred: ch.chain[1].clone(),
                        args: vec![y(), z()],
                    },
                ],
                head: vec![Atom {
                    pred: ch.sup.clone(),
                    args: vec![x(), z()],
                }],
                conf: ch.conf,
            });
        }
    }

    // FunctionalProperty r: r(x,y) ^ r(x,z) -> sameAs(y,z).
    for r in &ont.functional {
        rules.push(Rule {
            name: named("functional"),
            body: vec![
                Atom {
                    pred: r.clone(),
                    args: vec![x(), y()],
                },
                Atom {
                    pred: r.clone(),
                    args: vec![x(), z()],
                },
            ],
            head: vec![Atom {
                pred: iri(OWL_SAME_AS),
                args: vec![y(), z()],
            }],
            conf: 1.0,
        });
    }
    // InverseFunctionalProperty r: r(y,x) ^ r(z,x) -> sameAs(y,z).
    for r in &ont.inverse_functional {
        rules.push(Rule {
            name: named("inverse-functional"),
            body: vec![
                Atom {
                    pred: r.clone(),
                    args: vec![y(), x()],
                },
                Atom {
                    pred: r.clone(),
                    args: vec![z(), x()],
                },
            ],
            head: vec![Atom {
                pred: iri(OWL_SAME_AS),
                args: vec![y(), z()],
            }],
            conf: 1.0,
        });
    }

    rules
}

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
    let mut eng = Engine::default();
    for (pred, args, conf) in facts {
        eng.add_fact(pred, args, *conf, "asserted", false);
    }
    for (a, b) in &ont.same_as {
        eng.union(a, b);
    }
    eng.diff_pairs.extend(ont.different_from.iter().cloned());
    if !ont.same_as.is_empty() {
        eng.canonicalize();
    }

    let mut all_rules = builtin_rules(ont);
    all_rules.extend(custom.rules().iter().cloned());
    eng.run(&all_rules);

    // Project.
    let mut facts_out: Vec<(String, Vec<String>, f64)> = Vec::new();
    let mut derived_out: Vec<(String, Vec<String>, f64)> = Vec::new();
    for (pred, tuples) in &eng.by_pred {
        for t in tuples {
            let key = (pred.clone(), t.clone());
            let c = eng.conf.get(&key).copied().unwrap_or(1.0);
            facts_out.push((pred.clone(), t.clone(), c));
            if eng.derived.contains(&key) {
                derived_out.push((pred.clone(), t.clone(), c));
            }
        }
    }
    facts_out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    derived_out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let same_as: Vec<(String, String)> = eng.same_pairs.iter().cloned().collect();
    RuleReasonResult {
        facts: facts_out,
        derived: derived_out,
        same_as,
        consistent: eng.conflicts.is_empty(),
        conflicts: eng.conflicts,
    }
}

/// Convenience: load ground facts out of an RDF triple stream (skipping TBox schema
/// triples — those drive the built-in rules) and reason with `ont` + `custom`.
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
    fn term_id(t: &Term) -> String {
        match t {
            Term::NamedNode(n) => format!("<{}>", n.as_str()),
            Term::BlankNode(b) => format!("_:{}", b.as_str()),
            Term::Literal(l) => l.value().to_string(),
            #[allow(unreachable_patterns)]
            _ => String::new(),
        }
    }
    let mut out = Vec::new();
    for t in triples {
        let p = t.predicate.as_str();
        if is_schema_pred(p) {
            continue;
        }
        let s = match &t.subject {
            oxrdf::Subject::NamedNode(n) => format!("<{}>", n.as_str()),
            oxrdf::Subject::BlankNode(b) => format!("_:{}", b.as_str()),
            #[allow(unreachable_patterns)]
            _ => continue,
        };
        let o = term_id(&t.object);
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

/// One fact in a [`RuleReasonResponse`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuleFact {
    pub predicate: String,
    pub args: Vec<String>,
    pub confidence: f64,
    pub derived: bool,
}

/// The serialisable rule-reasoning response.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RuleReasonResponse {
    pub facts: Vec<RuleFact>,
    pub same_as: Vec<(String, String)>,
    pub consistent: bool,
    pub conflicts: Vec<String>,
    pub registered_rules: Vec<String>,
}

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
mod tests {
    use super::*;
    use crate::mapping::parse_turtle;

    fn c(s: &str) -> String {
        format!("<http://ex/{s}>")
    }

    /// A custom rule `parent(x,y) ∧ parent(y,z) → grandparent(x,z)` fires and the
    /// inferred fact carries propagated confidence (rule_conf × body-fact confs).
    #[test]
    fn custom_grandparent_rule_with_confidence() {
        let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:parent ex:bob .
ex:bob   ex:parent ex:carol .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let mut rs = RuleSet::new();
        let name = rs
            .add_str("gp: parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.9")
            .unwrap();
        assert_eq!(name, "gp");
        assert_eq!(rs.names(), vec!["gp".to_string()]);

        let res = reason_triples(&triples, &ont, &rs);
        // grandparent(alice, carol) inferred. The rule predicate is bare `grandparent`;
        // the parent facts come from the graph as canonical IRIs, so the head IRIs are
        // the bound individuals.
        let gp = res
            .facts
            .iter()
            .find(|(p, a, _)| p == "grandparent" && a == &vec![c("alice"), c("carol")]);
        assert!(
            gp.is_some(),
            "grandparent(alice,carol) must be derived; facts={:?}",
            res.facts
        );
        // Confidence = rule 0.9 × parent(1.0) × parent(1.0) = 0.9.
        let conf = gp.unwrap().2;
        assert!(
            (conf - 0.9).abs() < 1e-9,
            "grandparent conf 0.9, got {conf}"
        );

        // Removing the rule retracts it from the set.
        assert!(rs.remove("gp"));
        assert!(rs.is_empty());
    }

    /// Confidence PRODUCT: a low-confidence parent fact drags the grandparent down.
    #[test]
    fn custom_rule_confidence_product() {
        // bob->carol asserted at 0.5 via eg:confidence on the edge subject is not how
        // facts carry conf here (facts default 1.0); instead exercise rule_conf product
        // across a two-hop chain where the rule itself is 0.5.
        let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:parent ex:bob .
ex:bob   ex:parent ex:carol .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let mut rs = RuleSet::new();
        rs.add_str("parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.5")
            .unwrap();
        let res = reason_triples(&triples, &ont, &rs);
        let conf = res
            .fact_confidence("grandparent", &[&c("alice"), &c("carol")])
            .unwrap();
        assert!((conf - 0.5).abs() < 1e-9, "got {conf}");
    }

    /// FunctionalProperty merges: `hasFather` functional + two values ⇒ they are
    /// `owl:sameAs`, and facts of the merged individuals unify (congruence).
    #[test]
    fn functional_property_merges_individuals() {
        let ttl = r#"
@prefix ex:  <http://ex/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:hasFather a owl:FunctionalProperty .
ex:bob ex:hasFather ex:john .
ex:bob ex:hasFather ex:johnny .
ex:john ex:livesIn ex:paris .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let res = reason_triples(&triples, &ont, &RuleSet::new());
        // john and johnny are merged (lexicographic root = john).
        assert!(
            res.same_as.iter().any(|(a, b)| {
                (a == &c("john") && b == &c("johnny")) || (a == &c("johnny") && b == &c("john"))
            }),
            "functional hasFather must derive john owl:sameAs johnny; same_as={:?}",
            res.same_as
        );
        // Congruence: livesIn(john, paris) is now livesIn(<root>, paris) and both
        // names resolve — the representative carries the edge.
        let root = if c("john") <= c("johnny") {
            c("john")
        } else {
            c("johnny")
        };
        assert!(
            res.holds("<http://ex/livesIn>", &[&root, &c("paris")]),
            "merged individual keeps the livesIn edge; facts={:?}",
            res.facts
        );
        assert!(res.consistent);
    }

    /// InverseFunctionalProperty (a key): same SSN ⇒ same person.
    #[test]
    fn inverse_functional_property_is_a_key() {
        let ttl = r#"
@prefix ex:  <http://ex/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:ssn a owl:InverseFunctionalProperty .
ex:alice ex:ssn ex:s123 .
ex:alicia ex:ssn ex:s123 .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let res = reason_triples(&triples, &ont, &RuleSet::new());
        assert!(
            res.same_as
                .iter()
                .any(|(a, b)| (a, b) == (&c("alice"), &c("alicia"))
                    || (a, b) == (&c("alicia"), &c("alice"))),
            "same SSN ⇒ alice owl:sameAs alicia; {:?}",
            res.same_as
        );
    }

    /// `owl:sameAs` over an `owl:differentFrom` pair is an instance-level clash.
    #[test]
    fn sameas_over_differentfrom_is_inconsistent() {
        let ttl = r#"
@prefix ex:  <http://ex/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:hasFather a owl:FunctionalProperty .
ex:bob ex:hasFather ex:john .
ex:bob ex:hasFather ex:johnny .
ex:john owl:differentFrom ex:johnny .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let res = reason_triples(&triples, &ont, &RuleSet::new());
        assert!(
            !res.consistent,
            "functional merge of a differentFrom pair must be inconsistent"
        );
        assert!(!res.conflicts.is_empty());
    }

    /// Built-in OWL-RL: subPropertyOf + domain feed a custom rule in the same fixpoint.
    #[test]
    fn builtin_rl_chains_into_custom_rule() {
        let ttl = r#"
@prefix ex:   <http://ex/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
ex:hasDad rdfs:subPropertyOf ex:parent .
ex:alice ex:hasDad ex:bob .
ex:bob   ex:parent ex:carol .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let mut rs = RuleSet::new();
        rs.add_str("parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z)")
            .unwrap();
        let res = reason_triples(&triples, &ont, &rs);
        // hasDad ⊑ parent lifts alice->bob into a parent edge; then the custom rule fires.
        assert!(
            res.holds("grandparent", &[&c("alice"), &c("carol")]),
            "subPropertyOf must feed the custom grandparent rule; facts={:?}",
            res.facts
        );
    }

    /// The Datalog `:-` form parses equivalently to the `->` form.
    #[test]
    fn datalog_arrow_form_parses() {
        let r = Rule::parse("grandparent(?x,?z) :- parent(?x,?y), parent(?y,?z)").unwrap();
        assert_eq!(r.head.len(), 1);
        assert_eq!(r.head[0].pred, "grandparent");
        assert_eq!(r.body.len(), 2);
    }

    /// An unsafe rule (head var not bound by the body) is rejected.
    #[test]
    fn unsafe_rule_rejected() {
        assert!(Rule::parse("foo(?x) -> bar(?x, ?y)").is_err());
    }

    /// SWRL built-in atoms parse as body atoms WITHOUT disturbing the existing DSL
    /// (CONCEPT:EG-KG.ontology.concept-3): a plain bare-predicate Datalog rule parses unchanged, and a
    /// `swrlb:` built-in atom keeps its predicate intact (and a bare numeric arg becomes
    /// a constant, not a variable).
    #[test]
    fn swrl_builtin_atom_parses_and_plain_dsl_unaffected() {
        let plain = Rule::parse("grandparent(?x,?z) :- parent(?x,?y), parent(?y,?z)").unwrap();
        assert_eq!(plain.body.len(), 2);
        assert_eq!(plain.head[0].pred, "grandparent");

        let r = Rule::parse("Senior(?p) :- age(?p,?a), swrlb:greaterThan(?a, 65)").unwrap();
        assert_eq!(r.body.len(), 2);
        assert_eq!(r.body[1].pred, "swrlb:greaterThan");
        // The bare `65` is a constant; `?a` and `?p` are variables.
        assert_eq!(r.body[1].args[1], RTerm::Const("65".into()));
        assert_eq!(r.body[1].args[0], RTerm::Var("a".into()));
    }

    /// A SWRL comparison built-in filters bindings (CONCEPT:EG-KG.ontology.concept-3): only individuals
    /// whose age ≥ 18 are classified `Adult`.
    #[test]
    fn swrl_comparison_builtin_filters() {
        let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:age 30 .
ex:bob   ex:age 12 .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let mut rs = RuleSet::new();
        rs.add_str("Adult(?p) :- age(?p, ?a), swrlb:greaterThanOrEqual(?a, 18)")
            .unwrap();
        let res = reason_triples(&triples, &ont, &rs);
        assert!(
            res.holds("Adult", &[&c("alice")]),
            "alice (30) ≥ 18 ⇒ Adult; facts={:?}",
            res.facts
        );
        assert!(
            !res.holds("Adult", &[&c("bob")]),
            "bob (12) < 18 ⇒ filtered out; facts={:?}",
            res.facts
        );
    }

    /// A SWRL math built-in binds its result variable (CONCEPT:EG-KG.ontology.concept-3): `?n = ?a + 1`.
    #[test]
    fn swrl_math_builtin_binds_result() {
        let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:age 30 .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let mut rs = RuleSet::new();
        rs.add_str("age(?p, ?a) ^ swrlb:add(?n, ?a, 1) -> nextAge(?p, ?n)")
            .unwrap();
        let res = reason_triples(&triples, &ont, &rs);
        assert!(
            res.holds("nextAge", &[&c("alice"), "31"]),
            "30 + 1 = 31 binds ?n ⇒ nextAge(alice, 31); facts={:?}",
            res.facts
        );
    }

    /// A SWRL string built-in binds its result variable (CONCEPT:EG-KG.ontology.concept-3): upper-casing a
    /// data value, and a `stringConcat` producer.
    #[test]
    fn swrl_string_builtin_binds_result() {
        let ttl = r#"
@prefix ex: <http://ex/> .
ex:alice ex:firstName "alice" .
"#;
        let triples = parse_turtle(ttl).unwrap();
        let ont = crate::owl::parse_ontology(&triples);
        let mut rs = RuleSet::new();
        rs.add_str(r#"firstName(?p, ?n) ^ swrlb:upperCase(?u, ?n) -> upperName(?p, ?u)"#)
            .unwrap();
        rs.add_str(r#"firstName(?p, ?n) ^ swrlb:stringConcat(?g, "Ms ", ?n) -> greeting(?p, ?g)"#)
            .unwrap();
        let res = reason_triples(&triples, &ont, &rs);
        assert!(
            res.holds("upperName", &[&c("alice"), "ALICE"]),
            "upperCase(alice) = ALICE; facts={:?}",
            res.facts
        );
        assert!(
            res.holds("greeting", &[&c("alice"), "Ms alice"]),
            "stringConcat(\"Ms \", alice) = \"Ms alice\"; facts={:?}",
            res.facts
        );
    }

    /// The server-op request path round-trips Turtle + rules into a filtered response.
    #[test]
    fn run_rule_reasoning_request_path() {
        let req = RuleReasonRequest {
            ontology_ttl: r#"
@prefix ex: <http://ex/> .
ex:alice ex:parent ex:bob .
ex:bob   ex:parent ex:carol .
"#
            .into(),
            rules: vec!["parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.8".into()],
            query_predicate: Some("grandparent".into()),
            min_confidence: 0.0,
            derived_only: true,
        };
        let resp = run_rule_reasoning(&req).unwrap();
        assert_eq!(resp.registered_rules.len(), 1);
        assert!(resp.consistent);
        assert_eq!(
            resp.facts.len(),
            1,
            "only grandparent facts: {:?}",
            resp.facts
        );
        let f = &resp.facts[0];
        assert_eq!(f.predicate, "grandparent");
        assert!(f.derived);
        assert!((f.confidence - 0.8).abs() < 1e-9);
    }
}
