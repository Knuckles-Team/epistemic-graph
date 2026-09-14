//! Rule syntax, AST, registration, and shared RDF predicate helpers.

use std::collections::BTreeSet;

pub(super) const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub(super) const OWL_SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";

/// Canonical `<iri>` form (matches owl.rs / mapping node-id convention).
pub(super) fn iri(s: &str) -> String {
    format!("<{s}>")
}

/// OWL/RDFS meta-classes whose `rdf:type` assertions are TBox declarations, NOT ABox
/// individual typing — skipped when loading ground facts.
pub(super) fn is_meta_class(o: &str) -> bool {
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
pub(super) fn is_schema_pred(p: &str) -> bool {
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
pub(super) fn local_name(p: &str) -> &str {
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
pub(super) fn pred_matches(rule_pred: &str, fact_pred: &str) -> bool {
    if rule_pred == fact_pred {
        return true;
    }
    if rule_pred.starts_with('<') {
        return false; // an explicit IRI rule predicate must match exactly
    }
    local_name(fact_pred) == rule_pred
}

/// True when `pred` is the `owl:sameAs` equality predicate (IRI or short alias).
pub(super) fn is_same_as(pred: &str) -> bool {
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
        let conf = extract_trailing_confidence(&mut s);
        let mut name = extract_leading_name(&mut s);

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
        check_range_restricted(&body, &head, src)?;
        Ok(Rule {
            name,
            body,
            head,
            conf,
        })
    }
}

/// Optional trailing confidence `@0.8`: strip it off `s` in place and return its value,
/// or `1.0` when absent or the suffix doesn't parse as a float. Extracted from
/// [`Rule::parse`]'s first pre-processing stage.
fn extract_trailing_confidence(s: &mut String) -> f64 {
    if let Some(pos) = s.rfind('@') {
        // Only treat as a confidence suffix if what follows parses as a float.
        let tail = s[pos + 1..].trim();
        if let Ok(c) = tail.parse::<f64>() {
            let conf = c.clamp(0.0, 1.0);
            s.truncate(pos);
            *s = s.trim().to_string();
            return conf;
        }
    }
    1.0
}

/// Optional leading `name:` — but NOT the `:-` Datalog operator and not an IRI (which
/// contains `:`). The name is a bare identifier before the FIRST `:` that is not part
/// of `:-`. Strips the prefix off `s` in place when found. Extracted from
/// [`Rule::parse`]'s second pre-processing stage.
fn extract_leading_name(s: &mut String) -> String {
    if let Some(colon) = s.find(':') {
        let is_datalog = s[colon..].starts_with(":-");
        let head_part = &s[..colon];
        let looks_like_name =
            !head_part.contains('(') && !head_part.contains('<') && !head_part.trim().is_empty();
        if !is_datalog && looks_like_name {
            let name = head_part.trim().to_string();
            *s = s[colon + 1..].trim().to_string();
            return name;
        }
    }
    String::new()
}

/// Safety check for [`Rule::parse`]: every head variable must appear in the body
/// (range-restricted).
fn check_range_restricted(body: &[Atom], head: &[Atom], src: &str) -> Result<(), String> {
    let body_vars: BTreeSet<&String> = body
        .iter()
        .flat_map(|a| a.args.iter())
        .filter_map(|t| match t {
            RTerm::Var(v) => Some(v),
            _ => None,
        })
        .collect();
    for h in head {
        for t in &h.args {
            if let RTerm::Var(v) = t {
                if !body_vars.contains(v) {
                    return Err(format!("unsafe rule: head var ?{v} not in body: {src}"));
                }
            }
        }
    }
    Ok(())
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
