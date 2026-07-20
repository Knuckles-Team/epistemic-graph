//! EG-136 — hand-rolled JSON-LD 1.1 serialization + parse over the eg-rdf term model.
//!
//! CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix. A dependency-light JSON-LD path built directly on `serde_json`
//! (already a base dep of this crate) + the `oxrdf` term model — it pulls NO heavy
//! JSON-LD crate, so it links under the plain `rdf`/`sparql` features (and is safe in
//! `pi`, unlike the `oxjsonld`-backed [`crate::mapping::to_jsonld`] which rides the
//! out-of-pi `json-ld` feature). It also goes BEYOND that path: it emits both the
//! **expanded** document and a **compacted** document against a supplied `@context`,
//! and parses expanded/compacted JSON-LD back to quads (`@id`/`@type`/`@value`/`@graph`
//! handling, named graphs via `@graph`).
//!
//! Scope (RDF-round-trip subset of JSON-LD 1.1): node objects grouped by subject;
//! `@id`, `@type` (from `rdf:type`), `@value`/`@type`/`@language` literal objects,
//! `@graph` for a named graph, and `@context` term/prefix/`@vocab`/`@type:@id`
//! expansion + compaction. Framing, `@list`/`@set` containers, and remote contexts
//! are a documented follow-up (not needed for the concrete-syntax matrix round-trip).

use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term, Triple};
use serde_json::{Map, Value};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

// ── term-string helpers (CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix) ─────────────────────────────────────────

/// The JSON-LD `@id` string for a subject term (IRI verbatim; blank node as `_:b`).
fn subject_id(s: &NamedOrBlankNode) -> String {
    match s {
        NamedOrBlankNode::NamedNode(n) => n.as_str().to_string(),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
        #[allow(unreachable_patterns)]
        _ => "_:b".to_string(),
    }
}

/// A stable grouping key for a subject (identical to its `@id`).
fn subject_key(s: &NamedOrBlankNode) -> String {
    subject_id(s)
}

/// Build the expanded JSON value for a triple's object.
fn object_value(o: &Term) -> Value {
    match o {
        Term::NamedNode(n) => serde_json::json!({ "@id": n.as_str() }),
        Term::BlankNode(b) => serde_json::json!({ "@id": format!("_:{}", b.as_str()) }),
        Term::Literal(l) => {
            let mut m = Map::new();
            m.insert("@value".to_string(), Value::String(l.value().to_string()));
            if let Some(lang) = l.language() {
                m.insert("@language".to_string(), Value::String(lang.to_string()));
            } else {
                let dt = l.datatype();
                if dt.as_str() != XSD_STRING {
                    m.insert("@type".to_string(), Value::String(dt.as_str().to_string()));
                }
            }
            Value::Object(m)
        }
        #[allow(unreachable_patterns)]
        _ => Value::Null,
    }
}

// ── EG-136 expansion (triples → expanded JSON-LD) ────────────────────────────────

/// Build the expanded JSON-LD node-object array for `triples`. `rdf:type` object
/// triples are folded into an `@type` array; every other resource/literal object
/// becomes a value object under its predicate IRI (an array — JSON-LD's expanded form
/// always uses arrays for property values). CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix.
fn expand_nodes(triples: &[Triple]) -> Vec<Value> {
    // Preserve first-seen subject order for deterministic output.
    let mut order: Vec<String> = Vec::new();
    let mut nodes: std::collections::HashMap<String, Map<String, Value>> =
        std::collections::HashMap::new();

    for t in triples {
        let key = subject_key(&t.subject);
        if !nodes.contains_key(&key) {
            order.push(key.clone());
            let mut m = Map::new();
            m.insert("@id".to_string(), Value::String(subject_id(&t.subject)));
            nodes.insert(key.clone(), m);
        }
        let node = nodes.get_mut(&key).unwrap();
        let pred = t.predicate.as_str();
        if pred == RDF_TYPE {
            // rdf:type with a resource object → @type entry.
            if let Term::NamedNode(n) = &t.object {
                let arr = node
                    .entry("@type".to_string())
                    .or_insert_with(|| Value::Array(Vec::new()));
                if let Value::Array(a) = arr {
                    a.push(Value::String(n.as_str().to_string()));
                }
                continue;
            }
        }
        let arr = node
            .entry(pred.to_string())
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Value::Array(a) = arr {
            a.push(object_value(&t.object));
        }
    }

    order
        .into_iter()
        .map(|k| Value::Object(nodes.remove(&k).unwrap()))
        .collect()
}

/// Serialize `triples` to an **expanded** JSON-LD document string. When `graph` is
/// `Some`, the node array is wrapped in a single named-graph object
/// (`[{"@id": <graph>, "@graph": [...]}]`); otherwise it is the bare node array.
/// CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix.
pub fn to_expanded(triples: &[Triple], graph: Option<&str>) -> Result<String, String> {
    let nodes = expand_nodes(triples);
    let doc = match graph {
        Some(g) => Value::Array(vec![serde_json::json!({
            "@id": g,
            "@graph": Value::Array(nodes),
        })]),
        None => Value::Array(nodes),
    };
    serde_json::to_string_pretty(&doc).map_err(|e| format!("jsonld expand serialize: {e}"))
}

// ── EG-136 compaction (expanded → compacted against @context) ─────────────────────

/// A parsed `@context`: exact term→IRI aliases, prefix→namespace mappings, an optional
/// `@vocab`, and the set of terms declared `"@type": "@id"` (their string values are IRIs).
struct Context {
    /// term → IRI (exact aliases, longest-IRI-wins on the reverse map).
    terms: Vec<(String, String)>,
    /// prefix → namespace IRI.
    prefixes: Vec<(String, String)>,
    vocab: Option<String>,
    /// terms whose values coerce to `@id` (resources written as bare strings).
    id_terms: std::collections::HashSet<String>,
}

impl Context {
    fn parse(ctx: &Value) -> Context {
        let mut terms = Vec::new();
        let mut prefixes = Vec::new();
        let mut vocab = None;
        let mut id_terms = std::collections::HashSet::new();
        if let Value::Object(m) = ctx {
            for (k, v) in m {
                if k == "@vocab" {
                    if let Some(s) = v.as_str() {
                        vocab = Some(s.to_string());
                    }
                    continue;
                }
                if k.starts_with('@') {
                    continue;
                }
                match v {
                    Value::String(iri) => {
                        // A prefix definition (namespace ends in / or #) doubles as a term.
                        if iri.ends_with('/') || iri.ends_with('#') {
                            prefixes.push((k.clone(), iri.clone()));
                        }
                        terms.push((k.clone(), iri.clone()));
                    }
                    Value::Object(def) => {
                        if let Some(id) = def.get("@id").and_then(|x| x.as_str()) {
                            terms.push((k.clone(), id.to_string()));
                            if id.ends_with('/') || id.ends_with('#') {
                                prefixes.push((k.clone(), id.to_string()));
                            }
                        }
                        if def.get("@type").and_then(|x| x.as_str()) == Some("@id") {
                            id_terms.insert(k.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
        Context {
            terms,
            prefixes,
            vocab,
            id_terms,
        }
    }

    /// Compact an IRI to a term/prefixed-name/vocab-relative form, else the full IRI.
    fn compact_iri(&self, iri: &str) -> String {
        // Exact term alias (prefer the longest matching IRI).
        let mut best: Option<&str> = None;
        for (term, mapped) in &self.terms {
            if mapped == iri && best.map(|b| term.len() < b.len()).unwrap_or(true) {
                best = Some(term);
            }
        }
        if let Some(t) = best {
            return t.to_string();
        }
        // Prefixed name (longest namespace wins).
        let mut best_pfx: Option<(&str, &str)> = None;
        for (pfx, ns) in &self.prefixes {
            if iri.starts_with(ns.as_str())
                && best_pfx
                    .map(|(_, bns)| ns.len() > bns.len())
                    .unwrap_or(true)
            {
                best_pfx = Some((pfx, ns));
            }
        }
        if let Some((pfx, ns)) = best_pfx {
            return format!("{pfx}:{}", &iri[ns.len()..]);
        }
        // @vocab-relative.
        if let Some(v) = &self.vocab {
            if let Some(rest) = iri.strip_prefix(v.as_str()) {
                return rest.to_string();
            }
        }
        iri.to_string()
    }
}

/// Serialize `triples` to a **compacted** JSON-LD document against the supplied
/// `@context` (term aliases, prefixes, `@vocab`, `@type:@id` coercion). The emitted
/// document embeds the context so it is self-describing and round-trips through
/// [`from_jsonld`]. When `graph` is `Some`, the compacted nodes live under `@graph`
/// alongside the graph's `@id`. CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix.
pub fn to_compacted(
    triples: &[Triple],
    graph: Option<&str>,
    context: &Value,
) -> Result<String, String> {
    let ctx = Context::parse(context);
    let expanded = expand_nodes(triples);
    let compacted: Vec<Value> = expanded.iter().map(|n| compact_node(n, &ctx)).collect();

    let mut doc = Map::new();
    doc.insert("@context".to_string(), context.clone());
    match graph {
        Some(g) => {
            doc.insert("@id".to_string(), Value::String(ctx.compact_iri(g)));
            doc.insert("@graph".to_string(), Value::Array(compacted));
        }
        None => {
            doc.insert("@graph".to_string(), Value::Array(compacted));
        }
    }
    serde_json::to_string_pretty(&Value::Object(doc))
        .map_err(|e| format!("jsonld compact serialize: {e}"))
}

/// Compact a single expanded node object against `ctx`.
fn compact_node(node: &Value, ctx: &Context) -> Value {
    let Value::Object(m) = node else {
        return node.clone();
    };
    let mut out = Map::new();
    for (k, v) in m {
        match k.as_str() {
            "@id" => {
                if let Some(iri) = v.as_str() {
                    out.insert("@id".to_string(), Value::String(compact_id(iri, ctx)));
                }
            }
            "@type" => {
                let compacted: Vec<Value> = v
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|t| t.as_str())
                            .map(|t| Value::String(ctx.compact_iri(t)))
                            .collect()
                    })
                    .unwrap_or_default();
                let val = if compacted.len() == 1 {
                    compacted.into_iter().next().unwrap()
                } else {
                    Value::Array(compacted)
                };
                out.insert("@type".to_string(), val);
            }
            pred => {
                let term = ctx.compact_iri(pred);
                let coerce_id = ctx.id_terms.contains(&term);
                let vals: Vec<Value> = v
                    .as_array()
                    .map(|a| a.iter().map(|x| compact_value(x, ctx, coerce_id)).collect())
                    .unwrap_or_default();
                // Single value stays scalar; multiple stay an array.
                let val = if vals.len() == 1 {
                    vals.into_iter().next().unwrap()
                } else {
                    Value::Array(vals)
                };
                out.insert(term, val);
            }
        }
    }
    Value::Object(out)
}

/// Compact an `@id`/`@type` IRI (a bare `_:b` blank node passes through unchanged).
fn compact_id(iri: &str, ctx: &Context) -> String {
    if iri.starts_with("_:") {
        return iri.to_string();
    }
    ctx.compact_iri(iri)
}

/// Compact a single expanded value object. A resource under a `@type:@id` term becomes
/// a bare compacted-IRI string; otherwise `{"@id": ...}`. A plain `xsd:string` literal
/// becomes a bare string; a typed/lang literal keeps `@value`/`@type`/`@language`.
fn compact_value(v: &Value, ctx: &Context, coerce_id: bool) -> Value {
    let Value::Object(m) = v else {
        return v.clone();
    };
    if let Some(id) = m.get("@id").and_then(|x| x.as_str()) {
        let compacted = compact_id(id, ctx);
        return if coerce_id {
            Value::String(compacted)
        } else {
            serde_json::json!({ "@id": compacted })
        };
    }
    if let Some(lit) = m.get("@value") {
        // Plain string with no datatype/language → bare string.
        if m.get("@language").is_none() && m.get("@type").is_none() {
            if let Some(s) = lit.as_str() {
                return Value::String(s.to_string());
            }
        }
        let mut out = Map::new();
        out.insert("@value".to_string(), lit.clone());
        if let Some(lang) = m.get("@language") {
            out.insert("@language".to_string(), lang.clone());
        }
        if let Some(dt) = m.get("@type").and_then(|x| x.as_str()) {
            out.insert("@type".to_string(), Value::String(ctx.compact_iri(dt)));
        }
        return Value::Object(out);
    }
    v.clone()
}

// ── EG-136 the single writer entry point ─────────────────────────────────────────

/// Serialize `triples` to JSON-LD: **compacted** against `context` when one is supplied,
/// otherwise **expanded**. Named graphs via `@graph` when `graph` is `Some`. This is the
/// EG-136 counterpart to the EG-050 `to_turtle`/`to_ntriples` writers and the negotiation
/// form for the `application/ld+json` media type. CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix.
pub fn to_jsonld(
    triples: &[Triple],
    graph: Option<&str>,
    context: Option<&Value>,
) -> Result<String, String> {
    match context {
        Some(ctx) => to_compacted(triples, graph, ctx),
        None => to_expanded(triples, graph),
    }
}

// ── EG-136 parse (expanded/compacted JSON-LD → quads) ─────────────────────────────

/// Parse a JSON-LD document (expanded OR compacted-with-inline-`@context`) into quads.
/// Handles the top-level array or object form, a document-level `@graph` (named when the
/// wrapper carries an `@id`), `@type`, and `@value`/`@type`/`@language` literals.
/// CONCEPT:EG-KG.ontology.eg-concrete-syntax-matrix.
pub fn from_jsonld(doc: &str) -> Result<Vec<Quad>, String> {
    let root: Value = serde_json::from_str(doc).map_err(|e| format!("jsonld parse: {e}"))?;
    let mut out = Vec::new();
    // A document-level @context applies to every node (compacted input).
    let top_ctx = root
        .as_object()
        .and_then(|m| m.get("@context"))
        .map(Context::parse);
    match &root {
        Value::Array(items) => {
            for item in items {
                walk_node(item, &GraphName::DefaultGraph, top_ctx.as_ref(), &mut out)?;
            }
        }
        Value::Object(m) => {
            // Object with @graph = a (possibly named) graph container.
            if let Some(g) = m.get("@graph") {
                let graph = match m.get("@id").and_then(|x| x.as_str()) {
                    Some(id) => {
                        let expanded = top_ctx
                            .as_ref()
                            .map(|c| expand_id(id, c))
                            .unwrap_or_else(|| id.to_string());
                        graph_name(&expanded)?
                    }
                    None => GraphName::DefaultGraph,
                };
                for item in g.as_array().cloned().unwrap_or_default() {
                    walk_node(&item, &graph, top_ctx.as_ref(), &mut out)?;
                }
            } else {
                walk_node(&root, &GraphName::DefaultGraph, top_ctx.as_ref(), &mut out)?;
            }
        }
        _ => return Err("jsonld: root must be an array or object".to_string()),
    }
    Ok(out)
}

/// A named-graph term from a graph IRI/bnode string.
fn graph_name(id: &str) -> Result<GraphName, String> {
    if let Some(b) = id.strip_prefix("_:") {
        Ok(GraphName::BlankNode(
            BlankNode::new(b).map_err(|e| format!("bad graph bnode {b}: {e}"))?,
        ))
    } else {
        Ok(GraphName::NamedNode(
            NamedNode::new(id).map_err(|e| format!("bad graph iri {id}: {e}"))?,
        ))
    }
}

/// Expand a compact IRI/term/`_:b` using `ctx` (identity when it is already absolute).
fn expand_id(v: &str, ctx: &Context) -> String {
    if v.starts_with("_:") || v.starts_with("@") {
        return v.to_string();
    }
    // Exact term alias.
    for (term, iri) in &ctx.terms {
        if term == v {
            return iri.clone();
        }
    }
    // Prefixed name `pfx:local`.
    if let Some((pfx, local)) = v.split_once(':') {
        // Absolute IRI (scheme://…) passes through.
        if local.starts_with("//") {
            return v.to_string();
        }
        for (p, ns) in &ctx.prefixes {
            if p == pfx {
                return format!("{ns}{local}");
            }
        }
        // A scheme we don't know as a prefix → treat as absolute.
        if v.contains("://") {
            return v.to_string();
        }
    }
    // @vocab-relative bare term.
    if !v.contains(':') {
        if let Some(vocab) = &ctx.vocab {
            return format!("{vocab}{v}");
        }
    }
    v.to_string()
}

/// Turn a JSON-LD node object into quads in `graph`, expanding keys/values via `ctx`.
fn walk_node(
    node: &Value,
    graph: &GraphName,
    ctx: Option<&Context>,
    out: &mut Vec<Quad>,
) -> Result<(), String> {
    let Value::Object(m) = node else {
        return Ok(()); // scalars/arrays at node position are ignored.
    };
    // A nested @graph inside a node = a named graph keyed by this node's @id.
    if let Some(g) = m.get("@graph") {
        let inner = match m.get("@id").and_then(|x| x.as_str()) {
            Some(id) => {
                let e = ctx
                    .map(|c| expand_id(id, c))
                    .unwrap_or_else(|| id.to_string());
                graph_name(&e)?
            }
            None => graph.clone(),
        };
        for item in g.as_array().cloned().unwrap_or_default() {
            walk_node(&item, &inner, ctx, out)?;
        }
        return Ok(());
    }

    let subj_id = match m.get("@id").and_then(|x| x.as_str()) {
        Some(id) => ctx
            .map(|c| expand_id(id, c))
            .unwrap_or_else(|| id.to_string()),
        None => {
            // Anonymous node → a fresh blank node.
            format!("_:b{}", out.len())
        }
    };
    let subject = make_subject(&subj_id)?;

    for (k, v) in m {
        match k.as_str() {
            "@id" | "@context" => {}
            "@type" => {
                for ty in type_values(v) {
                    let e = ctx.map(|c| expand_id(&ty, c)).unwrap_or(ty);
                    let obj = Term::NamedNode(
                        NamedNode::new(&e).map_err(|err| format!("bad @type iri {e}: {err}"))?,
                    );
                    out.push(Quad::new(
                        subject.clone(),
                        NamedNode::new(RDF_TYPE).unwrap(),
                        obj,
                        graph.clone(),
                    ));
                }
            }
            key => {
                let pred_iri = ctx
                    .map(|c| expand_id(key, c))
                    .unwrap_or_else(|| key.to_string());
                let coerce_id = ctx
                    .map(|c| c.id_terms.contains(key) || c.id_terms.contains(&pred_iri))
                    .unwrap_or(false);
                let pred = NamedNode::new(&pred_iri)
                    .map_err(|err| format!("bad predicate iri {pred_iri}: {err}"))?;
                for item in value_items(v) {
                    let obj = make_object(item, ctx, coerce_id)?;
                    out.push(Quad::new(subject.clone(), pred.clone(), obj, graph.clone()));
                }
            }
        }
    }
    Ok(())
}

/// A subject/`NamedOrBlankNode` from an expanded id string.
fn make_subject(id: &str) -> Result<NamedOrBlankNode, String> {
    if let Some(b) = id.strip_prefix("_:") {
        Ok(NamedOrBlankNode::BlankNode(
            BlankNode::new(b).map_err(|e| format!("bad bnode {b}: {e}"))?,
        ))
    } else {
        Ok(NamedOrBlankNode::NamedNode(
            NamedNode::new(id).map_err(|e| format!("bad subject iri {id}: {e}"))?,
        ))
    }
}

/// Normalize an `@type` value (string or array of strings) to a Vec of IRIs/terms.
fn type_values(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => vec![s.clone()],
        Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

/// Normalize a property value to a Vec of value items (JSON-LD allows scalar or array).
fn value_items(v: &Value) -> Vec<&Value> {
    match v {
        Value::Array(a) => a.iter().collect(),
        other => vec![other],
    }
}

/// Build an object `Term` from a JSON-LD value item, expanding IRIs/terms via `ctx`.
/// `coerce_id` (the predicate's term declared `@type:@id`) makes a bare string an IRI.
fn make_object(item: &Value, ctx: Option<&Context>, coerce_id: bool) -> Result<Term, String> {
    match item {
        Value::Object(m) => {
            if let Some(id) = m.get("@id").and_then(|x| x.as_str()) {
                let e = ctx
                    .map(|c| expand_id(id, c))
                    .unwrap_or_else(|| id.to_string());
                return make_resource(&e);
            }
            if let Some(val) = m.get("@value") {
                let lex = value_lexical(val);
                if let Some(lang) = m.get("@language").and_then(|x| x.as_str()) {
                    return Literal::new_language_tagged_literal(lex, lang)
                        .map(Term::Literal)
                        .map_err(|e| format!("bad @language {lang}: {e}"));
                }
                if let Some(dt) = m.get("@type").and_then(|x| x.as_str()) {
                    let e = ctx
                        .map(|c| expand_id(dt, c))
                        .unwrap_or_else(|| dt.to_string());
                    let dtn = NamedNode::new(&e).map_err(|err| format!("bad @type {e}: {err}"))?;
                    return Ok(Term::Literal(Literal::new_typed_literal(lex, dtn)));
                }
                return Ok(Term::Literal(Literal::new_simple_literal(lex)));
            }
            Err("jsonld value object has neither @id nor @value".to_string())
        }
        Value::String(s) => {
            if coerce_id {
                let e = ctx.map(|c| expand_id(s, c)).unwrap_or_else(|| s.clone());
                make_resource(&e)
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(s)))
            }
        }
        Value::Bool(b) => Ok(Term::Literal(Literal::new_typed_literal(
            b.to_string(),
            NamedNode::new("http://www.w3.org/2001/XMLSchema#boolean").unwrap(),
        ))),
        Value::Number(n) => {
            let dt = if n.is_f64() {
                "http://www.w3.org/2001/XMLSchema#double"
            } else {
                "http://www.w3.org/2001/XMLSchema#integer"
            };
            Ok(Term::Literal(Literal::new_typed_literal(
                n.to_string(),
                NamedNode::new(dt).unwrap(),
            )))
        }
        _ => Err("unsupported jsonld value".to_string()),
    }
}

/// The lexical form of a JSON `@value` (strings verbatim; bool/number stringified).
fn value_lexical(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A resource object `Term` (IRI or `_:b` blank node).
fn make_resource(id: &str) -> Result<Term, String> {
    if let Some(b) = id.strip_prefix("_:") {
        Ok(Term::BlankNode(
            BlankNode::new(b).map_err(|e| format!("bad object bnode {b}: {e}"))?,
        ))
    } else {
        Ok(Term::NamedNode(
            NamedNode::new(id).map_err(|e| format!("bad object iri {id}: {e}"))?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{parse_turtle, triple_set_key};

    const TTL: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ;
         ex:name "Alice" ;
         ex:age "30"^^xsd:integer ;
         ex:knows ex:bob .
ex:bob  a ex:Person ;
        ex:name "Bob"@en .
"#;

    fn quads_as_triples(quads: &[Quad]) -> Vec<Triple> {
        quads
            .iter()
            .map(|q| Triple::new(q.subject.clone(), q.predicate.clone(), q.object.clone()))
            .collect()
    }

    /// EG-136: expanded JSON-LD write → parse round-trips the triple set.
    #[test]
    fn eg136_jsonld_expanded_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let jld = to_jsonld(&triples, None, None).unwrap();
        // Expanded form is a bare array of node objects.
        assert!(
            jld.trim_start().starts_with('['),
            "expanded form is an array"
        );
        let quads = from_jsonld(&jld).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
            "expanded JSON-LD must round-trip the triple set"
        );
        assert!(
            quads
                .iter()
                .all(|q| matches!(q.graph_name, GraphName::DefaultGraph)),
            "no @graph ⇒ default graph"
        );
    }

    /// EG-136: compacted JSON-LD against a supplied @context round-trips the triple set,
    /// and the compacted document is genuinely compacted (prefixed/`@vocab` keys, no
    /// full `http://example.org/` predicate IRIs).
    #[test]
    fn eg136_jsonld_compacted_round_trips_with_context() {
        let triples = parse_turtle(TTL).unwrap();
        let context = serde_json::json!({
            "@vocab": "http://example.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "knows": { "@id": "http://example.org/knows", "@type": "@id" }
        });
        let jld = to_jsonld(&triples, None, Some(&context)).unwrap();
        assert!(
            !jld.contains("\"http://example.org/name\""),
            "predicate IRIs must be compacted away, got:\n{jld}"
        );
        let quads = from_jsonld(&jld).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
            "compacted JSON-LD must round-trip the triple set"
        );
    }

    /// EG-136: `@type: @id` coercion — a resource object written as a bare compacted
    /// string still parses back to an IRI object (not a literal).
    #[test]
    fn eg136_jsonld_type_id_coercion_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let context = serde_json::json!({
            "@vocab": "http://example.org/",
            "knows": { "@id": "http://example.org/knows", "@type": "@id" }
        });
        let jld = to_jsonld(&triples, None, Some(&context)).unwrap();
        let quads = from_jsonld(&jld).unwrap();
        let knows = quads
            .iter()
            .find(|q| q.predicate.as_str().ends_with("knows"))
            .expect("knows quad present");
        assert!(
            matches!(&knows.object, Term::NamedNode(n) if n.as_str() == "http://example.org/bob"),
            "@type:@id coercion must yield an IRI object, got {:?}",
            knows.object
        );
    }

    /// EG-136: a named graph via `@graph` round-trips both the triples AND the graph name.
    #[test]
    fn eg136_jsonld_named_graph_round_trips() {
        let triples = parse_turtle(TTL).unwrap();
        let g = "http://example.org/g";
        let jld = to_jsonld(&triples, Some(g), None).unwrap();
        let quads = from_jsonld(&jld).unwrap();
        assert_eq!(
            triple_set_key(&triples),
            triple_set_key(&quads_as_triples(&quads)),
            "named-graph JSON-LD must round-trip the triple set"
        );
        assert!(
            quads
                .iter()
                .all(|q| matches!(&q.graph_name, GraphName::NamedNode(n) if n.as_str() == g)),
            "every quad must carry the @graph named graph"
        );
    }

    /// EG-136: typed (`xsd:integer`) and language-tagged (`@en`) literals survive the
    /// `@value`/`@type`/`@language` expansion and parse.
    #[test]
    fn eg136_jsonld_typed_and_lang_literals_survive() {
        let triples = parse_turtle(TTL).unwrap();
        let jld = to_jsonld(&triples, None, None).unwrap();
        let quads = from_jsonld(&jld).unwrap();
        let age = quads
            .iter()
            .find(|q| q.predicate.as_str().ends_with("age"))
            .expect("age present");
        assert!(
            matches!(&age.object, Term::Literal(l) if l.datatype().as_str() == "http://www.w3.org/2001/XMLSchema#integer"),
            "xsd:integer must survive"
        );
        let name_en = quads.iter().any(|q| {
            q.predicate.as_str().ends_with("name")
                && matches!(&q.object, Term::Literal(l) if l.language() == Some("en"))
        });
        assert!(name_en, "@en language tag must survive");
    }
}
