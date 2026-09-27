//! Graph pattern matching, path traversal, and joins.

use super::*;

// ── BGP — match each triple pattern, join on shared variables ───────────────────

pub(super) fn eval_bgp(ctx: &Ctx, patterns: &[TriplePattern]) -> Vec<Solution> {
    let mut acc: Vec<Solution> = vec![Solution::new()];
    for tp in patterns {
        let matches = match_triple_pattern(ctx, tp);
        let mut next = Vec::new();
        for base in &acc {
            for m in &matches {
                if let Some(merged) = merge(base, m) {
                    next.push(merged);
                }
            }
        }
        acc = next;
        if acc.is_empty() {
            break;
        }
    }
    acc
}

/// Property-path evaluation (CONCEPT:EG-KG.query.sparql-completeness). spargebra DESUGARS a sequence path
/// (`p1/p2`) into a BGP with anonymous-bnode intermediates, so a single-predicate
/// path reaching here is handled by the one-triple-pattern matcher. The variable-
/// length / combinator forms (`p+`, `p*`, `p?`, alternative `a|b`, inverse `^p`, and
/// their nesting) are evaluated by [`path_pairs`]: it computes the `(start, end)`
/// resource pairs the path connects, then binds subject/object against them.
pub(super) fn eval_path(
    ctx: &Ctx,
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
) -> Result<Vec<Solution>, String> {
    // A single named predicate stays the literal/edge triple-pattern matcher (it also
    // matches literal-valued predicates, which the resource-only path engine doesn't).
    if let PropertyPathExpression::NamedNode(n) = path {
        let pred = oxrdf::NamedNode::new(n.as_str()).map_err(|e| e.to_string())?;
        let tp = TriplePattern {
            subject: subject.clone(),
            predicate: NamedNodePattern::NamedNode(pred),
            object: object.clone(),
        };
        return Ok(match_triple_pattern(ctx, &tp));
    }

    // The combinator forms resolve over RESOURCE edges (a property path connects nodes,
    // not literals). Enumerate the connected pairs, then bind the subject/object terms.
    let pairs = path_pairs(ctx, path)?;
    let mut out = Vec::new();
    for (s, o) in pairs {
        let mut sol = Solution::new();
        if !bind_subject(subject, &s, &mut sol) {
            continue;
        }
        if !bind_object_node(object, &o, &mut sol) {
            continue;
        }
        out.push(sol);
    }
    Ok(out)
}

/// All `(start, end)` resource-node id pairs the property `path` connects, over the
/// GraphView's typed edges. Recurses on the path combinators:
///   * `NamedNode(p)`  → every edge typed `p`.
///   * `Reverse(p)`    → the pairs of `p` flipped (`^p`).
///   * `Sequence(a,b)` → join: `a` then `b` (shared midpoint).
///   * `Alternative(a,b)` → the union of both.
///   * `OneOrMore(p)`  → transitive closure (`p+`, ≥1 hop).
///   * `ZeroOrMore(p)` → reflexive-transitive closure (`p*`, incl. identity on EVERY
///     node, per SPARQL `x p* x`).
///   * `ZeroOrOne(p)`  → `p` ∪ identity (`p?`).
pub(super) fn path_pairs(
    ctx: &Ctx,
    path: &PropertyPathExpression,
) -> Result<Vec<(String, String)>, String> {
    Ok(match path {
        PropertyPathExpression::NamedNode(n) => edge_pairs(ctx, n.as_str()),
        PropertyPathExpression::Reverse(inner) => path_pairs(ctx, inner)?
            .into_iter()
            .map(|(s, o)| (o, s))
            .collect(),
        PropertyPathExpression::Sequence(a, b) => path_pairs_sequence(ctx, a, b)?,
        PropertyPathExpression::Alternative(a, b) => {
            let mut out = path_pairs(ctx, a)?;
            out.extend(path_pairs(ctx, b)?);
            dedup_pairs(out)
        }
        PropertyPathExpression::OneOrMore(inner) => {
            let base = path_pairs(ctx, inner)?;
            transitive_closure(&base, false, ctx)
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let base = path_pairs(ctx, inner)?;
            transitive_closure(&base, true, ctx)
        }
        PropertyPathExpression::ZeroOrOne(inner) => path_pairs_zero_or_one(ctx, inner)?,
        // Negated property set `!(p1|…|pn)` (CONCEPT:EG-KG.ontology.negated-property-set): every resource edge whose
        // projected predicate IRI is NOT one of the negated predicates.
        PropertyPathExpression::NegatedPropertySet(preds) => {
            let negated: std::collections::HashSet<String> =
                preds.iter().map(|p| p.as_str().to_string()).collect();
            negated_edge_pairs(ctx, &negated)
        }
    })
}

/// The `Sequence(a, b)` arm of [`path_pairs`]: `a`'s object joined to `b`'s subject.
pub(super) fn path_pairs_sequence(
    ctx: &Ctx,
    a: &PropertyPathExpression,
    b: &PropertyPathExpression,
) -> Result<Vec<(String, String)>, String> {
    let left = path_pairs(ctx, a)?;
    let right = path_pairs(ctx, b)?;
    let mut out = Vec::new();
    for (s, mid) in &left {
        for (rs, o) in &right {
            if rs == mid {
                out.push((s.clone(), o.clone()));
            }
        }
    }
    Ok(dedup_pairs(out))
}

/// The `ZeroOrOne(inner)` arm of [`path_pairs`]: `inner`'s pairs plus the identity
/// pair on every node (`x p? x`).
pub(super) fn path_pairs_zero_or_one(
    ctx: &Ctx,
    inner: &PropertyPathExpression,
) -> Result<Vec<(String, String)>, String> {
    let mut out = path_pairs(ctx, inner)?;
    for id in ctx.active.node_properties.keys() {
        let iri = ctx.proj.node_iri(id);
        out.push((iri.clone(), iri));
    }
    Ok(dedup_pairs(out))
}

/// Every `(subject, object)` resource pair carrying a typed edge whose projected
/// predicate IRI is NOT in `negated` — the negated property set `!p` (CONCEPT:EG-KG.ontology.negated-property-set).
pub(super) fn negated_edge_pairs(
    ctx: &Ctx,
    negated: &std::collections::HashSet<String>,
) -> Vec<(String, String)> {
    dedup_pairs(resource_edge_pairs_matching(ctx, |predicate| {
        !negated.contains(predicate)
    }))
}

/// Every `(subject, object)` resource pair carrying a typed edge whose projected
/// predicate IRI equals `pred` (the path predicate, already a full IRI from spargebra).
/// Subject/object are projected node IRIs so pairs match query terms + bind consistently.
pub(super) fn edge_pairs(ctx: &Ctx, pred: &str) -> Vec<(String, String)> {
    resource_edge_pairs_matching(ctx, |predicate| predicate == pred)
}

/// Project resource edges whose typed relationship satisfies the path predicate.
/// Stop at the first matching relationship per edge, matching both path arms.
fn resource_edge_pairs_matching(
    ctx: &Ctx,
    mut matches: impl FnMut(&str) -> bool,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for ((s, o), blobs) in &ctx.active.edge_properties {
        for blob in blobs {
            if let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) {
                if let Some(rel) = v.get("relationship").and_then(|x| x.as_str()) {
                    if matches(&ctx.proj.pred_iri(rel)) {
                        out.push((ctx.proj.node_iri(s), ctx.proj.node_iri(o)));
                        break;
                    }
                }
            }
        }
    }
    out
}

/// Transitive closure of `base` edge pairs. `reflexive` adds the identity pair on
/// EVERY graph node (the `p*` semantics: `x p* x` for any `x`, even isolated nodes).
pub(super) fn transitive_closure(
    base: &[(String, String)],
    reflexive: bool,
    ctx: &Ctx,
) -> Vec<(String, String)> {
    use std::collections::HashSet;
    let adj = path_pairs_adjacency(base);
    let starts: HashSet<&str> = base.iter().map(|(s, _)| s.as_str()).collect();
    let mut out = bfs_reachable_pairs(&adj, &starts);
    if reflexive {
        for id in ctx.active.node_properties.keys() {
            let iri = ctx.proj.node_iri(id);
            out.insert((iri.clone(), iri));
        }
    }
    out.into_iter().collect()
}

/// Adjacency list (`subject → objects`) over the `base` edge pairs, for the BFS in
/// [`bfs_reachable_pairs`].
pub(super) fn path_pairs_adjacency(
    base: &[(String, String)],
) -> std::collections::HashMap<&str, Vec<&str>> {
    let mut adj: std::collections::HashMap<&str, Vec<&str>> = std::collections::HashMap::new();
    for (s, o) in base {
        adj.entry(s.as_str()).or_default().push(o.as_str());
    }
    adj
}

/// BFS reachability (≥1 hop) from each of `starts` over `adj`, as `(start, reached)`
/// pairs.
pub(super) fn bfs_reachable_pairs<'a>(
    adj: &std::collections::HashMap<&'a str, Vec<&'a str>>,
    starts: &std::collections::HashSet<&'a str>,
) -> std::collections::HashSet<(String, String)> {
    let mut out = std::collections::HashSet::new();
    for &start in starts {
        let mut stack = vec![start];
        let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
        while let Some(cur) = stack.pop() {
            let Some(next) = adj.get(cur) else { continue };
            for &n in next {
                if visited.insert(n) {
                    out.insert((start.to_string(), n.to_string()));
                    stack.push(n);
                }
            }
        }
    }
    out
}

pub(super) fn dedup_pairs(v: Vec<(String, String)>) -> Vec<(String, String)> {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    v.into_iter().filter(|p| seen.insert(p.clone())).collect()
}

/// Resolve ONE triple pattern against the GraphView under the LPG→RDF projection
/// `proj`. Predicate may be an IRI or a variable; object an IRI/literal/variable;
/// subject an IRI/bnode/variable. Subject/object resource IRIs, property/edge predicate
/// IRIs, and the synthesized `rdf:type` object are all produced by `proj` so the
/// projected triples match the caller's vocabulary (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary).
pub(super) fn match_triple_pattern(ctx: &Ctx, tp: &TriplePattern) -> Vec<Solution> {
    let mut out = Vec::new();
    match_triple_pattern_edges(ctx, tp, &mut out);
    match_triple_pattern_nodes(ctx, tp, &mut out);
    out
}

/// EDGE patterns: object is a resource. Scan edges; project subject/predicate/object.
pub(super) fn match_triple_pattern_edges(ctx: &Ctx, tp: &TriplePattern, out: &mut Vec<Solution>) {
    let view = ctx.active;
    let proj = ctx.proj;
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
                continue;
            };
            let Some(rel) = v.get("relationship").and_then(|x| x.as_str()) else {
                continue;
            };
            let mut sol = Solution::new();
            if !bind_subject(&tp.subject, &proj.node_iri(s), &mut sol) {
                continue;
            }
            if !bind_predicate_iri(&tp.predicate, &proj.pred_iri(rel), &mut sol) {
                continue;
            }
            if !bind_object_node(&tp.object, &proj.node_iri(o), &mut sol) {
                continue;
            }
            out.push(sol);
        }
    }
}

/// NODE patterns: scan node property cells, yielding both the synthesized `rdf:type`
/// triple and one literal triple per remaining scalar property.
pub(super) fn match_triple_pattern_nodes(ctx: &Ctx, tp: &TriplePattern, out: &mut Vec<Solution>) {
    let view = ctx.active;
    let proj = ctx.proj;
    for (id, blob) in &view.node_properties {
        let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
            continue;
        };
        let Some(obj) = v.as_object() else { continue };
        let subj_iri = proj.node_iri(id);
        match_triple_pattern_type(ctx, tp, obj, &subj_iri, out);
        match_triple_pattern_literals(ctx, tp, obj, &subj_iri, out);
    }
}

/// `rdf:type` synthesis from the node `type`/`node_type` field. In the IDENTITY
/// projection this is `None` (no synthesis — `rdf:type` comes from explicit typing
/// edges, the prior behavior). Under a namespaced projection it yields `<subj>
/// rdf:type <base + CamelCase(type)>`, matching AU's materialization.
pub(super) fn match_triple_pattern_type(
    ctx: &Ctx,
    tp: &TriplePattern,
    obj: &serde_json::Map<String, serde_json::Value>,
    subj_iri: &str,
    out: &mut Vec<Solution>,
) {
    let Some(ty) = obj
        .get("type")
        .or_else(|| obj.get("node_type"))
        .and_then(|x| x.as_str())
    else {
        return;
    };
    let Some(type_obj) = ctx.proj.type_object_iri(ty) else {
        return;
    };
    let mut sol = Solution::new();
    if bind_subject(&tp.subject, subj_iri, &mut sol)
        && bind_predicate_iri(&tp.predicate, RDF_TYPE_IRI, &mut sol)
        && bind_object_node(&tp.object, &type_obj, &mut sol)
    {
        out.push(sol);
    }
}

/// LITERAL patterns: each scalar / typed-cell property (other than `type`/`node_type`,
/// emitted as `rdf:type` above / engine bookkeeping) → a literal triple.
pub(super) fn match_triple_pattern_literals(
    ctx: &Ctx,
    tp: &TriplePattern,
    obj: &serde_json::Map<String, serde_json::Value>,
    subj_iri: &str,
    out: &mut Vec<Solution>,
) {
    let proj = ctx.proj;
    for (k, cell) in literal_cells(obj) {
        let Some(lit_val) = cell_lexical(cell) else {
            continue;
        };
        let mut sol = Solution::new();
        if !bind_subject(&tp.subject, subj_iri, &mut sol) {
            continue;
        }
        if !bind_predicate_iri(&tp.predicate, &proj.pred_iri(k), &mut sol) {
            continue;
        }
        if !bind_object_literal(&tp.object, &lit_val, &mut sol) {
            continue;
        }
        out.push(sol);
    }
}

/// Every `(property key, literal cell)` a node blob asserts (EH-583): each ordinary
/// property plus EVERY value parked in the reserved multivalue cell, which is where a
/// subject's second-and-later values of one predicate live. `type`/`node_type` (the
/// `rdf:type` synthesis / engine bookkeeping) are not literal triples. Reading only the
/// ordinary keys would bind just the first value of a multi-valued property.
pub(super) fn literal_cells(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> impl Iterator<Item = (&str, &serde_json::Value)> {
    let extras = obj
        .get(RDF_MULTI_VALUE_KEY)
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flatten()
        .flat_map(|(k, cells)| {
            let values = cells.as_array().into_iter().flatten();
            values.map(move |cell| (k.as_str(), cell))
        });
    obj.iter()
        .filter(|(k, _)| !matches!(k.as_str(), "type" | "node_type" | RDF_MULTI_VALUE_KEY))
        .map(|(k, cell)| (k.as_str(), cell))
        .chain(extras)
}

/// The variable name a query blank node binds under. A blank node in a QUERY
/// pattern is a non-distinguished variable (SPARQL semantics) — spargebra desugars
/// a sequence property path `p1/p2` into a BGP whose intermediate is exactly such a
/// bnode. We key it in the solution by a reserved name so it joins like any other
/// variable but is dropped from the projected output (it isn't in `collect_vars`).
pub(super) fn bnode_var(b: &spargebra::term::BlankNode) -> String {
    format!("__bnode__{}", b.as_str())
}

pub(super) fn bind_subject(pat: &TermPattern, node_id: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::NamedNode(n) => format!("<{}>", n.as_str()) == node_id,
        TermPattern::BlankNode(b) => {
            sol.insert(bnode_var(b), Binding::Node(node_id.to_string()));
            true
        }
        _ => false,
    }
}

pub(super) fn bind_predicate_iri(
    pat: &NamedNodePattern,
    pred_iri: &str,
    sol: &mut Solution,
) -> bool {
    match pat {
        NamedNodePattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(pred_iri.to_string()));
            true
        }
        NamedNodePattern::NamedNode(n) => n.as_str() == pred_iri,
    }
}

pub(super) fn bind_object_node(pat: &TermPattern, node_id: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::NamedNode(n) => format!("<{}>", n.as_str()) == node_id,
        TermPattern::BlankNode(b) => {
            sol.insert(bnode_var(b), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::Literal(_) => false, // a literal pattern can't match a resource
        // RDF-star (CONCEPT:EG-KG.ontology.concept-5): a quoted-triple object pattern does not match a
        // plain resource node (LPG persistence of quoted triples is a documented
        // follow-up; quoted triples round-trip natively via parse/serialize).
        #[cfg(feature = "sparql-star")]
        TermPattern::Triple(_) => false,
    }
}

pub(super) fn bind_object_literal(pat: &TermPattern, lit_val: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(
                v.as_str().to_string(),
                Binding::Literal(lit_val.to_string()),
            );
            true
        }
        TermPattern::Literal(l) => l.value() == lit_val,
        _ => false, // a resource pattern can't match a literal
    }
}

/// Merge two solutions if they agree on every shared variable.
pub(super) fn merge(a: &Solution, b: &Solution) -> Option<Solution> {
    let mut out = a.clone();
    for (k, v) in b {
        match out.get(k) {
            Some(existing) if existing != v => return None,
            _ => {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Some(out)
}

pub(super) fn hash_join(l: &[Solution], r: &[Solution]) -> Vec<Solution> {
    let mut out = Vec::new();
    for a in l {
        for b in r {
            if let Some(m) = merge(a, b) {
                out.push(m);
            }
        }
    }
    out
}

pub(super) fn left_join(
    ctx: &Ctx,
    l: &[Solution],
    r: &[Solution],
    filter: Option<&Expression>,
) -> Vec<Solution> {
    let mut out = Vec::new();
    for a in l {
        let mut matched = false;
        for b in r {
            if let Some(m) = merge(a, b) {
                if filter.map(|e| eval_filter(ctx, e, &m)).unwrap_or(true) {
                    out.push(m);
                    matched = true;
                }
            }
        }
        if !matched {
            out.push(a.clone()); // OPTIONAL: keep the un-extended left solution
        }
    }
    out
}

// ── FILTER — a small expression evaluator (the increment's subset) ──────────────
