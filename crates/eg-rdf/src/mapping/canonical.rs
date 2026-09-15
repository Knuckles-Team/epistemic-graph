use super::*;

/// A canonical, bnode/order-insensitive comparison key for a quad set (mirrors
/// [`triple_set_key`], adding the graph name).
pub fn quad_set_key(quads: &[Quad]) -> BTreeSet<String> {
    quads
        .iter()
        .map(|q| {
            let g = match &q.graph_name {
                GraphName::NamedNode(n) => format!("<{}>", n.as_str()),
                GraphName::BlankNode(_) => "_:g".to_string(),
                GraphName::DefaultGraph => "default".to_string(),
            };
            let t = Triple::new(q.subject.clone(), q.predicate.clone(), q.object.clone());
            format!("{g} {}", canonical_triple_str(&t))
        })
        .collect()
}

/// A canonical, bnode/order-insensitive comparison key for a triple set: literals
/// keep datatype/lang; bnodes are normalized to a single placeholder so structural
/// equality holds (these datasets have no bnode-distinguishing structure).
pub fn triple_set_key(triples: &[Triple]) -> BTreeSet<String> {
    triples.iter().map(canonical_triple_str).collect()
}

fn canonical_term(t: &Term) -> String {
    match t {
        Term::NamedNode(n) => format!("<{}>", n.as_str()),
        Term::BlankNode(_) => "_:b".to_string(),
        Term::Literal(l) => {
            let mut parts = BTreeMap::new();
            parts.insert("v", l.value().to_string());
            parts.insert("d", l.datatype().as_str().to_string());
            if let Some(lang) = l.language() {
                parts.insert("l", lang.to_string());
            }
            format!("{parts:?}")
        }
        // RDF-star (CONCEPT:EG-KG.ontology.concept-5): a quoted-triple object contributes a canonical,
        // recursively-normalized `<<s p o>>` key so the round-trip set comparison is
        // meaningful (not collapsed to a placeholder).
        #[cfg(feature = "sparql-star")]
        Term::Triple(t) => {
            let s = match &t.subject {
                NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
                NamedOrBlankNode::BlankNode(_) => "_:b".to_string(),
            };
            format!(
                "<<{s} <{}> {}>>",
                t.predicate.as_str(),
                canonical_term(&t.object)
            )
        }
        #[allow(unreachable_patterns)]
        _ => "?".to_string(),
    }
}

fn canonical_triple_str(t: &Triple) -> String {
    let s = match &t.subject {
        NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        NamedOrBlankNode::BlankNode(_) => "_:b".to_string(),
        #[allow(unreachable_patterns)]
        _ => "?".to_string(),
    };
    format!(
        "{s} <{}> {}",
        t.predicate.as_str(),
        canonical_term(&t.object)
    )
}
