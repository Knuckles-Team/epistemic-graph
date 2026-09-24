//! Stage 2 for swarm topology, server half (SWARM-TOPOLOGY-DECIDE-DESIGN §4,
//! §5): admissibility entailed in-process under the request graph's composed
//! schema sources.
//!
//! The vocabulary is data. AU attaches it as the keyed `swarm-topology`
//! source (admin, `security:admin`) or ships it in a connector pack; a tenant
//! extends it under its own key. The engine knows exactly two anchor terms in
//! its own namespace: the role [`ADMITS_TOPOLOGY`] (a vocabulary states
//! `TaskShape ⊑ ∃ admits . TopologyClass`, with its own `admits` a
//! sub-property of the anchor) and the class [`NEEDS_INDEPENDENT_CHECK`].
//!
//! A template class `C` is admitted for task class `T` when the EL completion
//! entails `T ⊑ ∃ admitsTopology . D` for some `D` that `C` is subsumed by (a
//! class admitted as a whole admits its subclasses). Every fact is recorded in
//! the decision inputs with its proof axioms, so the pure decision function
//! and its replay read the same facts, and `OwlExplain` re-checks them.

use std::collections::BTreeSet;

use eg_types::contract::BoundedVec;
use eg_types::decision::{SchemaAuthority, TopologyAdmission};

/// The engine's admissibility role.
pub(crate) const ADMITS_TOPOLOGY: &str = "http://epistemic-graph/owl#admitsTopology";
/// The engine's "needs an independent check" task class.
pub(crate) const NEEDS_INDEPENDENT_CHECK: &str = "http://epistemic-graph/owl#NeedsIndependentCheck";

/// What the schema entailed for one topology question.
pub(super) struct Entailed {
    pub schema_digest: String,
    pub admissions: Vec<TopologyAdmission>,
    pub verify_required_by: Vec<String>,
}

fn key(iri: &str) -> String {
    format!("<{}>", iri.trim_start_matches('<').trim_end_matches('>'))
}

fn unkey(node: &str) -> String {
    node.trim_start_matches('<')
        .trim_end_matches('>')
        .to_string()
}

/// A vocabulary under a reserved importer prefix is a pack CLAIM; every other
/// source was attached by an admin.
fn authority_of(source_id: &str) -> SchemaAuthority {
    match eg_types::graph_schema::is_reserved_schema_source(source_id) {
        true => SchemaAuthority::Pack,
        false => SchemaAuthority::Admin,
    }
}

/// Every ontology triple of the composed sources, blank nodes scoped per
/// source exactly as the graph-view extractor scopes them, plus the weakest
/// authority among the sources that mention the admissibility role.
fn ontology(
    sources: &crate::graph::GraphSchemaSources,
) -> Result<(Vec<eg_rdf::oxrdf::Triple>, SchemaAuthority, String), String> {
    let mut triples = Vec::new();
    let mut authority = SchemaAuthority::Admin;
    let mut keys = BTreeSet::new();
    for (source_id, document) in sources.ontologies() {
        let scope = eg_types::contract::Digest256::sha256(source_id.as_bytes()).to_hex();
        if document.contains("admits") {
            keys.insert(source_id.to_string());
            authority = authority.max(authority_of(source_id));
        }
        for triple in eg_rdf::mapping::parse_turtle(document)? {
            triples.push(crate::server::graph_schema::compose::scope_blank_nodes(
                triple,
                &scope[..16],
            )?);
        }
    }
    let keys = keys.into_iter().collect::<Vec<_>>().join(",");
    Ok((triples, authority, keys))
}

/// Entail every admissibility fact for `task_classes` × `template_classes`.
pub(super) fn entail(
    core: &crate::graph::GraphCore,
    task_classes: &[String],
    template_classes: &[String],
) -> Result<Entailed, String> {
    let sources = core.schema_sources();
    crate::server::graph_schema::compose::validate_and_compose(&sources)?;
    let (triples, authority, source_key) = ontology(&sources)?;
    let classification = eg_rdf::owl::classify_hybrid_weighted(&triples);
    let reach = Reach {
        classification: &classification,
        authority,
        source_key,
    };
    let mut admissions: Vec<TopologyAdmission> = task_classes
        .iter()
        .flat_map(|task| {
            template_classes
                .iter()
                .filter_map(|class| reach.admits(task, class))
        })
        .collect();
    admissions.sort();
    admissions.dedup();
    let verify_required_by = task_classes
        .iter()
        .filter(|task| classification.entails_subclass(&key(task), &key(NEEDS_INDEPENDENT_CHECK)))
        .cloned()
        .collect();
    Ok(Entailed {
        schema_digest: format!("sha256:{}", sources.composed_digest().to_hex()),
        admissions,
        verify_required_by,
    })
}

struct Reach<'c> {
    classification: &'c eg_rdf::owl::Classification,
    authority: SchemaAuthority,
    source_key: String,
}

impl Reach<'_> {
    /// `task ⊑ ∃ admitsTopology . D` with `class ⊑ D`, and its proof axioms.
    fn admits(&self, task: &str, class: &str) -> Option<TopologyAdmission> {
        let witnesses = self.classification.roles.get(&key(ADMITS_TOPOLOGY))?;
        let (_, admitted) = witnesses
            .iter()
            .find(|(sub, filler)| *sub == key(task) && self.subsumed(&key(class), filler))?;
        Some(TopologyAdmission {
            task_class: task.to_string(),
            topology_class: class.to_string(),
            admit_class: unkey(admitted),
            source_key: self.source_key.clone(),
            authority: self.authority,
            axioms: self.axioms(&key(class), admitted),
        })
    }

    fn subsumed(&self, class: &str, filler: &str) -> bool {
        class == filler || self.classification.entails_subclass(class, filler)
    }

    /// The asserted axioms of `class ⊑ admitted`'s proof, bounded.
    fn axioms(&self, class: &str, admitted: &str) -> BoundedVec<String, 16> {
        let mut out = Vec::new();
        if let Some(proof) = self.classification.explain(class, admitted) {
            collect_axioms(&proof, &mut out);
        }
        out.sort();
        out.dedup();
        out.truncate(16);
        BoundedVec::new(out).expect("truncated to the bound")
    }
}

fn collect_axioms(node: &eg_rdf::owl::ProofNode, out: &mut Vec<String>) {
    out.extend(node.axioms.iter().cloned());
    for premise in &node.premises {
        collect_axioms(premise, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TBOX: &str = "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
        @prefix eg: <http://epistemic-graph/owl#> .\n\
        @prefix s: <http://knuckles.team/kg/swarm#> .\n\
        s:admits rdfs:subPropertyOf eg:admitsTopology .\n\
        s:Debate rdfs:subClassOf s:PeerTeam .\n\
        s:IndependentSubtasks rdfs:subClassOf [ a owl:Restriction ; owl:onProperty s:admits ; owl:someValuesFrom s:FanOutJoin ] .\n\
        s:NeedsNegotiation rdfs:subClassOf [ a owl:Restriction ; owl:onProperty s:admits ; owl:someValuesFrom s:PeerTeam ] .\n\
        s:NeedsIndependentCheck rdfs:subClassOf eg:NeedsIndependentCheck .\n";

    const S: &str = "http://knuckles.team/kg/swarm#";

    fn reach(classification: &eg_rdf::owl::Classification) -> Reach<'_> {
        Reach {
            classification,
            authority: SchemaAuthority::Admin,
            source_key: "swarm-topology".to_string(),
        }
    }

    #[test]
    fn a_task_admits_its_stated_class_and_that_class_s_subclasses_only() {
        let triples = eg_rdf::mapping::parse_turtle(TBOX).expect("parses");
        let classification = eg_rdf::owl::classify_hybrid_weighted(&triples);
        let reach = reach(&classification);
        let iri = |local: &str| format!("{S}{local}");
        assert!(reach
            .admits(&iri("IndependentSubtasks"), &iri("FanOutJoin"))
            .is_some());
        assert!(reach
            .admits(&iri("IndependentSubtasks"), &iri("PeerTeam"))
            .is_none());
        let debate = reach
            .admits(&iri("NeedsNegotiation"), &iri("Debate"))
            .expect("a subclass of an admitted class is admitted");
        assert_eq!(debate.admit_class, iri("PeerTeam"));
        assert!(
            !debate.axioms.is_empty(),
            "the subclass step carries its axiom"
        );
        assert!(classification.entails_subclass(
            &key(&iri("NeedsIndependentCheck")),
            &key(NEEDS_INDEPENDENT_CHECK)
        ));
    }
}
