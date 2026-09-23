use eg_core::graph::GraphView;
use eg_types::rdf_report::{SparqlObjectKind, SparqlProofCoverage, SparqlRowProof};

use super::super::{Dataset, Projection};
use super::execute_explained;

const EX: &str = "http://example.org/";

fn people() -> GraphView {
    super::super::tests::loaded_view()
}

fn explain(view: &GraphView, query: &str) -> (Vec<Vec<Option<String>>>, Vec<SparqlRowProof>) {
    let (table, proofs) =
        execute_explained(&Dataset::new(view, Vec::new()), query, &Projection::raw()).unwrap();
    (table.to_rows().1, proofs)
}

fn has_triple(proof: &SparqlRowProof, subject: &str, predicate: &str, object: &str) -> bool {
    proof.witnesses.iter().any(|t| {
        t.subject == format!("<{EX}{subject}>")
            && t.predicate == format!("{EX}{predicate}")
            && t.object == object
    })
}

/// Every row of a BGP + FILTER query is proved by triples of the graph, and the
/// witness satisfies the FILTER over a variable the SELECT does not project.
#[test]
fn a_filtered_bgp_row_is_proved_by_the_triples_that_match_it() {
    let view = people();
    let (rows, proofs) = explain(
        &view,
        r#"PREFIX ex: <http://example.org/>
           SELECT ?name WHERE { ?p a ex:Person . ?p ex:name ?name . ?p ex:age ?age .
                                FILTER (?age > 28) }"#,
    );
    assert_eq!(proofs.len(), rows.len());
    assert_eq!(rows.len(), 2);
    for (index, proof) in proofs.iter().enumerate() {
        assert_eq!(proof.row, index as u64);
        assert_eq!(proof.coverage, SparqlProofCoverage::Complete);
        assert_eq!(proof.witnesses.len(), 3);
        let name = rows[index][0].clone().unwrap();
        let (who, age) = match name.as_str() {
            "Alice" => ("alice", "30"),
            "Carol" => ("carol", "40"),
            other => panic!("unexpected row {other}"),
        };
        assert!(has_triple(proof, who, "name", &name));
        assert!(has_triple(proof, who, "age", age));
        let name_triple = proof.witnesses.iter().find(|t| t.object == name).unwrap();
        assert_eq!(name_triple.object_kind, SparqlObjectKind::Literal);
    }
}

/// An OPTIONAL that did not match contributes no triple — the witness never invents
/// one — and the row is still completely proved by its required patterns.
#[test]
fn an_unmatched_optional_adds_no_witness() {
    let view = people();
    let (rows, proofs) = explain(
        &view,
        r#"PREFIX ex: <http://example.org/>
           SELECT ?name ?friend WHERE { ?p ex:name ?name OPTIONAL { ?p ex:knows ?friend } }"#,
    );
    assert_eq!(rows.len(), 3);
    for (row, proof) in rows.iter().zip(&proofs) {
        assert_eq!(proof.coverage, SparqlProofCoverage::Complete);
        let expected = if row[1].is_some() { 2 } else { 1 };
        assert_eq!(proof.witnesses.len(), expected, "row {row:?}: {proof:?}");
        if let Some(friend) = &row[1] {
            let knows = proof
                .witnesses
                .iter()
                .find(|t| &t.object == friend)
                .unwrap();
            assert_eq!(knows.object_kind, SparqlObjectKind::Resource);
        }
    }
}

/// A UNION is algebra the witness does not certify: every row says so rather than
/// presenting a partial witness as a proof.
#[test]
fn uncertified_algebra_yields_partial_proofs() {
    let view = people();
    let (rows, proofs) = explain(
        &view,
        r#"PREFIX ex: <http://example.org/>
           SELECT ?x WHERE { { ?x ex:knows ex:bob } UNION { ?x ex:knows ex:alice } }"#,
    );
    assert_eq!(rows.len(), 2);
    assert!(proofs
        .iter()
        .all(|proof| proof.coverage == SparqlProofCoverage::Partial));
}

/// The proof table is aligned with the row table even when a row cannot be proved:
/// an ASK has one row and it is never certified.
#[test]
fn a_non_select_form_is_never_certified() {
    let view = people();
    let (rows, proofs) = explain(
        &view,
        r#"PREFIX ex: <http://example.org/> ASK { ex:alice ex:knows ex:bob }"#,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(proofs.len(), 1);
    assert_eq!(proofs[0].coverage, SparqlProofCoverage::Partial);
}
