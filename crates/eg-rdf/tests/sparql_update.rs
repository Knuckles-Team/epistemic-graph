//! SPARQL 1.1 UPDATE algebra coverage (EG-FEDERATED-QUERY-R707.1).
#![cfg(feature = "sparql")]

use eg_rdf::spargebra::GraphUpdateOperation as Op;
use eg_rdf::update::parse_update;

// spec: EG-FEDERATED-QUERY-R707.1
#[test]
fn update_forms_parse() {
    let g = "<http://ex/g>";
    let h = "<http://ex/h>";
    let forms: [(&str, String); 10] = [
        ("INSERT DATA", "INSERT DATA { <http://ex/s> <http://ex/p> <http://ex/o> }".into()),
        ("DELETE DATA", "DELETE DATA { <http://ex/s> <http://ex/p> <http://ex/o> }".into()),
        (
            "DELETE/INSERT WHERE",
            "DELETE { ?s <http://ex/p> ?o } INSERT { ?s <http://ex/q> ?o } WHERE { ?s <http://ex/p> ?o }"
                .into(),
        ),
        ("LOAD", "LOAD <http://ex/doc> INTO GRAPH <http://ex/g>".into()),
        ("CLEAR", format!("CLEAR GRAPH {g}")),
        ("CREATE", format!("CREATE GRAPH {g}")),
        ("DROP", format!("DROP GRAPH {g}")),
        ("COPY", format!("COPY {g} TO {h}")),
        ("MOVE", format!("MOVE {g} TO {h}")),
        ("ADD", format!("ADD {g} TO {h}")),
    ];
    for (name, text) in forms {
        let update = parse_update(&text).unwrap_or_else(|e| panic!("{name} failed to parse: {e}"));
        assert!(!update.operations.is_empty(), "{name} produced no operation");
        let ok = update.operations.iter().any(|op| match name {
            "INSERT DATA" => matches!(op, Op::InsertData { .. }),
            "DELETE DATA" => matches!(op, Op::DeleteData { .. }),
            "DELETE/INSERT WHERE" => matches!(op, Op::DeleteInsert { .. }),
            "LOAD" => matches!(op, Op::Load { .. }),
            "CLEAR" => matches!(op, Op::Clear { .. }),
            "CREATE" => matches!(op, Op::Create { .. }),
            "DROP" => matches!(op, Op::Drop { .. }),
            // COPY, MOVE and ADD are defined by the SPARQL spec as compositions of
            // the primitive operations; each must lower to at least one of them.
            _ => matches!(
                op,
                Op::Clear { .. } | Op::Drop { .. } | Op::DeleteInsert { .. } | Op::Create { .. }
            ),
        });
        assert!(ok, "{name} lowered to unexpected operations");
    }
    assert!(parse_update("FROBNICATE GRAPH <http://ex/g>").is_err());
}
