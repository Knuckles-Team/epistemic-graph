//! `DecisionShape`: SHACL validation of a record, fail-closed, inside the
//! commit handler (DECIDE-LAYER-DESIGN §4.8, EH-071).
//!
//! Independent of the opt-in native-write ICV guard: every commit projects the
//! record to RDF and validates it against the shapes below, and a record that
//! does not conform -- or a validation run that cannot complete -- is refused.
//! The shapes stay inside what eg-shacl supports: SHACL Core plus
//! aggregate-free `sh:sparql`, with "winner ∈ options" written as
//! `OPTIONAL { … FILTER(?o = ?w) } FILTER(!BOUND(?o))` (eg-shacl rejects
//! `FILTER NOT EXISTS`).
//!
//! Terms are minted from hex-encoded identifiers, so no caller-supplied text is
//! ever spliced into the Turtle unescaped.

use std::fmt::Write as _;

use eg_types::decision::{DecisionOutcome, DecisionRecord};

const NS: &str = "urn:eg:decide:";

/// The shapes every committed assembly record must conform to.
const DECISION_SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix d: <urn:eg:decide:> .

d:DecisionRecordShape
  a sh:NodeShape ;
  sh:targetClass d:DecisionRecord ;
  sh:property [ sh:path d:evidenceClass ; sh:minCount 1 ; sh:maxCount 1 ;
                sh:in ( "proof" "observation" "claim" ) ] ;
  sh:property [ sh:path d:outcome ; sh:minCount 1 ; sh:maxCount 1 ;
                sh:in ( "solved" "abstained" ) ] ;
  sh:sparql [
    sh:message "a selected component is not one of the record's candidates" ;
    sh:select """
      SELECT $this ?value
      WHERE {
        $this <urn:eg:decide:selected> ?value .
        OPTIONAL { $this <urn:eg:decide:candidate> ?o . FILTER(?o = ?value) }
        FILTER(!BOUND(?o))
      }""" ;
  ] ;
  sh:sparql [
    sh:message "an eliminated component is selected" ;
    sh:select """
      SELECT $this ?value
      WHERE {
        $this <urn:eg:decide:selected> ?value .
        $this <urn:eg:decide:eliminated> ?value .
      }""" ;
  ] .
"#;

fn term(kind: &str, id: &str) -> String {
    format!("<{NS}{kind}:{}>", hex::encode(id.as_bytes()))
}

/// The RDF projection of `record`, as Turtle.
fn project(record: &DecisionRecord) -> String {
    let node = term("record", &record.record_id);
    let evidence = serde_json::to_value(record.evidence_class).unwrap_or_default();
    let outcome = match &record.outcome {
        DecisionOutcome::Solved { .. } => "solved",
        DecisionOutcome::Abstained { .. } => "abstained",
    };
    let mut out = format!(
        "{node} a <{NS}DecisionRecord> ; <{NS}evidenceClass> {evidence} ; <{NS}outcome> \"{outcome}\" .\n"
    );
    for candidate in &record.inputs.candidates {
        let _ = writeln!(
            out,
            "{node} <{NS}candidate> {} .",
            term("component", &candidate.component_id)
        );
    }
    for elimination in &record.eliminated {
        let _ = writeln!(
            out,
            "{node} <{NS}eliminated> {} .",
            term("component", &elimination.component_id)
        );
    }
    if let DecisionOutcome::Solved { slots, .. } = &record.outcome {
        for slot in slots {
            let _ = writeln!(
                out,
                "{node} <{NS}selected> {} .",
                term("component", &slot.component.component_id)
            );
        }
    }
    out
}

/// Validate `record` against [`DECISION_SHAPES`]; any non-conformance, or a
/// validation run that cannot complete, refuses the commit.
pub(crate) fn check_decision_shape(record: &DecisionRecord) -> Result<(), String> {
    let report = eg_shacl::validate_turtle(DECISION_SHAPES, &project(record))
        .map_err(|error| format!("DERIVATION_REJECTED: DecisionShape could not run: {error}"))?;
    if report.conforms {
        return Ok(());
    }
    let first = report
        .results
        .first()
        .map(|result| format!("{result:?}"))
        .unwrap_or_default();
    Err(format!(
        "DERIVATION_REJECTED: the record does not conform to DecisionShape: {first}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_component::AgentComponentKind;
    use eg_types::test_support::contract_wave::decision;

    fn solved() -> DecisionRecord {
        decision::record(
            decision::every_decision_outcome()
                .into_iter()
                .next()
                .expect("the solved outcome is first"),
        )
    }

    #[test]
    fn a_record_whose_winner_is_a_candidate_conforms() {
        check_decision_shape(&solved()).expect("the sample's slot is its candidate");
    }

    /// The gate catches a known-bad input: a winner outside the options.
    #[test]
    fn a_winner_outside_the_candidates_is_refused() {
        let mut record = solved();
        let DecisionOutcome::Solved { slots, .. } = &mut record.outcome else {
            unreachable!()
        };
        let mut forged = slots.as_slice().to_vec();
        forged[0].component = decision::dependency("component-elsewhere", AgentComponentKind::Tool);
        *slots = eg_types::contract::BoundedVec::new(forged).expect("bounded");
        let error = check_decision_shape(&record).expect_err("refused");
        assert!(error.starts_with("DERIVATION_REJECTED: "), "{error}");
    }

    #[test]
    fn an_eliminated_winner_is_refused() {
        let mut record = solved();
        let mut eliminated = record.eliminated.as_slice().to_vec();
        eliminated[0].component_id = "component-a".to_string();
        record.eliminated = eg_types::contract::BoundedVec::new(eliminated).expect("bounded");
        assert!(check_decision_shape(&record).is_err());
    }
}
