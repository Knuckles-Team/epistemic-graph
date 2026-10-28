//! Statement row annotations, computed after the statement ran and from the SAME
//! snapshot: `WITH KNOWLEDGE` (EH-450) re-materializes each row as a
//! [`crate::knowledge::KnowledgeSet`] record, `WITH PROOF` (EH-448) attaches why the row
//! is in the result ([`super::proof`]). `EXPLAIN` executes nothing, so it has no rows to
//! annotate.

use eg_types::wire::{UqlKnowledge, UqlResult, UqlRow};

use super::{binding_plan, proof};
use crate::exec::PlanCtx;
use crate::knowledge::{KnowledgeRow, KnowledgeSet, ProvenanceFrame};
use crate::rowset::RowSet;
use crate::uql::Statement;

/// Attach the statement's requested annotations to `result`'s rows.
pub(super) fn annotate(
    result: &mut UqlResult,
    stmt: &Statement,
    ctx: &PlanCtx,
) -> Result<(), String> {
    let rows = match result {
        UqlResult::Rows { rows, .. } | UqlResult::Profile { rows, .. } => rows,
        UqlResult::Explain { .. } => return Ok(()),
    };
    if let Some(columns) = &stmt.annotations.knowledge {
        attach_knowledge(rows, columns, ctx)?;
    }
    if stmt.annotations.proof {
        proof::attach(rows, &binding_plan(stmt).ops, ctx)?;
    }
    Ok(())
}

fn attach_knowledge(rows: &mut [UqlRow], columns: &[String], ctx: &PlanCtx) -> Result<(), String> {
    let set = RowSet::from_rows(rows.iter().map(|r| (r.id.clone(), r.score)));
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    let knowledge = KnowledgeSet::from_rowset(&set, ctx.view, &columns);
    if knowledge.rows.len() != rows.len() {
        return Err("WITH KNOWLEDGE: the result repeats a row id".into());
    }
    ensure_row_identity(rows, &knowledge)?;
    let resolved = knowledge.provenance_frame == ProvenanceFrame::Resolved;
    for (row, record) in rows.iter_mut().zip(knowledge.rows) {
        row.knowledge = Some(wire_knowledge(record, resolved));
    }
    Ok(())
}

fn ensure_row_identity(rows: &[UqlRow], knowledge: &KnowledgeSet) -> Result<(), String> {
    if let Some((index, (row, record))) = rows
        .iter()
        .zip(knowledge.rows.iter())
        .enumerate()
        .find(|(_, (row, record))| row.id != record.id)
    {
        return Err(format!(
            "WITH KNOWLEDGE: row identity mismatch at position {index}: result `{}` vs knowledge `{}`",
            row.id, record.id
        ));
    }
    Ok(())
}

/// The wire form of one knowledge record.
fn wire_knowledge(record: KnowledgeRow, epistemic_resolved: bool) -> UqlKnowledge {
    UqlKnowledge {
        kind: record.kind,
        confidence: record.confidence,
        valid_from: record.valid_time.0,
        valid_until: record.valid_time.1,
        tx_from: record.tx_time.0,
        tx_until: record.tx_time.1,
        projection: record.projection,
        epistemic_resolved,
        source_refs: record.source_refs,
        evidence_refs: record
            .evidence_refs
            .iter()
            .map(|locus| locus.id.as_ref().to_string())
            .collect(),
        policy_labels: record.policy_labels,
        contradiction_ids: record.contradiction_ids,
        proof_ids: record.proof_ids,
        transformation_ids: record.transformation_ids,
        alternative_ids: record.alternative_ids,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str) -> UqlRow {
        UqlRow {
            id: id.into(),
            score: None,
            channels: Vec::new(),
            knowledge: None,
            proof: None,
        }
    }

    fn knowledge_row(id: &str) -> KnowledgeRow {
        KnowledgeRow {
            id: id.into(),
            kind: "Doc".into(),
            score: None,
            payload_ref: None,
            projection: None,
            valid_time: (None, None),
            tx_time: (None, None),
            confidence: 1.0,
            source_refs: Vec::new(),
            evidence_refs: Vec::new(),
            policy_labels: Vec::new(),
            contradiction_ids: Vec::new(),
            proof_ids: Vec::new(),
            transformation_ids: Vec::new(),
            alternative_ids: Vec::new(),
        }
    }

    #[test]
    fn refuses_knowledge_rows_with_different_identity_order() {
        let rows = [row("d1"), row("d2")];
        let knowledge = KnowledgeSet {
            rows: vec![knowledge_row("d2"), knowledge_row("d1")],
            ..KnowledgeSet::default()
        };

        assert_eq!(
            ensure_row_identity(&rows, &knowledge),
            Err("WITH KNOWLEDGE: row identity mismatch at position 0: result `d1` vs knowledge `d2`".into())
        );
    }
}
