//! `DECISIONS [WHERE pred]` (EH-066): the caller's visible decision log as a UQL source.
//!
//! Decision records are not graph nodes; they live in the engine's decision log. The
//! served path binds a [`DecisionSource`] that yields exactly the rows of the SQL
//! `decisions` relation the caller may read (visibility is applied before a row exists),
//! and this stage keeps the records whose columns satisfy every relational predicate —
//! the same three-valued evaluator the incremental circuit uses. Ids are record ids, in
//! record-id order; rows are unscored.

use serde_json::{Map, Value};

use super::PlanCtx;
use crate::pred_eval;
use crate::rowset::RowSet;
use eg_types::wire::Pred;

/// The caller's visible decision records, one JSON object per record carrying the
/// `decisions` relation's columns (`record_id`, `question_id`, `outcome`, …).
pub trait DecisionSource: Send + Sync {
    /// Every visible record, or why the log cannot be read.
    fn decision_rows(&self) -> Result<Vec<Map<String, Value>>, String>;
}

/// Run one `DECISIONS` stage (a source: its input is ignored, like `MATCH`).
pub(super) fn decision_scan(ctx: &PlanCtx, preds: &[Pred]) -> Result<RowSet, String> {
    if let Some(pred) = preds.iter().find(|p| !pred_eval::is_relational(p)) {
        return Err(format!(
            "DECISIONS filters on the record's columns with relational predicates only; \
             `{pred:?}` is not one"
        ));
    }
    let Some(source) = ctx.decisions else {
        return Err(
            "DECISIONS needs the caller's decision log, and none is bound to this plan".into(),
        );
    };
    let mut ids: Vec<String> = source
        .decision_rows()?
        .iter()
        .filter(|row| pred_eval::all_hold(row, preds))
        .filter_map(|row| row.get("record_id").and_then(Value::as_str))
        .map(str::to_string)
        .collect();
    ids.sort_unstable();
    Ok(RowSet::from_ids(ids))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::compute::semantic::SemanticStore;
    use eg_core::graph::GraphCore;
    use eg_types::wire::{CmpOp, PredLiteral};
    use serde_json::json;

    struct Log;

    impl DecisionSource for Log {
        fn decision_rows(&self) -> Result<Vec<Map<String, Value>>, String> {
            let rows = [
                json!({"record_id": "r2", "outcome": "acted", "committed_at_ms": 20}),
                json!({"record_id": "r1", "outcome": "abstained", "committed_at_ms": 10}),
                json!({"record_id": "r3", "outcome": "acted", "committed_at_ms": 30}),
            ];
            Ok(rows
                .into_iter()
                .filter_map(|v| v.as_object().cloned())
                .collect())
        }
    }

    fn run(preds: &[Pred], bound: bool) -> Result<Vec<String>, String> {
        let view = GraphCore::new().analysis_snapshot();
        let semantic = SemanticStore::new();
        let log = Log;
        let ctx = PlanCtx::new(&view, &semantic);
        let ctx = if bound { ctx.with_decisions(&log) } else { ctx };
        decision_scan(&ctx, preds).map(|rows| rows.ids())
    }

    #[test]
    fn predicates_select_records_in_record_id_order() {
        let acted = Pred::Eq {
            prop: "outcome".into(),
            value: "acted".into(),
        };
        assert_eq!(run(&[acted], true).unwrap(), ["r2", "r3"]);
        let late = Pred::Cmp {
            prop: "committed_at_ms".into(),
            op: CmpOp::Ge,
            value: PredLiteral::Num(20.0),
        };
        assert_eq!(run(&[late], true).unwrap(), ["r2", "r3"]);
        assert_eq!(run(&[], true).unwrap(), ["r1", "r2", "r3"]);
    }

    #[test]
    fn an_unbound_log_is_an_error_not_an_empty_set() {
        let error = run(&[], false).unwrap_err();
        assert!(error.contains("decision log"), "{error}");
    }
}
