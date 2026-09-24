//! Running a parsed UQL [`Statement`] (UQL-07/08/09): rows with named score channels,
//! `EXPLAIN` (canonical + optimized plan, per-stage estimated rows, incremental
//! maintainability) and `PROFILE` (execute, per-stage actual rows and time).
//!
//! The executor is unchanged: a plan without `RETURN` runs through [`crate::execute`]
//! exactly as `UnifiedQueryText` does. A plan WITH `RETURN` (or under `PROFILE`) runs the
//! optimized plan stage by stage through the same `apply` dispatch, recording after each
//! scoring stage its score under the stage's channel ([`OpKind::score_channel`]) — so the
//! channels are a side table keyed by row id and the `RowSet` currency itself is untouched.
//! Deterministic: the side table is ordered, the stage order is the optimizer's.

use std::collections::BTreeMap;
use std::time::Instant;

use eg_types::wire::{op_kind, Op, Plan, UqlResult, UqlRow, UqlStageReport};

use super::{Body, Mode, Statement};
use crate::cost::{Cardinality, ModalityCardinality, PlanStats};
use crate::exec::{apply, plan_optimize, PlanCtx};
use crate::rowset::RowSet;

// UQL-12, read-only by construction: the op dispatch every stage runs through takes the
// snapshot context by SHARED reference and returns a new RowSet — it has no path to a
// graph write. (The capability policy side is `eg-capabilities`' `uql_surfaces_are_read_only`.)
const _READ_ONLY_DISPATCH: for<'a> fn(&Op, RowSet, &PlanCtx<'a>) -> Result<RowSet, String> = apply;

/// Row id → channel → score.
type ChannelTable = BTreeMap<String, BTreeMap<&'static str, f32>>;

/// Every op of a statement, for the server's leg binding (text/spatial/foreign/tsdb
/// indexes are bound when ANY stage needs them, whatever the body shape).
pub fn binding_plan(stmt: &Statement) -> Plan {
    match &stmt.body {
        Body::Pipeline(plan) => plan.clone(),
        Body::Dag(nodes) => Plan::new(nodes.iter().map(|n| n.op.clone()).collect()),
    }
}

/// Run `stmt` over `ctx`.
pub fn run_statement(stmt: &Statement, ctx: &PlanCtx) -> Result<UqlResult, String> {
    let warnings: Vec<String> = stmt
        .warnings
        .iter()
        .map(|w| format!("{}: {}", w.code.as_str(), w.msg))
        .collect();
    match (&stmt.body, stmt.mode) {
        (Body::Pipeline(plan), Mode::Run) => run(plan, ctx, warnings),
        (Body::Pipeline(plan), Mode::Explain) => explain(plan, ctx, warnings),
        (Body::Pipeline(plan), Mode::Profile) => profile(plan, ctx, warnings),
        (Body::Dag(nodes), Mode::Run) => run_dag(nodes, ctx, warnings),
        (Body::Dag(_), Mode::Explain | Mode::Profile) => Err(
            "UQL_UNSUPPORTED: EXPLAIN/PROFILE of a LET … FROM/JOIN program is not supported; \
             explain each binding as a pipeline"
                .into(),
        ),
    }
}

/// The channels the LAST `RETURN` names (empty without one).
fn returned_channels(ops: &[Op]) -> Vec<String> {
    ops.iter()
        .rev()
        .find_map(|op| {
            if let Op::Project { channels } = op {
                Some(channels.clone())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn rows_of(rows: &RowSet, columns: &[String], table: &ChannelTable) -> Vec<UqlRow> {
    rows.rows()
        .iter()
        .map(|r| UqlRow {
            id: r.id.clone(),
            score: r.score,
            channels: columns
                .iter()
                .map(|c| table.get(&r.id).and_then(|m| m.get(c.as_str()).copied()))
                .collect(),
        })
        .collect()
}

fn run(plan: &Plan, ctx: &PlanCtx, warnings: Vec<String>) -> Result<UqlResult, String> {
    let columns = returned_channels(&plan.ops);
    if columns.is_empty() {
        let rows = crate::exec::execute(plan, ctx)?;
        return Ok(UqlResult::Rows {
            rows: rows_of(&rows, &columns, &ChannelTable::new()),
            columns,
            warnings,
        });
    }
    let traced = traced(plan, ctx)?;
    Ok(UqlResult::Rows {
        rows: rows_of(&traced.rows, &columns, &traced.table),
        columns,
        warnings,
    })
}

/// A stage-by-stage run of the optimized plan.
struct Traced {
    rows: RowSet,
    table: ChannelTable,
    /// (stage, rows out, micros) per stage.
    stages: Vec<(Op, u64, u64)>,
}

fn traced(plan: &Plan, ctx: &PlanCtx) -> Result<Traced, String> {
    let optimized = plan_optimize(plan.clone(), ctx);
    let mut cur = RowSet::new();
    let mut table = ChannelTable::new();
    let mut stages = Vec::with_capacity(optimized.ops.len());
    for op in &optimized.ops {
        let started = Instant::now();
        cur = apply(op, cur, ctx)?;
        let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        record_channel(op, &cur, &mut table);
        stages.push((op.clone(), cur.len() as u64, micros));
    }
    ctx.budget.check_result(cur.len())?;
    Ok(Traced {
        rows: cur,
        table,
        stages,
    })
}

fn record_channel(op: &Op, rows: &RowSet, table: &mut ChannelTable) {
    let Some(channel) = op_kind(op).score_channel() else {
        return;
    };
    for row in rows.rows() {
        if let Some(score) = row.score {
            table
                .entry(row.id.clone())
                .or_default()
                .insert(channel, score);
        }
    }
}

/// Per-stage estimated output rows under the cost model's catalog.
fn estimates(ops: &[Op], ctx: &PlanCtx) -> Vec<f64> {
    let model = ModalityCardinality::new(PlanStats::collect(ctx));
    let mut card = 0.0;
    ops.iter()
        .map(|op| {
            card = model.rows_out(op, card, ctx);
            card
        })
        .collect()
}

fn stage_text(op: &Op) -> String {
    eg_types::wire::uql_op(op).unwrap_or_else(|e| format!("<unprintable: {e}>"))
}

fn explain(plan: &Plan, ctx: &PlanCtx, warnings: Vec<String>) -> Result<UqlResult, String> {
    let optimized = plan_optimize(plan.clone(), ctx);
    let stages = optimized
        .ops
        .iter()
        .zip(estimates(&optimized.ops, ctx))
        .map(|(op, estimated_rows)| UqlStageReport {
            stage: stage_text(op),
            estimated_rows,
            rows: None,
            micros: None,
        })
        .collect();
    let (incremental, incremental_note) = match crate::incremental::Circuit::compile(plan) {
        Ok(_) => (true, String::new()),
        Err(why) => (false, why.to_string()),
    };
    Ok(UqlResult::Explain {
        canonical: plan.to_uql().map_err(|e| e.to_string())?,
        optimized: optimized.to_uql().map_err(|e| e.to_string())?,
        stages,
        incremental,
        incremental_note,
        warnings,
    })
}

fn profile(plan: &Plan, ctx: &PlanCtx, warnings: Vec<String>) -> Result<UqlResult, String> {
    let traced = traced(plan, ctx)?;
    let ops: Vec<Op> = traced.stages.iter().map(|(op, _, _)| op.clone()).collect();
    let stages = traced
        .stages
        .iter()
        .zip(estimates(&ops, ctx))
        .map(|((op, rows, micros), estimated_rows)| UqlStageReport {
            stage: stage_text(op),
            estimated_rows,
            rows: Some(*rows),
            micros: Some(*micros),
        })
        .collect();
    let columns = returned_channels(&plan.ops);
    Ok(UqlResult::Profile {
        rows: rows_of(&traced.rows, &columns, &traced.table),
        columns,
        stages,
        warnings,
    })
}

fn run_dag(
    nodes: &[super::DagNode],
    ctx: &PlanCtx,
    warnings: Vec<String>,
) -> Result<UqlResult, String> {
    let ops: Vec<Op> = nodes.iter().map(|n| n.op.clone()).collect();
    if !returned_channels(&ops).is_empty() {
        return Err(
            "UQL_UNSUPPORTED: RETURN channels are traced on pipelines, not on LET … \
                    FROM/JOIN programs"
                .into(),
        );
    }
    let rows = crate::dag_exec::execute_dag(&super::to_plan_dag(nodes), ctx)?;
    ctx.budget.check_result(rows.len())?;
    Ok(UqlResult::Rows {
        rows: rows_of(&rows, &[], &ChannelTable::new()),
        columns: Vec::new(),
        warnings,
    })
}
