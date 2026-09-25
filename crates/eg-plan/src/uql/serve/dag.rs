//! `LET … FROM/JOIN` programs under every mode (EH-449): rows with `RETURN` channels,
//! `EXPLAIN` and `PROFILE`, node by node.
//!
//! A program runs through [`crate::dag_exec::execute_dag`] exactly as written — there is
//! no DAG cost reordering — so `EXPLAIN` reports the program itself as both the canonical
//! and the optimized plan, with one stage per node (`#id <- #parents clause`). A node's
//! estimate is the cost model's output for its op given its input estimate: nothing for a
//! source, the parent's for one input, the smallest parent's for a `JOIN` (an
//! intersection never exceeds it). `PROFILE` and `RETURN` trace the run through
//! `execute_dag_with`'s per-node hook, which applies the same op dispatch and records each
//! node's rows, time and score channel; nodes run in the DAG's topological order, so when
//! two branches score the same channel for a row the later node's value is the one kept.

use std::time::Instant;

use eg_types::wire::{Op, UqlResult, UqlStageReport};

use super::{
    measured_federation, micros_since, planned_federation, record_channel, returned_channels,
    rows_of, stage_text, ChannelTable,
};
use crate::cost::{Cardinality, ModalityCardinality, PlanStats};
use crate::exec::{apply_with_channels, PlanCtx};
use crate::rowset::RowSet;
use crate::uql::DagNode;

/// Why a program's `EXPLAIN` reports `incremental: false`.
const DAG_NOTE: &str = "a LET … FROM/JOIN program runs node by node as written (no cost \
                        reordering) and is not incrementally maintained; EXPLAIN a binding \
                        as a pipeline to see either";

/// A program's rows: plain execution, or a traced run when it `RETURN`s channels.
pub(super) fn run(
    nodes: &[DagNode],
    ctx: &PlanCtx,
    warnings: Vec<String>,
) -> Result<UqlResult, String> {
    let columns = returned_channels(&node_ops(nodes));
    let (rows, table) = if columns.is_empty() {
        let rows = crate::dag_exec::execute_dag(&crate::uql::to_plan_dag(nodes), ctx)?;
        ctx.budget.check_result(rows.len())?;
        (rows, ChannelTable::new())
    } else {
        let trace = traced(nodes, ctx)?;
        (trace.rows, trace.table)
    };
    Ok(UqlResult::Rows {
        rows: rows_of(&rows, &columns, &table),
        columns,
        warnings,
    })
}

/// The program, its per-node estimates, and why it is not incrementally maintained.
pub(super) fn explain(
    nodes: &[DagNode],
    ctx: &PlanCtx,
    warnings: Vec<String>,
) -> Result<UqlResult, String> {
    let program = crate::uql::print::dag_to_uql(nodes).map_err(|e| e.to_string())?;
    let stages = reports(nodes, ctx, None);
    Ok(UqlResult::Explain {
        canonical: program.clone(),
        optimized: program,
        stages,
        incremental: false,
        incremental_note: DAG_NOTE.to_string(),
        federation: planned_federation(&node_ops(nodes)),
        warnings,
    })
}

/// Execute, recording each node's actual rows and time beside its estimate.
pub(super) fn profile(
    nodes: &[DagNode],
    ctx: &PlanCtx,
    warnings: Vec<String>,
) -> Result<UqlResult, String> {
    let trace = traced(nodes, ctx)?;
    let columns = returned_channels(&node_ops(nodes));
    Ok(UqlResult::Profile {
        rows: rows_of(&trace.rows, &columns, &trace.table),
        columns,
        stages: reports(nodes, ctx, Some(&trace.stages)),
        federation: measured_federation(ctx),
        warnings,
    })
}

fn node_ops(nodes: &[DagNode]) -> Vec<Op> {
    nodes.iter().map(|n| n.op.clone()).collect()
}

/// One stage report per node; `actual` carries each node's (rows, micros) under PROFILE.
fn reports(nodes: &[DagNode], ctx: &PlanCtx, actual: Option<&[(u64, u64)]>) -> Vec<UqlStageReport> {
    nodes
        .iter()
        .enumerate()
        .zip(node_estimates(nodes, ctx))
        .map(|((id, node), estimated_rows)| {
            let measured = actual.and_then(|a| a.get(id)).copied();
            UqlStageReport {
                stage: node_stage(id, node),
                estimated_rows,
                rows: measured.map(|(rows, _)| rows),
                micros: measured.map(|(_, micros)| micros),
            }
        })
        .collect()
}

/// `#id clause`, or `#id <- #p1,#p2 clause` for a node with inputs.
fn node_stage(id: usize, node: &DagNode) -> String {
    let clause = stage_text(&node.op);
    if node.inputs.is_empty() {
        return format!("#{id} {clause}");
    }
    let parents: Vec<String> = node.inputs.iter().map(|p| format!("#{p}")).collect();
    format!("#{id} <- {} {clause}", parents.join(","))
}

/// Estimated output rows per node (inputs precede their consumers in node order).
fn node_estimates(nodes: &[DagNode], ctx: &PlanCtx) -> Vec<f64> {
    let model = ModalityCardinality::new(PlanStats::collect(ctx));
    let mut out: Vec<f64> = Vec::with_capacity(nodes.len());
    for node in nodes {
        let input = node
            .inputs
            .iter()
            .filter_map(|&p| out.get(p).copied())
            .reduce(f64::min)
            .unwrap_or(0.0);
        out.push(model.rows_out(&node.op, input, ctx));
    }
    out
}

/// A node-by-node run of a program.
struct Trace {
    rows: RowSet,
    table: ChannelTable,
    /// (rows out, micros) per node id.
    stages: Vec<(u64, u64)>,
}

fn traced(nodes: &[DagNode], ctx: &PlanCtx) -> Result<Trace, String> {
    let dag = crate::uql::to_plan_dag(nodes);
    let mut table = ChannelTable::new();
    let mut stages = vec![(0, 0); nodes.len()];
    let rows = crate::dag_exec::execute_dag_with(&dag, ctx, |id, node, input| {
        let started = Instant::now();
        let (out, extra) = apply_with_channels(&node.op, input.clone(), ctx)?;
        if let Some(slot) = stages.get_mut(id) {
            *slot = (out.len() as u64, micros_since(started));
        }
        record_channel(&node.op, &out, &extra, &mut table);
        Ok(Some(out))
    })?;
    ctx.budget.check_result(rows.len())?;
    Ok(Trace {
        rows,
        table,
        stages,
    })
}
