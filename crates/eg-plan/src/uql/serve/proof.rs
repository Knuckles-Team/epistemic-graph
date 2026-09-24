//! `WITH PROOF` row proofs (EH-448, the UQL surface of EH-197's proof-carrying results).
//!
//! Every stage of the statement bears on a row's proof in one of three ways
//! ([`ProofRole`]): it ORDERS, SCORES or CUTS rows it was given (`RANK`, `LIMIT`,
//! `RETURN`, … — nothing to prove); it ADMITS rows and can prove each admission — a
//! `SPARQL … VAR v` source (the witness of a solution binding `v` to the row: the ground
//! triples instantiating the query's patterns) or a `REASON` stage (the OWL proof of
//! class membership); or it admits rows WITHOUT a proof (`MATCH`, `WHERE`, `TRAVERSE`,
//! `AS OF`, …). A row's proof lists one step per admitting stage in pipeline order, and
//! is `complete` only when every admitting stage proved it and each such proof is itself
//! complete — a partial proof is never presented as a complete one. A proof-carrying
//! stage that cannot prove the row (the row reached the result through another stage)
//! contributes an `Unproved` step. A `FUSE` is neutral exactly when all its branch
//! stages are.

#[cfg(feature = "owl")]
use std::collections::HashMap;

use eg_types::wire::{op_kind, Op, ProofRole, UqlProofCoverage, UqlProofStep, UqlRow, UqlRowProof};

use super::stage_text;
use crate::exec::PlanCtx;

/// Attach a proof to every row, proving each stage's admissions once for all rows.
pub(super) fn attach(rows: &mut [UqlRow], ops: &[Op], ctx: &PlanCtx) -> Result<(), String> {
    let provers = ops
        .iter()
        .map(|op| Prover::for_op(op, ctx))
        .collect::<Result<Vec<_>, String>>()?;
    for row in rows.iter_mut() {
        row.proof = Some(row_proof(&row.id, ops, &provers));
    }
    Ok(())
}

fn row_proof(id: &str, ops: &[Op], provers: &[Prover]) -> UqlRowProof {
    let mut steps = Vec::new();
    let mut complete = true;
    for (op, prover) in ops.iter().zip(provers) {
        if let Some((step, certified)) = prover.step(op, id) {
            complete &= certified;
            steps.push(step);
        }
    }
    let coverage = if complete {
        UqlProofCoverage::Complete
    } else {
        UqlProofCoverage::Partial
    };
    UqlRowProof { coverage, steps }
}

/// A stage's role, looking into a `FUSE`'s branches.
fn proof_role(op: &Op) -> ProofRole {
    #[cfg(feature = "text")]
    if let Op::FuseRrf { branches, .. } = op {
        let neutral = branches
            .iter()
            .flatten()
            .all(|stage| proof_role(stage) == ProofRole::Neutral);
        return if neutral {
            ProofRole::Neutral
        } else {
            ProofRole::Unproved
        };
    }
    op_kind(op).proof_role()
}

/// How one stage proves rows.
enum Prover {
    /// Orders, scores or cuts rows: contributes no step.
    Neutral,
    /// Admits rows without a proof.
    Unproved,
    /// Row id → the witness of a SPARQL solution binding it (a complete one when any is).
    #[cfg(feature = "owl")]
    Sparql(HashMap<String, eg_types::rdf_report::SparqlRowProof>),
    /// The stage's OWL membership closure.
    #[cfg(feature = "owl")]
    Reason(Box<crate::exec::reason::ReasonMembership>),
}

impl Prover {
    fn for_op(op: &Op, ctx: &PlanCtx) -> Result<Self, String> {
        match proof_role(op) {
            ProofRole::Neutral => Ok(Prover::Neutral),
            ProofRole::Unproved => Ok(Prover::Unproved),
            ProofRole::Certifies => certifying(op, ctx),
        }
    }

    /// This stage's step for row `id` and whether it certifies it; `None` for a neutral
    /// stage.
    fn step(&self, op: &Op, id: &str) -> Option<(UqlProofStep, bool)> {
        // Only a proof-carrying stage reads the row id; a build without one has none.
        #[cfg(not(feature = "owl"))]
        let _ = id;
        let stage = stage_text(op);
        let unproved = |stage| (UqlProofStep::Unproved { stage }, false);
        match self {
            Prover::Neutral => None,
            Prover::Unproved => Some(unproved(stage)),
            #[cfg(feature = "owl")]
            Prover::Sparql(by_id) => Some(match by_id.get(id) {
                Some(proof) => {
                    let complete =
                        proof.coverage == eg_types::rdf_report::SparqlProofCoverage::Complete;
                    let proof = proof.clone();
                    (UqlProofStep::Sparql { stage, proof }, complete)
                }
                None => unproved(stage),
            }),
            #[cfg(feature = "owl")]
            Prover::Reason(membership) => Some(match membership.explain(id) {
                Some(proof) => (UqlProofStep::Reason { stage, proof }, true),
                None => unproved(stage),
            }),
        }
    }
}

/// The prover of a proof-carrying stage.
#[cfg(feature = "owl")]
fn certifying(op: &Op, ctx: &PlanCtx) -> Result<Prover, String> {
    match op {
        Op::SparqlBgp { query, var } => sparql_prover(ctx, query, var),
        Op::Reason {
            target_class,
            ontology,
        } => {
            let membership =
                crate::exec::reason::ReasonMembership::of(ctx.view, ctx.decay, target_class, ontology)?;
            Ok(Prover::Reason(Box::new(membership)))
        }
        _ => Ok(Prover::Unproved),
    }
}

/// Without the reasoner no stage can prove its admissions.
#[cfg(not(feature = "owl"))]
fn certifying(_op: &Op, _ctx: &PlanCtx) -> Result<Prover, String> {
    Ok(Prover::Unproved)
}

/// Re-evaluate the stage's SPARQL with witnesses and index them by the row `var` binds.
#[cfg(feature = "owl")]
fn sparql_prover(ctx: &PlanCtx, query: &str, var: &str) -> Result<Prover, String> {
    use eg_rdf::sparql::{execute_explained, Binding, Dataset, Projection};
    use eg_types::rdf_report::{SparqlProofCoverage, SparqlRowProof};

    let dataset = Dataset::new(ctx.view, Vec::new());
    let (table, proofs) = execute_explained(&dataset, query, &Projection::raw())?;
    let mut by_id: HashMap<String, SparqlRowProof> = HashMap::new();
    for (solution, proof) in table.solutions.iter().zip(proofs) {
        let Some(Binding::Node(id)) = solution.get(var) else {
            continue;
        };
        let replace = by_id
            .get(id)
            .is_none_or(|held| held.coverage != SparqlProofCoverage::Complete);
        if replace {
            by_id.insert(id.clone(), proof);
        }
    }
    Ok(Prover::Sparql(by_id))
}
