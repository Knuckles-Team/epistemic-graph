//! `DecisionLog.retrieval` generation operations (EH-397): dual-serve the
//! judged runs of the log against a shadow embedding generation, receipt the
//! comparison, and move a logical graph's generation pointer only with the
//! passing receipt measured against the generation active at that moment.
//!
//! Both generations are probed as the caller may see them (graph ACL at open,
//! row-level security on every reported row), so a receipt never summarises a
//! row the caller could not read.

use std::collections::BTreeMap;

use eg_numeric::decision::adapter::{to_q16, wilson_lower};
use eg_numeric::decision::retrieval::Verdict;
use eg_types::decision::statistical::body::{canonical_body_bytes, content_digest_of};
use eg_types::decision::statistical::retrieval_generation::{
    GenerationEvalItem, GenerationEvalRequest, GenerationEvaluated, GenerationReceipt,
    GENERATION_RECEIPT_SCHEMA_VERSION, MAX_GENERATION_TOP_K,
};
use eg_types::decision::statistical::StatisticalErrorCode;

use super::stat_executor::ExecutionContext;
use super::stat_log::LogReader;
use super::stat_pointer::{read_pointer, Qualified};
use super::stat_retrieval::{judged_outcomes, Scope};
use super::stat_support::refusal;
use super::stat_vectors::GraphVectors;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::{decode_artifact, encode_artifact};

const Q16: f64 = 65_536.0;
/// Bins of the top-1 score PSI.
const PSI_BINS: usize = 10;

fn invalid(detail: impl std::fmt::Display) -> String {
    refusal(StatisticalErrorCode::ParameterInvalid, detail)
}

/// The pointer key of a logical graph's generation.
pub(super) fn generation_pointer(logical: &str) -> String {
    format!("generation:{logical}")
}

fn receipt_key(receipt_digest: &str) -> String {
    format!("generation-receipt:{receipt_digest}")
}

/// The active and shadow generations an evaluation probes.
pub(super) struct Generations<'a> {
    pub(super) active: &'a GraphVectors,
    pub(super) shadow: &'a GraphVectors,
}

/// Per-generation tallies of one evaluation.
#[derive(Default)]
struct Tally {
    n: u64,
    wins: u64,
    losses: u64,
    ties: u64,
    active_rr: f64,
    shadow_rr: f64,
    active_top1: Vec<f64>,
    shadow_top1: Vec<f64>,
}

fn check_request(request: &GenerationEvalRequest) -> Result<(), String> {
    let top_k_ok = (1..=MAX_GENERATION_TOP_K).contains(&request.top_k);
    let named = !request.logical.is_empty() && request.active_graph != request.shadow_graph;
    if !(top_k_ok && named && request.min_eval_items > 0 && request.max_score_psi_q16 >= 0) {
        return Err(invalid(
            "a generation evaluation names a logical graph and two distinct generations, \
             1..=256 top-k, at least one item and a non-negative PSI bound",
        ));
    }
    Ok(())
}

fn to_f32(q16: &[i32]) -> Vec<f32> {
    q16.iter().map(|v| (f64::from(*v) / Q16) as f32).collect()
}

/// The reciprocal rank of the first cited row in `hits` (0 when none is).
fn reciprocal_rank(hits: &[(String, f32)], cited: &[String]) -> f64 {
    hits.iter()
        .position(|(id, _)| cited.contains(id))
        .map_or(0.0, |index| 1.0 / (index + 1) as f64)
}

fn top1(hits: &[(String, f32)]) -> Option<f64> {
    hits.first().map(|(_, score)| f64::from(*score))
}

impl Tally {
    fn add(&mut self, item: &GenerationEvalItem, cited: &[String], gens: &Generations, k: usize) {
        let active = gens.active.probe(&to_f32(item.active_q16.as_slice()), k);
        let shadow = gens.shadow.probe(&to_f32(item.shadow_q16.as_slice()), k);
        let (a, s) = (
            reciprocal_rank(&active, cited),
            reciprocal_rank(&shadow, cited),
        );
        self.n += 1;
        self.active_rr += a;
        self.shadow_rr += s;
        self.wins += u64::from(s > a);
        self.losses += u64::from(s < a);
        self.ties += u64::from(s == a);
        self.active_top1.extend(top1(&active));
        self.shadow_top1.extend(top1(&shadow));
    }

    fn mean(&self, total: f64) -> i64 {
        if self.n == 0 {
            return 0;
        }
        to_q16(total / self.n as f64)
    }
}

#[cfg(feature = "ann")]
fn score_psi(tally: &Tally) -> Option<i64> {
    let report =
        eg_ann::drift::population_stability_index(&tally.active_top1, &tally.shadow_top1, PSI_BINS);
    Some(to_q16(report.psi))
}

#[cfg(not(feature = "ann"))]
fn score_psi(tally: &Tally) -> Option<i64> {
    let _ = (&tally.active_top1, &tally.shadow_top1, PSI_BINS);
    None
}

/// Cited units of every independently judged successful run the caller sees.
fn judged_citations(
    store: &AgentLibraryStore,
    reader: &LogReader,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    Ok(judged_outcomes(store, reader, Scope::ALL)?
        .into_iter()
        .filter(|judged| judged.verdict == Verdict::Success)
        .map(|judged| {
            let outcome = judged.stored.outcome;
            (outcome.record_id, outcome.cited.iter().cloned().collect())
        })
        .collect())
}

fn spaces(request: &GenerationEvalRequest, gens: &Generations) -> Result<(String, String), String> {
    let (active, shadow) = (&request.active_space, &request.shadow_space);
    let named = !active.is_empty() && !shadow.is_empty() && active != shadow;
    if !(named && gens.active.admits_space(active) && gens.shadow.admits_space(shadow)) {
        return Err(invalid(
            "the shadow generation is a NEW embedding space, and each store declares \
             (if anything) exactly the space named for it",
        ));
    }
    Ok((active.clone(), shadow.clone()))
}

fn receipt_of(
    request: &GenerationEvalRequest,
    gens: &Generations,
    (active_space, shadow_space): (String, String),
    tally: &Tally,
) -> GenerationReceipt {
    let (active_embedded, shadow_embedded) = (gens.active.embedded(), gens.shadow.embedded());
    let lower = to_q16(wilson_lower(tally.wins, tally.wins + tally.losses));
    let score_psi_q16 = score_psi(tally);
    let passed = shadow_embedded >= active_embedded
        && tally.n >= u64::from(request.min_eval_items)
        && lower > to_q16(0.5)
        && score_psi_q16.is_some_and(|psi| psi <= request.max_score_psi_q16);
    GenerationReceipt {
        schema_version: GENERATION_RECEIPT_SCHEMA_VERSION,
        logical: request.logical.clone(),
        active_graph: request.active_graph.clone(),
        active_space,
        shadow_graph: request.shadow_graph.clone(),
        shadow_space,
        active_embedded,
        shadow_embedded,
        n_eval: tally.n,
        wins: tally.wins,
        losses: tally.losses,
        ties: tally.ties,
        active_mrr_q16: tally.mean(tally.active_rr),
        shadow_mrr_q16: tally.mean(tally.shadow_rr),
        win_rate_lower_q16: lower,
        score_psi_q16,
        passed,
    }
}

/// Dual-serve the judged runs `request` names and store the receipt under the
/// served tenant `tenant`.
pub(super) fn evaluate_generation(
    ctx: &ExecutionContext,
    reader: &LogReader,
    request: &GenerationEvalRequest,
    gens: &Generations,
    tenant: &str,
) -> Result<GenerationEvaluated, String> {
    check_request(request)?;
    let spaces = spaces(request, gens)?;
    let citations = judged_citations(ctx.store, reader)?;
    let mut tally = Tally::default();
    for item in &request.items {
        if let Some(cited) = citations.get(&item.record_id) {
            tally.add(item, cited, gens, usize::from(request.top_k));
        }
    }
    let receipt = receipt_of(request, gens, spaces, &tally);
    let receipt_digest = content_digest_of(&canonical_body_bytes(&receipt)?);
    let evaluated = GenerationEvaluated {
        receipt_digest,
        receipt,
    };
    let key = receipt_key(&evaluated.receipt_digest);
    ctx.store
        .put_decision_artifacts(tenant, &[(key, encode_artifact(&evaluated)?)])?;
    Ok(evaluated)
}

/// The shadow generation `logical` may move to: the named receipt passed, it
/// measured exactly this shadow for this logical graph, and it measured it
/// against the generation active NOW (a receipt against a stale baseline is
/// refused).
pub(super) fn qualified_generation(
    store: &AgentLibraryStore,
    tenant: &str,
    (logical, shadow_graph): (&str, &str),
    receipt_digest: &str,
) -> Result<Qualified, String> {
    let mismatch = |detail: &str| refusal(StatisticalErrorCode::EvaluationReceiptMismatch, detail);
    let bytes = store
        .decision_artifact(tenant, &receipt_key(receipt_digest))?
        .ok_or_else(|| mismatch("no generation receipt with that digest"))?;
    let evaluated: GenerationEvaluated = decode_artifact(&bytes, "generation receipt")?;
    let receipt = &evaluated.receipt;
    if !receipt.passed || receipt.logical != logical || receipt.shadow_graph != shadow_graph {
        return Err(mismatch(
            "the receipt did not pass, or measured another generation",
        ));
    }
    let (_, pointer) = read_pointer(store, tenant, &generation_pointer(logical))?;
    // The measured baseline must still be the active generation -- or the
    // shadow already is (a replayed activation, answered as a replay).
    let current = pointer.target().unwrap_or(logical);
    if current != receipt.active_graph && current != shadow_graph {
        return Err(mismatch(
            "the receipt measured a generation that is no longer the active one",
        ));
    }
    Ok(Qualified {
        target: shadow_graph.to_string(),
        receipt_digest: receipt_digest.to_string(),
    })
}
