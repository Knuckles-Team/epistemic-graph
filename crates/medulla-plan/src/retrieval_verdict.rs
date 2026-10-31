//! Retrieval decisions (MEDULLA-FEDERATED-QUERY-R081): the sufficiency verdict and
//! its repair order (R081.2, R081.4), dual-score chunk selection (R081.7.2,
//! R081.7.4) and bounded typed-hierarchy descent (R081.9.2, R081.9.4).
//!
//! Every function here is pure over already computed inputs: selection makes
//! no model call, and descent scores only through the caller's
//! [`HierarchySource`], whose every call is counted against the policy.
//!
//! [`crate::leanrag`] stays the untyped ANN drill-down; the descent here is
//! the governed one: revision-checked, tenant-scoped at every stage, budgeted,
//! and it refuses instead of degrading to flat retrieval.

use std::collections::{BTreeMap, BTreeSet};

use medulla_types::retrieval_verdict::{
    ChunkScorePair, ChunkSelection, DescentPolicy, DescentReceipt, DescentStage, FallbackPolicy,
    HierarchyCommunity, HierarchyVisit, RepairStep, RetrievalRefusal, ScoreNormalization,
    ScoreRegion, SelectionPolicy, SelectionReceipt, SufficiencyVerdict, Verdict,
    RETRIEVAL_BUDGET_EXCEEDED, RETRIEVAL_HIERARCHY_INVALID, RETRIEVAL_NORMALIZATION_INVALID,
    RETRIEVAL_POLICY_UNVERIFIED, RETRIEVAL_ROUND_LIMIT_REQUIRED,
};

use crate::RowSet;

/// The default rule's name (MEDULLA-FEDERATED-QUERY-R081.2).
pub const MIN_EVIDENCE_COUNT: &str = "min_evidence_count";
/// The default rule's evidence threshold.
pub const DEFAULT_MIN_EVIDENCE: u64 = 3;
/// `BoostK` doubles `k` up to this cap.
pub const MAX_BOOST_K: u32 = 64;

/// The named, replaceable rule a verdict is judged under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SufficiencyRule {
    pub name: String,
    pub min_evidence: u64,
}

impl Default for SufficiencyRule {
    fn default() -> Self {
        Self {
            name: MIN_EVIDENCE_COUNT.to_string(),
            min_evidence: DEFAULT_MIN_EVIDENCE,
        }
    }
}

/// The round a verdict is asked for. `round_limit` is required; a call
/// without one refuses (MEDULLA-FEDERATED-QUERY-R081.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SufficiencyCall {
    /// 1-based round number.
    pub round: u32,
    pub round_limit: Option<u32>,
    /// The candidate count the round ran with.
    pub k: u32,
    /// The mode `SwitchMode` moves to.
    pub fallback_mode: String,
    pub fallback: FallbackPolicy,
}

/// Judge one round (MEDULLA-FEDERATED-QUERY-R081.2). Repairs follow the declared
/// order — `BoostK`, then `SwitchMode`, then `Stop` — and a round at its
/// limit always stops.
pub fn judge_sufficiency(
    rows: &RowSet,
    rule: &SufficiencyRule,
    call: &SufficiencyCall,
) -> Result<SufficiencyVerdict, RetrievalRefusal> {
    let round_limit = call.round_limit.ok_or_else(|| {
        RetrievalRefusal::new(RETRIEVAL_ROUND_LIMIT_REQUIRED, "round_limit is required")
    })?;
    let evidence_count = rows.len() as u64;
    let sufficient = evidence_count >= rule.min_evidence;
    let (verdict, repair, reason) = if sufficient {
        (
            Verdict::Sufficient,
            RepairStep::Stop,
            format!("{} met", rule.name),
        )
    } else {
        (
            Verdict::Insufficient,
            next_repair(call, round_limit),
            format!("{}: {evidence_count} < {}", rule.name, rule.min_evidence),
        )
    };
    Ok(SufficiencyVerdict {
        verdict,
        evidence_count,
        reason,
        repair,
        fallback: call.fallback,
        round: call.round,
        round_limit,
    })
}

fn next_repair(call: &SufficiencyCall, round_limit: u32) -> RepairStep {
    if call.round >= round_limit {
        return RepairStep::Stop;
    }
    match call.round {
        0 | 1 => RepairStep::BoostK {
            k: call.k.saturating_mul(2).clamp(1, MAX_BOOST_K),
        },
        2 => RepairStep::SwitchMode {
            mode: call.fallback_mode.clone(),
        },
        _ => RepairStep::Stop,
    }
}

/// What the harness may return under a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerMode {
    /// The evidence suffices.
    Answer,
    /// Insufficient under `Lenient`: an extractive answer from partial evidence.
    Extractive,
    /// Insufficient under `Strict`: abstain, no extractive answer.
    Abstain,
}

/// The answer mode a verdict allows (MEDULLA-FEDERATED-QUERY-R081.4).
pub fn answer_mode(verdict: &SufficiencyVerdict) -> AnswerMode {
    match (verdict.verdict, verdict.fallback) {
        (Verdict::Sufficient, _) => AnswerMode::Answer,
        (Verdict::Insufficient, FallbackPolicy::Lenient) => AnswerMode::Extractive,
        (Verdict::Insufficient, FallbackPolicy::Strict) => AnswerMode::Abstain,
    }
}

// ---------------------------------------------------------------------------
// Dual-score selection (R081.7.2, R081.7.4)
// ---------------------------------------------------------------------------

/// Select chunks from their already computed embedding and cross-encoder
/// scores (MEDULLA-FEDERATED-QUERY-R081.7.2). Activation requires the policy's
/// benchmark digest to equal `approved_benchmark` (R081.7.4).
pub fn select_chunks(
    policy: &SelectionPolicy,
    approved_benchmark: Option<&str>,
    pairs: &[ChunkScorePair],
) -> Result<SelectionReceipt, RetrievalRefusal> {
    policy.validate()?;
    check_activation(policy, approved_benchmark)?;
    policy.check_pairs(pairs)?;
    let embedding = normalize(
        &policy.normalization,
        pairs.iter().map(|p| p.embedding_score),
    )?;
    let relevance = normalize(
        &policy.normalization,
        pairs.iter().map(|p| p.cross_encoder_score),
    )?;
    let mut candidates: Vec<ChunkSelection> = pairs
        .iter()
        .zip(embedding.into_iter().zip(relevance))
        .map(|(pair, (e, r))| classify(policy, pair, e, r))
        .collect();
    let selected = rank_selected(&mut candidates, policy.max_chunks as usize);
    Ok(SelectionReceipt {
        policy_revision: policy.policy_revision.clone(),
        corpus_revision: policy.corpus_revision.clone(),
        normalization: policy.normalization.clone(),
        candidates,
        selected,
        max_chunks: policy.max_chunks,
        activation_benchmark_digest: policy.activation_benchmark_digest.clone(),
        selection_inference_calls: 0,
    })
}

fn check_activation(
    policy: &SelectionPolicy,
    approved_benchmark: Option<&str>,
) -> Result<(), RetrievalRefusal> {
    match (
        policy.activation_benchmark_digest.as_deref(),
        approved_benchmark,
    ) {
        (Some(declared), Some(approved)) if declared == approved => Ok(()),
        _ => Err(RetrievalRefusal::new(
            RETRIEVAL_POLICY_UNVERIFIED,
            "activation lacks the matching benchmark receipt",
        )),
    }
}

fn normalize(
    normalization: &ScoreNormalization,
    scores: impl Iterator<Item = f64>,
) -> Result<Vec<f64>, RetrievalRefusal> {
    let scores: Vec<f64> = scores.collect();
    match normalization {
        ScoreNormalization::Identity => {
            if let Some(out) = scores.iter().find(|s| !(0.0..=1.0).contains(*s)) {
                return Err(RetrievalRefusal::new(
                    RETRIEVAL_NORMALIZATION_INVALID,
                    format!("score {out} is outside [0, 1] under identity normalization"),
                ));
            }
            Ok(scores)
        }
        ScoreNormalization::MinMax { constant_value } => {
            let min = scores.iter().copied().fold(f64::INFINITY, f64::min);
            let max = scores.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            let span = max - min;
            Ok(scores
                .iter()
                .map(|s| {
                    if span > 0.0 {
                        (s - min) / span
                    } else {
                        *constant_value
                    }
                })
                .collect())
        }
    }
}

fn classify(policy: &SelectionPolicy, pair: &ChunkScorePair, e: f64, r: f64) -> ChunkSelection {
    let high_e = e >= policy.embedding_threshold;
    let high_r = r >= policy.relevance_threshold;
    let fused_score = policy.fusion_weight * e + (1.0 - policy.fusion_weight) * r;
    let (region, selected) = match (high_e, high_r) {
        (true, true) => (ScoreRegion::DualHigh, true),
        (false, false) => (ScoreRegion::DualLow, false),
        (false, true) => (ScoreRegion::Rescue, true),
        (true, false) => (
            ScoreRegion::Residual,
            fused_score >= policy.fusion_threshold,
        ),
    };
    ChunkSelection {
        chunk_id: pair.chunk_id.clone(),
        embedding_score: pair.embedding_score,
        cross_encoder_score: pair.cross_encoder_score,
        normalized_embedding: e,
        normalized_relevance: r,
        region,
        fused_score,
        selected,
    }
}

/// Order accepted candidates by fused score, ties by chunk id, and keep at
/// most `max`. Rejected candidates are never used to fill the count.
fn rank_selected(candidates: &mut [ChunkSelection], max: usize) -> Vec<String> {
    candidates.sort_by(|a, b| {
        b.fused_score
            .total_cmp(&a.fused_score)
            .then_with(|| a.chunk_id.cmp(&b.chunk_id))
    });
    let mut kept = 0;
    for candidate in candidates.iter_mut().filter(|c| c.selected) {
        if kept == max {
            candidate.selected = false;
        } else {
            kept += 1;
        }
    }
    candidates
        .iter()
        .filter(|c| c.selected)
        .map(|c| c.chunk_id.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Typed hierarchy descent (R081.9.2, R081.9.4)
// ---------------------------------------------------------------------------

/// One score from the caller's reranker, with what it cost.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StageScore {
    pub score: f64,
    pub tokens: u64,
    pub cost: u64,
}

/// The reads and scoring a descent is allowed to make.
pub trait HierarchySource {
    /// Score `id` at `stage` against the query.
    fn score(&mut self, stage: DescentStage, id: &str) -> StageScore;
    /// The tenant an entity belongs to; `None` when the entity does not exist.
    fn entity_tenant(&self, entity_id: &str) -> Option<String>;
}

/// A refused descent: the typed refusal and the stages completed before it.
#[derive(Debug, Clone, PartialEq)]
pub struct DescentRefusal {
    pub refusal: RetrievalRefusal,
    pub partial: DescentReceipt,
}

struct Descent<'a> {
    policy: &'a DescentPolicy,
    tenant: &'a str,
    children: BTreeMap<Option<&'a str>, Vec<&'a HierarchyCommunity>>,
    tokens: u64,
    receipt: DescentReceipt,
}

impl<'a> Descent<'a> {
    fn refuse(&mut self, code: &str, detail: String) -> Box<DescentRefusal> {
        self.receipt.incomplete = true;
        Box::new(DescentRefusal {
            refusal: RetrievalRefusal::new(code, detail),
            partial: self.receipt.clone(),
        })
    }

    fn charge(
        &mut self,
        source: &mut dyn HierarchySource,
        stage: DescentStage,
        id: &str,
    ) -> Result<f64, Box<DescentRefusal>> {
        if self.receipt.reranker_calls >= self.policy.max_reranker_calls {
            return Err(self.refuse(RETRIEVAL_BUDGET_EXCEEDED, "reranker calls".into()));
        }
        let scored = source.score(stage, id);
        self.receipt.reranker_calls += 1;
        self.tokens = self.tokens.saturating_add(scored.tokens);
        self.receipt.cost = self.receipt.cost.saturating_add(scored.cost);
        if self.tokens > self.policy.max_tokens {
            return Err(self.refuse(RETRIEVAL_BUDGET_EXCEEDED, "tokens".into()));
        }
        if self.receipt.cost > self.policy.max_cost {
            return Err(self.refuse(RETRIEVAL_BUDGET_EXCEEDED, "cost".into()));
        }
        Ok(scored.score)
    }

    /// Score, beam-prune and record one level of communities. Returns the kept ones with paths.
    fn level(
        &mut self,
        source: &mut dyn HierarchySource,
        stage: DescentStage,
        frontier: Vec<(&'a HierarchyCommunity, Vec<String>)>,
    ) -> Result<Vec<(&'a HierarchyCommunity, Vec<String>)>, Box<DescentRefusal>> {
        let mut scored = Vec::with_capacity(frontier.len());
        for (community, path) in frontier {
            if self.receipt.visited_communities >= self.policy.max_visited_communities {
                return Err(self.refuse(RETRIEVAL_BUDGET_EXCEEDED, "visited communities".into()));
            }
            self.receipt.visited_communities += 1;
            let score = self.charge(source, stage, &community.summary_id)?;
            scored.push((score, community, path));
        }
        scored.sort_by(|a, b| {
            b.0.total_cmp(&a.0)
                .then_with(|| a.1.community_id.cmp(&b.1.community_id))
        });
        let beam = self.policy.beam_width as usize;
        let mut kept = Vec::new();
        for (rank, (score, community, path)) in scored.into_iter().enumerate() {
            self.receipt.visits.push(HierarchyVisit {
                stage,
                id: community.community_id.clone(),
                score,
                score_provenance: "summary_rerank".into(),
                kept: rank < beam,
                path: path.clone(),
            });
            if rank < beam {
                kept.push((community, path));
            }
        }
        Ok(kept)
    }

    fn children_of(&self, parent: &str) -> Vec<(&'a HierarchyCommunity, Vec<String>)> {
        self.children
            .get(&Some(parent))
            .into_iter()
            .flatten()
            .copied()
            .filter(|c| c.tenant == self.tenant)
            .map(|c| (c, Vec::new()))
            .collect()
    }

    fn refine(
        &mut self,
        source: &mut dyn HierarchySource,
        mut kept: Vec<(&'a HierarchyCommunity, Vec<String>)>,
    ) -> Result<Vec<(&'a HierarchyCommunity, Vec<String>)>, Box<DescentRefusal>> {
        let mut leaves = Vec::new();
        while !kept.is_empty() {
            let mut frontier = Vec::new();
            for (community, path) in kept {
                let children = self.children_of(&community.community_id);
                if children.is_empty() {
                    leaves.push((community, path));
                    continue;
                }
                for (child, _) in children {
                    let mut child_path = path.clone();
                    child_path.push(child.community_id.clone());
                    frontier.push((child, child_path));
                }
            }
            if frontier.is_empty() {
                break;
            }
            if self.receipt.depth >= self.policy.max_depth {
                return Err(self.refuse(RETRIEVAL_BUDGET_EXCEEDED, "depth".into()));
            }
            self.receipt.depth += 1;
            kept = self.level(source, DescentStage::Community, frontier)?;
        }
        Ok(leaves)
    }

    fn entities(
        &mut self,
        source: &mut dyn HierarchySource,
        leaves: Vec<(&'a HierarchyCommunity, Vec<String>)>,
    ) -> Result<(), Box<DescentRefusal>> {
        let mut seen = BTreeSet::new();
        let mut candidates = Vec::new();
        for (community, path) in leaves {
            for entity in &community.entity_ids {
                match source.entity_tenant(entity) {
                    None => {
                        return Err(self.refuse(
                            RETRIEVAL_HIERARCHY_INVALID,
                            format!(
                                "community `{}` names dangling entity `{entity}`",
                                community.community_id
                            ),
                        ))
                    }
                    Some(tenant) if tenant != self.tenant => continue,
                    Some(_) => {}
                }
                if seen.insert(entity.clone()) {
                    candidates.push((entity.clone(), path.clone()));
                }
            }
        }
        let mut scored = Vec::with_capacity(candidates.len());
        for (entity, path) in candidates {
            if self.receipt.entity_candidates >= self.policy.max_entity_candidates {
                return Err(self.refuse(RETRIEVAL_BUDGET_EXCEEDED, "entity candidates".into()));
            }
            self.receipt.entity_candidates += 1;
            let score = self.charge(source, DescentStage::Entity, &entity)?;
            scored.push(HierarchyVisit {
                stage: DescentStage::Entity,
                id: entity,
                score,
                score_provenance: "entity_rerank".into(),
                kept: false,
                path,
            });
        }
        scored.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)));
        for (rank, visit) in scored.iter_mut().enumerate() {
            visit.kept = rank < self.policy.beam_width as usize;
        }
        self.receipt.entities = scored.iter().filter(|v| v.kept).cloned().collect();
        self.receipt.visits.extend(scored);
        Ok(())
    }
}

/// Descend a typed hierarchy for `tenant` (MEDULLA-FEDERATED-QUERY-R081.9.2):
/// score root summaries, refine kept communities level by level, then score
/// the leaves' entities. Every stage applies the tenant predicate before it
/// scores or truncates, and every bound refuses with the completed stages
/// rather than falling back to global retrieval (R081.9.4).
pub fn descend_hierarchy(
    policy: &DescentPolicy,
    tenant: &str,
    communities: &[HierarchyCommunity],
    source: &mut dyn HierarchySource,
) -> Result<DescentReceipt, Box<DescentRefusal>> {
    let mut descent = Descent {
        policy,
        tenant,
        children: BTreeMap::new(),
        tokens: 0,
        receipt: DescentReceipt {
            hierarchy_revision: policy.hierarchy_revision.clone(),
            policy_revision: policy.policy_revision.clone(),
            beam_width: policy.beam_width,
            depth: 0,
            visits: Vec::new(),
            visited_communities: 0,
            entity_candidates: 0,
            reranker_calls: 0,
            cost: 0,
            entities: Vec::new(),
            incomplete: false,
        },
    };
    let checked = policy.validate().and_then(|()| {
        medulla_types::retrieval_verdict::validate_hierarchy(communities, &policy.hierarchy_revision)
    });
    if let Err(refusal) = checked {
        return Err(descent.refuse(&refusal.code, refusal.detail));
    }
    for community in communities {
        descent
            .children
            .entry(community.parent_id.as_deref())
            .or_default()
            .push(community);
    }
    let roots: Vec<_> = descent
        .children
        .get(&None)
        .into_iter()
        .flatten()
        .copied()
        .filter(|c| c.tenant == tenant)
        .map(|c| (c, vec![c.community_id.clone()]))
        .collect();
    let kept = descent.level(source, DescentStage::Summary, roots)?;
    let leaves = descent.refine(source, kept)?;
    descent.entities(source, leaves)?;
    Ok(descent.receipt)
}

#[cfg(test)]
#[path = "retrieval_verdict_tests.rs"]
mod tests;
