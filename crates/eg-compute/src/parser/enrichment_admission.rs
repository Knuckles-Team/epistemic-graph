//! EH-557: deterministic post-parse admission for queued code enrichment.
//!
//! This is a planner, not a second executor. Rungs 0–2 run in the native
//! repository parser; rungs 3–5 are only candidates for EG's durable WorkItem
//! path. The caller must supply ENGINE-VERIFIED abstention and policy verdicts
//! for each digest. A serving adapter must bind those facts to committed rows
//! and submit the resulting candidates through the native WorkItem authority.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::epistemic_operations::RequestContext;
use eg_types::ingestion_wire::{
    ExtractedEdge, IndexFileOutcome, IndexFileStatus, NativeRungEvidence, ParseResult,
};
use eg_types::native_control::{
    NativeControlSchemaVersion, SubmitWorkItemRequest, SubmitWorkItemsRequest, MAX_SUBMIT_BATCH,
};
use sha2::{Digest, Sha256};

const MAX_OUTCOMES: usize = 4_096;
// A plan is one native transaction. Never admit more than its atomic command
// can carry: splitting later would leave only a prefix durable on failure.
const MAX_CANDIDATES: usize = MAX_SUBMIT_BATCH;

/// One content+parser version. The same bytes under two grammar capabilities
/// may have different parse outcomes, so a content digest alone is insufficient.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct UnitKey {
    pub content_digest: String,
    pub parser_capability_digest: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrichmentStage {
    Classical,
    Embedding,
    Llm,
}

impl EnrichmentStage {
    pub fn work_kind(self) -> &'static str {
        match self {
            Self::Classical => "enrichment.classical",
            Self::Embedding => "enrichment.embed",
            Self::Llm => "enrichment.llm",
        }
    }

    fn after_abstention(rung: u8) -> Option<Self> {
        match rung {
            2 => Some(Self::Classical),
            3 => Some(Self::Embedding),
            4 => Some(Self::Llm),
            _ => None,
        }
    }
}

/// Trusted policy input for one content+parser version. `abstained_through`
/// names the LAST rung that explicitly abstained; lower rungs may never be
/// overwritten by the next stage. `compute_units` is the admitted reservation,
/// not a latency estimate supplied by the content producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnitAdmission {
    pub abstained_through: u8,
    pub allowed: bool,
    pub compute_units: u64,
}

/// A budget/policy decision from the serving authority, never inferred from
/// a successful parse or supplied by a repository's content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativePolicyVerdict {
    pub allowed: bool,
    pub compute_units: u64,
}

/// Record whether native AST, symbol resolution and statistical passes ran
/// for each unique content+grammar unit. Raw parse facts include declarations
/// and unresolved imports/calls; any one prevents whole-unit promotion.
pub fn record_native_evidence(
    results: &[ParseResult],
    outcomes: &[IndexFileOutcome],
) -> Vec<NativeRungEvidence> {
    if results.len() != outcomes.len() {
        return Vec::new();
    }
    results
        .iter()
        .zip(outcomes)
        .map(|(result, outcome)| NativeRungEvidence {
            content_digest: outcome.content_digest.clone(),
            parser_capability_digest: outcome.parser_capability_digest.clone(),
            status: outcome.status,
            extracted_facts: result.nodes.len().saturating_add(result.edges.len()),
            inferred_facts: 0,
            derived_facts: 0,
            symbol_resolution_completed: outcome.status == IndexFileStatus::Success,
            statistical_completed: outcome.status == IndexFileStatus::Success,
        })
        .collect()
}

/// Attribute resolved and statistical edges back to every content unit they
/// touch. The caller includes symbol, blob and file-version endpoints. Unknown
/// provenance keeps that unit ineligible rather than silently treating it as
/// an abstention.
pub fn record_resolved_facts(
    evidence: &mut [NativeRungEvidence],
    unit_endpoints: &[Vec<String>],
    edges: &[ExtractedEdge],
) {
    if evidence.len() != unit_endpoints.len() {
        for item in evidence {
            item.symbol_resolution_completed = false;
            item.statistical_completed = false;
        }
        return;
    }
    let mut owners: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
    for (index, endpoints) in unit_endpoints.iter().enumerate() {
        for endpoint in endpoints {
            owners.entry(endpoint).or_default().insert(index);
        }
    }
    for edge in edges {
        let touched: BTreeSet<_> = owners
            .get(edge.source.as_str())
            .into_iter()
            .chain(owners.get(edge.target.as_str()))
            .flat_map(|indices| indices.iter().copied())
            .collect();
        for index in touched {
            let item = &mut evidence[index];
            match edge.properties.get("evidence_rung").map(String::as_str) {
                Some("EXTRACTED") => item.extracted_facts += 1,
                Some("INFERRED") => item.inferred_facts += 1,
                Some("DERIVED") => item.derived_facts += 1,
                _ => {
                    item.symbol_resolution_completed = false;
                    item.statistical_completed = false;
                }
            }
        }
    }
}

/// Produce a fail-closed rung-3 admission from engine-only evidence and a
/// serving policy verdict. A parse success alone is insufficient: all three
/// native passes must complete and assert no fact for the unit. This produces
/// no rung-4/5 admission; each worker must report its own explicit abstention.
pub fn native_admissions(
    evidence: &[NativeRungEvidence],
    policy: &BTreeMap<UnitKey, NativePolicyVerdict>,
) -> BTreeMap<UnitKey, UnitAdmission> {
    let mut decisions: BTreeMap<UnitKey, Option<UnitAdmission>> = BTreeMap::new();
    for item in evidence {
        let unit = UnitKey {
            content_digest: item.content_digest.clone(),
            parser_capability_digest: item.parser_capability_digest.clone(),
        };
        let decision = policy.get(&unit).and_then(|verdict| {
            (item.status == IndexFileStatus::Success
                && item.extracted_facts == 0
                && item.inferred_facts == 0
                && item.derived_facts == 0
                && item.symbol_resolution_completed
                && item.statistical_completed
                && verdict.allowed
                && verdict.compute_units > 0)
                .then_some(UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: verdict.compute_units,
                })
        });
        decisions
            .entry(unit)
            .and_modify(|prior| {
                if *prior != decision {
                    *prior = None;
                }
            })
            .or_insert(decision);
    }
    decisions
        .into_iter()
        .filter_map(|(unit, decision)| decision.map(|admission| (unit, admission)))
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichmentCandidate {
    pub unit: UnitKey,
    pub stage: EnrichmentStage,
    pub demanded: bool,
    pub reserved_compute_units: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionPlan {
    pub candidates: Vec<EnrichmentCandidate>,
    pub reserved_compute_units: u64,
    pub deferred_for_budget: usize,
    pub deferred_for_capacity: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    TooManyOutcomes,
    InvalidBudget,
    InvalidSourceRevision,
    InvalidPolicyDigest,
    StaleCursor,
}

/// Opaque position in one deterministic, policy-bound candidate ordering.
/// Persist it only with the source revision; changing evidence, admission,
/// demand or the per-page budget invalidates it rather than skipping work.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct EnrichmentCursor {
    fingerprint: String,
    next_index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichmentPage {
    pub plan: AdmissionPlan,
    pub next_cursor: Option<EnrichmentCursor>,
}

/// Engine-bound inputs required to submit one plan through the native,
/// all-or-nothing `SubmitWorkItems` transaction. `input_refs` must resolve to
/// durable source bytes; the content digest alone is not a worker payload.
pub struct QueueBinding {
    pub context: RequestContext,
    pub input_refs: BTreeMap<UnitKey, String>,
    pub policy_digest: String,
    pub catalog_digest: String,
    pub model_digest: String,
    pub source_commit_ref: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueBindingError {
    ExceedsAtomicBatch,
    EmptyPlan,
    MissingInput,
    MissingAuthority,
    InvalidPlan,
}

fn queue_digest(fields: &[&str]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"eg/repository-enrichment-work-item/v1");
    for field in fields {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

/// A public plan can also be constructed by a caller. Validate its admission
/// accounting before it can become an authority command.
fn validate_plan(plan: &AdmissionPlan) -> Result<(), QueueBindingError> {
    let mut seen = BTreeSet::new();
    let mut reserved = 0_u64;
    for candidate in &plan.candidates {
        if candidate.unit.content_digest.trim().is_empty()
            || candidate.unit.parser_capability_digest.trim().is_empty()
            || candidate.reserved_compute_units == 0
            || !seen.insert(&candidate.unit)
        {
            return Err(QueueBindingError::InvalidPlan);
        }
        reserved = reserved
            .checked_add(candidate.reserved_compute_units)
            .ok_or(QueueBindingError::InvalidPlan)?;
    }
    if reserved != plan.reserved_compute_units {
        return Err(QueueBindingError::InvalidPlan);
    }
    Ok(())
}

fn binding_has_authority(binding: &QueueBinding) -> bool {
    [
        &binding.context.tenant_id,
        &binding.context.graph,
        &binding.policy_digest,
        &binding.catalog_digest,
        &binding.model_digest,
        &binding.source_commit_ref,
    ]
    .into_iter()
    .all(|value| !value.trim().is_empty())
}

fn native_request(
    candidate: &EnrichmentCandidate,
    binding: &QueueBinding,
) -> Result<SubmitWorkItemRequest, QueueBindingError> {
    let input_ref = binding
        .input_refs
        .get(&candidate.unit)
        .filter(|value| !value.trim().is_empty())
        .ok_or(QueueBindingError::MissingInput)?;
    let digest = queue_digest(&[
        &binding.context.tenant_id,
        &binding.context.graph,
        &candidate.unit.content_digest,
        &candidate.unit.parser_capability_digest,
        candidate.stage.work_kind(),
        input_ref,
        &binding.policy_digest,
        &binding.catalog_digest,
        &binding.model_digest,
        &binding.source_commit_ref,
        &candidate.reserved_compute_units.to_string(),
        if candidate.demanded {
            "demanded"
        } else {
            "queued"
        },
    ]);
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "content_digest".into(),
        candidate.unit.content_digest.clone().into(),
    );
    metadata.insert(
        "parser_capability_digest".into(),
        candidate.unit.parser_capability_digest.clone().into(),
    );
    metadata.insert(
        "reserved_compute_units".into(),
        candidate.reserved_compute_units.into(),
    );
    Ok(SubmitWorkItemRequest {
        schema_version: NativeControlSchemaVersion::V1,
        context: binding.context.clone(),
        work_item_id: None,
        idempotency_key: format!("repository-enrichment:{digest}"),
        command_digest: digest,
        kind: candidate.stage.work_kind().into(),
        priority: if candidate.demanded { 1 } else { 0 },
        depends_on: Vec::new(),
        input_ref: input_ref.clone(),
        policy_digest: binding.policy_digest.clone(),
        catalog_digest: binding.catalog_digest.clone(),
        model_digest: binding.model_digest.clone(),
        max_attempts: 3,
        deadline_unix: None,
        metadata,
        provenance_refs: vec![binding.source_commit_ref.clone()],
        max_tenant_in_flight: 0,
    })
}

/// Lower one admitted plan to the existing native atomic queue command.
///
/// This does not submit or authenticate the command. The served adapter must
/// bind `QueueBinding` to the verified carrier and committed source rows, then
/// dispatch the result through the native WorkItem route. Refuse plans above
/// its atomic limit rather than silently split them into separate commits.
pub fn to_native_submit_batch(
    plan: &AdmissionPlan,
    binding: &QueueBinding,
) -> Result<SubmitWorkItemsRequest, QueueBindingError> {
    if plan.candidates.is_empty() {
        return Err(QueueBindingError::EmptyPlan);
    }
    if plan.candidates.len() > MAX_SUBMIT_BATCH {
        return Err(QueueBindingError::ExceedsAtomicBatch);
    }
    validate_plan(plan)?;
    if !binding_has_authority(binding) {
        return Err(QueueBindingError::MissingAuthority);
    }
    let mut requests = Vec::with_capacity(plan.candidates.len());
    for candidate in &plan.candidates {
        requests.push(native_request(candidate, binding)?);
    }
    let mut keys: Vec<_> = requests
        .iter()
        .map(|request| request.idempotency_key.as_str())
        .collect();
    keys.sort_unstable();
    let batch_digest = queue_digest(&keys);
    Ok(SubmitWorkItemsRequest {
        schema_version: NativeControlSchemaVersion::V1,
        context: binding.context.clone(),
        idempotency_key: format!("repository-enrichment-batch:{batch_digest}"),
        requests,
        enrichment_budget: None,
    })
}

/// Plan a bounded queue from successful native parse outcomes.
///
/// Duplicate membership of one blob is coalesced by digest+grammar. Conflicting
/// outcomes for the same unit are withheld. A demand signal reorders only
/// eligible work; it cannot bypass abstention, policy or budget. The result is
/// deterministic across input order and makes no network, model or graph call.
pub fn plan_enrichment(
    outcomes: &[IndexFileOutcome],
    admissions: &BTreeMap<UnitKey, UnitAdmission>,
    demanded: &BTreeSet<UnitKey>,
    budget_units: u64,
) -> Result<AdmissionPlan, AdmissionError> {
    plan_enrichment_page(
        outcomes,
        admissions,
        demanded,
        budget_units,
        "single-page",
        "single-page-policy",
        None,
    )
    .map(|page| page.plan)
}

fn validate_page_inputs(
    outcomes: &[IndexFileOutcome],
    budget_units: u64,
    source_revision: &str,
    policy_digest: &str,
) -> Result<(), AdmissionError> {
    if outcomes.len() > MAX_OUTCOMES {
        return Err(AdmissionError::TooManyOutcomes);
    }
    if budget_units == 0 {
        return Err(AdmissionError::InvalidBudget);
    }
    if source_revision.trim().is_empty() {
        return Err(AdmissionError::InvalidSourceRevision);
    }
    if policy_digest.trim().is_empty() {
        return Err(AdmissionError::InvalidPolicyDigest);
    }
    Ok(())
}

/// Select one atomic batch. Each continuation requires a fresh serving-policy
/// authorization for `budget_units`; this function never spends future pages'
/// budgets or submits their work. The cursor binds the exact eligible ordering
/// and refuses a changed source/policy/demand snapshot.
pub fn plan_enrichment_page(
    outcomes: &[IndexFileOutcome],
    admissions: &BTreeMap<UnitKey, UnitAdmission>,
    demanded: &BTreeSet<UnitKey>,
    budget_units: u64,
    source_revision: &str,
    policy_digest: &str,
    cursor: Option<&EnrichmentCursor>,
) -> Result<EnrichmentPage, AdmissionError> {
    validate_page_inputs(outcomes, budget_units, source_revision, policy_digest)?;
    let mut status: BTreeMap<UnitKey, Option<IndexFileStatus>> = BTreeMap::new();
    for outcome in outcomes {
        let key = UnitKey {
            content_digest: outcome.content_digest.clone(),
            parser_capability_digest: outcome.parser_capability_digest.clone(),
        };
        status
            .entry(key)
            .and_modify(|prior| {
                if *prior != Some(outcome.status) {
                    *prior = None;
                }
            })
            .or_insert(Some(outcome.status));
    }
    let mut eligible: Vec<_> = status
        .into_iter()
        .filter_map(|(unit, parsed)| (parsed == Some(IndexFileStatus::Success)).then_some(unit))
        .filter_map(|unit| {
            let admission = admissions.get(&unit)?;
            if !admission.allowed || admission.compute_units == 0 {
                return None;
            }
            let stage = EnrichmentStage::after_abstention(admission.abstained_through)?;
            Some(EnrichmentCandidate {
                demanded: demanded.contains(&unit),
                reserved_compute_units: admission.compute_units,
                unit,
                stage,
            })
        })
        .collect();
    eligible.sort_by(|a, b| b.demanded.cmp(&a.demanded).then(a.unit.cmp(&b.unit)));

    let fingerprint =
        candidate_fingerprint(&eligible, budget_units, source_revision, policy_digest);
    let start = match cursor {
        Some(cursor) if cursor.fingerprint == fingerprint && cursor.next_index < eligible.len() => {
            cursor.next_index
        }
        Some(_) => return Err(AdmissionError::StaleCursor),
        None => 0,
    };

    let mut plan = AdmissionPlan {
        candidates: Vec::new(),
        reserved_compute_units: 0,
        deferred_for_budget: 0,
        deferred_for_capacity: 0,
    };
    let mut next_index = None;
    for (index, candidate) in eligible.iter().enumerate().skip(start) {
        if plan.candidates.len() >= MAX_CANDIDATES {
            plan.deferred_for_capacity = eligible.len() - index;
            next_index = Some(index);
            break;
        }
        if candidate.reserved_compute_units > budget_units {
            // This unit cannot fit under the current policy, even on an empty
            // page. Retain a visible deferred count without stalling peers.
            plan.deferred_for_budget += 1;
            continue;
        }
        if candidate.reserved_compute_units > budget_units - plan.reserved_compute_units {
            plan.deferred_for_budget = eligible.len() - index;
            next_index = Some(index);
            break;
        }
        plan.reserved_compute_units += candidate.reserved_compute_units;
        plan.candidates.push(candidate.clone());
    }
    Ok(EnrichmentPage {
        plan,
        next_cursor: next_index.map(|next_index| EnrichmentCursor {
            fingerprint,
            next_index,
        }),
    })
}

fn candidate_fingerprint(
    candidates: &[EnrichmentCandidate],
    budget_units: u64,
    source_revision: &str,
    policy_digest: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"eg/repository-enrichment-page/v1");
    digest.update(budget_units.to_be_bytes());
    digest.update((source_revision.len() as u64).to_be_bytes());
    digest.update(source_revision.as_bytes());
    digest.update((policy_digest.len() as u64).to_be_bytes());
    digest.update(policy_digest.as_bytes());
    for candidate in candidates {
        for field in [
            candidate.unit.content_digest.as_str(),
            candidate.unit.parser_capability_digest.as_str(),
            candidate.stage.work_kind(),
        ] {
            digest.update((field.len() as u64).to_be_bytes());
            digest.update(field.as_bytes());
        }
        digest.update(candidate.reserved_compute_units.to_be_bytes());
        digest.update([u8::from(candidate.demanded)]);
    }
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::contract::BoundedVec;
    use eg_types::epistemic_operations::{
        RequestContextAuthenticationMethod, RequestContextSchemaVersion,
    };

    fn outcome(digest: &str, status: IndexFileStatus) -> IndexFileOutcome {
        IndexFileOutcome {
            file_path: format!("{digest}.py"),
            status,
            content_digest: digest.into(),
            parser_capability_digest: "grammar:v1".into(),
            diagnostics: BoundedVec::new(Vec::new()).unwrap(),
        }
    }

    fn key(digest: &str) -> UnitKey {
        UnitKey {
            content_digest: digest.into(),
            parser_capability_digest: "grammar:v1".into(),
        }
    }

    fn atomic_batch_fixture() -> (Vec<IndexFileOutcome>, BTreeMap<UnitKey, UnitAdmission>) {
        let outcomes: Vec<_> = (0..MAX_SUBMIT_BATCH + 2)
            .map(|index| outcome(&format!("blob-{index:03}"), IndexFileStatus::Success))
            .collect();
        let admissions = outcomes
            .iter()
            .map(|item| {
                (
                    key(&item.content_digest),
                    UnitAdmission {
                        abstained_through: 2,
                        allowed: true,
                        compute_units: 1,
                    },
                )
            })
            .collect();
        (outcomes, admissions)
    }

    fn binding() -> QueueBinding {
        QueueBinding {
            context: RequestContext {
                schema_version: RequestContextSchemaVersion::V2,
                request_id: "request".into(),
                subject_id: "subject".into(),
                tenant_id: "tenant".into(),
                agent_id: "indexer".into(),
                scopes: vec!["work:submit".into()],
                audience: "graph".into(),
                authentication_method: RequestContextAuthenticationMethod::LocalProcess,
                policy_version: "policy:v1".into(),
                graph: "code".into(),
                placement_epoch: None,
                trace_id: "trace".into(),
                issued_at_ms: 1,
                expires_at_ms: 2,
            },
            input_refs: BTreeMap::from([(key("a"), "blob:durable-a".into())]),
            policy_digest: "policy:digest".into(),
            catalog_digest: "catalog:digest".into(),
            model_digest: "model:digest".into(),
            source_commit_ref: "commit:123".into(),
        }
    }

    #[test]
    fn native_batch_identity_is_stable_and_requires_durable_input() {
        let plan = plan_enrichment(
            &[outcome("a", IndexFileStatus::Success)],
            &BTreeMap::from([(
                key("a"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 7,
                },
            )]),
            &BTreeSet::new(),
            7,
        )
        .unwrap();
        let mut binding = binding();
        let first = to_native_submit_batch(&plan, &binding).unwrap();
        assert_eq!(first, to_native_submit_batch(&plan, &binding).unwrap());
        assert_eq!(first.requests.len(), 1);
        assert_eq!(first.requests[0].command_digest.len(), 64);
        assert_eq!(first.requests[0].input_ref, "blob:durable-a");
        assert_eq!(first.requests[0].kind, "enrichment.classical");
        binding.input_refs.clear();
        assert_eq!(
            to_native_submit_batch(&plan, &binding).unwrap_err(),
            QueueBindingError::MissingInput
        );
    }

    #[test]
    fn native_batch_refuses_to_split_an_atomic_plan() {
        let candidate = EnrichmentCandidate {
            unit: key("a"),
            stage: EnrichmentStage::Classical,
            demanded: false,
            reserved_compute_units: 1,
        };
        let plan = AdmissionPlan {
            candidates: vec![candidate; MAX_SUBMIT_BATCH + 1],
            reserved_compute_units: (MAX_SUBMIT_BATCH + 1) as u64,
            deferred_for_budget: 0,
            deferred_for_capacity: 0,
        };
        assert_eq!(
            to_native_submit_batch(&plan, &binding()).unwrap_err(),
            QueueBindingError::ExceedsAtomicBatch
        );
    }

    #[test]
    fn demand_is_prioritized_but_cannot_bypass_policy_or_budget() {
        let outcomes = vec![
            outcome("a", IndexFileStatus::Success),
            outcome("b", IndexFileStatus::Success),
            outcome("c", IndexFileStatus::Success),
        ];
        let admissions = BTreeMap::from([
            (
                key("a"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 4,
                },
            ),
            (
                key("b"),
                UnitAdmission {
                    abstained_through: 3,
                    allowed: true,
                    compute_units: 4,
                },
            ),
            (
                key("c"),
                UnitAdmission {
                    abstained_through: 4,
                    allowed: false,
                    compute_units: 1,
                },
            ),
        ]);
        let demanded = BTreeSet::from([key("b"), key("c")]);
        let plan = plan_enrichment(&outcomes, &admissions, &demanded, 4).unwrap();
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].unit, key("b"));
        assert_eq!(plan.candidates[0].stage.work_kind(), "enrichment.embed");
        assert_eq!(plan.reserved_compute_units, 4);
        assert_eq!(plan.deferred_for_budget, 1);
    }

    #[test]
    fn only_explicit_abstention_advances_the_ladder() {
        let outcomes = vec![outcome("a", IndexFileStatus::Success)];
        for (abstained, expected) in [
            (0, None),
            (1, None),
            (2, Some("enrichment.classical")),
            (3, Some("enrichment.embed")),
            (4, Some("enrichment.llm")),
            (5, None),
        ] {
            let admissions = BTreeMap::from([(
                key("a"),
                UnitAdmission {
                    abstained_through: abstained,
                    allowed: true,
                    compute_units: 1,
                },
            )]);
            let plan = plan_enrichment(&outcomes, &admissions, &BTreeSet::new(), 1).unwrap();
            assert_eq!(
                plan.candidates.first().map(|item| item.stage.work_kind()),
                expected
            );
        }
    }

    #[test]
    fn duplicate_or_conflicting_blob_outcomes_are_conservative() {
        let good = outcome("a", IndexFileStatus::Success);
        let admissions = BTreeMap::from([(
            key("a"),
            UnitAdmission {
                abstained_through: 2,
                allowed: true,
                compute_units: 1,
            },
        )]);
        let same = [good.clone(), good];
        assert_eq!(
            plan_enrichment(&same, &admissions, &BTreeSet::new(), 1)
                .unwrap()
                .candidates
                .len(),
            1
        );
        let mixed = [
            outcome("a", IndexFileStatus::Success),
            outcome("a", IndexFileStatus::Error),
        ];
        assert!(plan_enrichment(&mixed, &admissions, &BTreeSet::new(), 1)
            .unwrap()
            .candidates
            .is_empty());
    }

    #[test]
    fn ordering_is_independent_of_membership_order() {
        let admissions = BTreeMap::from([
            (
                key("a"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
            (
                key("b"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
        ]);
        let first = [
            outcome("a", IndexFileStatus::Success),
            outcome("b", IndexFileStatus::Success),
        ];
        let second = [
            outcome("b", IndexFileStatus::Success),
            outcome("a", IndexFileStatus::Success),
        ];
        assert_eq!(
            plan_enrichment(&first, &admissions, &BTreeSet::new(), 2),
            plan_enrichment(&second, &admissions, &BTreeSet::new(), 2)
        );
    }

    #[test]
    fn parser_capability_is_part_of_the_queue_identity() {
        let mut next_grammar = outcome("a", IndexFileStatus::Success);
        next_grammar.parser_capability_digest = "grammar:v2".into();
        let v2 = UnitKey {
            parser_capability_digest: "grammar:v2".into(),
            ..key("a")
        };
        let admissions = BTreeMap::from([
            (
                key("a"),
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
            (
                v2,
                UnitAdmission {
                    abstained_through: 2,
                    allowed: true,
                    compute_units: 1,
                },
            ),
        ]);
        let plan = plan_enrichment(
            &[outcome("a", IndexFileStatus::Success), next_grammar],
            &admissions,
            &BTreeSet::new(),
            2,
        )
        .unwrap();
        assert_eq!(plan.candidates.len(), 2);
    }

    // spec: EG-TYPED-PACKS-R010
    #[test]
    fn planner_never_exceeds_one_atomic_native_batch() {
        let (outcomes, admissions) = atomic_batch_fixture();
        let plan = plan_enrichment(
            &outcomes,
            &admissions,
            &BTreeSet::new(),
            outcomes.len() as u64,
        )
        .unwrap();
        assert_eq!(plan.candidates.len(), MAX_SUBMIT_BATCH);
        assert_eq!(plan.deferred_for_capacity, 2);
        assert_eq!(plan.reserved_compute_units, MAX_SUBMIT_BATCH as u64);
    }

    // spec: EG-TYPED-PACKS-R010
    #[test]
    fn continuation_drains_more_than_one_atomic_batch_without_duplicates() {
        let (outcomes, admissions) = atomic_batch_fixture();
        let first = plan_enrichment_page(
            &outcomes,
            &admissions,
            &BTreeSet::new(),
            MAX_SUBMIT_BATCH as u64,
            "repo-revision-1",
            "policy-1",
            None,
        )
        .unwrap();
        assert_eq!(first.plan.candidates.len(), MAX_SUBMIT_BATCH);
        assert_eq!(first.plan.deferred_for_capacity, 2);
        let second = plan_enrichment_page(
            &outcomes,
            &admissions,
            &BTreeSet::new(),
            MAX_SUBMIT_BATCH as u64,
            "repo-revision-1",
            "policy-1",
            first.next_cursor.as_ref(),
        )
        .unwrap();
        assert_eq!(second.plan.candidates.len(), 2);
        assert_eq!(second.next_cursor, None);
        let unique: BTreeSet<_> = first
            .plan
            .candidates
            .iter()
            .chain(&second.plan.candidates)
            .map(|item| item.unit.clone())
            .collect();
        assert_eq!(unique.len(), outcomes.len());
        let persisted = rmp_serde::to_vec(first.next_cursor.as_ref().unwrap()).unwrap();
        let resumed: EnrichmentCursor = rmp_serde::from_slice(&persisted).unwrap();
        assert_eq!(Some(&resumed), first.next_cursor.as_ref());
        assert_eq!(
            first,
            plan_enrichment_page(
                &outcomes.iter().rev().cloned().collect::<Vec<_>>(),
                &admissions,
                &BTreeSet::new(),
                MAX_SUBMIT_BATCH as u64,
                "repo-revision-1",
                "policy-1",
                None,
            )
            .unwrap()
        );
    }

    #[test]
    fn continuation_preserves_demand_and_page_budget() {
        let outcomes: Vec<_> = ["a", "b", "c"]
            .into_iter()
            .map(|name| outcome(name, IndexFileStatus::Success))
            .collect();
        let admissions = outcomes
            .iter()
            .map(|item| {
                (
                    key(&item.content_digest),
                    UnitAdmission {
                        abstained_through: 2,
                        allowed: true,
                        compute_units: 2,
                    },
                )
            })
            .collect();
        let demanded = BTreeSet::from([key("c")]);
        let first = plan_enrichment_page(
            &outcomes,
            &admissions,
            &demanded,
            2,
            "repo-revision-1",
            "policy-1",
            None,
        )
        .unwrap();
        assert_eq!(first.plan.candidates[0].unit, key("c"));
        assert_eq!(first.plan.reserved_compute_units, 2);
        assert_eq!(first.plan.deferred_for_budget, 2);
        let second = plan_enrichment_page(
            &outcomes,
            &admissions,
            &demanded,
            2,
            "repo-revision-1",
            "policy-1",
            first.next_cursor.as_ref(),
        )
        .unwrap();
        assert_eq!(second.plan.candidates[0].unit, key("a"));
        assert_eq!(second.plan.reserved_compute_units, 2);
        let third = plan_enrichment_page(
            &outcomes,
            &admissions,
            &demanded,
            2,
            "repo-revision-1",
            "policy-1",
            second.next_cursor.as_ref(),
        )
        .unwrap();
        assert_eq!(third.plan.candidates[0].unit, key("b"));
        assert!(third.next_cursor.is_none());
        assert_eq!(
            plan_enrichment_page(
                &outcomes,
                &admissions,
                &BTreeSet::new(),
                2,
                "repo-revision-1",
                "policy-1",
                first.next_cursor.as_ref()
            ),
            Err(AdmissionError::StaleCursor)
        );
        assert_eq!(
            plan_enrichment_page(
                &outcomes,
                &admissions,
                &demanded,
                3,
                "repo-revision-1",
                "policy-1",
                first.next_cursor.as_ref()
            ),
            Err(AdmissionError::StaleCursor)
        );
        assert_eq!(
            plan_enrichment_page(
                &outcomes,
                &admissions,
                &demanded,
                2,
                "repo-revision-2",
                "policy-1",
                first.next_cursor.as_ref()
            ),
            Err(AdmissionError::StaleCursor)
        );
        assert_eq!(
            plan_enrichment_page(
                &outcomes,
                &admissions,
                &demanded,
                2,
                "repo-revision-1",
                "policy-2",
                first.next_cursor.as_ref()
            ),
            Err(AdmissionError::StaleCursor)
        );
    }

    #[test]
    fn native_queue_refuses_tampered_or_duplicate_plans() {
        let candidate = EnrichmentCandidate {
            unit: key("a"),
            stage: EnrichmentStage::Classical,
            demanded: false,
            reserved_compute_units: 1,
        };
        let mut plan = AdmissionPlan {
            candidates: vec![candidate.clone()],
            reserved_compute_units: 2,
            deferred_for_budget: 0,
            deferred_for_capacity: 0,
        };
        assert_eq!(
            to_native_submit_batch(&plan, &binding()).unwrap_err(),
            QueueBindingError::InvalidPlan
        );
        plan.candidates.push(candidate);
        assert_eq!(
            to_native_submit_batch(&plan, &binding()).unwrap_err(),
            QueueBindingError::InvalidPlan
        );
    }

    #[test]
    fn native_admission_requires_three_explicit_abstentions_and_policy() {
        let empty = || ParseResult {
            nodes: Vec::new(),
            edges: Vec::new(),
            symbols_extracted: 0,
        };
        let asserted = ParseResult {
            nodes: vec![eg_types::ingestion_wire::ExtractedNode {
                node_id: "symbol:a".into(),
                node_type: "SYMBOL".into(),
                properties: Default::default(),
            }],
            edges: Vec::new(),
            symbols_extracted: 1,
        };
        let outcomes = [
            outcome("empty", IndexFileStatus::Success),
            outcome("asserted", IndexFileStatus::Success),
            outcome("unsupported", IndexFileStatus::Unsupported),
        ];
        let mut evidence = record_native_evidence(&[empty(), asserted, empty()], &outcomes);
        let policy = BTreeMap::from([
            (
                key("empty"),
                NativePolicyVerdict {
                    allowed: true,
                    compute_units: 3,
                },
            ),
            (
                key("asserted"),
                NativePolicyVerdict {
                    allowed: true,
                    compute_units: 3,
                },
            ),
            (
                key("unsupported"),
                NativePolicyVerdict {
                    allowed: true,
                    compute_units: 3,
                },
            ),
        ]);
        let admitted = native_admissions(&evidence, &policy);
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[&key("empty")].abstained_through, 2);
        evidence[0].statistical_completed = false;
        assert!(native_admissions(&evidence, &policy).is_empty());
        evidence[0].statistical_completed = true;
        let conflicting = NativeRungEvidence {
            extracted_facts: 1,
            ..evidence[0].clone()
        };
        evidence.push(conflicting);
        assert!(native_admissions(&evidence, &policy).is_empty());
        assert!(native_admissions(&evidence[..1], &BTreeMap::new()).is_empty());
    }
}
