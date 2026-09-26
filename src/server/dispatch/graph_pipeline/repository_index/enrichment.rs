//! EH-557: a bounded enrichment intent in the repository snapshot commit.
//!
//! An operator must install the policy and worker/model digests before the
//! engine records a pending intent. Repository bytes and request bodies cannot
//! grant themselves budget or name a model. This module does not drain a queue
//! or run a worker; the outbox intent is committed with the source rows.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::ingestion_wire::IndexResult;
use eg_types::mutation_batch::MutationOutboxIntent;
use serde::{Deserialize, Serialize};

use super::super::*;
use crate::parser::enrichment_admission::{
    native_admissions, plan_enrichment_page, AdmissionPlan, EnrichmentCandidate, EnrichmentCursor,
    EnrichmentStage, NativePolicyVerdict, UnitKey,
};
use crate::parser::enrichment_snapshot::{
    EligibleSnapshot, EligibleUnit, EnrichmentWorkStage, MAX_ELIGIBLE_UNITS,
};

// The durable mutation record accepts at most 64 MiB. Keep the complete
// snapshot far below that limit, including its envelope and source rows.
const MAX_PENDING_INTENT_BYTES: usize = 4 * 1024 * 1024;

/// Complete, path-free eligibility evidence in the canonical source commit.
/// No compute budget has been reserved or spent at this point.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingSnapshot {
    budget_status: PendingBudgetStatus,
    snapshot: EligibleSnapshot,
}

/// Encode one complete immutable source as the only canonical pending intent
/// shape. Source commits and authorized replacements use the same bounded
/// format; neither may publish a partial page as a source snapshot.
pub(crate) fn intent_for_snapshot(
    snapshot: EligibleSnapshot,
) -> Result<MutationOutboxIntent, String> {
    snapshot.validate().map_err(|error| {
        format!("CONFLICT: repository enrichment snapshot is invalid: {error:?}")
    })?;
    let envelope_id = snapshot.source_envelope.clone();
    let pending = PendingSnapshot {
        budget_status: PendingBudgetStatus::Unreserved,
        snapshot,
    };
    let payload = rmp_serde::to_vec_named(&pending)
        .map_err(|error| format!("CONFLICT: repository enrichment snapshot is invalid: {error}"))?;
    if payload.len() > MAX_PENDING_INTENT_BYTES {
        return Err("CONFLICT: repository enrichment snapshot exceeds its byte bound".into());
    }
    Ok(MutationOutboxIntent {
        topic: "repository.enrichment.pending".into(),
        key: envelope_id,
        payload,
        headers: BTreeMap::new(),
    })
}

pub(super) struct PendingEnrichment {
    pub(super) intent: MutationOutboxIntent,
    pub(super) budget: crate::redb_store::enrichment_budget::SourceBudgetAuthority,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum PendingBudgetStatus {
    Unreserved,
}

struct Policy {
    digest: String,
    model_digest: String,
    budget_units: u64,
    max_total_units: u64,
    unit_cost: u64,
}

impl Policy {
    fn configured() -> Result<Option<Self>, String> {
        let names = [
            "EG_REPOSITORY_ENRICHMENT_POLICY_DIGEST",
            "EG_REPOSITORY_ENRICHMENT_MODEL_DIGEST",
            "EG_REPOSITORY_ENRICHMENT_BUDGET_UNITS",
            "EG_REPOSITORY_ENRICHMENT_UNIT_COST",
        ];
        let values: Vec<_> = names
            .iter()
            .map(|name| match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(_)) => {
                    Err("CONFLICT: repository enrichment policy is not UTF-8".to_string())
                }
            })
            .collect::<Result<_, _>>()?;
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }
        if values.iter().any(Option::is_none) {
            return Err("CONFLICT: repository enrichment policy is incomplete".into());
        }
        let value = |index: usize| values[index].as_deref().unwrap_or_default();
        let valid_digest =
            |value: &str| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
        if !valid_digest(value(0)) || !valid_digest(value(1)) {
            return Err("CONFLICT: repository enrichment policy/model digest is invalid".into());
        }
        let budget_units = value(2)
            .parse::<u64>()
            .map_err(|_| "CONFLICT: repository enrichment budget is invalid")?;
        let unit_cost = value(3)
            .parse::<u64>()
            .map_err(|_| "CONFLICT: repository enrichment unit cost is invalid")?;
        if budget_units == 0 || unit_cost == 0 || unit_cost > budget_units {
            return Err("CONFLICT: repository enrichment budget is invalid".into());
        }
        // This ceiling is committed with the source policy revision. Changing
        // an environment variable later cannot authorize a larger top-up.
        let max_total_units = match std::env::var("EG_REPOSITORY_ENRICHMENT_MAX_BUDGET_UNITS") {
            Ok(value) => value
                .parse::<u64>()
                .map_err(|_| "CONFLICT: repository enrichment maximum budget is invalid")?,
            Err(std::env::VarError::NotPresent) => budget_units,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err("CONFLICT: repository enrichment maximum budget is not UTF-8".into());
            }
        };
        if max_total_units < budget_units {
            return Err("CONFLICT: repository enrichment maximum budget is below its seed".into());
        }
        Ok(Some(Self {
            digest: value(0).into(),
            model_digest: value(1).into(),
            budget_units,
            max_total_units,
            unit_cost,
        }))
    }
}

#[cfg(feature = "blob")]
type CandidateSource = (UnitKey, String, u64);

fn repository_identity(result: &IndexResult) -> Result<String, String> {
    let mut repository_id = None;
    for node in result
        .nodes
        .iter()
        .filter(|node| node.node_type == "Branch")
    {
        let candidate = node
            .properties
            .get("repository_id")
            .filter(|value| !value.is_empty())
            .ok_or("CONFLICT: repository branch lacks its source identity")?;
        if repository_id
            .as_ref()
            .is_some_and(|prior| prior != candidate)
        {
            return Err("CONFLICT: repository branches disagree on source identity".into());
        }
        repository_id = Some(candidate.clone());
    }
    repository_id.ok_or_else(|| "CONFLICT: repository source has no branch identity".into())
}

#[cfg(feature = "blob")]
fn blob_metadata(result: &IndexResult) -> Result<BTreeMap<String, (String, u64)>, String> {
    let mut blobs = BTreeMap::new();
    for node in result.nodes.iter().filter(|node| node.node_type == "Blob") {
        let Some(digest) = node.properties.get("content_digest") else {
            continue;
        };
        let (Some(reference), Some(length)) = (
            node.properties.get("content_ref"),
            node.properties.get("content_length"),
        ) else {
            continue;
        };
        let length = length
            .parse::<u64>()
            .map_err(|_| "CONFLICT: repository source length is invalid")?;
        if blobs
            .insert(digest.clone(), (reference.clone(), length))
            .is_some()
        {
            return Err("CONFLICT: repository source has duplicate Blob rows".into());
        }
    }
    Ok(blobs)
}

/// Read the repository identity and the source refs projected in this batch.
/// A Blob property alone is only a claim: `verify_input_refs` checks its
/// tenant/repository holder, source digest and length against the CAS owner.
#[cfg(feature = "blob")]
fn candidate_sources(
    result: &IndexResult,
    plan: &crate::parser::enrichment_admission::AdmissionPlan,
) -> Result<(String, Vec<CandidateSource>), String> {
    let repository_id = repository_identity(result)?;
    let blobs = blob_metadata(result)?;
    let mut candidates = Vec::with_capacity(plan.candidates.len());
    for candidate in &plan.candidates {
        let unit = &candidate.unit;
        let raw_digest = unit
            .content_digest
            .strip_prefix("sha256:")
            .ok_or("CONFLICT: repository source digest is invalid")?;
        if raw_digest.len() != 64 || !raw_digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("CONFLICT: repository source digest is invalid".into());
        }
        let (reference, length) = blobs
            .get(&unit.content_digest)
            .ok_or("CONFLICT: repository source lacks committed CAS metadata")?;
        candidates.push((unit.clone(), reference.clone(), *length));
    }
    Ok((repository_id, candidates))
}

/// Verify each candidate with the same tenant/repository holder and byte-digest
/// rule that a background worker uses. Manifest hashes are NOT source hashes.
#[cfg(feature = "blob")]
fn verify_input_refs(
    store: &dyn crate::server::blob::store::ChunkStore,
    tenant: &str,
    repository_id: &str,
    candidates: Vec<CandidateSource>,
) -> Result<BTreeMap<UnitKey, (String, u64)>, String> {
    let mut refs = BTreeMap::new();
    for (unit, reference, length) in candidates {
        crate::server::blob::engine_bodies::read_repository_content_ref(
            store,
            tenant,
            repository_id,
            &reference,
            &unit.content_digest,
            length,
        )
        .map_err(|error| format!("CONFLICT: repository source CAS binding failed: {error}"))?;
        refs.insert(unit, (reference, length));
    }
    Ok(refs)
}

#[cfg(feature = "blob")]
async fn committed_input_refs(
    ctx: GraphOpRouting<'_>,
    result: &IndexResult,
    plan: &crate::parser::enrichment_admission::AdmissionPlan,
) -> Result<BTreeMap<UnitKey, (String, u64)>, String> {
    let (repository_id, candidates) = candidate_sources(result, plan)?;
    let blob = ctx
        .state
        .read()
        .await
        .blob
        .clone()
        .ok_or("CONFLICT: repository source CAS is unavailable")?;
    let tenant = ctx.verified_context.tenant().to_string();
    tokio::task::spawn_blocking(move || {
        verify_input_refs(blob.store.as_ref(), &tenant, &repository_id, candidates)
    })
    .await
    .map_err(|error| format!("CONFLICT: repository source CAS verification failed: {error}"))?
}

#[cfg(not(feature = "blob"))]
async fn committed_input_refs(
    _ctx: GraphOpRouting<'_>,
    _result: &IndexResult,
    _plan: &crate::parser::enrichment_admission::AdmissionPlan,
) -> Result<BTreeMap<UnitKey, (String, u64)>, String> {
    Err("CONFLICT: repository source CAS requires the blob feature".into())
}

fn eligible_candidates(
    result: &IndexResult,
    policy: &Policy,
    envelope_id: &str,
) -> Result<Vec<EnrichmentCandidate>, String> {
    let units: BTreeMap<_, _> = result
        .native_rung_evidence
        .iter()
        .map(|item| {
            (
                UnitKey {
                    content_digest: item.content_digest.clone(),
                    parser_capability_digest: item.parser_capability_digest.clone(),
                },
                NativePolicyVerdict {
                    allowed: true,
                    compute_units: policy.unit_cost,
                },
            )
        })
        .collect();
    let admissions = native_admissions(&result.native_rung_evidence, &units);
    let mut cursor: Option<EnrichmentCursor> = None;
    let mut all_candidates: Vec<EnrichmentCandidate> = Vec::new();
    loop {
        let page = plan_enrichment_page(
            &result.file_outcomes,
            &admissions,
            &BTreeSet::new(),
            policy.budget_units,
            envelope_id,
            &policy.digest,
            cursor.as_ref(),
        )
        .map_err(|error| {
            format!("INVALID_ARGUMENT: repository enrichment plan refused: {error:?}")
        })?;
        all_candidates.extend(page.plan.candidates);
        if all_candidates.len() > MAX_ELIGIBLE_UNITS {
            return Err("CONFLICT: repository enrichment exceeds its unit bound".into());
        }
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        if page.plan.deferred_for_capacity == 0 && page.plan.deferred_for_budget == 0 {
            return Err("CONFLICT: repository enrichment cursor did not advance".into());
        }
        cursor = Some(next_cursor);
    }
    Ok(all_candidates)
}

fn snapshot_units(
    candidates: &[EnrichmentCandidate],
    refs: &BTreeMap<UnitKey, (String, u64)>,
) -> Result<Vec<EligibleUnit>, String> {
    candidates
        .iter()
        .map(|candidate| {
            refs.get(&candidate.unit)
                .map(|(reference, content_length)| EligibleUnit {
                    content_digest: candidate.unit.content_digest.clone(),
                    parser_capability_digest: candidate.unit.parser_capability_digest.clone(),
                    stage: match candidate.stage {
                        EnrichmentStage::Classical => EnrichmentWorkStage::Classical,
                        EnrichmentStage::Embedding => EnrichmentWorkStage::Embedding,
                        EnrichmentStage::Llm => EnrichmentWorkStage::Llm,
                    },
                    input_ref: reference.clone(),
                    content_length: *content_length,
                    compute_units: candidate.reserved_compute_units,
                    demanded: candidate.demanded,
                })
                .ok_or_else(|| "CONFLICT: repository enrichment source ref is absent".to_string())
        })
        .collect()
}

/// Page the engine planner until every eligible unit is represented. Pagination
/// here selects a complete immutable snapshot; it does not reserve future-page
/// budget or submit work. Each CAS ref is bound before the source commit.
pub(super) async fn pending_intent(
    ctx: GraphOpRouting<'_>,
    result: &IndexResult,
    envelope_id: &str,
) -> Result<Option<PendingEnrichment>, String> {
    let Some(policy) = Policy::configured()? else {
        return Ok(None);
    };
    let all_candidates = eligible_candidates(result, &policy, envelope_id)?;
    if all_candidates.is_empty() {
        return Ok(None);
    }
    let all_plan = AdmissionPlan {
        candidates: all_candidates,
        reserved_compute_units: 0,
        deferred_for_budget: 0,
        deferred_for_capacity: 0,
    };
    let refs = committed_input_refs(ctx, result, &all_plan).await?;
    let units = snapshot_units(&all_plan.candidates, &refs)?;
    let repository_id = repository_identity(result)?;
    let snapshot = EligibleSnapshot {
        schema_version: 1,
        tenant_id: ctx.verified_context.tenant().into(),
        graph: ctx.graph_name.into(),
        repository_id,
        source_envelope: envelope_id.into(),
        source_commit_ref: envelope_id.into(),
        policy_digest: policy.digest.to_ascii_lowercase(),
        catalog_digest: eg_capabilities::CONTRACT_CATALOG_DIGEST.into(),
        model_digest: policy.model_digest.to_ascii_lowercase(),
        budget_units: policy.budget_units,
        units,
    };
    let intent = intent_for_snapshot(snapshot.clone())?;
    let budget = crate::redb_store::enrichment_budget::SourceBudgetAuthority {
        tenant_id: snapshot.tenant_id.clone(),
        source_envelope: snapshot.source_envelope.clone(),
        snapshot_digest: snapshot
            .digest()
            .map_err(|_| "CONFLICT: repository enrichment snapshot digest is invalid")?,
        repository_id: snapshot.repository_id.clone(),
        policy_digest: snapshot.policy_digest.clone(),
        total_budget_units: snapshot.budget_units,
        max_total_units: policy.max_total_units,
    };
    Ok(Some(PendingEnrichment { intent, budget }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "blob")]
    use crate::parser::enrichment_admission::{
        AdmissionPlan, EnrichmentCandidate, EnrichmentStage,
    };
    #[cfg(feature = "blob")]
    use crate::server::blob::store::ChunkStore;
    #[cfg(feature = "blob")]
    use eg_types::ingestion_wire::ExtractedNode;
    use eg_types::ingestion_wire::{IndexFileOutcome, IndexFileStatus, NativeRungEvidence};

    #[test]
    fn pending_snapshot_collects_beyond_one_native_page() {
        let mut result = IndexResult::default();
        for index in 0..129 {
            let digest = format!("sha256:{index:064x}");
            result.file_outcomes.push(IndexFileOutcome {
                file_path: format!("file-{index}"),
                status: IndexFileStatus::Success,
                content_digest: digest.clone(),
                parser_capability_digest: "grammar:v1".into(),
                diagnostics: Default::default(),
            });
            result.native_rung_evidence.push(NativeRungEvidence {
                content_digest: digest,
                parser_capability_digest: "grammar:v1".into(),
                status: IndexFileStatus::Success,
                extracted_facts: 0,
                inferred_facts: 0,
                derived_facts: 0,
                symbol_resolution_completed: true,
                statistical_completed: true,
            });
        }
        let policy = Policy {
            digest: "a".repeat(64),
            model_digest: "b".repeat(64),
            budget_units: 129,
            max_total_units: 129,
            unit_cost: 1,
        };
        let candidates = eligible_candidates(&result, &policy, "source-envelope").unwrap();
        assert_eq!(candidates.len(), 129);
        assert_eq!(
            candidates[0].unit.content_digest,
            format!("sha256:{:064x}", 0)
        );
        assert_eq!(
            candidates[128].unit.content_digest,
            format!("sha256:{:064x}", 128)
        );
    }

    #[cfg(feature = "blob")]
    #[test]
    fn worker_input_must_be_a_committed_cas_blob_ref() {
        use sha2::{Digest, Sha256};
        let store = crate::server::blob::store::RedbChunkStore::open_temp().unwrap();
        let body = b"source bytes";
        let other = b"different source bytes";
        let source_digest = eg_types::contract::Digest256::from_bytes(Sha256::digest(body).into());
        let other_digest = eg_types::contract::Digest256::from_bytes(Sha256::digest(other).into());
        let stored = store
            .put_repository_bodies(
                "tenant",
                "repo",
                &[
                    crate::server::blob::engine_bodies::EngineBody {
                        sha256: source_digest,
                        body: body.to_vec(),
                    },
                    crate::server::blob::engine_bodies::EngineBody {
                        sha256: other_digest,
                        body: other.to_vec(),
                    },
                ],
                1,
            )
            .unwrap();
        let digest = format!("sha256:{}", source_digest.to_hex());
        let key = UnitKey {
            content_digest: digest.clone(),
            parser_capability_digest: "grammar:v1".into(),
        };
        let plan = AdmissionPlan {
            candidates: vec![EnrichmentCandidate {
                unit: key.clone(),
                stage: EnrichmentStage::Classical,
                demanded: false,
                reserved_compute_units: 1,
            }],
            reserved_compute_units: 1,
            deferred_for_budget: 0,
            deferred_for_capacity: 0,
        };
        let mut result = IndexResult {
            nodes: vec![
                ExtractedNode {
                    node_id: "branch:repo".into(),
                    node_type: "Branch".into(),
                    properties: std::collections::HashMap::from([(
                        "repository_id".into(),
                        "repo".into(),
                    )]),
                },
                ExtractedNode {
                    node_id: "blob:source".into(),
                    node_type: "Blob".into(),
                    properties: std::collections::HashMap::from([
                        ("content_digest".into(), digest.clone()),
                        ("content_length".into(), body.len().to_string()),
                        (
                            "content_ref".into(),
                            format!("cas:sha256:{}", stored[1].manifest_digest),
                        ),
                    ]),
                },
            ],
            ..Default::default()
        };
        let (repo, candidates) = candidate_sources(&result, &plan).unwrap();
        assert!(verify_input_refs(&store, "tenant", &repo, candidates)
            .unwrap_err()
            .starts_with("CONFLICT:"));
        let expected = format!("cas:sha256:{}", stored[0].manifest_digest);
        result.nodes[1]
            .properties
            .insert("content_ref".into(), expected.clone());
        let (repo, candidates) = candidate_sources(&result, &plan).unwrap();
        assert_eq!(
            verify_input_refs(&store, "tenant", &repo, candidates)
                .unwrap()
                .get(&key),
            Some(&(expected, body.len() as u64))
        );
        let (repo, candidates) = candidate_sources(&result, &plan).unwrap();
        assert!(verify_input_refs(&store, "another-tenant", &repo, candidates).is_err());
    }
}
