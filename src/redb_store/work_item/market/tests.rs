//! Store-level proof of the work market over a real shard, through the SAME
//! WorkItem-family dispatch (`apply_work_item_rows`) the MutationBatch kernel
//! runs: Gap + WorkItem admission, the fenced claim, the terminal commit and
//! the engine-derived evidence write-back. Deterministic: every clock is an
//! argument.

use eg_types::epistemic_operations::{
    ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion, ClaimWorkItemResult,
};
use eg_types::work_market::{
    evidence_digest, gap_work_item_id, BlastRadius, GapEvidenceInput, GapListRequest,
    GapSettleOutcome, GapSettleRequest, GapSettled, GapStatus, GapTransitionOutcome,
    GapTransitionRequest, GapTransitionTarget, GapTransitioned, GapUpsertOutcome, GapUpsertRequest,
    GapUpserted, GapWorkSpec, WorkOffer, WorkOfferPutOutcome, WorkOfferPutRequest,
    WorkOfferRecorded,
};

use super::super::test_shard::{open, with_write, GRAPH};
use super::*;
use crate::protocol::ResultPayload;
use crate::redb_store::store_rows::GraphRowTables;
use crate::redb_store::store_state_tables::open_native_operation_lane_tables;

const GAP: &str = "gap:failure:timeout";

/// Apply one WorkItem-family method in its own committed transaction, exactly
/// as the kernel does; an error aborts the transaction (nothing is written).
fn apply(shard: &Shard, tag: &str, method: Method, now_ms: u64) -> Result<ResultPayload, String> {
    let op_id = format!("work-market-test/{tag}");
    with_write(shard, &op_id, now_ms, |admitted| {
        let member = admitted.graph(GRAPH).unwrap();
        let mut graph = GraphRowTables::open(member).unwrap();
        let (mut holds, index, mut counters, mut pressure, policies) =
            open_native_operation_lane_tables(&admitted, GRAPH).unwrap();
        apply_work_item_rows(WorkItemApplyRequest {
            graph: GRAPH,
            batch_id: tag,
            method: &method,
            nodes: &mut graph.nodes,
            holds: &mut holds,
            work_item_index: &index,
            counters: &mut counters,
            pressure_index: &mut pressure,
            policies: &policies,
            native_work_items: &mut graph.native_work_items,
            edges: &mut graph.edges,
            command_sequences: &mut graph.command_sequences,
            committed_at_ms: now_ms,
            crypto: DurableCrypto::none(),
        })
        .map(|result| result.expect("a WorkItem-family write always answers"))
    })
}

fn json<T: serde::de::DeserializeOwned>(payload: ResultPayload) -> T {
    match payload {
        ResultPayload::Json(value) => serde_json::from_value(value).unwrap(),
        other => panic!("work-market results are JSON, got {other:?}"),
    }
}

fn upsert(tenant: &str, evidence: &[&str]) -> Method {
    Method::GapUpsert {
        request: GapUpsertRequest {
            tenant: tenant.into(),
            gap_id: GAP.into(),
            source: "failure".into(),
            signature: "timeout".into(),
            statement: "tool calls time out under load".into(),
            domain: "runtime".into(),
            severity_ppm: 900_000,
            concept_ids: Vec::new(),
            evidence: evidence
                .iter()
                .map(|tag| GapEvidenceInput {
                    digest: evidence_digest(&[tag]),
                    kind: "failure_cluster".into(),
                    reference: format!("cluster:{tag}"),
                })
                .collect(),
            work: GapWorkSpec {
                kind: "gap_remediation".into(),
                max_attempts: 2,
            },
            idempotency_key: format!("upsert:{tenant}:{}", evidence.join(",")),
        },
    }
}

fn upserted(shard: &Shard, tag: &str, method: Method, now_ms: u64) -> GapUpserted {
    json(apply(shard, tag, method, now_ms).unwrap())
}

fn claim(shard: &Shard, tag: &str, work_item_id: &str, worker: &str) -> ClaimWorkItemResult {
    let method = Method::ClaimWorkItem {
        request: ClaimWorkItemRequest {
            schema_version: ClaimWorkItemRequestSchemaVersion::V1,
            tenant_ref: "tenant-a".into(),
            work_item_id: Some(work_item_id.into()),
            queue_ref: None,
            resource_class: None,
            fairness_group: None,
            worker_ref: worker.into(),
            now_ms: 2_000,
            lease_ms: 60_000,
            max_tenant_in_flight: 64,
        },
    };
    match apply(shard, tag, method, 2_000).unwrap() {
        ResultPayload::Raw(bytes) => decode_durable(&bytes).unwrap(),
        other => panic!("ClaimWorkItem answers a bin-encoded result, got {other:?}"),
    }
}

fn settle(shard: &Shard, tag: &str, tenant: &str, now_ms: u64) -> GapSettled {
    json(
        apply(
            shard,
            tag,
            Method::GapSettle {
                request: GapSettleRequest {
                    tenant: tenant.into(),
                    gap_id: GAP.into(),
                    idempotency_key: format!("settle:{tag}"),
                },
            },
            now_ms,
        )
        .unwrap(),
    )
}

fn read(shard: &Shard, tenant: &str) -> Option<eg_types::work_market::GapView> {
    read_gap(shard, GRAPH, tenant, GAP, DurableCrypto::none()).unwrap()
}

#[test]
fn a_signal_creates_one_gap_and_its_claimable_work_item_together() {
    let temp = open("market-create");
    let created = upserted(&temp.shard, "create", upsert("tenant-a", &["e1"]), 1_000);
    assert_eq!(created.outcome, GapUpsertOutcome::Created);
    assert!(created.work_item_created);
    let work_item_id = gap_work_item_id("tenant-a", GAP, 1);
    assert_eq!(created.gap.work_item_id, work_item_id);
    assert_eq!((created.gap.revision, created.gap.priority_bucket), (1, 0));
    assert!(created.changed_work_item_ids.contains(&work_item_id));

    let item = read_work_item(
        &temp.shard,
        GRAPH,
        "tenant-a",
        &work_item_id,
        DurableCrypto::none(),
    )
    .unwrap()
    .expect("the Gap's WorkItem was admitted in the same transaction");
    assert_eq!(item.input_ref, GAP);
    assert_eq!(item.kind, "gap_remediation");
    assert_eq!(
        item.metadata.get("gap_id").and_then(|v| v.as_str()),
        Some(GAP)
    );

    // The same signal again changes nothing: no second Gap, no second WorkItem.
    let again = upserted(&temp.shard, "again", upsert("tenant-a", &["e1"]), 1_100);
    assert_eq!(again.outcome, GapUpsertOutcome::Unchanged);
    assert!(again.changed_work_item_ids.is_empty());
    assert_eq!(read(&temp.shard, "tenant-a").unwrap().revision, 1);
    let page = list_gaps(
        &temp.shard,
        GRAPH,
        &GapListRequest {
            tenant: "tenant-a".into(),
            status: None,
            source: None,
            cursor: None,
            limit: 10,
        },
        DurableCrypto::none(),
    )
    .unwrap();
    assert_eq!(page.gaps.len(), 1);
}

#[test]
fn gaps_are_tenant_scoped_even_for_the_same_canonical_id() {
    let temp = open("market-tenants");
    let a = upserted(&temp.shard, "a", upsert("tenant-a", &["e1"]), 1_000);
    let b = upserted(&temp.shard, "b", upsert("tenant-b", &["e1"]), 1_000);
    assert_eq!(
        b.outcome,
        GapUpsertOutcome::Created,
        "tenant-a's Gap is invisible"
    );
    assert_ne!(a.gap.work_item_id, b.gap.work_item_id);
    assert_eq!(read(&temp.shard, "tenant-c"), None);
    // A foreign tenant can neither transition nor settle tenant-a's Gap.
    let foreign: GapTransitioned = json(
        apply(
            &temp.shard,
            "foreign",
            Method::GapTransition {
                request: GapTransitionRequest {
                    tenant: "tenant-c".into(),
                    gap_id: GAP.into(),
                    expected_revision: 1,
                    to: GapTransitionTarget::Deferred,
                    reference: "nope".into(),
                    idempotency_key: "foreign".into(),
                },
            },
            1_100,
        )
        .unwrap(),
    );
    assert_eq!(foreign.outcome, GapTransitionOutcome::NotFound);
    assert_eq!(
        read(&temp.shard, "tenant-a").unwrap().status,
        GapStatus::Open
    );
}

#[test]
fn two_claimants_of_one_gap_item_cannot_both_win() {
    let temp = open("market-claim");
    let created = upserted(&temp.shard, "create", upsert("tenant-a", &["e1"]), 1_000);
    let first = claim(
        &temp.shard,
        "claim-1",
        &created.gap.work_item_id,
        "worker-1",
    );
    let second = claim(
        &temp.shard,
        "claim-2",
        &created.gap.work_item_id,
        "worker-2",
    );
    assert!(first.claimed);
    assert!(
        !second.claimed,
        "the fenced native claim admits exactly one worker"
    );
    assert_eq!(first.lease_holder_ref.as_deref(), Some("worker-1"));
}

fn offer(evidence: &str) -> WorkOffer {
    WorkOffer {
        expected_utility_micros: 2_000_000,
        probability_of_closure_ppm: 500_000,
        expected_cost_microunits: 4_000,
        cost_uncertainty_ppm: 200_000,
        blast_radius: BlastRadius::Repository,
        reversible: true,
        required_capabilities: Vec::new(),
        repository_scope: vec!["agent-utilities".into()],
        cooldown_until_ms: 0,
        depends_on_gap_ids: Vec::new(),
        evidence_digests: vec![evidence_digest(&[evidence])],
    }
}

fn put(
    shard: &Shard,
    tag: &str,
    expected: u64,
    evidence: &str,
) -> Result<WorkOfferRecorded, String> {
    let method = Method::WorkOfferPut {
        request: WorkOfferPutRequest {
            tenant: "tenant-a".into(),
            gap_id: GAP.into(),
            expected_offer_version: expected,
            offer: offer(evidence),
            idempotency_key: format!("offer:{tag}"),
        },
    };
    apply(shard, tag, method, 1_500).map(json)
}

#[test]
fn an_offer_is_versioned_derived_state_citing_only_held_evidence() {
    let temp = open("market-offer");
    upserted(&temp.shard, "create", upsert("tenant-a", &["e1"]), 1_000);
    assert!(
        put(&temp.shard, "unheld", 0, "e9").is_err(),
        "evidence not on the Gap"
    );
    let first = put(&temp.shard, "first", 0, "e1").unwrap();
    assert_eq!(first.outcome, WorkOfferPutOutcome::Applied);
    let recorded = first.gap.unwrap().offer.unwrap();
    assert_eq!((recorded.version, recorded.utility_rate), (1, 250_000_000));
    let stale = put(&temp.shard, "stale", 0, "e1").unwrap();
    assert_eq!(stale.outcome, WorkOfferPutOutcome::Conflict);
    assert_eq!(
        put(&temp.shard, "next", 1, "e1").unwrap().outcome,
        WorkOfferPutOutcome::Applied
    );
}

fn commit(shard: &Shard, work_item_id: &str, hold: &ClaimWorkItemResult, outcome: &str) {
    let method = Method::CommitWorkItemResult {
        tenant: "tenant-a".into(),
        work_item_id: work_item_id.into(),
        worker_id: "worker-1".into(),
        lease_epoch: hold.lease_epoch.unwrap(),
        fencing_token: hold.fencing_token.unwrap(),
        idempotency_key: format!("commit:{work_item_id}:{outcome}"),
        outcome: outcome.into(),
        result_ref: Some("cas:result:patch".into()),
        error_ref: None,
        retryable: false,
        now_ms: 3_000,
        outcome_extension: None,
    };
    apply(shard, &format!("commit-{outcome}"), method, 3_000).unwrap();
}

#[test]
fn a_completed_item_writes_its_evidence_back_to_the_gap() {
    let temp = open("market-settle");
    let created = upserted(&temp.shard, "create", upsert("tenant-a", &["e1"]), 1_000);
    let work_item_id = created.gap.work_item_id.clone();
    let pending = settle(&temp.shard, "early", "tenant-a", 1_200);
    assert_eq!(pending.outcome, GapSettleOutcome::Pending);
    assert!(pending.changed_work_item_ids.is_empty());

    let hold = claim(&temp.shard, "claim", &work_item_id, "worker-1");
    commit(&temp.shard, &work_item_id, &hold, "succeeded");
    let settled = settle(&temp.shard, "settle", "tenant-a", 4_000);
    assert_eq!(settled.outcome, GapSettleOutcome::Resolved);
    let gap = read(&temp.shard, "tenant-a").unwrap();
    assert_eq!(gap.status, GapStatus::Resolved);
    let evidence = gap.evidence.last().unwrap();
    assert_eq!(evidence.kind, "work_item_outcome");
    assert_eq!(evidence.reference, work_item_id);
    assert_eq!(evidence.recorded_at_ms, 4_000);
    // Settling again is a no-op; a foreign tenant cannot settle at all.
    assert_eq!(
        settle(&temp.shard, "twice", "tenant-a", 4_100).outcome,
        GapSettleOutcome::Unchanged
    );
    assert_eq!(
        settle(&temp.shard, "foreign", "tenant-b", 4_100).outcome,
        GapSettleOutcome::NotFound
    );

    // A regression (new evidence) reopens the Gap as generation 2 with a NEW
    // WorkItem; the old evidence alone could not.
    let old = upserted(&temp.shard, "old", upsert("tenant-a", &["e1"]), 5_000);
    assert_eq!(old.outcome, GapUpsertOutcome::Unchanged);
    let reopened = upserted(&temp.shard, "regress", upsert("tenant-a", &["e2"]), 5_100);
    assert_eq!(reopened.outcome, GapUpsertOutcome::Reopened);
    assert_eq!(reopened.gap.generation, 2);
    assert_eq!(
        reopened.gap.work_item_id,
        gap_work_item_id("tenant-a", GAP, 2)
    );
    assert!(read_work_item(
        &temp.shard,
        GRAPH,
        "tenant-a",
        &reopened.gap.work_item_id,
        DurableCrypto::none()
    )
    .unwrap()
    .is_some());
}

#[test]
fn generic_writers_cannot_touch_a_gap_row() {
    let temp = open("market-guard");
    upserted(&temp.shard, "create", upsert("tenant-a", &["e1"]), 1_000);
    let key = eg_types::work_market::gap_row_key("tenant-a", GAP);
    let remove = Method::RemoveNode { node_id: key };
    let refused = super::super::test_shard::with_nodes(&temp.shard, "remove", |nodes| {
        Ok(super::super::refuse_generic_native_row_write(
            GRAPH,
            &remove,
            nodes,
            DurableCrypto::none(),
        ))
    });
    assert!(refused.is_err());
}
