use std::collections::BTreeSet;

use super::gap::GapMerge;
use super::lifecycle::SettledWorkItem;
use super::selection::{derive_legal_offers, MarketFacts, OfferExclusion};
use super::*;
use crate::keyset_page::KeysetListing;
use crate::work_item_read::WorkItemStatus;

fn digest(tag: &str) -> String {
    evidence_digest(&[tag])
}

fn upsert(tenant: &str, evidence: &[&str]) -> GapUpsertRequest {
    GapUpsertRequest {
        tenant: tenant.into(),
        gap_id: "gap:failure:timeout".into(),
        source: "failure".into(),
        signature: "timeout".into(),
        statement: "tool calls time out under load".into(),
        domain: "runtime".into(),
        severity_ppm: 700_000,
        concept_ids: vec!["concept:timeouts".into()],
        evidence: evidence
            .iter()
            .map(|tag| GapEvidenceInput {
                digest: digest(tag),
                kind: "failure_cluster".into(),
                reference: format!("cluster:{tag}"),
            })
            .collect(),
        work: GapWorkSpec {
            kind: "gap_remediation".into(),
            max_attempts: 3,
        },
        idempotency_key: format!("upsert:{}", evidence.join(",")),
    }
}

fn next_id(generation: u32) -> String {
    gap_work_item_id("tenant-a", "gap:failure:timeout", generation)
}

fn offer(utility: u64, closure_ppm: u32, cost: u64, evidence: &str) -> WorkOffer {
    WorkOffer {
        expected_utility_micros: utility,
        probability_of_closure_ppm: closure_ppm,
        expected_cost_microunits: cost,
        cost_uncertainty_ppm: 100_000,
        blast_radius: BlastRadius::Repository,
        reversible: true,
        required_capabilities: vec!["eg:capability/code".into()],
        repository_scope: vec!["agent-utilities".into()],
        cooldown_until_ms: 0,
        depends_on_gap_ids: Vec::new(),
        evidence_digests: vec![digest(evidence)],
    }
}

fn put(gap: &GapView, expected: u64, offer: WorkOffer) -> WorkOfferPutRequest {
    WorkOfferPutRequest {
        tenant: "tenant-a".into(),
        gap_id: gap.gap_id.clone(),
        expected_offer_version: expected,
        offer,
        idempotency_key: format!("offer:{expected}"),
    }
}

fn settled(gap: &GapView, status: WorkItemStatus) -> SettledWorkItem {
    SettledWorkItem {
        work_item_id: gap.work_item_id.clone(),
        status,
        reference: "result:1".into(),
    }
}

#[test]
fn row_keys_and_work_item_ids_are_tenant_and_generation_scoped() {
    let gap = "gap:failure:timeout";
    assert_ne!(gap_row_key("tenant-a", gap), gap_row_key("tenant-b", gap));
    assert_ne!(
        gap_work_item_id("tenant-a", gap, 1),
        gap_work_item_id("tenant-a", gap, 2)
    );
    assert_ne!(
        gap_work_item_id("tenant-a", gap, 1),
        gap_work_item_id("tenant-b", gap, 1)
    );
    assert_eq!(severity_bucket(900_000), 0);
    assert_eq!(severity_bucket(600_000), 1);
    assert_eq!(severity_bucket(300_000), 2);
    assert_eq!(severity_bucket(0), 3);
}

#[test]
fn an_upsert_refuses_malformed_evidence_and_empty_signals() {
    let mut request = upsert("tenant-a", &["e1"]);
    request.evidence[0].digest = "sha256:nothex".into();
    assert!(request.validate().is_err());
    assert!(upsert("tenant-a", &[]).validate().is_err());
    let mut request = upsert("tenant-a", &["e1"]);
    request.severity_ppm = PPM_SCALE + 1;
    assert!(request.validate().is_err());
    upsert("tenant-a", &["e1"]).validate().unwrap();
}

#[test]
fn only_unseen_evidence_changes_a_gap_and_only_it_reopens_a_closed_one() {
    let first = upsert("tenant-a", &["e1"]);
    let mut gap = first.fresh_gap(next_id(1), 10);
    gap.revision = 1;
    assert_eq!(
        (gap.status, gap.generation, gap.priority_bucket),
        (GapStatus::Open, 1, 1)
    );
    assert_eq!(gap.merge(&first, next_id, 11), GapMerge::Unchanged);
    assert_eq!(
        gap.merge(&upsert("tenant-a", &["e1", "e2"]), next_id, 12),
        GapMerge::Merged
    );
    assert_eq!(gap.evidence.len(), 2);

    gap.price(&put(&gap, 0, offer(1_000_000, 500_000, 2_000, "e1")), 13)
        .unwrap();
    assert!(
        gap.settle(&settled(&gap, WorkItemStatus::Succeeded), 14) == GapSettleOutcome::Resolved
    );
    // Re-sending evidence the Gap already holds cannot reopen it.
    assert_eq!(
        gap.merge(&upsert("tenant-a", &["e2"]), next_id, 15),
        GapMerge::Unchanged
    );
    assert_eq!(gap.status, GapStatus::Resolved);
    // New evidence (a regression) reopens it as generation 2 with a new WorkItem.
    assert_eq!(
        gap.merge(&upsert("tenant-a", &["e3"]), next_id, 16),
        GapMerge::Reopened
    );
    assert_eq!((gap.status, gap.generation), (GapStatus::Open, 2));
    assert_eq!(gap.work_item_id, next_id(2));
    assert!(gap.offer.is_none());
    assert_eq!(gap.offer_version, 1, "the offer counter survives a reopen");
}

#[test]
fn transitions_follow_the_legal_edge_table_under_revision_cas() {
    let mut gap = upsert("tenant-a", &["e1"]).fresh_gap(next_id(1), 10);
    gap.revision = 3;
    let request = |to, expected_revision| GapTransitionRequest {
        tenant: "tenant-a".into(),
        gap_id: "gap:failure:timeout".into(),
        expected_revision,
        to,
        reference: "spec:1".into(),
        idempotency_key: "t".into(),
    };
    assert!(!gap.transition(&request(GapTransitionTarget::Specified, 2), 11));
    assert!(gap.transition(&request(GapTransitionTarget::Specified, 3), 11));
    assert_eq!(gap.spec_refs, vec!["spec:1".to_string()]);
    assert!(gap.transition(&request(GapTransitionTarget::Resolved, 3), 12));
    assert_eq!(gap.evidence.last().unwrap().kind, "resolved");
    assert!(!gap.transition(&request(GapTransitionTarget::Specified, 3), 13));
    assert!(!gap.transition(&request(GapTransitionTarget::Deferred, 3), 13));
}

#[test]
fn settling_records_the_work_item_outcome_once_and_closes_a_live_gap() {
    let mut gap = upsert("tenant-a", &["e1"]).fresh_gap(next_id(1), 10);
    assert_eq!(
        gap.settle(&settled(&gap, WorkItemStatus::Running), 11),
        GapSettleOutcome::Pending
    );
    assert_eq!(
        gap.settle(&settled(&gap, WorkItemStatus::DeadLetter), 12),
        GapSettleOutcome::Deferred
    );
    assert_eq!(gap.status, GapStatus::Deferred);
    let recorded = gap.evidence.last().unwrap();
    assert_eq!(recorded.kind, lifecycle::WORK_ITEM_OUTCOME_EVIDENCE);
    assert_eq!(recorded.reference, gap.work_item_id);
    assert_eq!(
        gap.settle(&settled(&gap, WorkItemStatus::DeadLetter), 13),
        GapSettleOutcome::Unchanged
    );
    assert_eq!(gap.evidence.len(), 2);
}

#[test]
fn an_offer_cites_only_held_evidence_and_is_versioned() {
    let mut gap = upsert("tenant-a", &["e1"]).fresh_gap(next_id(1), 10);
    assert!(gap
        .price(&put(&gap, 0, offer(10, 10, 10, "unknown")), 11)
        .is_err());
    assert!(!gap
        .price(&put(&gap, 7, offer(10, 10, 10, "e1")), 11)
        .unwrap());
    assert!(gap
        .price(&put(&gap, 0, offer(1_000_000, 500_000, 2_000, "e1")), 11)
        .unwrap());
    let recorded = gap.offer.clone().unwrap();
    assert_eq!((recorded.version, recorded.utility_rate), (1, 250_000_000));
    assert_eq!(recorded.work_item_id, gap.work_item_id);
    assert!(!gap.price(&put(&gap, 0, offer(1, 1, 1, "e1")), 12).unwrap());
    assert!(gap.price(&put(&gap, 1, offer(1, 1, 1, "e1")), 12).unwrap());
}

#[test]
fn the_utility_rate_is_exact_fixed_point_with_a_cost_floor() {
    let base = offer(1_000_000, 500_000, 2_000, "e1");
    assert_eq!(work_offer_utility_rate(&base, 1_000), Some(250_000_000));
    // The floor dominates a cheaper declared cost.
    assert_eq!(work_offer_utility_rate(&base, 4_000), Some(125_000_000));
    let mut free = base.clone();
    free.expected_cost_microunits = 0;
    assert_eq!(work_offer_utility_rate(&free, 0), Some(500_000_000_000));
    let mut huge = base;
    huge.expected_utility_micros = u64::MAX;
    huge.probability_of_closure_ppm = PPM_SCALE;
    assert_eq!(work_offer_utility_rate(&huge, 1), None);
    assert!(huge.validate().is_err());
}

fn priced(gap_id: &str, created_at_ms: u64, rate_offer: WorkOffer) -> GapView {
    let mut request = upsert("tenant-a", &["e1"]);
    request.gap_id = gap_id.into();
    let mut gap = request.fresh_gap(format!("wi:{gap_id}"), created_at_ms);
    gap.price(&put(&gap, 0, rate_offer), created_at_ms).unwrap();
    gap
}

fn facts() -> MarketFacts {
    MarketFacts {
        now_ms: 1_000,
        resolved_gap_ids: BTreeSet::from(["gap:done".to_string()]),
        available_capabilities: BTreeSet::from(["eg:capability/code".to_string()]),
        repository_scope: None,
        max_blast_radius: BlastRadius::Repository,
        attempt_budget_microunits: 10_000,
        cost_floor_microunits: 1_000,
    }
}

#[test]
fn selection_ranks_only_legal_offers_in_a_total_order() {
    let best = priced("gap:b", 5, offer(1_000_000, 900_000, 2_000, "e1"));
    let tie_old = priced("gap:tie-old", 1, offer(1_000_000, 500_000, 2_000, "e1"));
    let tie_new = priced("gap:tie-new", 2, offer(1_000_000, 500_000, 2_000, "e1"));
    let mut blocked = offer(9_000_000, 900_000, 2_000, "e1");
    blocked.depends_on_gap_ids = vec!["gap:open".into()];
    let blocked = priced("gap:blocked", 0, blocked);
    let mut cooling = offer(9_000_000, 900_000, 2_000, "e1");
    cooling.cooldown_until_ms = 5_000;
    let cooling = priced("gap:cooling", 0, cooling);
    let over = priced("gap:over", 0, offer(9_000_000, 900_000, 20_000, "e1"));
    let mut fleet = offer(9_000_000, 900_000, 2_000, "e1");
    fleet.blast_radius = BlastRadius::Fleet;
    let fleet = priced("gap:fleet", 0, fleet);
    let mut unpriced = upsert("tenant-a", &["e1"]).fresh_gap("wi:u".into(), 0);
    unpriced.gap_id = "gap:unpriced".into();

    let gaps = vec![
        tie_new, blocked, best, over, cooling, tie_old, fleet, unpriced,
    ];
    let selection = derive_legal_offers(&gaps, &facts());
    let order: Vec<&str> = selection.ranked.iter().map(|r| r.gap_id.as_str()).collect();
    assert_eq!(order, vec!["gap:b", "gap:tie-old", "gap:tie-new"]);
    let excluded: Vec<(&str, OfferExclusion)> = selection
        .excluded
        .iter()
        .map(|(id, why)| (id.as_str(), *why))
        .collect();
    assert_eq!(
        excluded,
        vec![
            ("gap:blocked", OfferExclusion::DependencyOpen),
            ("gap:over", OfferExclusion::OverBudget),
            ("gap:cooling", OfferExclusion::CoolingDown),
            ("gap:fleet", OfferExclusion::BlastRadiusExceeded),
            ("gap:unpriced", OfferExclusion::Unpriced),
        ]
    );
    // Deterministic: the same inputs in another order rank identically.
    let mut reversed = gaps.clone();
    reversed.reverse();
    assert_eq!(
        derive_legal_offers(&reversed, &facts()).ranked,
        selection.ranked
    );
}

#[test]
fn a_gap_row_round_trips_and_lists_only_for_its_tenant() {
    let gap = upsert("tenant-a", &["e1"]).fresh_gap(next_id(1), 10);
    let mut row = serde_json::Map::new();
    gap.to_row("tenant-a", &mut row).unwrap();
    row.insert("row_revision".into(), 4.into());
    let read = GapView::from_row(&row).unwrap();
    assert_eq!(read.revision, 4);
    assert_eq!(read.gap_id, gap.gap_id);
    let listing = |tenant: &str| GapListRequest {
        tenant: tenant.into(),
        status: Some(GapStatus::Open),
        source: None,
        cursor: None,
        limit: 10,
    };
    assert!(listing("tenant-a").select("k", &row).unwrap().is_some());
    assert!(listing("tenant-b").select("k", &row).unwrap().is_none());
    let mut resolved = listing("tenant-a");
    resolved.status = Some(GapStatus::Resolved);
    assert!(resolved.select("k", &row).unwrap().is_none());
    assert!(GapListRequest {
        limit: 0,
        ..listing("tenant-a")
    }
    .validate()
    .is_err());
}
