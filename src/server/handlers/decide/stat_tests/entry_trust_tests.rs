//! EG-DECISION-ENGINE-R132.3 to R132.5: retrieved entries pass the trust
//! check once per served `Decide`; a refused entry is never a premise, is
//! recorded with its tier and check, and a premise with only refused entries
//! abstains with `poisoned_entry`.

use super::tool_subset_tests::{tool, tool_subset_request};
use super::*;
use eg_types::decision::entry_trust::{
    EntryCheck, EntryTrust, EntryTrustPolicy, RetrievedEntry, TrustTier,
};
use eg_types::decision::AbstainReason;

const KNOWN: &str = "sha256:known";

fn entry(id: &str, tier: TrustTier, value: &str, source: &str) -> RetrievedEntry {
    RetrievedEntry {
        entry_id: id.to_string(),
        subject: "urn:agent:planner".to_string(),
        fact_key: "max_tools".to_string(),
        value: value.to_string(),
        trust: EntryTrust {
            tier,
            source_digest: source.to_string(),
            observed_at: 10,
        },
    }
}

fn request_with(
    h: &Harness,
    schema: &ComponentDependency,
    entries: Vec<RetrievedEntry>,
) -> DecideRequest {
    let mut policy = eg_types::decision::DecisionPolicy::engine_default();
    let mut statistical = super::super::stat_support::default_statistical_policy();
    statistical.entry_trust = EntryTrustPolicy {
        known_source_digests: [KNOWN.to_string()].into(),
    };
    policy.statistical = Some(statistical);
    let pin = h.publish_policy("trust-policy", &policy);
    let mut request = tool_subset_request(
        schema,
        &["search"],
        100,
        vec![tool("tool-a", &["search"], 10)],
    );
    request.policy = DecisionPolicyRef::Pinned { component: pin };
    request.retrieved_entries = BoundedVec::new(entries).unwrap();
    request
}

// spec: EG-DECISION-ENGINE-R132.3
// spec: EG-DECISION-ENGINE-R132.5
#[tokio::test]
async fn unverified_entry_cannot_override_verified() {
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);
    let request = request_with(
        &h,
        &schema_pin,
        vec![
            entry("e-verified", TrustTier::Verified, "8", KNOWN),
            entry("e-unverified", TrustTier::Unverified, "99", KNOWN),
        ],
    );
    let record = decide(&h, request).await.unwrap().records.as_slice()[0].clone();

    let facts: Vec<_> = record
        .premises
        .iter()
        .filter(|p| p.trust_tier.is_some())
        .collect();
    assert_eq!(facts.len(), 1, "only the verified fact becomes a premise");
    assert_eq!(facts[0].fact, "max_tools=8");
    assert_eq!(facts[0].trust_tier, Some(TrustTier::Verified));

    let refused = record.refused_entries.as_slice();
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].entry_id, "e-unverified");
    assert_eq!(refused[0].tier, TrustTier::Unverified);
    assert_eq!(refused[0].check, EntryCheck::PoisonCheck);
}

// spec: EG-DECISION-ENGINE-R132.4
#[tokio::test]
async fn poisoned_only_premise_abstains() {
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);
    let request = request_with(
        &h,
        &schema_pin,
        vec![entry("e-stranger", TrustTier::Verified, "8", "sha256:unknown")],
    );
    let record = decide(&h, request).await.unwrap().records.as_slice()[0].clone();
    let StatisticalOutcome::Abstained { reasons } = &record.outcome else {
        panic!("a premise with only refused entries must abstain")
    };
    assert_eq!(
        reasons.as_slice(),
        [AbstainReason::PoisonedEntry {
            entry_id: "e-stranger".to_string(),
            check: EntryCheck::PoisonCheck,
        }]
    );
    assert!(record
        .premises
        .iter()
        .all(|p| !p.trust_tier.is_some()));
}
