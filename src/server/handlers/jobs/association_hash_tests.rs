//! Bounded fixture regression checks, not a proof that hashing is injective.

use super::{submit::submit_job_mine_associate, CarrierAuthority, JobKind};
use crate::server::auth::VerifiedRequestContext;
use std::collections::BTreeSet;

fn fixture_authority() -> CarrierAuthority {
    CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
        crate::acl::RequestContextClaims {
            principal: "association-hash-test".into(),
            agent_id: "association-hash-test".into(),
            tenant: "association-hash-tenant".into(),
            audience: "epistemic-graph".into(),
            policy_version: "test".into(),
            ..Default::default()
        },
        "association-hash-test-key".into(),
    ))
    .expect("verified fixture authority")
}

fn mapped_transactions(raw: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let (kind, _, _, _) =
        submit_job_mine_associate(&fixture_authority(), 306, raw, 0.1, 0.5, "fpgrowth".into())
            .expect("31 distinct fixture labels, including repetitions, must be accepted");
    match kind {
        JobKind::MineAssociate { transactions, .. } => transactions,
        #[cfg(feature = "program-optimization")]
        JobKind::ProgramOptimize { .. } => panic!("expected association job"),
    }
}

// spec: EG-PLACEMENT-R003.6
#[test]
fn thirty_one_raw_fixture_labels_produce_distinct_deterministic_opaque_refs() {
    let labels: Vec<String> = (0..31).map(|i| format!("raw fixture label {i}")).collect();
    let first = mapped_transactions(vec![labels.clone()]);
    let second = mapped_transactions(vec![labels.clone()]);
    assert_eq!(first, second, "repeated submission maps identically");
    assert_eq!(first[0].len(), 31);
    assert_eq!(first[0].iter().collect::<BTreeSet<_>>().len(), 31);
    for (label, reference) in labels.iter().zip(&first[0]) {
        let digest = reference
            .strip_prefix("eg:analytics_item:")
            .expect("stored item must be an opaque analytics reference");
        assert_eq!(digest.len(), 64);
        assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!reference.contains(label));
    }
}

// spec: EG-PLACEMENT-R003.6
#[test]
fn repeated_raw_label_counts_once_within_and_across_transactions() {
    let mut labels: Vec<String> = (0..31).map(|i| format!("raw fixture label {i}")).collect();
    let repeated = labels[0].clone();
    labels.push(repeated.clone());
    let mapped = mapped_transactions(vec![labels, vec![repeated.clone(), repeated]]);
    assert_eq!(mapped.iter().map(Vec::len).sum::<usize>(), 34);
    assert_eq!(mapped.iter().flatten().collect::<BTreeSet<_>>().len(), 31);
    assert_eq!(mapped[0][0], mapped[0][31]);
    assert_eq!(mapped[0][0], mapped[1][0]);
    assert_eq!(mapped[1][0], mapped[1][1]);
}
