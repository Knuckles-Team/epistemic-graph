use super::*;

fn entry(id: &str, tier: TrustTier, value: &str, source: &str) -> RetrievedEntry {
    RetrievedEntry {
        entry_id: id.into(),
        subject: "urn:agent:planner".into(),
        fact_key: "max_tools".into(),
        value: value.into(),
        trust: EntryTrust {
            tier,
            source_digest: source.into(),
            observed_at: 10,
        },
    }
}

fn policy() -> EntryTrustPolicy {
    EntryTrustPolicy {
        known_source_digests: ["sha256:a".to_string(), "sha256:b".to_string()].into(),
    }
}

// spec: MEDULLA-DECISION-ENGINE-R132.1
#[test]
fn entry_trust_round_trips() {
    let trust = entry("e1", TrustTier::Attested, "8", "sha256:a").trust;
    let value = serde_json::to_value(&trust).unwrap();
    assert_eq!(value["tier"], "attested");
    assert_eq!(
        serde_json::from_value::<EntryTrust>(value.clone()).unwrap(),
        trust
    );
    let mut unknown = value;
    unknown["tier"] = serde_json::json!("trusted");
    assert!(serde_json::from_value::<EntryTrust>(unknown).is_err());
    assert!(
        TrustTier::Verified > TrustTier::Attested && TrustTier::Attested > TrustTier::Unverified
    );
}

// spec: MEDULLA-DECISION-ENGINE-R132.2
#[test]
fn lower_tier_never_overrides_higher() {
    let entries = vec![
        entry("u1", TrustTier::Unverified, "64", "sha256:b"),
        entry("v1", TrustTier::Verified, "8", "sha256:a"),
        entry("a1", TrustTier::Attested, "16", "sha256:a"),
        entry("a2", TrustTier::Attested, "8", "sha256:b"),
        entry("x1", TrustTier::Verified, "8", "sha256:forged"),
    ];
    let admission = admit_entries(&entries, &policy());
    assert_eq!(
        admission
            .admitted
            .iter()
            .map(|e| e.entry_id.as_str())
            .collect::<Vec<_>>(),
        ["a2", "v1"],
        "the verified fact survives; an agreeing attested entry corroborates it"
    );
    assert_eq!(
        admission
            .refused
            .iter()
            .map(|r| (r.entry_id.as_str(), r.tier, r.check))
            .collect::<Vec<_>>(),
        [
            ("a1", TrustTier::Attested, EntryCheck::TierOrder),
            ("u1", TrustTier::Unverified, EntryCheck::PoisonCheck),
            ("x1", TrustTier::Verified, EntryCheck::PoisonCheck),
        ]
    );
    let mut reversed = entries.clone();
    reversed.reverse();
    assert_eq!(admit_entries(&reversed, &policy()), admission);
    // Without a verified fact, an unverified entry from a held source is kept.
    let alone = admit_entries(&entries[..1], &policy());
    assert_eq!(alone.admitted.len(), 1);
    assert!(alone.refused.is_empty());
}
