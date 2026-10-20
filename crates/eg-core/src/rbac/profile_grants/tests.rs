use super::*;

pub(crate) fn record() -> ProfileUseGrant {
    let component = PinnedProfileComponent {
        reference: "component:one".into(),
        revision: 7,
        digest: format!("sha256:{}", "a".repeat(64)),
    };
    ProfileUseGrant {
        schema_version: 1,
        key: ProfileGrantKey {
            tenant: "tenant:one".into(),
            grant_id: "grant:one".into(),
        },
        binding: ProfileGrantBinding {
            beneficiary_principal: "service:beneficiary".into(),
            graph: "graph:one".into(),
            profile: component.clone(),
            capability: component,
            scopes: BTreeSet::from(["query:unified".into()]),
        },
        approval: ProfileGrantApproval::DirectHuman {
            issuer_tenant: "tenant:one".into(),
            issuer_principal: "human:issuer".into(),
            approved_request_digest: format!("sha256:{}", "b".repeat(64)),
        },
        revision: 1,
        policy_revision: 3,
        issued_at_ms: 100,
        expires_at_ms: 200,
        revocation: None,
    }
}

#[test]
fn codec_preserves_exact_records_and_key_tuple_boundaries() {
    let record = record();
    assert_eq!(
        ProfileUseGrant::decode(&record.key, &record.encode().unwrap()).unwrap(),
        record
    );
    let mut foreign = record.key.clone();
    foreign.tenant = "other".into();
    assert!(ProfileUseGrant::decode(&foreign, &record.encode().unwrap()).is_err());
    let a = ProfileGrantKey {
        tenant: "a:b".into(),
        grant_id: "c".into(),
    };
    let b = ProfileGrantKey {
        tenant: "a".into(),
        grant_id: "b:c".into(),
    };
    assert_ne!(a.encode().unwrap(), b.encode().unwrap());
    let mut value = serde_json::to_value(&record).unwrap();
    value["extra"] = true.into();
    assert!(ProfileUseGrant::decode(&record.key, &serde_json::to_vec(&value).unwrap()).is_err());
    assert!(
        ProfileUseGrant::decode(&record.key, &vec![b' '; MAX_PROFILE_GRANT_BYTES + 1]).is_err()
    );
}

#[test]
fn approval_provenance_refuses_self_foreign_and_nonhuman_forms() {
    for issuer in ["service:beneficiary", "*"] {
        let mut record = record();
        let ProfileGrantApproval::DirectHuman {
            issuer_principal, ..
        } = &mut record.approval;
        *issuer_principal = issuer.into();
        assert!(record.encode().is_err());
    }
    let mut record = record();
    let ProfileGrantApproval::DirectHuman { issuer_tenant, .. } = &mut record.approval;
    *issuer_tenant = "foreign".into();
    assert!(record.encode().is_err());
    let mut value = serde_json::to_value(super::tests::record()).unwrap();
    for kind in ["api_key", "delegated", "service"] {
        value["approval"]["kind"] = kind.into();
        assert!(serde_json::from_value::<ProfileUseGrant>(value.clone()).is_err());
    }
}

#[test]
fn exact_binding_and_current_revisions_cannot_be_substituted() {
    let record = record();
    assert!(record
        .validate_binding(&record.key, &record.binding, 1, 3, 100)
        .is_ok());
    for revision in [0, 2] {
        assert!(record
            .validate_binding(&record.key, &record.binding, revision, 3, 150)
            .is_err());
    }
    assert!(record
        .validate_binding(&record.key, &record.binding, 1, 4, 150)
        .is_err());
    for field in ["graph", "principal", "profile", "capability", "scope"] {
        let mut binding = record.binding.clone();
        match field {
            "graph" => binding.graph = "other".into(),
            "principal" => binding.beneficiary_principal = "other".into(),
            "profile" => binding.profile.revision += 1,
            "capability" => binding.capability.digest = format!("sha256:{}", "c".repeat(64)),
            _ => {
                binding.scopes.insert("usage:read".into());
            }
        }
        assert!(record
            .validate_binding(&record.key, &binding, 1, 3, 150)
            .is_err());
    }
}

#[test]
fn expiry_and_revocation_never_renew_grant_authority() {
    let record = record();
    for now in [99, 200, 201] {
        assert!(record
            .validate_binding(&record.key, &record.binding, 1, 3, now)
            .is_err());
    }
    assert!(record.revoke(2, 150).is_err());
    let revoked = record.revoke(1, 150).unwrap();
    assert_eq!(revoked.binding, record.binding);
    assert_eq!(revoked.approval, record.approval);
    assert_eq!(revoked.expires_at_ms, record.expires_at_ms);
    assert_eq!(revoked.revision, 2);
    assert!(revoked
        .validate_binding(&record.key, &record.binding, 2, 3, 151)
        .is_err());
    assert!(revoked.revoke(2, 151).is_err());
    assert_eq!(
        ProfileUseGrant::decode(&record.key, &revoked.encode().unwrap()).unwrap(),
        revoked
    );
}

#[test]
fn wildcard_missing_and_unbounded_resources_refuse() {
    for resource in ["", "*", "graph?", "bad\nname"] {
        let mut record = record();
        record.binding.graph = resource.into();
        assert!(record.encode().is_err());
    }
    let mut record = record();
    record.binding.profile.reference = "x".repeat(MAX_TEXT_BYTES + 1);
    assert!(record.encode().is_err());
    record.binding.profile.reference = "profile".into();
    record.binding.scopes.clear();
    assert!(record.encode().is_err());
}
