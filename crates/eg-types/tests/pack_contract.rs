//! Pack contract versions and standalone grants (EG-TYPED-PACKS-R107.1).

use eg_types::connector_pack::contract_version::{
    ContractBreakKind, PackContractVersion, StandaloneGrant, GRANT_INCOMPLETE,
};

fn digest(byte: &str) -> String {
    format!("sha256:{}", byte.repeat(32))
}

fn release() -> PackContractVersion {
    PackContractVersion {
        connector: "connector-a".into(),
        version: "2.0.0".into(),
        contract_digest: digest("aa"),
        ontology_digest: digest("bb"),
        shapes_digest: digest("cc"),
        supersedes: Some("1.4.0".into()),
    }
}

fn grant() -> StandaloneGrant {
    StandaloneGrant {
        connector: "connector-a".into(),
        ontology_iri: "http://example.org/vendor".into(),
        reason: "vendor vocabulary with no parent class yet".into(),
        owner: "team:integrations".into(),
        expires_at: 1_800_000_000,
    }
}

// spec: EG-TYPED-PACKS-R107.1
#[test]
fn version_and_grant_round_trip() {
    let release = release();
    release.validate().unwrap();
    let json: PackContractVersion =
        serde_json::from_value(serde_json::to_value(&release).unwrap()).unwrap();
    assert_eq!(json, release);
    let packed: PackContractVersion =
        rmp_serde::from_slice(&rmp_serde::to_vec_named(&release).unwrap()).unwrap();
    assert_eq!(packed, release);
    let first: PackContractVersion = serde_json::from_value(serde_json::json!({
        "connector": "connector-a", "version": "1.0.0",
        "contract_digest": digest("01"), "ontology_digest": digest("02"),
        "shapes_digest": digest("03")
    }))
    .unwrap();
    assert_eq!(first.supersedes, None);
    first.validate().unwrap();

    let grant = grant();
    grant.validate().unwrap();
    let json: StandaloneGrant =
        serde_json::from_value(serde_json::to_value(&grant).unwrap()).unwrap();
    assert_eq!(json, grant);
    assert!(!grant.expired_at(1_799_999_999));
    assert!(grant.expired_at(1_800_000_000));

    for kind in [
        ContractBreakKind::RemovedClass,
        ContractBreakKind::RetypedClass,
        ContractBreakKind::RemovedProperty,
        ContractBreakKind::RetypedProperty,
        ContractBreakKind::RemovedField,
        ContractBreakKind::RetypedField,
        ContractBreakKind::NarrowedShape,
        ContractBreakKind::RemovedMapping,
    ] {
        let wire = serde_json::to_value(kind).unwrap();
        assert_eq!(
            serde_json::from_value::<ContractBreakKind>(wire).unwrap(),
            kind
        );
    }
}

// spec: EG-TYPED-PACKS-R107.1
#[test]
fn invalid_versions_and_incomplete_grants_are_refused() {
    let bad_digest = PackContractVersion {
        shapes_digest: "sha256:zz".into(),
        ..release()
    };
    assert_eq!(bad_digest.validate().unwrap_err().field, "shapes_digest");
    let self_superseding = PackContractVersion {
        supersedes: Some("2.0.0".into()),
        ..release()
    };
    assert_eq!(
        self_superseding.validate().unwrap_err().field,
        "supersedes"
    );
    let mut extra = serde_json::to_value(release()).unwrap();
    extra["package_version"] = "9.9.9".into();
    assert!(serde_json::from_value::<PackContractVersion>(extra).is_err());

    let no_reason = StandaloneGrant {
        reason: " ".into(),
        ..grant()
    };
    let error = no_reason.validate().unwrap_err();
    assert_eq!((error.code, error.field), (GRANT_INCOMPLETE, "reason"));
    let no_expiry = StandaloneGrant {
        expires_at: 0,
        ..grant()
    };
    let error = no_expiry.validate().unwrap_err();
    assert_eq!((error.code, error.field), (GRANT_INCOMPLETE, "expires_at"));
}
