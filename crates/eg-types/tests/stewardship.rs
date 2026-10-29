//! Stewardship records (MEDULLA-TYPED-PACKS-R802.1).

use eg_types::stewardship::{
    Certification, ChangeRequest, ChangeRequestState, StewardRole, StewardshipAssignment,
    StewardshipScope, DEFAULT_CERTIFICATION_TTL_SECS,
};

fn round_trip<T>(value: &T) -> T
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let json: T = serde_json::from_value(serde_json::to_value(value).unwrap()).unwrap();
    let packed: T = rmp_serde::from_slice(&rmp_serde::to_vec_named(value).unwrap()).unwrap();
    assert_eq!(
        serde_json::to_value(&json).unwrap(),
        serde_json::to_value(&packed).unwrap()
    );
    json
}

// spec: MEDULLA-TYPED-PACKS-R802.1
#[test]
fn records_round_trip() {
    for scope in [
        StewardshipScope::Domain("finance".into()),
        StewardshipScope::ConceptScheme("scheme:revenue".into()),
        StewardshipScope::Asset("table:orders".into()),
    ] {
        for role in [
            StewardRole::Owner,
            StewardRole::Steward,
            StewardRole::Custodian,
        ] {
            let assignment = StewardshipAssignment {
                scope: scope.clone(),
                role,
                principal: "user:ada".into(),
            };
            assignment.validate().unwrap();
            assert_eq!(round_trip(&assignment), assignment);
        }
    }
    for state in [
        ChangeRequestState::Open,
        ChangeRequestState::Approved,
        ChangeRequestState::Rejected,
        ChangeRequestState::Applied,
    ] {
        let request = ChangeRequest {
            target: "term:net-revenue".into(),
            proposed_digest: format!("sha256:{}", "ab".repeat(32)),
            state,
            reviewers: vec!["user:ada".into(), "user:lin".into()],
        };
        request.validate().unwrap();
        assert_eq!(round_trip(&request), request);
    }
    let certification = Certification {
        asset: "table:orders".into(),
        level: "trusted".into(),
        by: "user:ada".into(),
        expires_at: 1_700_000_000 + DEFAULT_CERTIFICATION_TTL_SECS,
    };
    certification.validate().unwrap();
    assert_eq!(round_trip(&certification), certification);
    assert!(certification.holds_at(1_700_000_000));
    assert!(!certification.holds_at(certification.expires_at));
}

// spec: MEDULLA-TYPED-PACKS-R802.1
#[test]
fn invalid_records_are_refused_naming_the_field() {
    let wire = serde_json::json!({
        "scope": {"kind": "asset", "id": "table:orders"},
        "role": "admin",
        "principal": "user:ada"
    });
    assert!(serde_json::from_value::<StewardshipAssignment>(wire).is_err());
    let mut extra = serde_json::json!({
        "scope": {"kind": "asset", "id": "table:orders"},
        "role": "owner",
        "principal": "user:ada"
    });
    extra["note"] = "x".into();
    assert!(serde_json::from_value::<StewardshipAssignment>(extra).is_err());

    let empty_principal = StewardshipAssignment {
        scope: StewardshipScope::Asset("table:orders".into()),
        role: StewardRole::Owner,
        principal: " ".into(),
    };
    assert_eq!(empty_principal.validate().unwrap_err().field, "principal");

    let request = ChangeRequest {
        target: "term:net-revenue".into(),
        proposed_digest: "sha256:short".into(),
        state: ChangeRequestState::Open,
        reviewers: vec![],
    };
    assert_eq!(request.validate().unwrap_err().field, "proposed_digest");
    let duplicate = ChangeRequest {
        proposed_digest: format!("sha256:{}", "00".repeat(32)),
        reviewers: vec!["user:ada".into(), "user:ada".into()],
        ..request
    };
    assert_eq!(duplicate.validate().unwrap_err().field, "reviewers");

    let never = Certification {
        asset: "table:orders".into(),
        level: "trusted".into(),
        by: "user:ada".into(),
        expires_at: 0,
    };
    assert_eq!(never.validate().unwrap_err().field, "expires_at");
}
