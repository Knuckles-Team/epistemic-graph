//! Data product, contract and access-request records (EG-TYPED-PACKS-R805.1).

use eg_types::data_product::{
    parse_product_version, AccessRequest, AccessRequestState, DataContract, DataProduct,
    ProductPort, ProductPortKind, ACCESS_PURPOSE_MISSING,
};

fn round_trip<T>(value: &T) -> T
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json: T = serde_json::from_value(serde_json::to_value(value).unwrap()).unwrap();
    let packed: T = rmp_serde::from_slice(&rmp_serde::to_vec_named(value).unwrap()).unwrap();
    assert_eq!(json, packed);
    json
}

fn product() -> DataProduct {
    DataProduct {
        id: "product:orders".into(),
        owner: "team:sales".into(),
        version: "2.1.0".into(),
        datasets: vec!["table:orders".into(), "table:refunds".into()],
        terms: vec!["term:net-revenue".into()],
        ports: vec![
            ProductPort {
                name: "sql".into(),
                kind: ProductPortKind::Query,
            },
            ProductPort {
                name: "feed".into(),
                kind: ProductPortKind::Stream,
            },
        ],
    }
}

// spec: EG-TYPED-PACKS-R805.1
#[test]
fn records_round_trip() {
    let product = product();
    product.validate().unwrap();
    assert_eq!(round_trip(&product), product);
    let contract = DataContract {
        product: "product:orders".into(),
        schema_digest: format!("sha256:{}", "1f".repeat(32)),
        quality_rules: vec!["rule:orders-not-null".into()],
        freshness_s: 3_600,
    };
    contract.validate().unwrap();
    assert_eq!(round_trip(&contract), contract);
    for state in [
        AccessRequestState::Pending,
        AccessRequestState::Approved,
        AccessRequestState::Rejected,
        AccessRequestState::Revoked,
    ] {
        let request = AccessRequest {
            product: "product:orders".into(),
            principal: "user:ada".into(),
            purpose: "quarterly revenue reporting".into(),
            state,
        };
        request.validate().unwrap();
        assert_eq!(round_trip(&request), request);
    }
    assert_eq!(parse_product_version("2.1.0"), Some((2, 1, 0)));
}

// spec: EG-TYPED-PACKS-R805.1
#[test]
fn invalid_records_are_refused() {
    for version in ["2.1", "2.1.0.1", "v2.1.0", "2..0", ""] {
        let bad = DataProduct {
            version: version.into(),
            ..product()
        };
        assert_eq!(bad.validate().unwrap_err().field, "version", "{version}");
    }
    let duplicate = DataProduct {
        datasets: vec!["table:orders".into(), "table:orders".into()],
        ..product()
    };
    assert_eq!(duplicate.validate().unwrap_err().field, "datasets");
    let empty = DataProduct {
        datasets: vec![],
        ..product()
    };
    assert_eq!(empty.validate().unwrap_err().field, "datasets");
    let contract = DataContract {
        product: "product:orders".into(),
        schema_digest: "md5:abc".into(),
        quality_rules: vec![],
        freshness_s: 1,
    };
    assert_eq!(contract.validate().unwrap_err().field, "schema_digest");
    let request = AccessRequest {
        product: "product:orders".into(),
        principal: "user:ada".into(),
        purpose: "  ".into(),
        state: AccessRequestState::Pending,
    };
    assert_eq!(request.validate().unwrap_err().code, ACCESS_PURPOSE_MISSING);
    assert!(serde_json::from_value::<AccessRequest>(serde_json::json!({
        "product": "p", "principal": "u", "purpose": "x", "state": "granted"
    }))
    .is_err());
}
