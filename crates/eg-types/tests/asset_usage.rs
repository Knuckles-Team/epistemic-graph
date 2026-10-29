use eg_types::asset_usage::{fuse_with_usage_prior, AssetUsage, CatalogSearchHit, USAGE_PRIOR_CAP};

// spec: MEDULLA-UNIFIED-DATA-PLANE-R764.1
#[test]
fn usage_round_trips() {
    let usage = AssetUsage {
        asset: "pg:app.public.orders".to_string(),
        principal: "user:alice".to_string(),
        period: "2026-10".to_string(),
        reads: 42,
        queries: 7,
    };
    let json = serde_json::to_string(&usage).unwrap();
    assert_eq!(serde_json::from_str::<AssetUsage>(&json).unwrap(), usage);

    let components = fuse_with_usage_prior(0.8, 0.6, 1.0);
    let hit = CatalogSearchHit {
        asset: usage.asset.clone(),
        kind: "table".to_string(),
        score: components.total(),
        usage_rank: 1,
        components,
    };
    let json = serde_json::to_string(&hit).unwrap();
    assert_eq!(
        serde_json::from_str::<CatalogSearchHit>(&json).unwrap(),
        hit
    );
    assert!(serde_json::from_str::<AssetUsage>(&json).is_err());
}

// spec: MEDULLA-UNIFIED-DATA-PLANE-R764.2
#[test]
fn usage_prior_is_capped() {
    for relevance in [0.0, 0.01, 0.1, 0.5, 1.0] {
        for usage in [0.0, 0.5, 1.0, 50.0] {
            let hit = fuse_with_usage_prior(relevance, relevance, usage);
            let total = hit.total();
            assert!(
                hit.usage <= USAGE_PRIOR_CAP * total + 1e-12,
                "usage {} over cap of {total}",
                hit.usage
            );
        }
    }
    // A used asset outranks an equally relevant unused one.
    assert!(
        fuse_with_usage_prior(0.5, 0.5, 1.0).total() > fuse_with_usage_prior(0.5, 0.5, 0.0).total()
    );
    // An irrelevant asset gains nothing from usage alone.
    assert_eq!(fuse_with_usage_prior(0.0, 0.0, 1.0).total(), 0.0);
}
