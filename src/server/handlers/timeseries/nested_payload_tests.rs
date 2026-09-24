use super::*;

#[test]
fn point_decoder_rejects_bombs_ragged_rows_and_non_finite_values() {
    assert!(decode_wire_points(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());

    let ragged =
        rmp_serde::to_vec_named(&vec![(1_i64, vec![1.0_f64]), (2_i64, vec![1.0_f64, 2.0])])
            .unwrap();
    assert!(decode_wire_points(&ragged).is_err());

    let non_finite = rmp_serde::to_vec_named(&vec![(1_i64, vec![f64::INFINITY])]).unwrap();
    assert!(decode_wire_points(&non_finite).is_err());

    let valid = rmp_serde::to_vec_named(&vec![
        (1_i64, vec![1.0_f64, 2.0]),
        (2_i64, vec![3.0_f64, 4.0]),
    ])
    .unwrap();
    assert_eq!(decode_wire_points(&valid).unwrap().len(), 2);
}

#[cfg(feature = "security")]
#[test]
fn same_series_name_isolated_by_actor_and_tenant() {
    let alice = CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            "alice", "tenant-a",
        ),
    )
    .unwrap();
    let bob = CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            "bob", "tenant-a",
        ),
    )
    .unwrap();
    let cross_tenant = CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            "alice", "tenant-b",
        ),
    )
    .unwrap();
    let key = |authority: &CarrierAuthority| {
        SeriesKey::new(
            authority.tenant_scope(),
            authority.namespace("timeseries-graph", "shared"),
            "cpu",
        )
        .encode()
    };
    assert_ne!(key(&alice), key(&bob));
    assert_ne!(key(&alice), key(&cross_tenant));
}

/// Stronger, end-to-end version of the proof above: instead of comparing
/// encoded keys, this appends real points through the exact
/// `MutationBatch`-compiled path `TsAppend` itself uses, then reads them
/// back through the exact `range_scoped`/`scan_all_scoped` primitives
/// `TsRange`/`TsWindow`/`TsGapFill`/`TsAsofJoin` call. This is the leak-
/// equivalence-to-RLS proof this file's module doc promises: default-deny
/// RLS filters rows a caller CAN address but was never granted; this
/// mechanism instead makes a non-owning caller unable to even ADDRESS the
/// row in the first place -- both leave a non-owning caller reading ZERO
/// rows of someone else's data, and this test exercises that end to end
/// against the real store rather than inferring it from key inequality.
#[cfg(feature = "security")]
#[test]
fn cross_actor_and_cross_tenant_reads_see_no_points_through_the_real_store() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = SeriesStore::open(
        &dir.path().join("series.redb"),
        crate::store_authority::process_verifier(),
        crate::store_authority::process_authority().principal(),
        &crate::store_authority::process_authority().proof(),
    )
    .expect("open test store");

    let alice = CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            "alice", "tenant-a",
        ),
    )
    .unwrap();
    let bob = CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            "bob", "tenant-a",
        ),
    )
    .unwrap();
    let cross_tenant_alice = CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            "alice", "tenant-b",
        ),
    )
    .unwrap();

    let graph = "shared-graph";
    let series_id = "cpu";
    let points = vec![
        Point {
            ts: 1,
            values: vec![1.0],
        },
        Point {
            ts: 2,
            values: vec![2.0],
        },
    ];

    // Alice appends through the exact MutationBatch-compiled path
    // `TsAppend` itself uses -- not a test shortcut straight into the
    // store, so this proves the SERVED write path is leak-equivalent too.
    let scope = alice.namespace("ts-scope", graph);
    let expected = store
        .mutation_version(alice.tenant_scope(), &scope)
        .expect("read initial mutation version");
    let method = Method::TsAppend {
        series_id: series_id.to_string(),
        n_fields: 1,
        bucket_ns: 60_000_000_000,
        field_names: vec!["value".to_string()],
        points_msgpack: Vec::new(),
    };
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("timeseries", &scope, 1, &method);
    let now = crate::server::dispatch::authoritative_now_ms();
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: 1,
            attempt_nonce: None,
            principal: Some(alice.actor_scope()),
            tenant: alice.tenant_scope(),
            graph: &scope,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: now,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        &method,
        MutationSurface::Other,
        DurabilityDomain::TimeSeries,
        "timeseries_append",
    )
    .expect("compile append batch");
    let alice_key = scoped_key(&alice, graph, series_id).unwrap();
    let committed = store
        .append_scoped_batch(
            &alice_key,
            ScopedAppendBatch {
                n_fields: 1,
                bucket_ns: 60_000_000_000,
                field_names: &["value".to_string()],
                points: &points,
                batch: &batch,
                committed_at_ms: now,
                derived: None,
            },
        )
        .expect("append committed");
    assert_eq!(committed, 2);

    // Alice reads her own points back -- proves the mechanism actually
    // serves data (not just that every read is uninterestingly empty).
    let alice_read = store
        .range_scoped(&alice_key, i64::MIN, i64::MAX)
        .expect("alice range read");
    assert_eq!(alice_read.len(), 2);
    assert_eq!(
        store
            .scan_all_scoped(&alice_key)
            .expect("alice full scan")
            .len(),
        2
    );

    // Bob, addressing the IDENTICAL graph+series_id, sees NOTHING.
    let bob_key = scoped_key(&bob, graph, series_id).unwrap();
    assert!(store
        .range_scoped(&bob_key, i64::MIN, i64::MAX)
        .expect("bob range read")
        .is_empty());
    assert!(store
        .scan_all_scoped(&bob_key)
        .expect("bob full scan")
        .is_empty());

    // Same actor id, different tenant, addressing the IDENTICAL
    // graph+series_id: also sees nothing.
    let cross_tenant_key = scoped_key(&cross_tenant_alice, graph, series_id).unwrap();
    assert!(store
        .range_scoped(&cross_tenant_key, i64::MIN, i64::MAX)
        .expect("cross-tenant range read")
        .is_empty());
    assert!(store
        .scan_all_scoped(&cross_tenant_key)
        .expect("cross-tenant full scan")
        .is_empty());
}
