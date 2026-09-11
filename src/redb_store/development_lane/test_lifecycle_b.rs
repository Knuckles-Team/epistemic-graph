use super::*;

#[test]
fn corrupt_durable_rows_fail_closed_before_native_use() {
    let fixture = NativeLaneFixture::new(policy());
    fixture
        .with_write("seed-corrupt-rows", |write| {
            let member = write.graph(TEST_GRAPH)?;
            let mut policies = member.open_scoped_table(POLICIES)?;
            let mut invalid_policy = policy();
            invalid_policy.tenant_count_limit = 0;
            let invalid_policy_row = DurableLanePolicy {
                policy: invalid_policy,
                policy_revision: 1,
                global_policy_revision: 1,
            };
            let policy_bytes = resource_encode(&invalid_policy_row, DurableCrypto::none())
                .expect("encode corrupt policy");
            policies.insert((TEST_GRAPH, "tenant:a"), policy_bytes.as_slice())?;
            drop(policies);

            let mut counters = member.open_scoped_table(COUNTERS)?;
            let invalid_counter = DurableLaneCounter {
                observed_disk_bytes: MAX_DISK_BYTES + 1,
                ..DurableLaneCounter::default()
            };
            let counter_bytes = resource_encode(&invalid_counter, DurableCrypto::none())
                .expect("encode corrupt counter");
            counters.insert((TEST_GRAPH, "corrupt-counter"), counter_bytes.as_slice())?;
            drop(counters);

            let mut invocations = member.open_scoped_table(INVOCATIONS)?;
            let invocation_method = fixture.reserve_method("corrupt-invocation");
            let invalid_invocation = DurableLaneInvocation {
                method: method_name(&invocation_method).to_string(),
                request_digest: request_digest(&invocation_method)
                    .expect("digest corrupt invocation"),
                result: vec![0; 64 * 1024 + 1],
            };
            let invocation_bytes = resource_encode(&invalid_invocation, DurableCrypto::none())
                .expect("encode corrupt invocation");
            invocations.insert(
                (TEST_GRAPH, "tenant:a", "corrupt-invocation"),
                invocation_bytes.as_slice(),
            )?;
            drop(invocations);

            let mut holds = member.open_scoped_table(HOLDS)?;
            let mut invalid_hold = DurableLaneHold {
                hold: hold("tenant:a", "host:a"),
                observation_revision: 0,
                last_observed_at_ms: None,
                terminal_state: None,
                terminal_expected_hold_revision: None,
                cleanup_removal_proof_ref: None,
                cleanup_expected_hold_revision: None,
                terminal_source_attempt: None,
                terminal_source_lease_epoch: None,
                terminal_source_fencing_token: None,
                terminal_source_work_item_fence: None,
                resource_reservation_id: "reservation:corrupt".into(),
                ttl_ms: 1_000,
            };
            invalid_hold.hold.worktree_locator = "../escape".into();
            let hold_bytes =
                resource_encode(&invalid_hold, DurableCrypto::none()).expect("encode corrupt hold");
            holds.insert(
                (TEST_GRAPH, invalid_hold.hold.hold_id.as_str()),
                hold_bytes.as_slice(),
            )
        })
        .expect("commit corrupt rows");

    fixture.with_read(|read| {
        let policies = read
            .scoped_owner_table(POLICIES)
            .expect("read corrupt policies");
        assert!(load_policy(&policies, TEST_GRAPH, "tenant:a", DurableCrypto::none()).is_err());
        let counters = read
            .scoped_owner_table(COUNTERS)
            .expect("read corrupt counters");
        assert!(load_counter(
            &counters,
            TEST_GRAPH,
            "corrupt-counter",
            Scope::Tenant,
            1,
            1,
            DurableCrypto::none()
        )
        .is_err());
        let invocations = read
            .scoped_owner_table(INVOCATIONS)
            .expect("read corrupt invocations");
        let invocation_method = fixture.reserve_method("corrupt-invocation");
        assert!(load_invocation(
            &invocations,
            TEST_GRAPH,
            "tenant:a",
            "corrupt-invocation",
            &invocation_method,
            DurableCrypto::none()
        )
        .is_err());
        let holds = read.scoped_owner_table(HOLDS).expect("read corrupt holds");
        let invalid_hold_id = hold("tenant:a", "host:a").hold_id;
        assert!(hold_load(&holds, TEST_GRAPH, &invalid_hold_id, DurableCrypto::none()).is_err());
    });
}

#[test]
fn invocation_replay_retention_is_bounded_and_keeps_the_current_key() {
    let fixture = NativeLaneFixture::new(policy());
    fixture
        .with_write("seed-invocation-retention", |write| {
            let mut invocations = write.graph(TEST_GRAPH)?.open_scoped_table(INVOCATIONS)?;
            // Simulate a pre-existing overfull/corrupt replay range.  The
            // repair must inspect the full bounded tenant prefix, not only a
            // MAX+2 prefix, and must retain the current key even though it
            // sorts before the newest lexical window.
            for index in 0..1_000u64 {
                let key = format!("invocation:{index:04}");
                let method = fixture.reserve_method(&key);
                let row = DurableLaneInvocation {
                    method: method_name(&method).to_string(),
                    request_digest: request_digest(&method).expect("digest pre-existing replay"),
                    result: b"bounded-result".to_vec(),
                };
                let bytes = resource_encode(&row, DurableCrypto::none())
                    .expect("encode pre-existing replay");
                invocations
                    .insert((TEST_GRAPH, "tenant:a", key.as_str()), bytes.as_slice())
                    .expect("seed pre-existing replay");
            }
            let other_method = fixture.reserve_method("tenant-b-current");
            let other_row = DurableLaneInvocation {
                method: method_name(&other_method).to_string(),
                request_digest: request_digest(&other_method).expect("digest other replay"),
                result: b"other-result".to_vec(),
            };
            let other_bytes = resource_encode(&other_row, DurableCrypto::none())
                .expect("encode other tenant replay");
            invocations
                .insert(
                    (TEST_GRAPH, "tenant:b", "tenant-b-current"),
                    other_bytes.as_slice(),
                )
                .expect("seed other tenant replay");
            let current_key = "aaa-current";
            let current_method = fixture.reserve_method(current_key);
            store_invocation(
                &mut invocations,
                TEST_GRAPH,
                "tenant:a",
                current_key,
                &current_method,
                b"current-result",
                DurableCrypto::none(),
            )
            .expect("repair bounded invocation range");
            Ok(())
        })
        .expect("commit invocation retention");
    fixture.with_read(|read| {
        let invocations = read
            .scoped_owner_table(INVOCATIONS)
            .expect("open retained invocations");
        let mut retained = 0usize;
        for row in invocations.scope_rows().expect("scan retained invocations") {
            let (key, _) = row.expect("read retained invocation");
            let (_, tenant, _) = key.value();
            if tenant == "tenant:a" {
                retained += 1;
            }
        }
        assert_eq!(retained, MAX_INVOCATIONS_PER_TENANT);
        assert!(invocations
            .get((TEST_GRAPH, "tenant:a", "aaa-current"))
            .expect("lookup current replay key")
            .is_some());
        let current_method = fixture.reserve_method("aaa-current");
        assert_eq!(
            load_invocation(
                &invocations,
                TEST_GRAPH,
                "tenant:a",
                "aaa-current",
                &current_method,
                DurableCrypto::none(),
            )
            .expect("load current replay"),
            Some((true, b"current-result".to_vec()))
        );
        assert!(invocations
            .get((TEST_GRAPH, "tenant:b", "tenant-b-current"))
            .expect("lookup other tenant replay")
            .is_some());
    });
}

#[test]
fn global_policy_allows_local_limits_but_freezes_shared_controls() {
    let first = policy();
    let mut local = first.clone();
    local.owner_count_limit = 2;
    assert!(global_policy_equal(&first, &local));
    local.global_count_limit = 2;
    assert!(!global_policy_equal(&first, &local));
    local = first.clone();
    local.drain_only = true;
    assert!(!global_policy_equal(&first, &local));
}

#[test]
fn policy_reduction_checks_every_maintained_scope() {
    let mut limits = policy();
    limits.owner_count_limit = 1;
    let owner_pressure = row(
        Scope::Owner,
        DurableLaneCounter {
            active_count: 2,
            ..DurableLaneCounter::default()
        },
    );
    assert!(policy_pressure(&[owner_pressure], &limits));
    limits.owner_count_limit = 3;
    assert!(!policy_pressure(
        &[row(
            Scope::Owner,
            DurableLaneCounter {
                active_count: 2,
                ..DurableLaneCounter::default()
            }
        )],
        &limits
    ));
    let host_pressure = row(
        Scope::Host,
        DurableLaneCounter {
            retained_disk_bytes: 101,
            ..DurableLaneCounter::default()
        },
    );
    assert!(policy_pressure(&[host_pressure], &policy()));
}
