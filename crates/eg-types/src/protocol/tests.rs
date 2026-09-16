use super::*;

fn node_binding_fixture_claims() -> crate::acl::RequestContextClaims {
    crate::acl::RequestContextClaims {
        principal: "p".into(),
        tenant: "t".into(),
        audience: "a".into(),
        agent_id: "p".into(),
        roles: vec![],
        scopes: vec![],
        policy_version: "v".into(),
        delegation: vec![],
        node: None,
        priority: None,
    }
}

#[test]
fn envelope_v2_bytes_are_unchanged_when_node_claim_is_absent() {
    // ADR-3 / W1.9: rebuild the PRE-ADR-3 canonical encoding by hand and
    // assert `build_envelope_v2_bytes` is byte-for-byte identical when
    // `node` is `None` -- proving the change is genuinely additive for
    // clients (`clients/js`, `clients/go`, or an un-upgraded Python
    // client) that never send the claim at all.
    //
    // Deliberately mirrors `build_envelope_v2_bytes`'s own 8-argument
    // shape (the fixed pre-ADR-3 wire fields) byte-for-byte, so it
    // carries the same scoped allow that function already does above.
    #[allow(clippy::too_many_arguments)]
    fn pre_adr3_bytes(
        request_id: u64,
        graph: &str,
        method_name: &str,
        body_hash: &str,
        claims: &crate::acl::RequestContextClaims,
        timestamp: u64,
        nonce: &str,
        idempotency_key: &str,
    ) -> Vec<u8> {
        fn put(buf: &mut Vec<u8>, value: &str) {
            buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
            buf.extend_from_slice(value.as_bytes());
        }
        fn put_list(buf: &mut Vec<u8>, values: &[String]) {
            buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
            for value in values {
                put(buf, value);
            }
        }
        let mut buf = Vec::new();
        put(&mut buf, "eg-envelope-v2");
        buf.extend_from_slice(&request_id.to_be_bytes());
        put(&mut buf, graph);
        put(&mut buf, method_name);
        put(&mut buf, body_hash);
        put(&mut buf, &claims.principal);
        put(&mut buf, &claims.tenant);
        put(&mut buf, &claims.audience);
        put(&mut buf, &claims.agent_id);
        put_list(&mut buf, &claims.roles);
        put_list(&mut buf, &claims.scopes);
        put(&mut buf, &claims.policy_version);
        put_list(&mut buf, &claims.delegation);
        buf.extend_from_slice(&timestamp.to_be_bytes());
        put(&mut buf, nonce);
        put(&mut buf, idempotency_key);
        buf
    }

    let claims = node_binding_fixture_claims();
    let current = build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
    let legacy = pre_adr3_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
    assert_eq!(
        current, legacy,
        "an absent node claim must encode byte-for-byte identically to the \
             pre-ADR-3 wire format"
    );
}

#[test]
fn envelope_v2_bytes_cover_the_node_claim_when_present() {
    let mut claims = node_binding_fixture_claims();
    let absent = build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
    claims.node = Some("node-a".into());
    let present_a = build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
    claims.node = Some("node-b".into());
    let present_b = build_envelope_v2_bytes(7, "g", "Ping", "hash", &claims, 111, "nonce", "idem");
    assert_ne!(
        absent, present_a,
        "a present node claim must change the MAC-covered bytes"
    );
    assert_ne!(
        present_a, present_b,
        "different target nodes must not share an encoding"
    );
}

// W2.4 engine-native QoS lanes: the priority claim is additive AND its
// encoding stays unambiguous against the node trailer (distinct tag bytes).
#[test]
fn envelope_v2_bytes_cover_the_priority_claim_without_node_collision() {
    let base = node_binding_fixture_claims();
    let absent = build_envelope_v2_bytes(7, "g", "Ping", "hash", &base, 111, "nonce", "idem");

    // A present priority claim changes the MAC-covered bytes ...
    let mut with_prio = base.clone();
    with_prio.priority = Some("background_ingestion".into());
    let prio_only =
        build_envelope_v2_bytes(7, "g", "Ping", "hash", &with_prio, 111, "nonce", "idem");
    assert_ne!(
        absent, prio_only,
        "a present priority claim must change the MAC-covered bytes"
    );

    // ... and distinct priority values encode distinctly.
    let mut with_prio2 = base.clone();
    with_prio2.priority = Some("interactive".into());
    let prio_only2 =
        build_envelope_v2_bytes(7, "g", "Ping", "hash", &with_prio2, 111, "nonce", "idem");
    assert_ne!(
        prio_only, prio_only2,
        "different priority classes must not share an encoding"
    );

    // The tag-distinctness guarantee: node="X" (tag 1) and priority="X"
    // (tag 2) with the SAME value must NOT produce the same MAC input — a
    // shared marker byte would let one claim be silently reinterpreted as
    // the other.
    let mut node_x = base.clone();
    node_x.node = Some("X".into());
    let node_only = build_envelope_v2_bytes(7, "g", "Ping", "hash", &node_x, 111, "nonce", "idem");
    let mut prio_x = base.clone();
    prio_x.priority = Some("X".into());
    let prio_x_bytes =
        build_envelope_v2_bytes(7, "g", "Ping", "hash", &prio_x, 111, "nonce", "idem");
    assert_ne!(
        node_only, prio_x_bytes,
        "node and priority trailers with the same value must not collide"
    );

    // Both trailers present: node (tag 1) precedes priority (tag 2), and the
    // combined encoding differs from either alone.
    let mut both = base.clone();
    both.node = Some("X".into());
    both.priority = Some("interactive".into());
    let both_bytes = build_envelope_v2_bytes(7, "g", "Ping", "hash", &both, 111, "nonce", "idem");
    assert_ne!(both_bytes, node_only);
    assert_ne!(both_bytes, prio_only2);
}

#[test]
fn test_request_roundtrip_add_node() {
    let req = Request {
        id: 1,
        graph: "agent:planner".to_string(),
        auth_token: "abc123".to_string(),
        agent_id: None,
        method: Method::AddNode {
            node_id: "n1".to_string(),
            properties_msgpack: vec![
                0x81, 0xa4, 0x74, 0x79, 0x70, 0x65, 0xa5, 0x41, 0x67, 0x65, 0x6e, 0x74,
            ],
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: Request = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.id, 1);
    assert_eq!(parsed.graph, "agent:planner");
}

#[test]
fn development_lane_protocol_matches_cross_language_golden_vector() {
    use crate::epistemic_operations::{
        DevelopmentLaneCleanupIntent, DevelopmentLaneCleanupIntentSchemaVersion,
        DevelopmentLaneIntent, DevelopmentLaneIntentHostTargetKind,
        DevelopmentLaneIntentSchemaVersion, DevelopmentLaneQueryRequest,
        DevelopmentLaneQueryRequestSchemaVersion, DevelopmentLaneQuotaUpdateRequest,
        DevelopmentLaneResultDecision,
    };

    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../protocols/epistemic-operations/v1/development-lane.golden.json"
    ))
    .expect("golden vector must be valid JSON");
    assert_eq!(
        serde_json::to_string(&DevelopmentLaneWorkItemKind::Lifecycle).unwrap(),
        "\"lane.lifecycle\""
    );
    assert_eq!(
        serde_json::to_string(&DevelopmentLaneWorkItemKind::Cleanup).unwrap(),
        "\"lane.cleanup\""
    );

    let intent = DevelopmentLaneIntent {
        schema_version: DevelopmentLaneIntentSchemaVersion::V1,
        tenant_ref: "tenant:golden".into(),
        request_id: "request:golden".into(),
        lane_id: "lane:golden".into(),
        repository_id: "repo:golden".into(),
        base_ref: "refs/heads/main".into(),
        base_sha: "0123456789abcdef0123456789abcdef01234567".into(),
        branch: "rmdd-28/golden".into(),
        host_target_kind: DevelopmentLaneIntentHostTargetKind::InventoryAlias,
        host_target_alias: Some("host:golden".into()),
        host_ref: "host-ref:golden".into(),
        resource_reservation_id: "reservation:golden".into(),
        workspace_ref: "workspace:golden".into(),
        worktree_locator: "lanes/golden".into(),
        owner_id: "agent:golden".into(),
        session_id: "session:golden".into(),
        fairness_group: "fairness:golden".into(),
        quota_policy_name: "default".into(),
        quota_policy_version: "1".into(),
        predicted_disk_bytes: 4096,
        ttl_ms: 60000,
        input_fingerprint: "v1:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .into(),
    };
    let encoded = serde_json::to_string(&intent).unwrap();
    assert_eq!(encoded, vector["intent_json"].as_str().unwrap());

    let mut unknown: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    unknown["unexpected"] = serde_json::Value::Bool(true);
    assert!(serde_json::from_value::<DevelopmentLaneIntent>(unknown).is_err());

    let cleanup = DevelopmentLaneCleanupIntent {
        schema_version: DevelopmentLaneCleanupIntentSchemaVersion::V1,
        hold_id: "v1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        lane_id: "lane:golden".into(),
        expected_hold_revision: 7,
    };
    assert_eq!(
        serde_json::to_string(&cleanup).unwrap(),
        vector["lane_cleanup_extension_json"].as_str().unwrap()
    );
    let mut unknown_cleanup: serde_json::Value = serde_json::to_value(&cleanup).unwrap();
    unknown_cleanup["unexpected"] = serde_json::Value::Bool(true);
    assert!(serde_json::from_value::<DevelopmentLaneCleanupIntent>(unknown_cleanup).is_err());

    let decisions = [
        DevelopmentLaneResultDecision::Accepted,
        DevelopmentLaneResultDecision::Idempotent,
        DevelopmentLaneResultDecision::Stale,
        DevelopmentLaneResultDecision::Conflict,
        DevelopmentLaneResultDecision::InputConflict,
        DevelopmentLaneResultDecision::Quota,
        DevelopmentLaneResultDecision::Policy,
        DevelopmentLaneResultDecision::Drained,
        DevelopmentLaneResultDecision::NotFound,
        DevelopmentLaneResultDecision::WrongKind,
        DevelopmentLaneResultDecision::WrongTenant,
        DevelopmentLaneResultDecision::WrongOwner,
        DevelopmentLaneResultDecision::WrongAttempt,
        DevelopmentLaneResultDecision::WrongLeaseEpoch,
        DevelopmentLaneResultDecision::WrongFence,
        DevelopmentLaneResultDecision::Expired,
        DevelopmentLaneResultDecision::Terminal,
        DevelopmentLaneResultDecision::CleanupRequired,
        DevelopmentLaneResultDecision::Exclusivity,
        DevelopmentLaneResultDecision::Invalid,
    ];
    let actual: Vec<String> = decisions
        .iter()
        .map(|decision| serde_json::to_string(decision).unwrap())
        .collect();
    let expected: Vec<String> = vector["refusal_decisions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| format!("\"{}\"", value.as_str().unwrap()))
        .collect();
    assert_eq!(actual, expected);

    let query = DevelopmentLaneQueryRequest {
        schema_version: DevelopmentLaneQueryRequestSchemaVersion::V1,
        tenant_ref: "tenant:golden".into(),
        hold_id: "hold:golden".into(),
        now_ms: 123,
    };
    let query_json = serde_json::to_value(query).unwrap();
    assert!(serde_json::from_value::<DevelopmentLaneQueryRequest>(query_json).is_ok());

    let policy = serde_json::json!({
        "schema_version": "1",
        "policy_name": "default",
        "policy_version": "2",
        "tenant_count_limit": 1,
        "owner_count_limit": 1,
        "session_count_limit": 1,
        "workspace_count_limit": 1,
        "repository_count_limit": 1,
        "host_count_limit": 1,
        "global_count_limit": 1,
        "tenant_predicted_disk_bytes": 4096,
        "owner_predicted_disk_bytes": 4096,
        "session_predicted_disk_bytes": 4096,
        "workspace_predicted_disk_bytes": 4096,
        "repository_predicted_disk_bytes": 4096,
        "host_predicted_disk_bytes": 4096,
        "global_predicted_disk_bytes": 4096,
        "tenant_observed_disk_bytes": 4096,
        "owner_observed_disk_bytes": 4096,
        "session_observed_disk_bytes": 4096,
        "workspace_observed_disk_bytes": 4096,
        "repository_observed_disk_bytes": 4096,
        "host_observed_disk_bytes": 4096,
        "global_observed_disk_bytes": 4096,
        "tenant_retained_disk_bytes": 4096,
        "owner_retained_disk_bytes": 4096,
        "session_retained_disk_bytes": 4096,
        "workspace_retained_disk_bytes": 4096,
        "repository_retained_disk_bytes": 4096,
        "host_retained_disk_bytes": 4096,
        "global_retained_disk_bytes": 4096,
        "min_ttl_ms": 1000,
        "max_ttl_ms": 60000,
        "max_observation_staleness_ms": 1000,
        "drain_only": false
    });
    let quota_update = serde_json::json!({
        "schema_version": "1",
        "tenant_ref": "tenant:golden",
        "policy": policy,
        "expected_policy_revision": vector["quota_policy_update_expected_revision"],
        "expected_policy_version": "1",
        "idempotency_key": "idem:golden",
        "now_ms": 123
    });
    let parsed: DevelopmentLaneQuotaUpdateRequest =
        serde_json::from_value(quota_update).expect("numeric policy CAS must be required");
    assert_eq!(
        parsed.expected_policy_revision,
        vector["quota_policy_update_expected_revision"]
            .as_u64()
            .unwrap()
    );
}

#[test]
fn stale_route_is_structured_and_carries_fencing_epoch() {
    let response = Response::stale_route(9, "opaque:graph", 3, 17, Some(2), "not leader");
    let detail: crate::epistemic_operations::OperationResult = match response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
        other => panic!("expected structured redirect, got {other:?}"),
    };
    let redirect = detail.redirect.unwrap();
    assert_eq!(redirect.group, 3);
    assert_eq!(redirect.epoch, 17);
    assert_eq!(redirect.fencing_token, 3);
    assert_eq!(response.error.as_deref(), Some("OPERATION_REDIRECTED"));
}

#[test]
fn test_request_roundtrip_create_channel() {
    let req = Request {
        id: 42,
        graph: "__commons__".to_string(),
        auth_token: "tok".to_string(),
        agent_id: Some("agent:a".to_string()),
        method: Method::CreateChannel {
            channel_id: "channel:p2p:a:b".to_string(),
            channel_type: ChannelType::PeerToPeer,
            creator: "agent:a".to_string(),
            initial_members: vec!["agent:a".to_string(), "agent:b".to_string()],
        },
    };
    let json = serde_json::to_string(&req).unwrap();
    let parsed: Request = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.id, 42);
    if let Method::CreateChannel { channel_type, .. } = parsed.method {
        assert_eq!(channel_type, ChannelType::PeerToPeer);
    } else {
        panic!("Wrong method variant");
    }
}

#[test]
fn test_response_ok() {
    let resp = Response::ok(1, ResultPayload::Json(serde_json::json!({"count": 42})));
    let json = serde_json::to_string(&resp).unwrap();
    assert!(json.contains("\"count\":42"));
    assert!(!json.contains("error"));
}

#[test]
fn test_response_err() {
    let resp = Response::err(2, "node not found");
    let json = serde_json::to_string(&resp).unwrap();
    assert!(json.contains("node not found"));
    assert!(!json.contains("result"));
}

#[test]
fn test_all_graph_types_roundtrip() {
    for gt in [
        GraphType::Agent,
        GraphType::Team,
        GraphType::Global,
        GraphType::Commons,
    ] {
        let json = serde_json::to_string(&gt).unwrap();
        let parsed: GraphType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, gt);
    }
}

#[test]
fn test_method_ping_roundtrip() {
    let method = Method::Ping;
    let json = serde_json::to_string(&method).unwrap();
    let parsed: Method = serde_json::from_str(&json).unwrap();
    assert!(matches!(parsed, Method::Ping));
}

#[test]
fn retired_methods_and_parameters_are_rejected() {
    for method in [
        "BatchCosineSimilarity",
        "SpectralCluster",
        "HypergraphEncodeInteraction",
        "FindSimilarPairs",
    ] {
        let value = serde_json::json!({"method": method, "params": {}});
        assert!(serde_json::from_value::<Method>(value).is_err());
    }

    let retired_parameter = serde_json::json!({
        "method": "UnifiedQueryText",
        "params": {
            "text": "MATCH (n) |> LIMIT 1",
            "reorder_filter_selectivity": 0.5
        }
    });
    assert!(serde_json::from_value::<Method>(retired_parameter).is_err());
}

#[cfg(feature = "mining")]
#[test]
fn mining_algorithms_accept_only_the_current_canonical_names() {
    assert_eq!(
        serde_json::from_str::<ForecastAlgorithm>("\"holtwinters\"").unwrap(),
        ForecastAlgorithm::Holtwinters
    );
    assert_eq!(
        serde_json::from_str::<ClassifyAlgorithm>("\"gaussiannb\"").unwrap(),
        ClassifyAlgorithm::Gaussiannb
    );
    assert_eq!(
        serde_json::from_str::<ReduceAlgorithm>("\"svd\"").unwrap(),
        ReduceAlgorithm::Svd
    );
    for retired in [
        "holt_winters",
        "hw",
        "ets",
        "gaussian_nb",
        "gnb",
        "multinomial_nb",
        "mnb",
        "linear_svc",
        "linearsvc",
        "truncated_svd",
        "truncatedsvd",
    ] {
        let encoded = serde_json::to_string(retired).unwrap();
        assert!(serde_json::from_str::<ForecastAlgorithm>(&encoded).is_err());
        assert!(serde_json::from_str::<ClassifyAlgorithm>(&encoded).is_err());
        assert!(serde_json::from_str::<ReduceAlgorithm>(&encoded).is_err());
    }
}

#[test]
fn test_method_pagerank_roundtrip() {
    let method = Method::PageRank {
        damping: 0.85,
        iterations: 100,
    };
    let json = serde_json::to_string(&method).unwrap();
    let parsed: Method = serde_json::from_str(&json).unwrap();
    if let Method::PageRank {
        damping,
        iterations,
    } = parsed
    {
        assert!((damping - 0.85).abs() < f64::EPSILON);
        assert_eq!(iterations, 100);
    } else {
        panic!("Wrong method");
    }
}

#[test]
fn raw_result_payload_decodes_to_typed_value() {
    // Phase C-D compact encoding: a Raw payload carries the typed result as a
    // MessagePack bin. Over the wire it round-trips as a bin and decodes back
    // to the EXACT typed value the JSON path produced — what the Python client
    // does on any top-level `bytes` result.
    let scores: Vec<(String, f64)> = vec![("a".into(), 0.5), ("b".into(), 0.25)];
    let resp = Response::ok(7, ResultPayload::raw(&scores));
    let wire = rmp_serde::to_vec_named(&resp).unwrap();
    let decoded: Response = rmp_serde::from_slice(&wire).unwrap();
    let inner = match decoded.result {
        Some(ResultPayload::Raw(b)) => b,
        other => panic!("expected a bin result payload, got {:?}", other),
    };
    let back: Vec<(String, f64)> = rmp_serde::from_slice(&inner).unwrap();
    assert_eq!(back, scores);
}

#[test]
fn raw_result_serialization_failure_is_an_error_response() {
    struct RejectSerialization;

    impl Serialize for RejectSerialization {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("fixture rejection"))
        }
    }

    assert!(ResultPayload::raw(&RejectSerialization).is_err());
    let response = Response::ok(11, ResultPayload::raw(&RejectSerialization));
    assert!(response.result.is_none());
    assert!(response
        .error
        .as_deref()
        .is_some_and(|error| error.contains("result serialization failed")));
}

#[test]
fn test_method_apply_mutation_roundtrip() {
    let method = Method::ApplyMutation {
        event_type: "TRIPLE_INSERT".to_string(),
        query: "INSERT DATA { <A> <B> <C> }".to_string(),
    };
    let json = serde_json::to_string(&method).unwrap();
    let parsed: Method = serde_json::from_str(&json).unwrap();
    if let Method::ApplyMutation { event_type, query } = parsed {
        assert_eq!(event_type, "TRIPLE_INSERT");
        assert_eq!(query, "INSERT DATA { <A> <B> <C> }");
    } else {
        panic!("Wrong method");
    }
}

/// CONCEPT:EG-KG.memory.eg-batch-decay-caller — the memory/scene/trajectory mutation + read variants
/// round-trip through MessagePack (the on-wire + WAL framing) byte-for-byte,
/// preserving every field.
#[test]
fn eg318_memory_scene_trajectory_methods_roundtrip() {
    let methods = vec![
        Method::CreateSummaryNode {
            level: 2,
            child_ids: vec!["e1".into(), "e2".into()],
            props_msgpack: vec![0x80],
        },
        Method::Consolidate {
            episodic_ids: vec!["a".into(), "b".into()],
            semantic_props_msgpack: vec![0x80],
        },
        Method::Maintain {
            ids: vec!["m1".into()],
            now_ms: 123,
            half_life_ms: 604_800_000,
            evict_threshold: 0.5,
            delete: false,
        },
        Method::AddSceneObject {
            pose_msgpack: vec![0x80],
            parent: Some("root".into()),
        },
        Method::StartTrajectory {
            props_msgpack: vec![0x80],
        },
        Method::AppendStep {
            traj_id: "trajectory:dead".into(),
            action_msgpack: vec![0xa4, b'l', b'e', b'f', b't'],
            reward: 1.5,
            state_ref: None,
            next_state_ref: None,
            t: 0,
        },
        Method::SummaryChildren {
            node_id: "s".into(),
        },
        Method::WorldTransform {
            node_id: "o".into(),
        },
        Method::DiscountedReturn {
            traj_id: "t".into(),
            gamma: 0.9,
        },
    ];
    for m in methods {
        let wire = rmp_serde::to_vec_named(&m).unwrap();
        let back: Method = rmp_serde::from_slice(&wire).unwrap();
        // The tag+content framing survives; spot-check a representative field.
        let re = rmp_serde::to_vec_named(&back).unwrap();
        assert_eq!(wire, re, "EG-318 method must msgpack-roundtrip identically");
    }
}

#[cfg(feature = "query")]
#[test]
fn governed_evidence_locus_wire_round_trips() {
    let locus = EvidenceLocusWire {
        id: "eg:locus:0000000000000001".to_string(),
        subject: EvidenceResourceWire::Occurrence("eg:occurrence:0000000000000002".to_string()),
        address: EvidenceAddressWire::PageRegion {
            page: 4,
            x: 1.0,
            y: 2.0,
            width: 3.0,
            height: 4.0,
        },
        policy_ref: "eg:policy:0000000000000003".to_string(),
        derivation_ref: "eg:derivation:0000000000000004".to_string(),
    };
    let encoded = rmp_serde::to_vec_named(&locus).unwrap();
    assert_eq!(
        rmp_serde::from_slice::<EvidenceLocusWire>(&encoded).unwrap(),
        locus
    );
}

#[cfg(feature = "query")]
#[test]
fn governed_evidence_locus_wire_rejects_unsafe_identity_and_coordinates() {
    let unsafe_identity = serde_json::json!({
        "id": "eg:locus:0000000000000001",
        "subject": { "kind": "artifact", "id": "not-an-opaque-reference" },
        "address": { "kind": "character_range", "start": 0, "end": 1 },
        "policy_ref": "eg:policy:0000000000000003",
        "derivation_ref": "eg:derivation:0000000000000004"
    });
    assert!(serde_json::from_value::<EvidenceLocusWire>(unsafe_identity).is_err());

    let invalid_range = serde_json::json!({
        "id": "eg:locus:0000000000000001",
        "subject": {
            "kind": "artifact",
            "id": "eg:artifact:0000000000000002"
        },
        "address": { "kind": "character_range", "start": 1, "end": 1 },
        "policy_ref": "eg:policy:0000000000000003",
        "derivation_ref": "eg:derivation:0000000000000004"
    });
    assert!(serde_json::from_value::<EvidenceLocusWire>(invalid_range).is_err());
}
