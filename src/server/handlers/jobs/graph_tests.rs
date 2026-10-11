//! Private graph-ref cache tests for the analytics job handler.

use super::*;

#[cfg(test)]
mod resolve_core_ref_tests {
    use super::*;
    use crate::protocol::GraphType;
    use crate::registry::GraphRegistry;

    /// A unique-enough name per call so parallel `cargo test` threads sharing
    /// the ONE process-wide `opaque_graph_ref_index()` singleton never collide
    /// on the same digest.
    fn unique_name(label: &str) -> String {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("w1b-resolve-core-ref-test-{label}-{n}-{nanos}")
    }

    #[test]
    fn resolves_every_resident_graph_by_its_opaque_ref_after_a_cold_scan() {
        let mut registry = GraphRegistry::new();
        let a = unique_name("a");
        let b = unique_name("b");
        registry.create_graph(&a, GraphType::Agent, None).unwrap();
        registry.create_graph(&b, GraphType::Team, None).unwrap();

        let ref_a = native_opaque_ref("graph", &a);
        let ref_b = native_opaque_ref("graph", &b);

        let (name, kind, _core) = resolve_opaque_graph_ref(&registry, &ref_a).unwrap();
        assert_eq!(name, a);
        assert_eq!(kind, GraphType::Agent);

        let (name, kind, _core) = resolve_opaque_graph_ref(&registry, &ref_b).unwrap();
        assert_eq!(name, b);
        assert_eq!(kind, GraphType::Team);

        assert!(resolve_opaque_graph_ref(&registry, "eg:graph:not-a-real-digest").is_none());
    }

    #[test]
    fn a_warm_cache_hit_still_returns_the_live_registry_entry() {
        let mut registry = GraphRegistry::new();
        let name = unique_name("warm");
        registry
            .create_graph(&name, GraphType::Agent, None)
            .unwrap();
        let graph_ref = native_opaque_ref("graph", &name);

        // Cold lookup: falls back to the full scan and populates the index.
        let first = resolve_opaque_graph_ref(&registry, &graph_ref).unwrap();
        assert_eq!(first.0, name);

        // Warm lookup: same digest, same registry -- must take the cache-hit
        // path and still resolve to the live entry.
        let second = resolve_opaque_graph_ref(&registry, &graph_ref).unwrap();
        assert_eq!(second.0, name);
        assert_eq!(second.1, GraphType::Agent);
    }

    #[test]
    fn a_poisoned_cache_entry_can_only_waste_a_rescan_never_resolve_the_wrong_graph() {
        let mut registry = GraphRegistry::new();
        let real_name = unique_name("real");
        registry
            .create_graph(&real_name, GraphType::Agent, None)
            .unwrap();
        let real_ref = native_opaque_ref("graph", &real_name);

        // Directly poison the shared process-wide index with a WRONG name for
        // this exact digest, simulating a stale or corrupted cache entry --
        // e.g. left over from a deleted graph whose name got reused for a
        // digest collision class this test forces by hand.
        opaque_graph_ref_index()
            .write_recovering("opaque graph-ref cache")
            .insert(real_ref.clone(), "not-the-real-graph-name".to_string());

        // The live registry has no graph by that poisoned name, so the digest
        // re-verification must reject the cache hit and fall back to a fresh
        // scan -- resolving to the REAL graph, never the poisoned name and
        // never `None`.
        let resolved = resolve_opaque_graph_ref(&registry, &real_ref);
        assert_eq!(resolved.map(|(name, _, _)| name), Some(real_name));
    }

    #[test]
    fn a_deleted_graphs_stale_cache_entry_resolves_to_none_not_a_wrong_graph() {
        let mut registry = GraphRegistry::new();
        let name = unique_name("deleted");
        registry
            .create_graph(&name, GraphType::Agent, None)
            .unwrap();
        let graph_ref = native_opaque_ref("graph", &name);

        // Populate the cache while the graph is still live.
        assert!(resolve_opaque_graph_ref(&registry, &graph_ref).is_some());

        registry.delete_graph(&name).unwrap();

        // The index may still hold `graph_ref -> name`, but the live registry
        // no longer backs it -- must resolve to `None`, never a stale core.
        assert!(resolve_opaque_graph_ref(&registry, &graph_ref).is_none());
    }
}

// EG-DURABLE-KERNEL-R082.2: exercise the same typed governor used by Submit.
#[cfg(test)]
mod mine_associate_submission_tests {
    use super::*;
    use crate::server::auth::VerifiedRequestContext;

    fn authority() -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
            crate::acl::RequestContextClaims {
                principal: "analytics-test".into(),
                agent_id: "analytics-test".into(),
                tenant: "analytics-test-tenant".into(),
                audience: "epistemic-graph".into(),
                policy_version: "test".into(),
                ..Default::default()
            },
            "analytics-test-key".into(),
        ))
        .unwrap()
    }

    fn govern(kind: JobKind) -> Result<(JobKind, String, String, serde_json::Value), Response> {
        govern_job_kind(&authority(), 82, "policy:test", "", kind, &mut Vec::new())
    }

    fn mining_kind(support: f64, confidence: f64, algorithm: &str) -> JobKind {
        JobKind::MineAssociate {
            transactions: vec![vec!["eg:item:a".into(), "eg:item:b".into()]],
            min_support: support,
            min_confidence: confidence,
            algorithm: algorithm.into(),
        }
    }

    // spec: EG-DURABLE-KERNEL-R082.2
    #[test]
    fn mine_associate_invalid_parameters_have_typed_bounded_refusals() {
        for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.01, 1.01] {
            for (support, confidence) in [(invalid, 0.5), (0.1, invalid)] {
                let response = govern(mining_kind(support, confidence, "fpgrowth")).unwrap_err();
                assert_eq!(response.id, 82);
                assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
                assert_eq!(
                    response.error_detail.as_deref(),
                    Some("association job thresholds are invalid")
                );
                assert!(response.result.is_none());
            }
        }
        for algorithm in ["", "unknown", "fpgrowth\nprivate-input", "private-input"] {
            let response = govern(mining_kind(0.1, 0.5, algorithm)).unwrap_err();
            assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
            assert_eq!(
                response.error_detail.as_deref(),
                Some("unsupported association job algorithm")
            );
        }
    }

    // spec: EG-DURABLE-KERNEL-R082.2
    #[test]
    fn mine_associate_defaults_aliases_and_threshold_boundaries_are_accepted() {
        let payload = rmp_serde::to_vec_named(&serde_json::json!({
            "id": 82, "graph": "analytics-test", "auth_token": "", "agent_id": null,
            "method": "AnalyticsJob", "params": {"op": {"Submit": {
                "graph": "analytics-test", "kind": {
                    "MineAssociate": {"transactions": [["eg:item:a", "eg:item:b"]]}
                }
            }}}
        }))
        .unwrap();
        let request = crate::server::transport::decode_request_payload(&payload, 128)
            .expect("the same envelope must accept valid transactions with defaults");
        let crate::protocol::Method::AnalyticsJob {
            op: JobOp::Submit(spec),
        } = request.method
        else {
            panic!("expected a decoded Submit operation");
        };
        let kind = spec.kind;
        let (kind, family, algorithm, params) = govern(kind).unwrap();
        assert_eq!(family, "mining.association");
        assert_eq!(algorithm, "fpgrowth");
        assert_eq!(params["min_support"], 0.1);
        assert_eq!(params["min_confidence"], 0.5);
        assert!(matches!(kind, JobKind::MineAssociate { .. }));
        for algorithm in ["apriori", "fpgrowth", "fp-growth", "fp_growth", "eclat"] {
            for (support, confidence) in [(0.0, 0.0), (1.0, 1.0)] {
                govern(mining_kind(support, confidence, algorithm)).unwrap();
            }
        }
    }

    // spec: EG-PLACEMENT-R003.3
    #[test]
    fn mine_associate_accepts_thirty_one_distinct_items_and_refuses_thirty_two() {
        for count in [31, 32] {
            let transactions = vec![(0..count).map(|i| format!("item:{i}")).collect()];
            let outcome = govern(JobKind::MineAssociate {
                transactions,
                min_support: 0.1,
                min_confidence: 0.5,
                algorithm: "fpgrowth".into(),
            });
            if count == 31 {
                let (kind, _, _, _) = outcome.expect("the distinct-item cap is inclusive");
                match kind {
                    JobKind::MineAssociate { transactions, .. } => {
                        assert_eq!(transactions[0].len(), 31);
                        assert_eq!(
                            transactions
                                .iter()
                                .flatten()
                                .collect::<std::collections::BTreeSet<_>>()
                                .len(),
                            31
                        );
                    }
                    #[cfg(feature = "program-optimization")]
                    JobKind::ProgramOptimize { .. } => panic!("expected association job"),
                }
            } else {
                let response = outcome.expect_err("one item above the cap must be refused");
                assert_eq!(response.id, 82);
                assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
                assert_eq!(
                    response.error_detail.as_deref(),
                    Some("association job exceeds the distinct-item limit")
                );
                assert!(response.result.is_none());
            }
        }
    }

    // spec: EG-DURABLE-KERNEL-R082.2
    #[test]
    fn mine_associate_malformed_transaction_shapes_have_typed_wire_refusals() {
        // Use the actual native transport decoder: malformed shapes must retain
        // their correlation id and return a bounded typed refusal before Submit.
        for transactions in [
            serde_json::json!(null),
            serde_json::json!("not-a-list"),
            serde_json::json!({}),
            serde_json::json!(7),
            serde_json::json!(true),
            serde_json::json!(["not-a-nested-list"]),
            serde_json::json!([null]),
            serde_json::json!([7]),
            serde_json::json!([true]),
            serde_json::json!([{}]),
            serde_json::json!([[7]]),
            serde_json::json!([[null]]),
            serde_json::json!([[true]]),
            serde_json::json!([[{}]]),
            serde_json::json!([[[]]]),
        ] {
            let payload = rmp_serde::to_vec_named(&serde_json::json!({
                "id": 82, "graph": "analytics-test", "auth_token": "", "agent_id": null,
                "method": "AnalyticsJob", "params": {"op": {"Submit": {
                    "graph": "analytics-test", "kind": {
                        "MineAssociate": {"transactions": transactions}
                    }
                }}}
            }))
            .unwrap();
            let response = crate::server::transport::decode_request_payload(&payload, 128)
                .expect_err("malformed transactions must be refused before submission");
            assert_eq!(response.id, 82);
            assert_eq!(response.error.as_deref(), Some("INVALID_ARGUMENT"));
            assert_eq!(
                response.error_detail.as_deref(),
                Some("invalid request encoding")
            );
            assert!(response.result.is_none());
        }
    }
}
