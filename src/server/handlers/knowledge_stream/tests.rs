use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(policy: u8) -> KnowledgeStreamAuthority {
        KnowledgeStreamAuthority {
            tenant_ref: OpaqueRef::scoped("tenant", &format!("{:064x}", 1)).unwrap(),
            access_policy_ref: OpaqueRef::scoped("access-policy", &format!("{policy:064x}"))
                .unwrap(),
            reference_key: [policy; 32],
            #[cfg(feature = "security")]
            policy_lease: None,
            #[cfg(feature = "security")]
            policy_store: None,
        }
    }

    #[test]
    fn verified_authority_exposes_only_keyed_opaque_references() {
        let claims = RequestContextClaims {
            principal: "originating-subject".to_string(),
            tenant: "tenant-display-name".to_string(),
            audience: "engine".to_string(),
            agent_id: "effective-agent".to_string(),
            roles: vec!["reader".to_string()],
            scopes: vec!["query:stream".to_string()],
            policy_version: "policy-display-name".to_string(),
            delegation: vec![
                "originating-subject".to_string(),
                "effective-agent".to_string(),
            ],
            node: None,
            priority: None,
        };
        let authority = KnowledgeStreamAuthority::from_verified("server-secret", &claims).unwrap();
        for reference in [&authority.tenant_ref, &authority.access_policy_ref] {
            assert!(OpaqueRef::new(reference.as_str().to_string()).is_ok());
            assert!(!reference.as_str().contains("tenant-display-name"));
            assert!(!reference.as_str().contains("originating-subject"));
            assert!(!reference.as_str().contains("effective-agent"));
            assert!(!reference.as_str().contains("engine"));
            assert!(!reference.as_str().contains("policy-display-name"));
        }
    }

    fn execution(
        authority: &KnowledgeStreamAuthority,
        family: KnowledgeResultFamily,
        count: usize,
    ) -> FamilyExecution {
        FamilyExecution {
            rows: (0..count)
                .map(|index| {
                    native_row(
                        authority,
                        family_name(family),
                        &index.to_le_bytes(),
                        1.0,
                        None,
                    )
                })
                .collect(),
            source_result: ResultPayload::Count(count as u64),
        }
    }

    fn query(family: KnowledgeResultFamily) -> KnowledgeStreamQuery {
        match family {
            KnowledgeResultFamily::Graph => KnowledgeStreamQuery::Graph {
                label: String::new(),
                limit: 0,
            },
            KnowledgeResultFamily::Sql => KnowledgeStreamQuery::Sql {
                query: "SELECT 1".to_string(),
                params_msgpack: Vec::new(),
            },
            KnowledgeResultFamily::Rdf => KnowledgeStreamQuery::Rdf {
                query: "SELECT * WHERE { ?s ?p ?o }".to_string(),
                base_iri: String::new(),
                type_convention: String::new(),
            },
            KnowledgeResultFamily::Vector => KnowledgeStreamQuery::Vector {
                keywords: Vec::new(),
                query_embedding: Vec::new(),
                k: 1,
            },
            KnowledgeResultFamily::TimeSeries => KnowledgeStreamQuery::TimeSeries {
                series_id: "series".to_string(),
                from: 0,
                to: 1,
            },
            KnowledgeResultFamily::Job => KnowledgeStreamQuery::Job {
                job_id: "job".to_string(),
            },
            KnowledgeResultFamily::CrossModal => KnowledgeStreamQuery::CrossModal {
                text: "MATCH (n) |> LIMIT 1".to_string(),
            },
        }
    }

    #[test]
    fn all_seven_families_use_bounded_batches_and_resumable_cursor() {
        for family in KnowledgeResultFamily::ALL {
            let authority = authority(2);
            let first = serve_execution(
                "tenant-graph",
                &authority,
                4,
                Some(9),
                KnowledgeStreamRequestV1 {
                    schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                    query: query(family),
                    batch_size: 2,
                    cursor: None,
                    projection: KnowledgeStreamProjection::ArrowIpcV1,
                },
                execution(&authority, family, 5),
            )
            .unwrap();
            assert_eq!(first.family, family);
            assert_eq!(first.cursor.row_offset, 2);
            assert!(!first.cursor.exhausted);
            assert_eq!(
                first.cursor.access_policy_ref.as_str(),
                authority.access_policy_ref.as_str()
            );
            assert!(!first.payload.is_empty());

            let second = serve_execution(
                "tenant-graph",
                &authority,
                4,
                Some(9),
                KnowledgeStreamRequestV1 {
                    schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                    query: query(family),
                    batch_size: 2,
                    cursor: Some(first.cursor),
                    projection: KnowledgeStreamProjection::ArrowIpcV1,
                },
                execution(&authority, family, 5),
            )
            .unwrap();
            assert_eq!(second.cursor.row_offset, 4);
            assert_eq!(second.cursor.batch_index, 2);
        }
    }

    #[test]
    fn cursor_cannot_cross_authority_placement_or_changed_snapshot() {
        let original_authority = authority(2);
        let first = serve_execution(
            "tenant-graph",
            &original_authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: None,
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&original_authority, KnowledgeResultFamily::Graph, 3),
        )
        .unwrap();
        let resumed = |authority: &KnowledgeStreamAuthority, epoch: u64, count: usize| {
            serve_execution(
                "tenant-graph",
                authority,
                epoch,
                Some(9),
                KnowledgeStreamRequestV1 {
                    schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                    query: query(KnowledgeResultFamily::Graph),
                    batch_size: 2,
                    cursor: Some(first.cursor.clone()),
                    projection: KnowledgeStreamProjection::ArrowIpcV1,
                },
                execution(authority, KnowledgeResultFamily::Graph, count),
            )
        };
        assert!(resumed(&authority(3), 4, 3).is_err());
        assert!(resumed(&original_authority, 5, 3).is_err());
        assert!(resumed(&original_authority, 4, 4).is_err());

        let mut tampered = first.cursor.clone();
        tampered.row_offset = 0;
        tampered.batch_index = 0;
        assert!(serve_execution(
            "tenant-graph",
            &original_authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: Some(tampered),
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&original_authority, KnowledgeResultFamily::Graph, 3),
        )
        .is_err());
    }

    #[test]
    fn native_projection_is_bounded_and_resumable() {
        let authority = authority(2);
        let response = serve_execution(
            "tenant-graph",
            &authority,
            0,
            None,
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Sql),
                batch_size: 2,
                cursor: None,
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&authority, KnowledgeResultFamily::Sql, 5),
        )
        .unwrap();
        assert!(!response.cursor.exhausted);
        assert_eq!(response.cursor.row_offset, 2);
        assert_eq!(response.projection, KnowledgeStreamProjection::ArrowIpcV1);
    }

    #[test]
    fn cursor_replay_is_idempotent_but_policy_replacement_is_denied() {
        let authority = authority(2);
        let first = serve_execution(
            "tenant-graph",
            &authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: None,
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&authority, KnowledgeResultFamily::Graph, 3),
        )
        .unwrap();

        // A client retrying the same acknowledged cursor is intentionally
        // idempotent: stateless cursors may be replayed while the exact policy
        // image remains current, and the same continuation page is returned.
        let second = serve_execution(
            "tenant-graph",
            &authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: Some(first.cursor.clone()),
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&authority, KnowledgeResultFamily::Graph, 3),
        )
        .unwrap();
        let replay = serve_execution(
            "tenant-graph",
            &authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: Some(first.cursor.clone()),
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&authority, KnowledgeResultFamily::Graph, 3),
        )
        .unwrap();
        assert_eq!(replay.payload, second.payload);

        // Even if an implementation accidentally reuses the visible policy
        // reference after an A→B→A replacement, rotating the lease-bound
        // reference key still rejects the old cursor (an ABA replay).
        let mut aba = authority(2);
        aba.reference_key = [3; 32];
        assert!(serve_execution(
            "tenant-graph",
            &aba,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: Some(first.cursor.clone()),
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&aba, KnowledgeResultFamily::Graph, 3),
        )
        .is_err());

        // A new policy image gets a different opaque policy binding.  The old
        // cursor must not be accepted even though all non-policy stream fields
        // (graph, placement, query, and source shape) are unchanged.
        let replacement = authority(3);
        assert!(serve_execution(
            "tenant-graph",
            &replacement,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: Some(first.cursor),
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&replacement, KnowledgeResultFamily::Graph, 3),
        )
        .is_err());
    }

    #[cfg(all(feature = "security", feature = "redb"))]
    #[test]
    fn durable_lease_revocation_rejects_resume_before_any_cursor_metadata() {
        use std::collections::BTreeMap;
        use std::sync::Arc;

        use eg_core::rbac::RbacPolicy;
        use eg_core::rbac_persist::{IdentityBootstrapState, RbacStore};
        use eg_types::acl::{
            AgentIdentity, AgentRole, Grant, GrantEffect, RbacAction, ResourceSelector, Role,
        };

        let directory = std::env::temp_dir().join(format!(
            "eg-knowledge-stream-lease-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let store = Arc::new(RbacStore::open(&directory).expect("open durable policy store"));
        let mut policy = RbacPolicy::new();
        policy.add_role(Role::new("reader"));
        let grant = Grant {
            role: "reader".to_string(),
            resource: ResourceSelector::Graph("tenant-graph".to_string()),
            action: RbacAction::Read,
            effect: GrantEffect::Allow,
        };
        policy.add_grant(grant.clone());
        let mut identities = BTreeMap::new();
        identities.insert(
            "alice".to_string(),
            AgentIdentity {
                agent_id: "alice".to_string(),
                role: AgentRole::Agent,
                teams: Vec::new(),
                roles: vec!["reader".to_string()],
            },
        );
        store
            .save(&policy, &identities, IdentityBootstrapState::Consumed)
            .expect("save authorized policy");
        let lease = store
            .acquire_graph_policy_lease(
                "alice",
                "tenant",
                "tenant-graph",
                crate::isolation::AccessLevel::Read,
            )
            .expect("mint graph policy lease");
        let claims = RequestContextClaims {
            principal: "alice-principal".to_string(),
            tenant: "tenant".to_string(),
            audience: "engine".to_string(),
            agent_id: "alice".to_string(),
            roles: vec!["reader".to_string()],
            scopes: vec!["query:stream".to_string()],
            policy_version: "caller-display-version-is-not-authority".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let authority = KnowledgeStreamAuthority::from_verified_with_lease(
            "server-secret",
            &claims,
            "tenant-graph",
            Arc::new(lease),
            store.clone(),
        )
        .expect("bind exact durable lease");
        let first = serve_execution(
            "tenant-graph",
            &authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: None,
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&authority, KnowledgeResultFamily::Graph, 3),
        )
        .expect("serve initial page");

        policy.remove_grant(&grant);
        store
            .save(&policy, &identities, IdentityBootstrapState::Consumed)
            .expect("persist revocation");
        assert!(authority.validate_before().is_err());
        let error = serve_execution(
            "tenant-graph",
            &authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: Some(first.cursor),
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&authority, KnowledgeResultFamily::Graph, 3),
        )
        .expect_err("revoked lease must not publish a resumed page");
        assert_eq!(error, "KnowledgeStream graph policy lease is stale");

        drop(authority);
        drop(store);
        let _ = std::fs::remove_dir_all(directory);
    }

    #[cfg(feature = "security")]
    #[test]
    fn claims_only_authority_cannot_enter_the_served_path() {
        let authority = authority(2);
        assert!(authority
            .validate_request_binding("verified-agent", "tenant-graph")
            .is_err());
        assert!(authority.validate_before().is_err());
        assert!(authority.validate_after().is_err());
    }
}
