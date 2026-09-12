//! Canonical durable mutation-batch contract.
//!
//! Graph and native stores share one typed logical identity and explicit version
//! semantics. Persistence commits authoritative rows, receipt, idempotency, OCC,
//! fencing, and outbox state atomically before serving projections are published.

mod fault;
mod model;
mod validation;

pub use fault::apply_certification_fault;
pub use model::*;

/// First product on-disk/wire schema. Any other schema is incompatible and is
/// rejected by current-only readers.
pub const MUTATION_BATCH_VERSION: u16 = 1;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::protocol::Method;
    use std::collections::BTreeMap;

    const TEST_ACTOR: &str =
        "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// An operation envelope for a test batch on `identity`.
    ///
    /// Built through the one public constructor, so a fixture cannot become a
    /// second minting path: every field it does not name is this deployment's
    /// documented constant, exactly as a producer with no verified request
    /// carrier gets.
    pub(crate) fn test_envelope(
        identity: &MutationScopeIdentity,
        request_id: u64,
        idempotency_key: &str,
    ) -> MutationEnvelope {
        MutationEnvelope::for_scope(
            CompiledScope {
                identity,
                actor: TEST_ACTOR,
                serving_principal: TEST_ACTOR,
                request_id,
                idempotency_key,
                nonce: crate::contract::Nonce::from_bytes([request_id as u8; 32]),
                now_ms: 10,
            },
            CompiledOperation {
                method: crate::contract::MethodId::new(BATCH_COMPILED_METHODS).unwrap(),
                method_schema_id: method_schema_id(
                    &crate::contract::MethodId::new(BATCH_COMPILED_METHODS).unwrap(),
                )
                .unwrap(),
                method_schema_digest: crate::contract::Digest256::from_bytes([1_u8; 32]),
                canonical_payload_digest: crate::contract::Digest256::from_bytes([2_u8; 32]),
            },
        )
        .unwrap()
    }

    /// Grant the reserved-system capability an `Unversioned` batch requires.
    pub(crate) fn grant_unversioned_capability(batch: &mut MutationBatch) {
        let MutationEnvelope::Operation(envelope) = &mut batch.envelope else {
            panic!("a maintenance batch has no caller capability to grant");
        };
        envelope
            .verified_capabilities
            .insert(MutationCapability::UnversionedSystemMutation);
    }

    fn graph_identity() -> MutationScopeIdentity {
        MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-a").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:test:mutation-batch").unwrap(),
        )
    }

    fn batch() -> MutationBatch {
        let mut batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "batch-1".into(),
            envelope: test_envelope(&graph_identity(), 7, "idem-1"),
            identity: graph_identity(),
            placement_epoch: 3,
            version_expectation: VersionExpectation::Graph(9),
            fencing_token: Some(4),
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: DurabilityDomain::GraphRows,
                method: Method::RemoveNode {
                    node_id: "n".into(),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 10,
        };
        // Mint the envelope over the FINAL body, exactly as the compile path
        // does: `validate` refuses an envelope that covers other bytes.
        batch
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals");
        batch
    }

    #[test]
    fn validates_contiguous_non_empty_batch() {
        batch().validate().unwrap();
        let mut invalid = batch();
        invalid.operations[0].ordinal = 1;
        invalid
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        assert!(invalid.validate().unwrap_err().contains("not contiguous"));
    }

    #[test]
    fn roundtrip_keeps_identity_fences_and_operation() {
        let original = batch();
        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        let decoded: MutationBatch = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.schema_version, 1);
        assert_eq!(decoded.identity, original.identity);
        assert_eq!(decoded.version_expectation, VersionExpectation::Graph(9));
        assert_eq!(decoded.operations[0].domain, DurabilityDomain::GraphRows);
    }

    #[test]
    fn prototype_wire_shape_is_quarantined_without_defaults_or_aliases() {
        let mut value = serde_json::to_value(batch()).unwrap();
        value["tenant"] = serde_json::json!("tenant-a");
        value["graph"] = serde_json::json!("graph-a");
        value["graph_incarnation_id"] = serde_json::json!("incarnation:prototype");
        assert!(serde_json::from_value::<MutationBatch>(value).is_err());

        let mut old = batch();
        old.schema_version = u16::MAX;
        assert!(old.validate().unwrap_err().contains("unsupported"));
    }

    #[test]
    fn semantic_domain_serde_rejects_component_aliases() {
        assert_eq!(
            serde_json::to_value(DurabilityDomain::SemanticIndex).unwrap(),
            serde_json::json!("semantic_index")
        );
        for alias in ["semantic", "vector_index", "ann_index", "text_index"] {
            assert!(
                serde_json::from_value::<DurabilityDomain>(serde_json::json!(alias)).is_err(),
                "accepted non-canonical semantic domain {alias:?}"
            );
        }
    }

    #[test]
    fn authoritative_state_requires_one_checked_graph_step() {
        let mut state_backed = batch();
        state_backed.authoritative_state = Some(MutationStateDescriptor {
            algorithm: "sha256".into(),
            digest: "0".repeat(64),
            source_graph_version: 9,
            target_graph_version: 11,
        });
        state_backed
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        assert!(state_backed
            .validate()
            .unwrap_err()
            .contains("exactly source version plus one"));

        let state = state_backed.authoritative_state.as_mut().unwrap();
        state.source_graph_version = u64::MAX;
        state.target_graph_version = u64::MAX;
        state_backed.version_expectation = VersionExpectation::Graph(u64::MAX);
        state_backed
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        assert!(state_backed.validate().unwrap_err().contains("overflow"));
    }

    #[test]
    fn authoritative_state_accepts_only_product_v2_row_delta() {
        let mut state_backed = batch();
        state_backed.authoritative_state = Some(MutationStateDescriptor {
            algorithm: "sha256-row-delta-v2".into(),
            digest: "0".repeat(64),
            source_graph_version: 9,
            target_graph_version: 10,
        });
        state_backed
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        state_backed.validate().unwrap();
        state_backed.authoritative_state.as_mut().unwrap().algorithm =
            "sha256-row-delta-prototype".into();
        state_backed
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        assert!(state_backed.validate().is_err());
    }

    /// The scope and the domain are two independent axes: the domain names the
    /// operation family, the scope names the storage authority. A graph scope
    /// must therefore carry the graph-authoritative families -- lifecycle,
    /// control-plane, cross-modal, multi-graph -- because their version IS the
    /// graph's OCC counter and no other counter exists for them. It must still
    /// reject a store-authoritative domain, which has its own counter.
    ///
    /// Both halves are asserted here. Accepting the first without the second
    /// would silently let a blob or SQL write ride a graph's version.
    #[test]
    fn graph_scope_carries_graph_authoritative_domains_and_rejects_store_owned_ones() {
        for domain in [
            DurabilityDomain::GraphRows,
            DurabilityDomain::GraphSnapshot,
            DurabilityDomain::RdfDataset,
            DurabilityDomain::Lifecycle,
            DurabilityDomain::ControlPlane,
            DurabilityDomain::CrossModal,
            DurabilityDomain::MultiGraph,
            // Store-authoritative by DEFAULT, but legitimately graph-routed too:
            // `compile_methods` commits whatever it is handed through the graph
            // kernel, and broker state owns no store of its own.
            DurabilityDomain::SqlCatalog,
            DurabilityDomain::Broker,
        ] {
            let mut accepted = batch();
            accepted.operations[0].domain = domain;
            accepted
                .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("a fixture batch reseals its envelope over its final body");
            assert!(
                accepted.validate().is_ok(),
                "graph scope rejected graph-authoritative domain {domain:?}"
            );
        }

        for domain in [
            DurabilityDomain::BlobStore,
            DurabilityDomain::KvStore,
            DurabilityDomain::TimeSeries,
            DurabilityDomain::AnalyticsJob,
        ] {
            let mut rejected = batch();
            rejected.operations[0].domain = domain;
            rejected
                .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("a fixture batch reseals its envelope over its final body");
            let error = rejected
                .validate()
                .expect_err("graph scope accepted store-authoritative domain {domain:?}");
            assert!(
                error.contains("store-authoritative"),
                "unexpected rejection for {domain:?}: {error}"
            );
        }
    }

    /// RF-RULING-007. The blanket refusal of every `Native(SemanticIndex)` batch
    /// is DELETED, not relaxed. It was vacuous and load-bearing at once: no
    /// producer ever built a batch on that domain (`canonical.rs` classified
    /// `AddEmbedding` into the graph domains), so it rejected nothing, while
    /// making the semantic owner tables unreachable through the mutation kernel
    /// at all -- the concrete blocker under the `eg-ann` cutover.
    ///
    /// A semantic write is now served on its own native scope, exactly like
    /// every other store-authoritative domain.
    #[test]
    fn a_semantic_index_batch_is_served_on_its_own_native_scope() {
        let mut semantic = batch();
        semantic.identity = MutationScopeIdentity::native(
            ScopeTenantId::new("tenant-a").unwrap(),
            DurabilityDomain::SemanticIndex,
            LogicalName::new("binding-a").unwrap(),
            IncarnationId::new("incarnation:semantic:1").unwrap(),
        )
        .unwrap();
        semantic.version_expectation = VersionExpectation::Native(9);
        semantic.operations[0].domain = DurabilityDomain::SemanticIndex;
        semantic
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        semantic.validate().unwrap();
    }

    /// What replaces the deleted guard at this layer: a semantic operation is
    /// refused unless the batch's scope IS the semantic authority it names.
    ///
    /// Both directions are asserted, because accepting either one alone would
    /// let a semantic write ride some other authority's version counter:
    /// a graph scope may not carry the store-authoritative semantic family, and
    /// a native scope on a different domain may not carry it either. The
    /// remaining half of the cross-binding proof -- that a handle bound to one
    /// `(tenant, binding, generation)` cannot write another's rows -- is not
    /// expressible here (an operation carries no binding) and is asserted at
    /// admission, where the bound serving scope exists.
    #[test]
    fn a_semantic_operation_is_refused_outside_a_semantic_scope() {
        let mut graph_scoped = batch();
        graph_scoped.operations[0].domain = DurabilityDomain::SemanticIndex;
        graph_scoped
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        assert!(graph_scoped
            .validate()
            .unwrap_err()
            .contains("store-authoritative"));

        let mut foreign_native = batch();
        foreign_native.identity = MutationScopeIdentity::native(
            ScopeTenantId::new("tenant-a").unwrap(),
            DurabilityDomain::KvStore,
            LogicalName::new("binding-a").unwrap(),
            IncarnationId::new("incarnation:kv:1").unwrap(),
        )
        .unwrap();
        foreign_native.version_expectation = VersionExpectation::Native(9);
        foreign_native.operations[0].domain = DurabilityDomain::SemanticIndex;
        foreign_native
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        assert!(foreign_native
            .validate()
            .unwrap_err()
            .contains("does not match its operation"));
    }

    #[test]
    fn unauthorized_unversioned_is_rejected() {
        let mut unversioned = batch();
        unversioned.identity = MutationScopeIdentity::native(
            ScopeTenantId::system(),
            DurabilityDomain::ControlPlane,
            LogicalName::new("cluster-bootstrap").unwrap(),
            IncarnationId::new("incarnation:bootstrap:1").unwrap(),
        )
        .unwrap();
        unversioned.operations[0].domain = DurabilityDomain::ControlPlane;
        unversioned
            .reseal_envelope(crate::contract::Digest256::from_bytes([1_u8; 32]))
            .expect("a fixture batch reseals its envelope over its final body");
        unversioned.version_expectation = VersionExpectation::Unversioned;
        assert!(unversioned
            .validate()
            .unwrap_err()
            .contains("verified capability"));

        grant_unversioned_capability(&mut unversioned);
        unversioned.validate().unwrap();

        unversioned.identity = MutationScopeIdentity::native(
            ScopeTenantId::new("tenant-a").unwrap(),
            DurabilityDomain::ControlPlane,
            LogicalName::new("cluster-bootstrap").unwrap(),
            IncarnationId::new("incarnation:bootstrap:1").unwrap(),
        )
        .unwrap();
        assert!(unversioned.validate().is_err());
    }

    fn outbox(
        identity: MutationScopeIdentity,
        committed_version: CommittedVersion,
    ) -> MutationOutboxRecord {
        MutationOutboxRecord {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "batch-1".into(),
            ordinal: 0,
            identity,
            committed_version,
            commit_sequence: None,
            intent: MutationOutboxIntent {
                topic: "engine.projection.rebuild".into(),
                key: "batch-1".into(),
                payload: Vec::new(),
                headers: BTreeMap::new(),
            },
            created_at_ms: 10,
        }
    }

    #[test]
    fn outbox_requires_committed_version_matching_scope() {
        let graph = outbox(
            graph_identity(),
            CommittedVersion::Graph {
                source: 9,
                target: 10,
            },
        );
        graph.validate().unwrap();
        assert!(outbox(
            graph_identity(),
            CommittedVersion::Native {
                source: 9,
                target: 10,
            }
        )
        .validate()
        .is_err());
    }
}
