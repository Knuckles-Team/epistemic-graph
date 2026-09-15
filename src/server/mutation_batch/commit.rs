//! Durable commit, serialization, and serving-projection publication.

mod envelope;
mod internal;
mod lifecycle;
mod work_item;

#[cfg(feature = "program-optimization")]
mod program;

pub(crate) use envelope::publish_change_envelope_projection;
#[cfg(any(
    test,
    feature = "jobs",
    all(feature = "raft", feature = "epistemic-tms")
))]
pub(crate) use internal::commit_internal_graph_methods;
pub(crate) use internal::{
    commit_internal_graph_methods_with_nonce, lock_graph, InternalGraphCommitRequest,
};
pub(crate) use lifecycle::{commit_lifecycle, lifecycle_was_committed, LifecycleCommitRequest};
#[cfg(test)]
pub(crate) use work_item::changed_work_item_ids;
pub(crate) use work_item::{commit_work_item, WorkItemCommitRequest};

#[cfg(test)]
use internal::{apply_projectable_method, install_validated_internal_replay_snapshot};

#[cfg(feature = "program-optimization")]
pub(crate) use program::{
    commit_program_promotion, resolve_program_promotion_identity, ProgramPromotionRequest,
};

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use crate::graph::GraphCore;
#[cfg(test)]
use crate::mutation_batch::{MutationStateDescriptor, MutationSurface};
#[cfg(test)]
use crate::protocol::{Method, ResultPayload};
#[cfg(test)]
use crate::server::mutation_batch::compile::{compile_methods, CompileBatch};
#[cfg(test)]
use crate::server::persistence::PersistenceBackend;
#[cfg(test)]
use eg_types::contract::Nonce;
#[cfg(test)]
use sha2::{Digest, Sha256};

#[cfg(test)]
mod internal_replay_tests {
    use super::*;

    /// The opaque serving principal this test's job store is opened as.
    /// `eg_types::mutation_batch`'s `validate_serving_principal` requires the
    /// `principal:sha256:<64 hex>` shape for every durable mutation authority,
    /// exactly like `eg_jobs::dev_scope_grant::DEV_PRINCIPAL`; a human-readable
    /// label is refused by the jobs codec before the store ever opens.
    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    const PROMOTION_TEST_PRINCIPAL: &str =
        "principal:sha256:9f2c1d0e4b7a836512cd94ef0a7b61d3428f5c9e0b13a6d748ff205ce9b374a1";

    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    struct PromotionJobScopeVerifier;

    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    impl eg_storage::ScopeGrantVerifier for PromotionJobScopeVerifier {
        fn verify(
            &self,
            _physical: &eg_storage::PhysicalStoreIdentity,
            layout: eg_storage::OwnerLayout,
            _identity: &eg_types::MutationScopeIdentity,
            principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            if layout == eg_storage::OwnerLayout::Jobs
                && principal == PROMOTION_TEST_PRINCIPAL
                && proof == b"promotion-test-proof"
            {
                Ok(())
            } else {
                Err("promotion test scope authority rejected".to_string())
            }
        }
    }

    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn internal_replay_rejects_a_conflicting_terminal_result_after_kernel_admission() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = crate::test_support::temp_dir(
            "eg-internal-mutation-replay",
            "terminal-result-conflict",
        );
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(
                dir.to_string_lossy().into_owned(),
                64,
            )
            .expect("open redb backend"),
        );
        let core = Arc::new(GraphCore::new());
        let method = Method::AddNode {
            node_id: "node-a".to_string(),
            properties_msgpack: Vec::new(),
        };
        let first = commit_internal_graph_methods_with_nonce(
            InternalGraphCommitRequest::new(
                Some(&persistence),
                &core,
                1,
                Some("internal-caller"),
                "internal-result-graph",
                "internal-result-key",
                vec![method.clone()],
                &ResultPayload::String("first-result".to_string()),
            )
            .with_attempt_nonce(Some(Nonce::from_bytes([1; 32]))),
        )
        .await
        .expect("first internal commit");
        assert!(!first.replayed);

        let error = commit_internal_graph_methods_with_nonce(
            InternalGraphCommitRequest::new(
                Some(&persistence),
                &core,
                2,
                Some("internal-caller"),
                "internal-result-graph",
                "internal-result-key",
                vec![method],
                &ResultPayload::String("contradictory-result".to_string()),
            )
            .with_attempt_nonce(Some(Nonce::from_bytes([2; 32]))),
        )
        .await
        .expect_err("a contradictory terminal result must not replay silently");
        assert!(error.contains("conflicting terminal result"), "{error}");
    }

    #[test]
    fn internal_replay_identity_excludes_attempt_fields_but_binds_key_actor_and_payload() {
        fn batch(
            request_id: u64,
            nonce: u8,
            principal: &str,
            key: &str,
            node: &str,
            source: u64,
        ) -> crate::mutation_batch::MutationBatch {
            compile_methods(
                CompileBatch {
                    batch_id: "internal-replay",
                    request_id,
                    attempt_nonce: Some(Nonce::from_bytes([nonce; 32])),
                    principal: Some(principal),
                    tenant: "graph-a",
                    graph: "graph-a",
                    placement_epoch: 0,
                    idempotency_key: key,
                    expected_graph_version: Some(source),
                    fencing_token: None,
                    created_at_ms: request_id,
                    default_surface: MutationSurface::Job,
                    authoritative_state: Some(MutationStateDescriptor {
                        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
                        digest: "0".repeat(64),
                        source_graph_version: source,
                        target_graph_version: source + 1,
                    }),
                },
                vec![Method::RemoveNode {
                    node_id: node.to_string(),
                }],
            )
            .unwrap()
        }
        let identity = |batch: &crate::mutation_batch::MutationBatch| {
            batch
                .envelope
                .operation()
                .unwrap()
                .operation_identity()
                .unwrap()
                .digest()
                .unwrap()
        };
        let first = batch(7, 1, "caller-a", "internal-replay", "node-a", 3);
        let fresh_attempt = batch(19, 2, "caller-a", "internal-replay", "node-a", 9);
        assert_eq!(identity(&first), identity(&fresh_attempt));
        assert_ne!(
            identity(&first),
            identity(&batch(20, 3, "caller-a", "different", "node-a", 9))
        );
        assert_ne!(
            identity(&first),
            identity(&batch(20, 3, "caller-b", "internal-replay", "node-a", 9))
        );
        assert_ne!(
            identity(&first),
            identity(&batch(20, 3, "caller-a", "internal-replay", "other", 9))
        );
    }

    #[test]
    fn internal_replay_snapshot_is_digest_and_version_bound_before_install() {
        let empty = GraphCore::new().snapshot();
        let serving = GraphCore::from_snapshot(empty.clone(), 3).unwrap();
        let staged = GraphCore::from_snapshot(empty, 3).unwrap();
        apply_projectable_method(
            &staged,
            &Method::AddNode {
                node_id: "node-a".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": 1}))
                    .unwrap(),
            },
        )
        .unwrap();
        let snapshot = staged.snapshot();
        let delta =
            crate::graph_delta::GraphRowDelta::between(&serving.snapshot(), &snapshot).unwrap();
        let descriptor = MutationStateDescriptor {
            algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
            digest: hex::encode(Sha256::digest(delta.to_msgpack().unwrap())),
            source_graph_version: 3,
            target_graph_version: 4,
        };

        let mut wrong_digest = descriptor.clone();
        wrong_digest.digest = "0".repeat(64);
        assert!(install_validated_internal_replay_snapshot(
            &serving,
            snapshot.clone(),
            4,
            &wrong_digest,
        )
        .is_err());
        assert_eq!(serving.version(), 3);
        assert!(install_validated_internal_replay_snapshot(
            &serving,
            snapshot.clone(),
            5,
            &descriptor,
        )
        .is_err());
        assert_eq!(serving.version(), 3);

        install_validated_internal_replay_snapshot(&serving, snapshot, 4, &descriptor).unwrap();
        assert_eq!(serving.version(), 4);
    }

    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn promotion_replay_and_failed_cas_preserve_authority_and_restart_identity() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        // `ProgramResultInput` is the public constructor's OWN grouping of the
        // job-result lineage (result/dataset/digest/version), so the fixture
        // takes it whole rather than restating its four fields positionally.
        fn identity(
            revision: u64,
            parent_ref: Option<eg_modality::OpaqueRef>,
            result_input: eg_program::ProgramResultInput,
            tool_policy_token: &str,
            model_profile_token: &str,
        ) -> eg_program::ProgramRevisionIdentity {
            // The corpus is read at the SAME snapshot as the result input, and
            // the candidate digest binds that version, so read it out before
            // the input itself is moved into the constructor below.
            let result_input_snapshot_version = result_input.snapshot_version;
            let program_ref = eg_modality::OpaqueRef::scoped("program", &"1".repeat(64)).unwrap();
            let policy = eg_modality::PolicyEnvelope {
                tenant_ref: eg_modality::OpaqueRef::scoped("tenant", &"2".repeat(64)).unwrap(),
                access_policy_ref: eg_modality::OpaqueRef::scoped("policy", &"3".repeat(64))
                    .unwrap(),
                classification: eg_modality::Classification::Internal,
                retention_policy_ref: eg_modality::OpaqueRef::scoped("retention", &"4".repeat(64))
                    .unwrap(),
                deletion_policy_ref: eg_modality::OpaqueRef::scoped("deletion", &"5".repeat(64))
                    .unwrap(),
                legal_hold_ref: None,
                purpose_refs: vec![
                    eg_modality::OpaqueRef::scoped("purpose", &"6".repeat(64)).unwrap()
                ],
            };
            let tool_policy_ref =
                Some(eg_modality::OpaqueRef::scoped("tool_policy", tool_policy_token).unwrap());
            let model_profile_ref =
                Some(eg_modality::OpaqueRef::scoped("model_profile", model_profile_token).unwrap());
            let demonstration_refs =
                vec![eg_modality::OpaqueRef::scoped("example", &"9".repeat(64)).unwrap()];
            let corpus_ref = eg_modality::OpaqueRef::scoped("corpus", &"a".repeat(64)).unwrap();
            let seed: u64 = 17;
            let program = eg_program::ProgramRevision {
                schema_version: eg_program::PROGRAM_SCHEMA_VERSION,
                program_ref: program_ref.clone(),
                revision: revision - 1,
                parent_ref: parent_ref.clone(),
                signature: eg_program::SignatureSpec {
                    signature_ref: eg_modality::OpaqueRef::scoped("signature", &"9".repeat(64))
                        .unwrap(),
                    instruction_ref: eg_modality::OpaqueRef::scoped("instruction", &"a".repeat(64))
                        .unwrap(),
                    fields: vec![
                        eg_program::FieldSpec {
                            name: "input".to_string(),
                            role: eg_program::FieldRole::Input,
                            schema_ref: eg_modality::OpaqueRef::scoped("schema", &"b".repeat(64))
                                .unwrap(),
                            description_ref: None,
                            required: true,
                        },
                        eg_program::FieldSpec {
                            name: "output".to_string(),
                            role: eg_program::FieldRole::Output,
                            schema_ref: eg_modality::OpaqueRef::scoped("schema", &"c".repeat(64))
                                .unwrap(),
                            description_ref: None,
                            required: true,
                        },
                    ],
                },
                module: eg_program::ModuleKind::Predict,
                adapter: eg_program::AdapterKind::Chat,
                tool_refs: Vec::new(),
                policy: policy.clone(),
            };
            let mut digest = Sha256::new();
            digest.update(b"eg-program.candidate.v3\0");
            digest.update(program.program_ref.as_str().as_bytes());
            digest.update(program.revision.to_le_bytes());
            digest.update(program.signature.signature_ref.as_str().as_bytes());
            digest.update(program.signature.instruction_ref.as_str().as_bytes());
            digest.update(program.module.as_str().as_bytes());
            digest.update(program.adapter.as_str().as_bytes());
            digest.update(program.policy.tenant_ref.as_str().as_bytes());
            digest.update(program.policy.access_policy_ref.as_str().as_bytes());
            digest.update(corpus_ref.as_str().as_bytes());
            digest.update(result_input_snapshot_version.to_le_bytes());
            digest.update(
                eg_program::OptimizerKind::LabeledFewShot
                    .as_str()
                    .as_bytes(),
            );
            digest.update(seed.to_le_bytes());
            digest.update(eg_program::CandidateRole::Proposal.as_str().as_bytes());
            digest.update(b"demonstrations".len().to_le_bytes());
            digest.update(b"demonstrations");
            digest.update((demonstration_refs.len() as u64).to_le_bytes());
            for reference in &demonstration_refs {
                digest.update((reference.as_str().len() as u64).to_le_bytes());
                digest.update(reference.as_str().as_bytes());
            }
            for label in [b"artifacts".as_slice(), b"composition".as_slice()] {
                digest.update((label.len() as u64).to_le_bytes());
                digest.update(label);
                digest.update(0_u64.to_le_bytes());
            }
            for (label, reference) in [
                (b"instruction".as_slice(), None),
                (b"tool_policy".as_slice(), tool_policy_ref.as_ref()),
                (b"model_profile".as_slice(), model_profile_ref.as_ref()),
            ] {
                digest.update((label.len() as u64).to_le_bytes());
                digest.update(label);
                digest.update([u8::from(reference.is_some())]);
                if let Some(reference) = reference {
                    digest.update((reference.as_str().len() as u64).to_le_bytes());
                    digest.update(reference.as_str().as_bytes());
                }
            }
            digest.update((b"modalities".len() as u64).to_le_bytes());
            digest.update(b"modalities");
            digest.update(1_u64.to_le_bytes());
            digest.update((b"text".len() as u64).to_le_bytes());
            digest.update(b"text");
            let content_digest = hex::encode(digest.finalize());
            let candidate_ref =
                eg_modality::OpaqueRef::scoped("program_candidate", &content_digest).unwrap();
            let candidate = eg_program::ProgramCandidate {
                candidate_ref,
                program_ref,
                optimizer: eg_program::OptimizerKind::LabeledFewShot,
                role: eg_program::CandidateRole::Proposal,
                demonstration_refs,
                artifact_refs: Vec::new(),
                composition_refs: Vec::new(),
                instruction_ref: None,
                tool_policy_ref,
                model_profile_ref,
                modalities: std::collections::BTreeSet::from([eg_program::ProgramModality::Text]),
                policy,
                content_digest,
                evaluation: None,
            };
            eg_program::ProgramRevisionIdentity::from_candidate_with_binding(
                &program,
                &candidate,
                parent_ref,
                result_input,
                eg_program::ProgramCorpusBinding {
                    corpus_ref,
                    snapshot_version: result_input_snapshot_version,
                },
                seed,
            )
            .expect("construct promotion identity from the public constructor")
        }

        fn candidate_knowledge(
            identity: &eg_program::ProgramRevisionIdentity,
            evidence_ref: &str,
            promotion_identity: serde_json::Value,
        ) -> serde_json::Value {
            let record = identity
                .candidate_record
                .as_ref()
                .expect("promotion test identity has a candidate record");
            let references = |values: &[eg_modality::OpaqueRef]| {
                serde_json::Value::Array(
                    values
                        .iter()
                        .map(|reference| serde_json::json!(reference.as_str()))
                        .collect(),
                )
            };
            let optional_reference = |reference: Option<&eg_modality::OpaqueRef>| {
                reference.map_or(serde_json::Value::Null, |value| {
                    serde_json::json!(value.as_str())
                })
            };
            serde_json::json!({
                "id": record.candidate_ref.as_str(),
                "kind": "program_candidate",
                "confidence": 0.0,
                "evidence_refs": [evidence_ref],
                "source_refs": [evidence_ref],
                "proof_ids": [],
                "contradiction_ids": [],
                "program_ref": record.program_ref.as_str(),
                "optimizer": record.optimizer.as_str(),
                "execution": record.optimizer.execution().as_str(),
                "candidate_role": record.role.as_str(),
                "demonstration_refs": references(&record.demonstration_refs),
                "artifact_refs": references(&record.artifact_refs),
                "composition_refs": references(&record.composition_refs),
                "instruction_ref": optional_reference(record.candidate_instruction_ref.as_ref()),
                "tool_policy_ref": optional_reference(record.tool_policy_ref.as_ref()),
                "model_profile_ref": optional_reference(record.model_profile_ref.as_ref()),
                "policy": &identity.policy,
                "modalities": &record.modalities,
                "plan_ref": serde_json::Value::Null,
                "plan_step_kinds": [],
                "plan_executors": [],
                "plan_input_refs": [],
                "plan_output_refs": [],
                "plan_depends_on": [],
                "max_operations": serde_json::Value::Null,
                "selected": true,
                "promotion_identity": promotion_identity,
            })
        }

        fn candidate_claim_method(identity: &eg_program::ProgramRevisionIdentity) -> Method {
            let record = identity
                .candidate_record
                .as_ref()
                .expect("promotion test identity has a candidate record");
            let knowledge = candidate_knowledge(
                identity,
                "eg:dataset:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                serde_json::to_value(identity).expect("promotion identity serializes"),
            );
            let properties = serde_json::json!({
                "type": "Claim",
                "family": "program.optimization",
                "about": record.candidate_ref.as_str(),
                "result_ref": record.result_ref.as_str(),
                "knowledge": knowledge,
            });
            Method::AddNode {
                node_id: record.candidate_claim_ref.clone(),
                properties_msgpack: rmp_serde::to_vec_named(&properties).unwrap(),
            }
        }

        fn actual_job_result(
            identity: &eg_program::ProgramRevisionIdentity,
            job: &eg_jobs::AnalyticsJob,
        ) -> eg_jobs::TypedJobResult {
            let knowledge = candidate_knowledge(
                identity,
                job.input_snapshot.dataset_ref.as_str(),
                serde_json::to_value(identity).expect("promotion identity serializes"),
            );
            let row = knowledge
                .as_object()
                .expect("candidate knowledge is an object")
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let schema = [
                ("id", "string", false),
                ("kind", "string", false),
                ("confidence", "float64", false),
                ("evidence_refs", "list<string>", false),
                ("source_refs", "list<string>", false),
                ("proof_ids", "list<string>", false),
                ("contradiction_ids", "list<string>", false),
                ("program_ref", "string", false),
                ("optimizer", "string", false),
                ("execution", "string", false),
                ("candidate_role", "string", true),
                ("demonstration_refs", "list<string>", false),
                ("artifact_refs", "list<string>", false),
                ("composition_refs", "list<string>", false),
                ("instruction_ref", "string", true),
                ("tool_policy_ref", "string", true),
                ("model_profile_ref", "string", true),
                ("policy", "object", true),
                ("modalities", "list<string>", false),
                ("plan_ref", "string", true),
                ("plan_step_kinds", "list<string>", false),
                ("plan_executors", "list<string>", false),
                ("plan_input_refs", "list<string>", false),
                ("plan_output_refs", "list<string>", false),
                ("plan_depends_on", "list<string>", false),
                ("max_operations", "uint64", true),
                ("selected", "bool", false),
                ("promotion_identity", "object", true),
            ]
            .into_iter()
            .map(|(name, logical_type, nullable)| eg_jobs::ResultColumn {
                name: name.to_string(),
                logical_type: logical_type.to_string(),
                nullable,
            })
            .collect();
            eg_jobs::TypedJobResult::new(
                schema,
                vec![row],
                vec![job.input_snapshot.dataset_ref.clone()],
                Vec::new(),
                None,
                None,
                eg_jobs::ReproducibilityManifest {
                    input_dataset_ref: job.input_snapshot.dataset_ref.clone(),
                    input_content_digest: job.input_snapshot.content_digest.clone(),
                    input_snapshot_version: job.input_snapshot.version,
                    algorithm_ref: format!("{}:{}", job.algo.family, job.algo.algorithm),
                    params_digest: job.algo.params_digest.clone(),
                    implementation_version: job.algo.code_version.clone(),
                    environment_version: job.algo.env_version.clone(),
                    policy_fingerprint: job.policy.policy_fingerprint.clone(),
                },
            )
            .expect("construct actual typed program result")
        }

        fn result_claim_method(identity: &eg_program::ProgramRevisionIdentity) -> Method {
            let record = identity
                .candidate_record
                .as_ref()
                .expect("promotion test identity has a candidate record");
            let result_claim_ref = format!("jobclaim:{}", record.result_ref.as_str());
            let properties = serde_json::json!({
                "type": "Claim",
                "family": "program.optimization",
                "about": record.result_ref.as_str(),
                "result_ref": record.result_ref.as_str(),
                "confidence": 0.5,
                "validation_state": "unvalidated",
                "job_id": "job-promotion-test",
                "input_dataset_ref": record.result_input_dataset_ref.as_str(),
                "input_content_digest": record.result_input_content_digest,
                "input_snapshot_version": record.result_input_snapshot_version,
                "algo_family": "program.optimization",
                "algo_algorithm": "labeled_few_shot",
                "algo_params_digest": "promotion-params",
                "algo_code_version": "promotion-test",
                "algo_env_version": "promotion-test",
            });
            Method::AddNode {
                node_id: result_claim_ref,
                properties_msgpack: rmp_serde::to_vec_named(&properties).unwrap(),
            }
        }

        fn claim_methods(identity: &eg_program::ProgramRevisionIdentity) -> Vec<Method> {
            vec![
                result_claim_method(identity),
                candidate_claim_method(identity),
            ]
        }

        let dir = crate::test_support::temp_dir("eg-program-promotion", "cas-replay");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(
                dir.to_string_lossy().into_owned(),
                64,
            )
            .expect("open redb backend"),
        );
        let core = Arc::new(GraphCore::new());
        let seeded_tool_policy_ref =
            eg_modality::OpaqueRef::scoped("tool_policy", &"7".repeat(64)).unwrap();
        let seeded_model_profile_ref =
            eg_modality::OpaqueRef::scoped("model_profile", &"8".repeat(64)).unwrap();
        let binding_seed_result = ResultPayload::Json(serde_json::json!({
            "seed": "governed-program-bindings"
        }));
        commit_internal_graph_methods(InternalGraphCommitRequest::new(
            Some(&persistence),
            &core,
            900,
            Some("promotion-binding-seeder"),
            "program-graph",
            "program-binding-seed",
            vec![
                Method::AddNode {
                    node_id: seeded_tool_policy_ref.as_str().to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "type": "ToolPolicy",
                        "ref": seeded_tool_policy_ref.as_str(),
                        "policy_version": "tool-policy-v1",
                    }))
                    .unwrap(),
                },
                Method::AddNode {
                    node_id: seeded_model_profile_ref.as_str().to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "type": "ModelProfile",
                        "ref": seeded_model_profile_ref.as_str(),
                        "profile_version": "model-profile-v1",
                    }))
                    .unwrap(),
                },
            ],
            &binding_seed_result,
        ))
        .await
        .expect("seed governed program bindings through an earlier durable commit");
        let job_dir = crate::test_support::temp_dir("eg-program-promotion", "job-store");
        std::fs::create_dir_all(&job_dir).expect("create separate job-store directory");
        let job_path = job_dir.join("jobs.redb");
        assert_ne!(job_path, dir.join("graph-0.redb"));
        let job_store = eg_jobs::JobStore::open(
            &job_path,
            &PromotionJobScopeVerifier,
            PROMOTION_TEST_PRINCIPAL,
            b"promotion-test-proof",
        )
        .expect("open physically separate job store");
        let dataset_ref =
            "eg:job_input:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let input_snapshot = eg_jobs::InputSnapshotHandle::new("program-graph", 7)
            .with_dataset(dataset_ref, "b".repeat(64));
        let submitted = job_store
            .submit(eg_jobs::SubmitSpec {
                input_snapshot,
                policy: eg_jobs::JobPolicy {
                    tenant: "promotion-test-tenant".to_string(),
                    actor: "promotion-test-actor".to_string(),
                    purpose: "program-promotion-test".to_string(),
                    policy_fingerprint: "promotion-policy".to_string(),
                    ..Default::default()
                },
                algo: eg_jobs::AlgoVersion {
                    family: "program.optimization".to_string(),
                    algorithm: "labeled_few_shot".to_string(),
                    params_digest: "promotion-params".to_string(),
                    code_version: "promotion-test".to_string(),
                    env_version: "promotion-test".to_string(),
                },
                input_payload: None,
                max_attempts: 1,
                backoff_ms: 0,
            })
            .expect("submit actual optimization job");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_millis() as i64;
        let worker_claim = job_store
            .claim_next(
                "promotion-test-worker",
                &[],
                now_ms,
                60_000,
                eg_jobs::TenantJobQuota::default(),
            )
            .expect("claim actual optimization job")
            .expect("actual optimization job is ready");
        let actual_result_ref = eg_modality::OpaqueRef::new(worker_claim.job.result_ref())
            .expect("job store issues an opaque result reference");
        let result_input_dataset_ref = eg_modality::OpaqueRef::new(dataset_ref.to_string())
            .expect("job input dataset is an opaque reference");
        let first_identity = identity(
            2,
            None,
            eg_program::ProgramResultInput {
                result_ref: actual_result_ref,
                dataset_ref: result_input_dataset_ref.clone(),
                content_digest: "b".repeat(64),
                snapshot_version: 7,
            },
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let staged = job_store
            .stage_result_fenced(
                &submitted.job_id,
                &worker_claim.lease.worker_ref,
                worker_claim.lease.epoch,
                actual_job_result(&first_identity, &worker_claim.job),
                now_ms + 1,
            )
            .expect("stage actual typed optimization result");
        let actual_claim_plan = eg_jobs::plan_result_claim(&staged, 0.5, None)
            .expect("publish actual job result as claim methods");
        let actual_claim_methods = actual_claim_plan.methods;
        let first_result = ResultPayload::Json(serde_json::json!({"receipt": "first"}));
        let first = commit_program_promotion(
            ProgramPromotionRequest::new(
                Some(&persistence),
                &core,
                1,
                Some("program-worker"),
                "program-graph",
                "program-promotion-one",
                actual_claim_methods.clone(),
                &first_result,
            )
            .with_identity(&first_identity)
            .with_attempt_nonce(Some(Nonce::from_bytes([11; 32]))),
        )
        .await
        .expect("first promotion");
        assert!(!first.replayed);

        let replay = commit_program_promotion(
            ProgramPromotionRequest::new(
                Some(&persistence),
                &core,
                2,
                Some("program-worker"),
                "program-graph",
                "program-promotion-one",
                actual_claim_methods.clone(),
                &first_result,
            )
            .with_identity(&first_identity)
            .with_attempt_nonce(Some(Nonce::from_bytes([12; 32]))),
        )
        .await
        .expect("fresh nonce retries the stored promotion receipt");
        assert!(replay.replayed);
        let published = job_store
            .complete_publication_fenced(
                &submitted.job_id,
                &worker_claim.lease.worker_ref,
                worker_claim.lease.epoch,
                now_ms + 2,
            )
            .expect("complete publication after graph commit");
        assert!(matches!(
            published.state,
            eg_jobs::JobState::Succeeded { .. }
        ));

        let second_dataset_ref =
            "eg:job_input:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let second_submitted = job_store
            .submit(eg_jobs::SubmitSpec {
                input_snapshot: eg_jobs::InputSnapshotHandle::new("program-graph", 8)
                    .with_dataset(second_dataset_ref, "c".repeat(64)),
                policy: eg_jobs::JobPolicy {
                    tenant: "promotion-test-tenant".to_string(),
                    actor: "promotion-test-actor".to_string(),
                    purpose: "program-promotion-test-second".to_string(),
                    policy_fingerprint: "promotion-policy".to_string(),
                    ..Default::default()
                },
                algo: eg_jobs::AlgoVersion {
                    family: "program.optimization".to_string(),
                    algorithm: "labeled_few_shot".to_string(),
                    params_digest: "promotion-params-second".to_string(),
                    code_version: "promotion-test".to_string(),
                    env_version: "promotion-test".to_string(),
                },
                input_payload: None,
                max_attempts: 1,
                backoff_ms: 0,
            })
            .expect("submit second actual optimization job");
        let second_worker_claim = job_store
            .claim_next(
                "promotion-test-worker-2",
                &[],
                now_ms + 3,
                60_000,
                eg_jobs::TenantJobQuota::default(),
            )
            .expect("claim second actual optimization job")
            .expect("second actual optimization job is ready");
        let second_result_ref = eg_modality::OpaqueRef::new(second_worker_claim.job.result_ref())
            .expect("second job store result reference is opaque");
        let second_input_dataset_ref = eg_modality::OpaqueRef::new(second_dataset_ref.to_string())
            .expect("second job input dataset is opaque");
        let second_identity_seed = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            eg_program::ProgramResultInput {
                result_ref: second_result_ref,
                dataset_ref: second_input_dataset_ref,
                content_digest: "c".repeat(64),
                snapshot_version: 8,
            },
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let second_staged = job_store
            .stage_result_fenced(
                &second_submitted.job_id,
                &second_worker_claim.lease.worker_ref,
                second_worker_claim.lease.epoch,
                actual_job_result(&second_identity_seed, &second_worker_claim.job),
                now_ms + 4,
            )
            .expect("stage second actual typed optimization result");
        let second_identity: eg_program::ProgramRevisionIdentity = second_staged
            .output
            .as_ref()
            .and_then(|output| output.rows.first())
            .and_then(|row| row.get("promotion_identity"))
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .expect("derive second promotion identity from staged JobStore output");
        second_identity
            .validate()
            .expect("staged second promotion identity remains valid");
        let second_claim_plan = eg_jobs::plan_result_claim(&second_staged, 0.5, None)
            .expect("publish second actual job result as claim methods");
        let second_result = ResultPayload::Json(serde_json::json!({"receipt": "second"}));
        let second = commit_program_promotion(
            ProgramPromotionRequest::new(
                Some(&persistence),
                &core,
                3,
                Some("program-worker"),
                "program-graph",
                "program-promotion-two",
                second_claim_plan.methods,
                &second_result,
            )
            .with_identity(&second_identity)
            .with_attempt_nonce(Some(Nonce::from_bytes([14; 32]))),
        )
        .await
        .expect("second promotion advances the active pointer");
        assert!(!second.replayed);
        let published_second = job_store
            .complete_publication_fenced(
                &second_submitted.job_id,
                &second_worker_claim.lease.worker_ref,
                second_worker_claim.lease.epoch,
                now_ms + 5,
            )
            .expect("complete second publication after graph commit");
        assert!(matches!(
            published_second.state,
            eg_jobs::JobState::Succeeded { .. }
        ));

        let missing_binding_identity = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            eg_program::ProgramResultInput {
                result_ref: second_identity
                    .candidate_record
                    .as_ref()
                    .expect("second identity candidate record")
                    .result_ref
                    .clone(),
                dataset_ref: second_identity
                    .candidate_record
                    .as_ref()
                    .expect("second identity input binding")
                    .result_input_dataset_ref
                    .clone(),
                content_digest: "c".repeat(64),
                snapshot_version: 8,
            },
            &"d".repeat(64),
            &"8".repeat(64),
        );
        let before_missing_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read pre-missing-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        let missing_binding = commit_program_promotion(
            ProgramPromotionRequest::new(
                Some(&persistence),
                &core,
                31,
                Some("program-worker"),
                "program-graph",
                "program-promotion-missing-binding",
                claim_methods(&missing_binding_identity),
                &ResultPayload::Json(serde_json::json!({"receipt": "missing-binding"})),
            )
            .with_identity(&missing_binding_identity)
            .with_attempt_nonce(Some(Nonce::from_bytes([31; 32]))),
        )
        .await;
        assert!(
            missing_binding.is_err(),
            "missing binding must abort staging"
        );
        let after_missing_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read post-missing-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        assert_eq!(before_missing_binding, after_missing_binding);
        assert!(persistence
            .read_mutation_batch(
                &crate::persist::sanitize("program-graph"),
                "program-promotion-missing-binding"
            )
            .await
            .expect("read missing-binding receipt")
            .is_none());

        let wrong_tool_policy_ref =
            eg_modality::OpaqueRef::scoped("tool_policy", &"9".repeat(64)).unwrap();
        let wrong_binding_seed_result = ResultPayload::Json(serde_json::json!({
            "seed": "wrong-type-program-binding"
        }));
        commit_internal_graph_methods(InternalGraphCommitRequest::new(
            Some(&persistence),
            &core,
            901,
            Some("promotion-binding-seeder"),
            "program-graph",
            "program-binding-wrong-type",
            vec![Method::AddNode {
                node_id: wrong_tool_policy_ref.as_str().to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "type": "ModelProfile",
                    "ref": wrong_tool_policy_ref.as_str(),
                }))
                .unwrap(),
            }],
            &wrong_binding_seed_result,
        ))
        .await
        .expect("seed wrong-type binding fixture through a durable commit");
        let wrong_binding_identity = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            eg_program::ProgramResultInput {
                result_ref: second_identity
                    .candidate_record
                    .as_ref()
                    .expect("second identity candidate record")
                    .result_ref
                    .clone(),
                dataset_ref: second_identity
                    .candidate_record
                    .as_ref()
                    .expect("second identity input binding")
                    .result_input_dataset_ref
                    .clone(),
                content_digest: "c".repeat(64),
                snapshot_version: 8,
            },
            &"9".repeat(64),
            &"8".repeat(64),
        );
        let before_wrong_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read pre-wrong-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        let wrong_binding = commit_program_promotion(
            ProgramPromotionRequest::new(
                Some(&persistence),
                &core,
                32,
                Some("program-worker"),
                "program-graph",
                "program-promotion-wrong-binding",
                claim_methods(&wrong_binding_identity),
                &ResultPayload::Json(serde_json::json!({"receipt": "wrong-binding"})),
            )
            .with_identity(&wrong_binding_identity)
            .with_attempt_nonce(Some(Nonce::from_bytes([32; 32]))),
        )
        .await;
        assert!(
            wrong_binding.is_err(),
            "wrong binding type must abort staging"
        );
        let after_wrong_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read post-wrong-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        assert_eq!(before_wrong_binding, after_wrong_binding);
        assert!(persistence
            .read_mutation_batch(
                &crate::persist::sanitize("program-graph"),
                "program-promotion-wrong-binding"
            )
            .await
            .expect("read wrong-binding receipt")
            .is_none());

        let replay_after_pointer_move = commit_program_promotion(
            ProgramPromotionRequest::new(
                Some(&persistence),
                &core,
                4,
                Some("program-worker"),
                "program-graph",
                "program-promotion-one",
                // The SAME operation the key was committed with above -- the real
                // `eg_jobs::plan_result_claim` output, not the hand-built
                // `claim_methods` pair. A replay is identified by its operation, so
                // presenting different methods under the same idempotency key is an
                // IDEMPOTENCY_CONFLICT by contract (which is what the other
                // `claim_methods(..)` call sites here deliberately provoke, each
                // with its own distinct identity).
                actual_claim_methods.clone(),
                &first_result,
            )
            .with_identity(&first_identity)
            .with_attempt_nonce(Some(Nonce::from_bytes([15; 32]))),
        )
        .await
        .expect("same-key retry returns the first receipt after pointer movement");
        assert!(replay_after_pointer_move.replayed);

        let fname = crate::persist::sanitize("program-graph");
        let before_failed = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read pre-CAS-failure snapshot")
            .expect("promotion snapshot");
        let before_failed_bytes = before_failed.0.to_msgpack().unwrap();
        let failed_identity = identity(
            3,
            Some(eg_modality::OpaqueRef::scoped("program_revision", &"4".repeat(64)).unwrap()),
            eg_program::ProgramResultInput {
                result_ref: eg_modality::OpaqueRef::scoped(
                    "job_result",
                    &format!("{}3", "b".repeat(63)),
                )
                .unwrap(),
                dataset_ref: result_input_dataset_ref,
                content_digest: "b".repeat(64),
                snapshot_version: 7,
            },
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let failed = commit_program_promotion(
            ProgramPromotionRequest::new(
                Some(&persistence),
                &core,
                5,
                Some("program-worker"),
                "program-graph",
                "program-promotion-three",
                claim_methods(&failed_identity),
                &ResultPayload::Json(serde_json::json!({"receipt": "failed"})),
            )
            .with_identity(&failed_identity)
            .with_attempt_nonce(Some(Nonce::from_bytes([13; 32]))),
        )
        .await;
        assert!(failed.is_err());
        let after_failed = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read post-CAS-failure snapshot")
            .expect("promotion snapshot");
        assert_eq!(before_failed_bytes, after_failed.0.to_msgpack().unwrap());
        assert!(persistence
            .read_mutation_batch(&fname, "program-promotion-three")
            .await
            .expect("read failed promotion receipt")
            .is_none());

        let graph_before_job_store_removal = after_failed.0.to_msgpack().unwrap();
        drop(job_store);
        assert!(
            job_path.exists(),
            "published job file exists before removal"
        );
        std::fs::remove_file(&job_path).expect("remove only the temporary job-store file");
        assert!(!job_path.exists(), "temporary job-store file was removed");
        let fresh_job_store = eg_jobs::JobStore::open(
            &job_path,
            &PromotionJobScopeVerifier,
            PROMOTION_TEST_PRINCIPAL,
            b"promotion-test-proof",
        )
        .expect("reopen a fresh job store after file removal");
        assert!(
            fresh_job_store
                .list_ids()
                .expect("list fresh job-store ids")
                .is_empty(),
            "the reopened job store has no retained job rows"
        );
        drop(fresh_job_store);
        std::fs::remove_file(&job_path).expect("remove reopened temporary job-store file");
        // Close every graph handle before reopening the authoritative graph. The
        // separate job-store deletion must not be masked by a live graph backend
        // or an in-memory GraphCore snapshot.
        drop(core);
        persistence.shutdown();
        drop(persistence);
        let reopened_persistence: Arc<dyn PersistenceBackend> = Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(
                dir.to_string_lossy().into_owned(),
                64,
            )
            .expect("reopen authoritative graph backend after job-store removal"),
        );
        let graph_after_job_store_reopen = reopened_persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read graph after independent job-store removal")
            .expect("promotion snapshot after independent job-store removal");
        assert_eq!(
            graph_before_job_store_removal,
            graph_after_job_store_reopen.0.to_msgpack().unwrap(),
            "job-store close/removal/reopen leaves retained graph bytes unchanged"
        );
        let restarted = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore promoted graph");
        let pointer = first_identity.active_pointer_ref();
        let pointer_bytes = restarted
            .get_node_properties(pointer.as_str())
            .expect("active pointer survives restart");
        let pointer_value = eg_types::msgpack::decode_property_value(&pointer_bytes)
            .expect("decode active pointer");
        assert_eq!(
            pointer_value
                .get("base_revision")
                .and_then(serde_json::Value::as_u64),
            Some(second_identity.base_revision)
        );
        assert_eq!(
            pointer_value
                .get("revision_ref")
                .and_then(serde_json::Value::as_str),
            Some(second_identity.revision_ref.as_str())
        );
        assert_eq!(
            pointer_value.get("policy"),
            Some(&serde_json::to_value(&second_identity.policy).unwrap()),
        );
        assert_eq!(
            pointer_value
                .get("tool_policy_ref")
                .and_then(serde_json::Value::as_str),
            second_identity
                .tool_policy_ref
                .as_ref()
                .map(eg_modality::OpaqueRef::as_str),
        );
        assert_eq!(
            pointer_value
                .get("model_profile_ref")
                .and_then(serde_json::Value::as_str),
            second_identity
                .model_profile_ref
                .as_ref()
                .map(eg_modality::OpaqueRef::as_str),
        );
        let revision_properties = restarted
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("durable revision survives restart after job retention");
        let revision_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode durable revision");
        let resolved =
            eg_program::ProgramRevisionIdentity::from_durable_properties(&revision_value)
                .expect("resolve durable revision identity after job purge");
        assert_eq!(resolved, second_identity);
        assert_eq!(resolved.policy, second_identity.policy);
        let resolved_chain =
            resolve_program_promotion_identity(&restarted, &second_identity.revision_ref)
                .expect("resolve candidate and claim chain after job purge");
        assert_eq!(resolved_chain, second_identity);
        reopened_persistence.shutdown();
        drop(reopened_persistence);
        let candidate_record = second_identity.candidate_record.as_ref().unwrap();
        assert!(restarted.has_node(second_identity.candidate_ref.as_str()));
        assert!(restarted.has_node(&candidate_record.candidate_claim_ref));
        assert!(restarted.has_node(first_identity.revision_ref.as_str()));
        assert!(restarted.has_node(second_identity.revision_ref.as_str()));

        fn assert_candidate_claim_tamper(
            snapshot: &crate::graph::GraphSnapshot,
            version: u64,
            claim_ref: &str,
            revision_ref: &eg_modality::OpaqueRef,
            mutate: impl FnOnce(&mut serde_json::Value),
        ) {
            let fixture = GraphCore::from_snapshot(snapshot.clone(), version)
                .expect("restore selected-claim tamper fixture");
            let properties = fixture
                .get_node_properties(claim_ref)
                .expect("selected claim exists in tamper fixture");
            let mut value = eg_types::msgpack::decode_property_value(&properties)
                .expect("decode selected claim tamper fixture");
            mutate(&mut value);
            fixture.add_node(
                claim_ref.to_string(),
                rmp_serde::to_vec_named(&value).expect("encode selected claim tamper fixture"),
            );
            assert!(
                resolve_program_promotion_identity(&fixture, revision_ref).is_err(),
                "selected-claim tamper must fail closed"
            );
        }

        assert_candidate_claim_tamper(
            &graph_after_job_store_reopen.0,
            graph_after_job_store_reopen.1,
            &candidate_record.candidate_claim_ref,
            &second_identity.revision_ref,
            |value| {
                value
                    .as_object_mut()
                    .expect("claim is an object")
                    .insert("family".to_string(), serde_json::json!("other.family"));
            },
        );
        assert_candidate_claim_tamper(
            &graph_after_job_store_reopen.0,
            graph_after_job_store_reopen.1,
            &candidate_record.candidate_claim_ref,
            &second_identity.revision_ref,
            |value| {
                value["knowledge"]["selected"] = serde_json::Value::Bool(false);
            },
        );
        assert_candidate_claim_tamper(
            &graph_after_job_store_reopen.0,
            graph_after_job_store_reopen.1,
            &candidate_record.candidate_claim_ref,
            &second_identity.revision_ref,
            |value| {
                value["knowledge"]["promotion_identity"] = serde_json::Value::Null;
            },
        );

        let result_claim_ref = format!("jobclaim:{}", candidate_record.result_ref.as_str());
        let result_digest_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore result-claim digest tamper fixture");
        let result_properties = result_digest_tamper
            .get_node_properties(&result_claim_ref)
            .expect("result claim exists in tamper fixture");
        let mut result_value = eg_types::msgpack::decode_property_value(&result_properties)
            .expect("decode result claim tamper fixture");
        // A digest that differs from the claim's real `input_content_digest` BY
        // CONSTRUCTION. The hardcoded `"c".repeat(64)` that stood here WAS
        // `second_identity`'s genuine input content digest, so the "tamper"
        // wrote the same bytes back: the claim stayed valid, resolution
        // correctly succeeded, and the assertion below proved nothing about
        // digest binding. Deriving the tampered value from the real one keeps
        // that collision impossible if the fixture's digests are ever changed.
        let genuine_input_digest = result_value["input_content_digest"]
            .as_str()
            .expect("result claim carries its input content digest")
            .to_string();
        let tampered_input_digest = if genuine_input_digest.starts_with('d') {
            "e".repeat(64)
        } else {
            "d".repeat(64)
        };
        assert_ne!(
            tampered_input_digest, genuine_input_digest,
            "the tampered input digest must actually differ from the real one"
        );
        result_value["input_content_digest"] = serde_json::json!(tampered_input_digest);
        result_digest_tamper.add_node(
            result_claim_ref.clone(),
            rmp_serde::to_vec_named(&result_value).expect("encode result claim tamper fixture"),
        );
        assert!(
            resolve_program_promotion_identity(
                &result_digest_tamper,
                &second_identity.revision_ref
            )
            .is_err(),
            "result-claim digest tamper must fail closed"
        );

        let modality_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore modality tamper fixture");
        let revision_properties = modality_tamper
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("revision exists in modality tamper fixture");
        let mut modality_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode modality tamper fixture");
        modality_value["candidate_record"]["modalities"] = serde_json::json!(["image"]);
        modality_tamper.add_node(
            second_identity.revision_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&modality_value).expect("encode modality tamper fixture"),
        );
        assert!(
            eg_program::ProgramRevisionIdentity::from_durable_properties(&modality_value).is_err(),
            "candidate modality tamper must invalidate the canonical candidate digest"
        );
        assert!(
            resolve_program_promotion_identity(&modality_tamper, &second_identity.revision_ref)
                .is_err(),
            "candidate modality tamper must invalidate restart resolution"
        );

        let binding_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore binding type tamper fixture");
        let tool_policy_ref = second_identity
            .tool_policy_ref
            .as_ref()
            .expect("tool policy binding");
        binding_tamper.add_node(
            tool_policy_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "type": "ModelProfile",
                "ref": tool_policy_ref.as_str(),
            }))
            .expect("encode binding type tamper fixture"),
        );
        assert!(
            resolve_program_promotion_identity(&binding_tamper, &second_identity.revision_ref)
                .is_err(),
            "binding canonical type tamper must fail closed"
        );

        let authority_digest_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore authority digest tamper fixture");
        let revision_properties = authority_digest_tamper
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("revision exists in authority digest tamper fixture");
        let mut authority_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode authority digest tamper fixture");
        let coordinated_tamper_identity = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            eg_program::ProgramResultInput {
                result_ref: eg_modality::OpaqueRef::scoped("job_result", &"e".repeat(64)).unwrap(),
                dataset_ref: second_identity
                    .candidate_record
                    .as_ref()
                    .expect("second identity input binding")
                    .result_input_dataset_ref
                    .clone(),
                content_digest: "c".repeat(64),
                snapshot_version: 8,
            },
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let coordinated_record = coordinated_tamper_identity
            .candidate_record
            .as_ref()
            .expect("coordinated tamper candidate record");
        coordinated_record
            .validate()
            .expect("coordinated result/claim tamper keeps the record digest valid");
        authority_value["candidate_record"] =
            serde_json::to_value(coordinated_record).expect("encode coordinated tamper record");
        authority_digest_tamper.add_node(
            second_identity.revision_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&authority_value)
                .expect("encode authority digest tamper fixture"),
        );
        assert!(
            resolve_program_promotion_identity(
                &authority_digest_tamper,
                &second_identity.revision_ref
            )
            .is_err(),
            "complete candidate result/claim tamper must fail revision-token validation"
        );

        let alias_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore opaque reference alias fixture");
        let revision_properties = alias_tamper
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("revision exists in opaque reference alias fixture");
        let mut alias_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode opaque reference alias fixture");
        alias_value["candidate_record"]["result_input_dataset_ref"] =
            serde_json::json!(format!("eg:job_input:alias:{}", "c".repeat(64)));
        assert!(
            eg_program::ProgramRevisionIdentity::from_durable_properties(&alias_value).is_err(),
            "result-input namespace aliases must not validate as canonical refs"
        );

        let mut candidate_alias_value = eg_types::msgpack::decode_property_value(
            &alias_tamper
                .get_node_properties(second_identity.revision_ref.as_str())
                .expect("revision remains available in candidate alias fixture"),
        )
        .expect("decode candidate alias fixture");
        candidate_alias_value["candidate_ref"] = serde_json::json!(format!(
            "eg:program_candidate:alias:{}",
            second_identity.content_digest
        ));
        assert!(
            eg_program::ProgramRevisionIdentity::from_durable_properties(&candidate_alias_value)
                .is_err(),
            "candidate namespace aliases must not validate as canonical refs"
        );

        let corrupted = GraphCore::from_snapshot(after_failed.0.clone(), after_failed.1)
            .expect("restore candidate corruption fixture");
        corrupted.add_node(
            second_identity.candidate_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "type": "ProgramCandidate",
                "content_digest": "f".repeat(64),
            }))
            .unwrap(),
        );
        assert!(
            resolve_program_promotion_identity(&corrupted, &second_identity.revision_ref).is_err()
        );

        let dangling_claim = GraphCore::from_snapshot(after_failed.0, after_failed.1)
            .expect("restore dangling claim fixture");
        dangling_claim.remove_node(candidate_record.candidate_claim_ref.clone());
        assert!(
            resolve_program_promotion_identity(&dangling_claim, &second_identity.revision_ref)
                .is_err()
        );
    }
}
