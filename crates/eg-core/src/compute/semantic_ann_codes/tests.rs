//! Positive and negative proofs for the kernel-backed ANN code tier.
//!
//! Layer (b) of RF-RULING-007's replacement for the deleted domain guard: the
//! guard said semantic mutations "remain unserved"; what actually protects the
//! semantic owner tables is that a handle bound to one `(tenant, binding,
//! generation)` cannot admit, write or retire another's rows -- and that a read
//! creates no authority at all.

use std::sync::Arc;

use super::rows::{BoundBindingRows, BoundCodeRows};
use super::{
    compare_source_revision, current_checkpoint_from_tables, generation_identity,
    persist_stage_artifact, stage_intent_outbox, store_file_name, validate_sql_source_revision,
    GenerationRetirement, SemanticCodeStore,
};
use crate::compute::semantic::SemanticStore;
use crate::test_scope_grant::{TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF};
use eg_storage::{
    OwnerLayout, PhysicalStoreIdentity, ScopeGrantVerifier, ANN_CODES, SEMANTIC_ANN,
    SEMANTIC_BINDINGS, SEMANTIC_CHECKPOINTS, SEMANTIC_CHECKPOINT_HEADS, SEMANTIC_DEAD_LETTERS,
    SEMANTIC_HEADS, SEMANTIC_LEXICAL, SEMANTIC_POINTERS, SEMANTIC_SOURCE_PROGRESS,
    SEMANTIC_SQL_SOURCES, SEMANTIC_STAGES, SEMANTIC_STATES,
};
use eg_types::contract::Nonce;
use eg_types::mutation_batch::MutationOutboxIntent;
use eg_types::semantic_index::{
    SemanticActivationTarget, SemanticActivePointer, SemanticAnnIndexManifest,
    SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticAuthorizationReceipt,
    SemanticAuthorizationReceiptDraft, SemanticBinding, SemanticBindingDraft, SemanticBindingState,
    SemanticBindingStateTransition, SemanticDeadLetter, SemanticDeadLetterDraft, SemanticDigest,
    SemanticExpectedEntity, SemanticGenerationAggregate, SemanticGenerationArtifact,
    SemanticGenerationCheckpoint, SemanticGenerationCheckpointDraft,
    SemanticGenerationCheckpointUpdate, SemanticGenerationDependency, SemanticGenerationMember,
    SemanticIndexMutation, SemanticLexicalIndexManifest, SemanticLexicalIndexSpec,
    SemanticModelIdentity, SemanticPolicyComponents, SemanticSourceProgress,
    SemanticSourceSelector, SemanticSqlSourceIdentity, SemanticSqlSourceManifest,
    SemanticSqlSourceManifestDraft, SemanticStage, SemanticStageArtifact, SemanticStageIntent,
    SemanticStageIntentDraft, SemanticStageOutcome, SemanticStagePredecessor, SemanticStageReceipt,
    SemanticStageScope, SemanticStageTransition, SemanticVectorMetric, SqlColumnRef,
    SEMANTIC_SQL_CATALOG_ID,
};
use eg_types::EmbeddingSpaceRef;
// `ReadableTable` only: it is the trait that puts `.get()` on the kernel's own
// returned table handles, and `semantic_arch_gate` deliberately excludes
// `redb::Table`/`redb::ReadOnlyTable` from its forbidden set for exactly that
// reason. No `Database`, `TableDefinition` or transaction type is imported --
// every table here is reached through the mutation kernel.
use redb::ReadableTable;

const TENANT: &str = "native";
const BINDING: &str = "semantic-binding-a";

fn digest(byte: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([byte; 32])
}

struct AnyTenantSemanticScopeVerifier;

impl ScopeGrantVerifier for AnyTenantSemanticScopeVerifier {
    fn verify(
        &self,
        _physical: &PhysicalStoreIdentity,
        layout: OwnerLayout,
        _identity: &eg_types::MutationScopeIdentity,
        principal: &str,
        proof: &[u8],
    ) -> Result<(), String> {
        if layout != OwnerLayout::SemanticIndex
            || principal != TEST_PRINCIPAL
            || proof != TEST_PROOF
        {
            return Err("test scope authority rejected".to_string());
        }
        Ok(())
    }
}

fn checkpoint_head_fixture(completed: usize, seed: u8) -> SemanticGenerationCheckpoint {
    let digest = |offset| SemanticDigest::from_bytes([seed.wrapping_add(offset); 32]);
    let binding_digest = SemanticDigest::from_bytes([8; 32]);
    let expected = ["entity:a", "entity:b"]
        .into_iter()
        .map(|source_entity_id| SemanticExpectedEntity {
            source_entity_id: source_entity_id.to_string(),
            source_revision: "r1".to_string(),
        })
        .collect();
    let members = ["entity:a", "entity:b"]
        .into_iter()
        .take(completed)
        .enumerate()
        .map(|(index, source_entity_id)| SemanticGenerationMember {
            source_entity_id: source_entity_id.to_string(),
            source_revision: "r1".to_string(),
            receipt_digest: digest(index as u8 + 1),
            artifact_digest: digest(index as u8 + 3),
        })
        .collect();
    let aggregate = SemanticGenerationAggregate::create(
        "binding-a",
        binding_digest,
        1,
        "r1",
        SemanticStage::LexicalIndex,
        expected,
        members,
    )
    .unwrap();
    SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: "binding-a".to_string(),
        binding_digest,
        generation: 1,
        source_revision: "r1".to_string(),
        stage: SemanticStage::LexicalIndex,
        aggregate: aggregate.clone(),
        dependency: SemanticGenerationDependency::None,
        artifact_digest: aggregate.aggregate_artifact_digest,
        completed_at: format!("2026-09-08T00:00:{seed:02}Z"),
    })
    .unwrap()
}

fn complete_checkpoint_fixture(
    stage: SemanticStage,
    seed: u8,
    dependency: SemanticGenerationDependency,
) -> SemanticGenerationCheckpoint {
    let digest = |offset| SemanticDigest::from_bytes([seed.wrapping_add(offset); 32]);
    let binding_digest = SemanticDigest::from_bytes([8; 32]);
    let member = SemanticGenerationMember {
        source_entity_id: "entity:a".to_string(),
        source_revision: "r1".to_string(),
        receipt_digest: digest(1),
        artifact_digest: digest(2),
    };
    let aggregate = SemanticGenerationAggregate::create(
        BINDING,
        binding_digest,
        1,
        "r1",
        stage,
        vec![SemanticExpectedEntity {
            source_entity_id: "entity:a".to_string(),
            source_revision: "r1".to_string(),
        }],
        vec![member],
    )
    .unwrap();
    SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: BINDING.to_string(),
        binding_digest,
        generation: 1,
        source_revision: "r1".to_string(),
        stage,
        aggregate: aggregate.clone(),
        dependency,
        artifact_digest: aggregate.aggregate_artifact_digest,
        completed_at: format!("2026-09-08T00:01:{seed:02}Z"),
    })
    .unwrap()
}

struct GenerationCheckpointFixture {
    progress: SemanticSourceProgress,
    transition: SemanticStageTransition,
    update: SemanticGenerationCheckpointUpdate,
    mutation_bytes: Vec<u8>,
}

fn generation_checkpoint_fixture(stage: SemanticStage) -> GenerationCheckpointFixture {
    let binding_digest = SemanticDigest::from_bytes([8; 32]);
    let previous_stage = match stage {
        SemanticStage::LexicalIndex => SemanticStage::GraphProjection,
        SemanticStage::AnnIndex => SemanticStage::Vector,
        _ => panic!("fixture only supports S3/S5"),
    };
    let previous_receipt_digest = SemanticDigest::from_bytes([17; 32]);
    let coverage = if stage == SemanticStage::AnnIndex {
        Some(complete_checkpoint_fixture(
            SemanticStage::Vector,
            40,
            SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::LexicalIndex,
                checkpoint_digest: SemanticDigest::from_bytes([39; 32]),
            },
        ))
    } else {
        None
    };
    let predecessor = match coverage.as_ref() {
        Some(checkpoint) => SemanticStagePredecessor::GenerationCoverage {
            checkpoint: Box::new(checkpoint.clone()),
        },
        None => SemanticStagePredecessor::EntityReceipt {
            stage: previous_stage,
            receipt_digest: previous_receipt_digest,
        },
    };
    let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: BINDING.to_string(),
        binding_digest,
        generation: 1,
        scope: SemanticStageScope::Entity {
            source_entity_id: "entity:a".to_string(),
        },
        source_revision: "r1".to_string(),
        stage,
        predecessor,
        input_digest: SemanticDigest::from_bytes([21; 32]),
    })
    .unwrap();
    let receipt = SemanticStageReceipt {
        intent_digest: intent.intent_digest,
        output_digest: SemanticDigest::from_bytes([22; 32]),
        cursor: "cursor:1".to_string(),
        completed_at: "2026-09-08T00:02:00Z".to_string(),
        outcome: SemanticStageOutcome::Completed,
    };
    let member = SemanticGenerationMember {
        source_entity_id: "entity:a".to_string(),
        source_revision: "r1".to_string(),
        receipt_digest: receipt.receipt_digest(),
        artifact_digest: receipt.output_digest,
    };
    let aggregate = SemanticGenerationAggregate::create(
        BINDING,
        binding_digest,
        1,
        "r1",
        stage,
        vec![SemanticExpectedEntity {
            source_entity_id: "entity:a".to_string(),
            source_revision: "r1".to_string(),
        }],
        vec![member.clone()],
    )
    .unwrap();
    let dependency = coverage
        .as_ref()
        .map_or(SemanticGenerationDependency::None, |checkpoint| {
            SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::Vector,
                checkpoint_digest: checkpoint.checkpoint_digest,
            }
        });
    let successor = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: BINDING.to_string(),
        binding_digest,
        generation: 1,
        source_revision: "r1".to_string(),
        stage,
        aggregate: aggregate.clone(),
        dependency,
        artifact_digest: aggregate.aggregate_artifact_digest,
        completed_at: receipt.completed_at.clone(),
    })
    .unwrap();
    let update = SemanticGenerationCheckpointUpdate {
        expected_previous_checkpoint_digest: None,
        expected_previous_completed_count: 0,
        member,
        successor,
    };
    let transition = SemanticStageTransition {
        intent,
        receipt,
        generation_checkpoint: Some(Box::new(update.clone())),
    };
    transition.validate().unwrap();
    let mutation = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(transition.clone()),
        artifact: SemanticStageArtifact::None,
    };
    GenerationCheckpointFixture {
        progress: SemanticSourceProgress {
            binding_id: BINDING.to_string(),
            binding_digest,
            generation: 1,
            source_entity_id: "entity:a".to_string(),
            source_revision: "r1".to_string(),
            completed_stage: Some(previous_stage),
            completed_receipt_digest: Some(previous_receipt_digest),
            superseded_by_revision: None,
            updated_at: "unix-ms:1".to_string(),
        },
        transition,
        update,
        mutation_bytes: mutation.to_canonical_cbor().unwrap(),
    }
}

/// A unique temp dir per test invocation (no external dev-dep needed).
fn tmp_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-semantic-codes-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ))
}

fn open_store_for(dir: &std::path::Path, tenant: &str, binding: &str) -> SemanticCodeStore {
    SemanticCodeStore::open(
        dir,
        Arc::new(TestScopeVerifier {
            layout: OwnerLayout::SemanticIndex,
        }),
        TEST_PRINCIPAL,
        TEST_PROOF,
        tenant,
        binding,
    )
    .unwrap()
}

fn open_store_for_any_tenant(
    dir: &std::path::Path,
    tenant: &str,
    binding: &str,
) -> SemanticCodeStore {
    SemanticCodeStore::open(
        dir,
        Arc::new(AnyTenantSemanticScopeVerifier),
        TEST_PRINCIPAL,
        TEST_PROOF,
        tenant,
        binding,
    )
    .unwrap()
}

fn open_store(dir: &std::path::Path) -> SemanticCodeStore {
    open_store_for(dir, TENANT, BINDING)
}

fn pending_binding(source_revision: &str) -> SemanticBinding {
    binding_for_generation(source_revision, 1)
}

fn binding_for_generation(source_revision: &str, generation: u64) -> SemanticBinding {
    SemanticBinding::create(SemanticBindingDraft {
        binding_id: BINDING.to_string(),
        tenant_id: TENANT.to_string(),
        actor_scope: "semantic:index-maintainer".to_string(),
        effective_actor_scope: "semantic:agent:index-maintainer".to_string(),
        purpose_id: "retrieval".to_string(),
        policy: SemanticPolicyComponents {
            rbac_policy_revision: 1,
            rbac_policy_digest: "sha256:rbac".to_string(),
            row_policy_revision: 1,
            row_policy_digest: "sha256:row-policy".to_string(),
            source_acl_revision: 7,
            source_acl_digest: "sha256:sql-acl-state".to_string(),
        },
        source_selector: SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
            catalog_id: SEMANTIC_SQL_CATALOG_ID.to_string(),
            schema_id: "public".to_string(),
            table_id: "articles".to_string(),
            column_id: "body".to_string(),
        }),
        source_schema_digest: "sha256:schema".to_string(),
        source_revision: source_revision.to_string(),
        source_field_set_digest: "sha256:field-set".to_string(),
        dimension: 3,
        metric: SemanticVectorMetric::Cosine,
        model: SemanticModelIdentity {
            model_id: "embedding-model".to_string(),
            model_revision: "revision-1".to_string(),
            preprocess_digest: "sha256:preprocess".to_string(),
            model_digest: "sha256:model".to_string(),
        },
        generation,
        maintenance_policy_id: "semantic-maintenance".to_string(),
        lexical_index: SemanticLexicalIndexSpec {
            analyzer_id: "standard".to_string(),
            analyzer_revision: "1".to_string(),
            analyzer_config_digest: "sha256:analyzer".to_string(),
        },
        ann_index: SemanticAnnIndexSpec {
            method: SemanticAnnIndexMethod::IvfPq,
            parameters_digest: "sha256:ann-parameters".to_string(),
        },
        created_at: "2026-09-08T00:00:00Z".to_string(),
    })
    .unwrap()
}

/// Rebuild the same binding contract with the model identity carried by a
/// durable ANN image.  The normal fixtures intentionally use a small fixed
/// model; the real S6 publication path must also prove that the image's
/// physical model identity is the one named by the generation being promoted.
fn binding_for_image(
    source_revision: &str,
    generation: u64,
    dimension: u32,
    model_digest: &str,
    preprocess_digest: &str,
) -> SemanticBinding {
    let template = binding_for_generation(source_revision, generation);
    SemanticBinding::create(SemanticBindingDraft {
        binding_id: template.binding_id,
        tenant_id: template.tenant_id,
        actor_scope: template.actor_scope,
        effective_actor_scope: template.effective_actor_scope,
        purpose_id: template.purpose_id,
        policy: template.policy_identity.components,
        source_selector: template.source_selector,
        source_schema_digest: template.source_schema_digest,
        source_revision: template.source_revision,
        source_field_set_digest: template.source_field_set_digest,
        dimension,
        metric: template.metric,
        model: SemanticModelIdentity {
            model_id: template.model_id,
            model_revision: template.model_revision,
            preprocess_digest: preprocess_digest.to_string(),
            model_digest: model_digest.to_string(),
        },
        generation,
        maintenance_policy_id: template.maintenance_policy_id,
        lexical_index: template.lexical_index_identity.spec(),
        ann_index: template.ann_index_identity.spec(),
        created_at: template.created_at,
    })
    .unwrap()
}

fn seed_binding_head(codes: &SemanticCodeStore, binding: &SemanticBinding) {
    let owner = codes.bind_for_write(binding.generation).unwrap();
    let read = codes.kernel.read_scope(&owner).unwrap();
    let version = eg_transaction::version(&read).unwrap();
    drop(read);
    let batch = codes.generation_batch(&owner, binding.generation, "reconciliation-seed", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let binding_bytes = binding.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_BINDINGS)
        .unwrap()
        .insert(
            (TENANT, BINDING, binding.generation),
            binding_bytes.as_slice(),
        )
        .unwrap();
    rows.open_table(SEMANTIC_HEADS)
        .unwrap()
        .insert((TENANT, BINDING), binding.generation)
        .unwrap();
    rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
}

fn reconciliation_source_entity(seed: u8) -> String {
    format!(
        "semantic-sql-source:{}",
        SemanticDigest::from_bytes([seed; 32])
    )
}

fn reconciliation_progress(
    binding: &SemanticBinding,
    source_entity_id: String,
    source_revision: &str,
) -> SemanticSourceProgress {
    SemanticSourceProgress {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_entity_id,
        source_revision: source_revision.to_string(),
        completed_stage: None,
        completed_receipt_digest: None,
        superseded_by_revision: None,
        updated_at: "unix-ms:1".to_string(),
    }
}

fn sql_source_identity(binding: &SemanticBinding, seed: u8) -> SemanticSqlSourceIdentity {
    let selector = match &binding.source_selector {
        SemanticSourceSelector::SqlColumnRef(selector) => selector.clone(),
        _ => panic!("SQL source fixture requires a SQL selector"),
    };
    SemanticSqlSourceIdentity::create(&selector, TENANT, digest(seed))
}

fn completed_sql_source_artifact(
    binding: &SemanticBinding,
    transition: &SemanticStageTransition,
    source_identity: &SemanticSqlSourceIdentity,
    authorized_at: &str,
    authorization_seed: u8,
) -> SemanticStageArtifact {
    let source_revision = transition.intent.source_revision.clone();
    let source_content_digest = transition.receipt.output_digest;
    let source_entity_id = source_identity.source_entity_id();
    let authorization = SemanticAuthorizationReceipt::create(SemanticAuthorizationReceiptDraft {
        tenant_id: binding.tenant_id.clone(),
        actor_scope: binding.actor_scope.clone(),
        effective_actor_scope: binding.effective_actor_scope.clone(),
        purpose_id: binding.purpose_id.clone(),
        policy_identity: binding.policy_identity.clone(),
        policy_decision_digest: format!("sha256:semantic-test-auth-{authorization_seed}"),
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: source_revision.clone(),
        authorized_at: authorized_at.to_string(),
    })
    .unwrap();
    let manifest = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_identity: source_identity.clone(),
        source_revision,
        source_content_digest,
        source_schema_revision: 1,
        source_schema_digest: binding.source_schema_digest.clone(),
        source_field_set_digest: binding.source_field_set_digest.clone(),
        source_acl_revision: binding.policy_identity.components.source_acl_revision,
        source_acl_digest: binding.policy_identity.components.source_acl_digest.clone(),
        authorization_receipt_digest: authorization.authorization_receipt_digest,
        completed_receipt_digest: transition.receipt.receipt_digest(),
        completed_at: transition.receipt.completed_at.clone(),
    })
    .unwrap();
    let artifact = SemanticStageArtifact::SqlSourceManifest {
        manifest: Box::new(manifest),
        authorization: Box::new(authorization),
    };
    artifact.validate_against(transition).unwrap();
    artifact
}

/// Byte fingerprint of every file under `dir`, so "this call wrote nothing" is
/// an assertion about the durable state rather than about the absence of an
/// error.
fn store_fingerprint(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_file() {
            out.push((
                path.file_name().unwrap().to_string_lossy().to_string(),
                std::fs::read(&path).unwrap(),
            ));
        }
    }
    out.sort_by(|left, right| left.0.cmp(&right.0));
    out
}

/// A warmed semantic store with `extra` members beyond the build threshold, and
/// one query vector drawn from it.
fn warmed_store(extra: usize) -> (SemanticStore, Vec<f32>) {
    let dim = 16;
    let n = crate::compute::semantic_ann::ANN_BUILD_THRESHOLD + 50 + extra;
    let mut store = SemanticStore::new();
    let mut seed = 0x5eed_u64;
    let mut rng = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    let centers: Vec<Vec<f32>> = (0..24)
        .map(|_| (0..dim).map(|_| rng() * 2.0).collect())
        .collect();
    let mut query = Vec::new();
    for i in 0..n {
        let c = &centers[i % centers.len()];
        let v: Vec<f32> = (0..dim).map(|j| c[j] + rng() * 0.2).collect();
        if i == 100 {
            query = v.clone();
        }
        store.add_embedding(format!("n{i}"), v).unwrap();
    }
    store.warm("test");
    assert!(store.is_ready());
    (store, query)
}

/// The S6 integration proof uses a pinned image so `finalize_generation` has
/// to compare the image manifest's model identity with the durable binding.
fn warmed_store_in_space(space: EmbeddingSpaceRef, extra: usize) -> SemanticStore {
    let dim = space.dimensions;
    let n = crate::compute::semantic_ann::ANN_BUILD_THRESHOLD + 50 + extra;
    let mut store = SemanticStore::new_in_space(space).unwrap();
    let mut seed = 0x5eed_u64;
    let mut rng = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    let centers: Vec<Vec<f32>> = (0..24)
        .map(|_| (0..dim).map(|_| rng() * 2.0).collect())
        .collect();
    for i in 0..n {
        let center = &centers[i % centers.len()];
        let vector: Vec<f32> = (0..dim).map(|j| center[j] + rng() * 0.2).collect();
        store.add_embedding(format!("n{i}"), vector).unwrap();
    }
    store.warm("test");
    assert!(store.is_ready());
    store
}

#[test]
fn direct_activation_is_refused_without_an_admitted_six_transition() {
    let dir = tmp_dir("direct-activation");
    let codes = open_store(&dir);
    let (store, _) = warmed_store(0);
    let image = store.export_generation().unwrap();
    let before = store_fingerprint(&dir);

    let error = codes
        .activate(1, &image)
        .expect_err("direct ANN publication must not bypass S6 admission");
    assert!(
        error.to_string().contains("admitted S6"),
        "unexpected direct publication refusal: {error}"
    );
    assert!(codes.read_live().unwrap().is_none());
    assert!(codes.read_generation(1).unwrap().is_none());
    assert_eq!(
        store_fingerprint(&dir),
        before,
        "a refused direct publication must leave every owner file unchanged"
    );
    let retire_error = codes
        .retire(1)
        .expect_err("direct ANN retirement must not bypass binding lifecycle admission");
    assert!(retire_error.to_string().contains("admitted binding"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dead_letter_registry_key_retains_intent_identity_at_one_attempt() {
    // Kernel-seeded: RF-RULING-007 forbids this region from re-acquiring a
    // store, and the table under test is already the declared
    // `SEMANTIC_DEAD_LETTERS`, so only the acquisition changes.
    let dir = tmp_dir("dead-letter-key");
    let codes = open_store(&dir);
    let owner = codes.bind_for_write(1).unwrap();
    let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "dead-letter-key", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    rows.open_table(SEMANTIC_DEAD_LETTERS)
        .unwrap()
        .insert(
            (TENANT, BINDING, "sha256:intent-a", 3),
            b"dead-letter-a".as_slice(),
        )
        .unwrap();
    rows.open_table(SEMANTIC_DEAD_LETTERS)
        .unwrap()
        .insert(
            (TENANT, BINDING, "sha256:intent-b", 3),
            b"dead-letter-b".as_slice(),
        )
        .unwrap();
    // Two distinct intent digests at the SAME attempt number must remain two
    // rows -- the attempt count must never collapse them onto one key.
    assert_eq!(
        rows.open_table(SEMANTIC_DEAD_LETTERS)
            .unwrap()
            .get((TENANT, BINDING, "sha256:intent-a", 3))
            .unwrap()
            .unwrap()
            .value(),
        b"dead-letter-a"
    );
    assert_eq!(
        rows.open_table(SEMANTIC_DEAD_LETTERS)
            .unwrap()
            .get((TENANT, BINDING, "sha256:intent-b", 3))
            .unwrap()
            .unwrap()
            .value(),
        b"dead-letter-b"
    );
    rows.finish_owner().unwrap();
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persisted_dead_letter_transitions_keep_distinct_intents_at_one_attempt() {
    let dir = tmp_dir("dead-letter-production-key");
    let codes = open_store(&dir);
    let owner = codes.bind_for_write(1).unwrap();
    let make = |entity: &str, seed: u8| {
        let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
            binding_id: BINDING.to_string(),
            binding_digest: digest(31),
            generation: 1,
            scope: SemanticStageScope::Entity {
                source_entity_id: entity.to_string(),
            },
            source_revision: "r1".to_string(),
            stage: SemanticStage::SourceCommit,
            predecessor: SemanticStagePredecessor::None,
            input_digest: digest(seed),
        })
        .unwrap();
        let receipt = SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: digest(seed.wrapping_add(1)),
            cursor: format!("cursor:{seed}"),
            completed_at: format!("2026-09-08T00:04:{seed:02}Z"),
            outcome: SemanticStageOutcome::RejectedDeadLetter,
        };
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt,
            generation_checkpoint: None,
        };
        transition.validate().unwrap();
        let dead_letter = SemanticDeadLetter::create(SemanticDeadLetterDraft {
            intent,
            attempt: 3,
            error_code: "source_rejected".to_string(),
            reason: "test rejection".to_string(),
            failed_at: format!("unix-ms:{}", u64::from(seed)),
        })
        .unwrap();
        (
            transition.clone(),
            SemanticStageArtifact::DeadLetter {
                dead_letter: Box::new(dead_letter),
            },
        )
    };
    let first = make("entity:one", 41);
    let second = make("entity:two", 51);
    for (ordinal, (transition, artifact)) in [first, second].into_iter().enumerate() {
        let read = codes.kernel.read_scope(&owner).unwrap();
        let version = eg_transaction::version(&read).unwrap();
        drop(read);
        let batch = codes.generation_batch(&owner, 1, &format!("dead-letter-{ordinal}"), version);
        let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
        let eg_transaction::Begin::Apply {
            source_version: source_version_write,
        } = begin
        else {
            panic!("expected a fresh Begin::Apply, got a replay");
        };
        let rows = write.owner_rows(&owner, &batch).unwrap();
        persist_stage_artifact(&rows, TENANT, BINDING, &transition, &artifact).unwrap();
        rows.finish_owner().unwrap();
        // A write only reaches the kernel's `Finished` admission state
        // through `finish`; `finish_owner` above closes only the owner-row
        // half, so `commit` alone is refused.
        codes
            .mutations
            .finish(&write, &batch, None, 0, source_version_write)
            .unwrap();
        codes.mutations.commit(write, &batch).unwrap();
    }
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let table = read.open_owner_table(SEMANTIC_DEAD_LETTERS).unwrap();
    let keys: Vec<_> = table
        .range((TENANT, BINDING, "", 0)..)
        .unwrap()
        .map(|row| {
            let (key, _) = row.unwrap();
            let (tenant, binding, intent, attempt) = key.value();
            (
                tenant.to_string(),
                binding.to_string(),
                intent.to_string(),
                attempt,
            )
        })
        .filter(|key| key.0 == TENANT && key.1 == BINDING)
        .collect();
    assert_eq!(keys.len(), 2, "both producer transitions must survive");
    assert_ne!(keys[0].2, keys[1].2);
    drop(table);
    drop(read);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn complete_stage_dead_letter_producer_keeps_distinct_intents_at_one_attempt() {
    let dir = tmp_dir("dead-letter-complete-stage");
    let codes = open_store(&dir);
    let binding = pending_binding(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1",
    );
    codes.store_binding(&binding, 1).unwrap();
    codes
        .transition_binding_operation(
            1,
            SemanticBindingState::Building,
            2,
            "actor-dlq",
            "dlq-build",
            Nonce::from_bytes([61; 32]),
        )
        .unwrap();
    codes.subscribe_stage_consumer("semantic-dlq-test").unwrap();

    let admit_and_reject = |entity: &str, seed: u8, now_ms: u64| {
        let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            scope: SemanticStageScope::Entity {
                source_entity_id: entity.to_string(),
            },
            source_revision: binding.source_revision.clone(),
            stage: SemanticStage::SourceCommit,
            predecessor: SemanticStagePredecessor::None,
            input_digest: digest(seed),
        })
        .unwrap();
        codes.enqueue_stage_intent(&intent, now_ms).unwrap();
        let mut budget = eg_transaction::OutboxClaimBudget::new(4, 5_000, now_ms + 1).unwrap();
        let outcome = codes
            .claim_stage_leases("semantic-dlq-test", &mut budget)
            .unwrap();
        assert_eq!(outcome.claims.len(), 1);
        let lease = outcome.claims.into_iter().next().unwrap();
        let completed_at = format!("2026-09-08T00:05:{seed:02}Z");
        let dead_letter = SemanticDeadLetter::create(SemanticDeadLetterDraft {
            intent: intent.clone(),
            attempt: 1,
            error_code: "source_rejected".to_string(),
            reason: "test rejection".to_string(),
            failed_at: completed_at.clone(),
        })
        .unwrap();
        let receipt = SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: dead_letter.failure_digest,
            cursor: format!("cursor:{seed}"),
            completed_at,
            outcome: SemanticStageOutcome::RejectedDeadLetter,
        };
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt,
            generation_checkpoint: None,
        };
        codes
            .complete_stage(
                &lease,
                &transition,
                &SemanticStageArtifact::DeadLetter {
                    dead_letter: Box::new(dead_letter),
                },
                None,
                now_ms + 2,
            )
            .unwrap();
    };

    admit_and_reject("entity:one", 61, 10);
    admit_and_reject("entity:two", 71, 20);

    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let table = read.open_owner_table(SEMANTIC_DEAD_LETTERS).unwrap();
    let keys: Vec<_> = table
        .range((TENANT, BINDING, "", 0)..)
        .unwrap()
        .map(|row| {
            let (key, _) = row.unwrap();
            let (tenant, binding, intent, attempt) = key.value();
            (
                tenant.to_string(),
                binding.to_string(),
                intent.to_string(),
                attempt,
            )
        })
        .filter(|key| key.0 == TENANT && key.1 == BINDING)
        .collect();
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0].2, keys[1].2);
    drop(table);
    drop(read);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn stage_intent_transport_key_uses_the_canonical_digest() {
    let draft = |source_entity_id: &str, source_revision: &str| SemanticStageIntentDraft {
        binding_id: "binding".to_string(),
        binding_digest: digest(21),
        generation: 1,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.to_string(),
        },
        source_revision: source_revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: digest(22),
    };
    // These components produce the same old delimiter-concatenated key:
    // `binding|1|entity|part|revision|source_commit`.
    let left = SemanticStageIntent::create(draft("entity|part", "revision")).unwrap();
    let right = SemanticStageIntent::create(draft("entity", "part|revision")).unwrap();
    let left_outbox = stage_intent_outbox(&left).unwrap();
    let right_outbox = stage_intent_outbox(&right).unwrap();
    assert_ne!(left.intent_digest, right.intent_digest);
    assert_ne!(left_outbox.key, right_outbox.key);
    assert_eq!(
        left_outbox.key,
        format!("semantic-stage-intent:{}", left.intent_digest)
    );
}

#[test]
fn colon_components_cannot_transplant_a_semantic_owner_file() {
    let source_dir = tmp_dir("colon-source");
    let source_tenant = "tenant:a";
    let source_binding = "binding";
    let source = open_store_for_any_tenant(&source_dir, source_tenant, source_binding);
    drop(source);

    let destination_dir = tmp_dir("colon-destination");
    std::fs::create_dir_all(&destination_dir).unwrap();
    let source_path = source_dir.join(store_file_name(source_tenant, source_binding));
    let destination_path = destination_dir.join(store_file_name("tenant", "a:binding"));
    assert_ne!(source_path, destination_path);
    std::fs::copy(&source_path, &destination_path).unwrap();

    let refused = SemanticCodeStore::open(
        &destination_dir,
        Arc::new(AnyTenantSemanticScopeVerifier),
        TEST_PRINCIPAL,
        TEST_PROOF,
        "tenant",
        "a:binding",
    );
    assert!(
        refused.is_err(),
        "a file copied across delimiter-colliding logical owners must fail the physical fence"
    );

    // The same framed identity remains stable across a normal reopen.
    let reopened = open_store_for_any_tenant(&source_dir, source_tenant, source_binding);
    drop(reopened);
    let _ = std::fs::remove_dir_all(&source_dir);
    let _ = std::fs::remove_dir_all(&destination_dir);
}

#[test]
fn operation_nonce_reuse_is_refused_before_a_second_binding_effect() {
    let dir = tmp_dir("nonce-reuse");
    let codes = open_store(&dir);
    let binding = pending_binding(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1",
    );
    let nonce = Nonce::from_bytes([7; 32]);
    codes
        .store_binding_operation(&binding, 1, "actor-a", "binding-create", nonce)
        .unwrap();

    let replay = codes
        .store_binding_operation(
            &binding,
            2,
            "actor-a",
            "binding-create",
            Nonce::from_bytes([8; 32]),
        )
        .expect("a fresh nonce with the same stable operation key must replay");
    assert!(replay.replayed);
    let replay_nonce_reuse = codes
        .store_binding_operation(
            &binding,
            3,
            "actor-a",
            "binding-create",
            Nonce::from_bytes([8; 32]),
        )
        .expect_err("the replay nonce must be consumed durably");
    assert!(replay_nonce_reuse
        .to_string()
        .to_ascii_lowercase()
        .contains("nonce"));

    let changed = pending_binding(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:2",
    );
    let changed_content = codes
        .store_binding_operation(
            &changed,
            4,
            "actor-a",
            "binding-create",
            Nonce::from_bytes([9; 32]),
        )
        .expect_err("the same stable key with changed content must conflict");
    assert!(changed_content
        .to_string()
        .to_ascii_lowercase()
        .contains("different content"));

    let reused = codes
        .store_binding_operation(&binding, 2, "actor-a", "binding-create", nonce)
        .expect_err("a consumed attempt nonce must not be accepted twice");
    let text = reused.to_string().to_ascii_lowercase();
    assert!(
        text.contains("nonce") || text.contains("replay"),
        "nonce reuse must fail through the replay fence: {reused}"
    );
    assert_eq!(codes.read_binding().unwrap().unwrap(), binding);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn building_transition_is_durable_before_six_can_publish_live() {
    let dir = tmp_dir("durable-building");
    let codes = open_store(&dir);
    let binding = pending_binding(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1",
    );
    codes
        .store_binding_operation(
            &binding,
            1,
            "actor-a",
            "binding-create",
            Nonce::from_bytes([8; 32]),
        )
        .unwrap();
    codes
        .transition_binding_operation(
            1,
            SemanticBindingState::Building,
            2,
            "actor-a",
            "binding-build",
            Nonce::from_bytes([9; 32]),
        )
        .unwrap();

    let replay = codes
        .transition_binding_operation(
            1,
            SemanticBindingState::Building,
            3,
            "actor-a",
            "binding-build",
            Nonce::from_bytes([10; 32]),
        )
        .expect("a fresh nonce must replay after Pending -> Building");
    assert!(replay.replayed);
    let changed = codes
        .transition_binding_operation(
            1,
            SemanticBindingState::Disabled,
            4,
            "actor-a",
            "binding-build",
            Nonce::from_bytes([11; 32]),
        )
        .expect_err("changed lifecycle content must not replay under the same key");
    assert!(changed
        .to_string()
        .to_ascii_lowercase()
        .contains("different content"));

    let binding_after = codes.read_binding().unwrap().unwrap();
    assert_eq!(binding_after.durable_state, SemanticBindingState::Building);
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let raw = read
        .open_owner_table(SEMANTIC_STATES)
        .unwrap()
        .get((TENANT, BINDING))
        .unwrap()
        .map(|value| value.value().to_vec())
        .expect("the Pending-to-Building receipt must be durable");
    let transition = SemanticBindingStateTransition::from_canonical_cbor(&raw).unwrap();
    assert_eq!(transition.expected, SemanticBindingState::Pending);
    assert_eq!(transition.next, SemanticBindingState::Building);
    drop(read);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn pending_binding_cannot_claim_stage_until_building_transition_is_durable() {
    let dir = tmp_dir("pending-stage-claim");
    let codes = open_store(&dir);
    let binding = pending_binding(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1",
    );
    codes.store_binding(&binding, 1).unwrap();
    codes
        .subscribe_stage_consumer("semantic-building-gate")
        .unwrap();
    let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: "entity:pending".to_string(),
        },
        source_revision: binding.source_revision.clone(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: digest(91),
    })
    .unwrap();
    codes.enqueue_stage_intent(&intent, 10).unwrap();
    let mut budget = eg_transaction::OutboxClaimBudget::new(1, 5_000, 11).unwrap();
    let refused = codes
        .claim_stage_leases("semantic-building-gate", &mut budget)
        .expect_err("pending work must not be claimable");
    assert!(refused.to_string().contains("Building"));

    codes
        .transition_binding_operation(
            1,
            SemanticBindingState::Building,
            12,
            "actor-building",
            "building-transition",
            Nonce::from_bytes([92; 32]),
        )
        .unwrap();
    let mut budget = eg_transaction::OutboxClaimBudget::new(1, 5_000, 13).unwrap();
    let outcome = codes
        .claim_stage_leases("semantic-building-gate", &mut budget)
        .expect("the durable Building transition opens the executor gate");
    assert_eq!(outcome.claims.len(), 1);
    codes.release_stage_lease(&outcome.claims[0]).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sql_refresh_revision_binds_authority_and_epoch() {
    let authority_a =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:";
    let authority_b =
        "sql-source:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:epoch:";
    let first = format!("{authority_a}7");
    let newer = format!("{authority_a}8");
    let foreign = format!("{authority_b}99");
    assert!(validate_sql_source_revision(&first).is_ok());
    assert!(validate_sql_source_revision(&newer).is_ok());
    assert!(validate_sql_source_revision(&foreign).is_ok());
    assert_eq!(
        compare_source_revision(&newer, &first),
        std::cmp::Ordering::Greater
    );
    let legacy_revision = format!("{}native:7:8:authority-a:batch-b:ordinal-0", "sql-");
    assert!(validate_sql_source_revision(&legacy_revision).is_err());
    assert!(validate_sql_source_revision(
        "sql-source:sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:epoch:8"
    )
    .is_err());
    assert!(validate_sql_source_revision(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:0"
    )
    .is_err());
    assert_ne!(first, foreign);
}

#[test]
fn reconciliation_checkpoint_is_durable_cas_and_restart_safe() {
    let dir = tmp_dir("reconciliation-checkpoint");
    let codes = open_store(&dir);
    let revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let binding = binding_for_generation(revision, 1);
    seed_binding_head(&codes, &binding);
    let scanning = super::SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: SemanticDigest::from_bytes([7; 32]),
        source_revision: revision.to_string(),
        phase: super::SemanticSourceReconciliationPhase::Scanning,
        source_cursor: Some(vec![1, 2, 3]),
        prior_cursor: None,
        rows_seen: 4,
        source_bytes_seen: 128,
        pages_seen: 2,
        complete_snapshot_receipt_digest: None,
    };
    assert!(codes
        .read_source_reconciliation_checkpoint(1)
        .unwrap()
        .is_none());
    codes
        .write_source_reconciliation_checkpoint(1, None, &scanning)
        .unwrap();
    assert_eq!(
        codes
            .read_source_reconciliation_checkpoint(1)
            .unwrap()
            .as_ref(),
        Some(&scanning)
    );

    let finalizing = super::SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: SemanticDigest::from_bytes([7; 32]),
        source_revision: revision.to_string(),
        phase: super::SemanticSourceReconciliationPhase::FinalizingTombstones,
        source_cursor: None,
        prior_cursor: Some(reconciliation_source_entity(1)),
        rows_seen: 5,
        source_bytes_seen: 256,
        pages_seen: 3,
        complete_snapshot_receipt_digest: Some(SemanticDigest::from_bytes([9; 32])),
    };
    assert!(codes
        .write_source_reconciliation_checkpoint(1, None, &finalizing)
        .is_err());
    codes
        .write_source_reconciliation_checkpoint(1, Some(&scanning), &finalizing)
        .unwrap();
    assert!(codes
        .write_source_reconciliation_checkpoint(1, Some(&scanning), &finalizing)
        .is_ok());
    assert!(codes
        .clear_source_reconciliation_checkpoint(1, &scanning)
        .is_err());
    drop(codes);

    let reopened = open_store(&dir);
    assert_eq!(
        reopened
            .read_source_reconciliation_checkpoint(1)
            .unwrap()
            .as_ref(),
        Some(&finalizing)
    );
    reopened
        .clear_source_reconciliation_checkpoint(1, &finalizing)
        .unwrap();
    assert!(reopened
        .read_source_reconciliation_checkpoint(1)
        .unwrap()
        .is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn reconciliation_entity_pages_are_bounded_and_revision_fenced() {
    let dir = tmp_dir("reconciliation-entities");
    let codes = open_store(&dir);
    let revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let binding = binding_for_generation(revision, 1);
    seed_binding_head(&codes, &binding);
    let first = reconciliation_source_entity(1);
    let second = reconciliation_source_entity(2);
    let owner = codes.bind_for_write(1).unwrap();
    let read = codes.kernel.read_scope(&owner).unwrap();
    let version = eg_transaction::version(&read).unwrap();
    drop(read);
    let batch = codes.generation_batch(&owner, 1, "reconciliation-progress-seed", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    for entity in [first.clone(), second.clone()] {
        let progress = reconciliation_progress(&binding, entity.clone(), revision);
        let bytes = progress.to_canonical_cbor().unwrap();
        rows.open_table(SEMANTIC_SOURCE_PROGRESS)
            .unwrap()
            .insert((TENANT, BINDING, 1, entity.as_str()), bytes.as_slice())
            .unwrap();
    }
    rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();

    let (page, next) = codes.list_source_entities_page(1, None, 1).unwrap();
    assert_eq!(page, vec![first.clone()]);
    assert_eq!(next.as_deref(), Some(first.as_str()));
    let (page, next) = codes
        .list_source_entities_page(1, next.as_deref(), 1)
        .unwrap();
    assert_eq!(page, vec![second.clone()]);
    assert!(next.is_none());
    assert!(codes.source_entity_exists(1, &first).unwrap());
    assert!(codes
        .source_entity_seen_at_revision(1, &first, revision)
        .unwrap());
    assert!(!codes
        .source_entity_seen_at_revision(
            1,
            &first,
            "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:2",
        )
        .unwrap());
    assert!(codes
        .list_source_entities_page(1, Some("entity:untrusted"), 1)
        .is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn complete_reconciliation_tombstone_replaces_completed_prior_revision() {
    let dir = tmp_dir("reconciliation-tombstone-admission");
    let codes = open_store(&dir);
    let old_revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let new_revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:2";
    let binding = binding_for_generation(old_revision, 1);
    seed_binding_head(&codes, &binding);
    let source_entity_id = reconciliation_source_entity(42);
    let old_input = digest(43);
    let old_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: old_revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: old_input,
    })
    .unwrap();
    let old_receipt = SemanticStageReceipt {
        intent_digest: old_intent.intent_digest,
        output_digest: old_input,
        cursor: "cursor:old".to_string(),
        completed_at: "2026-09-08T00:08:00Z".to_string(),
        outcome: SemanticStageOutcome::IdempotentNoop,
    };
    let old_transition = SemanticStageTransition {
        intent: old_intent.clone(),
        receipt: old_receipt.clone(),
        generation_checkpoint: None,
    };
    old_transition.validate().unwrap();
    let old_mutation = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(old_transition),
        artifact: SemanticStageArtifact::None,
    };
    old_mutation.validate().unwrap();
    let old_mutation_bytes = old_mutation.to_canonical_cbor().unwrap();
    let old_progress = SemanticSourceProgress {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_entity_id: source_entity_id.clone(),
        source_revision: old_revision.to_string(),
        completed_stage: Some(SemanticStage::SourceCommit),
        completed_receipt_digest: Some(old_receipt.receipt_digest()),
        superseded_by_revision: None,
        updated_at: "unix-ms:1".to_string(),
    };
    old_progress.validate().unwrap();

    let owner = codes.bind_for_write(1).unwrap();
    let read = codes.kernel.read_scope(&owner).unwrap();
    let version = eg_transaction::version(&read).unwrap();
    drop(read);
    let batch = codes.generation_batch(&owner, 1, "reconciliation-tombstone-seed", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let progress_bytes = old_progress.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .insert(
            (TENANT, BINDING, 1, source_entity_id.as_str()),
            progress_bytes.as_slice(),
        )
        .unwrap();
    let stage_bytes = old_mutation_bytes.as_slice();
    rows.open_table(SEMANTIC_STAGES)
        .unwrap()
        .insert(
            (
                TENANT,
                BINDING,
                super::stage_intent_key(&old_intent).as_str(),
            ),
            stage_bytes,
        )
        .unwrap();
    let receipt_key = super::stage_receipt_index_key(old_receipt.receipt_digest());
    rows.open_table(SEMANTIC_STAGES)
        .unwrap()
        .insert((TENANT, BINDING, receipt_key.as_str()), stage_bytes)
        .unwrap();
    rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();

    let checkpoint = super::SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: digest(44),
        source_revision: new_revision.to_string(),
        phase: super::SemanticSourceReconciliationPhase::FinalizingTombstones,
        source_cursor: None,
        prior_cursor: None,
        rows_seen: 2,
        source_bytes_seen: 128,
        pages_seen: 1,
        complete_snapshot_receipt_digest: Some(digest(45)),
    };
    codes
        .write_source_reconciliation_checkpoint(1, None, &checkpoint)
        .unwrap();
    let tombstone_input = super::reconciliation_tombstone_input_digest(
        &source_entity_id,
        new_revision,
        checkpoint.complete_snapshot_receipt_digest.unwrap(),
    );
    let tombstone_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: new_revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: tombstone_input,
    })
    .unwrap();

    let first = codes
        .enqueue_reconciliation_tombstone(&tombstone_intent, &checkpoint, 9)
        .unwrap();
    assert!(!first.replayed);
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let progress = read
        .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .get((TENANT, BINDING, 1, source_entity_id.as_str()))
        .unwrap()
        .map(|value| SemanticSourceProgress::from_canonical_cbor(value.value()).unwrap())
        .unwrap();
    assert_eq!(progress.source_revision, new_revision);
    assert!(progress.completed_stage.is_none());
    assert!(progress.completed_receipt_digest.is_none());
    let ledger = eg_transaction::read_ledger(&read, &first.batch_id)
        .unwrap()
        .unwrap();
    let event = ledger
        .batch
        .outbox
        .iter()
        .find(|event| event.topic == super::SEMANTIC_STAGE_INTENT_TOPIC)
        .unwrap();
    assert_eq!(
        event
            .headers
            .get("reconciliation_proof")
            .map(String::as_str),
        Some("semantic-source-tombstone/v1")
    );
    drop(read);

    let replay = codes
        .enqueue_reconciliation_tombstone(&tombstone_intent, &checkpoint, 10)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.batch_id, first.batch_id);
    assert_eq!(replay.mutation_digest, first.mutation_digest);
    assert!(codes.enqueue_stage_intent(&tombstone_intent, 11).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn completed_sql_source_replay_reads_retained_transition_and_manifest() {
    let dir = tmp_dir("sql-source-replay-retained");
    let codes = open_store(&dir);
    let revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let binding = pending_binding(revision);
    codes.store_binding(&binding, 1).unwrap();
    codes
        .transition_binding_operation(
            1,
            SemanticBindingState::Building,
            2,
            "actor-sql-replay",
            "sql-replay-build",
            Nonce::from_bytes([121; 32]),
        )
        .unwrap();
    let binding = codes.read_binding().unwrap().unwrap();
    let source_identity = sql_source_identity(&binding, 122);
    let source_entity_id = source_identity.source_entity_id();
    let input_digest = digest(123);
    let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest,
    })
    .unwrap();
    codes
        .subscribe_stage_consumer("semantic-sql-replay")
        .unwrap();
    codes.enqueue_stage_intent(&intent, 3).unwrap();
    let transition = SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: input_digest,
            cursor: "server-owned-cursor:123".to_string(),
            completed_at: "2026-09-08T00:11:23Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    transition.validate().unwrap();
    let artifact = completed_sql_source_artifact(
        &binding,
        &transition,
        &source_identity,
        "2026-09-08T00:11:23Z",
        124,
    );
    let mutation = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(transition.clone()),
        artifact: artifact.clone(),
    };
    mutation.validate().unwrap();
    let mutation_bytes = mutation.to_canonical_cbor().unwrap();
    let mutation_digest = super::semantic_digest(&mutation_bytes);
    let receipt_key = super::stage_receipt_index_key(transition.receipt.receipt_digest());
    let stage_key = intent.intent_digest.to_string();
    let outbox = vec![MutationOutboxIntent {
        topic: super::SEMANTIC_STAGE_RECEIPT_TOPIC.to_string(),
        key: intent.intent_digest.to_string(),
        payload: transition.to_canonical_cbor().unwrap(),
        headers: super::stage_receipt_headers(&transition),
    }];
    let batch_id = format!("semantic-index:stage:{}", intent.intent_digest);
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    &batch_id,
                    "semantic_source_stage_committed",
                    &source_entity_id,
                    mutation_digest,
                    outbox,
                    4,
                )
            },
            mutation_digest,
            4,
            |_, rows| {
                let mut stages = rows.open_table(SEMANTIC_STAGES).unwrap();
                stages
                    .insert(
                        (TENANT, BINDING, stage_key.as_str()),
                        mutation_bytes.as_slice(),
                    )
                    .unwrap();
                stages
                    .insert(
                        (TENANT, BINDING, receipt_key.as_str()),
                        mutation_bytes.as_slice(),
                    )
                    .unwrap();
                drop(stages);
                super::persist_stage_artifact(rows, TENANT, BINDING, &transition, &artifact)
            },
        )
        .unwrap();
    let remove_index_digest = digest(138);
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    "semantic-index:remove-replay-index",
                    "semantic_source_stage_replay_test",
                    &source_entity_id,
                    remove_index_digest,
                    Vec::new(),
                    5,
                )
            },
            remove_index_digest,
            5,
            |_, rows| {
                rows.open_table(SEMANTIC_STAGES)
                    .unwrap()
                    .remove((TENANT, BINDING, receipt_key.as_str()))
                    .unwrap();
                Ok(())
            },
        )
        .unwrap();

    let mut budget = eg_transaction::OutboxClaimBudget::new(1, 5_000, 5).unwrap();
    let lease = codes
        .claim_stage_leases("semantic-sql-replay", &mut budget)
        .unwrap()
        .claims
        .into_iter()
        .next()
        .unwrap();
    let changed_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: digest(125),
    })
    .unwrap();
    assert!(codes
        .replay_completed_sql_source_stage(&lease, &changed_intent, 6)
        .is_err());
    assert!(codes
        .replay_completed_sql_source_stage(&lease, &intent, 6)
        .is_err());
    let restore_index_digest = digest(139);
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    "semantic-index:restore-replay-index",
                    "semantic_source_stage_replay_test",
                    &source_entity_id,
                    restore_index_digest,
                    Vec::new(),
                    6,
                )
            },
            restore_index_digest,
            6,
            |_, rows| {
                rows.open_table(SEMANTIC_STAGES)
                    .unwrap()
                    .insert(
                        (TENANT, BINDING, receipt_key.as_str()),
                        mutation_bytes.as_slice(),
                    )
                    .unwrap();
                Ok(())
            },
        )
        .unwrap();
    let replay = codes
        .replay_completed_sql_source_stage(&lease, &intent, 7)
        .unwrap()
        .unwrap();
    assert_eq!(replay.0, transition);
    assert!(replay.1.replayed);
    assert_eq!(replay.1.mutation_digest, mutation_digest);

    let manifest = codes
        .sql_source_manifest(binding.generation, &source_entity_id)
        .unwrap()
        .unwrap();
    assert_eq!(manifest.source_identity, source_identity);
    assert_eq!(manifest.source_revision, revision);
    assert_eq!(manifest.source_content_digest, input_digest);
    drop(codes);
    let reopened = open_store(&dir);
    let reopened_manifest = reopened
        .sql_source_manifest(binding.generation, &source_entity_id)
        .unwrap()
        .unwrap();
    assert_eq!(reopened_manifest, manifest);
    let retained_artifact = reopened
        .recorded_sql_source_artifact(intent.intent_digest)
        .unwrap()
        .unwrap();
    let SemanticStageArtifact::SqlSourceManifest { authorization, .. } = retained_artifact else {
        panic!("replay fixture must retain the SQL source artifact");
    };
    assert_eq!(authorization.authorized_at, "2026-09-08T00:11:23Z");
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn retained_sql_manifest_rejects_a_foreign_source_authority() {
    let dir = tmp_dir("sql-source-manifest-authority");
    let codes = open_store(&dir);
    let binding_revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let foreign_revision =
        "sql-source:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:epoch:1";
    let binding = pending_binding(binding_revision);
    codes.store_binding(&binding, 1).unwrap();
    let source_identity = sql_source_identity(&binding, 133);
    let manifest = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_identity: source_identity.clone(),
        source_revision: foreign_revision.to_string(),
        source_content_digest: digest(134),
        source_schema_revision: 1,
        source_schema_digest: binding.source_schema_digest.clone(),
        source_field_set_digest: binding.source_field_set_digest.clone(),
        source_acl_revision: binding.policy_identity.components.source_acl_revision,
        source_acl_digest: binding.policy_identity.components.source_acl_digest.clone(),
        authorization_receipt_digest: digest(135),
        completed_receipt_digest: digest(136),
        completed_at: "2026-09-08T00:14:00Z".to_string(),
    })
    .unwrap();
    let manifest_bytes = manifest.to_canonical_cbor().unwrap();
    let write_digest = digest(137);
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    "semantic-index:foreign-source-manifest",
                    "semantic_source_manifest_seed",
                    &source_identity.source_entity_id(),
                    write_digest,
                    Vec::new(),
                    4,
                )
            },
            write_digest,
            4,
            |_, rows| {
                rows.open_table(SEMANTIC_SQL_SOURCES)
                    .unwrap()
                    .insert(
                        (
                            TENANT,
                            BINDING,
                            binding.generation,
                            source_identity.source_entity_id().as_str(),
                        ),
                        manifest_bytes.as_slice(),
                    )
                    .unwrap();
                Ok(())
            },
        )
        .unwrap();
    assert!(codes
        .sql_source_manifest(binding.generation, &source_identity.source_entity_id())
        .is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn completed_sql_tombstone_replaces_retained_manifest_through_real_completion() {
    let dir = tmp_dir("sql-source-tombstone-complete");
    let codes = open_store(&dir);
    let old_revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let new_revision =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:2";
    let binding = pending_binding(old_revision);
    codes.store_binding(&binding, 1).unwrap();
    codes
        .transition_binding_operation(
            1,
            SemanticBindingState::Building,
            2,
            "actor-tombstone",
            "tombstone-build",
            Nonce::from_bytes([126; 32]),
        )
        .unwrap();
    let binding = codes.read_binding().unwrap().unwrap();
    let source_identity = sql_source_identity(&binding, 127);
    let source_entity_id = source_identity.source_entity_id();
    let old_input = digest(128);
    let old_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: old_revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: old_input,
    })
    .unwrap();
    let old_transition = SemanticStageTransition {
        intent: old_intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: old_intent.intent_digest,
            output_digest: old_input,
            cursor: "source-cursor:old".to_string(),
            completed_at: "2026-09-08T00:12:00Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let old_artifact = completed_sql_source_artifact(
        &binding,
        &old_transition,
        &source_identity,
        "2026-09-08T00:12:00Z",
        129,
    );
    let old_mutation = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(old_transition.clone()),
        artifact: old_artifact.clone(),
    };
    old_mutation.validate().unwrap();
    let old_mutation_bytes = old_mutation.to_canonical_cbor().unwrap();
    let old_mutation_digest = super::semantic_digest(&old_mutation_bytes);
    let old_stage_key = old_intent.intent_digest.to_string();
    let old_receipt_key = super::stage_receipt_index_key(old_transition.receipt.receipt_digest());
    let old_progress = SemanticSourceProgress {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_entity_id: source_entity_id.clone(),
        source_revision: old_revision.to_string(),
        completed_stage: Some(SemanticStage::SourceCommit),
        completed_receipt_digest: Some(old_transition.receipt.receipt_digest()),
        superseded_by_revision: None,
        updated_at: "unix-ms:4".to_string(),
    };
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    "semantic-index:seed-completed-sql",
                    "semantic_source_stage_seed",
                    &source_entity_id,
                    old_mutation_digest,
                    Vec::new(),
                    4,
                )
            },
            old_mutation_digest,
            4,
            |_, rows| {
                let progress_bytes = old_progress.to_canonical_cbor().unwrap();
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .unwrap()
                    .insert(
                        (
                            TENANT,
                            BINDING,
                            binding.generation,
                            source_entity_id.as_str(),
                        ),
                        progress_bytes.as_slice(),
                    )
                    .unwrap();
                rows.open_table(SEMANTIC_STAGES)
                    .unwrap()
                    .insert(
                        (TENANT, BINDING, old_stage_key.as_str()),
                        old_mutation_bytes.as_slice(),
                    )
                    .unwrap();
                rows.open_table(SEMANTIC_STAGES)
                    .unwrap()
                    .insert(
                        (TENANT, BINDING, old_receipt_key.as_str()),
                        old_mutation_bytes.as_slice(),
                    )
                    .unwrap();
                super::persist_stage_artifact(rows, TENANT, BINDING, &old_transition, &old_artifact)
            },
        )
        .unwrap();

    let checkpoint = super::SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: digest(130),
        source_revision: new_revision.to_string(),
        phase: super::SemanticSourceReconciliationPhase::FinalizingTombstones,
        source_cursor: None,
        prior_cursor: None,
        rows_seen: 1,
        source_bytes_seen: 64,
        pages_seen: 1,
        complete_snapshot_receipt_digest: Some(digest(131)),
    };
    codes
        .write_source_reconciliation_checkpoint(binding.generation, None, &checkpoint)
        .unwrap();
    let tombstone_input = super::reconciliation_tombstone_input_digest(
        &source_entity_id,
        new_revision,
        checkpoint.complete_snapshot_receipt_digest.unwrap(),
    );
    let tombstone_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: new_revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: tombstone_input,
    })
    .unwrap();
    codes
        .subscribe_stage_consumer("semantic-tombstone-complete")
        .unwrap();
    let admission = codes
        .enqueue_reconciliation_tombstone(&tombstone_intent, &checkpoint, 5)
        .unwrap();
    assert!(!admission.replayed);
    let mut budget = eg_transaction::OutboxClaimBudget::new(1, 5_000, 6).unwrap();
    let lease = codes
        .claim_stage_leases("semantic-tombstone-complete", &mut budget)
        .unwrap()
        .claims
        .into_iter()
        .next()
        .unwrap();
    let tombstone_transition = SemanticStageTransition {
        intent: tombstone_intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: tombstone_intent.intent_digest,
            output_digest: tombstone_input,
            cursor: "source-cursor:complete-tombstone".to_string(),
            completed_at: "2026-09-08T00:13:00Z".to_string(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    };
    let tombstone_artifact = completed_sql_source_artifact(
        &binding,
        &tombstone_transition,
        &source_identity,
        "2026-09-08T00:13:00Z",
        132,
    );
    let receipt = codes
        .complete_stage(&lease, &tombstone_transition, &tombstone_artifact, None, 7)
        .unwrap();
    assert!(!receipt.replayed);
    let manifest = codes
        .sql_source_manifest(binding.generation, &source_entity_id)
        .unwrap()
        .unwrap();
    assert_eq!(manifest.source_identity, source_identity);
    assert_eq!(manifest.source_revision, new_revision);
    assert_eq!(manifest.source_content_digest, tombstone_input);
    assert_eq!(
        manifest.completed_receipt_digest,
        tombstone_transition.receipt.receipt_digest()
    );
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let progress = read
        .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .get((
            TENANT,
            BINDING,
            binding.generation,
            source_entity_id.as_str(),
        ))
        .unwrap()
        .map(|value| SemanticSourceProgress::from_canonical_cbor(value.value()).unwrap())
        .unwrap();
    assert_eq!(progress.source_revision, new_revision);
    assert_eq!(
        progress.completed_receipt_digest,
        Some(tombstone_transition.receipt.receipt_digest())
    );
    drop(read);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn refresh_with_s1_moves_head_and_replays_after_the_head_changed() {
    let dir = tmp_dir("refresh-with-s1");
    let codes = open_store(&dir);
    let revision_one =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let revision_two =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:2";
    let pending_one = pending_binding(revision_one);
    let to_building = SemanticBindingStateTransition::create(
        &pending_one,
        SemanticBindingState::Building,
        "test",
    )
    .unwrap();
    let building_one = pending_one.apply_state_transition(&to_building).unwrap();
    let to_live =
        SemanticBindingStateTransition::create(&building_one, SemanticBindingState::Live, "test")
            .unwrap();
    let live_one = building_one.apply_state_transition(&to_live).unwrap();
    let source_selector = match &live_one.source_selector {
        SemanticSourceSelector::SqlColumnRef(selector) => selector.clone(),
        _ => panic!("test binding must use a SQL selector"),
    };
    let source_identity = SemanticSqlSourceIdentity::create(
        &source_selector,
        TENANT,
        SemanticDigest::from_bytes([101; 32]),
    );
    let source_entity_id = source_identity.source_entity_id();
    let old_input = SemanticDigest::from_bytes([102; 32]);
    let old_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: live_one.binding_id.clone(),
        binding_digest: live_one.binding_digest,
        generation: live_one.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: revision_one.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: old_input,
    })
    .unwrap();
    let old_receipt = SemanticStageReceipt {
        intent_digest: old_intent.intent_digest,
        output_digest: old_input,
        cursor: "cursor:old".to_string(),
        completed_at: "2026-09-08T00:06:00Z".to_string(),
        outcome: SemanticStageOutcome::IdempotentNoop,
    };
    let old_transition = SemanticStageTransition {
        intent: old_intent.clone(),
        receipt: old_receipt.clone(),
        generation_checkpoint: None,
    };
    old_transition.validate().unwrap();
    let old_mutation = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(old_transition),
        artifact: SemanticStageArtifact::None,
    };
    old_mutation.validate().unwrap();
    let old_mutation_bytes = old_mutation.to_canonical_cbor().unwrap();
    let old_progress = SemanticSourceProgress {
        binding_id: live_one.binding_id.clone(),
        binding_digest: live_one.binding_digest,
        generation: live_one.generation,
        source_entity_id: source_entity_id.clone(),
        source_revision: revision_one.to_string(),
        completed_stage: Some(SemanticStage::SourceCommit),
        completed_receipt_digest: Some(old_receipt.receipt_digest()),
        superseded_by_revision: None,
        updated_at: "unix-ms:1".to_string(),
    };
    old_progress.validate().unwrap();

    let owner = codes.bind_for_write(1).unwrap();
    let read = codes.kernel.read_scope(&owner).unwrap();
    let version = eg_transaction::version(&read).unwrap();
    drop(read);
    let batch = codes.generation_batch(&owner, 1, "refresh-seed", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let live_bytes = live_one.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_BINDINGS)
        .unwrap()
        .insert((TENANT, BINDING, 1), live_bytes.as_slice())
        .unwrap();
    rows.open_table(SEMANTIC_HEADS)
        .unwrap()
        .insert((TENANT, BINDING), 1)
        .unwrap();
    let state_bytes = to_live.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_STATES)
        .unwrap()
        .insert((TENANT, BINDING), state_bytes.as_slice())
        .unwrap();
    let progress_bytes = old_progress.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .insert(
            (TENANT, BINDING, 1, source_entity_id.as_str()),
            progress_bytes.as_slice(),
        )
        .unwrap();
    let stage_key = old_intent.intent_digest.to_string();
    rows.open_table(SEMANTIC_STAGES)
        .unwrap()
        .insert(
            (TENANT, BINDING, stage_key.as_str()),
            old_mutation_bytes.as_slice(),
        )
        .unwrap();
    rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();

    let replacement = binding_for_generation(revision_two, 2);
    let new_input = SemanticDigest::from_bytes([103; 32]);
    let source_manifest = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: replacement.binding_id.clone(),
        binding_digest: replacement.binding_digest,
        generation: replacement.generation,
        source_identity: source_identity.clone(),
        source_revision: revision_two.to_string(),
        source_content_digest: new_input,
        source_schema_revision: 1,
        source_schema_digest: replacement.source_schema_digest.clone(),
        source_field_set_digest: replacement.source_field_set_digest.clone(),
        source_acl_revision: replacement.policy_identity.components.source_acl_revision,
        source_acl_digest: replacement
            .policy_identity
            .components
            .source_acl_digest
            .clone(),
        authorization_receipt_digest: SemanticDigest::from_bytes([104; 32]),
        completed_receipt_digest: SemanticDigest::from_bytes([105; 32]),
        completed_at: "2026-09-08T00:07:00Z".to_string(),
    })
    .unwrap();
    let replacement_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: replacement.binding_id.clone(),
        binding_digest: replacement.binding_digest,
        generation: replacement.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: revision_two.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: new_input,
    })
    .unwrap();
    let missing_index = codes
        .refresh_binding_operation_with_s1(
            1,
            &replacement,
            &source_manifest,
            &replacement_intent,
            8,
            "actor-refresh",
            "refresh-source-missing-index",
            Nonce::from_bytes([106; 32]),
        )
        .expect_err("refresh must refuse when the retained receipt index is absent");
    assert!(missing_index.to_string().contains("indexed stage row"));
    let owner = codes.bind_for_write(1).unwrap();
    let read = codes.kernel.read_scope(&owner).unwrap();
    let version = eg_transaction::version(&read).unwrap();
    drop(read);
    let batch = codes.generation_batch(&owner, 1, "refresh-index-repair", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let receipt_key = super::stage_receipt_index_key(old_receipt.receipt_digest());
    rows.open_table(SEMANTIC_STAGES)
        .unwrap()
        .insert(
            (TENANT, BINDING, receipt_key.as_str()),
            old_mutation_bytes.as_slice(),
        )
        .unwrap();
    rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let first = codes
        .refresh_binding_operation_with_s1(
            1,
            &replacement,
            &source_manifest,
            &replacement_intent,
            8,
            "actor-refresh",
            "refresh-source",
            Nonce::from_bytes([107; 32]),
        )
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(codes.read_binding().unwrap().unwrap().generation, 2);
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let old_progress = read
        .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .get((TENANT, BINDING, 1, source_entity_id.as_str()))
        .unwrap()
        .map(|value| SemanticSourceProgress::from_canonical_cbor(value.value()).unwrap())
        .unwrap();
    assert_eq!(
        old_progress.superseded_by_revision.as_deref(),
        Some(revision_two)
    );
    let new_progress = read
        .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .get((TENANT, BINDING, 2, source_entity_id.as_str()))
        .unwrap()
        .is_some();
    assert!(new_progress);
    let ledger = eg_transaction::read_ledger(&read, &first.batch_id)
        .unwrap()
        .unwrap();
    assert!(ledger
        .batch
        .outbox
        .iter()
        .any(|event| event.topic == super::SEMANTIC_STAGE_INTENT_TOPIC));
    drop(read);

    let replay = codes
        .refresh_binding_operation_with_s1(
            1,
            &replacement,
            &source_manifest,
            &replacement_intent,
            9,
            "actor-refresh",
            "refresh-source",
            Nonce::from_bytes([108; 32]),
        )
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.batch_id, first.batch_id);
    assert_eq!(codes.read_binding().unwrap().unwrap().generation, 2);

    let changed_input = SemanticDigest::from_bytes([108; 32]);
    let changed_manifest = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: source_manifest.binding_id.clone(),
        binding_digest: source_manifest.binding_digest,
        generation: source_manifest.generation,
        source_identity: source_manifest.source_identity.clone(),
        source_revision: source_manifest.source_revision.clone(),
        source_content_digest: changed_input,
        source_schema_revision: source_manifest.source_schema_revision,
        source_schema_digest: source_manifest.source_schema_digest.clone(),
        source_field_set_digest: source_manifest.source_field_set_digest.clone(),
        source_acl_revision: source_manifest.source_acl_revision,
        source_acl_digest: source_manifest.source_acl_digest.clone(),
        authorization_receipt_digest: source_manifest.authorization_receipt_digest,
        completed_receipt_digest: source_manifest.completed_receipt_digest,
        completed_at: source_manifest.completed_at.clone(),
    })
    .unwrap();
    let changed_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: replacement_intent.binding_id.clone(),
        binding_digest: replacement_intent.binding_digest,
        generation: replacement_intent.generation,
        scope: replacement_intent.scope.clone(),
        source_revision: replacement_intent.source_revision.clone(),
        stage: replacement_intent.stage,
        predecessor: replacement_intent.predecessor.clone(),
        input_digest: changed_input,
    })
    .unwrap();
    let changed = codes
        .refresh_binding_operation_with_s1(
            1,
            &replacement,
            &changed_manifest,
            &changed_intent,
            10,
            "actor-refresh",
            "refresh-source",
            Nonce::from_bytes([110; 32]),
        )
        .expect_err("the same stable key must reject changed replacement S1 content");
    assert!(changed.to_string().contains("different content"));
    assert_eq!(codes.read_binding().unwrap().unwrap().generation, 2);
    let _ = std::fs::remove_dir_all(&dir);
}

/// P0-1. A read is a read: it binds no scope, bootstraps no ledger, and
/// therefore cannot resurrect a generation that was retired.
#[test]
fn reads_write_nothing_and_cannot_resurrect_a_retired_generation() {
    let dir = tmp_dir("read-only");
    let codes = open_store(&dir);
    let (store, _) = warmed_store(0);
    let image = store.export_generation().unwrap();
    assert!(codes.activate(7, &image).is_err());

    // Probing generations that were never activated must not grow the store.
    let before = store_fingerprint(&dir);
    for generation in [1u64, 2, 3, 999_999] {
        assert!(codes.read_generation(generation).unwrap().is_none());
    }
    assert!(codes.read_live().unwrap().is_none());
    assert_eq!(
        store_fingerprint(&dir),
        before,
        "a read of an unbound generation must write nothing"
    );

    // A direct retirement is refused as well; repeated reads still cannot
    // create a row or resurrect a generation.
    assert!(codes.retire(7).is_err());
    assert!(codes.read_live().unwrap().is_none());
    let after_retire = store_fingerprint(&dir);
    for _ in 0..3 {
        assert!(
            codes.read_generation(7).unwrap().is_none(),
            "a read must not resurrect a retired generation"
        );
    }
    assert_eq!(store_fingerprint(&dir), after_retire);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn direct_publication_cannot_create_two_generations() {
    let dir = tmp_dir("direct-generations");
    let codes = open_store(&dir);
    let (first, _) = warmed_store(0);
    let (second, _) = warmed_store(7);
    let one = first.export_generation().unwrap();
    let two = second.export_generation().unwrap();
    assert_ne!(one, two, "the two generations must be distinguishable");

    assert!(codes.activate(1, &one).is_err());
    assert!(codes.activate(2, &two).is_err());
    assert!(codes.read_generation(1).unwrap().is_none());
    assert!(codes.read_generation(2).unwrap().is_none());
    assert_eq!(codes.live_generation().unwrap(), None);
    assert!(codes.retire(1).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The fail-closed admission proof that replaces the deleted domain guard: a
/// batch naming one generation's scope cannot be admitted against another
/// generation's bound handle, even though both live in the same physical file
/// and the same owner layout.
#[test]
fn a_batch_for_another_generation_is_refused_at_admission() {
    let dir = tmp_dir("cross-binding");
    let codes = open_store(&dir);

    let one = codes.bind_for_write(1).unwrap();
    let two = codes.bind_for_write(2).unwrap();
    let foreign = codes.generation_batch(&two, 2, "deadbeef", 0);
    let error = codes
        .mutations
        .admit(&one, &foreign)
        .map(|_| ())
        .expect_err("a batch for generation 2 must not be admitted against generation 1");
    assert!(
        error.contains("does not serve this scope"),
        "admission must fail closed on the scope, not incidentally: {error}"
    );

    // Generation 1's rows are untouched by the refused attempt.
    assert!(codes.read_generation(1).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-1. `AdmittedOwnerWrite::open_table` returns a RAW `redb::Table` -- the
/// kernel bounds an owner write to its layout, not to its row keys, because
/// owner tables in general carry no scope component. For the semantic tables
/// the key does carry it, so the row-key ACL is this owner's obligation and the
/// bound accessors are the only writer path in this crate.
#[test]
fn a_bound_accessor_refuses_another_tenants_or_generations_rows() {
    let dir = tmp_dir("row-acl");
    let codes = open_store(&dir);

    let owner = codes.bind_for_write(1).unwrap();
    let expected = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "acl-probe", expected);
    let write = codes.mutations.open_write(&owner).unwrap();
    assert!(matches!(
        write.begin(&batch).unwrap(),
        eg_transaction::Begin::Apply { .. }
    ));
    let rows = write.owner_rows(&owner, &batch).unwrap();
    {
        let mut bound = BoundCodeRows::new(rows.open_table(ANN_CODES).unwrap(), TENANT, BINDING, 1);
        // The kernel would accept every one of these; the accessor does not.
        for foreign in [
            ("tenant-b", BINDING, 1u64, "meta"),
            (TENANT, "binding-z", 1, "meta"),
            (TENANT, BINDING, 2, "meta"),
        ] {
            let refused = bound
                .insert(foreign, b"forged")
                .expect_err("a foreign key must be refused");
            assert!(
                refused.to_string().contains("another generation's rows"),
                "{refused}"
            );
        }
        // Its own key is accepted, so the refusal is an ACL and not a stub.
        bound.insert((TENANT, BINDING, 1, "meta"), b"own").unwrap();

        let mut binding_rows =
            BoundBindingRows::new(rows.open_table(SEMANTIC_POINTERS).unwrap(), TENANT, BINDING);
        let refused = binding_rows
            .insert(("tenant-b", BINDING), b"forged")
            .expect_err("a foreign binding key must be refused");
        assert!(
            refused.to_string().contains("another binding's rows"),
            "{refused}"
        );
    }
    rows.finish_owner().unwrap();
    write.abort().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The retirement carries the generation it sweeps, so pairing it with another
/// generation's purge is refused rather than silently sweeping the wrong rows.
#[test]
fn a_retirement_describing_another_generation_is_refused() {
    let dir = tmp_dir("retirement");
    let codes = open_store(&dir);

    let one = codes.bind_for_write(1).unwrap();
    let identity = generation_identity(TENANT, BINDING, 1).unwrap();
    let mismatched = GenerationRetirement {
        tenant: TENANT.to_string(),
        binding: BINDING.to_string(),
        generation: 2,
    };
    let error = codes
        .mutations
        .purge_scope_with(&one, &identity, &mismatched)
        .expect_err("a retirement for generation 2 must not purge generation 1");
    assert!(
        error.contains("does not describe the scope being purged"),
        "{error}"
    );

    assert!(codes.read_generation(1).unwrap().is_none());
    assert!(codes.read_generation(2).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn direct_publication_cannot_become_a_dimension_authority() {
    let dir = tmp_dir("direct-authority");
    let codes = open_store(&dir);
    let (narrow, _) = warmed_store(0);
    let image = narrow.export_generation().unwrap();
    let refused = codes
        .activate(1, &image)
        .expect_err("direct publication must be refused before model checks");
    assert!(refused.to_string().contains("admitted S6"), "{refused}");
    assert!(
        codes.read_generation(1).unwrap().is_none(),
        "a refused activation must leave no rows"
    );
    assert_eq!(codes.live_generation().unwrap(), None);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn s6_generation_two_demotes_the_previous_live_binding_in_one_write() {
    let dir = tmp_dir("s6-live-demotion");
    let codes = open_store(&dir);
    let pending = pending_binding(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1",
    );
    let building = pending
        .apply_state_transition(
            &SemanticBindingStateTransition::create(
                &pending,
                SemanticBindingState::Building,
                "test",
            )
            .unwrap(),
        )
        .unwrap();
    let live = building
        .apply_state_transition(
            &SemanticBindingStateTransition::create(&building, SemanticBindingState::Live, "test")
                .unwrap(),
        )
        .unwrap();
    let pointer = SemanticActivePointer {
        tenant_id: TENANT.to_string(),
        binding_id: BINDING.to_string(),
        binding_digest: live.binding_digest,
        generation: 1,
        source_revision: live.source_revision.clone(),
        vector_target_id: live.vector_target_id.clone(),
        lexical_index_identity: live.lexical_index_identity.clone(),
        ann_index_identity: live.ann_index_identity.clone(),
        composite_policy_digest: live.policy_digest.clone(),
        activation_receipt_digest: digest(111),
        activated_at: "2026-09-08T00:08:00Z".to_string(),
    };
    pointer.validate().unwrap();
    let owner = codes.bind_for_write(1).unwrap();
    let read = codes.kernel.read_scope(&owner).unwrap();
    let version = eg_transaction::version(&read).unwrap();
    drop(read);
    let batch = codes.generation_batch(&owner, 1, "s6-demotion", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let binding_bytes = live.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_BINDINGS)
        .unwrap()
        .insert((TENANT, BINDING, 1), binding_bytes.as_slice())
        .unwrap();
    rows.open_table(SEMANTIC_POINTERS)
        .unwrap()
        .insert(
            (TENANT, BINDING),
            pointer.to_canonical_cbor().unwrap().as_slice(),
        )
        .unwrap();
    super::demote_prior_live_binding_in_write(&rows, TENANT, BINDING, 1).unwrap();
    rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let demoted = read
        .open_owner_table(SEMANTIC_BINDINGS)
        .unwrap()
        .get((TENANT, BINDING, 1))
        .unwrap()
        .map(|value| SemanticBinding::from_canonical_cbor(value.value()).unwrap())
        .unwrap();
    assert_eq!(demoted.durable_state, SemanticBindingState::Disabled);
    drop(read);
    let _ = std::fs::remove_dir_all(&dir);

    let refusal_dir = tmp_dir("s6-live-demotion-refusal");
    let refusal_codes = open_store(&refusal_dir);
    let pending = pending_binding(
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1",
    );
    // Commit the prior binding DURABLY first, the way the sibling
    // `real_s6_generation_two_finalization_demotes_live_one_and_rolls_back_on_refusal`
    // does. The block below writes `pending` inside the write it is about to
    // ABORT, so without this the row never existed durably and
    // `read_binding()` -- which is head-pointer driven and reads
    // `SEMANTIC_HEADS` first -- correctly returned `None`, not `pending`: the
    // rollback assertion was measuring an uncommitted write, not a rollback.
    refusal_codes.store_binding(&pending, 1).unwrap();
    let owner = refusal_codes.bind_for_write(1).unwrap();
    let read = refusal_codes.kernel.read_scope(&owner).unwrap();
    let version = eg_transaction::version(&read).unwrap();
    drop(read);
    let batch = refusal_codes.generation_batch(&owner, 1, "s6-demotion-refusal", version);
    let (write, begin) = refusal_codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let binding_bytes = pending.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_BINDINGS)
        .unwrap()
        .insert((TENANT, BINDING, 1), binding_bytes.as_slice())
        .unwrap();
    let refused = super::demote_prior_live_binding_in_write(&rows, TENANT, BINDING, 1)
        .expect_err("S6 must refuse a pointer whose prior binding is not live");
    assert!(refused.to_string().contains("not durably live"));
    drop(rows);
    write.abort().unwrap();
    assert_eq!(refusal_codes.read_binding().unwrap().unwrap(), pending);
    let _ = std::fs::remove_dir_all(&refusal_dir);
}

#[test]
fn real_s6_generation_two_finalization_demotes_live_one_and_rolls_back_on_refusal() {
    let dir = tmp_dir("s6-finalize-generation-two");
    let codes = open_store(&dir);
    let revision_one =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
    let revision_two =
        "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:2";
    let space = EmbeddingSpaceRef::pinned(
        "embedding-model",
        "revision-1",
        "model-hash",
        "preprocess-hash",
        16,
        true,
    )
    .unwrap();
    let image_store = warmed_store_in_space(space, 0);
    let image = image_store.export_generation().unwrap();
    let (image_dimension, image_model) = image.identity().unwrap();
    let image_model = image_model.expect("the S6 image must carry its pinned model identity");
    let generation_one = binding_for_image(
        revision_one,
        1,
        image_dimension as u32,
        &image_model,
        "preprocess-hash",
    );
    let generation_two_pending = binding_for_image(
        revision_two,
        2,
        image_dimension as u32,
        &image_model,
        "preprocess-hash",
    );
    let building_transition = SemanticBindingStateTransition::create(
        &generation_two_pending,
        SemanticBindingState::Building,
        "test",
    )
    .unwrap();
    let generation_two = generation_two_pending
        .apply_state_transition(&building_transition)
        .unwrap();
    let generation_one_building = generation_one
        .apply_state_transition(
            &SemanticBindingStateTransition::create(
                &generation_one,
                SemanticBindingState::Building,
                "test",
            )
            .unwrap(),
        )
        .unwrap();
    let generation_one_live = generation_one_building
        .apply_state_transition(
            &SemanticBindingStateTransition::create(
                &generation_one_building,
                SemanticBindingState::Live,
                "test",
            )
            .unwrap(),
        )
        .unwrap();
    let pointer_one = SemanticActivePointer {
        tenant_id: TENANT.to_string(),
        binding_id: BINDING.to_string(),
        binding_digest: generation_one_live.binding_digest,
        generation: 1,
        source_revision: generation_one_live.source_revision.clone(),
        vector_target_id: generation_one_live.vector_target_id.clone(),
        lexical_index_identity: generation_one_live.lexical_index_identity.clone(),
        ann_index_identity: generation_one_live.ann_index_identity.clone(),
        composite_policy_digest: generation_one_live.policy_digest.clone(),
        activation_receipt_digest: digest(11),
        activated_at: "2026-09-08T00:08:00Z".to_string(),
    };
    pointer_one.validate().unwrap();

    let source_entity_id = reconciliation_source_entity(73);
    let lexical_member = SemanticGenerationMember {
        source_entity_id: source_entity_id.clone(),
        source_revision: revision_two.to_string(),
        receipt_digest: digest(21),
        artifact_digest: digest(22),
    };
    let expected_entity = SemanticExpectedEntity {
        source_entity_id: source_entity_id.clone(),
        source_revision: revision_two.to_string(),
    };
    let lexical_aggregate = SemanticGenerationAggregate::create(
        BINDING,
        generation_two.binding_digest,
        2,
        revision_two,
        SemanticStage::LexicalIndex,
        vec![expected_entity.clone()],
        vec![lexical_member.clone()],
    )
    .unwrap();
    let lexical_checkpoint =
        SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: BINDING.to_string(),
            binding_digest: generation_two.binding_digest,
            generation: 2,
            source_revision: revision_two.to_string(),
            stage: SemanticStage::LexicalIndex,
            aggregate: lexical_aggregate.clone(),
            dependency: SemanticGenerationDependency::None,
            artifact_digest: lexical_aggregate.aggregate_artifact_digest,
            completed_at: "2026-09-08T00:09:01Z".to_string(),
        })
        .unwrap();
    let vector_member = SemanticGenerationMember {
        source_entity_id: source_entity_id.clone(),
        source_revision: revision_two.to_string(),
        receipt_digest: digest(23),
        artifact_digest: digest(24),
    };
    let vector_aggregate = SemanticGenerationAggregate::create(
        BINDING,
        generation_two.binding_digest,
        2,
        revision_two,
        SemanticStage::Vector,
        vec![expected_entity.clone()],
        vec![vector_member],
    )
    .unwrap();
    let vector_checkpoint =
        SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: BINDING.to_string(),
            binding_digest: generation_two.binding_digest,
            generation: 2,
            source_revision: revision_two.to_string(),
            stage: SemanticStage::Vector,
            aggregate: vector_aggregate.clone(),
            dependency: SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::LexicalIndex,
                checkpoint_digest: lexical_checkpoint.checkpoint_digest,
            },
            artifact_digest: vector_aggregate.aggregate_artifact_digest,
            completed_at: "2026-09-08T00:09:02Z".to_string(),
        })
        .unwrap();
    let ann_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: revision_two.to_string(),
        stage: SemanticStage::AnnIndex,
        predecessor: SemanticStagePredecessor::GenerationCoverage {
            checkpoint: Box::new(vector_checkpoint.clone()),
        },
        input_digest: digest(25),
    })
    .unwrap();
    let ann_receipt = SemanticStageReceipt {
        intent_digest: ann_intent.intent_digest,
        output_digest: digest(26),
        cursor: "cursor:ann".to_string(),
        completed_at: "2026-09-08T00:09:03Z".to_string(),
        outcome: SemanticStageOutcome::Completed,
    };
    let ann_member = SemanticGenerationMember {
        source_entity_id: source_entity_id.clone(),
        source_revision: revision_two.to_string(),
        receipt_digest: ann_receipt.receipt_digest(),
        artifact_digest: ann_receipt.output_digest,
    };
    let ann_aggregate = SemanticGenerationAggregate::create(
        BINDING,
        generation_two.binding_digest,
        2,
        revision_two,
        SemanticStage::AnnIndex,
        vec![expected_entity.clone()],
        vec![ann_member.clone()],
    )
    .unwrap();
    let ann_checkpoint = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        source_revision: revision_two.to_string(),
        stage: SemanticStage::AnnIndex,
        aggregate: ann_aggregate.clone(),
        dependency: SemanticGenerationDependency::Checkpoint {
            stage: SemanticStage::Vector,
            checkpoint_digest: vector_checkpoint.checkpoint_digest,
        },
        artifact_digest: ann_aggregate.aggregate_artifact_digest,
        completed_at: ann_receipt.completed_at.clone(),
    })
    .unwrap();
    let ann_update = SemanticGenerationCheckpointUpdate {
        expected_previous_checkpoint_digest: None,
        expected_previous_completed_count: 0,
        member: ann_member.clone(),
        successor: ann_checkpoint.clone(),
    };
    let ann_transition = SemanticStageTransition {
        intent: ann_intent.clone(),
        receipt: ann_receipt.clone(),
        generation_checkpoint: Some(Box::new(ann_update)),
    };
    ann_transition.validate().unwrap();
    let ann_mutation = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(ann_transition),
        artifact: SemanticStageArtifact::None,
    };
    ann_mutation.validate().unwrap();

    let s6_aggregate = SemanticGenerationAggregate::create(
        BINDING,
        generation_two.binding_digest,
        2,
        revision_two,
        SemanticStage::ReconcileAndActivate,
        vec![expected_entity],
        vec![ann_member],
    )
    .unwrap();
    let activation_target = SemanticActivationTarget::create(&generation_two).unwrap();
    let s6_checkpoint = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        source_revision: revision_two.to_string(),
        stage: SemanticStage::ReconcileAndActivate,
        aggregate: s6_aggregate,
        dependency: SemanticGenerationDependency::Activation {
            lexical_checkpoint_digest: lexical_checkpoint.checkpoint_digest,
            ann_checkpoint_digest: ann_checkpoint.checkpoint_digest,
        },
        artifact_digest: activation_target.target_digest,
        completed_at: "2026-09-08T00:09:04Z".to_string(),
    })
    .unwrap();
    let pointer_two = SemanticActivePointer {
        tenant_id: TENANT.to_string(),
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        source_revision: revision_two.to_string(),
        vector_target_id: generation_two.vector_target_id.clone(),
        lexical_index_identity: generation_two.lexical_index_identity.clone(),
        ann_index_identity: generation_two.ann_index_identity.clone(),
        composite_policy_digest: generation_two.policy_digest.clone(),
        activation_receipt_digest: s6_checkpoint.checkpoint_digest,
        activated_at: s6_checkpoint.completed_at.clone(),
    };
    let activation_artifact = SemanticGenerationArtifact::Activation {
        target: Box::new(activation_target),
        pointer: Box::new(pointer_two.clone()),
    };
    activation_artifact
        .validate_against(&s6_checkpoint)
        .unwrap();
    let s6_intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        scope: SemanticStageScope::Generation,
        source_revision: revision_two.to_string(),
        stage: SemanticStage::ReconcileAndActivate,
        predecessor: SemanticStagePredecessor::Activation {
            lexical_checkpoint_digest: lexical_checkpoint.checkpoint_digest,
            ann_checkpoint_digest: ann_checkpoint.checkpoint_digest,
        },
        input_digest: s6_checkpoint.checkpoint_digest,
    })
    .unwrap();
    let s6_receipt = SemanticStageReceipt {
        intent_digest: s6_intent.intent_digest,
        output_digest: s6_checkpoint.artifact_digest,
        cursor: "cursor:s6".to_string(),
        completed_at: s6_checkpoint.completed_at.clone(),
        outcome: SemanticStageOutcome::Completed,
    };
    let s6_transition = SemanticStageTransition {
        intent: s6_intent.clone(),
        receipt: s6_receipt,
        generation_checkpoint: None,
    };
    s6_transition.validate().unwrap();
    let s6_outbox = super::stage_intent_outbox(&s6_intent).unwrap();

    let generation_one_bytes = generation_one_live.to_canonical_cbor().unwrap();
    let generation_two_bytes = generation_two.to_canonical_cbor().unwrap();
    let state_bytes = building_transition.to_canonical_cbor().unwrap();
    let pointer_one_bytes = pointer_one.to_canonical_cbor().unwrap();
    let ann_stage_key = ann_intent.intent_digest.to_string();
    let s6_stage_key = s6_intent.intent_digest.to_string();
    let progress = SemanticSourceProgress {
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        source_entity_id: source_entity_id.clone(),
        source_revision: revision_two.to_string(),
        completed_stage: Some(SemanticStage::AnnIndex),
        completed_receipt_digest: Some(ann_receipt.receipt_digest()),
        superseded_by_revision: None,
        updated_at: "unix-ms:9".to_string(),
    };
    let progress_bytes = progress.to_canonical_cbor().unwrap();
    let ann_mutation_bytes = ann_mutation.to_canonical_cbor().unwrap();
    let lexical_checkpoint_bytes = lexical_checkpoint.to_canonical_cbor().unwrap();
    let vector_checkpoint_bytes = vector_checkpoint.to_canonical_cbor().unwrap();
    let ann_checkpoint_bytes = ann_checkpoint.to_canonical_cbor().unwrap();
    let lexical_manifest = SemanticLexicalIndexManifest {
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        source_revision: revision_two.to_string(),
        identity: generation_two.lexical_index_identity.clone(),
        artifact_digest: lexical_checkpoint.artifact_digest,
        row_count: 1,
        completed_receipt_digest: lexical_member.receipt_digest,
        completed_at: lexical_checkpoint.completed_at.clone(),
    };
    lexical_manifest.validate().unwrap();
    let ann_manifest = SemanticAnnIndexManifest {
        binding_id: BINDING.to_string(),
        binding_digest: generation_two.binding_digest,
        generation: 2,
        source_revision: revision_two.to_string(),
        identity: generation_two.ann_index_identity.clone(),
        artifact_digest: ann_checkpoint.artifact_digest,
        vector_count: 1,
        completed_receipt_digest: ann_receipt.receipt_digest(),
        completed_at: ann_checkpoint.completed_at.clone(),
    };
    ann_manifest.validate().unwrap();
    let lexical_manifest_bytes = lexical_manifest.to_canonical_cbor().unwrap();
    let ann_manifest_bytes = ann_manifest.to_canonical_cbor().unwrap();

    // The first attempt deliberately removes the old binding after the S6
    // lease is claimed.  Finalization has already admitted the transition
    // rows when the prior-live demotion discovers the missing row, so the
    // transaction must roll every staged artifact back.
    let consumer = "semantic-s6-real";
    codes.subscribe_stage_consumer(consumer).unwrap();
    let seed_digest = digest(31);
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    "semantic-index:s6-real-seed",
                    "semantic_s6_real_seed",
                    "generation:2",
                    seed_digest,
                    vec![s6_outbox.clone()],
                    10,
                )
            },
            seed_digest,
            10,
            |_, rows| {
                rows.open_table(SEMANTIC_BINDINGS)
                    .unwrap()
                    .insert((TENANT, BINDING, 1), generation_one_bytes.as_slice())
                    .unwrap();
                rows.open_table(SEMANTIC_BINDINGS)
                    .unwrap()
                    .insert((TENANT, BINDING, 2), generation_two_bytes.as_slice())
                    .unwrap();
                rows.open_table(SEMANTIC_HEADS)
                    .unwrap()
                    .insert((TENANT, BINDING), 2)
                    .unwrap();
                rows.open_table(SEMANTIC_STATES)
                    .unwrap()
                    .insert((TENANT, BINDING), state_bytes.as_slice())
                    .unwrap();
                rows.open_table(SEMANTIC_POINTERS)
                    .unwrap()
                    .insert((TENANT, BINDING), pointer_one_bytes.as_slice())
                    .unwrap();
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .unwrap()
                    .insert(
                        (TENANT, BINDING, 2, source_entity_id.as_str()),
                        progress_bytes.as_slice(),
                    )
                    .unwrap();
                rows.open_table(SEMANTIC_STAGES)
                    .unwrap()
                    .insert(
                        (TENANT, BINDING, ann_stage_key.as_str()),
                        ann_mutation_bytes.as_slice(),
                    )
                    .unwrap();
                for (checkpoint, bytes) in [
                    (&lexical_checkpoint, lexical_checkpoint_bytes.as_slice()),
                    (&vector_checkpoint, vector_checkpoint_bytes.as_slice()),
                    (&ann_checkpoint, ann_checkpoint_bytes.as_slice()),
                ] {
                    let checkpoint_key = checkpoint.checkpoint_digest.to_string();
                    rows.open_table(SEMANTIC_CHECKPOINTS)
                        .unwrap()
                        .insert((TENANT, BINDING, 2, checkpoint_key.as_str()), bytes)
                        .unwrap();
                    rows.open_table(SEMANTIC_CHECKPOINT_HEADS)
                        .unwrap()
                        .insert(
                            (TENANT, BINDING, 2, revision_two, checkpoint.stage.as_str()),
                            bytes,
                        )
                        .unwrap();
                }
                rows.open_table(SEMANTIC_LEXICAL)
                    .unwrap()
                    .insert((TENANT, BINDING, 2), lexical_manifest_bytes.as_slice())
                    .unwrap();
                rows.open_table(SEMANTIC_ANN)
                    .unwrap()
                    .insert((TENANT, BINDING, 2), ann_manifest_bytes.as_slice())
                    .unwrap();
                Ok(())
            },
        )
        .unwrap();
    let mut budget = eg_transaction::OutboxClaimBudget::new(1, 5_000, 11).unwrap();
    let first_lease = codes
        .claim_stage_leases(consumer, &mut budget)
        .unwrap()
        .claims
        .into_iter()
        .next()
        .unwrap();

    let remove_digest = digest(32);
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    "semantic-index:s6-remove-prior",
                    "semantic_s6_remove_prior",
                    "generation:1",
                    remove_digest,
                    Vec::new(),
                    12,
                )
            },
            remove_digest,
            12,
            |_, rows| {
                rows.open_table(SEMANTIC_BINDINGS)
                    .unwrap()
                    .remove((TENANT, BINDING, 1))
                    .unwrap();
                Ok(())
            },
        )
        .unwrap();
    let refused = codes
        .finalize_generation(
            &first_lease,
            &s6_transition,
            &s6_checkpoint,
            &activation_artifact,
            &image,
            13,
        )
        .expect_err("S6 must roll back when the prior live generation disappears");
    assert!(
        refused.to_string().contains("missing prior binding"),
        "{refused}"
    );
    assert_eq!(codes.read_binding().unwrap().unwrap(), generation_two);
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let pointer_after_refusal = read
        .open_owner_table(SEMANTIC_POINTERS)
        .unwrap()
        .get((TENANT, BINDING))
        .unwrap()
        .map(|value| value.value().to_vec())
        .unwrap();
    assert_eq!(pointer_after_refusal, pointer_one_bytes);
    assert!(read
        .open_owner_table(SEMANTIC_STAGES)
        .unwrap()
        .get((TENANT, BINDING, ann_stage_key.as_str()))
        .unwrap()
        .is_some());
    assert!(read
        .open_owner_table(SEMANTIC_STAGES)
        .unwrap()
        .get((TENANT, BINDING, s6_stage_key.as_str()))
        .unwrap()
        .is_none());
    assert!(read
        .open_owner_table(SEMANTIC_CHECKPOINT_HEADS)
        .unwrap()
        .get((
            TENANT,
            BINDING,
            2,
            revision_two,
            SemanticStage::ReconcileAndActivate.as_str(),
        ))
        .unwrap()
        .is_none());
    drop(read);
    // The ANN row above is the durable predecessor and must remain; the S6
    // row is intentionally keyed by the S6 intent, so the assertion above
    // checks the seeded transition remains while no finalization row can be
    // mistaken for it.
    codes.release_stage_lease(&first_lease).unwrap();

    let restore_digest = digest(33);
    codes
        .commit_metadata(
            |version| {
                codes.metadata_batch(
                    &codes.serving,
                    version,
                    "semantic-index:s6-restore-prior",
                    "semantic_s6_restore_prior",
                    "generation:1",
                    restore_digest,
                    Vec::new(),
                    14,
                )
            },
            restore_digest,
            14,
            |_, rows| {
                rows.open_table(SEMANTIC_BINDINGS)
                    .unwrap()
                    .insert((TENANT, BINDING, 1), generation_one_bytes.as_slice())
                    .unwrap();
                Ok(())
            },
        )
        .unwrap();
    let mut budget = eg_transaction::OutboxClaimBudget::new(1, 5_000, 15).unwrap();
    let second_lease = codes
        .claim_stage_leases(consumer, &mut budget)
        .unwrap()
        .claims
        .into_iter()
        .next()
        .unwrap();
    let receipt = codes
        .finalize_generation(
            &second_lease,
            &s6_transition,
            &s6_checkpoint,
            &activation_artifact,
            &image,
            16,
        )
        .expect("the exact S6 proof must publish generation two");
    assert!(!receipt.replayed);
    assert_eq!(codes.live_generation().unwrap(), Some(2));
    let read = codes.kernel.read_scope(&codes.serving).unwrap();
    let prior = read
        .open_owner_table(SEMANTIC_BINDINGS)
        .unwrap()
        .get((TENANT, BINDING, 1))
        .unwrap()
        .map(|value| SemanticBinding::from_canonical_cbor(value.value()).unwrap())
        .unwrap();
    let current = read
        .open_owner_table(SEMANTIC_BINDINGS)
        .unwrap()
        .get((TENANT, BINDING, 2))
        .unwrap()
        .map(|value| SemanticBinding::from_canonical_cbor(value.value()).unwrap())
        .unwrap();
    let pointer = read
        .open_owner_table(SEMANTIC_POINTERS)
        .unwrap()
        .get((TENANT, BINDING))
        .unwrap()
        .map(|value| SemanticActivePointer::from_canonical_cbor(value.value()).unwrap())
        .unwrap();
    assert_eq!(prior.durable_state, SemanticBindingState::Disabled);
    assert_eq!(current.durable_state, SemanticBindingState::Live);
    assert_eq!(pointer.generation, 2);
    assert_eq!(
        pointer.activation_receipt_digest,
        s6_checkpoint.checkpoint_digest
    );
    drop(read);
    let ledger = codes.kernel.read_scope(&codes.serving).unwrap();
    let record = eg_transaction::read_ledger(&ledger, &receipt.batch_id)
        .unwrap()
        .expect("S6 must leave one exact durable ledger receipt");
    assert_eq!(
        super::mutation_digest_from_batch(&record.batch).unwrap(),
        receipt.mutation_digest
    );
    drop(ledger);
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-3. Two admitted writes for one generation that both decided against the
/// same version: the first commits, the second is refused by the ledger rather
/// than overwriting it. Deterministic rather than threaded -- the race is the
/// stale version expectation, and this reproduces exactly that state.
#[test]
fn a_second_admitted_write_racing_the_same_version_fails_closed() {
    let dir = tmp_dir("race");
    let codes = open_store(&dir);
    let owner = codes.bind_for_write(1).unwrap();
    let stale_version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let winner = codes.generation_batch(&owner, 1, "winner-digest", stale_version);
    let loser = codes.generation_batch(&owner, 1, "loser-digest", stale_version);

    // The winner is an ordinary admitted owner mutation. The legacy ANN
    // publication helper is not used to advance the ledger or scope version.
    let (winner_write, begin) = codes.mutations.admit(&owner, &winner).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_winner_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let owner_rows = winner_write.owner_rows(&owner, &winner).unwrap();
    owner_rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&winner_write, &winner, None, 0, source_version_winner_write)
        .unwrap();
    codes.mutations.commit(winner_write, &winner).unwrap();

    // The loser was built against the version the winner consumed.
    let error = codes
        .mutations
        .admit(&owner, &loser)
        .map(|_| ())
        .expect_err("an activation racing a committed one must fail closed");
    assert!(error.contains("STALE_VERSION"), "{error}");
    assert!(codes.read_generation(1).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn production_generation_checkpoint_heads_refuse_stale_s3_and_s5_cas() {
    for stage in [SemanticStage::LexicalIndex, SemanticStage::AnnIndex] {
        let dir = tmp_dir(&format!("checkpoint-cas-{}", stage.as_str()));
        let codes = open_store(&dir);
        let fixture = generation_checkpoint_fixture(stage);
        let owner = codes.bind_for_write(1).unwrap();
        let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
        let winner = codes.generation_batch(&owner, 1, "checkpoint-cas-winner", version);
        let (winner_write, begin) = codes.mutations.admit(&owner, &winner).unwrap();
        let eg_transaction::Begin::Apply {
            source_version: source_version_winner_write,
        } = begin
        else {
            panic!("expected a fresh Begin::Apply, got a replay");
        };
        let winner_rows = winner_write.owner_rows(&owner, &winner).unwrap();
        let progress_bytes = fixture.progress.to_canonical_cbor().unwrap();
        winner_rows
            .open_table(SEMANTIC_SOURCE_PROGRESS)
            .unwrap()
            .insert((TENANT, BINDING, 1, "entity:a"), progress_bytes.as_slice())
            .unwrap();
        let stage_key = fixture.transition.intent.intent_digest.to_string();
        winner_rows
            .open_table(SEMANTIC_STAGES)
            .unwrap()
            .insert(
                (TENANT, BINDING, stage_key.as_str()),
                fixture.mutation_bytes.as_slice(),
            )
            .unwrap();
        super::persist_generation_checkpoint_update(
            &winner_rows,
            TENANT,
            BINDING,
            &fixture.transition,
            &fixture.update,
        )
        .unwrap();
        winner_rows.finish_owner().unwrap();
        // A write only reaches the kernel's `Finished` admission state
        // through `finish`; `finish_owner` above closes only the owner-row
        // half, so `commit` alone is refused.
        codes
            .mutations
            .finish(&winner_write, &winner, None, 0, source_version_winner_write)
            .unwrap();
        codes.mutations.commit(winner_write, &winner).unwrap();

        let before = store_fingerprint(&dir);
        let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
        let loser = codes.generation_batch(&owner, 1, "checkpoint-cas-loser", version);
        let (loser_write, begin) = codes.mutations.admit(&owner, &loser).unwrap();
        let eg_transaction::Begin::Apply {
            source_version: source_version_loser_write,
        } = begin
        else {
            panic!("expected a fresh Begin::Apply, got a replay");
        };
        let loser_rows = loser_write.owner_rows(&owner, &loser).unwrap();
        let refused = super::persist_generation_checkpoint_update(
            &loser_rows,
            TENANT,
            BINDING,
            &fixture.transition,
            &fixture.update,
        )
        .expect_err("a stale S3/S5 predecessor must not advance the checkpoint head");
        drop(loser_rows);
        loser_write.abort().unwrap();
        assert!(
            refused.to_string().contains("CAS predecessor is stale"),
            "unexpected stale-head refusal: {refused}"
        );
        assert_eq!(
            store_fingerprint(&dir),
            before,
            "a stale checkpoint CAS must leave every owner row unchanged"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn production_six_rejects_complete_nonhead_checkpoint_dependencies() {
    let dir = tmp_dir("checkpoint-s6-head");
    let codes = open_store(&dir);
    let lexical = complete_checkpoint_fixture(
        SemanticStage::LexicalIndex,
        60,
        SemanticGenerationDependency::None,
    );
    let orphan_lexical = complete_checkpoint_fixture(
        SemanticStage::LexicalIndex,
        61,
        SemanticGenerationDependency::None,
    );
    let ann_fixture = generation_checkpoint_fixture(SemanticStage::AnnIndex);
    let ann = ann_fixture.update.successor.clone();
    let binding_digest = SemanticDigest::from_bytes([8; 32]);
    let member = ann_fixture.update.member.clone();
    let aggregate = SemanticGenerationAggregate::create(
        BINDING,
        binding_digest,
        1,
        "r1",
        SemanticStage::ReconcileAndActivate,
        vec![SemanticExpectedEntity {
            source_entity_id: "entity:a".to_string(),
            source_revision: "r1".to_string(),
        }],
        vec![member],
    )
    .unwrap();
    let make_six = |lexical_digest| {
        SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: BINDING.to_string(),
            binding_digest,
            generation: 1,
            source_revision: "r1".to_string(),
            stage: SemanticStage::ReconcileAndActivate,
            aggregate: aggregate.clone(),
            dependency: SemanticGenerationDependency::Activation {
                lexical_checkpoint_digest: lexical_digest,
                ann_checkpoint_digest: ann.checkpoint_digest,
            },
            artifact_digest: aggregate.aggregate_artifact_digest,
            completed_at: "2026-09-08T00:03:00Z".to_string(),
        })
        .unwrap()
    };
    let forged = make_six(orphan_lexical.checkpoint_digest);
    let exact = make_six(lexical.checkpoint_digest);

    let owner = codes.bind_for_write(1).unwrap();
    let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "checkpoint-s6-head", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let progress = SemanticSourceProgress {
        binding_id: BINDING.to_string(),
        binding_digest,
        generation: 1,
        source_entity_id: "entity:a".to_string(),
        source_revision: "r1".to_string(),
        completed_stage: Some(SemanticStage::AnnIndex),
        completed_receipt_digest: Some(ann_fixture.transition.receipt.receipt_digest()),
        superseded_by_revision: None,
        updated_at: "unix-ms:1".to_string(),
    };
    let progress_bytes = progress.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .insert((TENANT, BINDING, 1, "entity:a"), progress_bytes.as_slice())
        .unwrap();
    let ann_stage_key = ann_fixture.transition.intent.intent_digest.to_string();
    rows.open_table(SEMANTIC_STAGES)
        .unwrap()
        .insert(
            (TENANT, BINDING, ann_stage_key.as_str()),
            ann_fixture.mutation_bytes.as_slice(),
        )
        .unwrap();
    for checkpoint in [&lexical, &orphan_lexical, &ann] {
        let key = checkpoint.checkpoint_digest.to_string();
        let bytes = checkpoint.to_canonical_cbor().unwrap();
        rows.open_table(SEMANTIC_CHECKPOINTS)
            .unwrap()
            .insert((TENANT, BINDING, 1, key.as_str()), bytes.as_slice())
            .unwrap();
        if checkpoint != &orphan_lexical {
            rows.open_table(SEMANTIC_CHECKPOINT_HEADS)
                .unwrap()
                .insert(
                    (TENANT, BINDING, 1, "r1", checkpoint.stage.as_str()),
                    bytes.as_slice(),
                )
                .unwrap();
        }
    }
    let refused = super::validate_six_checkpoint_write(&rows, TENANT, BINDING, &forged)
        .expect_err("a complete checkpoint that names a non-head lexical branch must be refused");
    assert!(
        refused.to_string().contains("current lexical/ANN head"),
        "unexpected non-head S6 refusal: {refused}"
    );
    super::validate_six_checkpoint_write(&rows, TENANT, BINDING, &exact)
        .expect("the exact lexical and ANN heads must satisfy S6 validation");
    let lexical_bytes = lexical.to_canonical_cbor().unwrap();
    assert_eq!(
        rows.open_table(SEMANTIC_CHECKPOINT_HEADS)
            .unwrap()
            .get((
                TENANT,
                BINDING,
                1,
                "r1",
                SemanticStage::LexicalIndex.as_str(),
            ))
            .unwrap()
            .map(|value| value.value().to_vec()),
        Some(lexical_bytes),
        "the durable lexical head remains the selected branch"
    );
    let orphan_key = orphan_lexical.checkpoint_digest.to_string();
    let orphan_bytes = orphan_lexical.to_canonical_cbor().unwrap();
    assert_eq!(
        rows.open_table(SEMANTIC_CHECKPOINTS)
            .unwrap()
            .get((TENANT, BINDING, 1, orphan_key.as_str()))
            .unwrap()
            .map(|value| value.value().to_vec()),
        Some(orphan_bytes),
        "non-head lexical branch remains durable while excluded from authority"
    );
    rows.finish_owner().unwrap();
    // A write only reaches the kernel's `Finished` admission state
    // through `finish`; `finish_owner` above closes only the owner-row
    // half, so `commit` alone is refused.
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_head_resolves_only_the_durable_pointer() {
    // Kernel-seeded against the declared `SEMANTIC_CHECKPOINT*` tables; see the
    // note on `checkpoint_head_does_not_promote_orphan_higher_count_rows` for
    // why the fixture's own `"binding-a"` identity is preserved verbatim.
    let dir = tmp_dir("checkpoint-head");
    let codes = open_store(&dir);
    let partial = checkpoint_head_fixture(1, 1);
    let complete = checkpoint_head_fixture(2, 9);

    let owner = codes.bind_for_write(1).unwrap();
    let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "checkpoint-head", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let partial_key = partial.checkpoint_digest.to_string();
    let partial_bytes = partial.to_canonical_cbor().unwrap();
    let complete_key = complete.checkpoint_digest.to_string();
    let complete_bytes = complete.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_CHECKPOINTS)
        .unwrap()
        .insert(
            ("native", "binding-a", 1, partial_key.as_str()),
            partial_bytes.as_slice(),
        )
        .unwrap();
    rows.open_table(SEMANTIC_CHECKPOINTS)
        .unwrap()
        .insert(
            ("native", "binding-a", 1, complete_key.as_str()),
            complete_bytes.as_slice(),
        )
        .unwrap();
    // Only the COMPLETE checkpoint gets a durable head pointer; the partial one
    // stays a durable-but-unselected branch.
    rows.open_table(SEMANTIC_CHECKPOINT_HEADS)
        .unwrap()
        .insert(
            (
                "native",
                "binding-a",
                1,
                "r1",
                SemanticStage::LexicalIndex.as_str(),
            ),
            complete_bytes.as_slice(),
        )
        .unwrap();
    let head = current_checkpoint_from_tables(
        &rows.open_table(SEMANTIC_CHECKPOINTS).unwrap(),
        &rows.open_table(SEMANTIC_CHECKPOINT_HEADS).unwrap(),
        "native",
        "binding-a",
        1,
        SemanticDigest::from_bytes([8; 32]),
        "r1",
        SemanticStage::LexicalIndex,
    )
    .unwrap()
    .expect("a durable checkpoint head must be found");
    assert_eq!(head.checkpoint_digest, complete.checkpoint_digest);
    assert_eq!(head.aggregate.completed_entity_count, 2);
    rows.finish_owner().unwrap();
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_head_does_not_promote_orphan_higher_count_rows() {
    // Seeded through the mutation kernel against the real declared
    // `SEMANTIC_CHECKPOINT*` tables rather than a private redb file: RF-RULING-007
    // (`semantic_arch_gate`) forbids this region from re-acquiring a store, and
    // its allowlist is deliberately empty. The row identities below are the
    // fixture's own (`checkpoint_head_fixture` builds its aggregate under
    // `"binding-a"`), so the lookup still resolves the rows it seeded and a
    // `None` result still means "the orphan was not promoted" rather than
    // "the key never matched".
    let dir = tmp_dir("checkpoint-orphan");
    let codes = open_store(&dir);
    let partial = checkpoint_head_fixture(1, 1);
    let complete = checkpoint_head_fixture(2, 9);

    let owner = codes.bind_for_write(1).unwrap();
    let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "checkpoint-orphan", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    // No `SEMANTIC_CHECKPOINT_HEADS` row is written on purpose -- an orphan
    // checkpoint is exactly one with no durable head pointer.
    for checkpoint in [&partial, &complete] {
        let key = checkpoint.checkpoint_digest.to_string();
        let bytes = checkpoint.to_canonical_cbor().unwrap();
        rows.open_table(SEMANTIC_CHECKPOINTS)
            .unwrap()
            .insert(("native", "binding-a", 1, key.as_str()), bytes.as_slice())
            .unwrap();
    }
    assert!(
        current_checkpoint_from_tables(
            &rows.open_table(SEMANTIC_CHECKPOINTS).unwrap(),
            &rows.open_table(SEMANTIC_CHECKPOINT_HEADS).unwrap(),
            "native",
            "binding-a",
            1,
            SemanticDigest::from_bytes([8; 32]),
            "r1",
            SemanticStage::LexicalIndex,
        )
        .unwrap()
        .is_none(),
        "an orphan checkpoint row must not become a stage head by completion count"
    );
    rows.finish_owner().unwrap();
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_head_rejects_pointer_row_bytes_mismatch() {
    // Kernel-seeded against the declared `SEMANTIC_CHECKPOINT*` tables; see the
    // note on `checkpoint_head_does_not_promote_orphan_higher_count_rows`.
    let dir = tmp_dir("checkpoint-ambiguity");
    let codes = open_store(&dir);
    let left = checkpoint_head_fixture(1, 1);
    let right = checkpoint_head_fixture(1, 9);

    let owner = codes.bind_for_write(1).unwrap();
    let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "checkpoint-ambiguity", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let left_key = left.checkpoint_digest.to_string();
    let left_bytes = left.to_canonical_cbor().unwrap();
    let right_key = right.checkpoint_digest.to_string();
    let right_bytes = right.to_canonical_cbor().unwrap();
    // The row filed under LEFT's key deliberately holds RIGHT's bytes -- that
    // disagreement is what the head pointer must fail closed on.
    rows.open_table(SEMANTIC_CHECKPOINTS)
        .unwrap()
        .insert(
            ("native", "binding-a", 1, left_key.as_str()),
            right_bytes.as_slice(),
        )
        .unwrap();
    rows.open_table(SEMANTIC_CHECKPOINTS)
        .unwrap()
        .insert(
            ("native", "binding-a", 1, right_key.as_str()),
            right_bytes.as_slice(),
        )
        .unwrap();
    rows.open_table(SEMANTIC_CHECKPOINT_HEADS)
        .unwrap()
        .insert(
            (
                "native",
                "binding-a",
                1,
                "r1",
                SemanticStage::LexicalIndex.as_str(),
            ),
            left_bytes.as_slice(),
        )
        .unwrap();
    let error = current_checkpoint_from_tables(
        &rows.open_table(SEMANTIC_CHECKPOINTS).unwrap(),
        &rows.open_table(SEMANTIC_CHECKPOINT_HEADS).unwrap(),
        "native",
        "binding-a",
        1,
        SemanticDigest::from_bytes([8; 32]),
        "r1",
        SemanticStage::LexicalIndex,
    )
    .expect_err("a pointer whose checkpoint row bytes differ must fail closed");
    assert!(
        error
            .to_string()
            .contains("head bytes differ from its checkpoint row"),
        "unexpected ambiguity error: {error}"
    );
    rows.finish_owner().unwrap();
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn authoritative_state_rejects_completed_progress_without_stage_receipt() {
    // Kernel-seeded against the declared `SEMANTIC_SOURCE_PROGRESS` /
    // `SEMANTIC_STAGES` tables; see the note on
    // `checkpoint_head_does_not_promote_orphan_higher_count_rows`.
    let dir = tmp_dir("checkpoint-progress");
    let codes = open_store(&dir);
    let progress = SemanticSourceProgress {
        binding_id: "binding-a".to_string(),
        binding_digest: SemanticDigest::from_bytes([8; 32]),
        generation: 1,
        source_entity_id: "entity:a".to_string(),
        source_revision: "r1".to_string(),
        completed_stage: Some(SemanticStage::LexicalIndex),
        completed_receipt_digest: Some(SemanticDigest::from_bytes([1; 32])),
        superseded_by_revision: None,
        updated_at: "unix-ms:1".to_string(),
    };
    progress.validate().unwrap();

    let owner = codes.bind_for_write(1).unwrap();
    let version = eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "checkpoint-progress", version);
    let (write, begin) = codes.mutations.admit(&owner, &batch).unwrap();
    let eg_transaction::Begin::Apply {
        source_version: source_version_write,
    } = begin
    else {
        panic!("expected a fresh Begin::Apply, got a replay");
    };
    let rows = write.owner_rows(&owner, &batch).unwrap();
    let bytes = progress.to_canonical_cbor().unwrap();
    rows.open_table(SEMANTIC_SOURCE_PROGRESS)
        .unwrap()
        .insert(("native", "binding-a", 1, "entity:a"), bytes.as_slice())
        .unwrap();
    // `SEMANTIC_STAGES` is left EMPTY on purpose: the progress row above claims
    // a completed stage, and the missing stage receipt is what must be refused.
    let error = super::authoritative_generation_state(
        &rows.open_table(SEMANTIC_SOURCE_PROGRESS).unwrap(),
        &rows.open_table(SEMANTIC_STAGES).unwrap(),
        "native",
        "binding-a",
        1,
        SemanticDigest::from_bytes([8; 32]),
        "r1",
        SemanticStage::LexicalIndex,
        SemanticStage::LexicalIndex,
        None,
    )
    .expect_err("completed progress without a receipt must not complete a checkpoint");
    assert!(
        error
            .to_string()
            .contains("completed source progress without a stage receipt"),
        "unexpected authoritative-state error: {error}"
    );
    rows.finish_owner().unwrap();
    codes
        .mutations
        .finish(&write, &batch, None, 0, source_version_write)
        .unwrap();
    codes.mutations.commit(write, &batch).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
