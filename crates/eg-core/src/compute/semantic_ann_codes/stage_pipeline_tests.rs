//! Branch proofs for stage completion S1 through S5, driven through the real
//! lease and completion ports rather than seeded rows.
//!
//! One binding runs its source entity from S1 to a claimable S6: the SQL
//! manifest, graph projection manifest, lexical checkpoint and manifest,
//! vector with its coverage checkpoint, and ANN checkpoint and manifest are
//! each admitted exactly as an executor submits them. The refusal tests stop
//! the same pipeline at the stage whose rule they break.

use std::cell::Cell;
use std::path::PathBuf;

use eg_storage::SEMANTIC_SOURCE_PROGRESS;
use eg_transaction::OutboxClaimBudget;
use eg_types::contract::Nonce;
use eg_types::mutation_batch::MutationOutboxLease;
use eg_types::semantic_index::{
    SemanticAnnIndexManifest, SemanticBinding, SemanticBindingState, SemanticDeadLetter,
    SemanticDeadLetterDraft, SemanticDigest, SemanticExpectedEntity, SemanticGenerationAggregate,
    SemanticGenerationArtifact, SemanticGenerationCheckpoint, SemanticGenerationCheckpointDraft,
    SemanticGenerationCheckpointUpdate, SemanticGenerationDependency, SemanticGenerationMember,
    SemanticGraphProjectionManifest, SemanticGraphProjectionManifestDraft, SemanticIndexFilter,
    SemanticLexicalIndexManifest, SemanticSourceProgress, SemanticSqlSourceIdentity, SemanticStage,
    SemanticStageArtifact, SemanticStageIntent, SemanticStageIntentDraft, SemanticStageOutcome,
    SemanticStagePredecessor, SemanticStageReceipt, SemanticStageScope, SemanticStageTransition,
    SemanticVector,
};

use super::tests::{
    completed_sql_source_artifact, open_store, pending_binding, sql_source_identity, tmp_dir,
    BINDING, TENANT,
};
use super::{SemanticCodeError, SemanticCodeStore, SemanticMutationReceipt};

const REVISION: &str =
    "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:1";
const OTHER_REVISION: &str =
    "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:2";
const CONSUMER: &str = "semantic-pipeline";

type Completed = Result<SemanticMutationReceipt, SemanticCodeError>;

fn digest(seed: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([seed; 32])
}

fn receipt(
    intent: &SemanticStageIntent,
    output_digest: SemanticDigest,
    seed: u8,
) -> SemanticStageReceipt {
    SemanticStageReceipt {
        intent_digest: intent.intent_digest,
        output_digest,
        cursor: format!("cursor:{seed}"),
        completed_at: format!("2026-09-10T00:00:{seed:02}Z"),
        outcome: SemanticStageOutcome::Completed,
    }
}

fn transition(
    intent: &SemanticStageIntent,
    receipt: SemanticStageReceipt,
) -> SemanticStageTransition {
    SemanticStageTransition {
        intent: intent.clone(),
        receipt,
        generation_checkpoint: None,
    }
}

fn member_of(transition: &SemanticStageTransition) -> SemanticGenerationMember {
    let intent = &transition.intent;
    SemanticGenerationMember {
        source_entity_id: intent.scope.source_entity_id().unwrap().to_string(),
        source_revision: intent.source_revision.clone(),
        receipt_digest: transition.receipt.receipt_digest(),
        artifact_digest: transition.receipt.output_digest,
    }
}

/// The complete one-entity checkpoint of `stage` for the intent's generation.
fn one_entity_checkpoint(
    intent: &SemanticStageIntent,
    stage: SemanticStage,
    member: SemanticGenerationMember,
    dependency: SemanticGenerationDependency,
    completed_at: &str,
) -> SemanticGenerationCheckpoint {
    let expected = SemanticExpectedEntity {
        source_entity_id: member.source_entity_id.clone(),
        source_revision: member.source_revision.clone(),
    };
    let aggregate = SemanticGenerationAggregate::create(
        &intent.binding_id,
        intent.binding_digest,
        intent.generation,
        &intent.source_revision,
        stage,
        vec![expected],
        vec![member],
    )
    .unwrap();
    SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: intent.binding_id.clone(),
        binding_digest: intent.binding_digest,
        generation: intent.generation,
        source_revision: intent.source_revision.clone(),
        stage,
        artifact_digest: aggregate.aggregate_artifact_digest,
        aggregate,
        dependency,
        completed_at: completed_at.to_string(),
    })
    .unwrap()
}

/// The first checkpoint update an S3/S5 completion advances.
fn first_update(
    transition: &SemanticStageTransition,
    dependency: SemanticGenerationDependency,
) -> SemanticGenerationCheckpointUpdate {
    let member = member_of(transition);
    let successor = one_entity_checkpoint(
        &transition.intent,
        transition.intent.stage,
        member.clone(),
        dependency,
        &transition.receipt.completed_at,
    );
    SemanticGenerationCheckpointUpdate {
        expected_previous_checkpoint_digest: None,
        expected_previous_completed_count: 0,
        member,
        successor,
    }
}

fn successor_intent(
    intent: &SemanticStageIntent,
    stage: SemanticStage,
    scope: SemanticStageScope,
    predecessor: SemanticStagePredecessor,
    input_digest: SemanticDigest,
) -> SemanticStageIntent {
    SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: intent.binding_id.clone(),
        binding_digest: intent.binding_digest,
        generation: intent.generation,
        scope,
        source_revision: intent.source_revision.clone(),
        stage,
        predecessor,
        input_digest,
    })
    .unwrap()
}

fn refusal(result: Completed) -> String {
    result
        .expect_err("the stage completion must be refused")
        .to_string()
}

/// One building binding whose single SQL source entity walks the stages.
struct Pipeline {
    dir: PathBuf,
    codes: SemanticCodeStore,
    binding: SemanticBinding,
    identity: SemanticSqlSourceIdentity,
    clock: Cell<u64>,
}

impl Pipeline {
    fn start(tag: &str) -> Self {
        let dir = tmp_dir(tag);
        let codes = open_store(&dir);
        codes.store_binding(&pending_binding(REVISION), 1).unwrap();
        codes
            .transition_binding_operation(
                1,
                SemanticBindingState::Building,
                2,
                "actor-pipeline",
                "pipeline-build",
                Nonce::from_bytes([3; 32]),
            )
            .unwrap();
        codes.subscribe_stage_consumer(CONSUMER).unwrap();
        let binding = codes.read_binding().unwrap().unwrap();
        let identity = sql_source_identity(&binding, 90);
        Self {
            dir,
            codes,
            binding,
            identity,
            clock: Cell::new(10),
        }
    }

    fn now(&self) -> u64 {
        let now = self.clock.get() + 1;
        self.clock.set(now);
        now
    }

    fn entity(&self) -> String {
        self.identity.source_entity_id()
    }

    fn entity_scope(&self) -> SemanticStageScope {
        SemanticStageScope::Entity {
            source_entity_id: self.entity(),
        }
    }

    /// Claim the next stage intent and fence it as an executor would.
    fn claim(&self) -> (MutationOutboxLease, SemanticStageIntent) {
        let mut budget = OutboxClaimBudget::new(1, 60_000, self.now()).unwrap();
        let lease = self
            .codes
            .claim_stage_leases(CONSUMER, &mut budget)
            .unwrap()
            .claims
            .into_iter()
            .next()
            .expect("a stage intent is claimable");
        let intent = self
            .codes
            .validate_stage_lease(&lease, CONSUMER, self.now())
            .unwrap();
        (lease, intent)
    }

    fn complete(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticStageArtifact,
        successor: Option<&SemanticStageIntent>,
    ) -> Completed {
        self.codes
            .complete_stage(lease, transition, artifact, successor, self.now())
    }

    fn complete_generation(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticGenerationArtifact,
        successor: Option<&SemanticStageIntent>,
    ) -> Completed {
        self.codes
            .complete_generation_stage(lease, transition, artifact, successor, self.now())
    }

    fn enqueue_s1(&self) -> SemanticStageIntent {
        let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
            binding_id: self.binding.binding_id.clone(),
            binding_digest: self.binding.binding_digest,
            generation: self.binding.generation,
            scope: self.entity_scope(),
            source_revision: REVISION.to_string(),
            stage: SemanticStage::SourceCommit,
            predecessor: SemanticStagePredecessor::None,
            input_digest: digest(91),
        })
        .unwrap();
        self.codes
            .enqueue_stage_intent(&intent, self.now())
            .unwrap();
        intent
    }

    fn s1_completion(
        &self,
        intent: &SemanticStageIntent,
    ) -> (SemanticStageTransition, SemanticStageArtifact) {
        let completed = transition(intent, receipt(intent, intent.input_digest, 11));
        let artifact = completed_sql_source_artifact(
            &self.binding,
            &completed,
            &self.identity,
            &completed.receipt.completed_at,
            12,
        );
        (completed, artifact)
    }

    fn graph_completion(
        &self,
        intent: &SemanticStageIntent,
    ) -> (SemanticStageTransition, SemanticStageArtifact) {
        let completed = transition(intent, receipt(intent, digest(13), 13));
        let manifest =
            SemanticGraphProjectionManifest::create(SemanticGraphProjectionManifestDraft {
                binding_id: intent.binding_id.clone(),
                binding_digest: intent.binding_digest,
                generation: intent.generation,
                source_entity_id: self.entity(),
                source_revision: intent.source_revision.clone(),
                source_manifest_digest: intent.input_digest,
                projection_entity_digest: digest(14),
                projection_content_digest: completed.receipt.output_digest,
                completed_receipt_digest: completed.receipt.receipt_digest(),
                completed_at: completed.receipt.completed_at.clone(),
            })
            .unwrap();
        let artifact = SemanticStageArtifact::GraphProjectionManifest {
            manifest: Box::new(manifest),
        };
        (completed, artifact)
    }

    fn lexical_completion(
        &self,
        intent: &SemanticStageIntent,
    ) -> (SemanticStageTransition, SemanticGenerationArtifact) {
        let mut completed = transition(intent, receipt(intent, digest(15), 15));
        let update = first_update(&completed, SemanticGenerationDependency::None);
        let checkpoint = &update.successor;
        let manifest = SemanticLexicalIndexManifest {
            binding_id: intent.binding_id.clone(),
            binding_digest: intent.binding_digest,
            generation: intent.generation,
            source_revision: intent.source_revision.clone(),
            identity: self.binding.lexical_index_identity.clone(),
            artifact_digest: checkpoint.artifact_digest,
            row_count: 1,
            completed_receipt_digest: checkpoint.aggregate.aggregate_receipt_digest,
            completed_at: checkpoint.completed_at.clone(),
        };
        completed.generation_checkpoint = Some(Box::new(update));
        let artifact = SemanticGenerationArtifact::LexicalIndexManifest {
            manifest: Box::new(manifest),
        };
        (completed, artifact)
    }

    /// The S4 completion, its vector artifact, and the S5 successor carrying
    /// the vector coverage checkpoint S4 must produce.
    fn vector_completion(
        &self,
        intent: &SemanticStageIntent,
        lexical_digest: SemanticDigest,
    ) -> (
        SemanticStageTransition,
        SemanticStageArtifact,
        SemanticStageIntent,
    ) {
        let vector = SemanticVector::create(
            &self.binding,
            self.entity(),
            REVISION,
            vec![0.25, 0.5, 0.75],
        )
        .unwrap();
        let completed = transition(intent, receipt(intent, vector.values_digest, 16));
        let coverage = one_entity_checkpoint(
            intent,
            SemanticStage::Vector,
            member_of(&completed),
            SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::LexicalIndex,
                checkpoint_digest: lexical_digest,
            },
            &completed.receipt.completed_at,
        );
        let s5 = successor_intent(
            intent,
            SemanticStage::AnnIndex,
            self.entity_scope(),
            SemanticStagePredecessor::GenerationCoverage {
                checkpoint: Box::new(coverage),
            },
            completed.receipt.output_digest,
        );
        let artifact = SemanticStageArtifact::Vector {
            vector: Box::new(vector),
        };
        (completed, artifact, s5)
    }

    /// The S5 completion, its ANN manifest, and the S6 activation successor.
    fn ann_completion(
        &self,
        intent: &SemanticStageIntent,
        lexical_digest: SemanticDigest,
    ) -> (
        SemanticStageTransition,
        SemanticGenerationArtifact,
        SemanticStageIntent,
    ) {
        let SemanticStagePredecessor::GenerationCoverage {
            checkpoint: coverage,
        } = &intent.predecessor
        else {
            panic!("S5 runs on the vector coverage checkpoint");
        };
        let mut completed = transition(intent, receipt(intent, digest(17), 17));
        let update = first_update(
            &completed,
            SemanticGenerationDependency::Checkpoint {
                stage: SemanticStage::Vector,
                checkpoint_digest: coverage.checkpoint_digest,
            },
        );
        let checkpoint = &update.successor;
        let manifest = SemanticAnnIndexManifest {
            binding_id: intent.binding_id.clone(),
            binding_digest: intent.binding_digest,
            generation: intent.generation,
            source_revision: intent.source_revision.clone(),
            identity: self.binding.ann_index_identity.clone(),
            artifact_digest: checkpoint.artifact_digest,
            vector_count: 1,
            completed_receipt_digest: checkpoint.aggregate.aggregate_receipt_digest,
            completed_at: checkpoint.completed_at.clone(),
        };
        let s6 = successor_intent(
            intent,
            SemanticStage::ReconcileAndActivate,
            SemanticStageScope::Generation,
            SemanticStagePredecessor::Activation {
                lexical_checkpoint_digest: lexical_digest,
                ann_checkpoint_digest: checkpoint.checkpoint_digest,
            },
            completed.receipt.output_digest,
        );
        completed.generation_checkpoint = Some(Box::new(update));
        let artifact = SemanticGenerationArtifact::AnnIndexManifest {
            manifest: Box::new(manifest),
        };
        (completed, artifact, s6)
    }

    fn run_s1(&self) {
        self.enqueue_s1();
        let (lease, intent) = self.claim();
        let (completed, artifact) = self.s1_completion(&intent);
        self.complete(&lease, &completed, &artifact, None).unwrap();
    }

    fn run_s2(&self) {
        let (lease, intent) = self.claim();
        let (completed, artifact) = self.graph_completion(&intent);
        self.complete(&lease, &completed, &artifact, None).unwrap();
    }

    /// Run S1 through S3; the lexical checkpoint digest S4 depends on.
    fn run_through_lexical(&self) -> SemanticDigest {
        self.run_s1();
        self.run_s2();
        let (lease, intent) = self.claim();
        let (completed, artifact) = self.lexical_completion(&intent);
        self.complete_generation(&lease, &completed, &artifact, None)
            .unwrap();
        completed
            .generation_checkpoint
            .unwrap()
            .successor
            .checkpoint_digest
    }

    fn progress(&self) -> SemanticSourceProgress {
        let read = self.codes.door.serving_read().unwrap();
        let rows = read.open_owner_table(SEMANTIC_SOURCE_PROGRESS).unwrap();
        let entity = self.entity();
        let raw = rows
            .get((TENANT, BINDING, 1, entity.as_str()))
            .unwrap()
            .unwrap();
        SemanticSourceProgress::from_canonical_cbor(raw.value()).unwrap()
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn a_source_generation_runs_s1_through_s5_to_a_claimable_s6() {
    let pipeline = Pipeline::start("pipeline-s1-s5");
    let lexical = pipeline.run_through_lexical();
    assert_eq!(
        pipeline.progress().completed_stage,
        Some(SemanticStage::LexicalIndex)
    );

    let (lease, intent) = pipeline.claim();
    assert_eq!(
        intent.predecessor,
        SemanticStagePredecessor::GenerationCheckpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest: lexical,
        },
        "S3 publishes the S4 intent that names its lexical checkpoint"
    );
    let (completed, artifact, s5) = pipeline.vector_completion(&intent, lexical);
    let vector = pipeline
        .complete(&lease, &completed, &artifact, Some(&s5))
        .unwrap();
    assert!(!vector.replayed);

    let (lease, intent) = pipeline.claim();
    assert_eq!(intent, s5, "S4 publishes exactly its explicit S5 successor");
    let (completed, artifact, s6) = pipeline.ann_completion(&intent, lexical);
    pipeline
        .complete_generation(&lease, &completed, &artifact, Some(&s6))
        .unwrap();
    assert_eq!(
        pipeline.progress().completed_stage,
        Some(SemanticStage::AnnIndex)
    );

    let (_, intent) = pipeline.claim();
    assert_eq!(
        intent, s6,
        "S5 publishes the generation-scoped S6 activation"
    );
    assert!(pipeline
        .codes
        .source_entity_seen_at_revision(1, &pipeline.entity(), REVISION)
        .unwrap());
}

#[test]
fn generation_manifests_complete_only_s3_or_s5_with_their_checkpoint() {
    let pipeline = Pipeline::start("pipeline-generation-manifest");
    pipeline.run_s1();
    let (lease, intent) = pipeline.claim();
    let (graph, _) = pipeline.graph_completion(&intent);
    let (lexical_shape, lexical_artifact) = {
        let s3_shape = successor_intent(
            &intent,
            SemanticStage::LexicalIndex,
            pipeline.entity_scope(),
            SemanticStagePredecessor::EntityReceipt {
                stage: SemanticStage::GraphProjection,
                receipt_digest: graph.receipt.receipt_digest(),
            },
            graph.receipt.output_digest,
        );
        pipeline.lexical_completion(&s3_shape)
    };
    let not_s3 = pipeline.complete_generation(&lease, &graph, &lexical_artifact, None);
    assert!(refusal(not_s3).contains("only completed by S3 or S5"));
    let mut without_checkpoint = lexical_shape.clone();
    without_checkpoint.generation_checkpoint = None;
    let bare = pipeline.complete_generation(&lease, &without_checkpoint, &lexical_artifact, None);
    assert!(refusal(bare).contains("requires its exact checkpoint"));

    let (graph, artifact) = pipeline.graph_completion(&intent);
    pipeline.complete(&lease, &graph, &artifact, None).unwrap();
    assert_eq!(
        pipeline.progress().completed_stage,
        Some(SemanticStage::GraphProjection)
    );
}

#[test]
fn a_deferred_stage_releases_its_lease_and_publishes_nothing() {
    let pipeline = Pipeline::start("pipeline-deferred");
    let s1 = pipeline.enqueue_s1();
    let (lease, intent) = pipeline.claim();

    let mut diverged = transition(&intent, receipt(&intent, digest(99), 20));
    diverged.receipt.outcome = SemanticStageOutcome::Completed;
    let mismatch = pipeline.complete(&lease, &diverged, &SemanticStageArtifact::None, None);
    assert!(refusal(mismatch).contains("must equal its admitted input"));

    let mut deferred = transition(&intent, receipt(&intent, intent.input_digest, 21));
    deferred.receipt.outcome = SemanticStageOutcome::DeferredBackpressured;
    let with_successor =
        pipeline.complete(&lease, &deferred, &SemanticStageArtifact::None, Some(&s1));
    assert!(refusal(with_successor).contains("deferred semantic stage cannot publish a successor"));
    let pending = pipeline.complete(&lease, &deferred, &SemanticStageArtifact::None, None);
    assert!(refusal(pending).contains("remains pending and unacknowledged"));

    let (lease, reclaimed) = pipeline.claim();
    assert_eq!(
        reclaimed, s1,
        "a deferred lease is released back to the outbox"
    );
    let mut parked = deferred.clone();
    parked.receipt.outcome = SemanticStageOutcome::ParkedAwaitingPredecessor;
    let parked = pipeline.complete(&lease, &parked, &SemanticStageArtifact::None, None);
    assert!(refusal(parked).contains("remains pending and unacknowledged"));
    assert!(pipeline.progress().completed_stage.is_none());

    let (lease, intent) = pipeline.claim();
    let (completed, artifact) = pipeline.s1_completion(&intent);
    pipeline
        .complete(&lease, &completed, &artifact, None)
        .unwrap();
    assert_eq!(
        pipeline.progress().completed_stage,
        Some(SemanticStage::SourceCommit)
    );
}

#[test]
fn completed_stages_refuse_a_successor_that_is_not_their_exact_proof() {
    let pipeline = Pipeline::start("pipeline-successor-proof");
    let s1 = pipeline.enqueue_s1();
    let (lease, intent) = pipeline.claim();
    let (completed, artifact) = pipeline.s1_completion(&intent);
    pipeline
        .complete(&lease, &completed, &artifact, None)
        .unwrap();

    let (lease, intent) = pipeline.claim();
    let (graph, graph_artifact) = pipeline.graph_completion(&intent);
    let wrong_proof = successor_intent(
        &intent,
        SemanticStage::LexicalIndex,
        pipeline.entity_scope(),
        SemanticStagePredecessor::EntityReceipt {
            stage: SemanticStage::GraphProjection,
            receipt_digest: digest(1),
        },
        graph.receipt.output_digest,
    );
    let refused = refusal(pipeline.complete(&lease, &graph, &graph_artifact, Some(&wrong_proof)));
    assert!(
        refused.contains("does not carry the exact stage receipt proof"),
        "{refused}"
    );
    let stale_input = successor_intent(
        &intent,
        SemanticStage::LexicalIndex,
        pipeline.entity_scope(),
        SemanticStagePredecessor::EntityReceipt {
            stage: SemanticStage::GraphProjection,
            receipt_digest: graph.receipt.receipt_digest(),
        },
        digest(2),
    );
    let refused = refusal(pipeline.complete(&lease, &graph, &graph_artifact, Some(&stale_input)));
    assert!(
        refused.contains("coordinates or predecessor input are stale"),
        "{refused}"
    );

    let (rejected, rejected_artifact) = dead_letter_completion(&intent);
    let refused =
        refusal(pipeline.complete(&lease, &rejected, &rejected_artifact, Some(&stale_input)));
    assert!(
        refused.contains("non-terminal semantic stage outcome cannot publish"),
        "{refused}"
    );
    let (other_intent, other_artifact) = dead_letter_completion(&s1);
    let refused = refusal(pipeline.complete(&lease, &other_intent, &other_artifact, None));
    assert!(
        refused.contains("does not equal the leased canonical intent"),
        "{refused}"
    );

    pipeline
        .complete(&lease, &graph, &graph_artifact, None)
        .unwrap();
    assert_eq!(
        pipeline.progress().completed_stage,
        Some(SemanticStage::GraphProjection)
    );
}

fn dead_letter_completion(
    intent: &SemanticStageIntent,
) -> (SemanticStageTransition, SemanticStageArtifact) {
    let completed_at = "2026-09-10T00:00:30Z".to_string();
    let dead_letter = SemanticDeadLetter::create(SemanticDeadLetterDraft {
        intent: intent.clone(),
        attempt: 1,
        error_code: "projection_rejected".to_string(),
        reason: "branch proof".to_string(),
        failed_at: completed_at.clone(),
    })
    .unwrap();
    let mut rejected = transition(intent, receipt(intent, dead_letter.failure_digest, 30));
    rejected.receipt.outcome = SemanticStageOutcome::RejectedDeadLetter;
    rejected.receipt.completed_at = completed_at;
    let artifact = SemanticStageArtifact::DeadLetter {
        dead_letter: Box::new(dead_letter),
    };
    (rejected, artifact)
}

#[test]
fn vector_and_ann_stages_require_their_explicit_successor_proofs() {
    let pipeline = Pipeline::start("pipeline-explicit-successors");
    let lexical = pipeline.run_through_lexical();

    let (lease, intent) = pipeline.claim();
    let (vector, vector_artifact, s5) = pipeline.vector_completion(&intent, lexical);
    let refused = refusal(pipeline.complete(&lease, &vector, &vector_artifact, None));
    assert!(
        refused.contains("requires an explicit successor proof"),
        "{refused}"
    );
    // A well-formed successor of the wrong stage: S4 again, not S5.
    let uncovered = successor_intent(
        &intent,
        SemanticStage::Vector,
        pipeline.entity_scope(),
        SemanticStagePredecessor::GenerationCheckpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest: lexical,
        },
        vector.receipt.output_digest,
    );
    let refused = refusal(pipeline.complete(&lease, &vector, &vector_artifact, Some(&uncovered)));
    assert!(
        refused.contains("complete S5 generation-coverage intent"),
        "{refused}"
    );
    pipeline
        .complete(&lease, &vector, &vector_artifact, Some(&s5))
        .unwrap();

    let (lease, intent) = pipeline.claim();
    let (ann, ann_artifact, s6) = pipeline.ann_completion(&intent, lexical);
    let not_activation = successor_intent(
        &intent,
        SemanticStage::Vector,
        pipeline.entity_scope(),
        SemanticStagePredecessor::GenerationCheckpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest: lexical,
        },
        ann.receipt.output_digest,
    );
    let refused =
        refusal(pipeline.complete_generation(&lease, &ann, &ann_artifact, Some(&not_activation)));
    assert!(
        refused.contains("generation-scoped S6 activation intent"),
        "{refused}"
    );
    pipeline
        .complete_generation(&lease, &ann, &ann_artifact, Some(&s6))
        .unwrap();
    assert_eq!(
        pipeline.progress().completed_stage,
        Some(SemanticStage::AnnIndex)
    );
}

fn filter(source_entity_ids: Vec<String>, revision: Option<&str>) -> SemanticIndexFilter {
    SemanticIndexFilter {
        source_entity_ids,
        required_source_revision: revision.map(str::to_string),
        max_results: 10,
    }
}

#[test]
fn a_binding_filter_is_proved_from_durable_source_progress() {
    let empty_dir = tmp_dir("pipeline-filter-empty");
    let empty = open_store(&empty_dir);
    assert!(!empty
        .binding_matches_filter(&filter(Vec::new(), None))
        .unwrap());
    drop(empty);
    let _ = std::fs::remove_dir_all(&empty_dir);

    let pipeline = Pipeline::start("pipeline-filter");
    let codes = &pipeline.codes;
    let entity = vec![pipeline.entity()];
    assert!(!codes
        .binding_matches_filter(&filter(entity.clone(), None))
        .unwrap());
    pipeline.run_s1();

    assert!(codes
        .binding_matches_filter(&filter(Vec::new(), None))
        .unwrap());
    assert!(codes
        .binding_matches_filter(&filter(Vec::new(), Some(REVISION)))
        .unwrap());
    assert!(!codes
        .binding_matches_filter(&filter(Vec::new(), Some(OTHER_REVISION)))
        .unwrap());
    assert!(codes
        .binding_matches_filter(&filter(entity.clone(), None))
        .unwrap());
    assert!(codes
        .binding_matches_filter(&filter(entity.clone(), Some(REVISION)))
        .unwrap());
    assert!(!codes
        .binding_matches_filter(&filter(entity.clone(), Some(OTHER_REVISION)))
        .unwrap());

    let unbounded = SemanticIndexFilter {
        max_results: 0,
        ..filter(entity, None)
    };
    assert!(codes.binding_matches_filter(&unbounded).is_err());
}
