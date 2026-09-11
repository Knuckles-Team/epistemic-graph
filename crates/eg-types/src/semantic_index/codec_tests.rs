use sha2::{Digest, Sha256};

use super::codec::encode_test_envelope;
use super::digest::cbor;
use super::*;

fn digest(byte: u8) -> SemanticDigest {
    SemanticDigest::from_bytes([byte; 32])
}

fn binding() -> SemanticBinding {
    SemanticBinding::create(SemanticBindingDraft {
        binding_id: "binding:articles-body".into(),
        tenant_id: "tenant-a".into(),
        actor_scope: "semantic:index-maintainer".into(),
        effective_actor_scope: "semantic:agent:index-maintainer".into(),
        purpose_id: "retrieval".into(),
        policy: SemanticPolicyComponents {
            rbac_policy_revision: 1,
            rbac_policy_digest: "sha256:rbac".into(),
            row_policy_revision: 1,
            row_policy_digest: "sha256:row-policy".into(),
            source_acl_revision: 7,
            source_acl_digest: "sha256:sql-acl-state".into(),
        },
        source_selector: SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
            catalog_id: SEMANTIC_SQL_CATALOG_ID.into(),
            schema_id: "public".into(),
            table_id: "articles".into(),
            column_id: "body".into(),
        }),
        source_schema_digest: "sha256:schema".into(),
        source_revision: "42".into(),
        source_field_set_digest: "sha256:field-set".into(),
        dimension: 3,
        metric: SemanticVectorMetric::Cosine,
        model: SemanticModelIdentity {
            model_id: "embedding-model".into(),
            model_revision: "revision-1".into(),
            preprocess_digest: "sha256:preprocess".into(),
            model_digest: "sha256:model".into(),
        },
        generation: 1,
        maintenance_policy_id: "semantic-maintenance".into(),
        lexical_index: SemanticLexicalIndexSpec {
            analyzer_id: "standard".into(),
            analyzer_revision: "1".into(),
            analyzer_config_digest: "sha256:analyzer".into(),
        },
        ann_index: SemanticAnnIndexSpec {
            method: SemanticAnnIndexMethod::IvfPq,
            parameters_digest: "sha256:ann-parameters".into(),
        },
        created_at: "2026-09-04T00:00:00Z".into(),
    })
    .unwrap()
}

fn lexical_checkpoint(binding: &SemanticBinding) -> SemanticGenerationCheckpoint {
    let aggregate = SemanticGenerationAggregate::create(
        &binding.binding_id,
        binding.binding_digest,
        binding.generation,
        "42",
        SemanticStage::LexicalIndex,
        vec![
            SemanticExpectedEntity {
                source_entity_id: "article:1".into(),
                source_revision: "42".into(),
            },
            SemanticExpectedEntity {
                source_entity_id: "article:2".into(),
                source_revision: "42".into(),
            },
        ],
        vec![
            SemanticGenerationMember {
                source_entity_id: "article:1".into(),
                source_revision: "42".into(),
                receipt_digest: digest(8),
                artifact_digest: digest(9),
            },
            SemanticGenerationMember {
                source_entity_id: "article:2".into(),
                source_revision: "42".into(),
                receipt_digest: digest(10),
                artifact_digest: digest(11),
            },
        ],
    )
    .unwrap();
    SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_revision: "42".into(),
        stage: SemanticStage::LexicalIndex,
        artifact_digest: aggregate.aggregate_artifact_digest,
        aggregate,
        dependency: SemanticGenerationDependency::None,
        completed_at: "2026-09-04T00:00:30Z".into(),
    })
    .unwrap()
}

fn stage_transition(binding: &SemanticBinding) -> SemanticStageTransition {
    let stage = SemanticStage::Vector;
    let checkpoint = lexical_checkpoint(binding);
    let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: "article:1".into(),
        },
        source_revision: "42".into(),
        stage,
        predecessor: SemanticStagePredecessor::GenerationCheckpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest: checkpoint.checkpoint_digest,
        },
        input_digest: digest(11),
    })
    .unwrap();
    SemanticStageTransition {
        intent: intent.clone(),
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: digest(12),
            cursor: "cursor:42".into(),
            completed_at: "2026-09-04T00:01:00Z".into(),
            outcome: SemanticStageOutcome::Completed,
        },
        generation_checkpoint: None,
    }
}

fn checkpoint_transition(binding: &SemanticBinding) -> SemanticStageTransition {
    let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: "article:1".into(),
        },
        source_revision: binding.source_revision.clone(),
        stage: SemanticStage::LexicalIndex,
        predecessor: SemanticStagePredecessor::EntityReceipt {
            stage: SemanticStage::GraphProjection,
            receipt_digest: digest(40),
        },
        input_digest: digest(41),
    })
    .unwrap();
    let receipt = SemanticStageReceipt {
        intent_digest: intent.intent_digest,
        output_digest: digest(42),
        cursor: "lexical:42".into(),
        completed_at: "2026-09-04T00:00:30Z".into(),
        outcome: SemanticStageOutcome::Completed,
    };
    let member = SemanticGenerationMember {
        source_entity_id: "article:1".into(),
        source_revision: binding.source_revision.clone(),
        receipt_digest: receipt.receipt_digest(),
        artifact_digest: receipt.output_digest,
    };
    let aggregate = SemanticGenerationAggregate::create(
        &binding.binding_id,
        binding.binding_digest,
        binding.generation,
        &binding.source_revision,
        SemanticStage::LexicalIndex,
        ["article:1", "article:2"]
            .into_iter()
            .map(|source_entity_id| SemanticExpectedEntity {
                source_entity_id: source_entity_id.into(),
                source_revision: binding.source_revision.clone(),
            })
            .collect(),
        vec![member.clone()],
    )
    .unwrap();
    let successor = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_revision: binding.source_revision.clone(),
        stage: SemanticStage::LexicalIndex,
        artifact_digest: aggregate.aggregate_artifact_digest,
        aggregate,
        dependency: SemanticGenerationDependency::None,
        completed_at: receipt.completed_at.clone(),
    })
    .unwrap();
    SemanticStageTransition {
        intent,
        receipt,
        generation_checkpoint: Some(Box::new(SemanticGenerationCheckpointUpdate {
            expected_previous_checkpoint_digest: None,
            expected_previous_completed_count: 0,
            member,
            successor,
        })),
    }
}

fn manifest(binding: &SemanticBinding) -> SemanticIndexManifest {
    SemanticIndexManifest {
        lexical: SemanticLexicalIndexManifest {
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            source_revision: binding.source_revision.clone(),
            identity: binding.lexical_index_identity.clone(),
            artifact_digest: digest(20),
            row_count: 7,
            completed_receipt_digest: digest(21),
            completed_at: "2026-09-04T00:02:00Z".into(),
        },
        ann: SemanticAnnIndexManifest {
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            source_revision: binding.source_revision.clone(),
            identity: binding.ann_index_identity.clone(),
            artifact_digest: digest(22),
            vector_count: 7,
            completed_receipt_digest: digest(23),
            completed_at: "2026-09-04T00:03:00Z".into(),
        },
    }
}

fn tombstone() -> SemanticTombstone {
    let tombstone = SemanticTombstone::create(SemanticTombstoneDraft {
        tenant_id: "tenant-a".into(),
        binding_id: "binding:articles-body".into(),
        binding_digest: digest(30),
        generation: 1,
        deleted_at: "2026-09-04T00:06:00Z".into(),
    })
    .unwrap();
    assert_eq!(
        tombstone.tombstone_receipt_digest.to_hex(),
        "1c99b74e6425625cd9289a63ffbc4ef36be5cbbf7610c046a2e8aa0a97d2c821"
    );
    tombstone
}

fn source_records(
    binding: &SemanticBinding,
) -> (
    SemanticStageTransition,
    SemanticAuthorizationReceipt,
    SemanticSqlSourceManifest,
    SemanticGraphProjectionManifest,
) {
    let selector = match &binding.source_selector {
        SemanticSourceSelector::SqlColumnRef(selector) => selector,
        _ => unreachable!(),
    };
    let identity =
        SemanticSqlSourceIdentity::create(selector, binding.tenant_id.clone(), digest(30));
    let source_entity_id = identity.source_entity_id();
    let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        },
        source_revision: binding.source_revision.clone(),
        stage: SemanticStage::SourceCommit,
        predecessor: SemanticStagePredecessor::None,
        input_digest: digest(31),
    })
    .unwrap();
    let transition = SemanticStageTransition {
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: digest(32),
            cursor: "source:42".into(),
            completed_at: "2026-09-04T00:00:10Z".into(),
            outcome: SemanticStageOutcome::Completed,
        },
        intent,
        generation_checkpoint: None,
    };
    let authorization = SemanticAuthorizationReceipt::create(SemanticAuthorizationReceiptDraft {
        tenant_id: binding.tenant_id.clone(),
        actor_scope: binding.actor_scope.clone(),
        effective_actor_scope: binding.effective_actor_scope.clone(),
        purpose_id: binding.purpose_id.clone(),
        policy_identity: binding.policy_identity.clone(),
        policy_decision_digest: "sha256:sql-acl-decision".into(),
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: transition.intent.scope.clone(),
        source_revision: binding.source_revision.clone(),
        authorized_at: "2026-09-04T00:00:05Z".into(),
    })
    .unwrap();
    let source = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_identity: identity,
        source_revision: binding.source_revision.clone(),
        source_content_digest: transition.receipt.output_digest,
        source_schema_revision: 42,
        source_schema_digest: binding.source_schema_digest.clone(),
        source_field_set_digest: binding.source_field_set_digest.clone(),
        source_acl_revision: 7,
        source_acl_digest: "sha256:sql-acl-state".into(),
        authorization_receipt_digest: authorization.authorization_receipt_digest,
        completed_receipt_digest: transition.receipt.receipt_digest(),
        completed_at: transition.receipt.completed_at.clone(),
    })
    .unwrap();
    let graph = SemanticGraphProjectionManifest::create(SemanticGraphProjectionManifestDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_entity_id,
        source_revision: binding.source_revision.clone(),
        source_manifest_digest: source.manifest_digest,
        projection_entity_digest: digest(33),
        projection_content_digest: digest(34),
        completed_receipt_digest: digest(35),
        completed_at: "2026-09-04T00:00:20Z".into(),
    })
    .unwrap();
    (transition, authorization, source, graph)
}

fn rebuild_source_manifest(
    source: &SemanticSqlSourceManifest,
    source_schema_revision: u64,
    source_acl_revision: u64,
    source_acl_digest: &str,
) -> SemanticSqlSourceManifest {
    SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: source.binding_id.clone(),
        binding_digest: source.binding_digest,
        generation: source.generation,
        source_identity: source.source_identity.clone(),
        source_revision: source.source_revision.clone(),
        source_content_digest: source.source_content_digest,
        source_schema_revision,
        source_schema_digest: source.source_schema_digest.clone(),
        source_field_set_digest: source.source_field_set_digest.clone(),
        source_acl_revision,
        source_acl_digest: source_acl_digest.into(),
        authorization_receipt_digest: source.authorization_receipt_digest,
        completed_receipt_digest: source.completed_receipt_digest,
        completed_at: source.completed_at.clone(),
    })
    .unwrap()
}

fn lineage(binding: &SemanticBinding) -> SemanticLineage {
    SemanticLineage::create(SemanticLineageDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_entity_id: "article:1".into(),
        source_revision: binding.source_revision.clone(),
        purpose_id: binding.purpose_id.clone(),
        policy_digest: binding.policy_digest.clone(),
        model_digest: binding.model_digest.clone(),
        preprocess_digest: binding.preprocess_digest.clone(),
        lexical_index_digest: binding.lexical_index_identity.lexical_index_digest,
        ann_index_digest: binding.ann_index_identity.ann_index_digest,
        generation_checkpoint_digest: lexical_checkpoint(binding).checkpoint_digest,
        authorization_receipt_digest: digest(36),
    })
    .unwrap()
}

fn encoded_sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

macro_rules! assert_golden_round_trip {
    ($value:expr, $record:ty, $golden:expr) => {{
        let value = $value;
        let bytes = value.to_canonical_cbor().unwrap();
        assert_eq!(encoded_sha256(&bytes), $golden);
        assert_eq!(<$record>::from_canonical_cbor(&bytes).unwrap(), value);
    }};
}

#[test]
fn every_persisted_record_has_a_golden_canonical_round_trip() {
    let pending = binding();
    let source_dirty = SemanticSourceDirtyIntent::new(digest(1), digest(2));
    let vector = SemanticVector::create(&pending, "article:1", "42", vec![1.0, 0.5, 0.0]).unwrap();
    let stage = stage_transition(&pending);
    let checkpoint_stage = checkpoint_transition(&pending);
    let state = SemanticBindingStateTransition::create(
        &pending,
        SemanticBindingState::Building,
        "maintenance-started",
    )
    .unwrap();
    let mutation = SemanticIndexMutation::StoreBinding {
        binding: Box::new(pending.clone()),
    };
    let progress = SemanticSourceProgress {
        binding_id: pending.binding_id.clone(),
        binding_digest: pending.binding_digest,
        generation: pending.generation,
        source_entity_id: "article:1".into(),
        source_revision: "42".into(),
        completed_stage: Some(SemanticStage::Vector),
        completed_receipt_digest: Some(digest(12)),
        superseded_by_revision: None,
        updated_at: "2026-09-04T00:01:00Z".into(),
    };
    let manifest = manifest(&pending);
    let pointer = SemanticActivePointer {
        tenant_id: pending.tenant_id.clone(),
        binding_id: pending.binding_id.clone(),
        binding_digest: pending.binding_digest,
        generation: pending.generation,
        source_revision: pending.source_revision.clone(),
        vector_target_id: pending.vector_target_id.clone(),
        lexical_index_identity: pending.lexical_index_identity.clone(),
        ann_index_identity: pending.ann_index_identity.clone(),
        composite_policy_digest: pending.policy_digest.clone(),
        activation_receipt_digest: digest(24),
        activated_at: "2026-09-04T00:04:00Z".into(),
    };
    let dead_letter = SemanticDeadLetter::create(SemanticDeadLetterDraft {
        intent: stage.intent.clone(),
        attempt: 3,
        error_code: "model-unavailable".into(),
        reason: "retry-exhausted".into(),
        failed_at: "2026-09-04T00:05:00Z".into(),
    })
    .unwrap();
    let tombstone = tombstone();
    let (_, authorization, source, graph) = source_records(&pending);
    let lineage = lineage(&pending);

    assert_golden_round_trip!(
        pending.clone(),
        SemanticBinding,
        "5c63cdb1c473ac09b8badfeb8ecf5e8f6e4cf1cd3fb196035bf333e0c7c354bb"
    );
    assert_golden_round_trip!(
        source_dirty,
        SemanticSourceDirtyIntent,
        "20fae4c3a07779d3b1f5774f80b8e3a36dffa22f7c5593bbc6beb4bd114e1ef0"
    );
    assert_golden_round_trip!(
        vector,
        SemanticVector,
        "85a660b36db562c2e165429ac006fc7649148ab315c17d02ef59528b41623279"
    );
    assert_golden_round_trip!(
        stage.intent.clone(),
        SemanticStageIntent,
        "62b60f5fa9f65d609a5633d9118f27f55df7e7cb79222b8f0e17ac84875e4457"
    );
    assert_golden_round_trip!(
        lexical_checkpoint(&pending),
        SemanticGenerationCheckpoint,
        "a95de0ff0c1be12cd45244c9eaf4d3dc92e518da6a8d656ddd3234de37e7ce4d"
    );
    assert_golden_round_trip!(
        stage,
        SemanticStageTransition,
        "dd2a1ff40e9230f3d2c551d34b17eefb6ff2e7025f484fe625c199fe6eb804e3"
    );
    assert_golden_round_trip!(
        checkpoint_stage,
        SemanticStageTransition,
        "3b3ab74fbe66e32b352f4bab9b88a1cc6270ad73fbdf6315048422394b1f5860"
    );
    assert_golden_round_trip!(
        state,
        SemanticBindingStateTransition,
        "99897e9de685dc58b1367e55bb57215f91ae2f4801660a9b166b1d5ce2cb14b0"
    );
    assert_golden_round_trip!(
        mutation,
        SemanticIndexMutation,
        "be3a965971ef292d91029f8fdf68818b3adc359fbfff5fde7823ccaf95d20ba7"
    );
    assert_golden_round_trip!(
        pointer,
        SemanticActivePointer,
        "c42369ed5b259b3d8d1101ad2834637d278a10094102c59f86133db874789969"
    );
    assert_golden_round_trip!(
        progress,
        SemanticSourceProgress,
        "50a2939193a8273e9a94081ca2f3ef19448dd9a342fac6793862a2070471dea6"
    );
    assert_golden_round_trip!(
        manifest.lexical.clone(),
        SemanticLexicalIndexManifest,
        "6149b1b9ec92b3c551810f8e19224bbb8ae5132a2c66331f4ec5a2507fc95df3"
    );
    assert_golden_round_trip!(
        manifest.ann.clone(),
        SemanticAnnIndexManifest,
        "846d80dc861cc19aca537ea8801fd5f638eac2a6b77882c0d3ed9a1945d2f862"
    );
    assert_golden_round_trip!(
        manifest,
        SemanticIndexManifest,
        "1af61fdee8b72c03b5dce6d822a89eb2fca37e11e27bfa741d6f3f940924bd5c"
    );
    assert_golden_round_trip!(
        dead_letter,
        SemanticDeadLetter,
        "8cb9d0b2f16a42109af690af554ed6ad299d2c367a693e0a4ca29ce8345d5053"
    );
    assert_golden_round_trip!(
        tombstone,
        SemanticTombstone,
        "f1f22dba48763347093bc135bcb69734fffe30202503324c23579d79ca926a72"
    );
    assert_golden_round_trip!(
        authorization,
        SemanticAuthorizationReceipt,
        "f6c59060bd99fc43b5f765b6ee8d58e1fa1be4a0109fad1a92fb251db36f4999"
    );
    assert_golden_round_trip!(
        source,
        SemanticSqlSourceManifest,
        "6fd891d2cef23ee0c6a92d67748e992a2fba3c29cda571f032372d363ebea22d"
    );
    assert_golden_round_trip!(
        graph,
        SemanticGraphProjectionManifest,
        "6ecc551a0d9fe04239ea3fde56f6e90753ffa48acc0c7c81484a714e462ec808"
    );
    assert_golden_round_trip!(
        lineage,
        SemanticLineage,
        "53ba08d71c1eb170ffa6903066ffef2b9b25f880639d925ddb6e8aaf6636195d"
    );
}

#[test]
fn source_dirty_intent_rejects_unknown_missing_and_wrong_schema_records() {
    let intent = SemanticSourceDirtyIntent::new(digest(1), digest(2));

    let mut unknown = serde_json::to_value(&intent).unwrap();
    unknown["source_revision"] = serde_json::json!(7);
    let unknown = encode_test_envelope(SEMANTIC_SOURCE_DIRTY_INTENT_SCHEMA, unknown);
    assert!(SemanticSourceDirtyIntent::from_canonical_cbor(&unknown).is_err());

    let mut missing = serde_json::to_value(&intent).unwrap();
    missing
        .as_object_mut()
        .unwrap()
        .remove("source_scope_digest");
    let missing = encode_test_envelope(SEMANTIC_SOURCE_DIRTY_INTENT_SCHEMA, missing);
    assert!(SemanticSourceDirtyIntent::from_canonical_cbor(&missing).is_err());

    let wrong_schema = encode_test_envelope(
        "semantic-source-dirty-intent/v2",
        serde_json::to_value(intent).unwrap(),
    );
    assert!(SemanticSourceDirtyIntent::from_canonical_cbor(&wrong_schema).is_err());
}

#[test]
fn decoder_rejects_noncanonical_duplicate_unknown_and_malformed_records() {
    let binding = binding();
    let canonical = binding.to_canonical_cbor().unwrap();

    let mut trailing = canonical.clone();
    trailing.push(0);
    assert!(SemanticBinding::from_canonical_cbor(&trailing).is_err());

    let mut non_shortest = vec![0xb8, 0x02];
    non_shortest.extend_from_slice(&canonical[1..]);
    assert_eq!(
        SemanticBinding::from_canonical_cbor(&non_shortest),
        Err(SemanticIndexError::NonCanonicalRecord)
    );

    let mut duplicate = canonical.clone();
    duplicate[0] = 0xa3;
    duplicate.extend(cbor::text("schema"));
    duplicate.extend(cbor::text(SEMANTIC_BINDING_SCHEMA));
    assert!(SemanticBinding::from_canonical_cbor(&duplicate).is_err());

    let mut unknown = serde_json::to_value(&binding).unwrap();
    unknown["unknown_field"] = serde_json::json!(true);
    let unknown = encode_test_envelope(SEMANTIC_BINDING_SCHEMA, unknown);
    assert!(SemanticBinding::from_canonical_cbor(&unknown).is_err());

    let mut missing = serde_json::to_value(&binding).unwrap();
    missing.as_object_mut().unwrap().remove("tenant_id");
    let missing = encode_test_envelope(SEMANTIC_BINDING_SCHEMA, missing);
    assert!(SemanticBinding::from_canonical_cbor(&missing).is_err());

    let wrong_schema = encode_test_envelope("semantic-binding/v2", serde_json::json!(binding));
    assert!(SemanticBinding::from_canonical_cbor(&wrong_schema).is_err());
    assert!(SemanticBinding::from_canonical_cbor(&[]).is_err());
    assert!(SemanticBinding::from_canonical_cbor(&vec![
        0;
        SEMANTIC_CANONICAL_RECORD_MAX_BYTES + 1
    ])
    .is_err());
}

#[test]
fn vector_decoder_rejects_non_finite_and_nonpreferred_float_widths() {
    let binding = binding();
    let vector = SemanticVector::create(&binding, "article:1", "42", vec![1.0, 0.5, 0.0]).unwrap();
    let canonical = vector.to_canonical_cbor().unwrap();
    let offset = canonical
        .windows(3)
        .position(|window| window == [0xf9, 0x3c, 0x00])
        .unwrap();

    let mut non_finite = canonical.clone();
    non_finite[offset..offset + 3].copy_from_slice(&[0xf9, 0x7e, 0x00]);
    assert!(SemanticVector::from_canonical_cbor(&non_finite).is_err());

    let mut wider = canonical;
    wider.splice(offset..offset + 3, [0xfa, 0x3f, 0x80, 0x00, 0x00]);
    assert_eq!(
        SemanticVector::from_canonical_cbor(&wider),
        Err(SemanticIndexError::NonCanonicalRecord)
    );
}

#[test]
fn decoder_runs_nested_semantic_validation() {
    let binding = binding();
    let mut manifest = manifest(&binding);
    manifest.lexical.identity.lexical_index_digest = digest(99);
    let bytes = encode_test_envelope(
        SEMANTIC_INDEX_MANIFEST_SCHEMA,
        serde_json::to_value(manifest).unwrap(),
    );
    assert!(SemanticIndexManifest::from_canonical_cbor(&bytes).is_err());

    let mut transition = stage_transition(&binding);
    transition.receipt.cursor.clear();
    let bytes = encode_test_envelope(
        SEMANTIC_STAGE_TRANSITION_SCHEMA,
        serde_json::to_value(transition).unwrap(),
    );
    assert!(SemanticStageTransition::from_canonical_cbor(&bytes).is_err());

    let mut checkpoint_transition = checkpoint_transition(&binding);
    checkpoint_transition
        .generation_checkpoint
        .as_mut()
        .unwrap()
        .member
        .artifact_digest = digest(99);
    let bytes = encode_test_envelope(
        SEMANTIC_STAGE_TRANSITION_SCHEMA,
        serde_json::to_value(checkpoint_transition).unwrap(),
    );
    assert!(SemanticStageTransition::from_canonical_cbor(&bytes).is_err());
}

#[test]
fn search_hit_lineage_binds_purpose_and_policy() {
    let binding = binding();
    let lineage = lineage(&binding);
    let mut hit = SemanticSearchHit {
        binding_id: binding.binding_id.clone(),
        source_entity_id: lineage.source_entity_id.clone(),
        source_revision: lineage.source_revision.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        purpose_id: binding.purpose_id.clone(),
        policy_digest: binding.policy_digest.clone(),
        model_digest: binding.model_digest.clone(),
        preprocess_digest: binding.preprocess_digest.clone(),
        lexical_index_digest: binding.lexical_index_identity.lexical_index_digest,
        ann_index_digest: binding.ann_index_identity.ann_index_digest,
        lexical_score: Some(0.5),
        vector_score: Some(0.75),
        fused_score: 0.625,
        authorization_receipt_digest: lineage.authorization_receipt_digest,
        generation_checkpoint_digest: lineage.generation_checkpoint_digest,
        freshness_lag_ms: 0,
        fusion_explanation: "rrf-v1".into(),
        lineage_digest: lineage.lineage_digest,
    };
    assert!(hit.validate().is_ok());
    hit.purpose_id = "different-purpose".into();
    assert_eq!(hit.validate(), Err(SemanticIndexError::LineageMismatch));
    hit.purpose_id = binding.purpose_id;
    hit.policy_digest = "sha256:different-policy".into();
    assert_eq!(hit.validate(), Err(SemanticIndexError::LineageMismatch));
}

#[test]
fn response_rejects_a_tampered_nested_binding() {
    let mut binding = binding();
    binding.binding_digest = digest(99);
    let response = SemanticIndexResponse {
        request_id: "request-a".into(),
        operation: SemanticIndexOperation::GetBinding,
        outcome: SemanticIndexOutcome::Accepted {
            result: SemanticIndexResult::Binding {
                binding: Box::new(binding),
            },
            receipt_digest: None,
        },
    };
    assert!(matches!(
        response.validate(),
        Err(SemanticIndexError::DigestMismatch { .. })
    ));
}

#[test]
fn entity_progress_cannot_claim_generation_scoped_activation() {
    let progress = SemanticSourceProgress {
        binding_id: "binding-a".into(),
        binding_digest: digest(1),
        generation: 1,
        source_entity_id: "entity-a".into(),
        source_revision: "revision-a".into(),
        completed_stage: Some(SemanticStage::ReconcileAndActivate),
        completed_receipt_digest: Some(digest(2)),
        superseded_by_revision: None,
        updated_at: "2026-09-04T00:00:00Z".into(),
    };
    assert_eq!(
        progress.validate(),
        Err(SemanticIndexError::StageScopeMismatch {
            stage: SemanticStage::ReconcileAndActivate,
        })
    );
}

#[test]
fn stage_artifacts_are_closed_and_receipt_bound() {
    let binding = binding();
    let vector = SemanticVector::create(&binding, "article:1", "42", vec![1.0, 0.5, 0.0]).unwrap();
    let mut transition = stage_transition(&binding);
    transition.receipt.output_digest = vector.values_digest;
    let mutation = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(transition.clone()),
        artifact: SemanticStageArtifact::Vector {
            vector: Box::new(vector),
        },
    };
    assert!(mutation.validate().is_ok());
    let bytes = mutation.to_canonical_cbor().unwrap();
    assert_eq!(
        SemanticIndexMutation::from_canonical_cbor(&bytes).unwrap(),
        mutation
    );

    let missing = SemanticIndexMutation::RecordStageTransition {
        transition: Box::new(transition),
        artifact: SemanticStageArtifact::None,
    };
    assert_eq!(
        missing.validate(),
        Err(SemanticIndexError::StageArtifactMismatch)
    );
}

#[test]
fn source_manifest_accepts_schema_revision_zero_and_requires_exact_acl_identity() {
    let binding = binding();
    let (transition, authorization, source, _) = source_records(&binding);
    let revision_zero = rebuild_source_manifest(
        &source,
        0,
        source.source_acl_revision,
        &source.source_acl_digest,
    );
    let valid = SemanticStageArtifact::SqlSourceManifest {
        manifest: Box::new(revision_zero),
        authorization: Box::new(authorization.clone()),
    };
    assert!(valid.validate_against(&transition).is_ok());

    let mismatched_acl = rebuild_source_manifest(&source, 0, 8, "sha256:different-sql-acl");
    let invalid = SemanticStageArtifact::SqlSourceManifest {
        manifest: Box::new(mismatched_acl),
        authorization: Box::new(authorization),
    };
    assert_eq!(
        invalid.validate_against(&transition),
        Err(SemanticIndexError::AuthorizationReceiptMismatch)
    );
}

#[test]
fn graph_projection_manifest_binds_the_exact_source_manifest_input() {
    let binding = binding();
    let (_, _, source, _) = source_records(&binding);
    let intent = SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source.source_entity_id.clone(),
        },
        source_revision: binding.source_revision.clone(),
        stage: SemanticStage::GraphProjection,
        predecessor: SemanticStagePredecessor::EntityReceipt {
            stage: SemanticStage::SourceCommit,
            receipt_digest: source.completed_receipt_digest,
        },
        input_digest: source.manifest_digest,
    })
    .unwrap();
    let transition = SemanticStageTransition {
        receipt: SemanticStageReceipt {
            intent_digest: intent.intent_digest,
            output_digest: digest(34),
            cursor: "graph:42".into(),
            completed_at: "2026-09-04T00:00:20Z".into(),
            outcome: SemanticStageOutcome::Completed,
        },
        intent,
        generation_checkpoint: None,
    };
    let manifest = |source_manifest_digest| {
        SemanticGraphProjectionManifest::create(SemanticGraphProjectionManifestDraft {
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            source_entity_id: source.source_entity_id.clone(),
            source_revision: binding.source_revision.clone(),
            source_manifest_digest,
            projection_entity_digest: digest(33),
            projection_content_digest: transition.receipt.output_digest,
            completed_receipt_digest: transition.receipt.receipt_digest(),
            completed_at: transition.receipt.completed_at.clone(),
        })
        .unwrap()
    };
    assert!(manifest(source.manifest_digest)
        .validate_against_transition(&transition)
        .is_ok());
    assert_eq!(
        manifest(digest(99)).validate_against_transition(&transition),
        Err(SemanticIndexError::GraphProjectionManifestMismatch)
    );
}

#[test]
fn generation_finalization_carries_the_exact_global_artifact() {
    let binding = binding();
    let checkpoint = lexical_checkpoint(&binding);
    let manifest = SemanticLexicalIndexManifest {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_revision: binding.source_revision.clone(),
        identity: binding.lexical_index_identity.clone(),
        artifact_digest: checkpoint.artifact_digest,
        row_count: checkpoint.aggregate.completed_entity_count,
        completed_receipt_digest: checkpoint.aggregate.aggregate_receipt_digest,
        completed_at: checkpoint.completed_at.clone(),
    };
    let mutation = SemanticIndexMutation::FinalizeGeneration {
        checkpoint: Box::new(checkpoint),
        artifact: SemanticGenerationArtifact::LexicalIndexManifest {
            manifest: Box::new(manifest),
        },
    };
    assert!(mutation.validate().is_ok());
    let bytes = mutation.to_canonical_cbor().unwrap();
    assert_eq!(
        SemanticIndexMutation::from_canonical_cbor(&bytes).unwrap(),
        mutation
    );

    let mut altered = mutation.clone();
    let SemanticIndexMutation::FinalizeGeneration { artifact, .. } = &mut altered else {
        unreachable!();
    };
    let SemanticGenerationArtifact::LexicalIndexManifest { manifest } = artifact else {
        unreachable!();
    };
    manifest.completed_receipt_digest = digest(99);
    assert_eq!(
        altered.validate(),
        Err(SemanticIndexError::StageArtifactMismatch)
    );

    let mut altered = mutation;
    let SemanticIndexMutation::FinalizeGeneration { artifact, .. } = &mut altered else {
        unreachable!();
    };
    let SemanticGenerationArtifact::LexicalIndexManifest { manifest } = artifact else {
        unreachable!();
    };
    manifest.row_count += 1;
    assert_eq!(
        altered.validate(),
        Err(SemanticIndexError::StageArtifactMismatch)
    );
}

#[test]
fn activation_target_is_constructible_before_and_bound_by_the_sixth_checkpoint() {
    let binding = binding();
    let target = SemanticActivationTarget::create(&binding).unwrap();
    let aggregate = SemanticGenerationAggregate::create(
        &binding.binding_id,
        binding.binding_digest,
        binding.generation,
        &binding.source_revision,
        SemanticStage::ReconcileAndActivate,
        ["article:1", "article:2"]
            .into_iter()
            .map(|source_entity_id| SemanticExpectedEntity {
                source_entity_id: source_entity_id.to_string(),
                source_revision: binding.source_revision.clone(),
            })
            .collect(),
        [("article:1", 40_u8), ("article:2", 41_u8)]
            .into_iter()
            .map(|(source_entity_id, value)| SemanticGenerationMember {
                source_entity_id: source_entity_id.to_string(),
                source_revision: binding.source_revision.clone(),
                receipt_digest: digest(value),
                artifact_digest: digest(value + 2),
            })
            .collect(),
    )
    .unwrap();
    let checkpoint = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_revision: binding.source_revision.clone(),
        stage: SemanticStage::ReconcileAndActivate,
        aggregate,
        dependency: SemanticGenerationDependency::Activation {
            lexical_checkpoint_digest: digest(44),
            ann_checkpoint_digest: digest(45),
        },
        artifact_digest: target.target_digest,
        completed_at: "2026-09-04T00:04:00Z".into(),
    })
    .unwrap();
    let pointer = SemanticActivePointer {
        tenant_id: target.tenant_id.clone(),
        binding_id: target.binding_id.clone(),
        binding_digest: target.binding_digest,
        generation: target.generation,
        source_revision: target.source_revision.clone(),
        vector_target_id: target.vector_target_id.clone(),
        lexical_index_identity: target.lexical_index_identity.clone(),
        ann_index_identity: target.ann_index_identity.clone(),
        composite_policy_digest: target.composite_policy_digest.clone(),
        activation_receipt_digest: checkpoint.checkpoint_digest,
        activated_at: checkpoint.completed_at.clone(),
    };
    let mutation = SemanticIndexMutation::FinalizeGeneration {
        checkpoint: Box::new(checkpoint),
        artifact: SemanticGenerationArtifact::Activation {
            target: Box::new(target),
            pointer: Box::new(pointer),
        },
    };
    assert!(mutation.validate().is_ok());

    let mut altered = mutation;
    let SemanticIndexMutation::FinalizeGeneration { artifact, .. } = &mut altered else {
        unreachable!();
    };
    let SemanticGenerationArtifact::Activation { target, .. } = artifact else {
        unreachable!();
    };
    target.vector_target_id.push_str(":tampered");
    assert!(altered.validate().is_err());
}

#[test]
fn delete_mutation_carries_a_complete_tamper_evident_tombstone() {
    let tombstone = tombstone();
    let mutation = SemanticIndexMutation::DeleteBinding {
        tombstone: Box::new(tombstone.clone()),
    };
    assert!(mutation.validate().is_ok());
    let bytes = mutation.to_canonical_cbor().unwrap();
    assert_eq!(
        SemanticIndexMutation::from_canonical_cbor(&bytes).unwrap(),
        mutation
    );

    let mut altered = tombstone;
    altered.generation += 1;
    let altered = SemanticIndexMutation::DeleteBinding {
        tombstone: Box::new(altered),
    };
    assert!(matches!(
        altered.validate(),
        Err(SemanticIndexError::DigestMismatch { .. })
    ));

    let binding = binding();
    let exact = SemanticTombstone::create(SemanticTombstoneDraft {
        tenant_id: binding.tenant_id.clone(),
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        deleted_at: "2026-09-04T00:06:00Z".into(),
    })
    .unwrap();
    assert!(exact.validate_against(&binding).is_ok());

    let mut later_draft = SemanticTombstoneDraft {
        tenant_id: binding.tenant_id.clone(),
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        deleted_at: "2026-09-04T00:06:00Z".into(),
    };
    later_draft.generation += 1;
    let later_generation = SemanticTombstone::create(later_draft).unwrap();
    assert_eq!(
        later_generation.validate_against(&binding),
        Err(SemanticIndexError::GenerationMismatch {
            expected: binding.generation,
            actual: binding.generation + 1,
        })
    );
}

#[test]
fn tombstone_decoder_rejects_unknown_missing_and_noncanonical_records() {
    let tombstone = tombstone();
    let canonical = tombstone.to_canonical_cbor().unwrap();

    let mut trailing = canonical;
    trailing.push(0);
    assert!(SemanticTombstone::from_canonical_cbor(&trailing).is_err());

    let mut unknown = serde_json::to_value(&tombstone).unwrap();
    unknown["artifact_path"] = serde_json::json!("outside-owner-store");
    let unknown = encode_test_envelope(SEMANTIC_TOMBSTONE_SCHEMA, unknown);
    assert!(SemanticTombstone::from_canonical_cbor(&unknown).is_err());

    let mut missing = serde_json::to_value(&tombstone).unwrap();
    missing.as_object_mut().unwrap().remove("deleted_at");
    let missing = encode_test_envelope(SEMANTIC_TOMBSTONE_SCHEMA, missing);
    assert!(SemanticTombstone::from_canonical_cbor(&missing).is_err());

    let mut altered = serde_json::to_value(&tombstone).unwrap();
    altered["deleted_at"] = serde_json::json!("2026-09-04T00:07:00Z");
    let altered = encode_test_envelope(SEMANTIC_TOMBSTONE_SCHEMA, altered);
    assert!(matches!(
        SemanticTombstone::from_canonical_cbor(&altered),
        Err(SemanticIndexError::DigestMismatch { .. })
    ));
}
