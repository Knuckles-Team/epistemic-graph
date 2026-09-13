//! Generation checkpoint reads: the durable head, the authoritative
//! generation state and the S6 checkpoint proof.

use super::reconciliation::SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY;
use super::record::{decode, row_bytes};
use super::stage::{is_stage_receipt_index_key, is_terminal};
use super::{corrupt, ensure, kernel_error, refused, semantic_contract_error, SemanticCodeError};
use eg_types::semantic_index::{
    SemanticDigest, SemanticExpectedEntity, SemanticGenerationAggregate,
    SemanticGenerationCheckpoint, SemanticGenerationDependency, SemanticGenerationMember,
    SemanticIndexMutation, SemanticSourceProgress, SemanticStage, SemanticStageIntent,
};
use std::collections::BTreeMap;

/// Where one generation's rows live, and the binding identity those rows must
/// agree with.
///
/// Fifteen call sites plucked exactly these five off a
/// `SemanticGenerationCheckpoint`, a `SemanticBinding` or a
/// `SemanticStageTransition` and passed them one by one.
/// `tenant`/`binding`/`generation` are the row-key prefix of every
/// per-generation semantic table; `source_revision` selects the checkpoint head
/// inside that prefix; and `binding_digest` is the identity the rows found
/// there must carry, so a row written under a DIFFERENT binding of the same
/// name is refused rather than read. The digest travelling with the key is the
/// point of the grouping: a lookup given only the key could not tell those two
/// apart, and every caller that has the key has the digest.
#[derive(Clone, Copy)]
pub(super) struct GenerationCoordinates<'a> {
    pub(super) tenant: &'a str,
    pub(super) binding: &'a str,
    pub(super) generation: u64,
    pub(super) binding_digest: SemanticDigest,
    pub(super) source_revision: &'a str,
}

impl<'a> GenerationCoordinates<'a> {
    /// The generation a stage intent runs in.
    pub(super) fn of_intent(
        tenant: &'a str,
        binding: &'a str,
        intent: &'a SemanticStageIntent,
    ) -> Self {
        Self {
            tenant,
            binding,
            generation: intent.generation,
            binding_digest: intent.binding_digest,
            source_revision: &intent.source_revision,
        }
    }

    /// The generation a checkpoint describes.
    pub(super) fn of_checkpoint(
        tenant: &'a str,
        binding: &'a str,
        checkpoint: &'a SemanticGenerationCheckpoint,
    ) -> Self {
        Self {
            tenant,
            binding,
            generation: checkpoint.generation,
            binding_digest: checkpoint.binding_digest,
            source_revision: &checkpoint.source_revision,
        }
    }
}

type ProgressKey = (&'static str, &'static str, u64, &'static str);
type StageKey = (&'static str, &'static str, &'static str);
type HeadKey = (&'static str, &'static str, u64, &'static str, &'static str);

pub(super) fn current_checkpoint_from_tables<TC, TH>(
    checkpoint_table: &TC,
    head_table: &TH,
    at: GenerationCoordinates<'_>,
    stage: SemanticStage,
) -> Result<Option<SemanticGenerationCheckpoint>, SemanticCodeError>
where
    TC: redb::ReadableTable<ProgressKey, &'static [u8]>,
    TH: redb::ReadableTable<HeadKey, &'static [u8]>,
{
    let head_key = (
        at.tenant,
        at.binding,
        at.generation,
        at.source_revision,
        stage.as_str(),
    );
    let Some(head_bytes) = row_bytes(head_table.get(head_key))? else {
        return Ok(None);
    };
    let head: SemanticGenerationCheckpoint = decode(&head_bytes)?;
    head.validate().map_err(semantic_contract_error)?;
    if (
        head.binding_id.as_str(),
        head.binding_digest,
        head.generation,
        head.source_revision.as_str(),
        head.stage,
    ) != (
        at.binding,
        at.binding_digest,
        at.generation,
        at.source_revision,
        stage,
    ) {
        return Err(corrupt(
            "semantic checkpoint head pointer coordinates do not match its key",
        ));
    }
    let checkpoint_key = head.checkpoint_digest.to_string();
    let checkpoint_bytes = row_bytes(checkpoint_table.get((
        at.tenant,
        at.binding,
        at.generation,
        checkpoint_key.as_str(),
    )))?
    .ok_or_else(|| corrupt("semantic checkpoint head points to a missing checkpoint row"))?;
    if checkpoint_bytes != head_bytes {
        return Err(corrupt(
            "semantic checkpoint head bytes differ from its checkpoint row",
        ));
    }
    let checkpoint: SemanticGenerationCheckpoint = decode(&checkpoint_bytes)?;
    if checkpoint != head || checkpoint.checkpoint_digest.to_string() != checkpoint_key {
        return Err(corrupt(
            "semantic checkpoint head key or row bytes do not match the checkpoint digest",
        ));
    }
    Ok(Some(checkpoint))
}

pub(super) fn authoritative_generation_state<TP, TS>(
    source_progress_table: &TP,
    stage_table: &TS,
    at: GenerationCoordinates<'_>,
    member_stage: SemanticStage,
    aggregate_stage: SemanticStage,
    current_entity: Option<&str>,
) -> Result<
    (
        SemanticGenerationAggregate,
        BTreeMap<String, SemanticGenerationMember>,
    ),
    SemanticCodeError,
>
where
    TP: redb::ReadableTable<ProgressKey, &'static [u8]>,
    TS: redb::ReadableTable<StageKey, &'static [u8]>,
{
    let progress = progress_by_entity(source_progress_table, at)?;
    let members = completed_members(stage_table, at, member_stage)?;
    let (expected, completed) = aggregate_entities(
        &progress,
        &members,
        at.source_revision,
        member_stage,
        current_entity,
    )?;
    ensure(
        members
            .keys()
            .all(|source_entity_id| progress.contains_key(source_entity_id)),
        "semantic checkpoint stage receipt names an omitted entity",
    )?;
    let aggregate = SemanticGenerationAggregate::create(
        at.binding,
        at.binding_digest,
        at.generation,
        at.source_revision,
        aggregate_stage,
        expected,
        completed,
    )
    .map_err(semantic_contract_error)?;
    Ok((aggregate, members))
}

/// Every source entity's progress row in the generation, all at its current
/// revision and none superseded.
fn progress_by_entity<T>(
    table: &T,
    at: GenerationCoordinates<'_>,
) -> Result<BTreeMap<String, SemanticSourceProgress>, SemanticCodeError>
where
    T: redb::ReadableTable<ProgressKey, &'static [u8]>,
{
    let mut by_entity = BTreeMap::new();
    let rows = table
        .range((at.tenant, at.binding, at.generation, "")..)
        .map_err(kernel_error)?;
    for row in rows {
        let (key, value) = row.map_err(kernel_error)?;
        let key = key.value();
        if (key.0, key.1, key.2) != (at.tenant, at.binding, at.generation) {
            break;
        }
        if key.3 == SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY {
            continue;
        }
        let progress = current_generation_progress(value.value(), key.3, at)?;
        if by_entity
            .insert(progress.source_entity_id.clone(), progress)
            .is_some()
        {
            return Err(corrupt(
                "semantic checkpoint source-progress rows duplicate an entity",
            ));
        }
    }
    ensure(
        !by_entity.is_empty(),
        "semantic checkpoint has no authoritative source-progress entities",
    )?;
    Ok(by_entity)
}

fn current_generation_progress(
    bytes: &[u8],
    source_entity_id: &str,
    at: GenerationCoordinates<'_>,
) -> Result<SemanticSourceProgress, SemanticCodeError> {
    let progress: SemanticSourceProgress = decode(bytes)?;
    ensure(
        (
            progress.binding_id.as_str(),
            progress.binding_digest,
            progress.generation,
            progress.source_entity_id.as_str(),
            progress.source_revision.as_str(),
        ) == (
            at.binding,
            at.binding_digest,
            at.generation,
            source_entity_id,
            at.source_revision,
        ),
        "semantic checkpoint source-progress row is outside the current generation",
    )?;
    ensure(
        progress.superseded_by_revision.is_none(),
        "semantic checkpoint source-progress row is superseded",
    )?;
    Ok(progress)
}

/// The completed `member_stage` transition of every entity in the generation.
fn completed_members<T>(
    table: &T,
    at: GenerationCoordinates<'_>,
    member_stage: SemanticStage,
) -> Result<BTreeMap<String, SemanticGenerationMember>, SemanticCodeError>
where
    T: redb::ReadableTable<StageKey, &'static [u8]>,
{
    let mut members = BTreeMap::new();
    let rows = table
        .range((at.tenant, at.binding, "")..)
        .map_err(kernel_error)?;
    for row in rows {
        let (key, value) = row.map_err(kernel_error)?;
        let key = key.value();
        if (key.0, key.1) != (at.tenant, at.binding) {
            break;
        }
        // Receipt index rows carry the same canonical transition bytes as the
        // intent row. They are direct proof lookups, not additional completed
        // members of the generation aggregate.
        if is_stage_receipt_index_key(key.2) {
            continue;
        }
        let Some(member) = generation_member(decode(value.value())?, at, member_stage) else {
            continue;
        };
        if members
            .insert(member.source_entity_id.clone(), member)
            .is_some()
        {
            return Err(corrupt(
                "semantic checkpoint stage rows contain duplicate completed entities",
            ));
        }
    }
    Ok(members)
}

/// The member a stage row contributes: a terminal, entity-scoped
/// `member_stage` transition of exactly this generation.
fn generation_member(
    mutation: SemanticIndexMutation,
    at: GenerationCoordinates<'_>,
    member_stage: SemanticStage,
) -> Option<SemanticGenerationMember> {
    let SemanticIndexMutation::RecordStageTransition { transition, .. } = mutation else {
        return None;
    };
    let intent = &transition.intent;
    let in_generation = (
        intent.binding_id.as_str(),
        intent.binding_digest,
        intent.generation,
        intent.source_revision.as_str(),
        intent.stage,
    ) == (
        at.binding,
        at.binding_digest,
        at.generation,
        at.source_revision,
        member_stage,
    );
    let source_entity_id = intent
        .scope
        .source_entity_id()
        .filter(|_| in_generation && is_terminal(transition.receipt.outcome))?;
    Some(SemanticGenerationMember {
        source_entity_id: source_entity_id.to_string(),
        source_revision: at.source_revision.to_string(),
        receipt_digest: transition.receipt.receipt_digest(),
        artifact_digest: transition.receipt.output_digest,
    })
}

/// Every entity the generation expects, and the completed members among them:
/// an entity counts once its progress reached `member_stage` (or it is the
/// entity being completed), and then it must have exactly that stage receipt.
fn aggregate_entities(
    progress_by_entity: &BTreeMap<String, SemanticSourceProgress>,
    members: &BTreeMap<String, SemanticGenerationMember>,
    source_revision: &str,
    member_stage: SemanticStage,
    current_entity: Option<&str>,
) -> Result<(Vec<SemanticExpectedEntity>, Vec<SemanticGenerationMember>), SemanticCodeError> {
    let mut expected = Vec::with_capacity(progress_by_entity.len());
    let mut completed = Vec::new();
    for (source_entity_id, progress) in progress_by_entity {
        expected.push(SemanticExpectedEntity {
            source_entity_id: source_entity_id.clone(),
            source_revision: source_revision.to_string(),
        });
        let counts = progress
            .completed_stage
            .is_some_and(|stage| stage >= member_stage)
            || current_entity == Some(source_entity_id.as_str());
        match (counts, members.get(source_entity_id)) {
            (true, Some(member)) => {
                ensure(
                    progress.completed_stage != Some(member_stage)
                        || progress.completed_receipt_digest == Some(member.receipt_digest),
                    "semantic checkpoint source-progress receipt differs from its stage receipt",
                )?;
                completed.push(member.clone());
            }
            (true, None) => {
                return Err(refused(
                    "semantic checkpoint has completed source progress without a stage receipt",
                ));
            }
            (false, Some(_)) => {
                return Err(refused(
                    "semantic checkpoint has an uncommitted stage receipt for an incomplete entity",
                ));
            }
            (false, None) => {}
        }
    }
    Ok((expected, completed))
}

pub(super) fn validate_six_checkpoint_from_tables<TC, TH, TP, TS>(
    checkpoint_table: &TC,
    checkpoint_head_table: &TH,
    source_progress_table: &TP,
    stage_table: &TS,
    tenant: &str,
    binding: &str,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<(), SemanticCodeError>
where
    TC: redb::ReadableTable<ProgressKey, &'static [u8]>,
    TH: redb::ReadableTable<HeadKey, &'static [u8]>,
    TP: redb::ReadableTable<ProgressKey, &'static [u8]>,
    TS: redb::ReadableTable<StageKey, &'static [u8]>,
{
    checkpoint
        .require_complete()
        .map_err(semantic_contract_error)?;
    ensure(
        checkpoint.binding_id == binding && checkpoint.stage == SemanticStage::ReconcileAndActivate,
        "S6 checkpoint is outside the current semantic binding",
    )?;
    let at = GenerationCoordinates::of_checkpoint(tenant, binding, checkpoint);
    let complete_head = |stage: SemanticStage, missing: &str| {
        let head =
            current_checkpoint_from_tables(checkpoint_table, checkpoint_head_table, at, stage)?
                .ok_or_else(|| refused(missing))?;
        head.require_complete().map_err(semantic_contract_error)?;
        Ok::<_, SemanticCodeError>(head)
    };
    let lexical = complete_head(
        SemanticStage::LexicalIndex,
        "S6 checkpoint has no current lexical generation checkpoint",
    )?;
    let ann = complete_head(
        SemanticStage::AnnIndex,
        "S6 checkpoint has no current ANN generation checkpoint",
    )?;
    let SemanticGenerationDependency::Activation {
        lexical_checkpoint_digest,
        ann_checkpoint_digest,
    } = &checkpoint.dependency
    else {
        return Err(refused(
            "S6 checkpoint has no exact lexical/ANN dependency proof",
        ));
    };
    ensure(
        (*lexical_checkpoint_digest, *ann_checkpoint_digest)
            == (lexical.checkpoint_digest, ann.checkpoint_digest),
        "S6 checkpoint dependency is not the current lexical/ANN head",
    )?;
    let (aggregate, _) = authoritative_generation_state(
        source_progress_table,
        stage_table,
        at,
        SemanticStage::AnnIndex,
        SemanticStage::ReconcileAndActivate,
        None,
    )?;
    ensure(
        checkpoint.aggregate == aggregate,
        "S6 checkpoint aggregate is not the authoritative current generation state",
    )
}
