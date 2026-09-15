//! Stage predecessor proofs: a stage may run or be recorded only on top of
//! the durable receipt, checkpoint and manifests its predecessor names.
//!
//! The same rule is proved twice: once against a serving snapshot, to fence a
//! lease before work runs, and again inside the admitted write that records
//! the transition. Both apply [`entity_progress`] and
//! [`ensure_predecessor_proof`]; they differ only in where the rows are read
//! from and in the wording each site has always reported.

use super::checkpoint::{
    current_checkpoint_from_tables, validate_six_checkpoint_from_tables, GenerationCoordinates,
};
use super::record::{decode_valid, row, row_bytes, ValidatedRow};
use super::{
    ensure, kernel_error, refused, semantic_contract_error, SemanticCodeError, SemanticCodeStore,
};
use eg_storage::{
    ScopedRead, SemanticIndexOwner, SEMANTIC_ANN, SEMANTIC_CHECKPOINTS, SEMANTIC_CHECKPOINT_HEADS,
    SEMANTIC_LEXICAL, SEMANTIC_SOURCE_PROGRESS, SEMANTIC_STAGES,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite};
use eg_types::semantic_index::{
    SemanticAnnIndexManifest, SemanticBinding, SemanticDigest, SemanticGenerationCheckpoint,
    SemanticLexicalIndexManifest, SemanticSourceProgress, SemanticStage, SemanticStageIntent,
    SemanticStagePredecessor,
};

/// The refusal wording one proof site reports.
struct PredecessorWording {
    no_progress: &'static str,
    progress_mismatch: &'static str,
    superseded: &'static str,
    already_completed: &'static str,
    generation_scope: &'static str,
    s1_completed: &'static str,
    receipt_needs_progress: &'static str,
    receipt_mismatch: &'static str,
    checkpoint_stale: &'static str,
    coverage_stale: &'static str,
    activation_stale: &'static str,
}

/// A lease fenced against the serving snapshot before work runs.
const LEASE: PredecessorWording = PredecessorWording {
    no_progress: "semantic stage lease has no durable source-progress row",
    progress_mismatch: "semantic stage lease source-progress proof does not match intent",
    superseded: "semantic stage lease names a superseded source revision",
    already_completed: "semantic stage lease is already completed",
    generation_scope: "only S6 may use a generation-scoped lease",
    s1_completed: "S1 source progress already has a completed predecessor",
    receipt_needs_progress: "entity receipt requires entity source progress",
    receipt_mismatch: "entity predecessor receipt is not the durable receipt",
    checkpoint_stale: "generation predecessor checkpoint is absent or stale",
    coverage_stale: "generation coverage checkpoint is absent or stale",
    activation_stale: "S6 activation checkpoints are absent, mixed, or stale",
};

/// A transition re-proved inside its admitted write.
const TRANSITION: PredecessorWording = PredecessorWording {
    no_progress: "semantic stage transition has no source-progress row",
    progress_mismatch: "semantic transition source-progress proof does not match intent",
    superseded: "semantic transition names a superseded source revision",
    already_completed: "semantic transition stage is already completed",
    generation_scope: "only S6 may use a generation scope",
    s1_completed: "S1 transition has a completed predecessor",
    receipt_needs_progress: "entity receipt requires source progress",
    receipt_mismatch: "entity receipt is not the durable predecessor receipt",
    checkpoint_stale: "generation checkpoint is absent or mismatched",
    coverage_stale: "generation coverage is absent or mismatched",
    activation_stale: "S6 activation proof is absent or mixed-generation",
};

impl SemanticCodeStore {
    pub(super) fn validate_stage_predecessor_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        reject_completed: bool,
    ) -> Result<(), SemanticCodeError> {
        let (tenant, binding) = (self.tenant.as_str(), self.binding.as_str());
        let progress_rows = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let progress = entity_progress(
            |entity| row(progress_rows.get((tenant, binding, intent.generation, entity))),
            intent,
            reject_completed,
            &LEASE,
        )?;
        let checkpoints = read
            .open_owner_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let heads = read
            .open_owner_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        let at = GenerationCoordinates::of_intent(tenant, binding, intent);
        ensure_predecessor_proof(intent, progress.as_ref(), &LEASE, |stage| {
            current_checkpoint_from_tables(&checkpoints, &heads, at, stage)
        })
    }

    pub(super) fn validate_six_checkpoint_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        checkpoint: &SemanticGenerationCheckpoint,
    ) -> Result<(), SemanticCodeError> {
        let checkpoints = read
            .open_owner_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let heads = read
            .open_owner_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        validate_six_checkpoint_from_tables(
            &checkpoints,
            &heads,
            &read
                .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
                .map_err(kernel_error)?,
            &read
                .open_owner_table(SEMANTIC_STAGES)
                .map_err(kernel_error)?,
            self.tenant.as_str(),
            self.binding.as_str(),
            checkpoint,
        )
    }

    pub(super) fn validate_generation_manifests_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        binding: &SemanticBinding,
        intent: &SemanticStageIntent,
    ) -> Result<(), SemanticCodeError> {
        let key = (
            self.tenant.as_str(),
            self.binding.as_str(),
            intent.generation,
        );
        let lexical: SemanticLexicalIndexManifest = read_manifest(
            &read
                .open_owner_table(SEMANTIC_LEXICAL)
                .map_err(kernel_error)?,
            key,
            "S6 activation has no durable lexical index manifest",
        )?;
        ensure(
            manifest_generation(
                &lexical.binding_id,
                lexical.binding_digest,
                lexical.generation,
                &lexical.source_revision,
            ) == binding_generation(binding)
                && lexical.identity == binding.lexical_index_identity,
            "S6 lexical manifest is not bound to the durable binding",
        )?;
        let ann: SemanticAnnIndexManifest = read_manifest(
            &read.open_owner_table(SEMANTIC_ANN).map_err(kernel_error)?,
            key,
            "S6 activation has no durable ANN index manifest",
        )?;
        ensure(
            manifest_generation(
                &ann.binding_id,
                ann.binding_digest,
                ann.generation,
                &ann.source_revision,
            ) == binding_generation(binding)
                && ann.identity == binding.ann_index_identity,
            "S6 ANN manifest is not bound to the durable binding",
        )
    }
}

pub(super) fn validate_six_checkpoint_write(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<(), SemanticCodeError> {
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let source_progress_table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stage_table = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    validate_six_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        &source_progress_table,
        &stage_table,
        tenant,
        binding,
        checkpoint,
    )
}

pub(super) fn validate_stage_predecessor_write(
    write: &AdmittedMutation<'_, SemanticIndexOwner>,
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    intent: &SemanticStageIntent,
    reject_completed: bool,
) -> Result<(), SemanticCodeError> {
    let progress = {
        let progress_rows = write
            .open_read_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        entity_progress(
            |entity| row(progress_rows.get((tenant, binding, intent.generation, entity))),
            intent,
            reject_completed,
            &TRANSITION,
        )?
    };
    let checkpoints = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let heads = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let at = GenerationCoordinates::of_intent(tenant, binding, intent);
    ensure_predecessor_proof(intent, progress.as_ref(), &TRANSITION, |stage| {
        current_checkpoint_from_tables(&checkpoints, &heads, at, stage)
    })
}

/// The progress row an entity-scoped intent runs on, matching the intent and
/// not superseded; `None` for the one generation-scoped stage, S6. `lookup`
/// reads one entity's progress row wherever this proof site reads rows.
fn entity_progress<L>(
    lookup: L,
    intent: &SemanticStageIntent,
    reject_completed: bool,
    wording: &PredecessorWording,
) -> Result<Option<SemanticSourceProgress>, SemanticCodeError>
where
    L: FnOnce(&str) -> Result<Option<SemanticSourceProgress>, SemanticCodeError>,
{
    let Some(source_entity_id) = intent.scope.source_entity_id() else {
        ensure(
            intent.stage == SemanticStage::ReconcileAndActivate,
            wording.generation_scope,
        )?;
        return Ok(None);
    };
    let progress = lookup(source_entity_id)?.ok_or_else(|| refused(wording.no_progress))?;
    ensure(
        (
            progress.binding_digest,
            progress.generation,
            progress.source_entity_id.as_str(),
            progress.source_revision.as_str(),
        ) == (
            intent.binding_digest,
            intent.generation,
            source_entity_id,
            intent.source_revision.as_str(),
        ),
        wording.progress_mismatch,
    )?;
    ensure(
        progress.superseded_by_revision.is_none(),
        wording.superseded,
    )?;
    let completed = progress
        .completed_stage
        .is_some_and(|stage| stage >= intent.stage);
    ensure(!(reject_completed && completed), wording.already_completed)?;
    Ok(Some(progress))
}

/// The durable proof `intent.predecessor` names: an entity receipt recorded
/// in `progress`, or a generation checkpoint that is still the complete head
/// `current` returns for its stage.
fn ensure_predecessor_proof<F>(
    intent: &SemanticStageIntent,
    progress: Option<&SemanticSourceProgress>,
    wording: &PredecessorWording,
    current: F,
) -> Result<(), SemanticCodeError>
where
    F: Fn(SemanticStage) -> Result<Option<SemanticGenerationCheckpoint>, SemanticCodeError>,
{
    let is_head = |digest: SemanticDigest, stage: SemanticStage| {
        let head = current(stage)?;
        Ok::<_, SemanticCodeError>(head.is_some_and(|head| is_complete_head(&head, digest)))
    };
    let completed = |stage: SemanticStage| {
        progress.is_some_and(|progress| progress.completed_stage == Some(stage))
    };
    match &intent.predecessor {
        SemanticStagePredecessor::None => ensure(
            progress.is_none_or(|progress| progress.completed_stage.is_none()),
            wording.s1_completed,
        ),
        SemanticStagePredecessor::EntityReceipt {
            stage,
            receipt_digest,
        } => {
            let progress = progress.ok_or_else(|| refused(wording.receipt_needs_progress))?;
            ensure(
                (progress.completed_stage, progress.completed_receipt_digest)
                    == (Some(*stage), Some(*receipt_digest)),
                wording.receipt_mismatch,
            )
        }
        SemanticStagePredecessor::GenerationCheckpoint {
            stage,
            checkpoint_digest,
        } => ensure(
            completed(*stage) && is_head(*checkpoint_digest, *stage)?,
            wording.checkpoint_stale,
        ),
        SemanticStagePredecessor::GenerationCoverage { checkpoint } => {
            checkpoint
                .require_complete()
                .map_err(semantic_contract_error)?;
            ensure(
                completed(SemanticStage::Vector)
                    && is_head(checkpoint.checkpoint_digest, SemanticStage::Vector)?,
                wording.coverage_stale,
            )
        }
        SemanticStagePredecessor::Activation {
            lexical_checkpoint_digest,
            ann_checkpoint_digest,
        } => ensure(
            is_head(*lexical_checkpoint_digest, SemanticStage::LexicalIndex)?
                && is_head(*ann_checkpoint_digest, SemanticStage::AnnIndex)?,
            wording.activation_stale,
        ),
    }
}

/// A stage head proves a predecessor only as the named, complete checkpoint.
fn is_complete_head(head: &SemanticGenerationCheckpoint, digest: SemanticDigest) -> bool {
    head.checkpoint_digest == digest && head.require_complete().is_ok()
}

fn read_manifest<T, R>(
    table: &T,
    key: (&str, &str, u64),
    missing: &str,
) -> Result<R, SemanticCodeError>
where
    T: redb::ReadableTable<(&'static str, &'static str, u64), &'static [u8]>,
    R: ValidatedRow,
{
    let raw = row_bytes(table.get(key))?.ok_or_else(|| refused(missing))?;
    decode_valid(&raw)
}

fn manifest_generation<'a>(
    binding_id: &'a str,
    binding_digest: SemanticDigest,
    generation: u64,
    source_revision: &'a str,
) -> (&'a str, SemanticDigest, u64, &'a str) {
    (binding_id, binding_digest, generation, source_revision)
}

fn binding_generation(binding: &SemanticBinding) -> (&str, SemanticDigest, u64, &str) {
    (
        binding.binding_id.as_str(),
        binding.binding_digest,
        binding.generation,
        binding.source_revision.as_str(),
    )
}
