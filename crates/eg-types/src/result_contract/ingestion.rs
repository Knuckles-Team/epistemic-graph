//! Declared results of the `ingestion` contract domain.

#[cfg(feature = "asr-native")]
use crate::asr_wire::AsrTranscription;
use crate::ingestion_wire::{DiscoverHit, IndexResult, ParseResult, ScreenObservationResult};
#[cfg(feature = "quantum")]
use crate::quantum::{QuantumExpectationResult, QuantumQaoaResult, QuantumRankResult};
use crate::semantic_index::{
    SemanticBinding, SemanticBindingPage, SemanticSqlSourceManifest, SemanticStageIntent,
    SemanticStageLeasePage, SemanticStageTransition,
};
#[cfg(feature = "modality-serving")]
mod modality;
mod semantic;
#[cfg(feature = "viz")]
mod viz;

#[cfg(feature = "modality-serving")]
pub use modality::{
    ServedModalityApplyDisposition, ServedModalityApplyOutcome, ServedModalityAuthority,
    ServedModalityCapabilities, ServedModalityClassification, ServedModalityEvent,
    ServedModalityEventKind, ServedModalityStats, ServedModalityTombstoneCollection,
};
pub use semantic::{
    SemanticMutationReceipt, SemanticOutboxStatus, SemanticSqlSourcePageAdmission,
    SemanticSqlSourceReconciliationAdmission,
};
#[cfg(feature = "viz")]
pub use viz::{
    VizCapabilityEntry, VizCapabilityMatrix, VizPayloadRef, VizProvenanceRecord, VizRenderResponse,
    VizViewResult,
};

method_results! {
    visit_ingestion;
    ParseFile(ParseFile) => Json<ParseResult>;
    // One parse result per input file, in input order.
    ParseFiles(ParseFiles) => Json<Vec<ParseResult>>;
    IndexRepository(IndexRepository) => Json<IndexResult>;
    ObserveScreen(ObserveScreen) => Json<ScreenObservationResult>;
    AddEmbedding(AddEmbedding) => Text<String>;
    // `(node_id, weighted_similarity)` pairs, best first.
    SemanticSearch(SemanticSearch) => Raw<Vec<(String, f32)>>;
    Discover(Discover) => Json<Vec<DiscoverHit>>;
    SemanticIndexAdmitBinding(SemanticIndex / "admit_binding") => Raw<SemanticMutationReceipt>;
    SemanticIndexRefreshBinding(SemanticIndex / "refresh_binding") => Raw<SemanticMutationReceipt>;
    SemanticIndexTransitionBinding(SemanticIndex / "transition_binding") => Raw<SemanticMutationReceipt>;
    SemanticIndexDropBinding(SemanticIndex / "drop_binding") => Raw<SemanticMutationReceipt>;
    SemanticIndexAdmitSourceRecord(SemanticIndex / "admit_source_record") => Raw<SemanticMutationReceipt>;
    SemanticIndexAdmitSourcePage(SemanticIndex / "admit_source_page") => Raw<SemanticSqlSourcePageAdmission>;
    SemanticIndexAdmitSourceReconcile(SemanticIndex / "admit_source_reconcile") => Raw<SemanticSqlSourceReconciliationAdmission>;
    SemanticIndexAdmitSourceReplacement(SemanticIndex / "admit_source_replacement") => Raw<SemanticMutationReceipt>;
    SemanticIndexSubscribeStageConsumer(SemanticIndex / "subscribe_stage_consumer") => Raw<bool>;
    SemanticIndexClaimStageLeases(SemanticIndex / "claim_stage_leases") => Raw<SemanticStageLeasePage>;
    SemanticIndexValidateStageLease(SemanticIndex / "validate_stage_lease") => Raw<SemanticStageIntent>;
    SemanticIndexStageStatus(SemanticIndex / "stage_status") => Raw<SemanticOutboxStatus>;
    SemanticIndexCompleteStage(SemanticIndex / "complete_stage") => Raw<SemanticMutationReceipt>;
    SemanticIndexCompleteGenerationStage(SemanticIndex / "complete_generation_stage") => Raw<SemanticMutationReceipt>;
    SemanticIndexCompleteSqlSourceStage(SemanticIndex / "complete_sql_source_stage") => Raw<SemanticMutationReceipt>;
    SemanticIndexReplayCompletedSqlSourceStage(SemanticIndex / "replay_completed_sql_source_stage") => Raw<Option<(SemanticStageTransition, SemanticMutationReceipt)>>;
    SemanticIndexReleaseStageLease(SemanticIndex / "release_stage_lease") => Raw<bool>;
    SemanticIndexBinding(SemanticIndex / "binding") => Raw<Option<SemanticBinding>>;
    SemanticIndexSqlSourceManifest(SemanticIndex / "sql_source_manifest") => Raw<Option<SemanticSqlSourceManifest>>;
    SemanticIndexListBindings(SemanticIndex / "list_bindings") => Raw<SemanticBindingPage>;
    SemanticIndexLiveGeneration(SemanticIndex / "live_generation") => Raw<Option<u64>>;
    #[cfg(feature = "modality-serving")]
    ServedModalityAuthority(ServedModality / "authority") => Raw<ServedModalityAuthority>;
    #[cfg(feature = "modality-serving")]
    ServedModalityIngest(ServedModality / "ingest") => Raw<ServedModalityApplyOutcome>;
    #[cfg(feature = "modality-serving")]
    ServedModalityIngestStream(ServedModality / "ingest_stream") => Raw<Vec<ServedModalityApplyOutcome>>;
    #[cfg(feature = "modality-serving")]
    ServedModalityQuery(ServedModality / "query") => Raw<Dynamic> dynamic RequestedModality;
    #[cfg(feature = "modality-serving")]
    ServedModalityNativeQuery(ServedModality / "native_query") => Raw<Dynamic> dynamic RequestedModality;
    #[cfg(feature = "modality-serving")]
    ServedModalityDelete(ServedModality / "delete") => Raw<ServedModalityApplyOutcome>;
    #[cfg(feature = "modality-serving")]
    ServedModalityMoveToCold(ServedModality / "move_to_cold") => Raw<ServedModalityApplyOutcome>;
    #[cfg(feature = "modality-serving")]
    ServedModalityRestore(ServedModality / "restore") => Raw<ServedModalityApplyOutcome>;
    #[cfg(feature = "modality-serving")]
    ServedModalityEvents(ServedModality / "events") => Raw<Vec<ServedModalityEvent>>;
    #[cfg(feature = "modality-serving")]
    ServedModalityStats(ServedModality / "stats") => Raw<ServedModalityStats>;
    #[cfg(feature = "modality-serving")]
    ServedModalityCollectTombstones(ServedModality / "collect_tombstones") => Json<ServedModalityTombstoneCollection>;
    #[cfg(feature = "modality-serving")]
    ServedModalityCapabilities(ServedModality / "capabilities") => Json<ServedModalityCapabilities>;
    #[cfg(feature = "viz")]
    VizRender(Viz / "Render") => Raw<VizRenderResponse>;
    #[cfg(feature = "viz")]
    VizCapabilityMatrix(Viz / "CapabilityMatrix") => Raw<VizCapabilityMatrix>;
    #[cfg(feature = "viz")]
    VizRenderProvenance(Viz / "RenderProvenance") => Raw<Option<VizProvenanceRecord>>;
    #[cfg(feature = "quantum")]
    QuantumRank(Quantum / "rank") => Json<QuantumRankResult>;
    #[cfg(feature = "quantum")]
    QuantumOptimizeQaoa(Quantum / "optimize_qaoa") => Json<QuantumQaoaResult>;
    #[cfg(feature = "quantum")]
    QuantumExpectation(Quantum / "expectation") => Json<QuantumExpectationResult>;
    #[cfg(feature = "asr-native")]
    AsrTranscribeFile(Asr / "TranscribeFile") => Json<AsrTranscription>;
}
