//! RF-019 transport-neutral semantic-index contract.
//!
//! This module owns identities and operation DTOs only. It deliberately does
//! not activate `MutationScope::Native(SemanticIndex)`, persist a catalog,
//! build an index, or select an embedding provider.
//! Serializable actor, purpose, policy, and approval fields are claims to bind
//! to the server's private authorization guard; none is itself authority.
//! Clustered mutation remains unavailable until SQL ACL/schema ordering is an
//! authoritative, fail-closed prerequisite at the later activation slice.

mod codec;
#[cfg(test)]
mod codec_tests;
mod config;
mod digest;
mod generation;
mod identity;
mod persistence;
mod policy;
mod provenance;
mod request;
mod response;
mod selector;
mod selector_ref;
mod source_dirty;
mod source_manifest;
mod stage;
mod state;
mod tombstone;

pub use codec::{
    SEMANTIC_ACTIVE_POINTER_SCHEMA, SEMANTIC_ANN_INDEX_MANIFEST_SCHEMA,
    SEMANTIC_AUTHORIZATION_RECEIPT_SCHEMA, SEMANTIC_BINDING_STATE_TRANSITION_SCHEMA,
    SEMANTIC_CANONICAL_RECORD_MAX_BYTES, SEMANTIC_DEAD_LETTER_SCHEMA,
    SEMANTIC_GENERATION_CHECKPOINT_SCHEMA, SEMANTIC_GRAPH_PROJECTION_MANIFEST_SCHEMA,
    SEMANTIC_INDEX_MANIFEST_SCHEMA, SEMANTIC_INDEX_MUTATION_SCHEMA,
    SEMANTIC_LEXICAL_INDEX_MANIFEST_SCHEMA, SEMANTIC_LINEAGE_SCHEMA,
    SEMANTIC_SOURCE_DIRTY_INTENT_SCHEMA, SEMANTIC_SOURCE_DIRTY_TOPIC,
    SEMANTIC_SOURCE_PROGRESS_SCHEMA, SEMANTIC_SQL_SOURCE_MANIFEST_SCHEMA,
    SEMANTIC_STAGE_INTENT_SCHEMA, SEMANTIC_STAGE_TRANSITION_SCHEMA, SEMANTIC_TOMBSTONE_SCHEMA,
};
pub use config::{
    SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticBindingState, SemanticLexicalIndexSpec,
    SemanticModelIdentity, SemanticVectorMetric,
};
pub use digest::{
    SemanticDigest, SEMANTIC_ACTIVATION_TARGET_DIGEST_DOMAIN,
    SEMANTIC_AGGREGATE_ARTIFACT_DIGEST_DOMAIN, SEMANTIC_AGGREGATE_RECEIPT_DIGEST_DOMAIN,
    SEMANTIC_ANN_INDEX_DIGEST_DOMAIN, SEMANTIC_APPROVAL_DIGEST_DOMAIN,
    SEMANTIC_AUTHORIZATION_RECEIPT_DIGEST_DOMAIN, SEMANTIC_BINDING_DIGEST_DOMAIN,
    SEMANTIC_BINDING_STATE_RECEIPT_DOMAIN, SEMANTIC_DEAD_LETTER_DIGEST_DOMAIN,
    SEMANTIC_ENTITY_SET_DIGEST_DOMAIN, SEMANTIC_GENERATION_CHECKPOINT_DIGEST_DOMAIN,
    SEMANTIC_GRAPH_PROJECTION_MANIFEST_DIGEST_DOMAIN, SEMANTIC_LEXICAL_INDEX_DIGEST_DOMAIN,
    SEMANTIC_LINEAGE_DIGEST_DOMAIN, SEMANTIC_POLICY_DIGEST_DOMAIN,
    SEMANTIC_SQL_SOURCE_IDENTITY_DIGEST_DOMAIN, SEMANTIC_SQL_SOURCE_MANIFEST_DIGEST_DOMAIN,
    SEMANTIC_STAGE_INTENT_DIGEST_DOMAIN, SEMANTIC_STAGE_RECEIPT_DIGEST_DOMAIN,
    SEMANTIC_TOMBSTONE_RECEIPT_DIGEST_DOMAIN, SEMANTIC_VECTOR_DIGEST_DOMAIN,
};
pub use generation::{
    SemanticExpectedEntity, SemanticGenerationAggregate, SemanticGenerationCheckpoint,
    SemanticGenerationCheckpointDraft, SemanticGenerationCheckpointUpdate,
    SemanticGenerationDependency, SemanticGenerationMember, SemanticStageScope,
};
pub use identity::{
    SemanticAnnIndexIdentity, SemanticBinding, SemanticBindingDraft, SemanticLexicalIndexIdentity,
    SemanticVector, SEMANTIC_ANN_INDEX_SCHEMA, SEMANTIC_BINDING_SCHEMA,
    SEMANTIC_LEXICAL_INDEX_SCHEMA, SEMANTIC_VECTOR_SCHEMA,
};
pub use persistence::{
    SemanticActivationTarget, SemanticActivePointer, SemanticAnnIndexManifest, SemanticDeadLetter,
    SemanticDeadLetterDraft, SemanticIndexManifest, SemanticLexicalIndexManifest,
    SemanticSourceProgress,
};
pub use policy::{SemanticPolicyComponents, SemanticPolicyIdentity};
pub use provenance::{
    SemanticAuthorizationReceipt, SemanticAuthorizationReceiptDraft, SemanticLineage,
    SemanticLineageDraft,
};
pub use request::{
    SemanticIndexApproval, SemanticIndexApprovalDraft, SemanticIndexCommand, SemanticIndexFilter,
    SemanticIndexOperation, SemanticIndexRequest, SemanticSearchProbe,
    SEMANTIC_FILTER_MAX_ENTITY_IDS, SEMANTIC_FILTER_MAX_RESULTS, SEMANTIC_INDEX_APPROVAL_SCHEMA,
    SEMANTIC_INDEX_ERROR_SCHEMA, SEMANTIC_INDEX_FILTER_SCHEMA, SEMANTIC_INDEX_REQUEST_SCHEMA,
    SEMANTIC_INDEX_RESPONSE_SCHEMA, SEMANTIC_INDEX_STATUS_SCHEMA,
};
pub use response::{
    SemanticIndexOutcome, SemanticIndexResponse, SemanticIndexResult, SemanticSearchHit,
};
pub use selector::{SemanticSelectorKind, SemanticSourceSelector};
pub use selector_ref::{
    AgentLibraryCompositeRef, CanonicalTextAssetRef, GraphTextPropertyRef,
    LakehouseVectorProjectionRef, MultimodalAssetRef, SqlColumnRef, TimeSeriesWindowRef,
    SEMANTIC_SQL_CATALOG_ID, SEMANTIC_SQL_SCHEMA_ID,
};
pub use source_dirty::SemanticSourceDirtyIntent;
pub use source_manifest::{
    SemanticGraphProjectionManifest, SemanticGraphProjectionManifestDraft,
    SemanticSqlSourceIdentity, SemanticSqlSourceManifest, SemanticSqlSourceManifestDraft,
};
pub use stage::{
    SemanticQueueClass, SemanticStage, SemanticStageIntent, SemanticStageIntentDraft,
    SemanticStageOutcome, SemanticStagePredecessor, SemanticStageReceipt, SemanticStageTransition,
    SEMANTIC_FAST_CAPACITY, SEMANTIC_HEAVY_CONCURRENCY, SEMANTIC_LIGHT_CONCURRENCY,
    SEMANTIC_MEDIUM_CAPACITY, SEMANTIC_MEDIUM_CONCURRENCY, SEMANTIC_QUEUE_PROFILE_ID,
    SEMANTIC_SLOW_HEAVY_CAPACITY,
};
pub use state::{
    SemanticBindingStateTransition, SemanticGenerationArtifact, SemanticIndexError,
    SemanticIndexMutation, SemanticIndexStatus, SemanticQueueStatus, SemanticStageArtifact,
};
pub use tombstone::{SemanticTombstone, SemanticTombstoneDraft};
