//! Declared results of the `query` contract domain.

use super::Dynamic;
use crate::change_envelope::{ChangeCursor, ChangeEnvelopeRecord, ContentVersion};
use crate::decision::DecisionBatch;
use crate::epistemic_operations::EvidenceBundle;
#[cfg(feature = "knowledge-batch")]
use crate::knowledge_stream::KnowledgeStreamBatch;
#[cfg(feature = "epistemic")]
use crate::protocol::{
    CausalCounterfactualResult, CausalEstimateResult, EpistemicStatusResult, ExplainBeliefResponse,
    ExplainEvidenceResult, MaterializationStatusResult, RankByProvenanceResult,
    RecomputeMaterializationResult, ResolveConflictResult, StaleMaterializationsResult,
    WhatChangedResult,
};
#[cfg(feature = "query")]
use crate::protocol::{ExplainPlanResult, ExplainPolicyResult};
use crate::types::ContextView;

method_results! {
    visit_query;
    GetContextView(GetContextView) => Json<ContextView>;
    GetChangeEnvelope(GetChangeEnvelope) => Raw<Option<ChangeEnvelopeRecord>>;
    GetContentVersion(GetContentVersion) => Raw<Option<ContentVersion>>;
    GetChangeCursor(GetChangeCursor) => Raw<Option<ChangeCursor>>;
    // `{columns, rows}` with each row a MessagePack array of the statement's cells.
    Sql(Sql) => Raw<Dynamic> dynamic QueryRows;
    // `{columns, rows}` with each row a MessagePack array of the query's cells.
    CypherQuery(CypherQuery) => Raw<Dynamic> dynamic QueryRows;
    // The GraphQL `{data: ...}` response the document selects.
    GraphQl(GraphQl) => Raw<Dynamic> dynamic QueryRows;
    #[cfg(feature = "knowledge-batch")]
    KnowledgeStream(KnowledgeStream) => Raw<KnowledgeStreamBatch>;
    // `[node id, score | nil]` rows of the cross-modal plan.
    UnifiedQuery(UnifiedQuery) => Raw<Vec<(String, Option<f32>)>>;
    UnifiedQueryText(UnifiedQueryText) => Raw<Vec<(String, Option<f32>)>>;
    #[cfg(feature = "query")]
    ExplainPlan(ExplainPlan) => Raw<ExplainPlanResult>;
    ExplainProvenance(ExplainProvenance) => Raw<EvidenceBundle>;
    ExplainProvenanceByIds(ExplainProvenanceByIds) => Raw<EvidenceBundle>;
    #[cfg(feature = "query")]
    ExplainPolicy(ExplainPolicy) => Raw<ExplainPolicyResult>;
    #[cfg(feature = "epistemic")]
    ExplainBelief(ExplainBelief) => Raw<ExplainBeliefResponse>;
    #[cfg(feature = "epistemic")]
    EpistemicStatus(EpistemicStatus) => Raw<EpistemicStatusResult>;
    #[cfg(feature = "epistemic")]
    WhatChanged(WhatChanged) => Raw<WhatChangedResult>;
    #[cfg(feature = "epistemic")]
    RecomputeMaterialization(RecomputeMaterialization) => Raw<RecomputeMaterializationResult>;
    #[cfg(feature = "epistemic")]
    MaterializationStatus(MaterializationStatus) => Raw<MaterializationStatusResult>;
    #[cfg(feature = "epistemic")]
    StaleMaterializations(StaleMaterializations) => Raw<StaleMaterializationsResult>;
    #[cfg(feature = "epistemic")]
    ResolveConflict(ResolveConflict) => Raw<ResolveConflictResult>;
    #[cfg(feature = "epistemic")]
    ExplainEvidence(ExplainEvidence) => Raw<ExplainEvidenceResult>;
    #[cfg(feature = "epistemic")]
    CausalEstimate(CausalEstimate) => Raw<CausalEstimateResult>;
    #[cfg(feature = "epistemic")]
    CausalCounterfactual(CausalCounterfactual) => Raw<CausalCounterfactualResult>;
    #[cfg(feature = "epistemic")]
    RankByProvenance(RankByProvenance) => Raw<RankByProvenanceResult>;
    NlQuery(NlQuery) => Raw<Vec<(String, Option<f32>)>>;
    TxnUnifiedQuery(TxnUnifiedQuery) => Raw<Vec<(String, Option<f32>)>>;
    TxnUnifiedQueryText(TxnUnifiedQueryText) => Raw<Vec<(String, Option<f32>)>>;
    Decide(Decide) => Raw<DecisionBatch>;
}
