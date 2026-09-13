//! Results of the data-mining family (CONCEPT:EG-KG.mining.frequent-itemset-mining).
//!
//! Every writeback-capable result reports `written_back`, the number of typed nodes
//! the call materialized (0 when writeback was off).

mod insight;
mod patterns;
mod vector;

pub use insight::{
    CausalImpactMiningResult, CausalRelationRow, CommunityMiningResult, CommunityRow,
    DirectlyFollowsRow, EntityMatchRow, EntityResolutionMiningResult, OntologyGapMiningResult,
    OntologyGapRow, ParallelRelationRow, ProcessMiningResult, RetrievalQualityMiningResult,
    RiskPropagationMiningResult, RiskScoreRow, RootCauseCandidateRow, RootCauseMiningResult,
};
pub use patterns::{
    AssociationMiningResult, AssociationRuleRow, DocTerms, ForecastMiningResult, GspanMiningResult,
    MotifCountsRow, MotifMiningResult, PatternEdge, SequenceMiningResult, SequentialPatternRow,
    SubgraphMiningResult, SubgraphPatternRow, TermWeight, TextMiningResult, TopicTerms,
};
pub use vector::{
    AnomalyMiningResult, AnomalyRow, ClassificationMiningResult, ClassifiedRow,
    ClassifierFitResult, ClusterMiningResult, ClusterRow, ReducedRow, ReductionMiningResult,
    RowRef,
};
