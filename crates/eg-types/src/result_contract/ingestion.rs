//! Declared results of the `ingestion` contract domain.

#[cfg(feature = "asr-native")]
use crate::asr_wire::AsrTranscription;
use crate::ingestion_wire::{DiscoverHit, IndexResult, ParseResult, ScreenObservationResult};
#[cfg(feature = "quantum")]
use crate::quantum::{QuantumExpectationResult, QuantumQaoaResult, QuantumRankResult};

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
    #[cfg(feature = "quantum")]
    QuantumRank(Quantum / "rank") => Json<QuantumRankResult>;
    #[cfg(feature = "quantum")]
    QuantumOptimizeQaoa(Quantum / "optimize_qaoa") => Json<QuantumQaoaResult>;
    #[cfg(feature = "quantum")]
    QuantumExpectation(Quantum / "expectation") => Json<QuantumExpectationResult>;
    #[cfg(feature = "asr-native")]
    AsrTranscribeFile(Asr / "TranscribeFile") => Json<AsrTranscription>;
}
