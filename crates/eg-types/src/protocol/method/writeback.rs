//! Which feature-gated requests durably write through a write-back.
//!
//! The RDF, mining and graph-learning variants exist only when this crate's
//! `rdf`, `mining` and `graphlearn` features are on, which depends on which
//! crate enabled them, not on the caller's own features. Classifying them here,
//! under the same gates as the variants, keeps every build compiling and every
//! build that has a variant classifying it the same way.

use super::Method;
#[cfg(feature = "mining")]
use crate::protocol::{SubgraphAlgorithm, TextAlgorithm};

impl Method {
    /// True when this request durably mutates the graph as a triple edit or a
    /// mining / graph-learning write-back, and so must be logged for replay.
    /// Mirrors `access::requires_write` for the same variants.
    pub fn is_writeback_mutation(&self) -> bool {
        #[cfg(feature = "rdf")]
        if matches!(
            self,
            Method::AddTriples { .. } | Method::RemoveTriples { .. } | Method::DropNamedGraph
        ) {
            return true;
        }
        #[cfg(feature = "mining")]
        if mining_writeback(self) {
            return true;
        }
        #[cfg(feature = "graphlearn")]
        if matches!(
            self,
            Method::GraphLearnFit {
                writeback: true,
                ..
            } | Method::GraphLearnPredict {
                writeback: true,
                ..
            }
        ) {
            return true;
        }
        false
    }
}

#[cfg(feature = "mining")]
fn mining_writeback(m: &Method) -> bool {
    match m {
        // `tfidf` and `motif` never mutate, whatever the flag says.
        Method::MineText {
            writeback,
            algorithm,
            ..
        } => *writeback && !matches!(algorithm, TextAlgorithm::Tfidf),
        Method::MineSubgraph {
            writeback,
            algorithm,
            ..
        } => *writeback && !matches!(algorithm, SubgraphAlgorithm::Motif),
        Method::MineAssociate { writeback, .. }
        | Method::MineCluster { writeback, .. }
        | Method::MineAnomaly { writeback, .. }
        | Method::MineClassifyPredict { writeback, .. }
        | Method::MineReduce { writeback, .. }
        | Method::MineSequence { writeback, .. }
        | Method::MineForecast { writeback, .. }
        | Method::MineEntityResolve { writeback, .. }
        | Method::MineCausalImpact { writeback, .. }
        | Method::MineProcess { writeback, .. }
        | Method::MineRootCause { writeback, .. }
        | Method::MineRiskPropagation { writeback, .. }
        | Method::MineOntologyGap { writeback, .. }
        | Method::MineRetrievalQuality { writeback, .. }
        | Method::MineCommunity { writeback, .. } => *writeback,
        _ => false,
    }
}
