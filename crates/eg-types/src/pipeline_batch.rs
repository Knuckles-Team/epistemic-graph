//! Pipelined request batching (EG-DURABLE-KERNEL-R040): a RESP pipeline or a
//! pgwire extended-protocol batch is admitted once and committed as a single
//! group commit, returning ordered replies 1:1. This is the typed model
//! slice (`.1`): the batch and admission-decision types proving one
//! admission maps to exactly one commit with order-preserving replies. The
//! real RESP/pgwire listener wiring and the actual group-commit call are
//! later children.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The wire protocol a pipelined batch arrived over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineProtocol {
    Resp,
    PgwireExtended,
}

/// A batch with zero requests is never a valid admission unit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmptyPipelineBatch(pub PipelineProtocol);

impl fmt::Display for EmptyPipelineBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} pipeline batch refused: no requests", self.0)
    }
}

impl std::error::Error for EmptyPipelineBatch {}

/// A batch of requests admitted (or to be admitted) as one unit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineBatch {
    pub protocol: PipelineProtocol,
    pub request_ids: Vec<String>,
}

impl PipelineBatch {
    /// Refuses an empty `request_ids` vector.
    pub fn new(
        protocol: PipelineProtocol,
        request_ids: Vec<String>,
    ) -> Result<Self, EmptyPipelineBatch> {
        if request_ids.is_empty() {
            return Err(EmptyPipelineBatch(protocol));
        }
        Ok(Self {
            protocol,
            request_ids,
        })
    }
}

/// The result of admitting a batch: exactly one decision per batch, the
/// exact request count, and replies in the exact original order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchCommitDecision {
    pub admitted_count: usize,
    pub ordered_reply_ids: Vec<String>,
}

/// Admit `batch` as a single group commit. One call is structurally one
/// admission: `admitted_count` always equals the batch's request count, and
/// `ordered_reply_ids` is exactly `batch.request_ids` in the same order.
pub fn admit(batch: &PipelineBatch) -> BatchCommitDecision {
    BatchCommitDecision {
        admitted_count: batch.request_ids.len(),
        ordered_reply_ids: batch.request_ids.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocols_round_trip_through_their_wire_name() {
        for (protocol, name) in [
            (PipelineProtocol::Resp, "resp"),
            (PipelineProtocol::PgwireExtended, "pgwire_extended"),
        ] {
            let wire = serde_json::to_string(&protocol).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: PipelineProtocol = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, protocol);
        }
    }

    #[test]
    fn empty_batch_is_refused_for_both_protocols() {
        for protocol in [PipelineProtocol::Resp, PipelineProtocol::PgwireExtended] {
            let err = PipelineBatch::new(protocol, Vec::new()).unwrap_err();
            assert_eq!(err.0, protocol);
        }
    }

    #[test]
    fn non_empty_batch_constructs_successfully() {
        let batch = PipelineBatch::new(
            PipelineProtocol::Resp,
            vec!["r1".to_string(), "r2".to_string()],
        )
        .unwrap();
        assert_eq!(batch.request_ids.len(), 2);
    }

    #[test]
    fn admitting_a_batch_reports_exact_count_and_preserves_order() {
        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let batch = PipelineBatch::new(PipelineProtocol::PgwireExtended, ids.clone()).unwrap();
        let decision = admit(&batch);
        assert_eq!(decision.admitted_count, 3);
        assert_eq!(decision.ordered_reply_ids, ids);
    }
}
