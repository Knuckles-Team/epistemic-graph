//! Selective embedding regeneration over the existing semantic store.
//!
//! This module owns source/model identity checks and the provider seam. It does
//! not select a network endpoint, own a cache, or create a second vector store.

use super::{EmbeddingDimensionError, SemanticStore};
use crate::interrupt;
use eg_types::contract::Digest256;

/// The immutable identity of one source chunk that may be embedded into a
/// [`SemanticStore`].  The model, revision, model-content hash, and
/// preprocessing/tokenizer hash are carried by [`EmbeddingSpaceRef`]; the
/// source digest binds the exact text supplied to the provider.  This is an
/// internal Rust value, deliberately separate from the semantic-index wire
/// DTOs: a regeneration worker can use it without inventing a second public
/// protocol or a second vector store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingChunkRef {
    pub source_digest: Digest256,
    pub space: eg_types::EmbeddingSpaceRef,
}

impl EmbeddingChunkRef {
    /// Build an identity from the exact text that will be presented to an
    /// embedding provider.  Callers that already have an authoritative source
    /// digest may construct the value directly and the regeneration gate will
    /// still verify it against the supplied text before invoking a provider.
    pub fn for_text(space: eg_types::EmbeddingSpaceRef, text: &str) -> Self {
        Self {
            source_digest: Digest256::sha256(text.as_bytes()),
            space,
        }
    }

    fn validate(&self) -> Result<(), String> {
        self.space.validate()
    }
}

/// A source chunk and the pinned identity under which it is eligible for
/// embedding.  `existing` is the identity recorded alongside an already
/// indexed vector, when one exists.  The vector bytes remain in
/// [`SemanticStore`]; this value is only the source/model binding used to
/// decide whether regeneration is safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingRegenerationCandidate<'a> {
    pub node_id: &'a str,
    pub text: &'a str,
    pub identity: EmbeddingChunkRef,
    pub existing: Option<EmbeddingChunkRef>,
}

/// The only provider capability required by selective regeneration.  The
/// caller supplies the provider, so this layer never chooses a remote endpoint
/// or performs an ungoverned network call.  The batch surface lets the owner
/// (`eg-plan`) reuse its existing `TextEmbedder` batching implementation.
pub trait EmbeddingProvider: Send + Sync {
    fn space(&self) -> &eg_types::EmbeddingSpaceRef;
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, String>;
}

/// The result of one selective-regeneration pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddingRegenerationReport {
    pub reused: Vec<String>,
    pub regenerated: Vec<String>,
}

/// Fail-closed errors from [`SemanticStore::regenerate_eligible`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingRegenerationError {
    EmptyNodeId,
    EmptySource {
        node_id: String,
    },
    DuplicateNodeId {
        node_id: String,
    },
    InvalidCandidate {
        node_id: String,
        reason: String,
    },
    SourceDigestMismatch {
        node_id: String,
    },
    StaleSourceDigest {
        node_id: String,
    },
    EmbeddingSpaceMismatch {
        node_id: String,
    },
    MissingExistingBinding {
        node_id: String,
    },
    StoreSpaceUnbound,
    StoreSpaceInvalid(String),
    StoreSpaceMismatch {
        expected: String,
        received: String,
    },
    ProviderSpaceInvalid(String),
    ProviderSpaceMismatch {
        expected: String,
        received: String,
    },
    MissingProvider {
        node_id: String,
    },
    Provider(String),
    ProviderBatchLengthMismatch {
        expected: usize,
        received: usize,
    },
    Cancelled,
    InvalidEmbedding {
        node_id: String,
        source: EmbeddingDimensionError,
    },
}

impl std::fmt::Display for EmbeddingRegenerationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyNodeId => write!(f, "embedding regeneration node id must not be empty"),
            Self::EmptySource { node_id } => {
                write!(f, "embedding regeneration source for `{node_id}` is empty")
            }
            Self::DuplicateNodeId { node_id } => {
                write!(f, "embedding regeneration contains duplicate node `{node_id}`")
            }
            Self::InvalidCandidate { node_id, reason } => {
                write!(f, "embedding regeneration candidate `{node_id}` is invalid: {reason}")
            }
            Self::SourceDigestMismatch { node_id } => write!(
                f,
                "embedding regeneration candidate `{node_id}` does not match its pinned source digest"
            ),
            Self::StaleSourceDigest { node_id } => write!(
                f,
                "embedding regeneration candidate `{node_id}` changed source since its indexed vector"
            ),
            Self::EmbeddingSpaceMismatch { node_id } => write!(
                f,
                "embedding regeneration candidate `{node_id}` changed model, revision, or preprocessing space"
            ),
            Self::MissingExistingBinding { node_id } => write!(
                f,
                "existing embedding for `{node_id}` has no pinned source/model binding"
            ),
            Self::StoreSpaceUnbound => write!(
                f,
                "selective embedding regeneration requires a declared semantic store space"
            ),
            Self::StoreSpaceInvalid(reason) => {
                write!(f, "semantic store space is invalid: {reason}")
            }
            Self::StoreSpaceMismatch { expected, received } => write!(
                f,
                "candidate embedding space `{received}` does not match store space `{expected}`"
            ),
            Self::ProviderSpaceInvalid(reason) => {
                write!(f, "embedding provider space is invalid: {reason}")
            }
            Self::ProviderSpaceMismatch { expected, received } => write!(
                f,
                "embedding provider space `{received}` does not match required space `{expected}`"
            ),
            Self::MissingProvider { node_id } => write!(
                f,
                "candidate `{node_id}` needs regeneration but no compatible embedding provider was supplied"
            ),
            Self::Provider(reason) => write!(f, "embedding provider failed: {reason}"),
            Self::ProviderBatchLengthMismatch { expected, received } => write!(
                f,
                "embedding provider returned {received} vectors for {expected} requested chunks"
            ),
            Self::Cancelled => write!(f, "embedding regeneration was cancelled before commit"),
            Self::InvalidEmbedding { node_id, source } => {
                write!(f, "provider returned an invalid embedding for `{node_id}`: {source}")
            }
        }
    }
}

impl std::error::Error for EmbeddingRegenerationError {}

impl SemanticStore {
    /// Refine an optional candidate set through the backend's existing
    /// space-checked search path.  The default backend uses HNSW (or exact
    /// fallback for small stores); the `ann` build uses IVF-PQ.  This helper
    /// only supplies the allow-list and therefore does not introduce a second
    /// index or claim that source regeneration is a cold-query cache.
    pub fn semantic_search_stamped_candidates<'a>(
        &self,
        query: &eg_types::StampedVector,
        candidate_ids: impl IntoIterator<Item = &'a str>,
        n_results: usize,
    ) -> Result<Vec<(String, f32)>, super::SemanticQueryError> {
        let allowed: std::collections::HashSet<&'a str> = candidate_ids.into_iter().collect();
        self.semantic_search_stamped_filtered(query, n_results, |node_id| allowed.contains(node_id))
    }

    /// Regenerate only chunks whose source and pinned embedding space are
    /// unchanged and whose vector is absent.  All source/binding checks and
    /// provider outputs are validated before the first store mutation, so a
    /// stale source, changed model/preprocessing, missing provider, cancelled
    /// request, or malformed batch leaves the store untouched.
    pub fn regenerate_eligible(
        &mut self,
        candidates: &[EmbeddingRegenerationCandidate<'_>],
        provider: Option<&dyn EmbeddingProvider>,
    ) -> Result<EmbeddingRegenerationReport, EmbeddingRegenerationError> {
        if candidates.is_empty() {
            return Ok(EmbeddingRegenerationReport::default());
        }

        let store_space = self
            .space()
            .ok_or(EmbeddingRegenerationError::StoreSpaceUnbound)?;
        store_space
            .validate()
            .map_err(EmbeddingRegenerationError::StoreSpaceInvalid)?;

        let mut seen = std::collections::HashSet::with_capacity(candidates.len());
        let mut pending = Vec::new();
        let mut report = EmbeddingRegenerationReport::default();

        for (index, candidate) in candidates.iter().enumerate() {
            if candidate.node_id.is_empty() {
                return Err(EmbeddingRegenerationError::EmptyNodeId);
            }
            if !seen.insert(candidate.node_id) {
                return Err(EmbeddingRegenerationError::DuplicateNodeId {
                    node_id: candidate.node_id.to_string(),
                });
            }
            if candidate.text.is_empty() {
                return Err(EmbeddingRegenerationError::EmptySource {
                    node_id: candidate.node_id.to_string(),
                });
            }
            candidate.identity.validate().map_err(|reason| {
                EmbeddingRegenerationError::InvalidCandidate {
                    node_id: candidate.node_id.to_string(),
                    reason,
                }
            })?;
            if candidate.identity.space != *store_space {
                return Err(EmbeddingRegenerationError::StoreSpaceMismatch {
                    expected: store_space.digest.clone(),
                    received: candidate.identity.space.digest.clone(),
                });
            }
            if Digest256::sha256(candidate.text.as_bytes()) != candidate.identity.source_digest {
                return Err(EmbeddingRegenerationError::SourceDigestMismatch {
                    node_id: candidate.node_id.to_string(),
                });
            }

            if let Some(existing) = candidate.existing.as_ref() {
                existing.validate().map_err(|reason| {
                    EmbeddingRegenerationError::InvalidCandidate {
                        node_id: candidate.node_id.to_string(),
                        reason,
                    }
                })?;
                if existing.source_digest != candidate.identity.source_digest {
                    return Err(EmbeddingRegenerationError::StaleSourceDigest {
                        node_id: candidate.node_id.to_string(),
                    });
                }
                if existing.space != candidate.identity.space {
                    return Err(EmbeddingRegenerationError::EmbeddingSpaceMismatch {
                        node_id: candidate.node_id.to_string(),
                    });
                }
            }

            match (
                candidate.existing.is_some(),
                self.get_embedding(candidate.node_id),
            ) {
                (true, Some(_)) => report.reused.push(candidate.node_id.to_string()),
                (_, None) => pending.push(index),
                (false, Some(_)) => {
                    return Err(EmbeddingRegenerationError::MissingExistingBinding {
                        node_id: candidate.node_id.to_string(),
                    });
                }
            }
        }

        if pending.is_empty() {
            return Ok(report);
        }

        let provider = provider.ok_or_else(|| EmbeddingRegenerationError::MissingProvider {
            node_id: candidates[pending[0]].node_id.to_string(),
        })?;
        let provider_space = provider.space();
        provider_space
            .validate()
            .map_err(EmbeddingRegenerationError::ProviderSpaceInvalid)?;
        if provider_space != store_space {
            return Err(EmbeddingRegenerationError::ProviderSpaceMismatch {
                expected: store_space.digest.clone(),
                received: provider_space.digest.clone(),
            });
        }
        if interrupt::requested() {
            return Err(EmbeddingRegenerationError::Cancelled);
        }

        let texts: Vec<&str> = pending
            .iter()
            .map(|&index| candidates[index].text)
            .collect();
        let embeddings = provider
            .embed_batch(&texts)
            .map_err(EmbeddingRegenerationError::Provider)?;
        if embeddings.len() != pending.len() {
            return Err(EmbeddingRegenerationError::ProviderBatchLengthMismatch {
                expected: pending.len(),
                received: embeddings.len(),
            });
        }
        if interrupt::requested() {
            return Err(EmbeddingRegenerationError::Cancelled);
        }

        for (&index, embedding) in pending.iter().zip(embeddings.iter()) {
            self.validate_embedding(embedding).map_err(|source| {
                EmbeddingRegenerationError::InvalidEmbedding {
                    node_id: candidates[index].node_id.to_string(),
                    source,
                }
            })?;
        }
        if interrupt::requested() {
            return Err(EmbeddingRegenerationError::Cancelled);
        }

        // The validation pass above establishes the store dimension and
        // finiteness for every vector before this commit loop begins.  The
        // mutable borrow prevents another writer from changing that invariant.
        for (&index, embedding) in pending.iter().zip(embeddings) {
            self.add_embedding(candidates[index].node_id.to_string(), embedding)
                .expect("validated selective-regeneration embedding must be admissible");
            report
                .regenerated
                .push(candidates[index].node_id.to_string());
        }
        Ok(report)
    }
}

#[cfg(test)]
mod selective_regeneration_tests {
    use super::*;

    struct TestProvider {
        space: eg_types::EmbeddingSpaceRef,
        values: Vec<Vec<f32>>,
    }

    impl EmbeddingProvider for TestProvider {
        fn space(&self) -> &eg_types::EmbeddingSpaceRef {
            &self.space
        }

        fn embed_batch(&self, _texts: &[&str]) -> Result<Vec<Vec<f32>>, String> {
            Ok(self.values.clone())
        }
    }

    fn space(revision: &str) -> eg_types::EmbeddingSpaceRef {
        eg_types::EmbeddingSpaceRef::pinned(
            "test-model",
            revision,
            "model-hash",
            "tokenizer-and-preprocess-hash",
            2,
            true,
        )
        .unwrap()
    }

    fn candidate<'a>(
        space: &eg_types::EmbeddingSpaceRef,
        node_id: &'a str,
        text: &'a str,
        existing: Option<EmbeddingChunkRef>,
    ) -> EmbeddingRegenerationCandidate<'a> {
        EmbeddingRegenerationCandidate {
            node_id,
            text,
            identity: EmbeddingChunkRef::for_text(space.clone(), text),
            existing,
        }
    }

    #[test]
    fn regenerates_only_missing_vector_and_reports_the_real_batch() {
        let space = space("revision-1");
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        let text = "first immutable chunk";
        let candidates = [candidate(&space, "chunk-1", text, None)];
        let provider = TestProvider {
            space,
            values: vec![vec![1.0, 0.0]],
        };

        let report = store
            .regenerate_eligible(&candidates, Some(&provider))
            .unwrap();

        assert_eq!(report.reused, Vec::<String>::new());
        assert_eq!(report.regenerated, vec!["chunk-1"]);
        assert_eq!(store.get_embedding("chunk-1"), Some(vec![1.0, 0.0]));
    }

    #[test]
    fn reuses_exactly_bound_vector_without_a_provider() {
        let space = space("revision-1");
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        store
            .add_embedding("chunk-1".into(), vec![1.0, 0.0])
            .unwrap();
        let text = "already indexed chunk";
        let identity = EmbeddingChunkRef::for_text(space.clone(), text);
        let candidates = [candidate(&space, "chunk-1", text, Some(identity))];

        let report = store.regenerate_eligible(&candidates, None).unwrap();

        assert_eq!(report.reused, vec!["chunk-1"]);
        assert!(report.regenerated.is_empty());
    }

    #[test]
    fn candidate_refinement_uses_the_existing_space_checked_backend_path() {
        let space = space("revision-1");
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        store
            .add_embedding("allowed".into(), vec![1.0, 0.0])
            .unwrap();
        store
            .add_embedding("excluded".into(), vec![0.0, 1.0])
            .unwrap();
        let query = eg_types::StampedVector::new(space, vec![1.0, 0.0]).unwrap();

        let hits = store
            .semantic_search_stamped_candidates(&query, ["allowed", "missing"], 10)
            .unwrap();

        assert_eq!(hits, vec![("allowed".into(), 1.0)]);
    }

    #[test]
    fn refuses_a_stale_source_digest_before_provider_invocation() {
        let space = space("revision-1");
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        let old_identity = EmbeddingChunkRef::for_text(space.clone(), "old source");
        let candidates = [candidate(
            &space,
            "chunk-1",
            "new source",
            Some(old_identity),
        )];
        let provider = TestProvider {
            space,
            values: vec![vec![1.0, 0.0]],
        };

        assert_eq!(
            store.regenerate_eligible(&candidates, Some(&provider)),
            Err(EmbeddingRegenerationError::StaleSourceDigest {
                node_id: "chunk-1".into()
            })
        );
        assert!(store.embeddings_snapshot().is_empty());
    }

    #[test]
    fn refuses_a_changed_model_or_revision_before_provider_invocation() {
        let old_space = space("revision-1");
        let new_space = space("revision-2");
        let old_identity = EmbeddingChunkRef::for_text(old_space, "same source");
        let candidates = [candidate(
            &new_space,
            "chunk-1",
            "same source",
            Some(old_identity),
        )];
        let mut store = SemanticStore::new_in_space(new_space.clone()).unwrap();
        let provider = TestProvider {
            space: new_space,
            values: vec![vec![1.0, 0.0]],
        };

        assert_eq!(
            store.regenerate_eligible(&candidates, Some(&provider)),
            Err(EmbeddingRegenerationError::EmbeddingSpaceMismatch {
                node_id: "chunk-1".into()
            })
        );
        assert!(store.embeddings_snapshot().is_empty());
    }

    #[test]
    fn refuses_missing_provider_without_mutating_the_store() {
        let space = space("revision-1");
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        let candidates = [candidate(&space, "chunk-1", "needs embedding", None)];

        assert_eq!(
            store.regenerate_eligible(&candidates, None),
            Err(EmbeddingRegenerationError::MissingProvider {
                node_id: "chunk-1".into()
            })
        );
        assert!(store.embeddings_snapshot().is_empty());
    }

    #[test]
    fn cancellation_before_provider_call_leaves_the_store_unchanged() {
        let space = space("revision-1");
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        let candidates = [candidate(&space, "chunk-1", "cancel me", None)];
        let provider = TestProvider {
            space,
            values: vec![vec![1.0, 0.0]],
        };

        let result = interrupt::scoped(
            || true,
            || store.regenerate_eligible(&candidates, Some(&provider)),
        );

        assert_eq!(result, Err(EmbeddingRegenerationError::Cancelled));
        assert!(store.embeddings_snapshot().is_empty());
    }
}
