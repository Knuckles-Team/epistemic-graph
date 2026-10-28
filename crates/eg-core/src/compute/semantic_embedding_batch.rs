//! Bounded, content-addressed batching for selective semantic regeneration.
//!
//! The cache is process-local and disposable. Its key binds the pinned
//! embedding-space digest and exact input text; it never supplies or invents
//! provenance for vectors already resident in a [`super::SemanticStore`].

use super::EmbeddingProvider;
use eg_types::contract::Digest256;
use eg_types::EmbeddingSpaceRef;
use parking_lot::Mutex;
use std::collections::HashMap;

/// Maximum candidate count accepted by one regeneration call. This matches the
/// existing bounded semantic source page size, keeping returned vectors bounded
/// even when the caller asks the provider wrapper to batch internally.
pub const MAX_REGENERATION_CHUNKS: usize = 256;
/// Maximum combined UTF-8 source bytes accepted by one regeneration call.
pub const MAX_REGENERATION_TEXT_BYTES: usize = 8 * 1024 * 1024;
/// Maximum inputs sent to one concrete provider invocation.
pub const MAX_PROVIDER_BATCH_ITEMS: usize = 32;
/// Maximum logical resident cache budget, including a fixed per-entry allowance
/// for the digest key and map bookkeeping.
pub const MAX_EMBEDDING_CACHE_BYTES: usize = 8 * 1024 * 1024;
const CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;
const MAX_CACHE_ENTRIES: usize = 256;
const MAX_NAMESPACE_COMPONENT_BYTES: usize = 256;

/// Validated authority scope for reusable embedding outputs. A cache entry can
/// be reused only for the same tenant, corpus/source scope, and encoding-policy
/// revision, in addition to the pinned model/preprocessing space and source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheNamespace {
    tenant_id: String,
    source_scope: String,
    encoding_policy_revision: String,
}

impl CacheNamespace {
    /// Build a cache namespace from the caller's validated authority coordinates.
    pub fn new(
        tenant_id: impl Into<String>,
        source_scope: impl Into<String>,
        encoding_policy_revision: impl Into<String>,
    ) -> Result<Self, String> {
        let tenant_id = tenant_id.into();
        let source_scope = source_scope.into();
        let encoding_policy_revision = encoding_policy_revision.into();
        for (field, value) in [
            ("tenant_id", &tenant_id),
            ("source_scope", &source_scope),
            ("encoding_policy_revision", &encoding_policy_revision),
        ] {
            if value.trim().is_empty() {
                return Err(format!(
                    "embedding cache namespace {field} must not be empty"
                ));
            }
            if value.len() > MAX_NAMESPACE_COMPONENT_BYTES {
                return Err(format!(
                    "embedding cache namespace {field} exceeds {MAX_NAMESPACE_COMPONENT_BYTES} bytes"
                ));
            }
        }
        Ok(Self {
            tenant_id,
            source_scope,
            encoding_policy_revision,
        })
    }
}

/// A provider adapter that batches calls and reuses exact, pinned-space outputs
/// in a bounded in-memory cache. Provider calls and each internal batch are
/// synchronous; cooperative request interruption is checked between batches.
pub struct BoundedCachingEmbeddingProvider<'a> {
    provider: &'a dyn EmbeddingProvider,
    namespace: CacheNamespace,
    space: EmbeddingSpaceRef,
    batch_items: usize,
    cache_capacity: usize,
    cache: Mutex<HashMap<Digest256, (Vec<f32>, u64)>>,
    tick: Mutex<u64>,
}

impl<'a> BoundedCachingEmbeddingProvider<'a> {
    /// Wrap `provider` with explicit batch and cache budgets.
    ///
    /// `batch_items` must be in `1..=MAX_PROVIDER_BATCH_ITEMS`; the cache budget
    /// must not exceed [`MAX_EMBEDDING_CACHE_BYTES`]. A budget too small for one
    /// vector simply disables caching while preserving batching.
    pub fn new(
        provider: &'a dyn EmbeddingProvider,
        namespace: CacheNamespace,
        batch_items: usize,
        cache_bytes: usize,
    ) -> Result<Self, String> {
        if batch_items == 0 || batch_items > MAX_PROVIDER_BATCH_ITEMS {
            return Err(format!(
                "embedding batch size must be in 1..={MAX_PROVIDER_BATCH_ITEMS}"
            ));
        }
        if cache_bytes > MAX_EMBEDDING_CACHE_BYTES {
            return Err(format!(
                "embedding cache budget exceeds {MAX_EMBEDDING_CACHE_BYTES} bytes"
            ));
        }
        let space = provider.space().clone();
        space.validate()?;
        let entry_bytes = space
            .dimensions
            .checked_mul(std::mem::size_of::<f32>())
            .and_then(|bytes| bytes.checked_add(CACHE_ENTRY_OVERHEAD_BYTES))
            .ok_or_else(|| "embedding cache entry size overflowed".to_string())?;
        let cache_capacity = (cache_bytes / entry_bytes).min(MAX_CACHE_ENTRIES);
        Ok(Self {
            provider,
            namespace,
            space,
            batch_items,
            cache_capacity,
            cache: Mutex::new(HashMap::with_capacity(cache_capacity)),
            tick: Mutex::new(0),
        })
    }

    fn cache_key(&self, text: &str) -> Result<Digest256, String> {
        let source_digest = Digest256::sha256(text.as_bytes());
        Digest256::framed(
            b"eg/semantic-embedding-cache/v1",
            &[
                self.namespace.tenant_id.as_bytes(),
                self.namespace.source_scope.as_bytes(),
                self.namespace.encoding_policy_revision.as_bytes(),
                self.space.digest.as_bytes(),
                source_digest.as_bytes(),
            ],
        )
    }

    fn touch(&self, key: Digest256) -> Option<Vec<f32>> {
        let mut cache = self.cache.lock();
        let value = cache.get_mut(&key)?;
        let mut tick = self.tick.lock();
        *tick = tick.saturating_add(1);
        value.1 = *tick;
        Some(value.0.clone())
    }

    fn insert(&self, key: Digest256, vector: Vec<f32>) {
        if self.cache_capacity == 0 {
            return;
        }
        let mut cache = self.cache.lock();
        let mut tick = self.tick.lock();
        *tick = tick.saturating_add(1);
        cache.insert(key, (vector, *tick));
        while cache.len() > self.cache_capacity {
            let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(key, _)| *key)
            else {
                break;
            };
            cache.remove(&oldest);
        }
    }

    fn validate_vector(&self, text_index: usize, vector: &[f32]) -> Result<(), String> {
        if vector.len() != self.space.dimensions {
            return Err(format!(
                "embedding provider returned dimension {} for input {text_index}; pinned space requires {}",
                vector.len(), self.space.dimensions
            ));
        }
        if let Some(index) = vector.iter().position(|component| !component.is_finite()) {
            return Err(format!(
                "embedding provider returned a non-finite component at index {index} for input {text_index}"
            ));
        }
        Ok(())
    }
}

impl EmbeddingProvider for BoundedCachingEmbeddingProvider<'_> {
    fn space(&self) -> &EmbeddingSpaceRef {
        &self.space
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, String> {
        if crate::interrupt::requested() {
            return Err("embedding regeneration was cancelled before provider work".into());
        }
        if texts.len() > MAX_REGENERATION_CHUNKS {
            return Err(format!(
                "embedding request has {} chunks; maximum is {MAX_REGENERATION_CHUNKS}",
                texts.len()
            ));
        }
        let total_text_bytes = texts.iter().try_fold(0usize, |total, text| {
            total
                .checked_add(text.len())
                .filter(|bytes| *bytes <= MAX_REGENERATION_TEXT_BYTES)
        });
        if total_text_bytes.is_none() {
            return Err(format!(
                "embedding source text exceeds the {MAX_REGENERATION_TEXT_BYTES}-byte call budget"
            ));
        }
        if self.provider.space() != &self.space {
            return Err("embedding provider space changed after it was pinned".into());
        }
        self.space.validate()?;
        if texts.iter().any(|text| text.is_empty()) {
            return Err("embedding source text must not be empty".into());
        }
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let mut unique_indices = HashMap::<Digest256, usize>::with_capacity(texts.len());
        let mut unique = Vec::<(Digest256, usize, &str)>::new();
        for (index, text) in texts.iter().copied().enumerate() {
            let key = self.cache_key(text)?;
            if unique_indices.contains_key(&key) {
                continue;
            }
            unique_indices.insert(key, unique.len());
            unique.push((key, index, text));
        }

        let mut resolved = HashMap::<Digest256, Vec<f32>>::with_capacity(unique.len());
        let mut pending = Vec::<(Digest256, usize, &str)>::new();
        for (key, index, text) in unique {
            if let Some(vector) = self.touch(key) {
                resolved.insert(key, vector);
            } else {
                pending.push((key, index, text));
            }
        }

        for chunk in pending.chunks(self.batch_items) {
            if crate::interrupt::requested() {
                return Err("embedding regeneration was cancelled between provider batches".into());
            }
            let batch_texts: Vec<&str> = chunk.iter().map(|(_, _, text)| *text).collect();
            let vectors = self.provider.embed_batch(&batch_texts)?;
            if vectors.len() != chunk.len() {
                return Err(format!(
                    "embedding provider returned {} vectors for {} requested chunks",
                    vectors.len(),
                    chunk.len()
                ));
            }
            for (offset, vector) in vectors.iter().enumerate() {
                self.validate_vector(chunk[offset].1, vector)?;
            }
            if crate::interrupt::requested() {
                return Err("embedding regeneration was cancelled between provider batches".into());
            }
            for ((key, _index, _), vector) in chunk.iter().zip(vectors) {
                self.insert(*key, vector.clone());
                resolved.insert(*key, vector);
            }
        }

        texts
            .iter()
            .enumerate()
            .map(|(index, text)| {
                let key = self.cache_key(text)?;
                resolved
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| format!("embedding output missing for input {index}"))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::semantic::{
        EmbeddingChunkRef, EmbeddingRegenerationCandidate, SemanticStore,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Copy)]
    enum ProviderOutput {
        Valid,
        Error,
        WrongCardinality,
        WrongDimension,
        NonFinite,
    }

    struct TestProvider {
        space: EmbeddingSpaceRef,
        output: ProviderOutput,
        calls: AtomicUsize,
        batches: Mutex<Vec<Vec<String>>>,
        cancel_on_call: Option<Arc<AtomicBool>>,
    }

    impl TestProvider {
        fn new(output: ProviderOutput) -> Self {
            Self {
                space: EmbeddingSpaceRef::pinned(
                    "test-model",
                    "revision-1",
                    "model-content-hash",
                    "preprocessing-hash",
                    2,
                    true,
                )
                .unwrap(),
                output,
                calls: AtomicUsize::new(0),
                batches: Mutex::new(Vec::new()),
                cancel_on_call: None,
            }
        }

        fn batches(&self) -> Vec<Vec<String>> {
            self.batches.lock().clone()
        }
    }

    impl EmbeddingProvider for TestProvider {
        fn space(&self) -> &EmbeddingSpaceRef {
            &self.space
        }

        fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.batches
                .lock()
                .push(texts.iter().map(|text| (*text).to_string()).collect());
            if let Some(cancel) = &self.cancel_on_call {
                cancel.store(true, Ordering::SeqCst);
            }
            match self.output {
                ProviderOutput::Error => Err("provider unavailable".into()),
                ProviderOutput::WrongCardinality => Ok(vec![vec![1.0, 0.0]]),
                ProviderOutput::WrongDimension => Ok(texts.iter().map(|_| vec![1.0]).collect()),
                ProviderOutput::NonFinite => {
                    Ok(texts.iter().map(|_| vec![1.0, f32::NAN]).collect())
                }
                ProviderOutput::Valid => Ok(texts
                    .iter()
                    .map(|text| vec![text.len() as f32, 1.0])
                    .collect()),
            }
        }
    }

    fn namespace() -> CacheNamespace {
        CacheNamespace::new("tenant-a", "corpus-a", "encoding-v1").unwrap()
    }

    fn wrapper<'a>(
        provider: &'a TestProvider,
        batch_items: usize,
        cache_bytes: usize,
    ) -> BoundedCachingEmbeddingProvider<'a> {
        BoundedCachingEmbeddingProvider::new(provider, namespace(), batch_items, cache_bytes)
            .unwrap()
    }

    #[test]
    fn batches_within_limit_coalesces_duplicates_and_reuses_cached_vectors() {
        let provider = TestProvider::new(ProviderOutput::Valid);
        let wrapper = wrapper(&provider, 2, 4096);

        let first = wrapper.embed_batch(&["alpha", "beta", "alpha"]).unwrap();
        assert_eq!(first[0], vec![5.0, 1.0]);
        assert_eq!(first[1], vec![4.0, 1.0]);
        assert_eq!(first[0], first[2], "duplicate text shares one embedding");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.batches(),
            vec![vec!["alpha".to_string(), "beta".to_string()]]
        );

        let repeated = wrapper.embed_batch(&["beta", "alpha"]).unwrap();
        assert_eq!(repeated, vec![first[1].clone(), first[0].clone()]);
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            1,
            "cache hit avoids a provider call"
        );
    }

    #[test]
    fn splits_provider_work_and_rejects_oversized_or_empty_sources() {
        let provider = TestProvider::new(ProviderOutput::Valid);
        let wrapper = wrapper(&provider, 2, 0);
        let output = wrapper.embed_batch(&["a", "b", "c", "d", "e"]).unwrap();
        assert_eq!(output.len(), 5);
        assert_eq!(
            provider.batches().iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        assert!(wrapper.embed_batch(&[]).unwrap().is_empty());
        assert!(wrapper
            .embed_batch(&[""])
            .unwrap_err()
            .contains("must not be empty"));
        assert!(wrapper
            .embed_batch(&vec!["bounded"; MAX_REGENERATION_CHUNKS + 1])
            .unwrap_err()
            .contains("maximum is"));
        let oversized_source = "x".repeat(MAX_REGENERATION_TEXT_BYTES + 1);
        assert!(wrapper
            .embed_batch(&[oversized_source.as_str()])
            .unwrap_err()
            .contains("byte call budget"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn refuses_invalid_limits_and_provider_spaces() {
        let provider = TestProvider::new(ProviderOutput::Valid);
        assert!(BoundedCachingEmbeddingProvider::new(&provider, namespace(), 0, 0).is_err());
        assert!(BoundedCachingEmbeddingProvider::new(
            &provider,
            namespace(),
            MAX_PROVIDER_BATCH_ITEMS + 1,
            0
        )
        .is_err());
        assert!(BoundedCachingEmbeddingProvider::new(
            &provider,
            namespace(),
            1,
            MAX_EMBEDDING_CACHE_BYTES + 1
        )
        .is_err());

        let wrapper = wrapper(&provider, 1, 0);
        assert_eq!(wrapper.space(), &provider.space);
    }

    #[test]
    fn cache_eviction_keeps_the_configured_entry_budget() {
        let provider = TestProvider::new(ProviderOutput::Valid);
        // A 2-D vector plus its fixed entry allowance is 136 logical bytes.
        let wrapper = wrapper(&provider, 1, 136);
        wrapper.embed_batch(&["first"]).unwrap();
        wrapper.embed_batch(&["second"]).unwrap();
        assert_eq!(wrapper.cache.lock().len(), 1);
        wrapper.embed_batch(&["first"]).unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn provider_errors_cardinality_dimension_and_nonfinite_values_fail_closed() {
        for (mode, expected) in [
            (ProviderOutput::Error, "provider unavailable"),
            (ProviderOutput::WrongCardinality, "returned 1 vectors for 2"),
            (ProviderOutput::WrongDimension, "pinned space requires 2"),
            (ProviderOutput::NonFinite, "non-finite component"),
        ] {
            let provider = TestProvider::new(mode);
            let wrapper = wrapper(&provider, 2, 4096);
            let error = wrapper.embed_batch(&["one", "two"]).unwrap_err();
            assert!(error.contains(expected), "unexpected error: {error}");
        }
    }

    #[test]
    fn cancellation_between_batches_stops_work_without_returning_partial_vectors() {
        let cancel = Arc::new(AtomicBool::new(false));
        let mut provider = TestProvider::new(ProviderOutput::Valid);
        provider.cancel_on_call = Some(Arc::clone(&cancel));
        let wrapper = wrapper(&provider, 1, 4096);
        let probe = Arc::clone(&cancel);
        let result = crate::interrupt::scoped(
            move || probe.load(Ordering::SeqCst),
            || wrapper.embed_batch(&["first", "second"]),
        );
        assert!(result.unwrap_err().contains("cancelled"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn semantic_store_regeneration_uses_batched_provider_and_reuses_exact_binding() {
        let provider = TestProvider::new(ProviderOutput::Valid);
        let wrapper = wrapper(&provider, 2, 4096);
        let space = provider.space.clone();
        let mut store = SemanticStore::new_in_space(space.clone()).unwrap();
        store
            .add_embedding("present".into(), vec![6.0, 1.0])
            .unwrap();
        let present_text = "already present";
        let missing_text = "new chunk";
        let present_identity = EmbeddingChunkRef::for_text(space.clone(), present_text);
        let candidates = [
            EmbeddingRegenerationCandidate {
                node_id: "present",
                text: present_text,
                identity: present_identity.clone(),
                existing: Some(present_identity),
            },
            EmbeddingRegenerationCandidate {
                node_id: "missing",
                text: missing_text,
                identity: EmbeddingChunkRef::for_text(space, missing_text),
                existing: None,
            },
        ];

        let report = store
            .regenerate_eligible(&candidates, Some(&wrapper))
            .unwrap();
        assert_eq!(report.reused, vec!["present"]);
        assert_eq!(report.regenerated, vec!["missing"]);
        assert_eq!(store.get_embedding("missing"), Some(vec![9.0, 1.0]));
        assert_eq!(provider.batches(), vec![vec![missing_text.to_string()]]);
    }

    #[test]
    fn cache_key_separates_tenant_source_scope_policy_and_embedding_space() {
        let provider = TestProvider::new(ProviderOutput::Valid);
        let base = wrapper(&provider, 2, 4096);
        let same_text = "shared source text";
        let base_key = base.cache_key(same_text).unwrap();

        for other_namespace in [
            CacheNamespace::new("tenant-b", "corpus-a", "encoding-v1").unwrap(),
            CacheNamespace::new("tenant-a", "corpus-b", "encoding-v1").unwrap(),
            CacheNamespace::new("tenant-a", "corpus-a", "encoding-v2").unwrap(),
        ] {
            let other =
                BoundedCachingEmbeddingProvider::new(&provider, other_namespace, 2, 4096).unwrap();
            assert_ne!(base_key, other.cache_key(same_text).unwrap());
        }

        let other_space_provider = TestProvider {
            space: EmbeddingSpaceRef::pinned(
                "test-model",
                "revision-2",
                "model-content-hash-2",
                "preprocessing-hash",
                2,
                true,
            )
            .unwrap(),
            ..TestProvider::new(ProviderOutput::Valid)
        };
        let other_space = wrapper(&other_space_provider, 2, 4096);
        assert_ne!(base_key, other_space.cache_key(same_text).unwrap());
    }

    #[test]
    fn cache_namespace_requires_each_authority_coordinate() {
        assert!(CacheNamespace::new("", "corpus", "policy-v1")
            .unwrap_err()
            .contains("tenant_id"));
        assert!(CacheNamespace::new("tenant", "  ", "policy-v1")
            .unwrap_err()
            .contains("source_scope"));
        assert!(CacheNamespace::new("tenant", "corpus", "")
            .unwrap_err()
            .contains("encoding_policy_revision"));
        assert!(CacheNamespace::new(
            "tenant",
            "corpus",
            "x".repeat(MAX_NAMESPACE_COMPONENT_BYTES + 1)
        )
        .is_err());
    }
}
