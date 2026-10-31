//! Worth-based admission and eviction for one cache tier (MEDULLA-DURABLE-KERNEL-R800).
//!
//! `worth = hits * rebuild_cost_ms / bytes`. An entry is admitted only when its
//! worth reaches the tier's `min_worth`; under byte pressure the lowest-worth entry
//! leaves first through a lazily-pruned min-heap, so each admission or hit costs
//! O(log N). An answer entry stops being served once any dependency generation it
//! was built at advances, or once its volatility window passes. Every refusal is a
//! miss with no content: this policy never returns a stale or cross-tenant hit.
//!
//! The wiring into the served versioned KV tier is MEDULLA-DURABLE-KERNEL-R800.3.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use crate::tiered::Tier;

/// The generation of one dependency an entry was derived from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GenerationRef {
    pub source: String,
    pub generation: u64,
}

/// Bookkeeping for one cached entry.
#[derive(Clone, Debug, PartialEq)]
pub struct CacheEntryMeta {
    pub key_digest: String,
    pub tier: Tier,
    pub bytes: u64,
    pub hits: u64,
    pub rebuild_cost_ms: u64,
    pub deps: Vec<GenerationRef>,
}

impl CacheEntryMeta {
    /// `hits * rebuild_cost_ms / bytes`; a zero-byte entry has no worth to weigh.
    pub fn worth(&self) -> f64 {
        if self.bytes == 0 {
            return 0.0;
        }
        self.hits as f64 * self.rebuild_cost_ms as f64 / self.bytes as f64
    }
}

/// The byte budget and admission floor of one tier.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TierPolicy {
    pub max_bytes: u64,
    pub min_worth: f64,
}

/// Why an admission was refused. Each refusal stores nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionRefusal {
    /// The entry alone is larger than the tier.
    Oversize { bytes: u64, max_bytes: u64 },
    /// A dependency names a source the generation clock does not know, or a
    /// generation other than its current one.
    UnknownGeneration(GenerationRef),
    /// The entry's worth is below the tier's `min_worth`.
    BelowMinWorth,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Worth(f64);

impl Eq for Worth {}

impl PartialOrd for Worth {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Worth {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

type TenantKey = (String, String);

struct Resident {
    meta: CacheEntryMeta,
    content: Vec<u8>,
    expires_at_ms: u64,
    stamp: u64,
}

/// One tier governed by a [`TierPolicy`].
pub struct PolicyTier {
    policy: TierPolicy,
    generations: HashMap<String, u64>,
    entries: HashMap<TenantKey, Resident>,
    heap: BinaryHeap<Reverse<(Worth, u64, TenantKey)>>,
    used_bytes: u64,
    next_stamp: u64,
}

impl PolicyTier {
    pub fn new(policy: TierPolicy) -> Self {
        Self {
            policy,
            generations: HashMap::new(),
            entries: HashMap::new(),
            heap: BinaryHeap::new(),
            used_bytes: 0,
            next_stamp: 0,
        }
    }

    /// Declare or advance a dependency source's current generation. Entries built
    /// at an older generation stop being served.
    pub fn set_generation(&mut self, source: impl Into<String>, generation: u64) {
        self.generations.insert(source.into(), generation);
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, tenant: &str, key_digest: &str) -> bool {
        self.entries
            .contains_key(&(tenant.to_string(), key_digest.to_string()))
    }

    /// Admit `content` for `tenant`, valid until `expires_at_ms`. Evicts the
    /// lowest-worth residents until the entry fits.
    pub fn admit(
        &mut self,
        tenant: &str,
        meta: CacheEntryMeta,
        content: Vec<u8>,
        expires_at_ms: u64,
    ) -> Result<(), AdmissionRefusal> {
        if meta.bytes > self.policy.max_bytes {
            return Err(AdmissionRefusal::Oversize {
                bytes: meta.bytes,
                max_bytes: self.policy.max_bytes,
            });
        }
        if let Some(dep) = meta.deps.iter().find(|dep| !self.dep_is_current(dep)) {
            return Err(AdmissionRefusal::UnknownGeneration(dep.clone()));
        }
        if meta.worth() < self.policy.min_worth {
            return Err(AdmissionRefusal::BelowMinWorth);
        }
        let key = (tenant.to_string(), meta.key_digest.clone());
        self.remove(&key);
        while self.used_bytes + meta.bytes > self.policy.max_bytes {
            if self.evict_lowest().is_none() {
                break;
            }
        }
        self.used_bytes += meta.bytes;
        let stamp = self.push_heap(&key, meta.worth());
        self.entries.insert(
            key,
            Resident {
                meta,
                content,
                expires_at_ms,
                stamp,
            },
        );
        Ok(())
    }

    /// Serve `tenant`'s entry, counting a hit. A stale, expired or absent entry is
    /// a miss; a stale or expired entry is dropped on the way out.
    pub fn lookup(&mut self, tenant: &str, key_digest: &str, now_ms: u64) -> Option<Vec<u8>> {
        let key = (tenant.to_string(), key_digest.to_string());
        let fresh = {
            let resident = self.entries.get(&key)?;
            now_ms < resident.expires_at_ms
                && resident
                    .meta
                    .deps
                    .iter()
                    .all(|dep| self.dep_is_current(dep))
        };
        if !fresh {
            self.remove(&key);
            return None;
        }
        let worth = {
            let resident = self.entries.get_mut(&key)?;
            resident.meta.hits += 1;
            resident.meta.worth()
        };
        let stamp = self.push_heap(&key, worth);
        let resident = self.entries.get_mut(&key)?;
        resident.stamp = stamp;
        Some(resident.content.clone())
    }

    /// Remove and return the lowest-worth resident's metadata.
    pub fn evict_lowest(&mut self) -> Option<CacheEntryMeta> {
        while let Some(Reverse((_, stamp, key))) = self.heap.pop() {
            if self.entries.get(&key).is_some_and(|r| r.stamp == stamp) {
                return self.remove(&key);
            }
        }
        None
    }

    fn dep_is_current(&self, dep: &GenerationRef) -> bool {
        self.generations.get(&dep.source) == Some(&dep.generation)
    }

    fn push_heap(&mut self, key: &TenantKey, worth: f64) -> u64 {
        let stamp = self.next_stamp;
        self.next_stamp += 1;
        self.heap.push(Reverse((Worth(worth), stamp, key.clone())));
        stamp
    }

    fn remove(&mut self, key: &TenantKey) -> Option<CacheEntryMeta> {
        let resident = self.entries.remove(key)?;
        self.used_bytes -= resident.meta.bytes;
        Some(resident.meta)
    }
}
