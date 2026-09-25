//! What a foreign source can evaluate remotely, and what the optimizer asks of it.

use serde::Serialize;

/// Whether a source answers batched key lookups (`id IN (…)`), and how many keys one
/// request may carry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum KeyLookup {
    /// Keys cannot be pushed; a join fetches the source and intersects locally.
    Unsupported,
    /// Up to `max_keys` keys per request (a source may shrink a batch further to fit its
    /// request-size limit).
    Batched { max_keys: usize },
}

/// Whether a row limit can be pushed into the source's own request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum LimitPushdown {
    Unsupported,
    Native,
}

/// How a source returns a large result.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Paging {
    /// One request returns the whole (streamed) result.
    Single,
    /// Row-offset pages (`{offset}`).
    Offset,
    /// Page-number pages (`{page}`, 1-based).
    Page,
}

/// Whether the source can be read without keys at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum FullFetch {
    Allowed,
    /// A key-lookup-only API: without keys the optimizer refuses with
    /// [`crate::federation_opt::REQUIRES_KEYS`].
    RequiresKeys,
}

/// The capability row the optimizer plans against (design §3). Derived from the source
/// spec, never from the caller's request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct SourceCapabilities {
    pub key_lookup: KeyLookup,
    pub limit: LimitPushdown,
    pub paging: Paging,
    pub full_fetch: FullFetch,
}

impl SourceCapabilities {
    /// A non-paged source that permits a full read but may also push keys or a limit.
    pub const fn single_full_fetch(key_lookup: KeyLookup, limit: LimitPushdown) -> Self {
        Self {
            key_lookup,
            limit,
            paging: Paging::Single,
            full_fetch: FullFetch::Allowed,
        }
    }

    /// A source that can only be fetched whole (a remote engine today, a registry-only
    /// table/closure source): every request is answered by the full result.
    pub const fn fetch_only() -> Self {
        Self::single_full_fetch(KeyLookup::Unsupported, LimitPushdown::Unsupported)
    }

    /// The keys one request may carry, if key lookup is supported.
    pub fn max_keys(&self) -> Option<usize> {
        match self.key_lookup {
            KeyLookup::Unsupported => None,
            KeyLookup::Batched { max_keys } => Some(max_keys.max(1)),
        }
    }
}

/// One page of a paged read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct PageRequest {
    /// 0-based page index (`{page}` renders it 1-based).
    pub index: usize,
    /// Row offset of the page's first row.
    pub offset: usize,
    /// Rows per page.
    pub size: usize,
}

/// What the optimizer asks one remote fragment to return. Every field only narrows: a
/// source answers with a SUPERSET of the rows the local residual keeps, never fewer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RemoteRequest {
    /// Only rows whose id is one of these (empty ⇒ no key restriction).
    pub keys: Vec<String>,
    /// At most this many rows, in the source's own order.
    pub limit: Option<usize>,
    /// The page of a paged read.
    pub page: Option<PageRequest>,
}

impl RemoteRequest {
    /// The unrestricted request — the naive full fetch.
    pub fn full() -> Self {
        Self::default()
    }

    /// A key-lookup batch.
    pub fn keys(keys: Vec<String>) -> Self {
        Self {
            keys,
            ..Self::default()
        }
    }
}
