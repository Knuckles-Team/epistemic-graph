//! Typed unseal failures (EH-384, CONCEPT:EG-KG.storage.node-payload-scrub).
//!
//! `DurableCrypto::unseal` reports why a stored value cannot be opened as a
//! plain `String`, which is fine for a commit that is refused anyway but useless
//! to anything that has to NAME the unreadable row: the background scrub, a
//! metric, an operator. [`UnsealFailure`] is the closed set of reasons and
//! [`NodeUnreadable`] binds one to the exact node row it was found on.
//!
//! The read and dump paths still return `Result<_, String>` to their callers,
//! so a [`NodeUnreadable`] crosses that boundary as its `Display` form, which
//! leads with the stable `NODE_UNREADABLE:` code, the same convention as the
//! kernel's `STALE_VERSION:` and `IDEMPOTENCY_CONFLICT:` refusals. A read of a
//! corrupt node therefore fails closed and names the row; nothing skips it.

use std::fmt;

/// Why one stored value blob could not be opened. Closed and exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsealFailure {
    /// The stored blob, or the plaintext it opens to, exceeds the durable
    /// resource bound.
    Oversize,
    /// A sealed blob in a deployment with no configured data key.
    SealedWithoutKey,
    /// A plaintext blob while a data key is active.
    MissingFraming,
    /// AEAD authentication failed: a wrong key or tampered ciphertext.
    Authentication,
}

impl UnsealFailure {
    /// The human message. Byte-identical to the strings `unseal` returned
    /// before this type existed, so no caller that matches on them changes.
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Oversize => "durable value exceeds resource limits",
            Self::SealedWithoutKey => "encrypted durable value requires configured key material",
            Self::MissingFraming => "encrypted durable value is missing sealed framing",
            Self::Authentication => "decryption failed (wrong key or tampered ciphertext)",
        }
    }

    /// Stable machine code, used as the metric label.
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::Oversize => "oversize",
            Self::SealedWithoutKey => "sealed_without_key",
            Self::MissingFraming => "missing_framing",
            Self::Authentication => "authentication",
        }
    }
}

/// One node row whose stored payload cannot be opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeUnreadable {
    pub(crate) graph: String,
    pub(crate) key: String,
    pub(crate) cause: UnsealFailure,
}

impl NodeUnreadable {
    pub(crate) fn new(graph: &str, key: &str, cause: UnsealFailure) -> Self {
        Self {
            graph: graph.to_string(),
            key: key.to_string(),
            cause,
        }
    }
}

impl fmt::Display for NodeUnreadable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NODE_UNREADABLE: graph '{}' node '{}' ({}): {}",
            self.graph,
            self.key,
            self.cause.code(),
            self.cause.message()
        )
    }
}

impl From<NodeUnreadable> for String {
    fn from(unreadable: NodeUnreadable) -> Self {
        unreadable.to_string()
    }
}
