//! Result bodies of the served namespaced key-value surface
//! (`KvGet`/`KvPut`/`KvDelete`/`KvScan`/`KvCas`, MEDULLA-DURABLE-KERNEL-R102).
//!
//! Every served row carries a `version`: the namespace's mutation-kernel commit
//! version at the write that produced it. It is unique and strictly increasing
//! within one namespace, across deletes and re-creates, so a `KvCas` against an
//! observed version cannot be fooled by an A-B-A rewrite. `0` means "absent".

use serde::{Deserialize, Serialize};

/// One stored value and the version of the write that produced it (`KvGet`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KvEntry {
    /// The caller's value bytes, verbatim, as a MessagePack `bin`.
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    /// Commit version of the write that stored `value`.
    pub version: u64,
}

/// The committed version of an unconditional `KvPut`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KvWriteReceipt {
    /// Commit version now stored at the key.
    pub version: u64,
}

/// The verdict of a `KvCas`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KvCasOutcome {
    /// Whether the stored version matched `expected_version` and the swap happened.
    pub swapped: bool,
    /// The key's version after the call: the new commit version when swapped (`0`
    /// when the swap deleted the key), else the version the comparison observed.
    pub version: u64,
}

/// One row of a `KvScan` page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KvScanRow {
    pub key: String,
    #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
    pub version: u64,
}

/// One bounded page of a prefix scan, in key order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KvScanPage {
    pub entries: Vec<KvScanRow>,
    /// Pass back as `cursor` to read the next page; `None` when the prefix is exhausted.
    pub next_cursor: Option<String>,
}
