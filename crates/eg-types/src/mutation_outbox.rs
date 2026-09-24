//! The operator view of one owner's mutation outbox (X10).
//!
//! Every durable owner in the engine publishes its committed mutations on an
//! outbox, and until now the only way to see one was to read the store. This
//! surface makes the three questions an operator actually asks answerable over
//! the wire: how far behind is this consumer, what did it give up on, and can
//! I make it try again.
//!
//! `rewind` is the only mutating operation, and it is deliberately the only
//! one: re-delivery is safe because consumers are idempotent, whereas dropping
//! a dead letter is not recoverable and is therefore not offered here at all.

use serde::{Deserialize, Serialize};

use crate::contract::BoundedVec;

/// Format identity (RF-ADR-006) of the outbox views.
pub const MUTATION_OUTBOX_VIEW_SCHEMA_VERSION: u16 = 1;

/// Largest dead-letter page one read may return.
pub const MAX_OUTBOX_DEAD_LETTER_PAGE: u32 = 256;

/// Which native owner's outbox this is.
///
/// Closed, and closed on purpose: an outbox nobody named is an outbox nobody
/// monitors, so a new durable owner has to be added here to be operable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum NativeOutboxStore {
    AgentLibrary,
    SemanticIndex,
    Jobs,
    SqlCatalog,
}

/// Which ledger scope of a native store an operation names.
///
/// The Agent Library and the jobs store keep one outbox per tenant; a
/// semantic index keeps one per binding, and a SQL catalog one per resource
/// scope. The selector names the scope the store actually keys its outbox by,
/// so an operator can reach every outbox without the engine guessing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum NativeOutboxScope {
    /// The tenant's one scope (Agent Library, jobs).
    #[default]
    Tenant,
    /// One semantic binding's serving scope.
    SemanticBinding { binding_id: String },
    /// One resource scope of the tenant's shared SQL catalog.
    SqlResource { resource: String },
}

/// Which outbox an operation names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "target", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum OutboxTarget {
    Graph {
        graph: String,
    },
    NativeStore {
        store: NativeOutboxStore,
        tenant_id: String,
        #[serde(default)]
        scope: NativeOutboxScope,
    },
}

impl OutboxTarget {
    /// A native target must name the scope kind its store keys outboxes by.
    pub fn validate(&self) -> Result<(), String> {
        let Self::NativeStore { store, scope, .. } = self else {
            return Ok(());
        };
        let matches = match scope {
            NativeOutboxScope::Tenant => {
                matches!(
                    store,
                    NativeOutboxStore::AgentLibrary | NativeOutboxStore::Jobs
                )
            }
            NativeOutboxScope::SemanticBinding { binding_id } => {
                *store == NativeOutboxStore::SemanticIndex && !binding_id.is_empty()
            }
            NativeOutboxScope::SqlResource { resource } => {
                *store == NativeOutboxStore::SqlCatalog && !resource.is_empty()
            }
        };
        if matches {
            return Ok(());
        }
        Err(format!(
            "mutation outbox scope {scope:?} does not address a {store:?} outbox"
        ))
    }
}

/// One position in an outbox, exactly as the kernel orders rows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutboxPositionView {
    pub sequence: u64,
    pub created_at_ms: u64,
    pub batch_id: String,
    pub ordinal: u32,
}

/// Where a rewind puts the consumer's cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "to", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RewindTarget {
    /// Re-deliver everything the outbox still retains.
    Start,
    At {
        position: OutboxPositionView,
    },
}

/// Every mutation-outbox operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MutationOutboxOp {
    Status {
        target: OutboxTarget,
        consumer: String,
    },
    DeadLetters {
        target: OutboxTarget,
        consumer: String,
        #[serde(default)]
        after: Option<OutboxPositionView>,
        /// At most [`MAX_OUTBOX_DEAD_LETTER_PAGE`].
        limit: u32,
    },
    Rewind {
        target: OutboxTarget,
        consumer: String,
        to: RewindTarget,
    },
}

impl MutationOutboxOp {
    /// Only a rewind writes; the two views read a snapshot.
    pub fn is_mutation(&self) -> bool {
        matches!(self, Self::Rewind { .. })
    }

    /// One action for all three: reading another consumer's backlog is as
    /// operationally privileged as replaying it.
    pub fn authz_action(&self) -> &'static str {
        "admin:outbox"
    }

    /// The consumer this operation names.
    pub fn consumer(&self) -> &str {
        match self {
            Self::Status { consumer, .. }
            | Self::DeadLetters { consumer, .. }
            | Self::Rewind { consumer, .. } => consumer,
        }
    }

    /// The outbox this operation names.
    pub fn target(&self) -> &OutboxTarget {
        match self {
            Self::Status { target, .. }
            | Self::DeadLetters { target, .. }
            | Self::Rewind { target, .. } => target,
        }
    }

    /// Bounds, by the name of the field that is out of them.
    pub fn validate(&self) -> Result<(), String> {
        if self.consumer().is_empty() {
            return Err("mutation outbox consumer must be named".to_string());
        }
        self.target().validate()?;
        match self {
            Self::DeadLetters { limit, .. } if *limit == 0 => {
                Err("mutation outbox dead-letter limit must be at least one".to_string())
            }
            Self::DeadLetters { limit, .. } if *limit > MAX_OUTBOX_DEAD_LETTER_PAGE => Err(
                format!("mutation outbox dead-letter limit exceeds {MAX_OUTBOX_DEAD_LETTER_PAGE}"),
            ),
            Self::Status { .. } | Self::DeadLetters { .. } | Self::Rewind { .. } => Ok(()),
        }
    }
}

/// The oldest undelivered row, when there is one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutboxHeadView {
    pub position: OutboxPositionView,
    pub attempt: u32,
    pub leased: bool,
    pub age_ms: u64,
}

/// One consumer's standing on one outbox -- the fields that mean the same
/// thing regardless of WHICH durable outbox backs the view. Shared by
/// [`MutationOutboxStatusView`] (this generic outbox) and
/// [`crate::result_contract::ingestion::SemanticOutboxStatus`] (the
/// semantic-index stage queue's own outbox), which otherwise built an
/// identical 13-field struct independently; each view keeps its own
/// system-specific fields (a schema version and the head-of-queue detail
/// here, claim counters there) beside this rather than inside it, and this
/// type is neither `#[serde(flatten)]`ed nor a wire-visible name of its
/// own's alternative -- it is a genuinely nested field, precisely so both
/// containers can keep `#[serde(deny_unknown_fields)]`, which `flatten`
/// cannot coexist with.
///
/// Several counters carry an explicit `_is_lower_bound` flag rather than being
/// silently approximate: a saturated index can only be scanned so far, and an
/// operator deciding whether to rewind needs to know that "12" might mean
/// "at least 12".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutboxConsumerStatus {
    pub consumer: String,
    #[serde(default)]
    pub topic: Option<String>,
    pub live: bool,
    pub capacity: u32,
    pub inflight: u32,
    pub inflight_is_lower_bound: bool,
    pub pending: u64,
    pub pending_is_lower_bound: bool,
    pub delivered: u64,
    pub dead_lettered: u64,
    pub oldest_pending_age_ms: u64,
    pub lag_rows: u64,
    pub lag_versions: u64,
    pub saturated: bool,
    pub index_complete: bool,
}

/// One consumer's standing on the generic mutation outbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MutationOutboxStatusView {
    pub schema_version: u16,
    pub status: OutboxConsumerStatus,
    #[serde(default)]
    pub head: Option<OutboxHeadView>,
}

/// One row the consumer gave up on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutboxDeadLetterView {
    pub position: OutboxPositionView,
    pub topic: String,
    pub event_schema: String,
    pub attempt: u32,
    /// The payload's digest, never the payload: a dead-letter listing is an
    /// operational read and must not become a way to read another tenant's
    /// mutation content.
    pub payload_digest: String,
}

/// One page of dead letters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutboxDeadLetterPage {
    pub schema_version: u16,
    pub consumer: String,
    pub items: BoundedVec<OutboxDeadLetterView, 256>,
    #[serde(default)]
    pub next_after: Option<OutboxPositionView>,
}

/// What a rewind did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OutboxRewindReceipt {
    pub schema_version: u16,
    pub consumer: String,
    pub to: RewindTarget,
    /// False when the rewind ran out of its bounded transaction budget and
    /// must be called again to finish.
    pub completed: bool,
    pub deleted_deliveries: u64,
}

#[cfg(test)]
mod scope_tests {
    use super::*;

    fn native(store: NativeOutboxStore, scope: NativeOutboxScope) -> OutboxTarget {
        OutboxTarget::NativeStore {
            store,
            tenant_id: "tenant-a".to_string(),
            scope,
        }
    }

    #[test]
    fn every_store_is_addressed_by_its_own_scope_kind_only() {
        let binding = NativeOutboxScope::SemanticBinding {
            binding_id: "b".to_string(),
        };
        let resource = NativeOutboxScope::SqlResource {
            resource: "orders".to_string(),
        };
        let cases = [
            (
                NativeOutboxStore::AgentLibrary,
                NativeOutboxScope::Tenant,
                true,
            ),
            (NativeOutboxStore::Jobs, NativeOutboxScope::Tenant, true),
            (NativeOutboxStore::SemanticIndex, binding.clone(), true),
            (NativeOutboxStore::SqlCatalog, resource.clone(), true),
            (
                NativeOutboxStore::SemanticIndex,
                NativeOutboxScope::Tenant,
                false,
            ),
            (NativeOutboxStore::SqlCatalog, binding, false),
            (NativeOutboxStore::AgentLibrary, resource, false),
            (
                NativeOutboxStore::SqlCatalog,
                NativeOutboxScope::SqlResource {
                    resource: String::new(),
                },
                false,
            ),
        ];
        for (store, scope, valid) in cases {
            assert_eq!(
                native(store, scope.clone()).validate().is_ok(),
                valid,
                "{store:?} {scope:?}"
            );
        }
    }

    #[test]
    fn an_omitted_scope_is_the_tenant_scope() {
        let target: OutboxTarget = serde_json::from_value(serde_json::json!({
            "target": "native_store",
            "store": "jobs",
            "tenant_id": "tenant-a"
        }))
        .unwrap();
        assert_eq!(
            target,
            native(NativeOutboxStore::Jobs, NativeOutboxScope::Tenant)
        );
    }
}
