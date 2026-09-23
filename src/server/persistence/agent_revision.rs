//! The revision protocol the agent COMPONENT, GRAPH and TEMPLATE layers share.
//!
//! All three publish into the one Agent Library owner file with the same
//! protocol: a head row compare-and-swapped against the caller's expected
//! revision, an append-only revision row beside it, a replay identity resolved
//! nonce-first, one outbox intent per revision, and a typed receipt carrying the
//! committed result. They used to write that protocol out three times, as
//! record-typed copies that differed only in names and in the words of their
//! refusals. It is written once here, over [`RevisionLayer`], which is exactly
//! the part that differs: the record's types, its tables, its event, and what
//! it is called.
//!
//! # What stays with each layer
//!
//! * The public `publish_*` / `retire_*` entry points, which open the admitted
//!   write, and the `prepare_*_entry` step each hands to [`write_revision`].
//!   Admission of cross-record references is genuinely per layer -- a graph
//!   resolves its composition, a template its base, a component its pins --
//!   and the pinned-reference gate anchors each layer on that step.
//! * Each layer's tables, named by a `<layer>_tables()` function in the layer's
//!   own module, so every read of a layer's store stays visible where it is
//!   made.
//! * Everything that is not the shared protocol: component search, graph
//!   composition, template instantiation, and the component status read.
//!
//! The Agent Library layer (L2) keeps its own write path. Its replay check binds
//! the submitted draft and the full outbox effect, and it stamps an
//! authoritative commit time, so it shares only the steps that are identical:
//! [`recorded_receipt`], [`write::finish_replayed`], [`write::within_write`],
//! [`write::revision_scope`], [`write::policy_admitted_context`],
//! [`write::stage_batch`], [`write::apply_owner_rows`],
//! [`write::finish_committed_ledger`], [`lifecycle_operations`] and
//! [`definition_headers`].
//!
//! # Refusal texts
//!
//! Every refusal is built from the layer's [`RevisionLayer::NOUN`] and
//! [`RevisionLayer::RECORD`], so each layer's messages are byte-identical to the
//! ones its own copy produced. That includes the component layer's, which have
//! always named the record a "graph" in two places; callers match on these
//! strings, so they are carried rather than silently corrected.

use std::collections::BTreeMap;

use redb::ReadableTable;

use eg_storage::{RecordedOperation, ScopedRead};
use eg_transaction::{AdmittedOwnerWrite, ReplayResolution};
use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{
    DurabilityDomain, MutationBatch, MutationOperation, MutationOutboxIntent, MutationSurface,
};
use eg_types::protocol::Method;

use super::agent_library::{
    native_lifecycle_batch, next_revision, require_expected_revision, AgentLibraryStore,
};
use super::agent_row;

mod status;
#[cfg(test)]
mod tests;
mod write;

pub(super) use status::{committed_status, ledger_status_record, status_result_bytes};
#[cfg(test)]
pub(super) use status::{decode_status_result, validate_status_record};
pub(super) use write::{
    apply_owner_rows, finish_committed_ledger, finish_replayed, policy_admitted_context,
    require_context_tenant, retire_revision, revision_scope, stage_batch, within_write,
    write_revision, write_revision_with_rows, RevisionWrite, StagedBatch,
};

type Owner = eg_storage::AgentLibraryOwner;
type HeadsTable = redb::TableDefinition<'static, (&'static str, &'static str), u64>;
type RevisionsTable =
    redb::TableDefinition<'static, (&'static str, &'static str, u64), &'static [u8]>;

/// One layer's head and revision tables.
#[derive(Clone, Copy)]
pub(super) struct RevisionTables {
    pub(super) heads: HeadsTable,
    pub(super) revisions: RevisionsTable,
}

/// The lifecycle transitions a layer records. Every layer has `Publish`/
/// `Retire`; `Withdraw`/`Republish` exist only for [`super::agent_component`]'s
/// `ComponentLayer` (a pack entry that vanished from, then returned to, its
/// connector's current pack) -- the other layers' own `Kind` types have no
/// variant that ever constructs one, so they need no change here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RevisionVerb {
    Publish,
    Retire,
    Withdraw,
    Republish,
}

impl RevisionVerb {
    pub(super) fn as_str(self) -> &'static str {
        revision_verb_str(self)
    }

    /// The lifecycle a committed revision of this transition carries.
    pub(super) fn lifecycle(self) -> AgentLibraryLifecycle {
        revision_verb_lifecycle(self)
    }
}

/// [`RevisionVerb::as_str`]'s dispatch, kept as a free function so the trivial
/// two-arm shape every OTHER layer's transitions had before `ComponentLayer`
/// needed two more doesn't show as a regression on the method itself.
fn revision_verb_str(verb: RevisionVerb) -> &'static str {
    match verb {
        RevisionVerb::Publish => "publish",
        RevisionVerb::Retire => "retire",
        RevisionVerb::Withdraw => "withdraw",
        RevisionVerb::Republish => "republish",
    }
}

/// [`RevisionVerb::lifecycle`]'s dispatch (see [`revision_verb_str`] for why
/// it is a free function). `Republish` carries the same `Published` lifecycle
/// as `Publish` -- both are "this revision is live" -- and `Withdraw` carries
/// `Withdrawn`, the one lifecycle only `ComponentLayer` ever produces.
fn revision_verb_lifecycle(verb: RevisionVerb) -> AgentLibraryLifecycle {
    match verb {
        RevisionVerb::Publish | RevisionVerb::Republish => AgentLibraryLifecycle::Published,
        RevisionVerb::Retire => AgentLibraryLifecycle::Retired,
        RevisionVerb::Withdraw => AgentLibraryLifecycle::Withdrawn,
    }
}

/// The definition fields every layer's entry carries, borrowed.
pub(super) struct RevisionDefinition<'a> {
    pub(super) tenant_id: &'a str,
    pub(super) record_id: &'a str,
    pub(super) entry_revision: u64,
    pub(super) lifecycle: AgentLibraryLifecycle,
    pub(super) definition_digest: &'a str,
    pub(super) actor_scope: &'a str,
    pub(super) purpose_id: &'a str,
    pub(super) policy_digest: &'a str,
}

/// What differs between the layers that share the revision protocol.
pub(super) trait RevisionLayer {
    type Entry: Clone + serde::Serialize + serde::de::DeserializeOwned;
    type Kind: Copy;
    type Committed: serde::Serialize + serde::de::DeserializeOwned;
    type WriteResult;

    /// How a refusal names the layer: `agent component`.
    const NOUN: &'static str;
    /// How a refusal names one record of the layer in the tenant and replay
    /// checks. See the module notes for why it is not always the noun's tail.
    const RECORD: &'static str;
    /// The layer's slug: receipt ids, purposes (`<slug>:publish`) and event
    /// types (`<slug with underscores>_publish`) are derived from it.
    const SLUG: &'static str;
    /// The operation-identity prefix: `<operation>-publish`.
    const OPERATION: &'static str;
    const TOPIC: &'static str;
    const RESULT_SCHEMA_ID: &'static str;
    const SCHEMA_VERSION: u16;
    /// The outbox header that carries the record id.
    const ID_HEADER: &'static str;
    /// The layer's retire mutation kind.
    const RETIRE: Self::Kind;
    const MAX_REVISIONS: usize;
    const MAX_HISTORY_BYTES: usize;

    fn revision_definition(entry: &Self::Entry) -> RevisionDefinition<'_>;
    fn validate_entry(entry: &Self::Entry) -> Result<(), String>;
    fn verb(kind: Self::Kind) -> RevisionVerb;
    /// Build, validate and encode the layer's outbox event for one revision.
    fn encode_outbox_event(
        kind: Self::Kind,
        entry: &Self::Entry,
        context: &AgentLibraryMutationContext,
    ) -> Result<Vec<u8>, String>;
    /// Headers a layer adds to the shared definition headers.
    fn extend_outbox_headers(_entry: &Self::Entry, _headers: &mut BTreeMap<String, String>) {}
    fn retired_revision(
        entry: &Self::Entry,
        entry_revision: u64,
        retired_at_ms: u64,
    ) -> Result<Self::Entry, String>;
    fn committed(entry: Self::Entry, batch_id: String, committed_version: u64) -> Self::Committed;
    fn committed_entry(result: &Self::Committed) -> &Self::Entry;
    /// The committed result's batch id and committed version.
    fn committed_binding(result: &Self::Committed) -> (&str, u64);
    fn write_result(result: Self::Committed, replayed: bool) -> Self::WriteResult;
}

/// Head CAS plus the append-only revision row, in the layer's tables.
pub(super) fn apply_revision_rows<L: RevisionLayer>(
    owner_write: &AdmittedOwnerWrite<'_, Owner>,
    tables: RevisionTables,
    context: &AgentLibraryMutationContext,
    expected_revision: u64,
    entry: &L::Entry,
    entry_bytes: &[u8],
) -> Result<(), String> {
    let definition = L::revision_definition(entry);
    let key = (context.tenant_id.as_str(), definition.record_id);
    let mut heads = owner_write.open_table(tables.heads)?;
    let actual_revision = heads
        .get(key)
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    require_expected_revision(expected_revision, actual_revision)?;
    if definition.entry_revision != next_revision(expected_revision)? {
        return Err(format!(
            "{} entry revision does not follow its expected head",
            L::NOUN
        ));
    }
    let mut revisions = owner_write.open_table(tables.revisions)?;
    if actual_revision > 0 {
        let current = revisions
            .get((key.0, key.1, actual_revision))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("{} head points to a missing revision", L::NOUN))?;
        let current = decode_revision::<L>(current.value())?;
        if L::revision_definition(&current).lifecycle == AgentLibraryLifecycle::Retired {
            return Err(format!("retired {}s cannot be resurrected", L::NOUN));
        }
    }
    if revisions
        .get((key.0, key.1, definition.entry_revision))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err(format!("{} revision already exists", L::NOUN));
    }
    revisions
        .insert((key.0, key.1, definition.entry_revision), entry_bytes)
        .map_err(|error| error.to_string())?;
    heads
        .insert(key, definition.entry_revision)
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// The single lifecycle operation one agent-hierarchy mutation carries.
///
/// `query` is the whole record's definition digest, not just what it does: two
/// revisions with the same content but different metadata are different
/// mutations.
pub(super) fn lifecycle_operations(event_type: String, query: String) -> Vec<MutationOperation> {
    vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation { event_type, query },
    }]
}

pub(super) fn revision_operations<L: RevisionLayer>(
    kind: L::Kind,
    entry: &L::Entry,
) -> Vec<MutationOperation> {
    lifecycle_operations(
        format!("{}_{}", L::SLUG.replace('-', "_"), L::verb(kind).as_str()),
        L::revision_definition(entry).definition_digest.to_string(),
    )
}

/// The outbox key of one revision: `<tenant>:<record>:<revision>`.
pub(super) fn revision_key(definition: &RevisionDefinition<'_>) -> String {
    format!(
        "{}:{}:{}",
        definition.tenant_id, definition.record_id, definition.entry_revision
    )
}

/// The definition headers every layer's outbox intent carries.
pub(super) fn definition_headers(
    schema_version: u16,
    id_header: &str,
    definition: &RevisionDefinition<'_>,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("schema_version".to_string(), schema_version.to_string()),
        ("tenant_id".to_string(), definition.tenant_id.to_string()),
        (id_header.to_string(), definition.record_id.to_string()),
        (
            "entry_revision".to_string(),
            definition.entry_revision.to_string(),
        ),
        (
            "definition_digest".to_string(),
            definition.definition_digest.to_string(),
        ),
        (
            "definition_actor_scope".to_string(),
            definition.actor_scope.to_string(),
        ),
        (
            "definition_purpose_id".to_string(),
            definition.purpose_id.to_string(),
        ),
        (
            "definition_policy_digest".to_string(),
            definition.policy_digest.to_string(),
        ),
    ])
}

pub(super) fn revision_outbox_headers<L: RevisionLayer>(
    entry: &L::Entry,
) -> BTreeMap<String, String> {
    let mut headers = definition_headers(
        L::SCHEMA_VERSION,
        L::ID_HEADER,
        &L::revision_definition(entry),
    );
    L::extend_outbox_headers(entry, &mut headers);
    headers
}

pub(super) fn build_revision_batch<L: RevisionLayer>(
    owner: &eg_storage::OwnedStoreHandle<Owner>,
    context: &AgentLibraryMutationContext,
    kind: L::Kind,
    entry: &L::Entry,
    version: u64,
    batch_id: &str,
    event_bytes: Vec<u8>,
) -> Result<MutationBatch, String> {
    let outbox = vec![MutationOutboxIntent {
        topic: L::TOPIC.to_string(),
        key: revision_key(&L::revision_definition(entry)),
        payload: event_bytes,
        headers: revision_outbox_headers::<L>(entry),
    }];
    native_lifecycle_batch(
        owner,
        context,
        batch_id,
        version,
        revision_operations::<L>(kind, entry),
        outbox,
    )
}

/// Every retained revision of one record, oldest first, with its head.
pub(super) fn read_revision_history<L: RevisionLayer>(
    read: &ScopedRead<'_, Owner>,
    tables: RevisionTables,
    tenant_id: &str,
    record_id: &str,
) -> Result<(Option<u64>, Vec<L::Entry>), String> {
    let head = read
        .open_owner_table(tables.heads)?
        .get((tenant_id, record_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value());
    let table = read.open_owner_table(tables.revisions)?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for row in table
        .range((tenant_id, record_id, 0)..=(tenant_id, record_id, u64::MAX))
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        // Bounded: a caller can otherwise ask for an unbounded amount of work
        // by publishing revisions.
        if entries.len() >= L::MAX_REVISIONS {
            return Err(format!(
                "{} history exceeds its retained revision bound",
                L::NOUN
            ));
        }
        bytes = bytes.saturating_add(value.value().len());
        if bytes > L::MAX_HISTORY_BYTES {
            return Err(format!(
                "{} history exceeds its retained byte bound",
                L::NOUN
            ));
        }
        let (row_tenant, row_record, row_revision) = key.value();
        let entry = decode_revision::<L>(value.value())?;
        require_physical_key::<L>(&entry, row_tenant, row_record, row_revision)?;
        entries.push(entry);
    }
    Ok((head, entries))
}

/// Every retained revision of one record, oldest first, read from the store.
pub(super) fn revision_history<L: RevisionLayer>(
    store: &AgentLibraryStore,
    tables: RevisionTables,
    tenant_id: &str,
    record_id: &str,
) -> Result<Vec<L::Entry>, String> {
    eg_types::agent_library::validate_key(tenant_id, record_id)?;
    let read = store.read()?;
    Ok(read_revision_history::<L>(&read, tables, tenant_id, record_id)?.1)
}

/// One retained revision read inside the admitting write, or `None`.
pub(super) fn revision_at_in_write<L: RevisionLayer>(
    write: &eg_transaction::AdmittedMutation<'_, Owner>,
    tables: RevisionTables,
    tenant_id: &str,
    record_id: &str,
    revision: u64,
) -> Result<Option<L::Entry>, String> {
    if revision == 0 {
        return Ok(None);
    }
    let revisions = write.open_read_table(tables.revisions)?;
    let Some(value) = revisions.get((tenant_id, record_id, revision))? else {
        return Ok(None);
    };
    let entry = decode_revision::<L>(value.value())?;
    require_physical_key::<L>(&entry, tenant_id, record_id, revision)?;
    Ok(Some(entry))
}

/// Refuse a decoded row whose own identity is not the key it was read under.
pub(super) fn require_physical_key<L: RevisionLayer>(
    entry: &L::Entry,
    tenant_id: &str,
    record_id: &str,
    revision: u64,
) -> Result<(), String> {
    let definition = L::revision_definition(entry);
    if definition.tenant_id != tenant_id
        || definition.record_id != record_id
        || definition.entry_revision != revision
    {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} row does not match its physical key",
            L::NOUN
        ));
    }
    Ok(())
}

pub(super) fn decode_revision<L: RevisionLayer>(bytes: &[u8]) -> Result<L::Entry, String> {
    let entry: L::Entry = agent_row::decode(bytes, &format!("{} row", L::NOUN))?;
    L::validate_entry(&entry)?;
    Ok(entry)
}

pub(super) fn decode_committed<L: RevisionLayer>(bytes: &[u8]) -> Result<L::Committed, String> {
    let result: L::Committed = agent_row::decode(bytes, &format!("{} result", L::NOUN))?;
    L::validate_entry(L::committed_entry(&result))?;
    Ok(result)
}

pub(super) fn domain_result<L: RevisionLayer>(
    result: &L::Committed,
) -> Result<MutationResult, String> {
    agent_row::domain_result(result, L::RESULT_SCHEMA_ID, L::NOUN)
}

pub(super) fn encode_domain_result<L: RevisionLayer>(
    result: &L::Committed,
) -> Result<Vec<u8>, String> {
    eg_storage::encode_bounded(
        &domain_result::<L>(result)?,
        &format!("{} domain result", L::NOUN),
    )
}

/// The recorded receipt a replay resolved to, or `None` when it is fresh.
///
/// `noun` names the layer in the refusals.
pub(super) fn recorded_receipt(
    replay: ReplayResolution,
    noun: &str,
) -> Result<Option<MutationReceipt>, String> {
    let recorded = match replay {
        ReplayResolution::Fresh => return Ok(None),
        ReplayResolution::NonceRejected { idempotency_key } => {
            return Err(format!(
                "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
            ));
        }
        ReplayResolution::Conflict { .. } => {
            return Err(format!(
                "IDEMPOTENCY_CONFLICT: key was already used by a different {noun} mutation"
            ));
        }
        ReplayResolution::ReplayedResult(recorded) => *recorded,
    };
    let RecordedOperation::Receipt(receipt) = recorded else {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {noun} replay is missing its typed receipt"
        ));
    };
    Ok(Some(*receipt))
}

/// Turn a resolved replay into a caller result, or `None` when it is fresh.
pub(super) fn replayed_revision<L: RevisionLayer>(
    replay: ReplayResolution,
    record_id: &str,
) -> Result<Option<(L::WriteResult, MutationReceipt)>, String> {
    let Some(receipt) = recorded_receipt(replay, L::NOUN)? else {
        return Ok(None);
    };
    receipt.validate()?;
    let MutationResult::DomainResult { payload, .. } = &receipt.result else {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} replay receipt carries no domain result",
            L::NOUN
        ));
    };
    let committed = decode_committed::<L>(payload.as_slice())?;
    if L::revision_definition(L::committed_entry(&committed)).record_id != record_id {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} replay resolved a different {}",
            L::NOUN,
            L::RECORD
        ));
    }
    Ok(Some((L::write_result(committed, true), receipt)))
}
