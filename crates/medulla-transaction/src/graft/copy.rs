//! Verbatim ledger-table copying and destination fence restoration.

use super::*;

/// Put the destination's route fence back to what it was before the graft.
///
/// The copy is verbatim, so the destination inherits the source's graft fence
/// at `(u64::MAX, u64::MAX)` — which would leave it admitting nothing at all.
/// The marker recorded the fence in force before phase A precisely so it can be
/// restored here, in the same transaction as the copy.
pub(super) fn restore_fence<D: OwnerDomain>(
    write: &AdmittedMutation<'_, D>,
    scope: &str,
    identity: &MutationScopeIdentity,
    intent: &GraftIntent,
) -> Result<(), String> {
    let fence = ScopeFence {
        identity: identity.clone(),
        placement_epoch: intent.restore_epoch,
        fencing_token: intent.restore_token,
    };
    let bytes = encode_bounded(&fence, "grafted mutation fence")?;
    write.scoped_table(FENCES)?.insert(scope, bytes.as_slice())
}

/// Phase C: retire the source binding, under the same version fence.
///
/// Idempotent: a source whose binding is already gone cannot be opened, and
/// that is the completed state. The version is re-proved INSIDE the retirement
/// transaction, so a write that reached the source after phase B is refused
/// rather than swept away with the rest of the scope.
pub(super) fn retire_source<S: OwnerDomain>(
    source: &GraftSource<'_, S>,
    identity: &MutationScopeIdentity,
    intent: &GraftIntent,
) -> Result<(), String> {
    let write = match source.kernel.open_write(source.owner) {
        Ok(write) => write,
        Err(error) => {
            // Distinguish "already retired" from a transient failure through
            // the storage kernel's binding-aware probe. Reading any error as
            // success is how a graft would report one authority while leaving
            // two.
            return match source.storage.scope_is_bound(source.owner) {
                Ok(true) => Err(format!("graft could not retire its source scope: {error}")),
                Ok(false) => Ok(()),
                Err(probe_error) => Err(format!(
                    "graft could not prove source retirement: {error}; binding probe failed: {probe_error}"
                )),
            };
        }
    };
    match retire_in(&write, identity, intent, source.payload) {
        Ok(()) => write.commit(),
        Err(error) => {
            write.abort()?;
            Err(error)
        }
    }
}

fn retire_in<S: OwnerDomain>(
    write: &AdmittedMutation<'_, S>,
    identity: &MutationScopeIdentity,
    intent: &GraftIntent,
    payload: Option<&dyn OwnerPayloadRetirement<S>>,
) -> Result<(), String> {
    write.verify_scope(identity)?;
    let version = crate::ledger::bound_scope_version(write, identity)?;
    if version != intent.version {
        return Err(format!(
            "graft source moved from version {} to {} before it was retired",
            intent.version, version
        ));
    }
    crate::commit::purge_scope_for_graft(write, identity, payload)
}

/// Copy one scope's rows of one ledger table, verbatim.
///
/// Both key and value are re-inserted exactly as the source stored them: the
/// row bytes are never decoded, so a receipt, a fence, an outbox event or a
/// delivery lease crosses the file boundary unchanged, and the identity stamped
/// inside it still resolves against the destination's binding of the same
/// scope.
fn copy_rows<'k, S, D, K, V>(
    source: &ScopedRead<'_, S>,
    write: &AdmittedMutation<'_, D>,
    definition: TableDefinition<'static, K, V>,
    low: K::SelfType<'k>,
    high: K::SelfType<'k>,
) -> Result<u64, String>
where
    S: OwnerDomain,
    D: OwnerDomain,
    K: redb::Key + 'static,
    for<'a> K::SelfType<'a>: LedgerRowScope,
    V: redb::Value + 'static,
{
    let readable = source.scoped_table(definition)?;
    let mut writable = write.scoped_table(definition)?;
    let mut copied = 0u64;
    for row in readable.range_inclusive(low, high)? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        writable.insert(key.value(), value.value())?;
        copied = copied.saturating_add(1);
    }
    Ok(copied)
}

/// Every ledger table keyed by the scope key alone.
macro_rules! visit_scope_keyed {
    ($visit:ident) => {{
        $visit!(VERSIONS);
        $visit!(FENCES);
    }};
}

/// Every ledger table keyed `(scope, text)`.
macro_rules! visit_text_keyed {
    ($visit:ident) => {{
        $visit!(BATCHES);
        $visit!(MAINTENANCE);
        $visit!(PRIVATE_PAYLOADS);
        $visit!(CLASSES);
        $visit!(REPLAY_NONCES);
        $visit!(REPLAY_OPERATIONS);
        $visit!(OUTBOX_CONSUMERS);
        $visit!(OUTBOX_CURSORS);
        $visit!(OUTBOX_CLAIM_CURSORS);
        $visit!(OUTBOX_FAIRNESS);
    }};
}

/// The three delivery-side tables whose keys carry more than one component
/// after the scope, each with its own arity.
macro_rules! visit_wide_keyed {
    ($event:ident, $delivery:ident, $index:ident) => {{
        $event!(OUTBOX);
        $delivery!(OUTBOX_DELIVERIES);
        $index!(OUTBOX_TOPIC_INDEX);
    }};
}

/// Copy the whole ledger of one scope, table by table.
///
/// The three key arities need three different range bounds, which is why this
/// cannot be one expansion of the kernel's own `visit_ledger_tables!`. What
/// keeps the two lists from drifting is [`grafted_table_names`], which expands
/// exactly these macros and is asserted equal to
/// `crate::tables::ledger_table_names()`.
pub(super) fn copy_all<S: OwnerDomain, D: OwnerDomain>(
    source: &ScopedRead<'_, S>,
    write: &AdmittedMutation<'_, D>,
    scope: &str,
) -> Result<u64, String> {
    let mut rows = 0u64;
    macro_rules! scope_keyed {
        ($table:expr) => {{
            rows = rows.saturating_add(copy_rows(source, write, $table, scope, scope)?);
        }};
    }
    macro_rules! text_keyed {
        ($table:expr) => {{
            rows = rows.saturating_add(copy_rows(
                source,
                write,
                $table,
                (scope, ""),
                (scope, MAX_BATCH_ID_SENTINEL),
            )?);
        }};
    }
    macro_rules! event_keyed {
        ($table:expr) => {{
            rows = rows.saturating_add(copy_rows(
                source,
                write,
                $table,
                (scope, "", 0),
                (scope, MAX_BATCH_ID_SENTINEL, u32::MAX),
            )?);
        }};
    }
    macro_rules! delivery_keyed {
        ($table:expr) => {{
            rows = rows.saturating_add(copy_rows(
                source,
                write,
                $table,
                (scope, "", "", 0),
                (
                    scope,
                    MAX_BATCH_ID_SENTINEL,
                    MAX_BATCH_ID_SENTINEL,
                    u32::MAX,
                ),
            )?);
        }};
    }
    macro_rules! index_keyed {
        ($table:expr) => {{
            rows = rows.saturating_add(copy_rows(
                source,
                write,
                $table,
                (scope, "", 0, 0, "", 0),
                (
                    scope,
                    MAX_BATCH_ID_SENTINEL,
                    u64::MAX,
                    u64::MAX,
                    MAX_BATCH_ID_SENTINEL,
                    u32::MAX,
                ),
            )?);
        }};
    }
    visit_scope_keyed!(scope_keyed);
    visit_text_keyed!(text_keyed);
    visit_wide_keyed!(event_keyed, delivery_keyed, index_keyed);
    Ok(rows)
}

/// Every table [`copy_all`] moves, derived from the same three lists.
///
/// It exists to be compared against `crate::tables::ledger_table_names()`, so a
/// table added to the kernel's census and forgotten here fails a test instead
/// of silently staying behind in the source file on every graft.
#[cfg(test)]
pub(crate) fn grafted_table_names() -> Vec<&'static str> {
    use redb::TableHandle;
    let mut names = Vec::new();
    macro_rules! push {
        ($table:expr) => {{
            names.push($table.name());
        }};
    }
    visit_scope_keyed!(push);
    visit_text_keyed!(push);
    visit_wide_keyed!(push, push, push);
    names
}
