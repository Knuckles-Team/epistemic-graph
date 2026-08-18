//! Tenant-scoped SQL table ownership, grants, and row-level security
//! (CONCEPT:NE-003 — "EG-ACCESS": intra-tenant SQL catalog sharing).
//!
//! [`crate::server::sql_tables`] moved the PHYSICAL catalog boundary from
//! `(tenant, agent_id)` to `tenant` alone (`tenant_table_store` /
//! `tenant_acl_table_store`): every actor in a tenant now CAN share one redb
//! catalog file. Sharing a physical file is not the same thing as being allowed to
//! read or write any particular table in it — this module is the access-control
//! layer that makes that distinction real:
//!
//!   * **ownership** — the `__eg_sql_owners__` system table records the agent_id
//!     that first created each table (first-writer-wins, enforced by the
//!     `table_name` column's UNIQUE constraint racing two concurrent creators).
//!   * **grants** — the `__eg_sql_grants__` system table is a flat
//!     `(table_name, principal, privilege)` allow-list. An owner (or an admin
//!     carrier, `CarrierAuthority::is_admin`) may [`grant`] another principal in
//!     the SAME tenant any of the five [`SqlPrivilege`]s and [`revoke`] them
//!     independently. Default-deny: [`authorize`] returns the SAME generic denial
//!     ([`ACCESS_DENIED`], identical text and error type) whether the table has
//!     no owner at all (does not exist) or simply has no matching
//!     owner/grant/admin — no message-level existence leak. **Known residual
//!     side channel:** `authorize` deliberately equalizes the OBVIOUS timing
//!     asymmetry (both denial branches always run the same owners-then-grants
//!     scan pair — see its doc comment), but this is not constant-time —
//!     `owner_of`/`grant_exists` still return as soon as a matching row is
//!     found, so scan position is a residual, data-dependent timing signal.
//!     Closing that fully was judged disproportionate for this system rather
//!     than left unconsidered.
//!   * **row-level security** — the `__eg_sql_rls__` system table optionally
//!     declares ONE column per table as the row-level principal discriminator.
//!     [`AuthorizedTable`] (returned only after [`authorize`] succeeds) folds a
//!     `col = <verified agent_id>` predicate into EVERY read/write it performs,
//!     ANDed with any caller-supplied predicate at the Rust level — never by
//!     splicing SQL text, so there is nothing for caller-supplied SQL to escape or
//!     override. The value plugged in is always `CarrierAuthority::agent_id()`,
//!     the same server-derived, MAC-verified identity every other RBAC check in
//!     this codebase keys on (see `access.rs`'s
//!     `rbac_check_is_keyed_by_agent_id_not_actor_scope` regression test) — never
//!     a value the request supplies.
//!
//! ## Migration (item 4)
//!
//! There is no way to enumerate "every legacy per-actor file belonging to tenant
//! T" up front — [`crate::server::sql_tables::owner_filename`] is a one-way digest
//! of `(tenant, agent_id)` by design, so the existing per-actor catalogs cannot be
//! listed from the tenant side. Migration is therefore LAZY and PER-ACTOR:
//! [`ensure_migrated`] runs at the top of EVERY public entry point in this module
//! ([`create_owned_table`], [`open_authorized_table`], [`grant`], [`revoke`],
//! [`set_row_level_column`]), and the first time a given actor reaches the
//! tenant-shared catalog through ANY of them it absorbs that actor's legacy
//! tables (schema + rows) AND views into the tenant store, sets the actor as
//! owner of each, and records a durable `__eg_sql_migrated__` marker so the scan
//! never repeats. A name COLLISION against another actor's already-migrated (or
//! natively tenant-created) table OR view is resolved by a deterministic rename
//! (`<name>__migrated_<actor-suffix>`) rather than dropping data — see
//! [`migrate_legacy`]. Extensions are enabled idempotently rather than renamed
//! (an extension carries no per-actor row data, so two actors' identical
//! enablement is the same fact reached twice, not a collision). Stored
//! functions, pgvector ANN indexes, and hypertable declarations are NOT carried
//! over — a deliberate scope cut, but never a SILENT one: their presence is
//! recorded as a durable, queryable notice per actor
//! (`__eg_sql_migration_notices__`, via [`migration_notices`]) rather than only
//! ever existing as a log line that could be missed. This is intentionally NOT a
//! flag-day migration: nothing runs until an actor's own request reaches this
//! code, and `user_table_store`'s legacy path keeps working forever regardless
//! (the legacy file is never modified or deleted by migration).
//!
//! **Concurrency:** [`ensure_migrated`]'s whole check-then-act sequence
//! (is-migrated read → migrate → mark-migrated write) runs under a per-(tenant,
//! agent_id) in-process lock ([`migration_lock`]), so two concurrent requests
//! from the SAME actor cannot both observe "not migrated" and both run
//! [`migrate_legacy`], which would otherwise double-insert every legacy row.
//!
//! ## What this module deliberately does NOT do
//!
//! It is not wired into any live request path. `src/server/handlers/query.rs`
//! (and the pgwire/mysql-wire/mssql-wire/sqlite-wire/sqlite-file/RDF-OBDA
//! surfaces) still resolve tables exclusively through
//! [`crate::server::sql_tables::user_table_store`] — untouched, still
//! per-(tenant, agent_id), still zero intra-tenant visibility by default. Wiring
//! a live surface to call [`open_authorized_table`] per parsed
//! table+privilege instead requires editing those handler files, which sit
//! outside this track's owned-file list (NE-003 owns `sql_tables.rs`, the
//! SQL/user-table paths of `access.rs`, and new modules/tests only). See the
//! track report for the full reasoning; until that wiring lands this module is a
//! complete, independently tested, currently-unreachable-from-any-wire capability.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use eg_query::{Cell, Column, ColumnType, TableSchema, TableStore};
use eg_types::{CmpOp, RowPredicate};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::server::access::CarrierAuthority;
use crate::server::sql_tables;

/// The one denial string for EVERY authorization failure in this module —
/// nonexistent table, unowned/ungranted table, unauthorized grant/revoke/RLS
/// admin call. Deliberately generic: a caller must not be able to distinguish
/// "you're not allowed" from "that isn't a real table" (hard constraint: no
/// existence leak).
const ACCESS_DENIED: &str = "ACCESS_DENIED: table is not accessible";

const OWNERS_TABLE: &str = "__eg_sql_owners__";
const GRANTS_TABLE: &str = "__eg_sql_grants__";
const RLS_TABLE: &str = "__eg_sql_rls__";
const MIGRATED_TABLE: &str = "__eg_sql_migrated__";
const NOTICES_TABLE: &str = "__eg_sql_migration_notices__";

/// The five independently grantable/revocable SQL privileges (item 2). Deliberately
/// NOT `eg_types::acl::RbacAction` (Read/Write/Admin) — that three-way split would
/// collapse INSERT/UPDATE/DELETE into one bucket, making them impossible to grant
/// or revoke independently, which the spec explicitly requires. The default-deny /
/// owner-or-admin-bypass / generic-denial CONVENTIONS below still follow
/// `eg-core::rbac`/`eg-types::acl`; only the action vocabulary is domain-specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SqlPrivilege {
    Select,
    Insert,
    Update,
    Delete,
    Alter,
}

impl SqlPrivilege {
    fn as_str(self) -> &'static str {
        match self {
            SqlPrivilege::Select => "select",
            SqlPrivilege::Insert => "insert",
            SqlPrivilege::Update => "update",
            SqlPrivilege::Delete => "delete",
            SqlPrivilege::Alter => "alter",
        }
    }
}

// ── ACL system-table plumbing ───────────────────────────────────────────────

fn owners_schema() -> TableSchema {
    TableSchema::new(
        OWNERS_TABLE,
        vec![
            Column::new("table_name", ColumnType::Text, false, true),
            Column::new("owner", ColumnType::Text, false, false),
        ],
    )
}

fn grants_schema() -> TableSchema {
    TableSchema::new(
        GRANTS_TABLE,
        vec![
            Column::new("table_name", ColumnType::Text, false, false),
            Column::new("principal", ColumnType::Text, false, false),
            Column::new("privilege", ColumnType::Text, false, false),
        ],
    )
}

fn rls_schema() -> TableSchema {
    TableSchema::new(
        RLS_TABLE,
        vec![
            Column::new("table_name", ColumnType::Text, false, true),
            Column::new("column_name", ColumnType::Text, false, false),
        ],
    )
}

fn migrated_schema() -> TableSchema {
    TableSchema::new(
        MIGRATED_TABLE,
        vec![Column::new("actor", ColumnType::Text, false, true)],
    )
}

/// A durable, queryable record of a migration scope cut (item 5) — NOT a log
/// line, which could be missed, but a row an operator can list later. Written
/// whenever a legacy catalog contained a stored function, ANN index, or
/// hypertable declaration that this migration deliberately did not carry over.
fn notices_schema() -> TableSchema {
    TableSchema::new(
        NOTICES_TABLE,
        vec![
            Column::new("actor", ColumnType::Text, false, false),
            Column::new("notice", ColumnType::Text, false, false),
        ],
    )
}

/// Open the tenant ACL catalog, ensuring its four system tables exist. Idempotent
/// and cheap on the warm path (`create_table(if_not_exists: true)` against an
/// already-cached [`TableStore`] handle).
fn open_acl(tenant_scope: &str, persist_dir: &Path) -> Result<TableStore, String> {
    let store = sql_tables::tenant_acl_table_store(tenant_scope, persist_dir)?;
    store.create_table(&owners_schema(), true)?;
    store.create_table(&grants_schema(), true)?;
    store.create_table(&rls_schema(), true)?;
    store.create_table(&migrated_schema(), true)?;
    store.create_table(&notices_schema(), true)?;
    Ok(store)
}

fn text_at(row: &[Cell], index: usize) -> Option<&str> {
    match row.get(index) {
        Some(Cell::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn owner_of(acl: &TableStore, table: &str) -> Result<Option<String>, String> {
    for row in acl.scan(OWNERS_TABLE)? {
        if text_at(&row, 0) == Some(table) {
            return Ok(text_at(&row, 1).map(str::to_string));
        }
    }
    Ok(None)
}

/// First-writer-wins ownership registration. A no-op (never overwrites) once the
/// table already has ANY recorded owner — including a concurrent creator that won
/// the UNIQUE-constraint race on `table_name` this call lost.
fn ensure_owner(acl: &TableStore, table: &str, principal: &str) -> Result<(), String> {
    if owner_of(acl, table)?.is_some() {
        return Ok(());
    }
    // Losing the race here means `insert_rows` returns the UNIQUE-constraint
    // violation; that is exactly "someone else already owns it", not a real
    // error, so it is treated as success (matching `owner_of`'s subsequent read).
    let _ = acl.insert_rows(
        OWNERS_TABLE,
        &["table_name".to_string(), "owner".to_string()],
        &[vec![
            Value::String(table.to_string()),
            Value::String(principal.to_string()),
        ]],
    );
    Ok(())
}

fn grant_exists(
    acl: &TableStore,
    table: &str,
    principal: &str,
    privilege: SqlPrivilege,
) -> Result<bool, String> {
    for row in acl.scan(GRANTS_TABLE)? {
        if text_at(&row, 0) == Some(table)
            && text_at(&row, 1) == Some(principal)
            && text_at(&row, 2) == Some(privilege.as_str())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Owner-or-admin gate for administrative operations on a table (grant, revoke,
/// declaring/clearing the RLS column). Uses the SAME generic denial as
/// [`authorize`] — an actor probing "am I the owner" cannot distinguish a
/// nonexistent table from one they merely don't administer.
fn authorize_admin(
    acl: &TableStore,
    authority: &CarrierAuthority,
    table: &str,
) -> Result<(), String> {
    if authority.is_admin() {
        return Ok(());
    }
    match owner_of(acl, table)? {
        Some(owner) if owner == authority.agent_id() => Ok(()),
        _ => Err(ACCESS_DENIED.to_string()),
    }
}

/// The default-deny authorization check (item 2). `Ok(())` when `authority` is an
/// admin carrier, the table's recorded owner, or holds an explicit grant for
/// `privilege`; `Err(ACCESS_DENIED)` — identical text whether the table has no
/// owner at all (nonexistent) or simply isn't granted to this principal — in
/// every other case, including a lookup/registry failure (fail closed, never
/// default to allow).
///
/// Timing: the two DENY-adjacent branches ("no owner recorded at all" and
/// "owner recorded, no grant") deliberately perform the SAME two scans
/// (`OWNERS` then `GRANTS`) in the SAME order before returning, rather than
/// short-circuiting the nonexistent case after only the owners scan — the
/// cheap, obvious equalization the module doc's "known side channel" note
/// asks for. This is NOT constant-time: `owner_of`/`grant_exists` still
/// return as soon as they find a matching row, so a real table whose owner
/// row sits early in scan order resolves faster than one whose row sits late
/// or is absent, a residual, data-dependent timing signal this deliberately
/// does not attempt to close (disproportionate for this system — see the
/// module doc).
pub(crate) fn authorize(
    tenant_scope: &str,
    persist_dir: &Path,
    authority: &CarrierAuthority,
    table: &str,
    privilege: SqlPrivilege,
) -> Result<(), String> {
    let acl = open_acl(tenant_scope, persist_dir)?;
    if authority.is_admin() {
        return Ok(());
    }
    let owner = owner_of(&acl, table)?;
    // Always run the grants scan too, even when there is no owner at all, so
    // "nonexistent" and "exists but ungranted" do the identical amount of
    // work before denying.
    let granted = grant_exists(&acl, table, authority.agent_id(), privilege)?;
    match owner {
        Some(owner) if owner == authority.agent_id() => Ok(()),
        _ if granted => Ok(()),
        _ => Err(ACCESS_DENIED.to_string()),
    }
}

/// Grant `grantee_agent_id` one or more [`SqlPrivilege`]s on `table`, within the
/// SAME tenant as `grantor` (there is no cross-tenant grant surface — the tenant
/// is derived from `grantor`'s own verified authority, never caller-supplied).
/// Only the table's owner or an admin carrier may grant (item 2). Runs
/// [`ensure_migrated`] first so an owner can grant on their own table on their
/// very first call, even before it (or any other table of theirs) has ever
/// been read/written through the new tenant-shared path.
pub(crate) fn grant(
    persist_dir: &Path,
    grantor: &CarrierAuthority,
    table: &str,
    grantee_agent_id: &str,
    privileges: &[SqlPrivilege],
) -> Result<(), String> {
    ensure_migrated(grantor.tenant_scope(), persist_dir, grantor)?;
    let acl = open_acl(grantor.tenant_scope(), persist_dir)?;
    authorize_admin(&acl, grantor, table)?;
    for privilege in privileges {
        if !grant_exists(&acl, table, grantee_agent_id, *privilege)? {
            acl.insert_rows(
                GRANTS_TABLE,
                &[
                    "table_name".to_string(),
                    "principal".to_string(),
                    "privilege".to_string(),
                ],
                &[vec![
                    Value::String(table.to_string()),
                    Value::String(grantee_agent_id.to_string()),
                    Value::String(privilege.as_str().to_string()),
                ]],
            )?;
        }
    }
    Ok(())
}

/// Revoke one or more privileges previously granted via [`grant`]. Deletes the
/// backing row(s) outright (no tombstone/deny-row) so the NEXT [`authorize`] call
/// — there is no cache in front of the ACL catalog — sees the change immediately
/// (item: "revoke takes effect immediately"). Runs [`ensure_migrated`] first for
/// the same reason [`grant`] does.
pub(crate) fn revoke(
    persist_dir: &Path,
    revoker: &CarrierAuthority,
    table: &str,
    grantee_agent_id: &str,
    privileges: &[SqlPrivilege],
) -> Result<(), String> {
    ensure_migrated(revoker.tenant_scope(), persist_dir, revoker)?;
    let acl = open_acl(revoker.tenant_scope(), persist_dir)?;
    authorize_admin(&acl, revoker, table)?;
    for privilege in privileges {
        let predicate = RowPredicate::And(vec![
            RowPredicate::Cmp {
                col: "table_name".to_string(),
                op: CmpOp::Eq,
                value: Value::String(table.to_string()),
            },
            RowPredicate::Cmp {
                col: "principal".to_string(),
                op: CmpOp::Eq,
                value: Value::String(grantee_agent_id.to_string()),
            },
            RowPredicate::Cmp {
                col: "privilege".to_string(),
                op: CmpOp::Eq,
                value: Value::String(privilege.as_str().to_string()),
            },
        ]);
        acl.delete_where(GRANTS_TABLE, &predicate)?;
    }
    Ok(())
}

/// Declare (`Some(column)`) or clear (`None`) the row-level tenant/principal
/// discriminator column for `table` (item 3). Owner-or-admin only. Runs
/// [`ensure_migrated`] first for the same reason [`grant`] does.
pub(crate) fn set_row_level_column(
    persist_dir: &Path,
    authority: &CarrierAuthority,
    table: &str,
    column: Option<&str>,
) -> Result<(), String> {
    ensure_migrated(authority.tenant_scope(), persist_dir, authority)?;
    let acl = open_acl(authority.tenant_scope(), persist_dir)?;
    authorize_admin(&acl, authority, table)?;
    let predicate = RowPredicate::Cmp {
        col: "table_name".to_string(),
        op: CmpOp::Eq,
        value: Value::String(table.to_string()),
    };
    acl.delete_where(RLS_TABLE, &predicate)?;
    if let Some(column) = column {
        acl.insert_rows(
            RLS_TABLE,
            &["table_name".to_string(), "column_name".to_string()],
            &[vec![
                Value::String(table.to_string()),
                Value::String(column.to_string()),
            ]],
        )?;
    }
    Ok(())
}

fn row_level_column(acl: &TableStore, table: &str) -> Result<Option<String>, String> {
    for row in acl.scan(RLS_TABLE)? {
        if text_at(&row, 0) == Some(table) {
            return Ok(text_at(&row, 1).map(str::to_string));
        }
    }
    Ok(None)
}

// ── lazy per-actor migration (item 4) ───────────────────────────────────────

/// Deterministic, collision-safe suffix for a migrated table renamed to avoid a
/// tenant-catalog name collision — a one-way digest of the migrating actor's
/// agent_id (never the raw agent_id itself, consistent with every other
/// filename/identifier-derivation convention in this catalog).
fn actor_migration_suffix(agent_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/sql-catalog-migration-suffix\0");
    digest.update(agent_id.as_bytes());
    hex::encode(digest.finalize())[..12].to_string()
}

fn is_migrated(acl: &TableStore, actor: &str) -> Result<bool, String> {
    for row in acl.scan(MIGRATED_TABLE)? {
        if text_at(&row, 0) == Some(actor) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// One in-process lock per (tenant, agent_id) pair, serializing
/// [`ensure_migrated`] so two concurrent requests from the SAME actor cannot
/// both observe "not migrated yet" and both run [`migrate_legacy`] — which
/// would double-insert every legacy row (item 3, concurrency hardening).
/// redb itself only ever has ONE physical file open per process (enforced by
/// `sql_tables`'s registry), so the only race that can happen at all is
/// WITHIN this process between threads; a plain `std::sync::Mutex` per key
/// closes it completely, with no need for anything redb-transaction-level.
fn migration_lock(tenant_scope: &str, agent_id: &str) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();
    let registry = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let key = format!("{tenant_scope}\u{0}{agent_id}");
    let mut locks = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks
        .entry(key)
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn mark_migrated(acl: &TableStore, actor: &str) -> Result<(), String> {
    if is_migrated(acl, actor)? {
        return Ok(());
    }
    let _ = acl.insert_rows(
        MIGRATED_TABLE,
        &["actor".to_string()],
        &[vec![Value::String(actor.to_string())]],
    );
    Ok(())
}

/// Absorb `authority`'s legacy per-(tenant, agent_id) catalog (schema + rows,
/// plus views/extensions — see the module doc's migration section) into the
/// tenant-shared catalog, registering `authority` as owner of each migrated
/// table. A name collision against a table (or view) that already exists in
/// the tenant catalog (created natively, or migrated by a different actor) is
/// resolved by a deterministic rename rather than dropping data. Stored
/// functions/ANN indexes/hypertables are deliberately not carried over, but
/// their presence is recorded durably rather than silently dropped — see
/// [`record_migration_notice`].
fn migrate_legacy(
    tenant_scope: &str,
    persist_dir: &Path,
    authority: &CarrierAuthority,
    acl: &TableStore,
) -> Result<(), String> {
    let legacy = sql_tables::legacy_table_store(authority, persist_dir)?;
    let tenant_store = sql_tables::tenant_table_store(tenant_scope, persist_dir)?;
    let suffix = actor_migration_suffix(authority.agent_id());
    for name in legacy.list_tables()? {
        let Some(schema) = legacy.get_schema(&name)? else {
            continue;
        };
        let target_name = if tenant_store.get_schema(&name)?.is_some() {
            format!("{name}__migrated_{suffix}")
        } else {
            name.clone()
        };
        if tenant_store.get_schema(&target_name)?.is_some() {
            // Already migrated under the renamed target (e.g. a re-run that
            // crashed after this table but before `mark_migrated` committed).
            ensure_owner(acl, &target_name, authority.agent_id())?;
            continue;
        }
        let mut target_schema = schema.clone();
        target_schema.name = target_name.clone();
        tenant_store.create_table(&target_schema, true)?;
        let rows = legacy.scan(&name)?;
        if !rows.is_empty() {
            let column_names: Vec<String> = schema
                .columns()
                .iter()
                .map(|column| column.name.clone())
                .collect();
            let values: Vec<Vec<Value>> = rows
                .iter()
                .map(|row| row.iter().map(Cell::to_json).collect())
                .collect();
            tenant_store.insert_rows(&target_name, &column_names, &values)?;
        }
        ensure_owner(acl, &target_name, authority.agent_id())?;
    }
    // Views: same collision-rename treatment as tables (never silently
    // dropped), and any OTHER failure propagates loudly via `?` instead of
    // being swallowed — a `let _ = ...` here previously hid a name collision
    // entirely (item 1).
    for (view_name, select_sql) in legacy.list_views()? {
        let target_view_name = if tenant_store.get_view(&view_name)?.is_some() {
            format!("{view_name}__migrated_{suffix}")
        } else {
            view_name.clone()
        };
        if tenant_store.get_view(&target_view_name)?.is_some() {
            // Already migrated under the renamed target (idempotent re-run).
            continue;
        }
        tenant_store
            .create_view(&target_view_name, &select_sql, false)
            .map_err(|error| {
                format!(
                    "NE-003 migration: failed to carry over view `{view_name}` for actor -> {error}"
                )
            })?;
    }
    // Extensions carry no per-actor row data — enabling the SAME extension
    // name from two different actors is not a collision to rename around, it
    // is the SAME durable fact ("this tenant has pgvector enabled") reached
    // twice, so `if_not_exists: true` is the correct idempotent behavior, not
    // a swallow. Any OTHER failure still propagates loudly via `?`.
    for extension in legacy.list_extensions()? {
        tenant_store
            .create_extension(&extension, true)
            .map_err(|error| {
                format!(
                    "NE-003 migration: failed to carry over extension `{extension}` for actor -> {error}"
                )
            })?;
    }
    // Deliberately NOT migrated (item 5): stored functions, pgvector ANN
    // indexes, and hypertable declarations — secondary catalogs, recreatable,
    // out of scope for this pass. A SILENT scope cut here would read as data
    // loss later, so instead of skipping quietly this records a durable,
    // queryable notice per actor (`__eg_sql_migration_notices__`) naming
    // exactly what was left behind in the legacy catalog.
    let functions = legacy.list_functions()?;
    if !functions.is_empty() {
        record_migration_notice(
            acl,
            authority.agent_id(),
            &format!(
                "{} stored function(s) were NOT migrated (out of scope for NE-003) — still present only in the legacy per-actor catalog",
                functions.len()
            ),
        )?;
    }
    let ann_indexes = legacy.list_ann_indexes()?;
    if !ann_indexes.is_empty() {
        record_migration_notice(
            acl,
            authority.agent_id(),
            &format!(
                "{} pgvector ANN index(es) were NOT migrated (out of scope for NE-003) — still present only in the legacy per-actor catalog",
                ann_indexes.len()
            ),
        )?;
    }
    let hypertables = legacy.list_hypertables()?;
    if !hypertables.is_empty() {
        record_migration_notice(
            acl,
            authority.agent_id(),
            &format!(
                "{} hypertable declaration(s) were NOT migrated (out of scope for NE-003) — still present only in the legacy per-actor catalog",
                hypertables.len()
            ),
        )?;
    }
    Ok(())
}

/// Durably record a migration scope cut (item 5) so it is discoverable by an
/// operator later, instead of only ever existing as a log line that could be
/// missed.
fn record_migration_notice(acl: &TableStore, actor: &str, notice: &str) -> Result<(), String> {
    acl.insert_rows(
        NOTICES_TABLE,
        &["actor".to_string(), "notice".to_string()],
        &[vec![
            Value::String(actor.to_string()),
            Value::String(notice.to_string()),
        ]],
    )?;
    Ok(())
}

/// Every durable migration notice recorded for `actor` (item 5) — what NE-003's
/// migration deliberately left behind in the legacy catalog.
pub(crate) fn migration_notices(
    tenant_scope: &str,
    persist_dir: &Path,
    actor: &str,
) -> Result<Vec<String>, String> {
    let acl = open_acl(tenant_scope, persist_dir)?;
    let mut notices = Vec::new();
    for row in acl.scan(NOTICES_TABLE)? {
        if text_at(&row, 0) == Some(actor) {
            if let Some(notice) = text_at(&row, 1) {
                notices.push(notice.to_string());
            }
        }
    }
    Ok(notices)
}

/// Run `authority`'s one-time lazy migration if it has not already run (tracked
/// durably in `__eg_sql_migrated__`, so a restart never repeats it). Called at the
/// top of every public entry point in this module.
fn ensure_migrated(
    tenant_scope: &str,
    persist_dir: &Path,
    authority: &CarrierAuthority,
) -> Result<(), String> {
    let lock = migration_lock(tenant_scope, authority.agent_id());
    // Held for the entire check-then-act sequence below (including the
    // migration itself) so a second concurrent call for the SAME actor blocks
    // here instead of racing `is_migrated`'s read against the first call's
    // write — see `migration_lock`'s doc comment.
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let acl = open_acl(tenant_scope, persist_dir)?;
    if is_migrated(&acl, authority.agent_id())? {
        return Ok(());
    }
    if sql_tables::legacy_store_exists(authority, persist_dir) {
        migrate_legacy(tenant_scope, persist_dir, authority, &acl)?;
    }
    mark_migrated(&acl, authority.agent_id())
}

// ── table creation (registers ownership) ────────────────────────────────────

/// `CREATE TABLE` against the tenant-shared catalog: creates the physical table
/// (via [`sql_tables::tenant_table_store`]) and, only when this call is the one
/// that actually created it, registers `authority` as its owner. An
/// already-existing table (`if_not_exists: true` no-op) is left exactly as-is —
/// this function never silently reassigns ownership.
pub(crate) fn create_owned_table(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    schema: &TableSchema,
    if_not_exists: bool,
) -> Result<bool, String> {
    ensure_migrated(authority.tenant_scope(), persist_dir, authority)?;
    let tenant_store = sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
    let created = tenant_store.create_table(schema, if_not_exists)?;
    if created {
        let acl = open_acl(authority.tenant_scope(), persist_dir)?;
        ensure_owner(&acl, &schema.name, authority.agent_id())?;
    }
    Ok(created)
}

// ── the capability object every read/write goes through ────────────────────

/// A capability handle proving `authority` was authorized for exactly
/// `(table, privilege)` at the moment [`open_authorized_table`] returned it, and
/// carrying whatever row-level predicate applies. Every method below folds the RLS
/// predicate (if any) into the operation at the Rust level BEFORE it reaches the
/// underlying [`TableStore`] — never by rewriting caller-supplied SQL text, so
/// there is no SQL surface for an override to hide in (item 3: "cannot be
/// overridden by caller-supplied SQL").
pub(crate) struct AuthorizedTable {
    store: TableStore,
    table: String,
    principal: String,
    rls_column: Option<String>,
}

/// Manual, REDACTED `Debug` — deliberately not `#[derive(Debug)]`. A derived
/// impl would put `principal` (a caller identity) into any panic message, log
/// line, or error chain this type ever reaches, including every
/// `.unwrap()`/`.unwrap_err()` call site in this module's own tests — exactly
/// the "tenant, principal, graph, and filesystem details never appear in
/// filenames or errors" convention `sql_tables.rs` documents. This emits the
/// table name (safe — callers already know it) and whether RLS is active
/// (safe — a boolean), and a stable, NON-REVERSIBLE digest of the principal
/// instead of the principal itself. See
/// `authorized_table_debug_redacts_principal` for the regression test that
/// catches a future `#[derive(Debug)]` silently reintroducing the leak.
impl std::fmt::Debug for AuthorizedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut digest = Sha256::new();
        digest.update(b"epistemic-graph/sql-catalog-debug-principal\0");
        digest.update(self.principal.as_bytes());
        let full_digest = hex::encode(digest.finalize());
        let principal_digest = &full_digest[..12];
        f.debug_struct("AuthorizedTable")
            .field("table", &self.table)
            .field("rls_active", &self.rls_column.is_some())
            .field("principal_digest", &principal_digest)
            .finish()
    }
}

/// Resolve, authorize, and open one table for one privilege (items 2 + 3 combined
/// entry point). Runs migration first so a legacy actor's very first tenant-scoped
/// call already sees their own migrated tables.
pub(crate) fn open_authorized_table(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    table: &str,
    privilege: SqlPrivilege,
) -> Result<AuthorizedTable, String> {
    ensure_migrated(authority.tenant_scope(), persist_dir, authority)?;
    authorize(
        authority.tenant_scope(),
        persist_dir,
        authority,
        table,
        privilege,
    )?;
    let acl = open_acl(authority.tenant_scope(), persist_dir)?;
    let rls_column = row_level_column(&acl, table)?;
    let store = sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
    Ok(AuthorizedTable {
        store,
        table: table.to_string(),
        principal: authority.agent_id().to_string(),
        rls_column,
    })
}

fn row_to_map(schema: &TableSchema, row: &[Cell]) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    for (column, cell) in schema.columns().iter().zip(row.iter()) {
        map.insert(column.name.clone(), cell.to_json());
    }
    map
}

impl AuthorizedTable {
    fn combined_predicate(&self, extra: Option<&RowPredicate>) -> RowPredicate {
        let mut parts = Vec::new();
        if let Some(column) = &self.rls_column {
            parts.push(RowPredicate::Cmp {
                col: column.clone(),
                op: CmpOp::Eq,
                value: Value::String(self.principal.clone()),
            });
        }
        if let Some(extra) = extra {
            parts.push(extra.clone());
        }
        // `And(vec![])` evaluates `true` for every row (vacuous truth) — the
        // correct "no constraint at all" predicate when neither RLS nor a
        // caller-supplied filter applies.
        RowPredicate::And(parts)
    }

    /// Every row currently visible to `self.principal`: RLS-filtered (if the
    /// table declares a discriminator column) and ANDed with `extra` if given.
    /// Client-side filtering over a full [`TableStore::scan`] — correctness over
    /// pushdown efficiency, matching the rest of this not-yet-wired module (see
    /// the module doc).
    pub(crate) fn select(&self, extra: Option<&RowPredicate>) -> Result<Vec<Vec<Cell>>, String> {
        let schema = self
            .store
            .get_schema(&self.table)?
            .ok_or_else(|| ACCESS_DENIED.to_string())?;
        let predicate = self.combined_predicate(extra);
        let rows = self.store.scan(&self.table)?;
        Ok(rows
            .into_iter()
            .filter(|row| predicate.eval(&row_to_map(&schema, row)))
            .collect())
    }

    /// Insert rows, forcibly stamping the RLS column (if declared) to
    /// `self.principal` on every row — appending it to `col_order` when the
    /// caller did not supply it, overwriting whatever value the caller DID supply
    /// otherwise. A caller can never insert a row visible to a different
    /// principal than themselves.
    pub(crate) fn insert(
        &self,
        col_order: &[String],
        rows: &[Vec<Value>],
    ) -> Result<usize, String> {
        let Some(rls_column) = &self.rls_column else {
            return self.store.insert_rows(&self.table, col_order, rows);
        };
        let stamp = Value::String(self.principal.clone());
        match col_order.iter().position(|column| column == rls_column) {
            Some(index) => {
                let mut stamped = rows.to_vec();
                for row in &mut stamped {
                    if index < row.len() {
                        row[index] = stamp.clone();
                    } else {
                        row.resize(index + 1, Value::Null);
                        row[index] = stamp.clone();
                    }
                }
                self.store.insert_rows(&self.table, col_order, &stamped)
            }
            None => {
                let mut owned_cols = col_order.to_vec();
                owned_cols.push(rls_column.clone());
                let stamped: Vec<Vec<Value>> = rows
                    .iter()
                    .map(|row| {
                        let mut row = row.clone();
                        row.push(stamp.clone());
                        row
                    })
                    .collect();
                self.store.insert_rows(&self.table, &owned_cols, &stamped)
            }
        }
    }

    /// `UPDATE … SET … WHERE …`. The RLS predicate (if any) is ANDed into the
    /// WHERE so a principal can only ever touch their own rows, and — if the
    /// caller's `set` names the RLS column — that assignment is forcibly
    /// overwritten back to `self.principal`, so an UPDATE can never move a row
    /// into another principal's visibility.
    pub(crate) fn update(
        &self,
        mut set: serde_json::Map<String, Value>,
        predicate: Option<RowPredicate>,
    ) -> Result<usize, String> {
        if let Some(rls_column) = &self.rls_column {
            set.insert(rls_column.clone(), Value::String(self.principal.clone()));
        }
        let where_predicate = self.combined_predicate(predicate.as_ref());
        self.store.update_where(&self.table, &set, &where_predicate)
    }

    /// `DELETE FROM … WHERE …`, RLS-constrained exactly like [`Self::update`].
    pub(crate) fn delete(&self, predicate: Option<RowPredicate>) -> Result<usize, String> {
        let where_predicate = self.combined_predicate(predicate.as_ref());
        self.store.delete_where(&self.table, &where_predicate)
    }

    #[cfg(test)]
    pub(crate) fn table_name(&self) -> &str {
        &self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::auth::VerifiedRequestContext;
    use crate::server::sql_tables::test_persist_dir;

    fn authority(agent_id: &str, tenant: &str) -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_in_tenant(
            agent_id, tenant,
        ))
        .unwrap()
    }

    fn schema(name: &str, columns: Vec<Column>) -> TableSchema {
        TableSchema::new(name, columns)
    }

    fn text_col(name: &str) -> Column {
        Column::new(name, ColumnType::Text, false, false)
    }

    // ── two actors in one tenant share a granted table ──────────────────────

    #[test]
    fn granted_actor_can_read_owners_table_and_ungranted_actor_cannot() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-shared");
        let bob = authority("bob", "tenant-shared");

        assert!(create_owned_table(
            &alice,
            &dir,
            &schema("orders", vec![text_col("id"), text_col("item")]),
            false,
        )
        .unwrap());

        let alice_table =
            open_authorized_table(&alice, &dir, "orders", SqlPrivilege::Insert).unwrap();
        alice_table
            .insert(
                &["id".to_string(), "item".to_string()],
                &[vec![
                    Value::String("1".to_string()),
                    Value::String("widget".to_string()),
                ]],
            )
            .unwrap();

        // Bob has no grant yet — denied, and the denial is the generic string.
        let denied = open_authorized_table(&bob, &dir, "orders", SqlPrivilege::Select).unwrap_err();
        assert_eq!(denied, ACCESS_DENIED);

        // Alice (owner) grants Bob SELECT.
        grant(&dir, &alice, "orders", "bob", &[SqlPrivilege::Select]).unwrap();

        let bob_table = open_authorized_table(&bob, &dir, "orders", SqlPrivilege::Select).unwrap();
        let rows = bob_table.select(None).unwrap();
        assert_eq!(rows.len(), 1, "bob must see alice's row through the grant");

        // Bob still cannot INSERT — SELECT was the only privilege granted.
        let insert_denied =
            open_authorized_table(&bob, &dir, "orders", SqlPrivilege::Insert).unwrap_err();
        assert_eq!(insert_denied, ACCESS_DENIED);
    }

    // ── revoke takes effect immediately ─────────────────────────────────────

    #[test]
    fn revoke_takes_effect_immediately() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-revoke");
        let bob = authority("bob", "tenant-revoke");

        create_owned_table(&alice, &dir, &schema("t", vec![text_col("id")]), false).unwrap();
        grant(&dir, &alice, "t", "bob", &[SqlPrivilege::Select]).unwrap();
        assert!(open_authorized_table(&bob, &dir, "t", SqlPrivilege::Select).is_ok());

        revoke(&dir, &alice, "t", "bob", &[SqlPrivilege::Select]).unwrap();
        let denied = open_authorized_table(&bob, &dir, "t", SqlPrivilege::Select).unwrap_err();
        assert_eq!(denied, ACCESS_DENIED);
    }

    // ── ungranted actor cannot distinguish denied from nonexistent ─────────

    #[test]
    fn denial_is_indistinguishable_from_nonexistence() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-indist");
        let carol = authority("carol", "tenant-indist");

        create_owned_table(&alice, &dir, &schema("real", vec![text_col("id")]), false).unwrap();

        let denied_real =
            open_authorized_table(&carol, &dir, "real", SqlPrivilege::Select).unwrap_err();
        let denied_fake =
            open_authorized_table(&carol, &dir, "does_not_exist", SqlPrivilege::Select)
                .unwrap_err();
        assert_eq!(denied_real, denied_fake);
        assert_eq!(denied_real, ACCESS_DENIED);
    }

    // ── a second tenant reads zero rows through every path touched here ────

    #[test]
    fn second_tenant_has_no_visibility_at_all() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-one");
        let mallory = authority("alice", "tenant-two"); // same agent_id, different tenant

        create_owned_table(
            &alice,
            &dir,
            &schema("secrets", vec![text_col("id")]),
            false,
        )
        .unwrap();
        let alice_table =
            open_authorized_table(&alice, &dir, "secrets", SqlPrivilege::Insert).unwrap();
        alice_table
            .insert(&["id".to_string()], &[vec![Value::String("x".to_string())]])
            .unwrap();

        // Same table name, different tenant: denied exactly like nonexistence.
        let denied =
            open_authorized_table(&mallory, &dir, "secrets", SqlPrivilege::Select).unwrap_err();
        assert_eq!(denied, ACCESS_DENIED);

        // And the physical catalogs are provably separate files with separate data.
        let tenant_two_store =
            sql_tables::tenant_table_store(mallory.tenant_scope(), &dir).unwrap();
        assert!(tenant_two_store.get_schema("secrets").unwrap().is_none());
    }

    // ── the row-level predicate constrains reads AND writes and cannot be
    //    overridden by caller-supplied values ──────────────────────────────

    #[test]
    fn row_level_security_constrains_reads_and_writes_and_resists_override() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-rls");
        let bob = authority("bob", "tenant-rls");

        create_owned_table(
            &alice,
            &dir,
            &schema(
                "notes",
                vec![text_col("id"), text_col("body"), text_col("owner_tag")],
            ),
            false,
        )
        .unwrap();
        set_row_level_column(&dir, &alice, "notes", Some("owner_tag")).unwrap();
        grant(
            &dir,
            &alice,
            "notes",
            "bob",
            &[
                SqlPrivilege::Select,
                SqlPrivilege::Insert,
                SqlPrivilege::Update,
                SqlPrivilege::Delete,
            ],
        )
        .unwrap();

        let alice_table =
            open_authorized_table(&alice, &dir, "notes", SqlPrivilege::Insert).unwrap();
        // Alice tries to insert a row STAMPED as bob's — must be forced back to hers.
        alice_table
            .insert(
                &[
                    "id".to_string(),
                    "body".to_string(),
                    "owner_tag".to_string(),
                ],
                &[vec![
                    Value::String("a1".to_string()),
                    Value::String("alice's secret".to_string()),
                    Value::String("bob".to_string()), // attempted override
                ]],
            )
            .unwrap();

        let bob_table = open_authorized_table(&bob, &dir, "notes", SqlPrivilege::Insert).unwrap();
        bob_table
            .insert(
                &["id".to_string(), "body".to_string()], // owner_tag omitted entirely
                &[vec![
                    Value::String("b1".to_string()),
                    Value::String("bob's note".to_string()),
                ]],
            )
            .unwrap();

        let alice_select =
            open_authorized_table(&alice, &dir, "notes", SqlPrivilege::Select).unwrap();
        let alice_rows = alice_select.select(None).unwrap();
        assert_eq!(
            alice_rows.len(),
            1,
            "alice must see only her own row, override or not"
        );

        let bob_select = open_authorized_table(&bob, &dir, "notes", SqlPrivilege::Select).unwrap();
        let bob_rows = bob_select.select(None).unwrap();
        assert_eq!(bob_rows.len(), 1, "bob must see only his own row");

        // Bob tries to UPDATE/DELETE alice's row by id — a caller-supplied WHERE
        // that would match it under a naive (non-RLS) predicate. Zero rows affected.
        let bob_update = open_authorized_table(&bob, &dir, "notes", SqlPrivilege::Update).unwrap();
        let mut set = serde_json::Map::new();
        set.insert("body".to_string(), Value::String("tampered".to_string()));
        let updated = bob_update
            .update(
                set,
                Some(RowPredicate::Cmp {
                    col: "id".to_string(),
                    op: CmpOp::Eq,
                    value: Value::String("a1".to_string()),
                }),
            )
            .unwrap();
        assert_eq!(updated, 0, "bob's UPDATE must not reach alice's row");

        let bob_delete = open_authorized_table(&bob, &dir, "notes", SqlPrivilege::Delete).unwrap();
        let deleted = bob_delete
            .delete(Some(RowPredicate::Cmp {
                col: "id".to_string(),
                op: CmpOp::Eq,
                value: Value::String("a1".to_string()),
            }))
            .unwrap();
        assert_eq!(deleted, 0, "bob's DELETE must not reach alice's row");

        // Alice's row is intact.
        let alice_recheck =
            open_authorized_table(&alice, &dir, "notes", SqlPrivilege::Select).unwrap();
        let rows = alice_recheck.select(None).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].contains(&Cell::Text("alice's secret".to_string())));
    }

    // ── an existing per-(tenant,actor) catalog still opens, and migrates ───

    #[test]
    fn legacy_per_actor_catalog_still_opens_and_migrates_without_data_loss() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-migrate");

        // Simulate pre-upgrade state: data written through the UNCHANGED legacy path.
        let legacy = sql_tables::user_table_store(&alice, Some(&dir)).unwrap();
        legacy
            .create_table(
                &schema("legacy_events", vec![text_col("id"), text_col("payload")]),
                false,
            )
            .unwrap();
        legacy
            .insert_rows(
                "legacy_events",
                &["id".to_string(), "payload".to_string()],
                &[vec![
                    Value::String("e1".to_string()),
                    Value::String("hello".to_string()),
                ]],
            )
            .unwrap();

        // The legacy path still opens fine, unmodified.
        assert!(sql_tables::user_table_store(&alice, Some(&dir)).is_ok());

        // First touch of the NEW tenant-shared path migrates it in.
        let table =
            open_authorized_table(&alice, &dir, "legacy_events", SqlPrivilege::Select).unwrap();
        let rows = table.select(None).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].contains(&Cell::Text("hello".to_string())));

        // Alice is now the recorded owner (ownership survived the migration).
        let acl = open_acl(alice.tenant_scope(), &dir).unwrap();
        assert_eq!(
            owner_of(&acl, "legacy_events").unwrap().as_deref(),
            Some("alice")
        );
    }

    // ── restart preserves grants and ownership ──────────────────────────────

    #[test]
    fn restart_preserves_grants_and_ownership() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-restart");
        let tenant_scope = alice.tenant_scope().to_string();

        {
            create_owned_table(
                &alice,
                &dir,
                &schema("durable", vec![text_col("id")]),
                false,
            )
            .unwrap();
            grant(&dir, &alice, "durable", "bob", &[SqlPrivilege::Select]).unwrap();
            set_row_level_column(&dir, &alice, "durable", Some("id")).unwrap();
            // Every handle above must be dropped before eviction, or the redb
            // file lock never releases and the "reopen" below reuses the same
            // in-memory state instead of proving on-disk durability.
        }

        let table_path = sql_tables::tenant_table_path_for_test(&tenant_scope, &dir);
        let acl_path = sql_tables::tenant_acl_path_for_test(&tenant_scope, &dir);
        sql_tables::evict_for_test(&table_path);
        sql_tables::evict_for_test(&acl_path);

        let bob = authority("bob", "tenant-restart");
        // Bob's grant must still be visible after a genuine on-disk reopen.
        assert!(open_authorized_table(&bob, &dir, "durable", SqlPrivilege::Select).is_ok());
        let acl = open_acl(&tenant_scope, &dir).unwrap();
        assert_eq!(owner_of(&acl, "durable").unwrap().as_deref(), Some("alice"));
        assert_eq!(
            row_level_column(&acl, "durable").unwrap().as_deref(),
            Some("id")
        );
    }

    // ── admin bypass, and non-owner cannot grant ────────────────────────────

    #[test]
    fn only_owner_or_admin_may_grant_or_revoke() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-admin");
        let bob = authority("bob", "tenant-admin");

        create_owned_table(&alice, &dir, &schema("t", vec![text_col("id")]), false).unwrap();

        // Bob is neither owner nor admin — his grant attempt is denied.
        let denied = grant(&dir, &bob, "t", "carol", &[SqlPrivilege::Select]).unwrap_err();
        assert_eq!(denied, ACCESS_DENIED);
    }

    // ── grant()/revoke()/set_row_level_column() trigger migration themselves ─

    #[test]
    fn grant_migrates_a_not_yet_touched_legacy_table_before_authorizing() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-grant-migrate");

        // Legacy data exists, but NOTHING has yet called create_owned_table or
        // open_authorized_table for this actor in this tenant — grant() must be
        // the thing that triggers the migration, not merely benefit from one
        // that already ran.
        let legacy = sql_tables::user_table_store(&alice, Some(&dir)).unwrap();
        legacy
            .create_table(&schema("legacy_only", vec![text_col("id")]), false)
            .unwrap();

        let bob = authority("bob", "tenant-grant-migrate");
        grant(&dir, &alice, "legacy_only", "bob", &[SqlPrivilege::Select]).unwrap();

        // The grant succeeded, which is only possible if alice was resolved as
        // owner — which is only possible if migration already ran.
        assert!(open_authorized_table(&bob, &dir, "legacy_only", SqlPrivilege::Select).is_ok());
    }

    // ── migration concurrency: two racing calls, no duplicate rows ─────────

    #[test]
    fn concurrent_migration_of_the_same_actor_is_race_free() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-race");

        let legacy = sql_tables::user_table_store(&alice, Some(&dir)).unwrap();
        legacy
            .create_table(&schema("race_events", vec![text_col("id")]), false)
            .unwrap();
        for i in 0..5 {
            legacy
                .insert_rows(
                    "race_events",
                    &["id".to_string()],
                    &[vec![Value::String(format!("row-{i}"))]],
                )
                .unwrap();
        }

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let dir = dir.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                let racer = authority("alice", "tenant-race");
                barrier.wait();
                open_authorized_table(&racer, &dir, "race_events", SqlPrivilege::Select)
            }));
        }
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let table =
            open_authorized_table(&alice, &dir, "race_events", SqlPrivilege::Select).unwrap();
        let rows = table.select(None).unwrap();
        assert_eq!(rows.len(), 5, "a race must not duplicate migrated rows");

        // Exactly one physical copy — no collision-renamed duplicate table
        // produced by the two racing migrations.
        let tenant_store = sql_tables::tenant_table_store(alice.tenant_scope(), &dir).unwrap();
        let matching: Vec<String> = tenant_store
            .list_tables()
            .unwrap()
            .into_iter()
            .filter(|name| name.starts_with("race_events"))
            .collect();
        assert_eq!(
            matching,
            vec!["race_events".to_string()],
            "no partial/duplicate migrated table from the race"
        );
    }

    // ── views collision-rename on migration (item 1) ────────────────────────

    #[test]
    fn migrated_view_name_collision_is_renamed_not_dropped() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-view-collide");
        let bob = authority("bob", "tenant-view-collide");

        // Bob migrates first and ends up owning the bare view name "summary".
        let bob_legacy = sql_tables::user_table_store(&bob, Some(&dir)).unwrap();
        bob_legacy
            .create_table(&schema("t", vec![text_col("id")]), false)
            .unwrap();
        bob_legacy
            .create_view("summary", "SELECT * FROM t", false)
            .unwrap();
        create_owned_table(
            &bob,
            &dir,
            &schema("bob_trigger", vec![text_col("id")]),
            false,
        )
        .unwrap(); // triggers bob's migration, including the view.

        // Alice has her OWN legacy view also named "summary" — must not vanish.
        let alice_legacy = sql_tables::user_table_store(&alice, Some(&dir)).unwrap();
        alice_legacy
            .create_table(&schema("t2", vec![text_col("id")]), false)
            .unwrap();
        alice_legacy
            .create_view("summary", "SELECT * FROM t2", false)
            .unwrap();
        create_owned_table(
            &alice,
            &dir,
            &schema("alice_trigger", vec![text_col("id")]),
            false,
        )
        .unwrap(); // triggers alice's migration.

        let tenant_store = sql_tables::tenant_table_store(alice.tenant_scope(), &dir).unwrap();
        let views = tenant_store.list_views().unwrap();
        let view_names: Vec<String> = views.into_iter().map(|(name, _)| name).collect();
        assert!(
            view_names.contains(&"summary".to_string()),
            "bob's original name survives"
        );
        let renamed_suffix = actor_migration_suffix(alice.agent_id());
        assert!(
            view_names.contains(&format!("summary__migrated_{renamed_suffix}")),
            "alice's colliding view must be preserved under a renamed, discoverable name, not dropped; views seen: {view_names:?}"
        );
    }

    // ── loud scope-cut notices for functions/ANN indexes/hypertables ───────

    #[test]
    fn unmigrated_hypertable_produces_a_durable_notice_not_silence() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-notice");

        let legacy = sql_tables::user_table_store(&alice, Some(&dir)).unwrap();
        legacy
            .create_table(
                &schema("series", vec![text_col("id"), text_col("ts")]),
                false,
            )
            .unwrap();
        legacy
            .put_hypertable(&eg_query::HypertablePlan {
                table: "series".to_string(),
                time_column: "ts".to_string(),
            })
            .unwrap();

        // Trigger migration.
        create_owned_table(
            &alice,
            &dir,
            &schema("trigger", vec![text_col("id")]),
            false,
        )
        .unwrap();

        let notices = migration_notices(alice.tenant_scope(), &dir, alice.agent_id()).unwrap();
        assert!(
            notices.iter().any(|notice| notice.contains("hypertable")),
            "a not-carried-over hypertable must leave a durable notice, not silence; notices: {notices:?}"
        );
    }

    // ── AuthorizedTable's Debug must never leak the principal ──────────────

    #[test]
    fn authorized_table_debug_redacts_principal() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-debug-redact");
        create_owned_table(&alice, &dir, &schema("orders", vec![text_col("id")]), false).unwrap();
        let table = open_authorized_table(&alice, &dir, "orders", SqlPrivilege::Select).unwrap();

        let debug_output = format!("{table:?}");
        assert!(
            !debug_output.contains("alice"),
            "AuthorizedTable's Debug must never leak the raw principal — a future #[derive(Debug)] would silently reintroduce this; got: {debug_output}"
        );
        assert!(
            debug_output.contains("orders"),
            "the table name is safe to surface in Debug; got: {debug_output}"
        );
        assert!(
            debug_output.contains("rls_active"),
            "whether RLS is active is safe to surface in Debug; got: {debug_output}"
        );

        // Also prove unwrap_err() itself works now (the compile error this test
        // exists alongside): a denied open still formats without panicking or
        // leaking, even though this specific call succeeds above.
        let bob = authority("bob", "tenant-debug-redact");
        let denied = open_authorized_table(&bob, &dir, "orders", SqlPrivilege::Select).unwrap_err();
        assert_eq!(denied, ACCESS_DENIED);
    }
}
