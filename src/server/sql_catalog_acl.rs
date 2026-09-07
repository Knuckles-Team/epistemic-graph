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
//! ## What this module deliberately does NOT do
//!
//! The shared wire-neutral SQL path in `src/server/wire/mod.rs` enforces this
//! authority for pgwire and the other adapters that delegate to `WireSession`.
//! Non-wire query surfaces must still opt into the same parsed-table checks;
//! callers must never infer that opening the tenant-shared physical store alone
//! grants access.

use std::collections::BTreeSet;
use std::path::Path;

use eg_query::{Cell, Column, ColumnType, TableSchema, TableStore, TableTxn, TxnOp};
use eg_types::{CmpOp, RowPredicate};
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::server::access::CarrierAuthority;
use crate::server::sql_tables;

/// The one denial string for EVERY authorization failure in this module —
/// nonexistent table, unowned/ungranted table, unauthorized grant/revoke/RLS
/// admin call. Deliberately generic: a caller must not be able to distinguish
/// "you're not allowed" from "that isn't a real table" (hard constraint: no
/// existence leak).
pub(crate) const ACCESS_DENIED: &str = "ACCESS_DENIED: table is not accessible";

const OWNERS_TABLE: &str = "__eg_sql_owners__";
const GRANTS_TABLE: &str = "__eg_sql_grants__";
const RLS_TABLE: &str = "__eg_sql_rls__";
const SOURCE_ACL_RESOURCE: &str = "sql-source-acl";
type SemanticCursorMac = Hmac<Sha256>;

/// Ordering capability attached to every SQL source-ACL snapshot.
///
/// The current redb catalog is opened exclusively by one process and its cached
/// handle owns one shared [`std::sync::RwLock`]. That makes snapshots coherent
/// and mutations serializable inside this engine process, but it is not a Raft
/// or other cluster-wide ordering authority. Consumers that require the latter
/// must fail closed on `LocalOnly`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlSourceAuthorityOrdering {
    LocalOnly,
}

/// Canonical, actor-and-table-specific view of the tenant SQL source authority.
/// Raw ACL rows are intentionally not copied into this token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlSourceAclSnapshot {
    /// Total accepted-operation ordering for this tenant source. A unique
    /// first-time intent consumes one revision even when its requested ACL row
    /// state is already effective, because the same stable retry must remain a
    /// no-op after intervening mutations. Exact receipt replay consumes none.
    pub(crate) source_acl_revision: u64,
    /// Canonical semantic ACL row identity. Unlike the operation revision, this
    /// remains unchanged for an accepted semantic no-op.
    pub(crate) source_acl_digest: String,
    pub(crate) decision_digest: String,
    pub(crate) owner: bool,
    pub(crate) privileges: BTreeSet<SqlPrivilege>,
    pub(crate) rls_column: Option<String>,
    pub(crate) ordering: SqlSourceAuthorityOrdering,
}

#[cfg(test)]
thread_local! {
    static SNAPSHOT_BEFORE_READ_LOCK: std::cell::RefCell<Option<(
        std::sync::mpsc::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn pause_before_snapshot_read_lock(
    reached: std::sync::mpsc::Sender<()>,
    proceed: std::sync::mpsc::Receiver<()>,
) {
    SNAPSHOT_BEFORE_READ_LOCK.with(|hook| *hook.borrow_mut() = Some((reached, proceed)));
}

#[cfg(test)]
fn run_snapshot_before_read_lock_hook() {
    SNAPSHOT_BEFORE_READ_LOCK.with(|hook| {
        if let Some((reached, proceed)) = hook.borrow_mut().take() {
            reached.send(()).expect("snapshot test receiver is alive");
            proceed.recv().expect("snapshot test sender is alive");
        }
    });
}

/// The five independently grantable/revocable SQL privileges (item 2). Deliberately
/// NOT `eg_types::acl::RbacAction` (Read/Write/Admin) — that three-way split would
/// collapse INSERT/UPDATE/DELETE into one bucket, making them impossible to grant
/// or revoke independently, which the spec explicitly requires. The default-deny /
/// owner-or-admin-bypass / generic-denial CONVENTIONS below still follow
/// `eg-core::rbac`/`eg-types::acl`; only the action vocabulary is domain-specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum SqlPrivilege {
    Select,
    Insert,
    Update,
    Delete,
    Alter,
}

/// Batch-local proof that `authority` may operate on exactly one table that an
/// earlier `CREATE TABLE` in the same atomic [`eg_query::TableTxn`] will create.
///
/// The fields stay private so callers cannot manufacture or retarget this
/// capability. It is issued only when the physical table and ACL owner are both
/// absent and the create is not `IF NOT EXISTS`; the underlying redb transaction
/// therefore remains the final first-writer-wins collision fence.
#[derive(Debug, Clone)]
pub(crate) struct ProvisionalCreateAuthority {
    tenant_scope: String,
    actor: String,
    table: String,
}

impl ProvisionalCreateAuthority {
    /// True only for the exact verified carrier and table this capability binds.
    pub(crate) fn permits(&self, authority: &CarrierAuthority, table: &str) -> bool {
        self.tenant_scope == authority.tenant_scope()
            && self.actor == authority.agent_id()
            && self.table == table
    }
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

fn update_len_prefixed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn digest_fields(domain: &[u8], fields: &[&str]) -> String {
    let mut digest = Sha256::new();
    update_len_prefixed(&mut digest, domain);
    for field in fields {
        update_len_prefixed(&mut digest, field.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn canonical_rows(acl: &TableStore, table: &str, width: usize) -> Result<Vec<Vec<String>>, String> {
    let mut canonical = Vec::new();
    for row in acl.scan(table)? {
        if row.len() != width {
            return Err("SQL source ACL contains a malformed canonical row".to_string());
        }
        let mut fields = Vec::with_capacity(width);
        for cell in &row {
            let Cell::Text(value) = cell else {
                return Err("SQL source ACL contains a malformed canonical row".to_string());
            };
            fields.push(value.clone());
        }
        canonical.push(fields);
    }
    canonical.sort();
    canonical.dedup();
    Ok(canonical)
}

fn digest_acl_state(owners: &[Vec<String>], grants: &[Vec<String>], rls: &[Vec<String>]) -> String {
    let mut digest = Sha256::new();
    update_len_prefixed(&mut digest, b"epistemic-graph/sql-source-acl-state");
    for (kind, rows) in [
        (b"owners".as_slice(), owners),
        (b"grants", grants),
        (b"rls", rls),
    ] {
        update_len_prefixed(&mut digest, kind);
        digest.update((rows.len() as u64).to_be_bytes());
        for row in rows {
            digest.update((row.len() as u64).to_be_bytes());
            for field in row {
                update_len_prefixed(&mut digest, field.as_bytes());
            }
        }
    }
    hex::encode(digest.finalize())
}

fn sql_privilege(value: &str) -> Result<SqlPrivilege, String> {
    match value {
        "select" => Ok(SqlPrivilege::Select),
        "insert" => Ok(SqlPrivilege::Insert),
        "update" => Ok(SqlPrivilege::Update),
        "delete" => Ok(SqlPrivilege::Delete),
        "alter" => Ok(SqlPrivilege::Alter),
        _ => Err("SQL source ACL contains an unknown privilege".to_string()),
    }
}

fn effective_privileges(
    authority: &CarrierAuthority,
    table: &str,
    owner: bool,
    grants: &[Vec<String>],
) -> Result<BTreeSet<SqlPrivilege>, String> {
    if authority.is_admin() || owner {
        return Ok(BTreeSet::from([
            SqlPrivilege::Select,
            SqlPrivilege::Insert,
            SqlPrivilege::Update,
            SqlPrivilege::Delete,
            SqlPrivilege::Alter,
        ]));
    }
    grants
        .iter()
        .filter(|row| row[0] == table && row[1] == authority.agent_id())
        .map(|row| sql_privilege(&row[2]))
        .collect()
}

fn digest_acl_decision(
    authority: &CarrierAuthority,
    table: &str,
    owner: bool,
    privileges: &BTreeSet<SqlPrivilege>,
    rls_column: Option<&str>,
    source_acl_digest: &str,
    source_acl_revision: u64,
) -> String {
    let privilege_names = privileges
        .iter()
        .map(|privilege| privilege.as_str())
        .collect::<Vec<_>>()
        .join(",");
    digest_fields(
        b"epistemic-graph/sql-source-acl-decision",
        &[
            authority.tenant_scope(),
            authority.agent_id(),
            table,
            if authority.is_admin() {
                "admin"
            } else {
                "actor"
            },
            if owner { "owner" } else { "not-owner" },
            &privilege_names,
            rls_column.unwrap_or(""),
            source_acl_digest,
            &source_acl_revision.to_string(),
        ],
    )
}

/// Recompute one authoritative SQL source-ACL snapshot while holding the read
/// side of the cached tenant handle's source-authority lock. Only ownership,
/// grants, and RLS rows participate. Retired catalog metadata is neither
/// authorization state nor revision material.
pub(crate) fn source_acl_snapshot(
    persist_dir: &Path,
    authority: &CarrierAuthority,
    table: &str,
) -> Result<SqlSourceAclSnapshot, String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let acl = open_acl(authority.tenant_scope(), persist_dir)?;
    let lock = sql_tables::tenant_source_authority_lock(authority.tenant_scope(), persist_dir)?;
    #[cfg(test)]
    run_snapshot_before_read_lock_hook();
    let guard = lock
        .read()
        .map_err(|_| "SQL source authority lock is unavailable".to_string())?;
    source_acl_snapshot_locked(&guard, &acl, authority, table)
}

trait SourceAuthorityProof {}

impl SourceAuthorityProof for std::sync::RwLockReadGuard<'_, ()> {}

/// Non-cloneable proof that one tenant's local SQL source authority is held
/// exclusively. Wire commits keep this value alive from authorization through
/// the physical table commit and any owner registration it entails.
pub(crate) struct SqlSourceAuthorityWrite<'scope, 'lock> {
    _guard: &'scope std::sync::RwLockWriteGuard<'lock, ()>,
    acl: TableStore,
    authority: &'scope CarrierAuthority,
}

impl SourceAuthorityProof for SqlSourceAuthorityWrite<'_, '_> {}

/// Execute one current-catalog operation while holding the cached tenant ACL
/// handle's sole source-authority write lock. No migration or fallback path is
/// entered before or during the capability.
pub(crate) fn with_source_authority_write<T>(
    persist_dir: &Path,
    authority: &CarrierAuthority,
    operation: impl FnOnce(&SqlSourceAuthorityWrite<'_, '_>) -> Result<T, String>,
) -> Result<T, String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let acl = open_acl(authority.tenant_scope(), persist_dir)?;
    let lock = acl.source_authority_lock();
    let guard = lock
        .write()
        .map_err(|_| "SQL source authority lock is unavailable".to_string())?;
    let source = SqlSourceAuthorityWrite {
        _guard: &guard,
        acl,
        authority,
    };
    operation(&source)
}

fn source_acl_snapshot_locked<G: SourceAuthorityProof>(
    _guard: &G,
    acl: &TableStore,
    authority: &CarrierAuthority,
    table: &str,
) -> Result<SqlSourceAclSnapshot, String> {
    let owners = canonical_rows(acl, OWNERS_TABLE, 2)?;
    let grants = canonical_rows(acl, GRANTS_TABLE, 3)?;
    let rls = canonical_rows(acl, RLS_TABLE, 2)?;
    let source_acl_revision =
        acl.mutation_version(authority.tenant_scope(), SOURCE_ACL_RESOURCE)?;
    let source_acl_digest = digest_acl_state(&owners, &grants, &rls);

    let owner = owners
        .iter()
        .any(|row| row[0] == table && row[1] == authority.agent_id());
    let rls_column = rls
        .iter()
        .find(|row| row[0] == table)
        .map(|row| row[1].clone());
    let privileges = effective_privileges(authority, table, owner, &grants)?;
    let decision_digest = digest_acl_decision(
        authority,
        table,
        owner,
        &privileges,
        rls_column.as_deref(),
        &source_acl_digest,
        source_acl_revision,
    );
    Ok(SqlSourceAclSnapshot {
        source_acl_revision,
        source_acl_digest,
        decision_digest,
        owner,
        privileges,
        rls_column,
        ordering: SqlSourceAuthorityOrdering::LocalOnly,
    })
}

impl SqlSourceAuthorityWrite<'_, '_> {
    pub(crate) fn authority(&self) -> &CarrierAuthority {
        self.authority
    }

    fn snapshot(&self, table: &str) -> Result<SqlSourceAclSnapshot, String> {
        source_acl_snapshot_locked(self, &self.acl, self.authority, table)
    }

    fn authorized_snapshot(
        &self,
        table: &str,
        privilege: SqlPrivilege,
    ) -> Result<SqlSourceAclSnapshot, String> {
        let snapshot = self.snapshot(table)?;
        require_source_ordering(snapshot.ordering, cluster_source_ordering_required())?;
        if snapshot.privileges.contains(&privilege) {
            Ok(snapshot)
        } else {
            Err(ACCESS_DENIED.to_string())
        }
    }
}

fn cluster_source_ordering_required() -> bool {
    cfg!(feature = "raft") && std::env::var_os("EPISTEMIC_GRAPH_RAFT_NODE_ID").is_some()
}

fn require_source_ordering(
    ordering: SqlSourceAuthorityOrdering,
    cluster_active: bool,
) -> Result<(), String> {
    if cluster_active && ordering == SqlSourceAuthorityOrdering::LocalOnly {
        Err("SQL source authority has no replicated ordering".to_string())
    } else {
        Ok(())
    }
}

/// Fail closed unless this process can truthfully serialize SQL source
/// authority. Current storage provides only process-local ordering.
pub(crate) fn require_source_authority() -> Result<(), String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )
}

fn commit_source_acl_mutation(
    _guard: &std::sync::RwLockWriteGuard<'_, ()>,
    acl: &TableStore,
    authority: &CarrierAuthority,
    operation_id: uuid::Uuid,
    kind: &str,
    fields: &[&str],
    txn: &TableTxn,
) -> Result<u64, String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let request_id = u64::from_be_bytes(
        operation_id.as_bytes()[..8]
            .try_into()
            .expect("UUID prefix is eight bytes"),
    );
    let operation_digest = digest_fields(b"epistemic-graph/sql-source-acl-operation", fields);
    // One caller operation may legitimately contain several ACL effects (for
    // example, a multi-table import registering each table owner). Bind the
    // durable child identity to the canonical effect as well as the stable
    // parent UUID so those effects neither collide nor become fresh retries.
    let child_identity = format!("{}:{kind}:{operation_digest}", operation_id.simple());
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "sql-source-acl",
        authority.tenant_scope(),
        &child_identity,
    );
    let method = crate::protocol::Method::ApplyMutation {
        event_type: kind.to_string(),
        query: format!("sha256:{operation_digest}"),
    };
    let created_at_ms = crate::server::txn::now_ms();
    let expected_revision = acl.mutation_version(authority.tenant_scope(), SOURCE_ACL_RESOURCE)?;
    let batch = crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id,
            principal: Some(authority.actor_scope()),
            tenant: authority.tenant_scope(),
            graph: SOURCE_ACL_RESOURCE,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected_revision),
            fencing_token: None,
            created_at_ms,
            default_surface: crate::mutation_batch::MutationSurface::Query,
            authoritative_state: None,
        },
        &method,
        crate::mutation_batch::MutationSurface::Query,
        crate::mutation_batch::DurabilityDomain::SqlCatalog,
        "sql_source_acl_operation",
    )?;
    let committed = acl.commit_txn_batch(txn, &batch, created_at_ms)?;
    match committed.record.committed_version {
        crate::mutation_batch::CommittedVersion::Native { target, .. } => Ok(target),
        _ => Err("SQL source ACL committed without a native revision".to_string()),
    }
}

/// Derive a stable source-operation UUID from an opaque durable caller key.
/// Callers retain one parent identity for a whole logical operation; each ACL
/// effect is disambiguated internally by [`commit_source_acl_mutation`].
pub(crate) fn stable_source_operation_id(operation_key: &str) -> uuid::Uuid {
    let mut digest = Sha256::new();
    update_len_prefixed(&mut digest, b"epistemic-graph/sql-source-operation-id");
    update_len_prefixed(&mut digest, operation_key.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Uuid::from_bytes(bytes)
}

#[cfg(test)]
fn owner_operation_id(authority: &CarrierAuthority, table: &str) -> uuid::Uuid {
    stable_source_operation_id(&format!(
        "test-owner:{}:{}:{table}",
        authority.tenant_scope(),
        authority.agent_id()
    ))
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

/// Open the tenant ACL catalog, ensuring its three system tables exist. Idempotent
/// and cheap on the warm path (`create_table(if_not_exists: true)` against an
/// already-cached [`TableStore`] handle).
fn open_acl(tenant_scope: &str, persist_dir: &Path) -> Result<TableStore, String> {
    let store = sql_tables::tenant_acl_table_store(tenant_scope, persist_dir)?;
    store.create_table(&owners_schema(), true)?;
    store.create_table(&grants_schema(), true)?;
    store.create_table(&rls_schema(), true)?;
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

/// Issue a non-persisted capability for a table an earlier op in the same
/// all-or-nothing batch will create. The caller must first prove the physical
/// table is absent and that the create is not `IF NOT EXISTS`; this function
/// independently proves that no different ACL owner is already retained.
///
/// `Ok(None)` means normal ACL authorization already suffices (admin or the same
/// retained owner). A different owner always receives the generic denial.
pub(crate) fn begin_provisional_create(
    source: &SqlSourceAuthorityWrite<'_, '_>,
    table: &str,
) -> Result<Option<ProvisionalCreateAuthority>, String> {
    let authority = source.authority;
    let acl = &source.acl;
    if authority.is_admin() {
        return Ok(None);
    }
    match owner_of(acl, table)? {
        None => Ok(Some(ProvisionalCreateAuthority {
            tenant_scope: authority.tenant_scope().to_string(),
            actor: authority.agent_id().to_string(),
            table: table.to_string(),
        })),
        Some(owner) if owner == authority.agent_id() => Ok(None),
        Some(_) => Err(ACCESS_DENIED.to_string()),
    }
}

/// `true` only for the exact, deterministic message
/// [`crate::server::sql_tables`]'s underlying `eg_query::TableStore` raises when
/// an `INSERT` loses a UNIQUE-constraint race (`validate_uniqueness_in` in
/// `eg-query/src/tables/store.rs`) — never for any other failure. Used to
/// narrowly distinguish "someone else's concurrent insert already won" (a
/// real, expected outcome to treat as success) from every other `insert_rows`
/// error (storage I/O, a corrupt catalog, …), which must still propagate
/// (item 1: no blanket error-swallowing).
fn is_duplicate_key_error(error: &str) -> bool {
    error.contains("duplicate key value violates unique constraint")
}

/// First-writer-wins ownership registration. A no-op (never overwrites) once the
/// table already has ANY recorded owner — including a concurrent creator that won
/// the UNIQUE-constraint race on `table_name` this call lost.
fn ensure_owner_locked(
    guard: &std::sync::RwLockWriteGuard<'_, ()>,
    acl: &TableStore,
    authority: &CarrierAuthority,
    table: &str,
    operation_id: uuid::Uuid,
) -> Result<(), String> {
    if owner_of(acl, table)?.is_some() {
        return Ok(());
    }
    let mut txn = TableTxn::new();
    txn.push(TxnOp::Insert {
        table: OWNERS_TABLE.to_string(),
        col_order: vec!["table_name".to_string(), "owner".to_string()],
        rows: vec![vec![
            Value::String(table.to_string()),
            Value::String(authority.agent_id().to_string()),
        ]],
    });
    match commit_source_acl_mutation(
        guard,
        acl,
        authority,
        operation_id,
        "sql_source_acl_owner",
        &[table, authority.agent_id()],
        &txn,
    ) {
        Ok(_) => Ok(()),
        // Losing the race here means `insert_rows` returns the UNIQUE-constraint
        // violation on `table_name`; that is exactly "someone else already owns
        // it", not a real error, so ONLY this specific error is treated as
        // success (matching `owner_of`'s subsequent read). Any other error
        // (item 1) propagates — it must never be assumed to be the benign race.
        Err(error) if is_duplicate_key_error(&error) => Ok(()),
        Err(error) => Err(error),
    }
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

fn grant_txn(
    acl: &TableStore,
    table: &str,
    principal: &str,
    privileges: &[SqlPrivilege],
) -> Result<TableTxn, String> {
    let mut rows = Vec::new();
    for privilege in privileges.iter().copied().collect::<BTreeSet<_>>() {
        if !grant_exists(acl, table, principal, privilege)? {
            rows.push(vec![
                Value::String(table.to_string()),
                Value::String(principal.to_string()),
                Value::String(privilege.as_str().to_string()),
            ]);
        }
    }
    let mut txn = TableTxn::new();
    if !rows.is_empty() {
        txn.push(TxnOp::Insert {
            table: GRANTS_TABLE.to_string(),
            col_order: vec![
                "table_name".to_string(),
                "principal".to_string(),
                "privilege".to_string(),
            ],
            rows,
        });
    }
    Ok(txn)
}

fn revoke_txn(
    acl: &TableStore,
    table: &str,
    principal: &str,
    privileges: &[SqlPrivilege],
) -> Result<TableTxn, String> {
    let mut txn = TableTxn::new();
    for privilege in privileges.iter().copied().collect::<BTreeSet<_>>() {
        if grant_exists(acl, table, principal, privilege)? {
            txn.push(TxnOp::Delete {
                table: GRANTS_TABLE.to_string(),
                selector: RowPredicate::And(vec![
                    RowPredicate::Cmp {
                        col: "table_name".to_string(),
                        op: CmpOp::Eq,
                        value: Value::String(table.to_string()),
                    },
                    RowPredicate::Cmp {
                        col: "principal".to_string(),
                        op: CmpOp::Eq,
                        value: Value::String(principal.to_string()),
                    },
                    RowPredicate::Cmp {
                        col: "privilege".to_string(),
                        op: CmpOp::Eq,
                        value: Value::String(privilege.as_str().to_string()),
                    },
                ]),
            });
        }
    }
    Ok(txn)
}

fn canonical_privilege_list(privileges: &[SqlPrivilege]) -> String {
    privileges
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(SqlPrivilege::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

fn rls_txn(acl: &TableStore, table: &str, column: Option<&str>) -> Result<TableTxn, String> {
    let mut txn = TableTxn::new();
    if row_level_column(acl, table)?.as_deref() == column {
        return Ok(txn);
    }
    txn.push(TxnOp::Delete {
        table: RLS_TABLE.to_string(),
        selector: RowPredicate::Cmp {
            col: "table_name".to_string(),
            op: CmpOp::Eq,
            value: Value::String(table.to_string()),
        },
    });
    if let Some(column) = column {
        txn.push(TxnOp::Insert {
            table: RLS_TABLE.to_string(),
            col_order: vec!["table_name".to_string(), "column_name".to_string()],
            rows: vec![vec![
                Value::String(table.to_string()),
                Value::String(column.to_string()),
            ]],
        });
    }
    Ok(txn)
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
    if tenant_scope != authority.tenant_scope() {
        return Err(ACCESS_DENIED.to_string());
    }
    authorized_snapshot(persist_dir, authority, table, privilege).map(|_| ())
}

fn authorized_snapshot(
    persist_dir: &Path,
    authority: &CarrierAuthority,
    table: &str,
    privilege: SqlPrivilege,
) -> Result<SqlSourceAclSnapshot, String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let snapshot = source_acl_snapshot(persist_dir, authority, table)?;
    require_source_ordering(snapshot.ordering, cluster_source_ordering_required())?;
    if snapshot.privileges.contains(&privilege) {
        Ok(snapshot)
    } else {
        Err(ACCESS_DENIED.to_string())
    }
}

/// Grant `grantee_agent_id` one or more [`SqlPrivilege`]s on `table`, within the
/// SAME tenant as `grantor` (there is no cross-tenant grant surface — the tenant
/// is derived from `grantor`'s own verified authority, never caller-supplied).
/// Only the table's owner or an admin carrier may grant (item 2).
pub(crate) fn grant(
    persist_dir: &Path,
    grantor: &CarrierAuthority,
    table: &str,
    grantee_agent_id: &str,
    privileges: &[SqlPrivilege],
    operation_id: uuid::Uuid,
) -> Result<(), String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let acl = open_acl(grantor.tenant_scope(), persist_dir)?;
    let lock = acl.source_authority_lock();
    let guard = lock
        .write()
        .map_err(|_| "SQL source authority lock is unavailable".to_string())?;
    authorize_admin(&acl, grantor, table)?;
    let txn = grant_txn(&acl, table, grantee_agent_id, privileges)?;
    let privilege_list = canonical_privilege_list(privileges);
    commit_source_acl_mutation(
        &guard,
        &acl,
        grantor,
        operation_id,
        "sql_source_acl_grant",
        &[table, grantee_agent_id, &privilege_list],
        &txn,
    )?;
    Ok(())
}

/// Revoke one or more privileges previously granted via [`grant`]. Deletes the
/// backing row(s) outright (no tombstone/deny-row) so the NEXT [`authorize`] call
/// — there is no cache in front of the ACL catalog — sees the change immediately
/// (item: "revoke takes effect immediately").
pub(crate) fn revoke(
    persist_dir: &Path,
    revoker: &CarrierAuthority,
    table: &str,
    grantee_agent_id: &str,
    privileges: &[SqlPrivilege],
    operation_id: uuid::Uuid,
) -> Result<(), String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let acl = open_acl(revoker.tenant_scope(), persist_dir)?;
    let lock = acl.source_authority_lock();
    let guard = lock
        .write()
        .map_err(|_| "SQL source authority lock is unavailable".to_string())?;
    authorize_admin(&acl, revoker, table)?;
    let txn = revoke_txn(&acl, table, grantee_agent_id, privileges)?;
    let privilege_list = canonical_privilege_list(privileges);
    commit_source_acl_mutation(
        &guard,
        &acl,
        revoker,
        operation_id,
        "sql_source_acl_revoke",
        &[table, grantee_agent_id, &privilege_list],
        &txn,
    )?;
    Ok(())
}

/// Declare (`Some(column)`) or clear (`None`) the row-level tenant/principal
/// discriminator column for `table` (item 3). Owner-or-admin only.
pub(crate) fn set_row_level_column(
    persist_dir: &Path,
    authority: &CarrierAuthority,
    table: &str,
    column: Option<&str>,
    operation_id: uuid::Uuid,
) -> Result<(), String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let acl = open_acl(authority.tenant_scope(), persist_dir)?;
    let lock = acl.source_authority_lock();
    let guard = lock
        .write()
        .map_err(|_| "SQL source authority lock is unavailable".to_string())?;
    authorize_admin(&acl, authority, table)?;
    let txn = rls_txn(&acl, table, column)?;
    let rls_operation = rls_operation_kind(column);
    commit_source_acl_mutation(
        &guard,
        &acl,
        authority,
        operation_id,
        "sql_source_acl_rls",
        &[table, rls_operation, column.unwrap_or("")],
        &txn,
    )?;
    Ok(())
}

fn rls_operation_kind(column: Option<&str>) -> &'static str {
    match column {
        Some(_) => "set",
        None => "clear",
    }
}

fn row_level_column(acl: &TableStore, table: &str) -> Result<Option<String>, String> {
    for row in acl.scan(RLS_TABLE)? {
        if text_at(&row, 0) == Some(table) {
            return Ok(text_at(&row, 1).map(str::to_string));
        }
    }
    Ok(None)
}

// ── table creation (registers ownership) ────────────────────────────────────

/// `CREATE TABLE` against the tenant-shared catalog: creates the physical table
/// (via [`sql_tables::tenant_table_store`]) and, only when this call is the one
/// that actually created it, registers `authority` as its owner. An
/// already-existing table (`if_not_exists: true` no-op) is left exactly as-is —
/// this function never silently reassigns ownership.
#[cfg(test)]
pub(crate) fn create_owned_table(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    schema: &TableSchema,
    if_not_exists: bool,
) -> Result<bool, String> {
    with_source_authority_write(persist_dir, authority, |source| {
        let tenant_store = sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
        let created = tenant_store.create_table(schema, if_not_exists)?;
        if created {
            ensure_owner_locked(
                source._guard,
                &source.acl,
                authority,
                &schema.name,
                owner_operation_id(authority, &schema.name),
            )?;
        }
        Ok(created)
    })
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
    acl: TableStore,
    authority: CarrierAuthority,
    source_acl_revision: u64,
    source_acl_digest: String,
    decision_digest: String,
    table: String,
    rls_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticTextRecord {
    /// Opaque SHA-256 bytes derived from the qualified table identity and the
    /// declared primary-key tuple. Raw key values never leave this module. A
    /// non-key update preserves this identity; a primary-key update deliberately
    /// removes the old logical identity and creates a new one. A consumer can
    /// therefore reconcile delete/create semantics across complete snapshots
    /// without treating the redb physical row id as durable identity.
    pub(crate) record_identity_digest: [u8; 32],
    pub(crate) text: String,
}

/// Eight encrypted position bytes plus a 32-byte HMAC. This server-private token
/// is never serialized into a source manifest or receipt. Even its ordinary
/// byte-array Debug form reveals no physical row id without the private key.
pub(crate) type SemanticTextCursor = [u8; 40];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticTextSnapshot {
    pub(crate) tenant_scope: String,
    pub(crate) table: String,
    pub(crate) column: String,
    pub(crate) schema_revision: u64,
    pub(crate) schema_digest: String,
    pub(crate) source_acl_revision: u64,
    pub(crate) source_acl_digest: String,
    pub(crate) decision_digest: String,
    pub(crate) records: Vec<SemanticTextRecord>,
    /// Opaque, authenticated server-private continuation. Physical row ids are
    /// never exposed by this authorized projection.
    pub(crate) next_cursor: Option<SemanticTextCursor>,
    /// Visible rows whose selected text cell is SQL NULL.
    pub(crate) visible_null_count: usize,
    /// Already-authorized rows skipped because they contain no text content.
    /// RLS-hidden rows are nonexistent to this count.
    pub(crate) skipped_count: usize,
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
        digest.update(self.authority.agent_id().as_bytes());
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
/// entry point).
pub(crate) fn open_authorized_table(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    table: &str,
    privilege: SqlPrivilege,
) -> Result<AuthorizedTable, String> {
    let snapshot = authorized_snapshot(persist_dir, authority, table, privilege)?;
    let store = sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
    let acl = open_acl(authority.tenant_scope(), persist_dir)?;
    Ok(AuthorizedTable {
        store,
        acl,
        authority: authority.clone(),
        source_acl_revision: snapshot.source_acl_revision,
        source_acl_digest: snapshot.source_acl_digest,
        decision_digest: snapshot.decision_digest,
        table: table.to_string(),
        rls_column: snapshot.rls_column,
    })
}

fn row_to_map(schema: &TableSchema, row: &[Cell]) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    for (column, cell) in schema.columns().iter().zip(row.iter()) {
        map.insert(column.name.clone(), cell.to_json());
    }
    map
}

/// AND `extra` (if any) with a `rls_column = principal` predicate (if `rls_column`
/// is declared) — the ONE place this fold happens, shared by [`AuthorizedTable`]'s
/// own methods (single-table open/select/update/delete) AND the wire-level
/// pre-commit rewrite of a buffered multi-op [`eg_query::TxnOp::Update`]/`Delete`
/// (CONCEPT:NE-046 — EG-WIRE-CATALOG), which cannot construct an [`AuthorizedTable`]
/// at all (a batched `TableTxn` commits as ONE atomic redb write via
/// `TableStore::commit_txn_batch`, never through this type's own one-shot
/// `TableStore` calls) but must still fold in the SAME RLS predicate before that
/// batch commits. `And(vec![])` evaluates `true` for every row (vacuous truth) —
/// the correct "no constraint at all" predicate when neither RLS nor a
/// caller-supplied filter applies.
fn scoped_predicate(
    rls_column: Option<&str>,
    principal: &str,
    extra: Option<&RowPredicate>,
) -> RowPredicate {
    let mut parts = Vec::new();
    if let Some(column) = rls_column {
        parts.push(RowPredicate::Cmp {
            col: column.to_string(),
            op: CmpOp::Eq,
            value: Value::String(principal.to_string()),
        });
    }
    if let Some(extra) = extra {
        parts.push(extra.clone());
    }
    RowPredicate::And(parts)
}

/// Stamp `rls_column` (if declared) to `principal` on every row of an INSERT —
/// appending it to `col_order` when the caller did not supply it, overwriting
/// whatever value the caller DID supply otherwise (item 3: a caller can never
/// insert a row visible to a different principal than themselves). Shared by
/// the test-only authorized-table insert helper and the wire-level pre-commit rewrite of a
/// buffered [`eg_query::TxnOp::Insert`] for the same reason [`scoped_predicate`]
/// is shared.
fn stamp_insert_rls(
    rls_column: Option<&str>,
    principal: &str,
    col_order: &[String],
    rows: &[Vec<Value>],
) -> (Vec<String>, Vec<Vec<Value>>) {
    let Some(rls_column) = rls_column else {
        return (col_order.to_vec(), rows.to_vec());
    };
    let stamp = Value::String(principal.to_string());
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
            (col_order.to_vec(), stamped)
        }
        None => {
            let mut owned_cols = col_order.to_vec();
            owned_cols.push(rls_column.to_string());
            let stamped: Vec<Vec<Value>> = rows
                .iter()
                .map(|row| {
                    let mut row = row.clone();
                    row.push(stamp.clone());
                    row
                })
                .collect();
            (owned_cols, stamped)
        }
    }
}

fn primary_key_indexes(schema: &TableSchema) -> Result<Vec<usize>, String> {
    let column_primary_key: Vec<usize> = schema
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.then_some(index))
        .collect();
    let table_primary_key = schema.constraints().iter().find_map(|constraint| {
        if let eg_query::tables::schema::TableConstraint::PrimaryKey { columns, .. } = constraint {
            Some(columns)
        } else {
            None
        }
    });
    match (column_primary_key.as_slice(), table_primary_key) {
        ([], None) => Err("semantic SQL source requires a primary key".to_string()),
        (columns, None) => Ok(columns.to_vec()),
        ([], Some(columns)) => columns
            .iter()
            .map(|column| {
                schema
                    .column_index(column)
                    .ok_or_else(|| "semantic SQL source primary key is invalid".to_string())
            })
            .collect(),
        _ => Err("semantic SQL source has ambiguous primary-key identity".to_string()),
    }
}

fn semantic_record_identity_digest(
    tenant_scope: &str,
    selector: &eg_types::semantic_index::SqlColumnRef,
    schema: &TableSchema,
    row: &[Cell],
    primary_key_indexes: &[usize],
) -> Result<[u8; 32], String> {
    let mut digest = Sha256::new();
    update_len_prefixed(
        &mut digest,
        b"epistemic-graph/sql-semantic-record-identity-v1",
    );
    for field in [
        tenant_scope,
        selector.catalog_id.as_str(),
        selector.schema_id.as_str(),
        selector.table_id.as_str(),
    ] {
        update_len_prefixed(&mut digest, field.as_bytes());
    }
    digest.update((primary_key_indexes.len() as u64).to_be_bytes());
    for &index in primary_key_indexes {
        let column = schema
            .columns()
            .get(index)
            .ok_or_else(|| "semantic SQL source primary key is invalid".to_string())?;
        let cell = row
            .get(index)
            .ok_or_else(|| "semantic SQL source row has no primary-key value".to_string())?;
        if matches!(cell, Cell::Null) {
            return Err("semantic SQL source primary key contains NULL".to_string());
        }
        update_len_prefixed(&mut digest, column.name.as_bytes());
        let typed_value = rmp_serde::to_vec_named(&(column.ty, cell))
            .map_err(|_| "semantic SQL source primary key is not encodable".to_string())?;
        update_len_prefixed(&mut digest, &typed_value);
    }
    Ok(digest.finalize().into())
}

fn semantic_cursor_base_mac(
    cursor_auth_secret: &[u8; 32],
    domain: &[u8],
    authority: &CarrierAuthority,
    selector: &eg_types::semantic_index::SqlColumnRef,
    acl: &SqlSourceAclSnapshot,
) -> Result<SemanticCursorMac, String> {
    // The server authentication secret is supplied only to this semantic seam;
    // it is never stored on ordinary table handles or placed in the cursor.
    // Stable configuration lets a durable S1 retry resume after a handle reopen
    // or process restart while every authenticated payload field remains bound.
    let mut mac = SemanticCursorMac::new_from_slice(cursor_auth_secret)
        .map_err(|_| "SQL semantic cursor authority is unavailable".to_string())?;
    for field in [
        domain,
        authority.tenant_scope().as_bytes(),
        authority.actor_scope().as_bytes(),
        authority.owner_scope().as_bytes(),
        authority.agent_id().as_bytes(),
        if authority.is_admin() {
            b"admin".as_slice()
        } else {
            b"actor".as_slice()
        },
        if authority.can_read() {
            b"read".as_slice()
        } else {
            b"no-read".as_slice()
        },
        if authority.can_write() {
            b"write".as_slice()
        } else {
            b"no-write".as_slice()
        },
        selector.catalog_id.as_bytes(),
        selector.schema_id.as_bytes(),
        selector.table_id.as_bytes(),
        selector.column_id.as_bytes(),
        acl.source_acl_digest.as_bytes(),
        acl.decision_digest.as_bytes(),
    ] {
        mac.update(&(field.len() as u64).to_be_bytes());
        mac.update(field);
    }
    mac.update(&acl.source_acl_revision.to_be_bytes());
    Ok(mac)
}

fn semantic_cursor_position(
    cursor_auth_secret: &[u8; 32],
    authority: &CarrierAuthority,
    selector: &eg_types::semantic_index::SqlColumnRef,
    acl: &SqlSourceAclSnapshot,
    cursor: &SemanticTextCursor,
) -> Result<u64, String> {
    let mut mask = semantic_cursor_base_mac(
        cursor_auth_secret,
        b"epistemic-graph/sql-semantic-cursor-position-v1",
        authority,
        selector,
        acl,
    )?;
    mac_len_prefixed(&mut mask, &cursor[8..]);
    let mask = mask.finalize().into_bytes();
    let mut position = [0; 8];
    for (decoded, (encrypted, mask)) in position
        .iter_mut()
        .zip(cursor[..8].iter().zip(mask[..8].iter()))
    {
        *decoded = *encrypted ^ *mask;
    }
    Ok(u64::from_be_bytes(position))
}

fn semantic_cursor_tag(
    cursor_auth_secret: &[u8; 32],
    authority: &CarrierAuthority,
    selector: &eg_types::semantic_index::SqlColumnRef,
    acl: &SqlSourceAclSnapshot,
    page: &eg_query::tables::store::TableRowSnapshot,
    physical_position: &[u8; 8],
) -> Result<SemanticCursorMac, String> {
    let mut mac = semantic_cursor_base_mac(
        cursor_auth_secret,
        b"epistemic-graph/sql-semantic-cursor-tag-v1",
        authority,
        selector,
        acl,
    )?;
    mac.update(&page.schema_revision.to_be_bytes());
    mac.update(&(page.schema_digest.len() as u64).to_be_bytes());
    mac.update(page.schema_digest.as_bytes());
    mac_len_prefixed(&mut mac, physical_position);
    Ok(mac)
}

fn mac_len_prefixed(mac: &mut SemanticCursorMac, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}

fn semantic_cursor(
    cursor_auth_secret: &[u8; 32],
    authority: &CarrierAuthority,
    selector: &eg_types::semantic_index::SqlColumnRef,
    acl: &SqlSourceAclSnapshot,
    page: &eg_query::tables::store::TableRowSnapshot,
    physical_after_row_id: u64,
) -> Result<SemanticTextCursor, String> {
    let physical_position = physical_after_row_id.to_be_bytes();
    let tag = semantic_cursor_tag(
        cursor_auth_secret,
        authority,
        selector,
        acl,
        page,
        &physical_position,
    )?
    .finalize()
    .into_bytes();
    let mut mask = semantic_cursor_base_mac(
        cursor_auth_secret,
        b"epistemic-graph/sql-semantic-cursor-position-v1",
        authority,
        selector,
        acl,
    )?;
    mac_len_prefixed(&mut mask, &tag);
    let mask = mask.finalize().into_bytes();
    let mut cursor = [0; 40];
    for (encrypted, (position, mask)) in cursor[..8]
        .iter_mut()
        .zip(physical_position.iter().zip(mask[..8].iter()))
    {
        *encrypted = *position ^ *mask;
    }
    cursor[8..].copy_from_slice(&tag);
    Ok(cursor)
}

type SemanticRowProjection = (Vec<SemanticTextRecord>, usize, usize, Option<u64>);

fn project_semantic_text_rows(
    tenant_scope: &str,
    selector: &eg_types::semantic_index::SqlColumnRef,
    schema: &TableSchema,
    rows: &[eg_query::tables::store::TableSnapshotRow],
    column_index: usize,
    primary_key_indexes: &[usize],
) -> Result<SemanticRowProjection, String> {
    let mut records = Vec::with_capacity(rows.len());
    let mut visible_null_count = 0usize;
    let mut skipped_count = 0usize;
    let mut payload_bytes = 0usize;
    let mut last_completed_row_id = None;
    let mut payload_cursor = None;
    for physical_row in rows {
        match physical_row.cells.get(column_index) {
            Some(Cell::Text(text)) if text.is_empty() => skipped_count += 1,
            Some(Cell::Text(text)) => {
                let record_bytes = text
                    .len()
                    .checked_add(32)
                    .ok_or_else(|| "semantic SQL source payload is too large".to_string())?;
                if record_bytes > eg_query::tables::store::ROW_SNAPSHOT_MAX_BYTES {
                    return Err("semantic SQL source text exceeds snapshot byte limit".to_string());
                }
                if payload_bytes
                    .checked_add(record_bytes)
                    .is_none_or(|bytes| bytes > eg_query::tables::store::ROW_SNAPSHOT_MAX_BYTES)
                {
                    payload_cursor = last_completed_row_id;
                    break;
                }
                let record_identity_digest = semantic_record_identity_digest(
                    tenant_scope,
                    selector,
                    schema,
                    &physical_row.cells,
                    primary_key_indexes,
                )?;
                records.push(SemanticTextRecord {
                    record_identity_digest,
                    text: text.clone(),
                });
                payload_bytes += record_bytes;
            }
            Some(Cell::Null) => visible_null_count += 1,
            _ => {
                return Err(
                    "semantic SQL source text column contains invalid stored data".to_string(),
                );
            }
        }
        last_completed_row_id = Some(physical_row.row_id);
    }
    Ok((records, visible_null_count, skipped_count, payload_cursor))
}

impl AuthorizedTable {
    fn with_current_authority<T>(
        &self,
        operation: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        require_source_ordering(
            SqlSourceAuthorityOrdering::LocalOnly,
            cluster_source_ordering_required(),
        )?;
        let lock = self.acl.source_authority_lock();
        let _guard = lock
            .read()
            .map_err(|_| "SQL source authority lock is unavailable".to_string())?;
        let current = self
            .acl
            .mutation_version(self.authority.tenant_scope(), SOURCE_ACL_RESOURCE)?;
        if current != self.source_acl_revision {
            return Err(ACCESS_DENIED.to_string());
        }
        operation()
    }

    fn combined_predicate(&self, extra: Option<&RowPredicate>) -> RowPredicate {
        scoped_predicate(self.rls_column.as_deref(), self.authority.agent_id(), extra)
    }

    /// The authorized table's current schema — safe to expose beyond `select`'s own
    /// row-shaping use: the caller already passed [`authorize`] for this exact
    /// `(table, privilege)` to hold an [`AuthorizedTable`] at all, so schema
    /// metadata leaks nothing an existence check wouldn't already.
    pub(crate) fn schema(&self) -> Result<TableSchema, String> {
        self.with_current_authority(|| {
            self.store
                .get_schema(&self.table)?
                .ok_or_else(|| ACCESS_DENIED.to_string())
        })
    }

    /// Every row currently visible to the verified actor: RLS-filtered (if the
    /// table declares a discriminator column) and ANDed with `extra` if given.
    /// Client-side filtering over a full [`TableStore::scan`] — correctness over
    /// pushdown efficiency, matching the rest of this not-yet-wired module (see
    /// the module doc).
    pub(crate) fn select(&self, extra: Option<&RowPredicate>) -> Result<Vec<Vec<Cell>>, String> {
        self.with_current_authority(|| {
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
        })
    }

    /// Produce one bounded semantic source page from a canonical SQL text
    /// selector. The tenant ACL read lock stays held while schema, schema
    /// revision, physical rows, ACL revision, and RLS visibility are sampled.
    /// Raw primary keys and the executable RLS predicate never enter the result.
    pub(crate) fn semantic_text_snapshot(
        &self,
        source_selector: &eg_types::semantic_index::SemanticSourceSelector,
        cursor_auth_secret: &[u8; 32],
        cursor: Option<&SemanticTextCursor>,
    ) -> Result<SemanticTextSnapshot, String> {
        require_source_ordering(
            SqlSourceAuthorityOrdering::LocalOnly,
            cluster_source_ordering_required(),
        )?;
        let selector = source_selector
            .require_implemented()
            .map_err(|_| ACCESS_DENIED.to_string())?;
        if selector.table_id != self.table {
            return Err(ACCESS_DENIED.to_string());
        }

        let lock = self.acl.source_authority_lock();
        let guard = lock
            .read()
            .map_err(|_| "SQL source authority lock is unavailable".to_string())?;
        let acl = source_acl_snapshot_locked(&guard, &self.acl, &self.authority, &self.table)?;
        if acl.source_acl_revision != self.source_acl_revision
            || acl.source_acl_digest != self.source_acl_digest
            || acl.decision_digest != self.decision_digest
            || !acl.privileges.contains(&SqlPrivilege::Select)
        {
            return Err(ACCESS_DENIED.to_string());
        }

        let after_row_id = cursor
            .map(|cursor| {
                semantic_cursor_position(
                    cursor_auth_secret,
                    &self.authority,
                    selector,
                    &acl,
                    cursor,
                )
            })
            .transpose()?;
        let predicate =
            scoped_predicate(acl.rls_column.as_deref(), self.authority.agent_id(), None);
        let page = self
            .store
            .row_snapshot(&self.table, after_row_id, Some(&predicate))?;
        if let (Some(cursor), Some(after_row_id)) = (cursor, after_row_id) {
            let expected = semantic_cursor_tag(
                cursor_auth_secret,
                &self.authority,
                selector,
                &acl,
                &page,
                &after_row_id.to_be_bytes(),
            )?;
            expected
                .verify_slice(&cursor[8..])
                .map_err(|_| ACCESS_DENIED.to_string())?;
        }
        let column_index = page
            .schema
            .column_index(&selector.column_id)
            .filter(|&index| page.schema.columns()[index].ty == ColumnType::Text)
            .ok_or_else(|| ACCESS_DENIED.to_string())?;
        let primary_key_indexes = primary_key_indexes(&page.schema)?;
        let (records, visible_null_count, skipped_count, payload_cursor) =
            project_semantic_text_rows(
                self.authority.tenant_scope(),
                selector,
                &page.schema,
                &page.rows,
                column_index,
                &primary_key_indexes,
            )?;
        let next_cursor = payload_cursor
            .or(page.next_cursor)
            .map(|physical_after_row_id| {
                semantic_cursor(
                    cursor_auth_secret,
                    &self.authority,
                    selector,
                    &acl,
                    &page,
                    physical_after_row_id,
                )
            })
            .transpose()?;
        Ok(SemanticTextSnapshot {
            tenant_scope: self.authority.tenant_scope().to_string(),
            table: selector.table_id.clone(),
            column: selector.column_id.clone(),
            schema_revision: page.schema_revision,
            schema_digest: page.schema_digest,
            source_acl_revision: acl.source_acl_revision,
            source_acl_digest: acl.source_acl_digest,
            decision_digest: acl.decision_digest,
            records,
            next_cursor,
            visible_null_count,
            skipped_count,
        })
    }

    /// Insert rows, forcibly stamping the RLS column (if declared) to
    /// the verified actor on every row — appending it to `col_order` when the
    /// caller did not supply it, overwriting whatever value the caller DID supply
    /// otherwise. A caller can never insert a row visible to a different
    /// principal than themselves.
    #[cfg(test)]
    pub(crate) fn insert(
        &self,
        col_order: &[String],
        rows: &[Vec<Value>],
    ) -> Result<usize, String> {
        self.with_current_authority(|| {
            let (col_order, rows) = stamp_insert_rls(
                self.rls_column.as_deref(),
                self.authority.agent_id(),
                col_order,
                rows,
            );
            self.store.insert_rows(&self.table, &col_order, &rows)
        })
    }

    /// `UPDATE … SET … WHERE …`. The RLS predicate (if any) is ANDed into the
    /// WHERE so a principal can only ever touch their own rows, and — if the
    /// caller's `set` names the RLS column — that assignment is forcibly
    /// overwritten back to the verified actor, so an UPDATE can never move a row
    /// into another principal's visibility.
    #[cfg(test)]
    pub(crate) fn update(
        &self,
        mut set: serde_json::Map<String, Value>,
        predicate: Option<RowPredicate>,
    ) -> Result<usize, String> {
        self.with_current_authority(|| {
            if let Some(rls_column) = &self.rls_column {
                set.insert(
                    rls_column.clone(),
                    Value::String(self.authority.agent_id().to_string()),
                );
            }
            let where_predicate = self.combined_predicate(predicate.as_ref());
            self.store.update_where(&self.table, &set, &where_predicate)
        })
    }

    /// `DELETE FROM … WHERE …`, RLS-constrained like the test-only update helper.
    #[cfg(test)]
    pub(crate) fn delete(&self, predicate: Option<RowPredicate>) -> Result<usize, String> {
        self.with_current_authority(|| {
            let where_predicate = self.combined_predicate(predicate.as_ref());
            self.store.delete_where(&self.table, &where_predicate)
        })
    }

    #[cfg(test)]
    pub(crate) fn table_name(&self) -> &str {
        &self.table
    }
}

// ── wire-layer entry points (CONCEPT:NE-046 — EG-WIRE-CATALOG) ─────────────
//
// The functions below are the ONLY surface this module adds specifically to be
// callable from `src/server/wire/mod.rs`'s buffered, multi-op `TxnOp`/`TableTxn`
// commit path (and the single-table OBDA/COPY call sites in `handlers/rdf.rs` /
// `handlers/sqlite_file.rs`). A buffered `TableTxn` — potentially mixing a
// `CreateTable`, an `Insert`, and an `AlterTable` targeting DIFFERENT tables —
// commits as ONE atomic `TableStore::commit_txn_batch` redb write; there is no
// point at which the wire layer can hold an `AuthorizedTable` per op inside that
// SAME atomic commit (each `AuthorizedTable` method opens its own one-shot
// `TableStore` call). So instead: every op in the buffered batch is authorized
// and RLS-rewritten HERE, entirely BEFORE the batch is handed to
// `commit_txn_batch` — a denial on ANY op aborts the WHOLE transaction with
// NOTHING written, matching the batch's own all-or-nothing atomicity — and
// `commit_txn_batch` itself is never made aware of ACL/RLS at all.

/// Authorize a DDL-shaped op (`DROP TABLE`, every `ALTER TABLE` action, an ANN
/// index / hypertable declaration against a named table) against `table` for
/// `privilege` — always [`SqlPrivilege::Alter`] at every current wire call site,
/// but parameterized rather than hardcoded so a future DDL-shaped op can pick a
/// different privilege without a new entry point. Does not
/// open the table (no read/write follows for this op type through this module —
/// the raw batch commit performs the actual DDL) and does not leak whether the
/// denial was "no such table" vs. "exists but not yours" (the SAME [`authorize`]
/// path every other entry point uses).
pub(crate) fn authorize_ddl(
    source: &SqlSourceAuthorityWrite<'_, '_>,
    table: &str,
    privilege: SqlPrivilege,
) -> Result<(), String> {
    source.authorized_snapshot(table, privilege).map(|_| ())
}

/// Authorize a buffered `TxnOp::Insert` against `table` for
/// [`SqlPrivilege::Insert`] and return the (possibly RLS-stamped) `col_order`/
/// `rows` the wire layer must substitute back into that op before the batch
/// commits — the SAME stamping as the test-only authorized-table insert helper, without
/// executing the write itself (the batch commit does that).
pub(crate) fn authorize_insert(
    source: &SqlSourceAuthorityWrite<'_, '_>,
    table: &str,
    col_order: &[String],
    rows: &[Vec<Value>],
) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
    let snapshot = source.authorized_snapshot(table, SqlPrivilege::Insert)?;
    Ok(stamp_insert_rls(
        snapshot.rls_column.as_deref(),
        source.authority.agent_id(),
        col_order,
        rows,
    ))
}

/// Authorize a buffered `TxnOp::Update` against `table` for
/// [`SqlPrivilege::Update`] and return the (possibly RLS-rewritten) `set`/
/// `selector` the wire layer must substitute back into that op — RLS forces
/// `set[rls_column] = principal` (an UPDATE can never move a row into another
/// principal's visibility) and ANDs `rls_column = principal` into `selector` (a
/// principal can only ever touch their own rows), exactly like
/// the test-only authorized-table update helper.
pub(crate) fn authorize_update(
    source: &SqlSourceAuthorityWrite<'_, '_>,
    table: &str,
    mut set: serde_json::Map<String, Value>,
    selector: RowPredicate,
) -> Result<(serde_json::Map<String, Value>, RowPredicate), String> {
    let snapshot = source.authorized_snapshot(table, SqlPrivilege::Update)?;
    let rls_column = snapshot.rls_column;
    if let Some(column) = &rls_column {
        set.insert(
            column.clone(),
            Value::String(source.authority.agent_id().to_string()),
        );
    }
    let where_predicate = scoped_predicate(
        rls_column.as_deref(),
        source.authority.agent_id(),
        Some(&selector),
    );
    Ok((set, where_predicate))
}

/// Authorize a buffered `TxnOp::Delete` against `table` for
/// [`SqlPrivilege::Delete`] and return the RLS-scoped `selector` the wire layer
/// must substitute back into that op, exactly like the test-only delete helper.
pub(crate) fn authorize_delete(
    source: &SqlSourceAuthorityWrite<'_, '_>,
    table: &str,
    selector: RowPredicate,
) -> Result<RowPredicate, String> {
    let snapshot = source.authorized_snapshot(table, SqlPrivilege::Delete)?;
    Ok(scoped_predicate(
        snapshot.rls_column.as_deref(),
        source.authority.agent_id(),
        Some(&selector),
    ))
}

/// Register an owner without reacquiring the source-authority lock. The wire
/// commit path calls this before releasing the exclusive capability that fenced
/// its schema commit, eliminating the orphan-observation window between the two
/// separately persisted catalogs.
pub(crate) fn register_owner_after_create_in(
    source: &SqlSourceAuthorityWrite<'_, '_>,
    table: &str,
    operation_id: uuid::Uuid,
) -> Result<(), String> {
    ensure_owner_locked(
        source._guard,
        &source.acl,
        source.authority,
        table,
        operation_id,
    )
}

/// Every table name in `authority`'s tenant-shared catalog that `authority` may
/// [`SqlPrivilege::Select`] from — every table it owns, plus every table where it
/// holds an explicit Select grant; every table in the catalog for an admin
/// carrier. Used by the wire read path
/// (CONCEPT:EG-KG.query.register-user-tables-alongside) to build an
/// authorized-only projection of the shared catalog for a free-form multi-table
/// SQL read — the free-form SELECT/JOIN surface has no per-statement table list
/// the way OBDA's `tables` param or a single-table DML op does, so gating happens
/// by restricting the SET of tables materialized for the query instead of a
/// per-reference check.
/// Resolve a SQL/PGQ `GRAPH_TABLE` read against the tenant catalog and lower it,
/// under the CALLER'S OWN authorization.
///
/// A property-graph definition is catalog metadata: it names every base
/// relation, its key columns, its labels, and each property's source column.
/// Resolving it therefore requires the same `Select` the lowered query will
/// need on EVERY base relation the graph pins — otherwise a tenant member with
/// no grants could read the shape of tables it cannot read a row of, and could
/// probe which graph names exist.
///
/// Absence, a foreign tenant scope, and a missing grant on any base relation
/// all return the SAME [`ACCESS_DENIED`], so none of them is distinguishable
/// from the others. Past that gate the caller may already `Select` every
/// relation involved, so lowering diagnostics (unknown label, unresolved
/// property) are returned verbatim: they disclose nothing new.
pub(crate) fn authorized_graph_table_sql(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    query: &eg_query::GraphTableQuery,
) -> Result<String, String> {
    require_source_authority()?;
    let tenant_scope = authority.tenant_scope();
    let store = sql_tables::tenant_table_store(tenant_scope, persist_dir)?;
    let record = store
        .property_graph(tenant_scope, &query.graph)?
        .ok_or_else(|| ACCESS_DENIED.to_string())?;
    let selectable: BTreeSet<String> = selectable_tables(authority, persist_dir)?
        .into_iter()
        .collect();
    if !record
        .dependencies
        .iter()
        .all(|dependency| selectable.contains(dependency.name.object.value()))
    {
        return Err(ACCESS_DENIED.to_string());
    }
    // The base-DDL fence prevents a pinned relation from changing underneath an
    // admitted graph; this re-checks the pinned schema digests at READ time so a
    // bypass fails closed instead of lowering against a schema that moved.
    store.verify_property_graph_dependencies(&record)?;
    eg_query::sql::lower_graph_table(query, &record.accepted_definition, tenant_scope)
        .map(|plan| plan.to_sql())
}

pub(crate) fn selectable_tables(
    authority: &CarrierAuthority,
    persist_dir: &Path,
) -> Result<Vec<String>, String> {
    require_source_ordering(
        SqlSourceAuthorityOrdering::LocalOnly,
        cluster_source_ordering_required(),
    )?;
    let tenant_store = sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
    let all = tenant_store.list_tables()?;
    if authority.is_admin() {
        return Ok(all);
    }
    let acl = open_acl(authority.tenant_scope(), persist_dir)?;
    let mut out = Vec::new();
    for name in all {
        let owner = owner_of(&acl, &name)?;
        if owner.as_deref() == Some(authority.agent_id())
            || grant_exists(&acl, &name, authority.agent_id(), SqlPrivilege::Select)?
        {
            out.push(name);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::auth::VerifiedRequestContext;
    use crate::server::sql_tables::test_persist_dir;

    const SEMANTIC_CURSOR_SECRET: &[u8; 32] = b"semantic-cursor-test-secret-0001";
    const OTHER_SEMANTIC_CURSOR_SECRET: &[u8; 32] = &[7; 32];

    fn authority(agent_id: &str, tenant: &str) -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_in_tenant(
            agent_id, tenant,
        ))
        .unwrap()
    }

    fn mutation_id() -> uuid::Uuid {
        uuid::Uuid::new_v4()
    }

    fn schema(name: &str, columns: Vec<Column>) -> TableSchema {
        TableSchema::new(name, columns)
    }

    fn text_col(name: &str) -> Column {
        Column::new(name, ColumnType::Text, false, false)
    }

    fn with_write<T>(
        dir: &Path,
        authority: &CarrierAuthority,
        operation: impl FnOnce(&SqlSourceAuthorityWrite<'_, '_>) -> Result<T, String>,
    ) -> Result<T, String> {
        with_source_authority_write(dir, authority, operation)
    }

    #[test]
    fn provisional_create_capability_is_exact_and_existing_owner_fails_closed() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-provisional");
        let bob = authority("bob", "tenant-provisional");
        let other_tenant_alice = authority("alice", "tenant-provisional-other");

        with_write(&dir, &alice, |source| {
            let capability = begin_provisional_create(source, "fresh")?
                .expect("unowned table receives a batch-local capability");
            assert!(capability.permits(&alice, "fresh"));
            assert!(!capability.permits(&bob, "fresh"));
            assert!(!capability.permits(&alice, "other"));
            assert!(!capability.permits(&other_tenant_alice, "fresh"));
            Ok(())
        })
        .unwrap();

        with_write(&dir, &alice, |source| {
            register_owner_after_create_in(source, "owned", mutation_id())
        })
        .unwrap();
        assert_eq!(
            with_write(&dir, &bob, |source| {
                begin_provisional_create(source, "owned")
            })
            .unwrap_err(),
            ACCESS_DENIED
        );
        assert!(with_write(&dir, &alice, |source| {
            begin_provisional_create(source, "owned")
        })
        .unwrap()
        .is_none());
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
        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();

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
        grant(
            &dir,
            &alice,
            "t",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        assert!(open_authorized_table(&bob, &dir, "t", SqlPrivilege::Select).is_ok());

        revoke(
            &dir,
            &alice,
            "t",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
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
        set_row_level_column(&dir, &alice, "notes", Some("owner_tag"), mutation_id()).unwrap();
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
            mutation_id(),
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

    // ── restart preserves grants and ownership ──────────────────────────────

    #[test]
    fn restart_preserves_grants_and_ownership() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-restart");
        let tenant_scope = alice.tenant_scope().to_string();
        let before_restart;

        {
            create_owned_table(
                &alice,
                &dir,
                &schema("durable", vec![text_col("id")]),
                false,
            )
            .unwrap();
            grant(
                &dir,
                &alice,
                "durable",
                "bob",
                &[SqlPrivilege::Select],
                mutation_id(),
            )
            .unwrap();
            set_row_level_column(&dir, &alice, "durable", Some("id"), mutation_id()).unwrap();
            before_restart =
                source_acl_snapshot(&dir, &authority("bob", "tenant-restart"), "durable").unwrap();
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
        let after_restart = source_acl_snapshot(&dir, &bob, "durable").unwrap();
        assert_eq!(after_restart, before_restart);
    }

    // ── admin bypass, and non-owner cannot grant ────────────────────────────

    #[test]
    fn only_owner_or_admin_may_grant_or_revoke() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-admin");
        let bob = authority("bob", "tenant-admin");

        create_owned_table(&alice, &dir, &schema("t", vec![text_col("id")]), false).unwrap();

        // Bob is neither owner nor admin — his grant attempt is denied.
        let denied = grant(
            &dir,
            &bob,
            "t",
            "carol",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap_err();
        assert_eq!(denied, ACCESS_DENIED);
    }

    // ── item 5: authorize_ddl is the SAME fail-closed gate the wire layer maps
    //    ANN-index and hypertable DDL onto (`TxnOp::PutAnnIndex`/`PutHypertable`
    //    both call `sql_catalog_acl::authorize_ddl(.., SqlPrivilege::Alter)` in
    //    `src/server/wire/mod.rs::authorize_table_txn` — see that function's
    //    doc comment). These class-specific tests prove the shared primitive
    //    those two object classes are gated on is fail-closed and non-leaky,
    //    without duplicating wire-layer plumbing this module does not own.
    //    Stored functions are catalog-wide (no per-table owner in this ACL's
    //    data model) and are instead gated to `authority.is_admin()` directly
    //    in `wire/mod.rs`'s `authorize_table_txn` — that check never reaches
    //    this module at all, so it has no addressable surface here; see this
    //    track's final report for why that is not a gap in THIS file's scope.

    #[test]
    fn authorize_ddl_denies_ann_index_style_alter_to_a_non_owner_non_admin() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-ann-ddl");
        let bob = authority("bob", "tenant-ann-ddl");

        create_owned_table(
            &alice,
            &dir,
            &schema("vectors", vec![text_col("id"), text_col("embedding")]),
            false,
        )
        .unwrap();

        // Bob has no grant at all — the exact shape `PutAnnIndex { plan }`
        // authorization takes in the wire layer (SqlPrivilege::Alter on
        // `plan.table`). Denied, and indistinguishable from a nonexistent
        // table (same ACCESS_DENIED convention `authorize` documents).
        let denied = with_write(&dir, &bob, |source| {
            authorize_ddl(source, "vectors", SqlPrivilege::Alter)
        })
        .unwrap_err();
        assert_eq!(denied, ACCESS_DENIED);
        let denied_nonexistent = with_write(&dir, &bob, |source| {
            authorize_ddl(source, "no_such_table", SqlPrivilege::Alter)
        })
        .unwrap_err();
        assert_eq!(denied_nonexistent, ACCESS_DENIED);
    }

    #[test]
    fn authorize_ddl_permits_hypertable_style_alter_for_owner_and_admin_only() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-hypertable-ddl");
        let admin = CarrierAuthority::from_verified(
            &VerifiedRequestContext::verified_for_test_with_scopes(
                "root",
                "tenant-hypertable-ddl",
                &["kg:admin"],
            ),
        )
        .unwrap();

        create_owned_table(
            &alice,
            &dir,
            &schema("series", vec![text_col("id"), text_col("ts")]),
            false,
        )
        .unwrap();

        // The owner may Alter — the exact privilege `PutHypertable { plan }`
        // is authorized under in the wire layer.
        assert!(with_write(&dir, &alice, |source| {
            authorize_ddl(source, "series", SqlPrivilege::Alter)
        })
        .is_ok());
        // An admin carrier may Alter ANY table, owned or not, same as every
        // other `authorize`-gated entry point in this module.
        assert!(with_write(&dir, &admin, |source| {
            authorize_ddl(source, "series", SqlPrivilege::Alter)
        })
        .is_ok());
    }

    /// [`is_duplicate_key_error`] must recognize ONLY the exact
    /// `validate_uniqueness_in` race message — proving the item-1 fix
    /// propagates every other `insert_rows` failure instead of blanket
    /// treating any error as "lost the race, ignore it".
    #[test]
    fn duplicate_key_error_detection_is_narrow_not_a_blanket_swallow() {
        assert!(is_duplicate_key_error(
            "duplicate key value violates unique constraint on column `table_name`"
        ));
        assert!(!is_duplicate_key_error("table `t` does not exist"));
        assert!(!is_duplicate_key_error("some unrelated storage failure"));
        assert!(!is_duplicate_key_error(""));
    }

    #[test]
    fn source_acl_revision_advances_once_per_atomic_logical_mutation() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-source-revision");
        let bob = authority("bob", "tenant-source-revision");
        create_owned_table(
            &alice,
            &dir,
            &schema("orders", vec![text_col("owner_tag")]),
            false,
        )
        .unwrap();

        let owner = source_acl_snapshot(&dir, &alice, "orders").unwrap();
        assert_eq!(owner.source_acl_revision, 1);
        assert!(owner.owner);

        let grant_id = mutation_id();
        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Insert, SqlPrivilege::Select],
            grant_id,
        )
        .unwrap();
        let granted = source_acl_snapshot(&dir, &bob, "orders").unwrap();
        assert_eq!(granted.source_acl_revision, 2);
        assert_eq!(granted.privileges.len(), 2);
        assert_ne!(granted.source_acl_digest, owner.source_acl_digest);
        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select, SqlPrivilege::Insert],
            grant_id,
        )
        .unwrap();
        assert_eq!(
            source_acl_snapshot(&dir, &bob, "orders")
                .unwrap()
                .source_acl_revision,
            2,
            "an already-effective grant must not manufacture a source revision"
        );

        let rls_id = mutation_id();
        set_row_level_column(&dir, &alice, "orders", Some("owner_tag"), rls_id).unwrap();
        let rls = source_acl_snapshot(&dir, &bob, "orders").unwrap();
        assert_eq!(rls.source_acl_revision, 3);
        assert_eq!(rls.rls_column.as_deref(), Some("owner_tag"));
        assert_ne!(rls.source_acl_digest, granted.source_acl_digest);
        assert_ne!(rls.decision_digest, granted.decision_digest);
        set_row_level_column(&dir, &alice, "orders", Some("owner_tag"), rls_id).unwrap();
        assert_eq!(
            source_acl_snapshot(&dir, &bob, "orders")
                .unwrap()
                .source_acl_revision,
            3,
            "an already-effective RLS binding must not manufacture a source revision"
        );

        let revoke_id = mutation_id();
        revoke(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select, SqlPrivilege::Insert],
            revoke_id,
        )
        .unwrap();
        let revoked = source_acl_snapshot(&dir, &bob, "orders").unwrap();
        assert_eq!(revoked.source_acl_revision, 4);
        assert!(revoked.privileges.is_empty());
        assert_ne!(revoked.source_acl_digest, rls.source_acl_digest);
        assert_ne!(revoked.decision_digest, rls.decision_digest);

        revoke(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select, SqlPrivilege::Insert],
            revoke_id,
        )
        .unwrap();
        assert_eq!(
            source_acl_snapshot(&dir, &bob, "orders")
                .unwrap()
                .source_acl_revision,
            4,
            "a logical no-op must not manufacture a source revision"
        );
    }

    #[test]
    fn one_stable_parent_identity_registers_multiple_table_children() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-source-parent");
        let parent = stable_source_operation_id("sqlite-import:durable-batch-17");
        assert_eq!(
            parent,
            stable_source_operation_id("sqlite-import:durable-batch-17")
        );
        assert_ne!(
            parent,
            stable_source_operation_id("sqlite-import:durable-batch-18")
        );

        with_write(&dir, &alice, |source| {
            register_owner_after_create_in(source, "orders", parent)?;
            register_owner_after_create_in(source, "items", parent)
        })
        .unwrap();

        let acl = open_acl(alice.tenant_scope(), &dir).unwrap();
        assert_eq!(owner_of(&acl, "orders").unwrap().as_deref(), Some("alice"));
        assert_eq!(owner_of(&acl, "items").unwrap().as_deref(), Some("alice"));
        assert_eq!(
            acl.mutation_version(alice.tenant_scope(), SOURCE_ACL_RESOURCE)
                .unwrap(),
            2,
            "each canonical table effect is a distinct child mutation"
        );

        with_write(&dir, &alice, |source| {
            register_owner_after_create_in(source, "rls_option", parent)
        })
        .unwrap();
        set_row_level_column(&dir, &alice, "rls_option", Some(""), parent).unwrap();
        set_row_level_column(&dir, &alice, "rls_option", None, parent).unwrap();
        assert_eq!(
            row_level_column(&acl, "rls_option").unwrap(),
            None,
            "set(empty) and clear are distinct canonical children"
        );
    }

    #[test]
    fn delayed_acl_retries_never_reverse_newer_authority_state() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-source-delayed-retry");
        let bob = authority("bob", "tenant-source-delayed-retry");
        create_owned_table(&alice, &dir, &schema("orders", vec![text_col("id")]), false).unwrap();

        let old_grant = stable_source_operation_id("grant-request-1");
        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            old_grant,
        )
        .unwrap();
        let old_revoke = stable_source_operation_id("revoke-request-1");
        revoke(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            old_revoke,
        )
        .unwrap();

        // A delayed exact replay may report its prior receipt or an explicit
        // idempotency conflict, but it must never reapply after the revoke.
        let _ = grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            old_grant,
        );
        assert_eq!(
            authorize(
                alice.tenant_scope(),
                &dir,
                &bob,
                "orders",
                SqlPrivilege::Select,
            )
            .unwrap_err(),
            ACCESS_DENIED
        );

        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            stable_source_operation_id("grant-request-2"),
        )
        .unwrap();
        let _ = revoke(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            old_revoke,
        );
        authorize(
            alice.tenant_scope(),
            &dir,
            &bob,
            "orders",
            SqlPrivilege::Select,
        )
        .expect("a delayed revoke replay must not erase a newer grant");
    }

    #[test]
    fn initially_noop_retries_never_become_effective_after_intervening_state() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-source-noop-retry");
        let bob = authority("bob", "tenant-source-noop-retry");
        for table in ["grant_case", "revoke_case", "set_case", "clear_case"] {
            create_owned_table(
                &alice,
                &dir,
                &schema(table, vec![text_col("owner_tag")]),
                false,
            )
            .unwrap();
        }

        grant(
            &dir,
            &alice,
            "grant_case",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        let before_noop = source_acl_snapshot(&dir, &bob, "grant_case").unwrap();
        let initially_noop_grant = stable_source_operation_id("initially-noop-grant");
        grant(
            &dir,
            &alice,
            "grant_case",
            "bob",
            &[SqlPrivilege::Select],
            initially_noop_grant,
        )
        .unwrap();
        let after_noop = source_acl_snapshot(&dir, &bob, "grant_case").unwrap();
        assert_eq!(
            after_noop.source_acl_revision,
            before_noop.source_acl_revision + 1,
            "one accepted unique no-op intent consumes one ordering revision"
        );
        assert_eq!(
            after_noop.source_acl_digest, before_noop.source_acl_digest,
            "a semantic no-op does not change canonical ACL row identity"
        );
        assert_ne!(
            after_noop.decision_digest, before_noop.decision_digest,
            "decision identity binds the accepted-operation ordering revision"
        );
        grant(
            &dir,
            &alice,
            "grant_case",
            "bob",
            &[SqlPrivilege::Select],
            initially_noop_grant,
        )
        .unwrap();
        assert_eq!(
            source_acl_snapshot(&dir, &bob, "grant_case").unwrap(),
            after_noop,
            "exact replay of the accepted no-op consumes no second revision"
        );
        revoke(
            &dir,
            &alice,
            "grant_case",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        grant(
            &dir,
            &alice,
            "grant_case",
            "bob",
            &[SqlPrivilege::Select],
            initially_noop_grant,
        )
        .unwrap();
        assert_eq!(
            authorize(
                alice.tenant_scope(),
                &dir,
                &bob,
                "grant_case",
                SqlPrivilege::Select,
            )
            .unwrap_err(),
            ACCESS_DENIED
        );

        let initially_noop_revoke = stable_source_operation_id("initially-noop-revoke");
        revoke(
            &dir,
            &alice,
            "revoke_case",
            "bob",
            &[SqlPrivilege::Select],
            initially_noop_revoke,
        )
        .unwrap();
        grant(
            &dir,
            &alice,
            "revoke_case",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        revoke(
            &dir,
            &alice,
            "revoke_case",
            "bob",
            &[SqlPrivilege::Select],
            initially_noop_revoke,
        )
        .unwrap();
        authorize(
            alice.tenant_scope(),
            &dir,
            &bob,
            "revoke_case",
            SqlPrivilege::Select,
        )
        .expect("an initially no-op revoke retry cannot erase a later grant");

        set_row_level_column(&dir, &alice, "set_case", Some("owner_tag"), mutation_id()).unwrap();
        let initially_noop_set = stable_source_operation_id("initially-noop-rls-set");
        set_row_level_column(
            &dir,
            &alice,
            "set_case",
            Some("owner_tag"),
            initially_noop_set,
        )
        .unwrap();
        set_row_level_column(&dir, &alice, "set_case", None, mutation_id()).unwrap();
        set_row_level_column(
            &dir,
            &alice,
            "set_case",
            Some("owner_tag"),
            initially_noop_set,
        )
        .unwrap();
        let acl = open_acl(alice.tenant_scope(), &dir).unwrap();
        assert_eq!(row_level_column(&acl, "set_case").unwrap(), None);

        let initially_noop_clear = stable_source_operation_id("initially-noop-rls-clear");
        set_row_level_column(&dir, &alice, "clear_case", None, initially_noop_clear).unwrap();
        set_row_level_column(&dir, &alice, "clear_case", Some("owner_tag"), mutation_id()).unwrap();
        set_row_level_column(&dir, &alice, "clear_case", None, initially_noop_clear).unwrap();
        assert_eq!(
            row_level_column(&acl, "clear_case").unwrap().as_deref(),
            Some("owner_tag")
        );
    }

    #[test]
    fn source_acl_batch_fault_rolls_back_rows_and_revision_together() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-source-fault");
        create_owned_table(&alice, &dir, &schema("orders", vec![text_col("id")]), false).unwrap();
        let acl = open_acl(alice.tenant_scope(), &dir).unwrap();
        let before = acl
            .mutation_version(alice.tenant_scope(), SOURCE_ACL_RESOURCE)
            .unwrap();
        let lock = acl.source_authority_lock();
        let guard = lock.write().unwrap();
        let mut txn = TableTxn::new();
        txn.push(TxnOp::Insert {
            table: GRANTS_TABLE.to_string(),
            col_order: vec![
                "table_name".to_string(),
                "principal".to_string(),
                "privilege".to_string(),
            ],
            rows: vec![vec![
                Value::String("orders".to_string()),
                Value::String("bob".to_string()),
                Value::String("select".to_string()),
            ]],
        });
        txn.push(TxnOp::Insert {
            table: OWNERS_TABLE.to_string(),
            col_order: vec!["table_name".to_string(), "owner".to_string()],
            rows: vec![vec![
                Value::String("orders".to_string()),
                Value::String("mallory".to_string()),
            ]],
        });
        assert!(commit_source_acl_mutation(
            &guard,
            &acl,
            &alice,
            mutation_id(),
            "sql_source_acl_fault_probe",
            &["orders"],
            &txn,
        )
        .is_err());
        drop(guard);

        assert!(!grant_exists(&acl, "orders", "bob", SqlPrivilege::Select).unwrap());
        assert_eq!(
            acl.mutation_version(alice.tenant_scope(), SOURCE_ACL_RESOURCE)
                .unwrap(),
            before
        );
    }

    #[test]
    fn duplicate_privileges_are_one_mutation_and_do_not_change_canonical_identity() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-source-duplicate");
        let bob = authority("bob", "tenant-source-duplicate");
        create_owned_table(&alice, &dir, &schema("orders", vec![text_col("id")]), false).unwrap();

        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select, SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        let before_duplicate = source_acl_snapshot(&dir, &bob, "orders").unwrap();
        assert_eq!(before_duplicate.source_acl_revision, 2);
        let acl = open_acl(alice.tenant_scope(), &dir).unwrap();
        let matching = acl
            .scan(GRANTS_TABLE)
            .unwrap()
            .into_iter()
            .filter(|row| {
                text_at(row, 0) == Some("orders")
                    && text_at(row, 1) == Some("bob")
                    && text_at(row, 2) == Some("select")
            })
            .count();
        assert_eq!(matching, 1, "one request cannot persist duplicate grants");

        acl.insert_rows(
            GRANTS_TABLE,
            &["table_name".into(), "principal".into(), "privilege".into()],
            &[vec![
                Value::String("orders".into()),
                Value::String("bob".into()),
                Value::String("select".into()),
            ]],
        )
        .unwrap();
        let after_duplicate = source_acl_snapshot(&dir, &bob, "orders").unwrap();
        assert_eq!(
            after_duplicate.source_acl_digest, before_duplicate.source_acl_digest,
            "duplicate physical rows are not distinct semantic ACL state"
        );
        assert_eq!(
            after_duplicate.decision_digest, before_duplicate.decision_digest,
            "duplicate physical rows cannot perturb an authorization decision"
        );
    }

    #[test]
    fn local_source_authority_fails_closed_when_cluster_ordering_is_required() {
        assert!(require_source_ordering(SqlSourceAuthorityOrdering::LocalOnly, false).is_ok());
        assert!(require_source_ordering(SqlSourceAuthorityOrdering::LocalOnly, true).is_err());
    }

    #[test]
    fn source_acl_digest_is_canonical_and_decision_is_tenant_actor_rls_specific() {
        let dir = test_persist_dir();
        let alice_a = authority("alice", "tenant-canonical-a");
        let alice_b = authority("alice", "tenant-canonical-b");
        for alice in [&alice_a, &alice_b] {
            create_owned_table(
                alice,
                &dir,
                &schema("orders", vec![text_col("owner_tag")]),
                false,
            )
            .unwrap();
            set_row_level_column(&dir, alice, "orders", Some("owner_tag"), mutation_id()).unwrap();
        }
        grant(
            &dir,
            &alice_a,
            "orders",
            "bob",
            &[SqlPrivilege::Select, SqlPrivilege::Insert],
            mutation_id(),
        )
        .unwrap();
        grant(
            &dir,
            &alice_b,
            "orders",
            "bob",
            &[SqlPrivilege::Insert, SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();

        let bob_a =
            source_acl_snapshot(&dir, &authority("bob", "tenant-canonical-a"), "orders").unwrap();
        let bob_b =
            source_acl_snapshot(&dir, &authority("bob", "tenant-canonical-b"), "orders").unwrap();
        assert_eq!(bob_a.source_acl_digest, bob_b.source_acl_digest);
        assert_ne!(bob_a.decision_digest, bob_b.decision_digest);
        assert_eq!(bob_a.rls_column.as_deref(), Some("owner_tag"));
        assert_eq!(bob_a.ordering, SqlSourceAuthorityOrdering::LocalOnly);

        let alice_snapshot = source_acl_snapshot(&dir, &alice_a, "orders").unwrap();
        assert_ne!(alice_snapshot.decision_digest, bob_a.decision_digest);
        assert!(alice_snapshot.owner);
        assert_eq!(alice_snapshot.privileges.len(), 5);
    }

    #[test]
    fn inert_retired_catalog_rows_do_not_enter_source_acl_identity() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-retired-metadata");
        create_owned_table(&alice, &dir, &schema("orders", vec![text_col("id")]), false).unwrap();
        let before = source_acl_snapshot(&dir, &alice, "orders").unwrap();
        let acl = open_acl(alice.tenant_scope(), &dir).unwrap();

        // Deployments may still contain these inert tables on disk. They are
        // neither opened nor interpreted by current-only runtime authority.
        let marker = "__eg_sql_migrated__";
        acl.create_table(&schema(marker, vec![text_col("actor")]), true)
            .unwrap();
        acl.insert_rows(
            marker,
            &["actor".to_string()],
            &[vec![Value::String("retired-actor".to_string())]],
        )
        .unwrap();
        let notices = "__eg_sql_migration_notices__";
        acl.create_table(
            &schema(notices, vec![text_col("actor"), text_col("notice")]),
            true,
        )
        .unwrap();
        acl.insert_rows(
            notices,
            &["actor".to_string(), "notice".to_string()],
            &[vec![
                Value::String("retired-actor".to_string()),
                Value::String("inert".to_string()),
            ]],
        )
        .unwrap();

        let after = source_acl_snapshot(&dir, &alice, "orders").unwrap();
        assert_eq!(after.source_acl_revision, before.source_acl_revision);
        assert_eq!(after.source_acl_digest, before.source_acl_digest);
        assert_eq!(after.decision_digest, before.decision_digest);
    }

    #[test]
    fn concurrent_snapshot_holds_the_same_lock_as_source_mutation() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-concurrent-snapshot");
        let bob = authority("bob", "tenant-concurrent-snapshot");
        create_owned_table(&alice, &dir, &schema("orders", vec![text_col("id")]), false).unwrap();
        let acl = open_acl(alice.tenant_scope(), &dir).unwrap();
        let lock = acl.source_authority_lock();
        let write_guard = lock.write().unwrap();
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let reader_dir = dir.clone();
        let reader_authority = bob.clone();
        let handle = std::thread::spawn(move || {
            pause_before_snapshot_read_lock(reached_tx, proceed_rx);
            let snapshot = source_acl_snapshot(&reader_dir, &reader_authority, "orders").unwrap();
            done_tx.send(snapshot).unwrap();
        });
        reached_rx.recv().unwrap();
        proceed_tx.send(()).unwrap();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .is_err(),
            "the public snapshot API must block behind a source mutation guard"
        );
        drop(write_guard);
        let during = done_rx.recv().unwrap();
        assert_eq!(during.source_acl_revision, 1);
        assert!(during.privileges.is_empty());
        handle.join().unwrap();
    }

    // ── AuthorizedTable's Debug must never leak the principal ──────────────

    #[test]
    fn authorized_table_is_invalidated_by_a_later_source_acl_revision() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-authorized-revision");
        let bob = authority("bob", "tenant-authorized-revision");
        create_owned_table(&alice, &dir, &schema("orders", vec![text_col("id")]), false).unwrap();
        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        let table = open_authorized_table(&bob, &dir, "orders", SqlPrivilege::Select).unwrap();
        assert!(table.select(None).is_ok());

        revoke(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        assert_eq!(table.select(None).unwrap_err(), ACCESS_DENIED);
        assert_eq!(table.schema().unwrap_err(), ACCESS_DENIED);
    }

    fn semantic_selector(
        table: &str,
        column: &str,
    ) -> eg_types::semantic_index::SemanticSourceSelector {
        eg_types::semantic_index::SemanticSourceSelector::SqlColumnRef(
            eg_types::semantic_index::SqlColumnRef {
                catalog_id: eg_types::semantic_index::SEMANTIC_SQL_CATALOG_ID.to_string(),
                schema_id: eg_types::semantic_index::SEMANTIC_SQL_SCHEMA_ID.to_string(),
                table_id: table.to_string(),
                column_id: column.to_string(),
            },
        )
    }

    #[test]
    fn semantic_text_snapshot_reapplies_rls_and_hides_raw_identity() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-semantic-source");
        let bob = authority("bob", "tenant-semantic-source");
        create_owned_table(
            &alice,
            &dir,
            &schema(
                "documents",
                vec![
                    Column::new("id", ColumnType::Text, false, true),
                    Column::new("body", ColumnType::Text, true, false),
                    Column::new("owner_tag", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
        set_row_level_column(&dir, &alice, "documents", Some("owner_tag"), mutation_id()).unwrap();
        grant(
            &dir,
            &alice,
            "documents",
            "bob",
            &[SqlPrivilege::Select, SqlPrivilege::Insert],
            mutation_id(),
        )
        .unwrap();
        let alice_insert =
            open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Insert).unwrap();
        alice_insert
            .insert(
                &["id".to_string(), "body".to_string()],
                &[
                    vec![
                        Value::String("alice-1".into()),
                        Value::String("first".into()),
                    ],
                    vec![Value::String("alice-2".into()), Value::Null],
                    vec![
                        Value::String("alice-3".into()),
                        Value::String("third".into()),
                    ],
                    vec![
                        Value::String("alice-4".into()),
                        Value::String(String::new()),
                    ],
                ],
            )
            .unwrap();
        let table = open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Select).unwrap();
        let before_hidden = table
            .semantic_text_snapshot(
                &semantic_selector("documents", "body"),
                SEMANTIC_CURSOR_SECRET,
                None,
            )
            .unwrap();
        let bob_insert =
            open_authorized_table(&bob, &dir, "documents", SqlPrivilege::Insert).unwrap();
        bob_insert
            .insert(
                &["id".to_string(), "body".to_string()],
                &[vec![
                    Value::String("bob-1".into()),
                    Value::String("hidden".repeat(4 * 1024 * 1024 / "hidden".len())),
                ]],
            )
            .unwrap();

        let snapshot = table
            .semantic_text_snapshot(
                &semantic_selector("documents", "body"),
                SEMANTIC_CURSOR_SECRET,
                None,
            )
            .unwrap();
        assert_eq!(snapshot.tenant_scope, alice.tenant_scope());
        assert_eq!(snapshot.table, "documents");
        assert_eq!(snapshot.column, "body");
        assert_eq!(snapshot.records.len(), 2);
        assert_eq!(snapshot.records[0].text, "first");
        assert_ne!(snapshot.records[0].record_identity_digest, [0; 32]);
        assert_ne!(
            snapshot.records[0].record_identity_digest,
            snapshot.records[1].record_identity_digest
        );
        assert_eq!(snapshot.records, before_hidden.records);
        assert_eq!(snapshot.visible_null_count, 1);
        assert_eq!(
            snapshot.visible_null_count,
            before_hidden.visible_null_count
        );
        assert_eq!(snapshot.skipped_count, 1);
        assert_eq!(snapshot.skipped_count, before_hidden.skipped_count);
        assert_eq!(snapshot.next_cursor, None);
        assert_eq!(snapshot.source_acl_revision, table.source_acl_revision);
        assert_eq!(snapshot.source_acl_digest, table.source_acl_digest);
        assert_eq!(snapshot.decision_digest, table.decision_digest);

        let first_identity = snapshot.records[0].record_identity_digest;
        let update =
            open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Update).unwrap();
        let mut set = serde_json::Map::new();
        set.insert("body".to_string(), Value::String("updated".to_string()));
        update
            .update(
                set,
                Some(RowPredicate::Cmp {
                    col: "id".to_string(),
                    op: CmpOp::Eq,
                    value: Value::String("alice-1".to_string()),
                }),
            )
            .unwrap();
        let after_update = table
            .semantic_text_snapshot(
                &semantic_selector("documents", "body"),
                SEMANTIC_CURSOR_SECRET,
                None,
            )
            .unwrap();
        let updated = after_update
            .records
            .iter()
            .find(|record| record.text == "updated")
            .unwrap();
        assert_eq!(updated.record_identity_digest, first_identity);

        let mut move_identity = serde_json::Map::new();
        move_identity.insert("id".to_string(), Value::String("alice-1-new".to_string()));
        update
            .update(
                move_identity,
                Some(RowPredicate::Cmp {
                    col: "id".to_string(),
                    op: CmpOp::Eq,
                    value: Value::String("alice-1".to_string()),
                }),
            )
            .unwrap();
        let after_key_update = table
            .semantic_text_snapshot(
                &semantic_selector("documents", "body"),
                SEMANTIC_CURSOR_SECRET,
                None,
            )
            .unwrap();
        let moved = after_key_update
            .records
            .iter()
            .find(|record| record.text == "updated")
            .unwrap();
        assert_ne!(moved.record_identity_digest, first_identity);
        assert!(!after_key_update
            .records
            .iter()
            .any(|record| record.record_identity_digest == first_identity));
    }

    #[test]
    fn semantic_text_snapshot_requires_canonical_selector_text_and_primary_key() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-semantic-validation");
        create_owned_table(
            &alice,
            &dir,
            &schema(
                "notes",
                vec![Column::new("body", ColumnType::Text, true, false)],
            ),
            false,
        )
        .unwrap();
        let table = open_authorized_table(&alice, &dir, "notes", SqlPrivilege::Select).unwrap();
        assert_eq!(
            table
                .semantic_text_snapshot(
                    &semantic_selector("notes", "body"),
                    SEMANTIC_CURSOR_SECRET,
                    None,
                )
                .unwrap_err(),
            "semantic SQL source requires a primary key"
        );

        let mut wrong_schema = semantic_selector("notes", "body");
        let eg_types::semantic_index::SemanticSourceSelector::SqlColumnRef(selector) =
            &mut wrong_schema
        else {
            unreachable!()
        };
        selector.schema_id = "private".to_string();
        assert_eq!(
            table
                .semantic_text_snapshot(&wrong_schema, SEMANTIC_CURSOR_SECRET, None)
                .unwrap_err(),
            ACCESS_DENIED
        );

        let mut wrong_catalog = semantic_selector("notes", "body");
        let eg_types::semantic_index::SemanticSourceSelector::SqlColumnRef(selector) =
            &mut wrong_catalog
        else {
            unreachable!()
        };
        selector.catalog_id = "other-tenant".to_string();
        assert_eq!(
            table
                .semantic_text_snapshot(&wrong_catalog, SEMANTIC_CURSOR_SECRET, None)
                .unwrap_err(),
            ACCESS_DENIED
        );
        assert_eq!(
            table
                .semantic_text_snapshot(
                    &semantic_selector("other", "body"),
                    SEMANTIC_CURSOR_SECRET,
                    None,
                )
                .unwrap_err(),
            ACCESS_DENIED
        );

        create_owned_table(
            &alice,
            &dir,
            &schema(
                "metrics",
                vec![
                    Column::new("id", ColumnType::BigInt, false, true),
                    Column::new("body", ColumnType::Text, true, false),
                ],
            ),
            false,
        )
        .unwrap();
        let metrics = open_authorized_table(&alice, &dir, "metrics", SqlPrivilege::Select).unwrap();
        assert_eq!(
            metrics
                .semantic_text_snapshot(
                    &semantic_selector("metrics", "id"),
                    SEMANTIC_CURSOR_SECRET,
                    None,
                )
                .unwrap_err(),
            ACCESS_DENIED
        );
    }

    #[test]
    fn semantic_text_snapshot_rejects_stale_acl_revision() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-semantic-revision");
        create_owned_table(
            &alice,
            &dir,
            &schema(
                "documents",
                vec![
                    Column::new("id", ColumnType::Text, false, true),
                    Column::new("body", ColumnType::Text, true, false),
                ],
            ),
            false,
        )
        .unwrap();
        let table = open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Select).unwrap();
        grant(
            &dir,
            &alice,
            "documents",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        assert_eq!(
            table
                .semantic_text_snapshot(
                    &semantic_selector("documents", "body"),
                    SEMANTIC_CURSOR_SECRET,
                    None,
                )
                .unwrap_err(),
            ACCESS_DENIED
        );
    }

    #[test]
    fn semantic_record_identity_respects_composite_primary_key_order() {
        use eg_query::tables::schema::TableConstraint;

        let columns = vec![
            Column::new("left", ColumnType::Text, false, false),
            Column::new("right", ColumnType::BigInt, false, false),
            Column::new("body", ColumnType::Text, false, false),
        ];
        let left_then_right = schema("documents", columns.clone()).with_constraints(vec![
            TableConstraint::PrimaryKey {
                name: None,
                columns: vec!["left".to_string(), "right".to_string()],
            },
        ]);
        let right_then_left =
            schema("documents", columns).with_constraints(vec![TableConstraint::PrimaryKey {
                name: None,
                columns: vec!["right".to_string(), "left".to_string()],
            }]);
        let row = vec![Cell::Text("key".to_string()), Cell::Int(7), Cell::Null];
        let eg_types::semantic_index::SemanticSourceSelector::SqlColumnRef(selector) =
            semantic_selector("documents", "body")
        else {
            unreachable!()
        };
        let first_order = primary_key_indexes(&left_then_right).unwrap();
        let first = semantic_record_identity_digest(
            "tenant-composite",
            &selector,
            &left_then_right,
            &row,
            &first_order,
        )
        .unwrap();
        assert_eq!(
            first,
            semantic_record_identity_digest(
                "tenant-composite",
                &selector,
                &left_then_right,
                &row,
                &first_order,
            )
            .unwrap()
        );
        let reverse_order = primary_key_indexes(&right_then_left).unwrap();
        assert_ne!(
            first,
            semantic_record_identity_digest(
                "tenant-composite",
                &selector,
                &right_then_left,
                &row,
                &reverse_order,
            )
            .unwrap()
        );
    }

    #[test]
    fn semantic_text_cursor_is_bounded_authenticated_and_actor_scoped() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-semantic-cursor");
        let bob = authority("bob", "tenant-semantic-cursor");
        create_owned_table(
            &alice,
            &dir,
            &schema(
                "documents",
                vec![
                    Column::new("id", ColumnType::BigInt, false, true),
                    Column::new("body", ColumnType::Text, false, false),
                    Column::new("summary", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
        let insert =
            open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Insert).unwrap();
        let rows = (0..257)
            .map(|id| {
                vec![
                    Value::from(id),
                    Value::from(format!("body-{id}")),
                    Value::from(format!("summary-{id}")),
                ]
            })
            .collect::<Vec<_>>();
        insert
            .insert(
                &["id".to_string(), "body".to_string(), "summary".to_string()],
                &rows,
            )
            .unwrap();
        grant(
            &dir,
            &alice,
            "documents",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();

        let alice_table =
            open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Select).unwrap();
        let bob_table =
            open_authorized_table(&bob, &dir, "documents", SqlPrivilege::Select).unwrap();
        let selector = semantic_selector("documents", "body");
        let first = alice_table
            .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, None)
            .unwrap();
        assert_eq!(
            first.records.len(),
            eg_query::tables::store::ROW_SNAPSHOT_MAX_RECORDS
        );
        let cursor = first.next_cursor.unwrap();
        assert_ne!(&cursor[..8], &255u64.to_be_bytes());
        assert_eq!(
            alice_table
                .semantic_text_snapshot(
                    &semantic_selector("documents", "summary"),
                    SEMANTIC_CURSOR_SECRET,
                    Some(&cursor),
                )
                .unwrap_err(),
            ACCESS_DENIED
        );
        assert_eq!(
            bob_table
                .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, Some(&cursor))
                .unwrap_err(),
            ACCESS_DENIED
        );
        let other_tenant = authority("alice", "tenant-semantic-cursor-other");
        create_owned_table(
            &other_tenant,
            &dir,
            &schema(
                "documents",
                vec![
                    Column::new("id", ColumnType::BigInt, false, true),
                    Column::new("body", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
        let other_table =
            open_authorized_table(&other_tenant, &dir, "documents", SqlPrivilege::Select).unwrap();
        assert_eq!(
            other_table
                .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, Some(&cursor))
                .unwrap_err(),
            ACCESS_DENIED
        );

        let mut tampered = cursor;
        tampered[0] ^= 1;
        assert_eq!(
            alice_table
                .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, Some(&tampered))
                .unwrap_err(),
            ACCESS_DENIED
        );
        assert!(alice_table
            .semantic_text_snapshot(&selector, OTHER_SEMANTIC_CURSOR_SECRET, Some(&cursor))
            .is_err());
        let reopened =
            open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Select).unwrap();
        let final_page = reopened
            .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, Some(&cursor))
            .unwrap();
        assert_eq!(final_page.records.len(), 1);
        assert_eq!(final_page.records[0].text, "body-256");
        assert_eq!(final_page.next_cursor, None);

        let current_schema = reopened.store.get_schema("documents").unwrap().unwrap();
        let migration = eg_query::tables::migration::SchemaMigration::for_schema(
            "semantic-cursor-schema-change",
            alice.tenant_scope(),
            0,
            &current_schema,
            vec![
                eg_query::tables::migration::SchemaMigrationOperation::AddColumn {
                    column: Column::new("label", ColumnType::Text, true, false),
                },
            ],
            Default::default(),
        )
        .unwrap();
        with_write(&dir, &alice, |_source| {
            reopened
                .store
                .apply_schema_migration(&migration)
                .map(|_| ())
        })
        .unwrap();
        assert_eq!(
            reopened
                .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, Some(&cursor))
                .unwrap_err(),
            ACCESS_DENIED
        );
    }

    #[test]
    fn semantic_text_snapshot_bounds_sparse_rls_traversal_without_counting_hidden_rows() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-semantic-sparse");
        let bob = authority("bob", "tenant-semantic-sparse");
        create_owned_table(
            &alice,
            &dir,
            &schema(
                "documents",
                vec![
                    Column::new("id", ColumnType::BigInt, false, true),
                    Column::new("body", ColumnType::Text, false, false),
                    Column::new("owner_tag", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
        set_row_level_column(&dir, &alice, "documents", Some("owner_tag"), mutation_id()).unwrap();
        grant(
            &dir,
            &alice,
            "documents",
            "bob",
            &[SqlPrivilege::Insert],
            mutation_id(),
        )
        .unwrap();
        let bob_insert =
            open_authorized_table(&bob, &dir, "documents", SqlPrivilege::Insert).unwrap();
        let hidden = (0..eg_query::tables::store::ROW_SNAPSHOT_MAX_RECORDS)
            .map(|id| vec![Value::from(id as i64), Value::from("hidden")])
            .collect::<Vec<_>>();
        bob_insert
            .insert(&["id".to_string(), "body".to_string()], &hidden)
            .unwrap();
        let alice_insert =
            open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Insert).unwrap();
        alice_insert
            .insert(
                &["id".to_string(), "body".to_string()],
                &[vec![Value::from(256), Value::from("visible")]],
            )
            .unwrap();

        let table = open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Select).unwrap();
        let selector = semantic_selector("documents", "body");
        let first = table
            .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, None)
            .unwrap();
        assert!(first.records.is_empty());
        assert_eq!(first.visible_null_count, 0);
        assert_eq!(first.skipped_count, 0);
        let cursor = first
            .next_cursor
            .expect("bounded sparse page must continue");

        let second = table
            .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, Some(&cursor))
            .unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].text, "visible");
        assert_eq!(second.visible_null_count, 0);
        assert_eq!(second.skipped_count, 0);
        assert_eq!(second.next_cursor, None);
    }

    #[test]
    fn semantic_text_snapshot_enforces_authorized_payload_byte_limit() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-semantic-bytes");
        create_owned_table(
            &alice,
            &dir,
            &schema(
                "documents",
                vec![
                    Column::new("id", ColumnType::BigInt, false, true),
                    Column::new("body", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
        let insert =
            open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Insert).unwrap();
        insert
            .insert(
                &["id".to_string(), "body".to_string()],
                &[
                    vec![Value::from(1), Value::from("a".repeat(5 * 1024 * 1024))],
                    vec![Value::from(2), Value::from("b".repeat(4 * 1024 * 1024))],
                ],
            )
            .unwrap();
        let table = open_authorized_table(&alice, &dir, "documents", SqlPrivilege::Select).unwrap();
        let selector = semantic_selector("documents", "body");
        let first = table
            .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, None)
            .unwrap();
        assert_eq!(first.records.len(), 1);
        let cursor = first.next_cursor.expect("authorized payload must be paged");
        let second = table
            .semantic_text_snapshot(&selector, SEMANTIC_CURSOR_SECRET, Some(&cursor))
            .unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.next_cursor, None);

        create_owned_table(
            &alice,
            &dir,
            &schema(
                "oversized",
                vec![
                    Column::new("id", ColumnType::BigInt, false, true),
                    Column::new("body", ColumnType::Text, false, false),
                ],
            ),
            false,
        )
        .unwrap();
        let oversized_insert =
            open_authorized_table(&alice, &dir, "oversized", SqlPrivilege::Insert).unwrap();
        oversized_insert
            .insert(
                &["id".to_string(), "body".to_string()],
                &[vec![
                    Value::from(1),
                    Value::from("x".repeat(eg_query::tables::store::ROW_SNAPSHOT_MAX_BYTES + 1)),
                ]],
            )
            .unwrap();
        let oversized =
            open_authorized_table(&alice, &dir, "oversized", SqlPrivilege::Select).unwrap();
        assert_eq!(
            oversized
                .semantic_text_snapshot(
                    &semantic_selector("oversized", "body"),
                    SEMANTIC_CURSOR_SECRET,
                    None,
                )
                .unwrap_err(),
            "semantic SQL source text exceeds snapshot byte limit"
        );
    }

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

    // ── SQL/PGQ: the definition is metadata, and metadata needs a grant ──────

    #[cfg(feature = "query")]
    fn graph_fixture(dir: &Path, owner: &CarrierAuthority) {
        for table in [
            schema(
                "customers",
                vec![
                    Column::new("customer_id", ColumnType::Text, false, true),
                    text_col("name"),
                ],
            ),
            schema(
                "orders",
                vec![
                    Column::new("order_id", ColumnType::Text, false, true),
                    text_col("ordered_when"),
                ],
            ),
        ] {
            assert!(create_owned_table(owner, dir, &table, false).unwrap());
        }
        let ddl = "CREATE PROPERTY GRAPH shop VERTEX TABLES (\
                   customers KEY (customer_id) LABEL customer PROPERTIES (name), \
                   orders KEY (order_id) LABEL \"order\" PROPERTIES (ordered_when))";
        let eg_query::tables::PropertyGraphStatement::Create(definition) =
            eg_query::sql::parse_property_graph_ddl(ddl, owner.tenant_scope()).unwrap()
        else {
            panic!("expected CREATE PROPERTY GRAPH");
        };
        sql_tables::tenant_table_store(owner.tenant_scope(), dir)
            .unwrap()
            .create_property_graph(owner.tenant_scope(), &definition, owner.agent_id())
            .unwrap();
    }

    #[cfg(feature = "query")]
    fn graph_table_query(graph: &str) -> eg_query::GraphTableQuery {
        let sql = format!(
            "SELECT * FROM GRAPH_TABLE ({graph} MATCH (c:customer) COLUMNS (c.name AS customer_name))"
        );
        match eg_query::classify(&sql).unwrap() {
            eg_query::StatementKind::GraphTableReadRequiresCatalogAdmission(query) => query,
            _ => panic!("expected a GRAPH_TABLE read"),
        }
    }

    #[cfg(feature = "query")]
    #[test]
    fn graph_table_resolution_requires_select_on_every_pinned_base_relation() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-pgq-grants");
        let bob = authority("bob", "tenant-pgq-grants");
        graph_fixture(&dir, &alice);
        let query = graph_table_query("shop");

        // The owner of every base relation resolves and lowers it.
        let owner_sql = authorized_graph_table_sql(&alice, &dir, &query).unwrap();
        assert!(
            owner_sql.contains(r#"FROM "public"."customers""#),
            "unexpected lowering: {owner_sql}"
        );

        // Bob has no grant at all: denied, with the generic string.
        assert_eq!(
            authorized_graph_table_sql(&bob, &dir, &query).unwrap_err(),
            ACCESS_DENIED
        );

        // A PARTIAL grant is still a denial -- the definition names both tables.
        grant(
            &dir,
            &alice,
            "customers",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        assert_eq!(
            authorized_graph_table_sql(&bob, &dir, &query).unwrap_err(),
            ACCESS_DENIED
        );

        // Granted on every pinned relation, Bob resolves it like the owner.
        grant(
            &dir,
            &alice,
            "orders",
            "bob",
            &[SqlPrivilege::Select],
            mutation_id(),
        )
        .unwrap();
        assert_eq!(
            authorized_graph_table_sql(&bob, &dir, &query).unwrap(),
            authorized_graph_table_sql(&alice, &dir, &query).unwrap()
        );
    }

    #[cfg(feature = "query")]
    #[test]
    fn graph_catalog_metadata_is_not_disclosed_to_an_ungranted_caller() {
        let dir = test_persist_dir();
        let alice = authority("alice", "tenant-pgq-probe");
        let bob = authority("bob", "tenant-pgq-probe");
        let other_tenant = authority("bob", "tenant-pgq-probe-other");
        graph_fixture(&dir, &alice);

        // An existing graph Bob may not read, a graph that does not exist, and
        // one in another tenant are INDISTINGUISHABLE: the denial carries no
        // base-relation name, no column, and no existence signal.
        let existing = authorized_graph_table_sql(&bob, &dir, &graph_table_query("shop"));
        let absent = authorized_graph_table_sql(&bob, &dir, &graph_table_query("no_such_graph"));
        let foreign = authorized_graph_table_sql(&other_tenant, &dir, &graph_table_query("shop"));
        assert_eq!(existing.as_ref().unwrap_err(), &ACCESS_DENIED.to_string());
        assert_eq!(absent.as_ref().unwrap_err(), &ACCESS_DENIED.to_string());
        assert_eq!(foreign.as_ref().unwrap_err(), &ACCESS_DENIED.to_string());
        for denial in [&existing, &absent, &foreign] {
            let text = denial.as_ref().unwrap_err();
            for leak in ["customers", "orders", "customer_id", "ordered_when", "name"] {
                assert!(!text.contains(leak), "denial leaked `{leak}`: {text}");
            }
        }
    }
}
