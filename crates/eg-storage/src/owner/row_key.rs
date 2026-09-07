//! How one owner table's key relates to the serving scopes of its file.
//!
//! Ledger rows have carried a row-level scope bound since the kernel landed:
//! [`crate::ScopedTable`] and [`crate::ScopedTableMut`] refuse any key whose
//! leading component is not the holder's own scope. Owner rows had no such
//! bound, because until `OwnerLayout::GraphShard` every owner file served
//! either one scope or scopes whose rows were separate files.
//!
//! A shard is the first layout where one physical file holds many tenants'
//! owner rows: 42 of its 53 tables lead their key with the graph name and 11
//! belong to the file itself (Raft log and meta, the cross-shard 2PC records,
//! the matview and canary rows, the series key spaces). Without an attribute
//! saying which is which, "no member may reach another member's rows"
//! (RF-RULING-008) is unenforceable for owner rows, and any graph bound to a
//! shard can write the Raft log.
//!
//! The attribute is derived from the census the manifest already carries —
//! `TableScope::Serving` on a shard means, in the registry's own words, "the
//! key's leading component is the graph name" — rather than added as a new
//! `TableContract` field. A new field would change `hash_table_contract` and
//! therefore every layout digest and every persisted owner manifest, and the
//! contract is the registry's to shape; this reads it. Promoting it to a
//! declared field is the follow-up.

use crate::owner::contract::expected_owner_table_contract;
use crate::owner::layout::OwnerLayout;
use crate::physical::manifest::TableScope;
use eg_types::MutationScopeIdentity;

/// The reserved logical name of a graph shard's own file-wide scope.
///
/// Every scope on a shard file is a *graph* scope — `GraphShard` declares
/// `MutationDomain::GraphRows`, which may never own a native scope — so the
/// file's own control rows need a scope too, and the only thing that can
/// distinguish it is its name. This one is reserved: it is refused on every
/// other layout, and on a shard it *is* the control scope, so a user graph
/// routed to it would be the control scope rather than a tenant beside it.
///
/// **One owner for the literal.** The root refuses this name at its own
/// durable chokepoints (`redb_store::SHARD_CONTROL_GRAPH`); the kernel guard
/// and the durable guard have to agree, and two copies of a string that must
/// agree is one copy too many. Every other layer references this constant.
pub const GRAPH_SHARD_CONTROL_GRAPH: &str = "__shard_control__";

/// The tenant component of every graph-shard mutation scope.
///
/// RF-RULING-004 application note 2: a shard scope is
/// `(this tenant, graph name, graph incarnation)` — the same shape the
/// cluster-admin scope already uses. The shard's durable authority has always
/// been per graph (its OCC counter and every shard table key lead with the
/// graph name, never a tenant), and no durable structure records a graph→tenant
/// binding at all, so the Raft follower apply path and recovery — which have no
/// request carrier — could not supply a caller's tenant even in principle.
///
/// The caller's tenant is request-boundary authorization and outbox
/// attribution; it is deliberately NOT part of the scope identity, because
/// making it so would give one graph a different binding digest, ledger scope
/// key, OCC counter and fence depending on who wrote to it.
///
/// Reserved, and refused as a caller tenant at the request boundary: a caller
/// able to name it could compile a batch whose scope identity collides with the
/// shard's own.
pub const GRAPH_SHARD_TENANT: &str = "__shard__";

/// What one owner table's key says about scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKey {
    /// The key's leading component is the serving scope's own name, so a row
    /// is addressable within one scope and the table is shared by many.
    ScopePrefixed,
    /// The row belongs to the file, not to any one serving scope.
    FileWide,
    /// The layout draws no such distinction: its owner tables are reached
    /// under the layout bound alone, as they were before scope groups existed.
    Unscoped,
}

/// The row-key class of one owner table of `layout`.
pub fn owner_row_key(name: &str, layout: OwnerLayout) -> RowKey {
    if layout != OwnerLayout::GraphShard {
        return RowKey::Unscoped;
    }
    match expected_owner_table_contract(name, layout).scope {
        TableScope::Serving => RowKey::ScopePrefixed,
        _ => RowKey::FileWide,
    }
}

/// The reserved control scope name of `layout`, when it has one.
pub fn reserved_control_graph(layout: OwnerLayout) -> Option<&'static str> {
    (layout == OwnerLayout::GraphShard).then_some(GRAPH_SHARD_CONTROL_GRAPH)
}

/// Whether `identity` is `layout`'s reserved file-wide control scope.
///
/// Derived from the identity, never from a caller's argument order, so the
/// control/serving split holds identically for a sole admit, for a group
/// member, and on the read side.
pub fn is_control_scope(layout: OwnerLayout, identity: &MutationScopeIdentity) -> bool {
    match (reserved_control_graph(layout), identity.scope().graph_name()) {
        (Some(reserved), Some(graph)) => graph.as_str() == reserved,
        _ => false,
    }
}

/// Refuse the reserved control name to a layout that has not reserved it.
///
/// Checked where a grant is authenticated, so no store outside the shard
/// layout can ever bind a scope by that name and inherit a class it does not
/// declare.
pub(crate) fn permit_reserved_name(
    layout: OwnerLayout,
    identity: &MutationScopeIdentity,
) -> Result<(), String> {
    let Some(graph) = identity.scope().graph_name() else {
        return Ok(());
    };
    if graph.as_str() == GRAPH_SHARD_CONTROL_GRAPH && reserved_control_graph(layout).is_none() {
        return Err("this layout does not reserve a file-wide control scope".to_string());
    }
    Ok(())
}

/// A key whose leading component names the serving scope that owns the row.
///
/// The owner-row counterpart of [`crate::LedgerRowScope`]. A ledger key leads
/// with the scope's 64-hex binding digest; an owner key leads with the scope's
/// logical name, because that is what the domain's own reads and writes carry.
pub trait OwnerRowScope {
    fn owner_scope(&self) -> &str;
}

impl OwnerRowScope for &str {
    fn owner_scope(&self) -> &str {
        self
    }
}

macro_rules! owner_row_scope_tuples {
    ($( ($($rest:ty),*) ),* $(,)?) => {
        $(
            impl OwnerRowScope for (&str, $($rest),*) {
                fn owner_scope(&self) -> &str {
                    self.0
                }
            }
        )*
    };
}

owner_row_scope_tuples!(
    (u64),
    (&str),
    (&str, u32),
    (&str, u64),
    (&str, &str),
    (&str, &str, u32),
    (&str, &str, &str),
    (&str, &str, u64, &str),
);
