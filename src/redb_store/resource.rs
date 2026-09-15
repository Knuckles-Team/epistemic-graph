//! Native resource reservation: admission, lifecycle, query and status.
//!
//! Every table this module touches is scope-prefixed -- a reservation, a host,
//! an exclusivity key and a fairness debt all lead their key with the graph --
//! so every row access here goes through the graph member's own
//! `open_scoped_table` / `scoped_owner_table` and is confined to that graph by
//! the capability rather than by an argument. There is no file-wide table here,
//! and therefore no control-member access: the nine `resource_*` tables plus
//! `nodes` are all `Serving`.
//!
//! Two consequences of that confinement are visible in the read paths below.
//! A scoped table has no `range(start..)`, so the two prefix scans this module
//! runs -- the tenant index and one host's disk policies -- are
//! `ScopedOwnerTable::scope_rows`, which starts at the graph's least key rather
//! than at the caller's prefix. Both therefore SKIP the rows sorting before the
//! prefix instead of taking the first non-matching row as the end of the scan;
//! their `MAX_RESOURCE_*_SCAN` budgets are unchanged and still bound the work.

use super::*;

use eg_storage::{GraphShardOwner, ScopedRead};

use crate::redb_store::shard::Shard;

// RMDD-27 native reservation bounds.  These are deliberately independent of
// the much larger durable MessagePack budget: reservation strings and status
// scans are public control-plane inputs and must remain cheap to validate.
pub(crate) const MAX_RESOURCE_TEXT: usize = 256;
pub(crate) const MAX_RESOURCE_LABELS: usize = 128;
pub(crate) const MAX_RESOURCE_STATUS_LIMIT: usize = 1_000;
pub(crate) const MAX_RESOURCE_STATUS_SCAN: usize = 100_000;
// ResourceHostUpdate/Status schemas expose at most 128 versioned disk-policy
// rows. Admission uses the same bound before creating a new host+policy key;
// otherwise a native peer could persist a snapshot that generated clients
// cannot decode or force an unbounded policy scan during reconciliation.
pub(crate) const MAX_RESOURCE_HOST_DISK_POLICIES: usize = 128;
// Graph clear/delete is an administrative operation, but its drain check must
// remain bounded in allocation even if a hostile or corrupted graph accumulated
// a large terminal history.  Deletion proceeds in bounded key chunks from an
// in-transaction cursor; the cap is not a lifetime limit on tombstone history.
pub(crate) const MAX_RESOURCE_CLEAR_SCAN: usize = 100_000;
pub(crate) const MAX_RESOURCE_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
pub(crate) const RESOURCE_HEARTBEAT_GRACE_MS: u64 = 120_000;
pub(crate) const MAX_RESOURCE_DIMENSION: u64 = 1_000_000_000_000;

mod codec;
mod extension;
mod fingerprint;
mod query;
mod status;
mod validation;
mod write_admission;
mod write_admit;
mod write_lifecycle;

pub(crate) use codec::*;
pub(crate) use extension::*;
pub(crate) use fingerprint::*;
pub(crate) use query::*;
#[cfg(any(test, feature = "server"))]
pub(crate) use status::*;
pub(crate) use validation::*;
pub(crate) use write_admission::*;
pub(crate) use write_admit::*;
pub(crate) use write_lifecycle::*;
