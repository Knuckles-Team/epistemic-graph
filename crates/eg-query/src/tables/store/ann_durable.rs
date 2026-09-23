//! Durable state of the maintained user-table ANN authority (RF-019, EH-352).
//!
//! Two owner tables of the SQL layout carry it, both written only through the
//! owner write of an admitted mutation:
//!
//! * `__sql_ann_dirty__` — the changed-row log. Every row write of a table that
//!   has an ANN registration stamps `(table, row id) -> epoch` in the SAME owner
//!   write as the row change, where `epoch` is the source epoch that write
//!   commits at. Row id [`TABLE_EPOCH_ROW`] holds the table's last change epoch,
//!   so staleness is a per-table fact, not a tenant-wide one.
//! * `__sql_ann_generations__` — the activated generations. Generation
//!   [`POINTER_GENERATION`] part 0 names the live generation; a generation's
//!   part 0 is its manifest and parts `1..` its payload chunks. The bytes are
//!   opaque here: the authority encodes and verifies them.
//!
//! Nothing here builds, trains or decodes an index.

use eg_storage::{SQL_ANN_DIRTY, SQL_ANN_GENERATIONS};
use redb::ReadableTable;

use super::{decode_stored, map_err, SqlRead, SqlWrite, TableStore, ANN_INDEXES};
use crate::sql::AnnIndexPlan;

/// The changed-row log's per-table last-change row. Row ids are allocated
/// from a monotonic sequence that never reaches it.
pub(crate) const TABLE_EPOCH_ROW: u64 = u64::MAX;
/// The generation number whose part 0 is the live-generation pointer.
const POINTER_GENERATION: u64 = 0;
/// One stored payload chunk, well inside the store's per-value bound.
const PAYLOAD_PART_BYTES: usize = 8 * 1024 * 1024;

pub(crate) type DirtyReadTable = redb::ReadOnlyTable<(&'static str, u64), u64>;

/// The lower-cased key a table's registrations and changed rows share.
pub(crate) fn ann_table_key(table: &str) -> String {
    table.to_ascii_lowercase()
}

/// The registration-key prefix of every ANN index over `table`.
fn registration_prefix(table: &str) -> String {
    format!("{}.", ann_table_key(table))
}

/// One activated generation, as the authority encoded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredGeneration {
    pub(crate) generation: u64,
    pub(crate) manifest: Vec<u8>,
    pub(crate) payload: Vec<u8>,
}

/// What one activation writes: the generation, the one other generation it
/// still depends on, and how far the changed-row log may be pruned.
pub(crate) struct GenerationWrite<'a> {
    pub(crate) index: &'a str,
    pub(crate) table: &'a str,
    pub(crate) stored: &'a StoredGeneration,
    /// A full generation this one extends; every other generation is removed.
    pub(crate) keeps: Option<u64>,
    /// Changed rows stamped at or below this epoch are covered by every live
    /// generation of the table and are removed. `None` prunes nothing.
    pub(crate) prune_through: Option<u64>,
}

/// Changed row ids read from one snapshot, bounded.
pub(crate) struct ChangedRows {
    pub(crate) rowids: Vec<u64>,
    /// `false` when more changed rows exist than the bound admitted.
    pub(crate) complete: bool,
}

/// Row ids of `table` changed after `epoch`, at most `bound` of them, from the
/// changed-row log of the snapshot `dirty` belongs to.
pub(crate) fn changed_since(
    dirty: &DirtyReadTable,
    table: &str,
    epoch: u64,
    bound: usize,
) -> Result<ChangedRows, String> {
    let key = ann_table_key(table);
    let mut rowids = Vec::new();
    for entry in dirty
        .range((key.as_str(), 0u64)..(key.as_str(), TABLE_EPOCH_ROW))
        .map_err(map_err)?
    {
        let (row, stamped) = entry.map_err(map_err)?;
        if stamped.value() <= epoch {
            continue;
        }
        if rowids.len() == bound {
            return Ok(ChangedRows {
                rowids,
                complete: false,
            });
        }
        rowids.push(row.value().1);
    }
    Ok(ChangedRows {
        rowids,
        complete: true,
    })
}

/// The epoch of `table`'s last row change, `0` when none is logged.
pub(crate) fn table_change_epoch(dirty: &DirtyReadTable, table: &str) -> Result<u64, String> {
    Ok(dirty
        .get((ann_table_key(table).as_str(), TABLE_EPOCH_ROW))
        .map_err(map_err)?
        .map_or(0, |epoch| epoch.value()))
}

/// Stamp one changed row of `table` in the changed-row log, when `table` has an
/// ANN registration. Called from the one row-write chokepoint.
pub(super) fn record_ann_change_in(
    wtx: &SqlWrite<'_>,
    table: &str,
    rowid: u64,
) -> Result<(), String> {
    if !has_registration_in(wtx, table)? {
        return Ok(());
    }
    let epoch = super::authority::staged_source_epoch(wtx)?;
    let key = ann_table_key(table);
    let mut dirty = wtx.open_table(SQL_ANN_DIRTY)?;
    dirty
        .insert((key.as_str(), rowid), epoch)
        .map_err(map_err)?;
    dirty
        .insert((key.as_str(), TABLE_EPOCH_ROW), epoch)
        .map_err(map_err)?;
    Ok(())
}

fn has_registration_in(wtx: &SqlWrite<'_>, table: &str) -> Result<bool, String> {
    let prefix = registration_prefix(table);
    let indexes = wtx.open_table(ANN_INDEXES)?;
    let first = indexes
        .range(prefix.as_str()..)
        .map_err(map_err)?
        .next()
        .transpose()
        .map_err(map_err)?;
    Ok(first.is_some_and(|(key, _)| key.value().starts_with(&prefix)))
}

/// Register `plan` (`CREATE INDEX … USING hnsw|ivfflat`); a redefinition drops
/// the generations built for the old definition.
pub(super) fn put_ann_index_in(wtx: &SqlWrite<'_>, plan: &AnnIndexPlan) -> Result<(), String> {
    let key = TableStore::ann_index_key(plan);
    let bytes = rmp_serde::to_vec_named(plan).map_err(|e| format!("encode ann index: {e}"))?;
    let mut indexes = wtx.open_table(ANN_INDEXES)?;
    let replaced = indexes
        .insert(key.as_str(), bytes.as_slice())
        .map_err(map_err)?
        .is_some_and(|previous| previous.value() != bytes.as_slice());
    drop(indexes);
    if replaced {
        drop_generations_in(wtx, &key)?;
    }
    Ok(())
}

/// Drop every ANN registration on `table`.`column` (all metrics).
pub(super) fn drop_ann_indexes_for_column_in(
    wtx: &SqlWrite<'_>,
    table: &str,
    column: &str,
) -> Result<usize, String> {
    let prefix = format!(
        "{}.{}.",
        table.to_ascii_lowercase(),
        column.to_ascii_lowercase()
    );
    drop_registrations_in(wtx, table, &prefix)
}

/// Remove every ANN registration of `table` whose key starts with `prefix`
/// (`"<table>."` for the whole table, `"<table>.<column>."` for one column),
/// with every generation it owns; a table left with no registration also loses
/// its changed-row log. `Ok(n)` is the number of registrations removed.
pub(super) fn drop_registrations_in(
    wtx: &SqlWrite<'_>,
    table: &str,
    prefix: &str,
) -> Result<usize, String> {
    let mut indexes = wtx.open_table(ANN_INDEXES)?;
    let mut keys = Vec::new();
    for entry in indexes.range(prefix..).map_err(map_err)? {
        let (key, _) = entry.map_err(map_err)?;
        if !key.value().starts_with(prefix) {
            break;
        }
        keys.push(key.value().to_string());
    }
    for key in &keys {
        indexes.remove(key.as_str()).map_err(map_err)?;
    }
    drop(indexes);
    for key in &keys {
        drop_generations_in(wtx, key)?;
    }
    forget_unindexed_table_in(wtx, table)?;
    Ok(keys.len())
}

/// Remove every ANN registration named `name` (its `CREATE INDEX` name, or its
/// catalog key), with its generations and, for a table left unindexed, its
/// changed-row log. `Ok(n)` is the number of registrations removed.
fn drop_named_registrations_in(wtx: &SqlWrite<'_>, name: &str) -> Result<usize, String> {
    let named: Vec<(String, String)> = {
        let indexes = wtx.open_table(ANN_INDEXES)?;
        let mut named = Vec::new();
        for entry in indexes.iter().map_err(map_err)? {
            let (key, value) = entry.map_err(map_err)?;
            let plan: AnnIndexPlan = decode_stored(value.value(), "ANN index")?;
            if plan.name.as_deref() == Some(name) || key.value() == name {
                named.push((key.value().to_string(), plan.table));
            }
        }
        named
    };
    for (key, table) in &named {
        drop_registrations_in(wtx, table, key)?;
    }
    Ok(named.len())
}

/// Remove every generation of registration `index`.
pub(super) fn drop_generations_in(wtx: &SqlWrite<'_>, index: &str) -> Result<(), String> {
    remove_generations_except_in(wtx, index, &[])
}

/// Forget `table`'s changed-row log once it has no ANN registration left.
pub(super) fn forget_unindexed_table_in(wtx: &SqlWrite<'_>, table: &str) -> Result<(), String> {
    if has_registration_in(wtx, table)? {
        return Ok(());
    }
    prune_changes_in(wtx, table, u64::MAX, true)
}

/// Every generation row of `index` except those of `keep`, removed.
fn remove_generations_except_in(
    wtx: &SqlWrite<'_>,
    index: &str,
    keep: &[u64],
) -> Result<(), String> {
    let mut generations = wtx.open_table(SQL_ANN_GENERATIONS)?;
    let stale: Vec<(u64, u64)> = generations
        .range((index, 0u64, 0u64)..=(index, u64::MAX, u64::MAX))
        .map_err(map_err)?
        .map(|entry| entry.map(|(key, _)| (key.value().1, key.value().2)))
        .collect::<Result<_, _>>()
        .map_err(map_err)?;
    for (generation, part) in stale {
        if !keep.contains(&generation) {
            generations
                .remove((index, generation, part))
                .map_err(map_err)?;
        }
    }
    Ok(())
}

/// Remove `table`'s changed rows stamped at or below `through`; the last-change
/// row goes too only when `with_table_row` says so.
fn prune_changes_in(
    wtx: &SqlWrite<'_>,
    table: &str,
    through: u64,
    with_table_row: bool,
) -> Result<(), String> {
    let key = ann_table_key(table);
    let mut dirty = wtx.open_table(SQL_ANN_DIRTY)?;
    let covered: Vec<u64> = dirty
        .range((key.as_str(), 0u64)..=(key.as_str(), TABLE_EPOCH_ROW))
        .map_err(map_err)?
        .filter_map(|entry| match entry {
            Ok((row, stamped)) => {
                let rowid = row.value().1;
                let kept = rowid == TABLE_EPOCH_ROW && !with_table_row;
                (stamped.value() <= through && !kept).then_some(Ok(rowid))
            }
            Err(error) => Some(Err(map_err(error))),
        })
        .collect::<Result<_, _>>()?;
    for rowid in covered {
        dirty.remove((key.as_str(), rowid)).map_err(map_err)?;
    }
    Ok(())
}

fn write_generation_in(wtx: &SqlWrite<'_>, write: &GenerationWrite<'_>) -> Result<(), String> {
    // The drop fence: a build that finished after its registration was dropped
    // never writes a generation for it.
    if wtx
        .open_table(ANN_INDEXES)?
        .get(write.index)
        .map_err(map_err)?
        .is_none()
    {
        return Err(format!("ANN index `{}` was dropped", write.index));
    }
    let stored = write.stored;
    let mut keep = vec![POINTER_GENERATION, stored.generation];
    keep.extend(write.keeps);
    remove_generations_except_in(wtx, write.index, &keep)?;
    let mut generations = wtx.open_table(SQL_ANN_GENERATIONS)?;
    generations
        .insert(
            (write.index, stored.generation, 0u64),
            stored.manifest.as_slice(),
        )
        .map_err(map_err)?;
    for (offset, chunk) in stored.payload.chunks(PAYLOAD_PART_BYTES).enumerate() {
        generations
            .insert((write.index, stored.generation, offset as u64 + 1), chunk)
            .map_err(map_err)?;
    }
    generations
        .insert(
            (write.index, POINTER_GENERATION, 0u64),
            stored.generation.to_be_bytes().as_slice(),
        )
        .map_err(map_err)?;
    Ok(())
}

fn read_generation(
    rtx: &SqlRead<'_>,
    index: &str,
    generation: u64,
) -> Result<Option<StoredGeneration>, String> {
    let generations = rtx.open_owner_table(SQL_ANN_GENERATIONS)?;
    let mut manifest = None;
    let mut payload = Vec::new();
    for entry in generations
        .range((index, generation, 0u64)..=(index, generation, u64::MAX))
        .map_err(map_err)?
    {
        let (key, value) = entry.map_err(map_err)?;
        match key.value().2 {
            0 => manifest = Some(value.value().to_vec()),
            _ => payload.extend_from_slice(value.value()),
        }
    }
    Ok(manifest.map(|manifest| StoredGeneration {
        generation,
        manifest,
        payload,
    }))
}

fn live_generation_number(rtx: &SqlRead<'_>, index: &str) -> Result<Option<u64>, String> {
    let generations = rtx.open_owner_table(SQL_ANN_GENERATIONS)?;
    let Some(pointer) = generations
        .get((index, POINTER_GENERATION, 0u64))
        .map_err(map_err)?
    else {
        return Ok(None);
    };
    let bytes: [u8; 8] = pointer
        .value()
        .try_into()
        .map_err(|_| format!("ANN index `{index}` has an invalid live-generation pointer"))?;
    Ok(Some(u64::from_be_bytes(bytes)))
}

impl TableStore {
    /// Persist one activated generation and move the live pointer to it, in one
    /// maintenance write that also prunes the changed-row log.
    pub(crate) fn persist_ann_generation(&self, write: &GenerationWrite<'_>) -> Result<(), String> {
        self.authority
            .maintain("ann-generation", write.table, |wtx| {
                write_generation_in(wtx, write)?;
                match write.prune_through {
                    Some(through) => prune_changes_in(wtx, write.table, through, false),
                    None => Ok(()),
                }
            })
    }

    /// Drop every ANN index named `name` — the typed drop of the managed-index
    /// lifecycle (EH-352). Fenced: its generations and, for a table left
    /// unindexed, its changed-row log go in the same write, and a build still
    /// running for it can no longer persist. `Ok(n)` is the number dropped.
    pub fn drop_ann_index(&self, name: &str) -> Result<usize, String> {
        let dropped = self.authority.maintain("drop-ann-index", name, |wtx| {
            drop_named_registrations_in(wtx, name)
        })?;
        let registered = self
            .list_ann_indexes()?
            .iter()
            .map(TableStore::ann_index_key)
            .collect();
        self.ann_authority().retain(&registered);
        Ok(dropped)
    }

    /// The live persisted generation of `index`, when one was activated.
    pub(crate) fn live_ann_generation(
        &self,
        index: &str,
    ) -> Result<Option<StoredGeneration>, String> {
        let rtx = self.authority.read()?;
        match live_generation_number(&rtx, index)? {
            Some(generation) => read_generation(&rtx, index, generation),
            None => Ok(None),
        }
    }

    /// Persisted generation `generation` of `index`.
    pub(crate) fn stored_ann_generation(
        &self,
        index: &str,
        generation: u64,
    ) -> Result<Option<StoredGeneration>, String> {
        let rtx = self.authority.read()?;
        read_generation(&rtx, index, generation)
    }

    /// The epoch of `table`'s last row change (per-table ANN staleness).
    pub(crate) fn ann_table_change_epoch(&self, table: &str) -> Result<u64, String> {
        let rtx = self.authority.read()?;
        table_change_epoch(&rtx.open_owner_table(SQL_ANN_DIRTY)?, table)
    }
}
