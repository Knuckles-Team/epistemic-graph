use super::store_prelude::*;
use super::*;

pub(crate) fn clear_graph_rows(
    graph: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
) -> Result<(), String> {
    let node_keys: Vec<String> = nodes
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, node_id) = key.value();
            if row_graph != graph {
                return Err("graph node row escaped its scope".to_string());
            }
            Ok(node_id.to_string())
        })
        .collect::<Result<_, String>>()?;
    for id in node_keys {
        let _ = nodes.remove((graph, id.as_str()));
    }
    let edge_keys: Vec<(String, String, u32)> = edges
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, source, target, ordinal) = key.value();
            if row_graph != graph {
                return Err("graph edge row escaped its scope".to_string());
            }
            Ok((source.to_string(), target.to_string(), ordinal))
        })
        .collect::<Result<_, String>>()?;
    for (s, t, o) in edge_keys {
        let _ = edges.remove((graph, s.as_str(), t.as_str(), o));
    }
    let seqs: Vec<u64> = ledger
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, sequence) = key.value();
            if row_graph != graph {
                return Err("graph ledger row escaped its scope".to_string());
            }
            Ok(sequence)
        })
        .collect::<Result<_, String>>()?;
    for seq in seqs {
        let _ = ledger.remove((graph, seq));
    }
    // EG-029: every edge for `graph` is gone — drop all cached ordinals so a later
    // AddEdge (incl. checkpoint re-population, which clears then re-adds) re-seeds from
    // the post-clear state. Covers ClearGraph, purge_graph_rows, and apply_checkpoint.
    invalidate_graph_edge_ords(graph);
    Ok(())
}

/// `Method::ClearLedger`'s durable table-row effect: remove every durable
/// `LEDGER` row for `graph`, leaving `nodes`/`edges`/resources untouched --
/// the row-scoped sibling of [`clear_graph_rows`]'s ledger-clearing loop
/// (factored out rather than shared, since `ClearGraph` legitimately clears
/// nodes/edges/resources TOO and this method must not).
pub(crate) fn clear_ledger_rows(
    graph: &str,
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
) -> Result<(), String> {
    let seqs: Vec<u64> = ledger
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (row_graph, sequence) = key.value();
            if row_graph != graph {
                return Err("graph ledger row escaped its scope".to_string());
            }
            Ok(sequence)
        })
        .collect::<Result<_, String>>()?;
    for seq in seqs {
        let _ = ledger.remove((graph, seq));
    }
    Ok(())
}

// Terminal reservation rows are retained as exact lifecycle tombstones,
// but they no longer hold capacity.  A graph clear/delete may remove that
// terminal history atomically; a live Reserved row still requires an
// explicit release/reclaim drain so it cannot silently strand capacity.
pub(crate) fn resource_reservation_row_is_active(stored: &DurableResourceReservation) -> bool {
    stored.record.state == ResourceReservationRecordState::Reserved
        || stored.held_cpu_weight != 0
        || stored.held_memory_mib != 0
        || stored.held_disk_mib != 0
        || stored.held_process_slots != 0
}

pub(crate) fn check_resource_reservations_active(
    graph: &str,
    reservations: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    tenant_index: &ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    let mut has_active_rows = false;
    let rows = reservations
        .scope_rows()
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, row_reservation_id) = key.value();
        if row_graph != graph {
            return Err("resource reservation row escaped its scope".into());
        }
        let stored: DurableResourceReservation = resource_decode(value.value(), crypto)?;
        if row_reservation_id != stored.record.reservation_id {
            return Err("resource reservation key/index consistency check failed".into());
        }
        let index_value = tenant_index
            .get((
                graph,
                stored.record.tenant_ref.as_str(),
                stored.record.reservation_id.as_str(),
            ))
            .map_err(|error| error.to_string())?;
        if index_value.as_ref().map(|entry| entry.value())
            != Some(stored.record.reservation_id.as_str())
        {
            return Err("resource tenant index consistency check failed".into());
        }
        // A Reserved row, or any row that still carries held capacity, is
        // a live claim even if a corrupted or partially written value also
        // carries the tombstone bit.  Refuse the destructive lifecycle
        // operation for either representation; never infer that held
        // capacity is safe to drop from one flag.
        if resource_reservation_row_is_active(&stored) {
            has_active_rows = true;
        }
    }
    Ok(has_active_rows)
}

pub(crate) fn check_resource_tenant_index_consistency(
    graph: &str,
    tenant_index: &ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    reservations: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let rows = tenant_index
        .scope_rows()
        .map_err(|error| error.to_string())?;
    for row in rows {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, tenant, reservation_id) = key.value();
        if row_graph != graph {
            return Err("resource tenant index row escaped its scope".into());
        }
        if value.value() != reservation_id {
            return Err("resource tenant index key/value consistency check failed".into());
        }
        let reservation = reservations
            .get((graph, reservation_id))
            .map_err(|error| error.to_string())?;
        let Some(reservation) = reservation else {
            return Err("resource tenant index references missing reservation".into());
        };
        let stored: DurableResourceReservation = resource_decode(reservation.value(), crypto)?;
        if stored.record.tenant_ref != tenant {
            return Err("resource tenant index tenant mismatch".into());
        }
    }
    Ok(())
}

pub(crate) fn collect_resource_two_part_clear_keys<V: redb::Value + 'static>(
    table: &ScopedOwnerTableMut<'_, (&str, &str), V>,
    graph: &str,
    cursor: &Option<String>,
) -> Result<Vec<String>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, key_part) = key.value();
        if row_graph != graph {
            return Err("resource row escaped its scope".into());
        }
        if cursor
            .as_deref()
            .is_some_and(|cursor_key| key_part <= cursor_key)
        {
            continue;
        }
        keys.push(key_part.to_string());
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

/// Clear every `(graph, key)` row of a two-part-key table for `graph`, in
/// bounded `MAX_RESOURCE_CLEAR_SCAN`-sized passes so the caller's open
/// `WriteTransaction` never has to allocate an unbounded key list.
/// One bounded scan pass of `clear_resource_two_part_table`: collects at most
/// `MAX_RESOURCE_CLEAR_SCAN` second-key parts for `graph`, starting after
/// `cursor`.  Mirrors `collect_resource_attempts_clear_keys`.
pub(crate) fn clear_resource_two_part_table<V: redb::Value + 'static>(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str), V>,
    graph: &str,
) -> Result<(), String> {
    let mut cursor: Option<String> = None;
    loop {
        let keys = collect_resource_two_part_clear_keys(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for key in &keys {
            table
                .remove((graph, key.as_str()))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

pub(crate) fn collect_resource_attempts_clear_keys(
    table: &ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    graph: &str,
    cursor: &Option<(String, u64)>,
) -> Result<Vec<(String, u64)>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, work_item, attempt) = key.value();
        if row_graph != graph {
            return Err("resource attempt row escaped its scope".into());
        }
        if cursor
            .as_ref()
            .is_some_and(|(cursor_work_item, cursor_attempt)| {
                (work_item, attempt) <= (cursor_work_item.as_str(), *cursor_attempt)
            })
        {
            continue;
        }
        keys.push((work_item.to_string(), attempt));
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

pub(crate) fn clear_resource_attempts_table(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    graph: &str,
) -> Result<(), String> {
    let mut cursor: Option<(String, u64)> = None;
    loop {
        let keys = collect_resource_attempts_clear_keys(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for (work_item, attempt) in &keys {
            table
                .remove((graph, work_item.as_str(), *attempt))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

pub(crate) fn collect_resource_tenant_index_clear_keys(
    table: &ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    graph: &str,
    cursor: &Option<(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, tenant, reservation_id) = key.value();
        if row_graph != graph {
            return Err("resource tenant-index row escaped its scope".into());
        }
        if value.value() != reservation_id {
            return Err("resource tenant index key/value escaped clear scope".into());
        }
        if cursor
            .as_ref()
            .is_some_and(|(cursor_tenant, cursor_reservation)| {
                (tenant, reservation_id) <= (cursor_tenant.as_str(), cursor_reservation.as_str())
            })
        {
            continue;
        }
        keys.push((tenant.to_string(), reservation_id.to_string()));
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

/// Table-specific row validation + key extraction for one
/// [`clear_resource_three_part_table`] pass: given the table, the owning
/// graph, and the resume cursor, return up to `MAX_RESOURCE_CLEAR_SCAN`
/// `(second, third)` key parts eligible for removal.
pub(crate) type ResourceRowCollector<V> = fn(
    &ScopedOwnerTableMut<'_, (&str, &str, &str), V>,
    &str,
    &Option<(String, String)>,
) -> Result<Vec<(String, String)>, String>;

/// One bounded scan-and-remove pass over a three-part-key owner table,
/// generalized over the row value type `V`. `collect` supplies the
/// table-specific row validation and key extraction — the tenant-index and
/// anti-affinity tables enforce different invariants on their differently
/// shaped values (the tenant index redundantly stores the reservation id as
/// its value and checks it against the third key part; anti-affinity's value
/// is an unrelated `u64` weight), so that half stays table-specific. This
/// helper owns only the shared cursor-pagination and delete loop around it:
/// collect up to `MAX_RESOURCE_CLEAR_SCAN` keys past the resume cursor,
/// delete them, and resume from the last one, until a pass collects none.
/// Mirrors `clear_resource_two_part_table` for the two-part-key tables.
pub(crate) fn clear_resource_three_part_table<V: redb::Value + 'static>(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), V>,
    graph: &str,
    collect: ResourceRowCollector<V>,
) -> Result<(), String> {
    let mut cursor: Option<(String, String)> = None;
    loop {
        let keys = collect(table, graph, &cursor)?;
        if keys.is_empty() {
            break;
        }
        for (a, b) in &keys {
            table
                .remove((graph, a.as_str(), b.as_str()))
                .map_err(|error| error.to_string())?;
        }
        cursor = keys.last().cloned();
    }
    Ok(())
}

pub(crate) fn clear_resource_tenant_index_table(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), &str>,
    graph: &str,
) -> Result<(), String> {
    clear_resource_three_part_table(table, graph, collect_resource_tenant_index_clear_keys)
}

pub(crate) fn collect_resource_anti_affinity_clear_keys(
    table: &ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    graph: &str,
    cursor: &Option<(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let mut keys = Vec::with_capacity(MAX_RESOURCE_CLEAR_SCAN);
    for row in table.scope_rows().map_err(|error| error.to_string())? {
        let (key, _) = row.map_err(|error| error.to_string())?;
        let (row_graph, host, tag) = key.value();
        if row_graph != graph {
            return Err("resource anti-affinity row escaped its scope".into());
        }
        if cursor.as_ref().is_some_and(|(cursor_host, cursor_tag)| {
            (host, tag) <= (cursor_host.as_str(), cursor_tag.as_str())
        }) {
            continue;
        }
        keys.push((host.to_string(), tag.to_string()));
        if keys.len() == MAX_RESOURCE_CLEAR_SCAN {
            break;
        }
    }
    Ok(keys)
}

pub(crate) fn clear_resource_anti_affinity_table(
    table: &mut ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    graph: &str,
) -> Result<(), String> {
    clear_resource_three_part_table(table, graph, collect_resource_anti_affinity_clear_keys)
}

pub(crate) struct ResourceRowTables<'txn> {
    pub(crate) reservations: ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    pub(crate) tenant_index:
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), &'static str>,
    pub(crate) attempts: ScopedOwnerTableMut<'txn, (&'static str, &'static str, u64), &'static str>,
    pub(crate) hosts: ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    pub(crate) exclusivity: ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static str>,
    pub(crate) fairness: ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    pub(crate) concurrency: ScopedOwnerTableMut<'txn, (&'static str, &'static str), u64>,
    pub(crate) anti_affinity:
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), u64>,
    pub(crate) disk_policies:
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
}

pub(crate) struct ResourceRowTablesRef<'a, 'txn> {
    reservations: &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    tenant_index:
        &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), &'static str>,
    attempts: &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str, u64), &'static str>,
    hosts: &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    exclusivity: &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static str>,
    fairness: &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    concurrency: &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), u64>,
    anti_affinity:
        &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), u64>,
    disk_policies: &'a mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
}

impl<'txn> ResourceRowTables<'txn> {
    pub(crate) fn open<'borrow>(
        write: &'borrow ShardWrite<'txn>,
        graph: &str,
    ) -> Result<ResourceRowTables<'borrow>, String> {
        Ok(ResourceRowTables {
            reservations: write
                .graph(graph)?
                .open_scoped_table(RESOURCE_RESERVATIONS)?,
            tenant_index: write
                .graph(graph)?
                .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)?,
            attempts: write
                .graph(graph)?
                .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)?,
            hosts: write.graph(graph)?.open_scoped_table(RESOURCE_HOSTS)?,
            exclusivity: write
                .graph(graph)?
                .open_scoped_table(RESOURCE_EXCLUSIVITY)?,
            fairness: write.graph(graph)?.open_scoped_table(RESOURCE_FAIRNESS)?,
            concurrency: write
                .graph(graph)?
                .open_scoped_table(RESOURCE_CONCURRENCY)?,
            anti_affinity: write
                .graph(graph)?
                .open_scoped_table(RESOURCE_ANTI_AFFINITY)?,
            disk_policies: write
                .graph(graph)?
                .open_scoped_table(RESOURCE_DISK_POLICIES)?,
        })
    }

    fn as_ref<'a>(&'a mut self) -> ResourceRowTablesRef<'a, 'txn> {
        ResourceRowTablesRef {
            reservations: &mut self.reservations,
            tenant_index: &mut self.tenant_index,
            attempts: &mut self.attempts,
            hosts: &mut self.hosts,
            exclusivity: &mut self.exclusivity,
            fairness: &mut self.fairness,
            concurrency: &mut self.concurrency,
            anti_affinity: &mut self.anti_affinity,
            disk_policies: &mut self.disk_policies,
        }
    }
}

pub(crate) fn clear_resource_reservation_side_tables(
    graph: &str,
    tables: &mut ResourceRowTablesRef<'_, '_>,
) -> Result<(), String> {
    clear_resource_two_part_table(&mut tables.reservations, graph)?;
    clear_resource_tenant_index_table(&mut tables.tenant_index, graph)?;
    clear_resource_attempts_table(&mut tables.attempts, graph)?;
    Ok(())
}

pub(crate) fn clear_resource_host_side_tables(
    graph: &str,
    tables: &mut ResourceRowTablesRef<'_, '_>,
) -> Result<(), String> {
    clear_resource_two_part_table(&mut tables.hosts, graph)?;
    clear_resource_two_part_table(&mut tables.exclusivity, graph)?;
    clear_resource_two_part_table(&mut tables.fairness, graph)?;
    clear_resource_two_part_table(&mut tables.concurrency, graph)?;
    clear_resource_anti_affinity_table(&mut tables.anti_affinity, graph)?;
    clear_resource_two_part_table(&mut tables.disk_policies, graph)?;
    Ok(())
}

/// Clear all native reservation indexes together with a graph image.  These
/// rows are not a cache: retaining one across DeleteGraph/recreate would leak
/// held capacity into the new incarnation.  The caller invokes this inside the
/// same WriteTransaction as the graph clear/purge, so no half-cleared resource
/// authority is observable.
///
/// The clear/delete operation is itself the governed administrative
/// continuation for terminal history: every range pass below handles at
/// most MAX_RESOURCE_CLEAR_SCAN keys, then resumes from the last key while
/// the same write transaction remains open.  This keeps allocation bounded
/// without imposing a lifetime bound on retained tombstones, so a graph
/// cannot become uncleareable merely because its terminal history is large.
/// The active-row validation remains a complete streaming pass and happens
/// before any removal; no active hold is silently deleted.
fn clear_resource_rows_impl(
    graph: &str,
    tables: &mut ResourceRowTablesRef<'_, '_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let has_active_rows = check_resource_reservations_active(
        graph,
        &mut tables.reservations,
        &mut tables.tenant_index,
        crypto,
    )?;
    check_resource_tenant_index_consistency(
        graph,
        &mut tables.tenant_index,
        &mut tables.reservations,
        crypto,
    )?;
    if has_active_rows {
        return Err("resource graph clear requires native reservation rows to be drained".into());
    }
    clear_resource_reservation_side_tables(graph, tables)?;
    clear_resource_host_side_tables(graph, tables)?;
    Ok(())
}

pub(crate) fn clear_resource_rows_with_tables(
    graph: &str,
    tables: &mut ResourceRowTables<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut tables_ref = tables.as_ref();
    clear_resource_rows_impl(graph, &mut tables_ref, crypto)
}

#[cfg(not(test))]
pub(crate) fn clear_resource_rows(
    graph: &str,
    tables: &mut ResourceRowTables<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    clear_resource_rows_with_tables(graph, tables, crypto)
}

#[cfg(test)]
pub(crate) fn clear_resource_rows<'txn>(
    graph: &str,
    reservations: &mut ScopedOwnerTableMut<'txn, (&str, &str), &[u8]>,
    tenant_index: &mut ScopedOwnerTableMut<'txn, (&str, &str, &str), &str>,
    attempts: &mut ScopedOwnerTableMut<'txn, (&str, &str, u64), &str>,
    hosts: &mut ScopedOwnerTableMut<'txn, (&str, &str), &[u8]>,
    exclusivity: &mut ScopedOwnerTableMut<'txn, (&str, &str), &str>,
    fairness: &mut ScopedOwnerTableMut<'txn, (&str, &str), &[u8]>,
    concurrency: &mut ScopedOwnerTableMut<'txn, (&str, &str), u64>,
    anti_affinity: &mut ScopedOwnerTableMut<'txn, (&str, &str, &str), u64>,
    disk_policies: &mut ScopedOwnerTableMut<'txn, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut tables = ResourceRowTablesRef {
        reservations,
        tenant_index,
        attempts,
        hosts,
        exclusivity,
        fairness,
        concurrency,
        anti_affinity,
        disk_policies,
    };
    clear_resource_rows_impl(graph, &mut tables, crypto)
}

/// Open the complete resource table family for a graph-member clear. The
/// compact and cross-modal paths already hold these tables and call
/// [`clear_resource_rows`] directly; the ordinary graph-method path only has
/// its core row bundle open, so this adapter opens the resource rows once and
/// releases them before the member is finished.
pub(crate) fn clear_resource_rows_in_wtx(
    write: &ShardWrite<'_>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut tables = ResourceRowTables::open(write, graph)?;
    let mut tables_ref = tables.as_ref();
    clear_resource_rows_impl(graph, &mut tables_ref, crypto)
}

/// Remove every current ChangeEnvelope projection for a graph inside the caller's
/// open transaction. The immutable MutationBatch/outbox audit ledger is retained;
/// current object/material/governance state cannot leak into a same-name graph.
pub(crate) fn clear_change_material_rows(
    write: &ShardWrite<'_>,
    graph: &str,
) -> Result<(), String> {
    let mut envelopes = write
        .graph(graph)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    let envelope_keys: Vec<String> = envelopes
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            Ok(key.value().1.to_string())
        })
        .collect::<Result<_, String>>()?;
    for id in envelope_keys {
        envelopes
            .remove((graph, id.as_str()))
            .map_err(|e| e.to_string())?;
    }

    macro_rules! purge_graph_three_part_table {
        ($definition:expr) => {{
            let mut table = write
                .graph(graph)?
                .open_scoped_table($definition)
                .map_err(|e| e.to_string())?;
            let keys: Vec<(String, String)> = table
                .scope_rows()
                .map_err(|e| e.to_string())?
                .map(|row| {
                    let (key, _) = row.map_err(|e| e.to_string())?;
                    let (_, tenant, id) = key.value();
                    Ok((tenant.to_string(), id.to_string()))
                })
                .collect::<Result<_, String>>()?;
            for (tenant, id) in keys {
                table
                    .remove((graph, tenant.as_str(), id.as_str()))
                    .map_err(|e| e.to_string())?;
            }
        }};
    }
    purge_graph_three_part_table!(CONTENT_VERSIONS);
    purge_graph_three_part_table!(CHANGE_BLOBS);
    purge_graph_three_part_table!(CHANGE_FEATURES);
    purge_graph_three_part_table!(CHANGE_EVIDENCE);
    purge_graph_three_part_table!(CHANGE_POLICIES);
    purge_graph_three_part_table!(CHANGE_LINEAGE);

    let mut cursors = write
        .graph(graph)?
        .open_scoped_table(CHANGE_CURSORS)
        .map_err(|e| e.to_string())?;
    let cursor_keys: Vec<(String, String, String)> = cursors
        .scope_rows()
        .map_err(|e| e.to_string())?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (_, tenant, source, partition) = key.value();
            Ok((
                tenant.to_string(),
                source.to_string(),
                partition.to_string(),
            ))
        })
        .collect::<Result<_, String>>()?;
    for (tenant, source, partition) in cursor_keys {
        cursors
            .remove((graph, tenant.as_str(), source.as_str(), partition.as_str()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Admitted-member variant used by online reshard imports.  The member's bound
/// graph identity supplies the scope; the graph argument is checked before any
/// table is opened, then every removal goes through the scoped owner handles.
pub(crate) fn clear_change_material_rows_in_wtx(
    write: &impl OwnerPayloadWrite,
    graph: &str,
) -> Result<(), String> {
    if write.scope().graph_name().map(|name| name.as_str()) != Some(graph) {
        return Err("change material clear graph does not match admitted scope".to_string());
    }
    let mut envelopes = write.open_scoped_table(CHANGE_ENVELOPES)?;
    let envelope_keys: Vec<String> = envelopes
        .scope_rows()?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            Ok(key.value().1.to_string())
        })
        .collect::<Result<_, String>>()?;
    for id in envelope_keys {
        envelopes.remove((graph, id.as_str()))?;
    }

    macro_rules! purge_graph_three_part_table {
        ($definition:expr) => {{
            let mut table = write.open_scoped_table($definition)?;
            let keys: Vec<(String, String)> = table
                .scope_rows()?
                .map(|row| {
                    let (key, _) = row.map_err(|e| e.to_string())?;
                    let (_, tenant, id) = key.value();
                    Ok((tenant.to_string(), id.to_string()))
                })
                .collect::<Result<_, String>>()?;
            for (tenant, id) in keys {
                table.remove((graph, tenant.as_str(), id.as_str()))?;
            }
        }};
    }
    purge_graph_three_part_table!(CONTENT_VERSIONS);
    purge_graph_three_part_table!(CHANGE_BLOBS);
    purge_graph_three_part_table!(CHANGE_FEATURES);
    purge_graph_three_part_table!(CHANGE_EVIDENCE);
    purge_graph_three_part_table!(CHANGE_POLICIES);
    purge_graph_three_part_table!(CHANGE_LINEAGE);

    let mut cursors = write.open_scoped_table(CHANGE_CURSORS)?;
    let cursor_keys: Vec<(String, String, String)> = cursors
        .scope_rows()?
        .map(|row| {
            let (key, _) = row.map_err(|e| e.to_string())?;
            let (_, tenant, source, partition) = key.value();
            Ok((
                tenant.to_string(),
                source.to_string(),
                partition.to_string(),
            ))
        })
        .collect::<Result<_, String>>()?;
    for (tenant, source, partition) in cursor_keys {
        cursors.remove((graph, tenant.as_str(), source.as_str(), partition.as_str()))?;
    }
    Ok(())
}

/// Retire one graph entirely: its authority and every row it owns, in ONE
/// transaction (CONCEPT:EG-KG.backend.tenant-delete-recreate-same, the tenant-DELETE path).
///
/// Unlike `clear_graph_rows`, which empties a LIVE graph's data and keeps its
/// identity, this ends the graph's durable existence: the scope's binding is
/// retired, so the generation can never be authenticated again, and its owner
/// rows go with it. A recreate of the same name binds a NEW incarnation and
/// starts clean.
///
/// This is the whole of what the retired `clear_mutation_authority_rows` used to
/// hand-sweep. Replay keys, receipts, outbox rows, delivery leases, projection
/// cursors, the version and the fence are ledger rows now, and the kernel
/// removes them with the binding; the payload half is
/// [`GraphShardRetirement`], which sweeps the 41 scope-prefixed tables through
/// the capability's own scope. The retired binding is also what replaces
/// `mutation_lifecycle_head`: a request carrying the old incarnation's scope is
/// refused at the kernel before any replay or admission question arises
/// (RF-RULING-004 application note 3).
///
/// The catalog row is the exception and is removed separately: `graph_meta` is
/// FILE-WIDE, so it belongs to the control scope, not to the graph being
/// retired.
pub(crate) fn purge_graph_rows(shard: &Shard, graph: &str) -> Result<(), String> {
    reject_reserved_graph(graph)?;
    let handle = shard.graph(graph)?;
    let identity = handle.identity().clone();
    shard
        .mutations()
        .purge_scope_with(&handle, &identity, &GraphShardRetirement)?;
    shard.forget_graph(graph, &identity)?;
    remove_graph_catalog_row(shard, graph)
}

/// Drop one graph's catalog entry on the control scope.
pub(crate) fn remove_graph_catalog_row(shard: &Shard, graph: &str) -> Result<(), String> {
    let op_id = format!("graph_purge/{graph}");
    let (group, batches) = shard.admit_maintenance(&[], &op_id)?;
    let write = ShardWrite::open(shard, &group, &[], &batches)?;
    let removed = write
        .control()
        .open_table(GRAPH_META)?
        .remove(graph)
        .map(|_| ())
        .map_err(|error| error.to_string());
    let finished = write.finish();
    match (removed, finished) {
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, 0),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}
