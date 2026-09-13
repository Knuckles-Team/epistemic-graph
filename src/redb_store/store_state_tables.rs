use super::store_prelude::*;
use super::*;

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub(crate) fn open_native_operation_graph_tables<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str, u32), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, u64), &'static str>,
        ScopedOwnerTableMut<'txn, &'static str, &'static [u8]>,
        ScopedOwnerTableMut<'txn, &'static str, u64>,
    ),
    String,
> {
    let nodes = write
        .graph(graph_fname)?
        .open_scoped_table(NODES)
        .map_err(|e| e.to_string())?;
    let native_work_items = write
        .graph(graph_fname)?
        .open_scoped_table(work_item_capability::NATIVE_WORK_ITEMS)
        .map_err(|e| e.to_string())?;
    let edges = write
        .graph(graph_fname)?
        .open_scoped_table(EDGES)
        .map_err(|e| e.to_string())?;
    let ledger = write
        .graph(graph_fname)?
        .open_scoped_table(LEDGER)
        .map_err(|e| e.to_string())?;
    let semantic = write
        .graph(graph_fname)?
        .open_scoped_table(SEMANTIC)
        .map_err(|e| e.to_string())?;
    let command_sequences = write
        .graph(graph_fname)?
        .open_scoped_table(WORK_ITEM_COMMAND_SEQUENCE)
        .map_err(|e| e.to_string())?;
    Ok((
        nodes,
        native_work_items,
        edges,
        ledger,
        semantic,
        command_sequences,
    ))
}

#[allow(clippy::type_complexity)]
pub(crate) fn open_native_operation_resource_tables_a<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), &'static str>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, u64), &'static str>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static str>,
    ),
    String,
> {
    let resource_reservations = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_RESERVATIONS)
        .map_err(|e| e.to_string())?;
    let resource_tenant_index = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
        .map_err(|e| e.to_string())?;
    let resource_attempts = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
        .map_err(|e| e.to_string())?;
    let resource_hosts = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_HOSTS)
        .map_err(|e| e.to_string())?;
    let resource_exclusivity = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_EXCLUSIVITY)
        .map_err(|e| e.to_string())?;
    Ok((
        resource_reservations,
        resource_tenant_index,
        resource_attempts,
        resource_hosts,
        resource_exclusivity,
    ))
}

#[allow(clippy::type_complexity)]
pub(crate) fn open_native_operation_resource_tables_b<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), u64>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, &'static str), u64>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let resource_fairness = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_FAIRNESS)
        .map_err(|e| e.to_string())?;
    let resource_concurrency = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_CONCURRENCY)
        .map_err(|e| e.to_string())?;
    let resource_anti_affinity = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_ANTI_AFFINITY)
        .map_err(|e| e.to_string())?;
    let resource_disk_policies = write
        .graph(graph_fname)?
        .open_scoped_table(RESOURCE_DISK_POLICIES)
        .map_err(|e| e.to_string())?;
    Ok((
        resource_fairness,
        resource_concurrency,
        resource_anti_affinity,
        resource_disk_policies,
    ))
}

#[allow(clippy::type_complexity)]
pub(crate) fn open_native_operation_lane_tables<'txn>(
    write: &'txn ShardWrite<'txn>,
    graph_fname: &str,
) -> Result<
    (
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str, u64), &'static str>,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
        ScopedOwnerTableMut<
            'txn,
            (
                &'static str,
                &'static str,
                &'static str,
                &'static str,
                u64,
                &'static str,
            ),
            u8,
        >,
        ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    ),
    String,
> {
    let lane_holds = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::HOLDS)
        .map_err(|e| e.to_string())?;
    let lane_work_item_index = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::WORK_ITEM_INDEX)
        .map_err(|e| e.to_string())?;
    let lane_counters = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::COUNTERS)
        .map_err(|e| e.to_string())?;
    let lane_pressure_index = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::PRESSURE_INDEX)
        .map_err(|e| e.to_string())?;
    let lane_policies = write
        .graph(graph_fname)?
        .open_scoped_table(development_lane::POLICIES)
        .map_err(|e| e.to_string())?;
    Ok((
        lane_holds,
        lane_work_item_index,
        lane_counters,
        lane_pressure_index,
        lane_policies,
    ))
}
