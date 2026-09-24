//! EH-406: one error-budget AIMD step on one durable capacity cell.
//!
//! The pure step (`eg_types::capacity_throttle::apply_error_budget`) decides;
//! this writes its result in the same admitted group the capacity ledger uses
//! for every other cell change. Only the throttle's ceiling, cursor and
//! history move: the declared capacity, reserved floor, epoch and policy are
//! the operator's (`UpdateCapacityCell`) and are written back unchanged, and a
//! narrowed ceiling never touches a lease already held.

use eg_types::capacity_lease::CapacityCell;
use eg_types::capacity_throttle::apply_error_budget;
use eg_types::native_control::{
    CapacityThrottleRequest, CapacityThrottleResult, NativeControlSchemaVersion,
};

use super::super::shard::ShardWrite;
use super::super::{decode_durable, DurableCrypto};
use super::{validate_cell_bounds, CELLS};

pub(super) fn throttle_cell(
    write: &ShardWrite<'_>,
    graph: &str,
    request: &CapacityThrottleRequest,
    crypto: DurableCrypto<'_>,
) -> Result<CapacityThrottleResult, String> {
    request.validate()?;
    let rows = write.graph(graph)?;
    let mut cells = rows.open_scoped_table(CELLS)?;
    let mut cell = cells
        .get((graph, request.cell_id.as_str()))?
        .map(|value| decode_durable::<CapacityCell>(&crypto.unseal(value.value())?))
        .transpose()?
        .ok_or_else(|| "capacity cell was not found".to_string())?;
    let action = apply_error_budget(
        cell.throttle.as_mut(),
        cell.capacity,
        &request.sample,
        request.now_ms,
    )?;
    validate_cell_bounds(&cell)?;
    let sealed_bytes = rmp_serde::to_vec_named(&cell).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&sealed_bytes);
    cells.insert((graph, request.cell_id.as_str()), sealed.as_ref())?;
    Ok(CapacityThrottleResult {
        schema_version: NativeControlSchemaVersion::V1,
        cell,
        action,
    })
}

#[cfg(test)]
mod tests;
