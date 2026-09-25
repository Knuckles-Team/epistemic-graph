//! Per-cell headroom for a planner (SWARM-TOPOLOGY-DECIDE-DESIGN §5 stage 1b,
//! ST-8): what each named cell still admits for one priority, from one MVCC
//! snapshot, priced by exactly the rule `AcquireCapacity` uses.

use eg_types::capacity_lease::{CapacityCell, LeasePriority};
use eg_types::decision::CapacityHeadroom;

use super::super::shard::Shard;
use super::super::{decode_durable, DurableCrypto};
use super::{validate_cell_bounds, DurableUsage, CELLS, USAGE};

/// The headroom one cell offers `priority` with `leased` already out: the
/// acquire path's own [`CapacityCell::available_for`], so a
/// `BackgroundIngestion` planner never counts the reserved floor.
pub(super) fn cell_headroom(
    cell: &CapacityCell,
    leased: u64,
    priority: LeasePriority,
) -> CapacityHeadroom {
    CapacityHeadroom {
        cell_id: cell.cell_id.clone(),
        class: cell.resource_class,
        available: cell.available_for(priority, leased),
        epoch: cell.epoch,
    }
}

/// What each named cell still admits for `priority`, from ONE MVCC snapshot,
/// under exactly the rule `AcquireCapacity` prices a demand with
/// ([`CapacityCell::available_for`] over the cell's durable usage row): a
/// planner reading this can never plan into a reserved floor its priority may
/// not spend (SWARM-TOPOLOGY-DECIDE-DESIGN §5 stage 1b, ST-8).
pub(crate) fn headroom(
    shard: &Shard,
    graph: &str,
    cells: &[String],
    priority: LeasePriority,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<CapacityHeadroom>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let cell_rows = read.scoped_owner_table(CELLS)?;
    let usage_rows = read.scoped_owner_table(USAGE)?;
    let mut out = Vec::with_capacity(cells.len());
    for cell_id in cells {
        let cell: CapacityCell = cell_rows
            .get((graph, cell_id.as_str()))?
            .ok_or_else(|| format!("capacity cell '{cell_id}' was not found"))
            .and_then(|value| decode_durable(&crypto.unseal(value.value())?))?;
        validate_cell_bounds(&cell)?;
        let leased = usage_rows
            .get((graph, cell_id.as_str()))?
            .map(|value| decode_durable::<DurableUsage>(&crypto.unseal(value.value())?))
            .transpose()?
            .map_or(0, |row| row.leased_amount);
        out.push(cell_headroom(&cell, leased, priority));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use eg_types::capacity_lease::{CapacityResourceClass, LeasePriority};

    use super::*;

    fn cell() -> CapacityCell {
        CapacityCell {
            cell_id: "cell-llm".to_string(),
            parent_id: None,
            resource_class: CapacityResourceClass::LlmGenerator,
            capacity: 10,
            reserved_floor: 4,
            epoch: 7,
            policy_digest: "a".repeat(64),
            updated_at_ms: 0,
            throttle: None,
        }
    }

    #[test]
    fn a_background_planner_never_plans_into_the_reserved_floor() {
        let orchestration = cell_headroom(&cell(), 3, LeasePriority::Orchestration);
        assert_eq!(orchestration.available, 7);
        let background = cell_headroom(&cell(), 3, LeasePriority::BackgroundIngestion);
        assert_eq!(background.available, 3, "10 - 4 floor - 3 leased");
        assert_eq!(background.epoch, 7);
        let exhausted = cell_headroom(&cell(), 8, LeasePriority::BackgroundIngestion);
        assert_eq!(exhausted.available, 0);
    }
}
