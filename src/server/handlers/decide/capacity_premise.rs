//! Stage 1b capacity premise for swarm topology (SWARM-TOPOLOGY-DECIDE-DESIGN
//! §5, ST-8).
//!
//! Each requested cell's headroom is read ONCE per decision, at the request's
//! `LeasePriority`, through the capacity ledger's own pricing rule
//! (`CapacityCell::available_for` over the durable usage row) -- so a
//! `BackgroundIngestion` plan never counts the reserved floor. The numbers
//! become OBSERVATION premises in the record's inputs; the plan they bound
//! still grants nothing (invariant T1): only `AcquireCapacity` does.
//!
//! A headroom the plan exceeds is named `lease:<cell>:<class>` by the model.

use eg_types::decision::{CapacityHeadroom, CapacityScope};

use super::SharedState;

/// The headroom of every cell `scope` names, in request order.
pub(super) async fn headroom(
    state: &SharedState,
    graph: &str,
    scope: &CapacityScope,
) -> Result<Vec<CapacityHeadroom>, String> {
    let persistence = state.read().await.persistence.clone().ok_or_else(|| {
        "CAPACITY_UNAVAILABLE: a topology decision needs the durable capacity ledger".to_string()
    })?;
    let cells: Vec<String> = scope.cells.iter().cloned().collect();
    persistence
        .read_capacity_headroom(&crate::persist::sanitize(graph), &cells, scope.priority)
        .await
        .map_err(|error| format!("CAPACITY_UNAVAILABLE: {error}"))
}
