//! Sweep engine-owned component bodies no revision holds any more.
//!
//! Owned after S1 by the pack-bodies package.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Run the orphan-body reconciler now, within its bounded scan budget.
    serve(eg_types::connector_pack::ConnectorPackReconcileRequest)
        refuses "ConnectorPack.reconcile_bodies", tested by reconcile_stub_tests
}
