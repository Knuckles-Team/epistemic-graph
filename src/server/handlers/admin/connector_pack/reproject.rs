//! Re-drive the graph projection of a connector's current pack head.
//!
//! Owned after S1 by the pack-projection package.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Re-project the current head into the graph.
    serve(eg_types::connector_pack::ConnectorPackReprojectRequest)
        refuses "ConnectorPack.reproject", tested by reproject_stub_tests
}
