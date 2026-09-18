//! The connector-pack operations that change what a connector MAY publish:
//! binding an importer, removing that binding, and permanent retirement.
//!
//! Owned after S1 by the pack-admin package.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Bind a connector to the importer allowed to publish its packs.
    serve_bind(eg_types::connector_pack::ConnectorPackBindRequest)
        refuses "ConnectorPack.bind", tested by bind_stub_tests
}

contract_wave_stub! {
    /// Remove a connector's importer binding.
    serve_unbind(eg_types::connector_pack::ConnectorPackUnbindRequest)
        refuses "ConnectorPack.unbind", tested by unbind_stub_tests
}

contract_wave_stub! {
    /// Permanently retire named entries. Unlike withdrawal this cannot be
    /// undone by a later import.
    serve_retire(eg_types::connector_pack::ConnectorPackRetireRequest)
        refuses "ConnectorPack.retire", tested by retire_stub_tests
}
