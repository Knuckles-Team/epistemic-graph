//! `Method::MutationOutbox`: the operator view of one owner's outbox.
//!
//! Owned after S1 by the outbox-admin package, which replaces the stub body
//! with the status projection, the dead-letter page and the bounded rewind
//! over the mutation kernel.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Read one consumer's standing, its dead letters, or rewind its cursor.
    handle_mutation_outbox(eg_types::mutation_outbox::MutationOutboxOp)
        refuses "MutationOutbox", tested by outbox_stub_tests
}
