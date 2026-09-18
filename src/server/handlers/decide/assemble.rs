//! `Method::AgentAssemble`: prove an agent graph out of one tenant's library.
//!
//! Owned after S1 by the assembly package, which replaces the stub body with
//! the candidate read, the step-1b eliminations, the model build, the template
//! enumeration and the no-good loop. Nothing here commits.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Read the candidate scope, solve, and answer a record plus -- only when
    /// the outcome is `Solved` -- the graph draft it proves.
    handle_agent_assemble(eg_types::decision::AssemblyRequest)
        refuses "AgentAssemble", tested by assemble_stub_tests
}
