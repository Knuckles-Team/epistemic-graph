//! `Method::Solve`: the general bounded 0-1 integer programme.
//!
//! Owned after S1 by the solver package, which replaces the stub body with
//! model validation, the deterministic search and a re-verification of the
//! certificate before it is returned. Pure compute: it reads no store, so it
//! takes neither the server state nor the verified context.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Solve one model and answer its verified certificate.
    pure handle_solve(eg_types::solve::SolveRequest)
        refuses "Solve", tested by solve_stub_tests
}
