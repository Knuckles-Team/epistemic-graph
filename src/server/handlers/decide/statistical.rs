//! `Method::Decide`: the statistical executor.
//!
//! Owned after S1 by the statistical package, which replaces the stub body
//! with the candidate read, the feature matrix, the head evaluation and the
//! calibrated outcome. It stays evaluate-only in 2.27.x.

use crate::server::contract_wave::contract_wave_stub;

contract_wave_stub! {
    /// Answer a bounded batch of statistical decision records; commit none.
    handle_decide(eg_types::decision::DecideRequest)
        refuses "Decide", tested by statistical_stub_tests
}
