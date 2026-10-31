//! Re-export of the wire scalars this module's algorithms use. The
//! definitions live in `medulla_types::solve::scalar` -- see that module's own
//! doc for why there is exactly one copy.

pub use medulla_types::solve::scalar::{Scalar, ScalarParseError, Sha256Digest};
