//! The schema bound every declared result body carries.

/// Under `contract-schema` every body publishes a JSON Schema; otherwise no bound.
#[cfg(feature = "contract-schema")]
pub trait ResultSchema: schemars::JsonSchema {}
#[cfg(feature = "contract-schema")]
impl<T: schemars::JsonSchema + ?Sized> ResultSchema for T {}
/// Under `contract-schema` every body publishes a JSON Schema; otherwise no bound.
#[cfg(not(feature = "contract-schema"))]
pub trait ResultSchema {}
#[cfg(not(feature = "contract-schema"))]
impl<T: ?Sized> ResultSchema for T {}
