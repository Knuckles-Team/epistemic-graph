//! UQL front-end tests. `contract` holds the executable exhaustiveness/single-source
//! obligations (UQL-06/UQL-10); `roundtrip` the printer ⇄ parser property.

#[cfg(feature = "query")]
mod annotations;
#[cfg(feature = "query")]
mod attribution;
mod contract;
mod diagnostics;
mod legacy;
mod lexing;
mod params;
mod predicates;
mod program;
mod roundtrip;
#[cfg(feature = "query")]
mod serve;
