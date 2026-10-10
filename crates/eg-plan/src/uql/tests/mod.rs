//! UQL front-end tests. `contract` holds the executable exhaustiveness/single-source
//! obligations (UQL-06/UQL-10); `roundtrip` the printer ⇄ parser property.

#[cfg(feature = "query")]
fn blob(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

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
#[cfg(feature = "query")]
mod propagate;
mod roundtrip;
mod scrubber;
#[cfg(all(feature = "query", feature = "owl"))]
mod shape;
#[cfg(feature = "query")]
mod serve;
