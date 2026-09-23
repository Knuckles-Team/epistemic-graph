//! UQL front-end tests. `contract` holds the executable exhaustiveness/single-source
//! obligations (EH-370/EH-374); `roundtrip` the printer ⇄ parser property.

mod contract;
mod diagnostics;
mod legacy;
mod lexing;
mod params;
mod predicates;
mod program;
mod roundtrip;
