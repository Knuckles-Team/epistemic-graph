//! Exact numbers on the decision wire.
//!
//! Nothing in a decision record is a float. A probability, a coverage level or
//! a risk budget travels as an exact rational, and a score travels as an
//! integer on a named fixed-point scale, so two engines that agree on the
//! inputs agree on the record digest bit for bit.
//!
//! [`QuantScaleTag`] and [`UnitRationalWire`] mirror `eg_numeric`'s `QuantScale`
//! and `UnitRational`. They are mirrored rather than re-exported because
//! `eg-types` sits at the bottom of the crate DAG and must not grow an edge to
//! a compute crate for two value shapes.

use serde::{Deserialize, Serialize};

/// Largest denominator an exact wire rational may carry, matching
/// `eg_numeric`'s `MAX_DENOMINATOR`.
pub const MAX_UNIT_RATIONAL_DENOMINATOR: u64 = 1_000_000_000_000;

/// Serde shape of [`UnitRationalWire`].
///
/// The validated type keeps its fields private, so this is the only place the
/// pair is representable unchecked -- and it exists only for the moment
/// between decoding and validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UnitRationalFields {
    pub numerator: u64,
    pub denominator: u64,
}

/// An exact rational in `[0, 1]`: `numerator / denominator`.
///
/// Validated on the way in rather than at each use site, because it appears in
/// six different records and a per-site check is a check that one of them
/// eventually forgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "UnitRationalFields", into = "UnitRationalFields")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "contract-schema", schemars(with = "UnitRationalFields"))]
pub struct UnitRationalWire {
    numerator: u64,
    denominator: u64,
}

impl UnitRationalWire {
    /// The only constructor. Refuses a zero denominator, a value above one, and
    /// a denominator past [`MAX_UNIT_RATIONAL_DENOMINATOR`].
    pub fn new(numerator: u64, denominator: u64) -> Result<Self, String> {
        if denominator == 0 {
            return Err("unit rational denominator must be non-zero".into());
        }
        if denominator > MAX_UNIT_RATIONAL_DENOMINATOR {
            return Err(format!(
                "unit rational denominator exceeds {MAX_UNIT_RATIONAL_DENOMINATOR}"
            ));
        }
        if numerator > denominator {
            return Err("unit rational numerator exceeds its denominator".into());
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }

    pub fn numerator(self) -> u64 {
        self.numerator
    }

    pub fn denominator(self) -> u64 {
        self.denominator
    }
}

impl TryFrom<UnitRationalFields> for UnitRationalWire {
    type Error = String;

    fn try_from(fields: UnitRationalFields) -> Result<Self, Self::Error> {
        Self::new(fields.numerator, fields.denominator)
    }
}

impl From<UnitRationalWire> for UnitRationalFields {
    fn from(value: UnitRationalWire) -> Self {
        Self {
            numerator: value.numerator,
            denominator: value.denominator,
        }
    }
}

/// The fixed-point scale a [`QuantisedValue`] is read on. Mirrors
/// `eg_numeric`'s `QuantScale`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum QuantScaleTag {
    /// `value / 10^12`.
    Pico,
    /// `value / 2^32`.
    Q32,
}

/// An exact fixed-point number: `value` read on `scale`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct QuantisedValue {
    pub scale: QuantScaleTag,
    pub value: i64,
}
