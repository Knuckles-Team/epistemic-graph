//! Write-side mirror targets (EG-DURABLE-KERNEL-R024's mirror-target half).
//!
//! A read-side federation source ([`crate::federation::ForeignSource`]) pulls rows
//! IN; a mirror target pushes a committed row OUT to a downstream destination fanned
//! out from the local mutation outbox. The seam is the same shape: one trait,
//! [`MirrorTarget`], built from one wire spec, [`eg_types::wire::MirrorTargetSpec`].
//!
//! This is the typed-model slice (`EG-DURABLE-KERNEL-R024.2.1`): the `FanOut` spec
//! (in `eg-types`) plus a registration-refusal builder here, mirroring how
//! `federation::source_for` refuses an [`eg_types::wire::ForeignSourceSpec::Trino`]
//! /`SparkBatch`/unbuilt-`Cypher` spec through `Oq2Unbound` before its dedicated
//! backend exists. The entry point that drives real writes through a bound driver
//! is a later child (`R024.2.2`).

use eg_types::wire::MirrorTargetSpec;

/// A destination a mirrored row can be pushed to.
pub trait MirrorTarget {
    /// Push one mirrored row's already-serialized payload. A delivery failure is
    /// an `Err` — a mirror target being unreachable is a real error, never a
    /// silently dropped row.
    fn send(&self, payload: &[u8]) -> Result<(), String>;
}

/// Build the right [`MirrorTarget`] for a wire [`MirrorTargetSpec`]. Every spec
/// kind is validated first: an invalid spec (for example an empty `FanOut`) is
/// refused before any binding is attempted.
pub fn target_for(spec: &MirrorTargetSpec) -> Result<Box<dyn MirrorTarget + '_>, String> {
    spec.validate().map_err(|error| error.to_string())?;
    match spec {
        MirrorTargetSpec::FanOut { targets } => Ok(Box::new(MirrorUnbound {
            kind: "fan-out",
            target_count: targets.len(),
        })),
    }
}

/// A `MirrorTargetSpec` alone has no verified registration or bound driver yet.
/// Never silently pretend a write was delivered.
struct MirrorUnbound {
    kind: &'static str,
    target_count: usize,
}

impl MirrorTarget for MirrorUnbound {
    fn send(&self, _payload: &[u8]) -> Result<(), String> {
        Err(format!(
            "federation: {} mirror target ({} downstream target(s)) requires a verified registration and bound driver",
            self.kind, self.target_count
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fan_out(targets: &[&str]) -> MirrorTargetSpec {
        MirrorTargetSpec::FanOut {
            targets: targets.iter().map(|t| t.to_string()).collect(),
        }
    }

    #[test]
    fn fan_out_spec_round_trips_and_validates() {
        let spec = fan_out(&["lake", "audit-sink"]);
        spec.validate().unwrap();
        let wire = rmp_serde::to_vec_named(&spec).unwrap();
        let round_trip: MirrorTargetSpec = rmp_serde::from_slice(&wire).unwrap();
        assert_eq!(round_trip, spec);
    }

    #[test]
    fn empty_fan_out_is_refused() {
        let spec = fan_out(&[]);
        assert!(spec.validate().is_err());
        let error = target_for(&spec).unwrap_err();
        assert!(error.contains("no downstream target"), "{error}");
    }

    #[test]
    fn duplicate_fan_out_target_is_refused() {
        let spec = fan_out(&["lake", "lake"]);
        let error = target_for(&spec).unwrap_err();
        assert!(error.contains("more than once"), "{error}");
    }

    #[test]
    fn unbound_fan_out_fails_closed_without_reaching_any_target() {
        let spec = fan_out(&["lake", "audit-sink"]);
        let target = target_for(&spec).unwrap();
        let error = target.send(b"row").unwrap_err();
        assert!(error.contains("fan-out"), "{error}");
        assert!(error.contains("2 downstream"), "{error}");
        assert!(error.contains("verified registration and bound driver"));
    }
}
