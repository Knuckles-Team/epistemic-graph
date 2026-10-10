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

use std::collections::HashMap;

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
        // EG-DURABLE-KERNEL-R024.4: a single, directly-addressable mirror
        // target. Same unbound refusal shape as `FanOut` until a driver
        // registers under this name.
        MirrorTargetSpec::Named { .. } => Ok(Box::new(MirrorUnbound {
            kind: "named",
            target_count: 1,
        })),
        // EG-DURABLE-KERNEL-R024.5: an outbox-based mirror. Replaying the
        // local mutation outbox under `consumer` is the delivery mechanism
        // the outbox contract (R001/R002/R052) already guarantees committed,
        // per-consumer, idempotently-replayable rows for; no separate bound
        // driver exists yet to actually drain that consumer's cursor into a
        // downstream sink, so this is the same unbound-refusal shape as
        // `FanOut`/`Named` until one registers.
        MirrorTargetSpec::Outbox { .. } => Ok(Box::new(MirrorUnbound {
            kind: "outbox",
            target_count: 1,
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

/// The registration entry point (`EG-DURABLE-KERNEL-R024.2.2`): where a
/// mirror-sink driver registers a named target spec, and where the outbox
/// fan-out path resolves a name to a bound (or, pre-driver, refused) send.
/// Mirrors `federation::ForeignSourceRegistry::register_spec`/`resolve`.
#[derive(Default)]
pub struct MirrorTargetRegistry {
    specs: HashMap<String, MirrorTargetSpec>,
}

impl MirrorTargetRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a spec under `name`. Refuses an invalid spec (for example an
    /// empty `FanOut`) at registration time, before any send is attempted.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        spec: MirrorTargetSpec,
    ) -> Result<(), String> {
        spec.validate().map_err(|error| error.to_string())?;
        self.specs.insert(name.into(), spec);
        Ok(())
    }

    /// How many targets are registered.
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    /// Whether NO targets are registered.
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// Push `payload` to the target registered under `name`. A name with no
    /// registration is a clean typed error (never a silently dropped row);
    /// a registered-but-still-unbound target (no driver exists for its kind
    /// yet) fails the same way through `target_for`'s `MirrorUnbound`.
    pub fn send_to(&self, name: &str, payload: &[u8]) -> Result<(), String> {
        match self.specs.get(name) {
            Some(spec) => target_for(spec)?.send(payload),
            None => {
                let mut known: Vec<&str> = self.specs.keys().map(String::as_str).collect();
                known.sort_unstable();
                Err(format!(
                    "federation: no mirror target registered under name '{name}' (registered: {known:?})"
                ))
            }
        }
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

    // spec: EG-DURABLE-KERNEL-R024.2.1
    #[test]
    fn fan_out_spec_round_trips_and_validates() {
        let spec = fan_out(&["lake", "audit-sink"]);
        spec.validate().unwrap();
        let wire = rmp_serde::to_vec_named(&spec).unwrap();
        let round_trip: MirrorTargetSpec = rmp_serde::from_slice(&wire).unwrap();
        assert_eq!(round_trip, spec);
    }

    // spec: EG-DURABLE-KERNEL-R024.2.1
    #[test]
    fn empty_fan_out_is_refused() {
        let spec = fan_out(&[]);
        assert!(spec.validate().is_err());
        let Err(error) = target_for(&spec) else {
            panic!("the fan-out spec must be refused");
        };
        assert!(error.contains("no downstream target"), "{error}");
    }

    // spec: EG-DURABLE-KERNEL-R024.2.1
    #[test]
    fn duplicate_fan_out_target_is_refused() {
        let spec = fan_out(&["lake", "lake"]);
        let Err(error) = target_for(&spec) else {
            panic!("the fan-out spec must be refused");
        };
        assert!(error.contains("more than once"), "{error}");
    }

    // spec: EG-DURABLE-KERNEL-R024.2.1
    #[test]
    fn unbound_fan_out_fails_closed_without_reaching_any_target() {
        let spec = fan_out(&["lake", "audit-sink"]);
        let target = target_for(&spec).unwrap();
        let error = target.send(b"row").unwrap_err();
        assert!(error.contains("fan-out"), "{error}");
        assert!(error.contains("2 downstream"), "{error}");
        assert!(error.contains("verified registration and bound driver"));
    }

    // spec: EG-DURABLE-KERNEL-R024.2.2
    #[test]
    fn registry_refuses_an_invalid_spec_at_registration_time() {
        let mut registry = MirrorTargetRegistry::new();
        let error = registry.register("broken", fan_out(&[])).unwrap_err();
        assert!(error.contains("no downstream target"), "{error}");
        assert!(registry.is_empty());
    }

    // spec: EG-DURABLE-KERNEL-R024.2.2
    #[test]
    fn registry_send_to_reaches_the_real_validate_and_dispatch_path() {
        let mut registry = MirrorTargetRegistry::new();
        registry
            .register("lake-mirror", fan_out(&["lake", "audit-sink"]))
            .unwrap();
        assert_eq!(registry.len(), 1);
        // Registered, but no driver exists for "fan-out" yet: the same
        // closed-failure path `target_for` proves directly, now reached
        // through the registry's name-resolution entry point.
        let error = registry.send_to("lake-mirror", b"row").unwrap_err();
        assert!(
            error.contains("verified registration and bound driver"),
            "{error}"
        );
    }

    // spec: EG-DURABLE-KERNEL-R024.2.2
    #[test]
    fn registry_send_to_an_unknown_name_is_a_distinct_clean_error() {
        let registry = MirrorTargetRegistry::new();
        let error = registry.send_to("missing", b"row").unwrap_err();
        assert!(error.contains("no mirror target registered"), "{error}");
    }

    // EG-DURABLE-KERNEL-R024.4: the single, directly-addressable mirror
    // target. Same shape as the `FanOut` coverage above.

    fn named(name: &str) -> MirrorTargetSpec {
        MirrorTargetSpec::Named {
            name: name.to_string(),
        }
    }

    #[test]
    fn named_spec_round_trips_and_validates() {
        let spec = named("warehouse");
        spec.validate().unwrap();
        let wire = rmp_serde::to_vec_named(&spec).unwrap();
        let round_trip: MirrorTargetSpec = rmp_serde::from_slice(&wire).unwrap();
        assert_eq!(round_trip, spec);
    }

    #[test]
    fn empty_named_target_is_refused() {
        let spec = named("");
        assert!(spec.validate().is_err());
        let Err(error) = target_for(&spec) else {
            panic!("the named spec must be refused");
        };
        assert!(error.contains("no downstream target"), "{error}");
    }

    #[test]
    fn unbound_named_target_fails_closed_without_reaching_any_target() {
        let spec = named("warehouse");
        let target = target_for(&spec).unwrap();
        let error = target.send(b"row").unwrap_err();
        assert!(error.contains("named"), "{error}");
        assert!(error.contains("verified registration and bound driver"));
    }

    // EG-DURABLE-KERNEL-R024.5: the outbox-based mirror target. Same shape
    // as the `FanOut`/`Named` coverage above.

    fn outbox(consumer: &str) -> MirrorTargetSpec {
        MirrorTargetSpec::Outbox {
            consumer: consumer.to_string(),
        }
    }

    // spec: EG-DURABLE-KERNEL-R024.5
    #[test]
    fn outbox_spec_round_trips_and_validates() {
        let spec = outbox("warehouse-mirror");
        spec.validate().unwrap();
        let wire = rmp_serde::to_vec_named(&spec).unwrap();
        let round_trip: MirrorTargetSpec = rmp_serde::from_slice(&wire).unwrap();
        assert_eq!(round_trip, spec);
    }

    // spec: EG-DURABLE-KERNEL-R024.5
    #[test]
    fn empty_outbox_consumer_is_refused() {
        let spec = outbox("");
        assert!(spec.validate().is_err());
        let Err(error) = target_for(&spec) else {
            panic!("the outbox spec must be refused");
        };
        assert!(error.contains("empty or padded"), "{error}");
    }

    // spec: EG-DURABLE-KERNEL-R024.5
    #[test]
    fn padded_outbox_consumer_is_refused() {
        let spec = outbox(" warehouse-mirror ");
        let Err(error) = target_for(&spec) else {
            panic!("the padded-consumer outbox spec must be refused");
        };
        assert!(error.contains("empty or padded"), "{error}");
    }

    // spec: EG-DURABLE-KERNEL-R024.5
    #[test]
    fn unbound_outbox_target_fails_closed_without_reaching_any_target() {
        let spec = outbox("warehouse-mirror");
        let target = target_for(&spec).unwrap();
        let error = target.send(b"row").unwrap_err();
        assert!(error.contains("outbox"), "{error}");
        assert!(error.contains("verified registration and bound driver"));
    }
}
