//! Capability descriptors and request-scoped designation
//! (EG-DECISION-ENGINE-R650.1).
//!
//! Medulla holds one versioned descriptor per capability. A descriptor
//! carries typed inputs and outputs, side effects, data types, scopes, cost,
//! latency, reliability, locality and approval class. For one request
//! Medulla projects the classified closure; the harness receives the
//! designation and holds no descriptor index. Persisting the descriptor,
//! generating the power descriptor and projecting the OWL closure are later
//! children (R650.2); the served `CapabilityDesignate` method is R650.3.

use serde::{Deserialize, Serialize};

use crate::contract::{ResourceId, ScopeKind, TenantId};

/// The wire version of the descriptor family.
pub const CAPABILITY_DESCRIPTOR_VERSION: u16 = 1;

/// The largest reliability, in basis points of 1.0 (1 = 0.01%).
pub const MAX_CAPABILITY_RELIABILITY: u32 = 10_000;

/// The side-effect class a capability declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SideEffectClass {
    ReadOnly,
    GraphWrite,
    External,
    Irreversible,
}

/// The locality a capability runs from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum Locality {
    SameHost,
    SameRegion,
    Network,
}

/// The approval class a capability requires before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ApprovalClass {
    None,
    Low,
    Medium,
    High,
}

/// One typed input or output port of a capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DataPort {
    pub name: ResourceId,
    /// The data type the port carries.
    pub data_type: ResourceId,
}

/// One versioned capability descriptor, tenant-scoped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapabilityDescriptor {
    /// The descriptor version; [`CAPABILITY_DESCRIPTOR_VERSION`] for the
    /// current wire shape.
    pub version: u16,
    pub tenant: TenantId,
    pub capability_id: ResourceId,
    /// The typed inputs.
    pub inputs: Vec<DataPort>,
    /// The typed outputs.
    pub outputs: Vec<DataPort>,
    pub side_effect_class: SideEffectClass,
    /// The scopes the capability acts under.
    pub scopes: Vec<ScopeKind>,
    /// The unit cost of one call.
    pub cost: u64,
    /// The expected latency in milliseconds.
    pub latency_ms: u64,
    /// Reliability in basis points of 1.0. The value a projection carries;
    /// it is derived from the learned reward, not stored by the capability
    /// node (R650.2).
    pub reliability: u32,
    pub locality: Locality,
    pub approval_class: ApprovalClass,
}

impl CapabilityDescriptor {
    /// Validate before the descriptor is projected or designated.
    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 {
            return Err("capability descriptor version must be above zero".to_owned());
        }
        if self.reliability > MAX_CAPABILITY_RELIABILITY {
            return Err(format!(
                "capability descriptor reliability exceeds {MAX_CAPABILITY_RELIABILITY} basis points"
            ));
        }
        for (label, ports) in [("inputs", &self.inputs), ("outputs", &self.outputs)] {
            let mut seen = std::collections::BTreeSet::new();
            for port in ports {
                if !seen.insert(&port.name) {
                    return Err(format!(
                        "capability descriptor {label} repeat port name '{}'",
                        port.name
                    ));
                }
            }
        }
        Ok(())
    }
}

/// The power descriptor generated from the registry and the capability
/// ledger (R650.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapabilityPowerDescriptor {
    /// The descriptor version the power view was generated from.
    pub version: u16,
    pub tenant: TenantId,
    pub capability_id: ResourceId,
    /// The registry digest the power view derives from.
    pub registry_digest: String,
    /// The capability-ledger digest the power view derives from.
    pub ledger_digest: String,
}

impl CapabilityPowerDescriptor {
    /// Validate before the power descriptor is served.
    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 {
            return Err("capability power descriptor version must be above zero".to_owned());
        }
        super::validate_digest("registry_digest", &self.registry_digest)?;
        super::validate_digest("ledger_digest", &self.ledger_digest)
    }
}

/// A request whose classified closure Medulla projects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DesignationRequest {
    pub tenant: TenantId,
    pub request_id: ResourceId,
    /// The capabilities the request requires.
    pub required_capabilities: Vec<ResourceId>,
    /// Milliseconds since the Unix epoch: the instant the projection reads at.
    pub as_of_ms: i64,
}

impl DesignationRequest {
    /// Validate before the closure projection starts.
    pub fn validate(&self) -> Result<(), String> {
        if self.required_capabilities.is_empty() {
            return Err(
                "a designation request needs at least one required capability".to_owned(),
            );
        }
        if self.as_of_ms <= 0 {
            return Err(format!(
                "designation request as-of instant {} must be a positive unix-millisecond instant",
                self.as_of_ms
            ));
        }
        Ok(())
    }
}

/// One capability designated for a request, bound to its descriptor version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DesignationEntry {
    pub capability_id: ResourceId,
    pub descriptor_version: u16,
}

/// The designation the harness receives; it holds no descriptor index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct Designation {
    pub tenant: TenantId,
    pub request_id: ResourceId,
    /// The designated capabilities, in closure order.
    pub entries: Vec<DesignationEntry>,
    /// The digest of the projected closure the designation covers.
    pub closure_digest: String,
}

impl Designation {
    /// Validate before the designation leaves Medulla.
    pub fn validate(&self) -> Result<(), String> {
        super::validate_digest("closure_digest", &self.closure_digest)
    }
}

/// The request-scoped classified closure Medulla projects; R650.2 computes
/// it from OWL classification in O(closure size).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClosureProjection {
    pub tenant: TenantId,
    pub request_id: ResourceId,
    /// The capability whose closure this is.
    pub capability_id: ResourceId,
    /// The closure members, in deterministic order.
    pub members: Vec<ResourceId>,
    /// The ontology digest the classification ran under.
    pub ontology_digest: String,
}

impl ClosureProjection {
    /// Validate before the projection is bound into a designation.
    pub fn validate(&self) -> Result<(), String> {
        super::validate_digest("ontology_digest", &self.ontology_digest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A valid sha256 hex digest (64 lowercase hex bytes).
    const DIGEST: &str = "abababababababababababababababababababababababababababababababab";

    fn descriptor() -> CapabilityDescriptor {
        CapabilityDescriptor {
            version: CAPABILITY_DESCRIPTOR_VERSION,
            tenant: TenantId::new("tenant-a").unwrap(),
            capability_id: ResourceId::new("eg:capability/translate").unwrap(),
            inputs: vec![DataPort {
                name: ResourceId::new("text").unwrap(),
                data_type: ResourceId::new("eg:type/text").unwrap(),
            }],
            outputs: vec![DataPort {
                name: ResourceId::new("translation").unwrap(),
                data_type: ResourceId::new("eg:type/text").unwrap(),
            }],
            side_effect_class: SideEffectClass::External,
            scopes: vec![ScopeKind::new("tenant").unwrap()],
            cost: 12,
            latency_ms: 300,
            reliability: 9_800,
            locality: Locality::Network,
            approval_class: ApprovalClass::Low,
        }
    }

    // spec: EG-DECISION-ENGINE-R650.1
    #[test]
    fn capability_descriptor_types_round_trip() {
        let descriptor = descriptor();
        let power = CapabilityPowerDescriptor {
            version: 1,
            tenant: TenantId::new("tenant-a").unwrap(),
            capability_id: ResourceId::new("eg:capability/translate").unwrap(),
            registry_digest: DIGEST.into(),
            ledger_digest: DIGEST.into(),
        };
        let request = DesignationRequest {
            tenant: TenantId::new("tenant-a").unwrap(),
            request_id: ResourceId::new("designate:1").unwrap(),
            required_capabilities: vec![ResourceId::new("eg:capability/translate").unwrap()],
            as_of_ms: 1_700_000_000_000,
        };
        let designation = Designation {
            tenant: TenantId::new("tenant-a").unwrap(),
            request_id: ResourceId::new("designate:1").unwrap(),
            entries: vec![DesignationEntry {
                capability_id: ResourceId::new("eg:capability/translate").unwrap(),
                descriptor_version: 1,
            }],
            closure_digest: DIGEST.into(),
        };
        let projection = ClosureProjection {
            tenant: TenantId::new("tenant-a").unwrap(),
            request_id: ResourceId::new("designate:1").unwrap(),
            capability_id: ResourceId::new("eg:capability/translate").unwrap(),
            members: vec![
                ResourceId::new("eg:capability/translate").unwrap(),
                ResourceId::new("eg:capability/gloss").unwrap(),
            ],
            ontology_digest: DIGEST.into(),
        };
        macro_rules! round_trip {
            ($value:expr, $ty:ty) => {{
                let json = serde_json::to_string(&$value).unwrap();
                assert_eq!(serde_json::from_str::<$ty>(&json).unwrap(), $value);
            }};
        }
        round_trip!(descriptor, CapabilityDescriptor);
        round_trip!(power, CapabilityPowerDescriptor);
        round_trip!(request, DesignationRequest);
        round_trip!(designation, Designation);
        round_trip!(projection, ClosureProjection);
        assert_eq!(
            serde_json::to_value(&descriptor.side_effect_class).unwrap(),
            "external"
        );
        assert_eq!(
            serde_json::to_value(&descriptor.approval_class).unwrap(),
            "low"
        );
        assert!(descriptor.validate().is_ok());
        assert!(power.validate().is_ok());
        assert!(request.validate().is_ok());
        assert!(designation.validate().is_ok());
        assert!(projection.validate().is_ok());
    }

    // spec: EG-DECISION-ENGINE-R650.1
    #[test]
    fn capability_descriptor_types_reject_invalid_inputs() {
        assert!(serde_json::from_str::<CapabilityDescriptor>(
            r#"{"version":1,"tenant":"tenant a","capability_id":"eg:capability/x","inputs":[],"outputs":[],"side_effect_class":"read_only","scopes":[],"cost":0,"latency_ms":1,"reliability":0,"locality":"same_host","approval_class":"none"}"#
        )
        .is_err());
        let mut descriptor = descriptor();
        descriptor.reliability = MAX_CAPABILITY_RELIABILITY + 1;
        assert!(descriptor.validate().is_err());
        descriptor.reliability = 100;
        descriptor.version = 0;
        assert!(descriptor.validate().is_err());
        descriptor.version = 1;
        descriptor.inputs.push(descriptor.inputs[0].clone());
        assert!(descriptor.validate().is_err());

        let mut power = CapabilityPowerDescriptor {
            registry_digest: "not-a-digest".into(),
            ..descriptor_power()
        };
        assert!(power.validate().is_err());

        let mut request = DesignationRequest {
            tenant: TenantId::new("tenant-a").unwrap(),
            request_id: ResourceId::new("designate:1").unwrap(),
            required_capabilities: vec![],
            as_of_ms: 1,
        };
        assert!(request.validate().is_err());
        request.required_capabilities =
            vec![ResourceId::new("eg:capability/translate").unwrap()];
        request.as_of_ms = 0;
        assert!(request.validate().is_err());

        let mut designation = Designation {
            tenant: TenantId::new("tenant-a").unwrap(),
            request_id: ResourceId::new("designate:1").unwrap(),
            entries: vec![],
            closure_digest: "ab".repeat(31),
        };
        assert!(designation.validate().is_err());
        designation.closure_digest = DIGEST.into();
        assert!(designation.validate().is_ok());

        assert!(serde_json::from_str::<DesignationRequest>(
            r#"{"tenant":"tenant-a","request_id":"designate:1","required_capabilities":[],"as_of_ms":1,"extra":2}"#
        )
        .is_err());
    }

    fn descriptor_power() -> CapabilityPowerDescriptor {
        CapabilityPowerDescriptor {
            version: 1,
            tenant: TenantId::new("tenant-a").unwrap(),
            capability_id: ResourceId::new("eg:capability/translate").unwrap(),
            registry_digest: DIGEST.into(),
            ledger_digest: DIGEST.into(),
        }
    }
}
