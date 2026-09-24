//! Which methods a caller may name, directly or inside a carrier.
//!
//! Some methods are engine-internal: the engine dispatches them to itself
//! after its own admission (EH-346's `PolicyEvolutionStore` is the kernel write
//! behind `PolicyEvolution`). A caller may never name one, neither as the
//! request method nor as an operation inside a carrier -- a `ChangeEnvelope`'s
//! `MutationBatch` is the one wire shape that nests whole `Method`s.
//!
//! A carrier additionally commits its operations through the generic graph-row
//! applier, which holds no native authority. A method that commits through a
//! dedicated native kernel (WorkItem submission/lease/resource, capacity
//! leases, control leases) is therefore refused inside a carrier as well: the
//! carrier would either bypass that kernel's rules or silently do nothing.

use super::{Method, MethodWriteFamily};

/// Why a carrier may not dispatch an inner method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CarrierRefusal {
    /// The contract classifies the method as engine-internal.
    EngineInternal,
    /// The method commits only through a dedicated native kernel.
    NativeAuthority,
}

impl CarrierRefusal {
    fn reason(self) -> &'static str {
        match self {
            Self::EngineInternal => "engine-internal",
            Self::NativeAuthority => "native-authority",
        }
    }
}

/// The typed error prefix of every carrier refusal.
pub const CARRIER_REFUSAL_CODE: &str = "CARRIER_INNER_METHOD_REFUSED";
/// The typed error prefix of a caller naming an engine-internal method.
pub const ENGINE_INTERNAL_CODE: &str = "ENGINE_INTERNAL_METHOD";

impl Method {
    /// Whether a caller may name this method at all. Engine-internal methods
    /// are dispatched only by the engine itself.
    pub fn is_wire_callable(&self) -> bool {
        !matches!(self, Self::PolicyEvolutionStore { .. })
    }

    /// Why a carrier must refuse this method as an inner operation, if it must.
    pub fn carrier_refusal(&self) -> Option<CarrierRefusal> {
        if !self.is_wire_callable() {
            return Some(CarrierRefusal::EngineInternal);
        }
        match self.write_family() {
            Some(
                MethodWriteFamily::WorkItemSubmission
                | MethodWriteFamily::WorkItemLease
                | MethodWriteFamily::WorkItemResource
                | MethodWriteFamily::CapacityLease,
            ) => Some(CarrierRefusal::NativeAuthority),
            Some(
                MethodWriteFamily::GraphElement
                | MethodWriteFamily::MemoryScene
                | MethodWriteFamily::Broker,
            )
            | None => None,
        }
    }

    /// The typed refusal a carrier answers for this inner method, if any.
    pub fn carrier_refusal_message(&self) -> Option<String> {
        let name = self.tag_name();
        self.carrier_refusal()
            .map(|refusal| format!("{CARRIER_REFUSAL_CODE}: {name} ({})", refusal.reason()))
    }

    /// The typed refusal for a caller naming an engine-internal method.
    pub fn engine_internal_message(&self) -> Option<String> {
        let name = self.tag_name();
        (!self.is_wire_callable()).then(|| format!("{ENGINE_INTERNAL_CODE}: {name}"))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn from_json(value: serde_json::Value) -> Method {
        serde_json::from_value(value).expect("fixture method decodes")
    }

    pub(crate) fn policy_store() -> Method {
        let record = serde_json::json!({
            "kind": "model_policy_version",
            "record": {"checkpoint_digest": "11".repeat(32), "tokenizer_digest": "22".repeat(32),
                       "artifact_ref": "artifacts:base", "origin": {"origin": "base"}},
        });
        from_json(
            serde_json::json!({"method": "PolicyEvolutionStore", "params": {"request": {
                "record_id": "polver:x", "tenant_id": "tenant-a", "recorded_by": "p",
                "recorded_at_ms": 1, "record": record,
            }}}),
        )
    }

    pub(crate) fn control_lease_issue() -> Method {
        from_json(
            serde_json::json!({"method": "IssueControlLease", "params": {"request": {
                "tenant": "tenant-a", "lease_id": "lease-1", "kind": "browser.control",
                "grant": {}, "issued_at_ms": 1, "expires_at_ms": 2, "hard_expires_at_ms": 3,
                "idempotency_key": "k",
            }}}),
        )
    }

    #[test]
    fn carriers_refuse_internal_and_native_kernel_methods_and_pass_graph_writes() {
        assert!(!policy_store().is_wire_callable());
        assert_eq!(
            policy_store().carrier_refusal(),
            Some(CarrierRefusal::EngineInternal)
        );
        assert!(policy_store()
            .engine_internal_message()
            .unwrap()
            .starts_with(ENGINE_INTERNAL_CODE));
        let lease = control_lease_issue();
        assert!(lease.is_wire_callable());
        assert_eq!(
            lease.carrier_refusal(),
            Some(CarrierRefusal::NativeAuthority)
        );
        assert!(lease
            .carrier_refusal_message()
            .unwrap()
            .starts_with("CARRIER_INNER_METHOD_REFUSED: IssueControlLease"));
        let node = Method::AddNode {
            node_id: "n1".into(),
            properties_msgpack: Vec::new(),
        };
        assert!(node.is_wire_callable() && node.carrier_refusal().is_none());
    }
}
