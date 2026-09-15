//! Statechart result bodies of the `coordination` domain: projections of
//! `eg-statechart`'s durable instance and step outcome, decoded strictly from their JSON.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// `Statechart` op `Define`: the content-addressed definition id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatechartDefinitionId {
    pub def_id: String,
}

/// The active states of an instance, plus composite-state history memory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatechartConfiguration {
    pub active: BTreeSet<String>,
    pub history: BTreeMap<String, BTreeSet<String>>,
}

/// Whether an instance has reached a final state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StatechartInstanceStatus {
    Active,
    Final,
}

/// A durable statechart instance. `context` is the extended state the caller's
/// definition and events wrote.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatechartInstance {
    pub instance_id: String,
    pub def_id: String,
    pub configuration: StatechartConfiguration,
    pub context: BTreeMap<String, serde_json::Value>,
    pub version: u64,
    pub status: StatechartInstanceStatus,
    pub tenant: String,
    pub actor: String,
    pub events_seen: u64,
    pub transitions_fired: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Where an `assign` action takes its value from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StatechartActionValue {
    Const { value: serde_json::Value },
    Event { key: String },
    Context { key: String },
}

/// One action a step performed or asks the interpreter to perform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "do", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StatechartAction {
    Assign {
        key: String,
        value: StatechartActionValue,
    },
    Remove {
        key: String,
    },
    Emit {
        signal: String,
    },
    Log {
        message: String,
    },
    Custom {
        name: String,
        args: serde_json::Value,
    },
}

/// `Statechart` op `SendEvent`: the instance after the event and the step it took.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatechartEventOutcome {
    pub instance: StatechartInstance,
    pub fired: bool,
    /// `NoTransitionDefined` or `AllGuardsFalse` on a no-op.
    pub no_op_reason: Option<String>,
    pub fired_label: Option<String>,
    /// Every action of the step, context mutations included.
    pub actions: Vec<StatechartAction>,
    /// The actions the interpreter still has to perform.
    pub effects: Vec<StatechartAction>,
}

/// `Statechart` op `List`: the caller's instances.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct StatechartInstanceList {
    pub instance_ids: Vec<String>,
    pub count: u64,
}
