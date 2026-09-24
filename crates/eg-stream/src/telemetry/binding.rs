//! Telemetry → ontology entity binding (EH-408).
//!
//! A signal is bound to the Server, Service, Host, Deployment or Agent it came
//! from through **declared resolution keys**: a [`ResolutionRule`] names an
//! entity class and the attribute names whose values identify one individual of
//! it (`service.name`, `k8s.namespace.name` + `k8s.deployment.name`, `job`, …).
//! An [`EntityDirectory`] indexes the ontology individuals by the values they
//! declare for those same keys.
//!
//! Resolution is deterministic and never guesses:
//! * rules are tried in declared order and the FIRST rule whose keys are all
//!   present on the signal decides — a later, looser rule is never consulted to
//!   rescue an unknown key;
//! * a key no individual declares is [`Unresolved::UnknownEntity`];
//! * a key two individuals declare is [`Unresolved::Ambiguous`];
//! * a signal carrying no rule's keys is [`Unresolved::NoResolutionKey`].

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::signal::TelemetrySignal;

/// The core ontology namespace (`kg:`).
pub const KG_NAMESPACE: &str = "http://knuckles.team/kg#";
/// The infrastructure module namespace.
pub const INFRASTRUCTURE_NAMESPACE: &str = "http://knuckles.team/kg/infrastructure#";

/// The ontology classes telemetry binds to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityClass {
    /// `kg:Server`.
    Server,
    /// `infrastructure:Service`.
    Service,
    /// `infrastructure:Host`.
    Host,
    /// `infrastructure:Workload` — the Deployment/StatefulSet/DaemonSet controller.
    Deployment,
    /// `kg:Agent`.
    Agent,
}

impl EntityClass {
    /// The node `type` label and the local name of the class IRI.
    pub fn label(self) -> &'static str {
        match self {
            EntityClass::Server => "Server",
            EntityClass::Service => "Service",
            EntityClass::Host => "Host",
            EntityClass::Deployment => "Workload",
            EntityClass::Agent => "Agent",
        }
    }

    /// The namespace the class is declared in.
    fn namespace(self) -> &'static str {
        match self {
            EntityClass::Server | EntityClass::Agent => KG_NAMESPACE,
            EntityClass::Service | EntityClass::Host | EntityClass::Deployment => {
                INFRASTRUCTURE_NAMESPACE
            }
        }
    }

    /// The full class IRI.
    pub fn class_iri(self) -> String {
        format!("{}{}", self.namespace(), self.label())
    }
}

/// One ontology individual a signal can bind to.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EntityRef {
    pub class: EntityClass,
    /// The individual's graph node id.
    pub id: String,
}

/// One declared resolution key: signals carrying every attribute in `keys` bind
/// to the `class` individual declaring the same values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionRule {
    pub id: String,
    pub class: EntityClass,
    pub keys: Vec<String>,
}

impl ResolutionRule {
    pub fn new(id: &str, class: EntityClass, keys: &[&str]) -> ResolutionRule {
        ResolutionRule {
            id: id.to_string(),
            class,
            keys: keys.iter().map(|key| key.to_string()).collect(),
        }
    }

    /// The rule's key values read from `attributes`, or `None` if any is absent.
    fn key_values(&self, attributes: &BTreeMap<String, String>) -> Option<Vec<String>> {
        self.keys
            .iter()
            .map(|key| attributes.get(key).cloned())
            .collect()
    }
}

/// The declared resolution rules, in precedence order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionPolicy {
    pub rules: Vec<ResolutionRule>,
}

impl ResolutionPolicy {
    /// The OpenTelemetry semantic-convention and Prometheus defaults, most
    /// specific first: an agent id, a Kubernetes deployment, an OTLP service, a
    /// Prometheus job, a host name, a server address.
    pub fn standard() -> ResolutionPolicy {
        ResolutionPolicy {
            rules: vec![
                ResolutionRule::new("otel.agent", EntityClass::Agent, &["gen_ai.agent.id"]),
                ResolutionRule::new(
                    "k8s.deployment",
                    EntityClass::Deployment,
                    &["k8s.namespace.name", "k8s.deployment.name"],
                ),
                ResolutionRule::new("otel.service", EntityClass::Service, &["service.name"]),
                ResolutionRule::new("prometheus.job", EntityClass::Service, &["job"]),
                ResolutionRule::new("otel.host", EntityClass::Host, &["host.name"]),
                ResolutionRule::new("otel.server", EntityClass::Server, &["server.address"]),
            ],
        }
    }
}

/// An ontology individual together with the resolution-key values it declares.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredEntity {
    pub entity: EntityRef,
    pub keys: BTreeMap<String, String>,
}

/// Why a signal did not bind.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum Unresolved {
    /// The signal carries no declared rule's full key set.
    NoResolutionKey,
    /// The deciding rule's key names no declared individual.
    UnknownEntity { rule: String, key: Vec<String> },
    /// The deciding rule's key names more than one individual.
    Ambiguous {
        rule: String,
        key: Vec<String>,
        candidates: Vec<EntityRef>,
    },
}

/// The outcome of binding one signal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "resolution", rename_all = "snake_case")]
pub enum Resolution {
    Bound { entity: EntityRef, rule: String },
    Unresolved(Unresolved),
}

/// The individuals indexed by `(rule position, key values)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntityDirectory {
    rules: Vec<ResolutionRule>,
    index: BTreeMap<(usize, Vec<String>), BTreeSet<EntityRef>>,
}

impl EntityDirectory {
    /// Index `entities` under every rule of `policy` whose class matches the
    /// individual's and whose keys the individual declares in full.
    pub fn build(policy: &ResolutionPolicy, entities: &[DeclaredEntity]) -> EntityDirectory {
        let mut index: BTreeMap<(usize, Vec<String>), BTreeSet<EntityRef>> = BTreeMap::new();
        for declared in entities {
            for (position, rule) in policy.rules.iter().enumerate() {
                if rule.class != declared.entity.class {
                    continue;
                }
                if let Some(values) = rule.key_values(&declared.keys) {
                    index
                        .entry((position, values))
                        .or_default()
                        .insert(declared.entity.clone());
                }
            }
        }
        EntityDirectory {
            rules: policy.rules.clone(),
            index,
        }
    }

    /// Bind one signal (see the module docs for the precedence rule).
    pub fn resolve(&self, signal: &TelemetrySignal) -> Resolution {
        let deciding = self.rules.iter().enumerate().find_map(|(position, rule)| {
            rule.key_values(&signal.attributes)
                .map(|values| (position, rule, values))
        });
        let Some((position, rule, values)) = deciding else {
            return Resolution::Unresolved(Unresolved::NoResolutionKey);
        };
        let candidates = self.index.get(&(position, values.clone()));
        match candidates.map(|set| set.iter().cloned().collect::<Vec<_>>()) {
            Some(mut found) if found.len() == 1 => Resolution::Bound {
                entity: found.remove(0),
                rule: rule.id.clone(),
            },
            Some(found) if found.len() > 1 => Resolution::Unresolved(Unresolved::Ambiguous {
                rule: rule.id.clone(),
                key: values,
                candidates: found,
            }),
            _ => Resolution::Unresolved(Unresolved::UnknownEntity {
                rule: rule.id.clone(),
                key: values,
            }),
        }
    }
}
