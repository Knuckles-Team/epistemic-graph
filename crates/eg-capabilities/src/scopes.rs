//! The scope registry (IDM-05): every scope the engine or a domain app
//! authorizes, with its class.
//!
//! This table is the ONE source of truth for scope names. Every capability
//! ledger action and every op-level action must be registered here (the rot
//! test `every_authorized_action_is_registered` fails otherwise); the contract
//! generator publishes it as `contract/scopes.json` (and the packaged
//! `epistemic_graph/contract/scopes.json`), from which agent-utilities
//! GENERATES its session-scope allowlist. A hand-maintained list anywhere
//! else is drift.
//!
//! Class rules (enforced by the identity store, `eg_types::identity`):
//! * `user` / `domain` -- humans and services;
//! * `service-only` -- infrastructure a service executes on a caller's
//!   behalf; never reachable by a human (DECISIONS 2026-09-24 domain scopes);
//! * `approver` -- humans only, only through the scope's built-in group;
//! * `admin` -- humans only.

use eg_types::identity::{ScopeClass, ScopeClassifier};

use ScopeClass::{Admin, Approver, Domain, ServiceOnly, User};

/// One registered scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeEntry {
    pub scope: &'static str,
    pub class: ScopeClass,
    /// Who declares it: `engine` (capability ledger) or a domain app.
    pub owner: &'static str,
}

const fn entry(scope: &'static str, class: ScopeClass, owner: &'static str) -> ScopeEntry {
    ScopeEntry {
        scope,
        class,
        owner,
    }
}

/// Approver-class scopes and the one built-in group that confers each.
pub const APPROVER_GROUPS: &[(&str, &str)] = &[
    ("rbac:approve-elevation", "elevation-approvers"),
    ("finance:approve-live-order", "live-order-approvers"),
    ("governance:approve-schema-repair", "schema-approvers"),
];

/// The registry, sorted by scope.
pub const SCOPES: &[ScopeEntry] = &[
    entry("admin:backup", Admin, "engine"),
    entry("admin:cluster", Admin, "engine"),
    entry("admin:cluster-read", ServiceOnly, "engine"),
    entry("admin:connector-pack", Admin, "engine"),
    entry("admin:decision-eval", Admin, "engine"),
    entry("admin:decision-fit", Admin, "engine"),
    entry("admin:decision-log", Admin, "engine"),
    entry("admin:fleet-catalog", Admin, "engine"),
    entry("admin:outbox", Admin, "engine"),
    entry("admin:sqlite-file", Admin, "engine"),
    entry("agent:assemble-read", User, "engine"),
    entry("agent:component-read", User, "engine"),
    entry("agent:component-write", User, "engine"),
    entry("agent:decision-evaluate", User, "engine"),
    entry("agent:decision-read", User, "engine"),
    entry("agent:decision-write", User, "engine"),
    entry("agent:graph-read", User, "engine"),
    entry("agent:graph-write", User, "engine"),
    entry("agent:library-read", User, "engine"),
    entry("agent:library-write", User, "engine"),
    entry("agent:pack-control", User, "engine"),
    entry("agent:pack-read", User, "engine"),
    entry("agent:template-read", User, "engine"),
    entry("agent:template-write", User, "engine"),
    entry("analytics:worker", ServiceOnly, "engine"),
    entry("asr:transcribe", User, "engine"),
    entry("blob:admin", Admin, "engine"),
    entry("blob:read", User, "engine"),
    entry("blob:write", User, "engine"),
    entry("broker:ack", ServiceOnly, "engine"),
    entry("broker:admin", ServiceOnly, "engine"),
    entry("broker:consume", ServiceOnly, "engine"),
    entry("broker:publish", ServiceOnly, "engine"),
    entry("capacity:admin", ServiceOnly, "engine"),
    entry("capacity:lease", ServiceOnly, "engine"),
    entry("capacity:read", ServiceOnly, "engine"),
    entry("capacity:throttle", ServiceOnly, "engine"),
    entry("cdc:admin", Admin, "engine"),
    entry("cdc:read", User, "engine"),
    entry("cep:admin", Admin, "engine"),
    entry("cep:read", User, "engine"),
    entry("channel:admin", Admin, "engine"),
    entry("channel:read", User, "engine"),
    entry("channel:write", User, "engine"),
    entry("cluster:placement-read", User, "engine"),
    entry("cluster:topology-read", User, "engine"),
    entry("compute:datascience", ServiceOnly, "engine"),
    entry("compute:finance", ServiceOnly, "engine"),
    entry("compute:graph-algo", ServiceOnly, "engine"),
    entry("compute:parse", ServiceOnly, "engine"),
    entry("compute:semantic", ServiceOnly, "engine"),
    entry("compute:solve", ServiceOnly, "engine"),
    entry("compute:vision", ServiceOnly, "engine"),
    entry("connector:write-back", ServiceOnly, "engine"),
    entry("connector:write-back-read", ServiceOnly, "engine"),
    entry("distcompute:read", User, "engine"),
    entry("edge:read", User, "engine"),
    entry("edge:write", User, "engine"),
    entry("explain:read", User, "engine"),
    entry("federation:admin", Admin, "engine"),
    entry("finance:alerts", Domain, "finance"),
    entry("finance:approve-live-order", Approver, "finance"),
    entry("finance:backfill", Domain, "finance"),
    entry("finance:propose-order", Domain, "finance"),
    entry("finance:track", Domain, "finance"),
    entry("fleet:events", ServiceOnly, "graph-os"),
    entry("governance:approve-schema-repair", Approver, "engine"),
    entry("governance:propose", Domain, "engine"),
    entry("governance:read", User, "engine"),
    entry("graph:admin", Admin, "engine"),
    entry("graph:read", User, "engine"),
    entry("graph:write", User, "engine"),
    entry("graphlearn:write", User, "engine"),
    entry("identity:admin", Admin, "engine"),
    entry("identity:authenticate", ServiceOnly, "engine"),
    entry("identity:provision", ServiceOnly, "engine"),
    entry("identity:read", Admin, "engine"),
    entry("identity:self", User, "engine"),
    entry("ingest:read", User, "engine"),
    entry("ingest:write", User, "engine"),
    entry("jobs:write", User, "engine"),
    entry("kg:admin", Admin, "engine"),
    entry("kg:read", User, "engine"),
    entry("kg:write", User, "engine"),
    entry("kv:read", User, "engine"),
    entry("kv:write", User, "engine"),
    entry("lane:cleanup", ServiceOnly, "engine"),
    entry("lane:quota", ServiceOnly, "engine"),
    entry("lane:read", User, "engine"),
    entry("lane:reserve", ServiceOnly, "engine"),
    entry("lease:read", ServiceOnly, "engine"),
    entry("lease:write", ServiceOnly, "engine"),
    entry("ledger:admin", Admin, "engine"),
    entry("ledger:read", User, "engine"),
    entry("ledger:write", User, "engine"),
    entry("matview:admin", Admin, "engine"),
    entry("matview:read", User, "engine"),
    entry("memory:read", User, "engine"),
    entry("memory:write", User, "engine"),
    entry("mining:read", User, "engine"),
    entry("mining:write", User, "engine"),
    entry("modality:read", User, "engine"),
    entry("modality:write", User, "engine"),
    entry("node:admin", Admin, "engine"),
    entry("node:read", User, "engine"),
    entry("node:write", User, "engine"),
    entry("owl:read", User, "engine"),
    entry("quantum:run", User, "engine"),
    entry("query:cypher", User, "engine"),
    entry("query:decide", User, "engine"),
    entry("query:graphql", User, "engine"),
    entry("query:nl", User, "engine"),
    entry("query:sql", User, "engine"),
    entry("query:stream", User, "engine"),
    entry("query:unified", User, "engine"),
    entry("rbac:approve-elevation", Approver, "engine"),
    entry("rbac:elevation", User, "engine"),
    entry("rbac:elevation-read", User, "engine"),
    entry("rdf:read", User, "engine"),
    entry("rdf:write", User, "engine"),
    entry("reasoning:read", User, "engine"),
    entry("reasoning:write", User, "engine"),
    entry("registry:read", User, "engine"),
    entry("registry:write", User, "engine"),
    entry("resource:host", ServiceOnly, "engine"),
    entry("resource:read", User, "engine"),
    entry("resource:reserve", ServiceOnly, "engine"),
    entry("scene:read", User, "engine"),
    entry("scene:write", User, "engine"),
    entry("security:admin", Admin, "engine"),
    entry("security:audit", Admin, "engine"),
    entry("security:bootstrap", ServiceOnly, "engine"),
    entry("security:check", ServiceOnly, "engine"),
    entry("semantic:binding-read", User, "engine"),
    entry("semantic:binding-write", User, "engine"),
    entry("semantic:source-admit", ServiceOnly, "engine"),
    entry("semantic:stage-claim", ServiceOnly, "engine"),
    entry("semantic:stage-complete", ServiceOnly, "engine"),
    entry("semantic:stage-read", User, "engine"),
    entry("service:admin", Admin, "engine"),
    entry("service:control", User, "engine"),
    entry("source:ingest", User, "engine"),
    entry("sparql:read", User, "engine"),
    entry("statechart:write", User, "engine"),
    entry("stream:admin", Admin, "engine"),
    entry("stream:read", User, "engine"),
    entry("stream:write", User, "engine"),
    entry("telemetry:write", ServiceOnly, "telemetry"),
    entry("timeseries:read", User, "engine"),
    entry("timeseries:write", ServiceOnly, "engine"),
    entry("txn:control", User, "engine"),
    entry("txn:read", User, "engine"),
    entry("txn:write", User, "engine"),
    entry("udf:admin", Admin, "engine"),
    entry("udf:exec", User, "engine"),
    entry("validation:read", User, "engine"),
    entry("viz:render", User, "engine"),
    entry("webui:admin", Admin, "graph-os"),
    entry("webui:maintainer", User, "graph-os"),
    entry("webui:reader", User, "graph-os"),
    entry("webui:user", User, "graph-os"),
    entry("work:claim", User, "engine"),
    entry("work:claim-capability", ServiceOnly, "engine"),
    entry("work:delegate", User, "engine"),
    entry("work:read", User, "engine"),
    entry("work:submit", User, "engine"),
    entry("work:write", User, "engine"),
];

/// The registry entry of `scope`.
pub fn scope_entry(scope: &str) -> Option<&'static ScopeEntry> {
    SCOPES
        .binary_search_by(|entry| entry.scope.cmp(scope))
        .ok()
        .map(|index| &SCOPES[index])
}

/// The registry as the identity store consumes it.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScopeRegistry;

impl ScopeClassifier for ScopeRegistry {
    fn class_of(&self, scope: &str) -> Option<ScopeClass> {
        scope_entry(scope).map(|entry| entry.class)
    }

    fn approver_group_of(&self, scope: &str) -> Option<&'static str> {
        APPROVER_GROUPS
            .iter()
            .find(|(approver, _)| *approver == scope)
            .map(|(_, group)| *group)
    }
}

/// The class name as published (`user`, `domain`, `service-only`,
/// `approver`, `admin`).
pub fn class_name(class: ScopeClass) -> &'static str {
    match class {
        User => "user",
        Domain => "domain",
        ServiceOnly => "service-only",
        Approver => "approver",
        Admin => "admin",
    }
}

#[cfg(test)]
mod tests;
