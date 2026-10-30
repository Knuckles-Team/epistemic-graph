//! The placement-directory model (EG-PLACEMENT-R611.1): the validated cluster
//! snapshot, member and route a placement client follows, the bounded per-tenant
//! client pool and the per-endpoint circuit breaker.
//!
//! The engine is the placement authority: [`ClusterSnapshot`] wraps the served
//! `ClusterMembers` body and [`PlacementRoute`] is the served `PlacementRoute`
//! body. Neither type computes a route; both only validate what the engine said.
//! Every identifier is validated when a value is constructed (or deserialized),
//! so an invalid snapshot or route cannot exist as a typed value.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

pub use crate::epistemic_operations::PlacementRoute;
pub use crate::result_contract::cluster::ClusterMember;
use crate::result_contract::cluster::{ClusterDiscoverySnapshot, PlacementRouteWire};

/// Longest identifier the directory accepts, in bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 512;

/// Why a placement-directory value was refused at construction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementModelError {
    /// An identifier field is empty, too long, padded or carries control characters.
    #[error("placement identifier `{field}` is invalid: {reason}")]
    InvalidIdentifier {
        field: &'static str,
        reason: &'static str,
    },
    /// A structural rule other than identifier shape was broken.
    #[error("placement value is invalid: {0}")]
    Invalid(&'static str),
}

fn invalid(field: &'static str, reason: &'static str) -> PlacementModelError {
    PlacementModelError::InvalidIdentifier { field, reason }
}

/// Validate one identifier: non-empty, at most [`MAX_IDENTIFIER_BYTES`], no
/// surrounding whitespace, no control characters.
pub fn validate_identifier(field: &'static str, value: &str) -> Result<(), PlacementModelError> {
    if value.is_empty() {
        return Err(invalid(field, "empty"));
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(invalid(field, "too long"));
    }
    if value.trim() != value {
        return Err(invalid(field, "surrounding whitespace"));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(field, "control character"));
    }
    Ok(())
}

fn validate_digest(
    field: &'static str,
    value: &str,
    prefix: &str,
) -> Result<(), PlacementModelError> {
    let Some(hex) = value.strip_prefix(prefix) else {
        return Err(invalid(field, "wrong digest prefix"));
    };
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invalid(field, "not a 64-hex digest"));
    }
    Ok(())
}

fn validate_endpoint(field: &'static str, value: &str) -> Result<(), PlacementModelError> {
    validate_identifier(field, value)?;
    if value.chars().any(char::is_whitespace) {
        return Err(invalid(field, "whitespace in endpoint"));
    }
    Ok(())
}

impl ClusterMember {
    /// Validate this member's identifiers (EG-PLACEMENT-R611.1).
    pub fn validate(&self) -> Result<(), PlacementModelError> {
        validate_identifier("member_identity", &self.member_identity)?;
        validate_endpoint("client_endpoint", &self.client_endpoint)?;
        if !matches!(self.role.as_str(), "leader" | "follower" | "learner") {
            return Err(invalid("role", "not leader, follower or learner"));
        }
        if !matches!(self.health.as_str(), "healthy" | "degraded") {
            return Err(invalid("health", "not healthy or degraded"));
        }
        if let Some(name) = &self.tls_name {
            validate_identifier("tls_name", name)?;
        }
        if let Some(id) = &self.certificate.id {
            validate_identifier("certificate.id", id)?;
        }
        Ok(())
    }
}

fn validate_route_fields(
    route_id: &str,
    tenant_ref: &str,
    partition_ref: &str,
    placed: bool,
    epoch: u64,
    leader_ref: Option<&str>,
) -> Result<(), PlacementModelError> {
    validate_identifier("route_id", route_id)?;
    validate_identifier("tenant_ref", tenant_ref)?;
    validate_identifier("partition_ref", partition_ref)?;
    if let Some(leader) = leader_ref {
        validate_identifier("leader_ref", leader)?;
    }
    if placed && epoch == 0 {
        return Err(PlacementModelError::Invalid("a placed route has epoch 0"));
    }
    Ok(())
}

impl PlacementRoute {
    /// Validate this route's identifiers (EG-PLACEMENT-R611.1).
    pub fn validate(&self) -> Result<(), PlacementModelError> {
        validate_route_fields(
            &self.route_id,
            &self.tenant_ref,
            &self.partition_ref,
            self.placed,
            self.epoch,
            self.leader_ref.as_deref(),
        )
    }
}

impl PlacementRouteWire {
    /// Validate the served route body: the route identifiers plus every endpoint.
    /// The engine calls this before it answers `PlacementRoute`.
    pub fn validate(&self) -> Result<(), PlacementModelError> {
        validate_route_fields(
            &self.route_id,
            &self.tenant_ref,
            &self.partition_ref,
            self.placed,
            self.epoch,
            self.leader_ref.as_deref(),
        )?;
        self.endpoints
            .iter()
            .try_for_each(|endpoint| validate_endpoint("endpoints", endpoint))
    }
}

/// A validated `ClusterMembers` snapshot (EG-PLACEMENT-R611.1). Constructing or
/// deserializing one validates the cluster identity, every member, the group
/// structure and the signature shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "ClusterDiscoverySnapshot",
    into = "ClusterDiscoverySnapshot"
)]
pub struct ClusterSnapshot(ClusterDiscoverySnapshot);

impl ClusterSnapshot {
    /// Validate a served discovery snapshot.
    pub fn new(snapshot: ClusterDiscoverySnapshot) -> Result<Self, PlacementModelError> {
        validate_digest("cluster_id", &snapshot.cluster_id, "sha256:")?;
        validate_digest("signature", &snapshot.signature, "hmac-sha256:")?;
        let binding = &snapshot.auth_binding;
        validate_digest(
            "auth_binding.tenant_digest",
            &binding.tenant_digest,
            "sha256:",
        )?;
        validate_digest(
            "auth_binding.principal_digest",
            &binding.principal_digest,
            "sha256:",
        )?;
        validate_digest(
            "auth_binding.agent_digest",
            &binding.agent_digest,
            "sha256:",
        )?;
        let mut groups = HashSet::new();
        for group in &snapshot.groups {
            if !groups.insert(group.group_id) {
                return Err(PlacementModelError::Invalid("duplicate group id"));
            }
            let mut nodes = HashSet::new();
            for member in &group.members {
                member.validate()?;
                if !nodes.insert(member.node_id) {
                    return Err(PlacementModelError::Invalid("duplicate member in a group"));
                }
            }
            if let Some(leader) = group.leader_id {
                if !nodes.contains(&leader) {
                    return Err(PlacementModelError::Invalid("group leader is not a member"));
                }
            }
        }
        Ok(Self(snapshot))
    }

    /// The validated snapshot body.
    pub fn snapshot(&self) -> &ClusterDiscoverySnapshot {
        &self.0
    }

    /// The membership epoch the snapshot was taken at.
    pub fn membership_epoch(&self) -> u64 {
        self.0.membership_epoch
    }

    /// The placement (routing) epoch the snapshot was taken at.
    pub fn placement_epoch(&self) -> u64 {
        self.0.placement_epoch
    }

    /// The validated body, consumed.
    pub fn into_inner(self) -> ClusterDiscoverySnapshot {
        self.0
    }
}

impl TryFrom<ClusterDiscoverySnapshot> for ClusterSnapshot {
    type Error = PlacementModelError;

    fn try_from(snapshot: ClusterDiscoverySnapshot) -> Result<Self, Self::Error> {
        Self::new(snapshot)
    }
}

impl From<ClusterSnapshot> for ClusterDiscoverySnapshot {
    fn from(snapshot: ClusterSnapshot) -> Self {
        snapshot.0
    }
}

/// A breaker's observable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakerState {
    /// Calls flow; consecutive failures are counted.
    Closed,
    /// Fail fast until the cooldown elapses.
    Open,
    /// One trial call is allowed; its outcome closes or re-opens the breaker.
    HalfOpen,
}

/// A breaker's configuration, validated at construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BreakerPolicyWire", into = "BreakerPolicyWire")]
pub struct BreakerPolicy {
    failure_threshold: u32,
    cooldown_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BreakerPolicyWire {
    failure_threshold: u32,
    cooldown_ms: u64,
}

impl BreakerPolicy {
    /// A policy that opens after `failure_threshold` (at least 1) consecutive
    /// failures and stays open for `cooldown_ms` (at least 1).
    pub fn new(failure_threshold: u32, cooldown_ms: u64) -> Result<Self, PlacementModelError> {
        if failure_threshold == 0 {
            return Err(PlacementModelError::Invalid(
                "breaker failure threshold is 0",
            ));
        }
        if cooldown_ms == 0 {
            return Err(PlacementModelError::Invalid("breaker cooldown is 0"));
        }
        Ok(Self {
            failure_threshold,
            cooldown_ms,
        })
    }

    /// Consecutive failures that open the breaker.
    pub fn failure_threshold(&self) -> u32 {
        self.failure_threshold
    }

    /// How long the breaker stays open.
    pub fn cooldown(&self) -> Duration {
        Duration::from_millis(self.cooldown_ms)
    }
}

impl TryFrom<BreakerPolicyWire> for BreakerPolicy {
    type Error = PlacementModelError;

    fn try_from(wire: BreakerPolicyWire) -> Result<Self, Self::Error> {
        Self::new(wire.failure_threshold, wire.cooldown_ms)
    }
}

impl From<BreakerPolicy> for BreakerPolicyWire {
    fn from(policy: BreakerPolicy) -> Self {
        Self {
            failure_threshold: policy.failure_threshold,
            cooldown_ms: policy.cooldown_ms,
        }
    }
}

#[derive(Debug, Default)]
struct BreakerInner {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    half_open_in_flight: bool,
}

/// A per-target circuit breaker: Closed, then Open after consecutive failures,
/// then HalfOpen on a timer. Driven by an explicit `now` so it is deterministic.
#[derive(Debug)]
pub struct Breaker {
    policy: BreakerPolicy,
    inner: Mutex<BreakerInner>,
}

impl Breaker {
    /// A closed breaker with `policy`.
    pub fn new(policy: BreakerPolicy) -> Self {
        Self {
            policy,
            inner: Mutex::new(BreakerInner::default()),
        }
    }

    /// The breaker's policy.
    pub fn policy(&self) -> BreakerPolicy {
        self.policy
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BreakerInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The state at `now`.
    pub fn state_at(&self, now: Instant) -> BreakerState {
        match self.lock().open_until {
            Some(until) if now < until => BreakerState::Open,
            Some(_) => BreakerState::HalfOpen,
            None => BreakerState::Closed,
        }
    }

    /// Whether a call may proceed at `now`. Closed admits; Open refuses; HalfOpen
    /// admits exactly one trial whose outcome the caller reports.
    pub fn allow(&self, now: Instant) -> bool {
        let mut inner = self.lock();
        match inner.open_until {
            Some(until) if now < until => false,
            Some(_) if inner.half_open_in_flight => false,
            Some(_) => {
                inner.half_open_in_flight = true;
                true
            }
            None => true,
        }
    }

    /// Record a success: close the breaker and reset the failure count.
    pub fn on_success(&self) {
        *self.lock() = BreakerInner::default();
    }

    /// Record a failure at `now`: open once the threshold is reached or a
    /// half-open trial failed.
    pub fn on_failure(&self, now: Instant) {
        let mut inner = self.lock();
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        let trial_failed = std::mem::take(&mut inner.half_open_in_flight);
        if trial_failed || inner.consecutive_failures >= self.policy.failure_threshold {
            inner.open_until = Some(now + self.policy.cooldown());
        }
    }
}

/// A bounded LRU pool of per-tenant clients (EG-PLACEMENT-R611.1). Tenant keys
/// are validated identifiers; the least recently used tenant is evicted when the
/// pool is full.
#[derive(Debug)]
pub struct TenantPool<C> {
    capacity: usize,
    tick: u64,
    entries: HashMap<String, (C, u64)>,
    order: BTreeMap<u64, String>,
    evictions: u64,
}

impl<C> TenantPool<C> {
    /// An empty pool holding at most `capacity` (at least 1) tenants.
    pub fn new(capacity: usize) -> Result<Self, PlacementModelError> {
        if capacity == 0 {
            return Err(PlacementModelError::Invalid("tenant pool capacity is 0"));
        }
        Ok(Self {
            capacity,
            tick: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
            evictions: 0,
        })
    }

    fn touch(&mut self, tenant: &str) {
        self.tick += 1;
        if let Some((_, used)) = self.entries.get_mut(tenant) {
            self.order.remove(used);
            *used = self.tick;
            self.order.insert(self.tick, tenant.to_string());
        }
    }

    /// The client for `tenant`, built with `make` on a miss. A miss on a full
    /// pool evicts the least recently used tenant first.
    pub fn acquire(
        &mut self,
        tenant: &str,
        make: impl FnOnce() -> C,
    ) -> Result<&C, PlacementModelError> {
        validate_identifier("tenant", tenant)?;
        if self.entries.contains_key(tenant) {
            self.touch(tenant);
        } else {
            if self.entries.len() >= self.capacity {
                if let Some((_, oldest)) = self.order.pop_first() {
                    self.entries.remove(&oldest);
                    self.evictions += 1;
                }
            }
            self.tick += 1;
            self.order.insert(self.tick, tenant.to_string());
            self.entries.insert(tenant.to_string(), (make(), self.tick));
        }
        Ok(&self.entries[tenant].0)
    }

    /// Tenants currently pooled.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The pool's bound.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Tenants evicted so far.
    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Pooled tenants, least recently used first.
    pub fn tenants(&self) -> Vec<&str> {
        self.order.values().map(String::as_str).collect()
    }
}

#[cfg(test)]
mod tests;
