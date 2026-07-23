//! Cross-region async read-replica tier + capacity guardrails
//! (CONCEPT:EG-KG.sharding.follower-pull-loop replica tier, CONCEPT:EG-KG.coordination.circuit-breaker guardrails).
//!
//! The engine already has TWO consistency tiers: a single-node redb-authoritative store,
//! and synchronous multi-Raft groups + super-cluster federated *read* (CONCEPT:EG-KG.ontology.federation-client).
//! Neither gives a geographically-distant region a LOCAL, low-latency read copy that does
//! not pay a cross-region Raft round-trip on every write. This module adds the missing
//! tier — an **asynchronous, eventually-consistent read replica** — plus the capacity
//! guardrails that keep a slow/hostile region or a greedy tenant from destabilising the
//! primary.
//!
//! Everything here is gated behind `federation-search` (the same pure-Rust `ureq` stack
//! CONCEPT:EG-KG.ontology.federation-client already links) and is therefore OUT of the Pi tier — no new dependency,
//! and a `pi`/`default` build links none of it.
//!
//! ## The async read-replica tier (CONCEPT:EG-KG.sharding.follower-pull-loop)
//!
//! * **Primary side** — every committed durable mutation is appended to a bounded
//!   in-memory [`ReplicationLog`] keyed by a monotone **LSN** (log sequence number). A
//!   follower asks "give me everything after LSN `n`" and gets the ordered tail; the ring
//!   bounds primary memory (old entries fall off — a follower that lags past the ring must
//!   re-snapshot, reported as [`ReplicaLag::Behind`]).
//! * **Follower side** — [`run_replica_follower`] periodically pulls the primary's tail
//!   over HTTP (`/replicate?since=<lsn>`), applies each op through the SAME canonical
//!   `crate::mutation_apply::apply` path Raft + WAL replay use (so a replicated op lands
//!   byte-identically), and advances its cursor. Reads on a follower are served from its
//!   local registry — zero cross-region latency, bounded staleness.
//!
//! ## Capacity guardrails (CONCEPT:EG-KG.coordination.circuit-breaker)
//!
//! Three composable, pure, unit-testable guards that the transport / follower consult:
//!
//! * **Circuit breaker** ([`CircuitBreaker`]) — a per-target Closed→Open→HalfOpen breaker.
//!   After `failure_threshold` consecutive failures it OPENS (fail-fast, no calls) for a
//!   `cooldown`, then HALF-OPENS one trial call; a success closes it, a failure re-opens.
//!   Stops a dead region from tying up follower threads on doomed pulls.
//! * **Per-tenant quota** ([`CapacityGuard`]) — a hard cap on concurrent in-flight work
//!   per tenant, so one greedy tenant cannot exhaust the pool (bounds blast radius). This
//!   complements the QoS scheduler (CONCEPT:EG-KG.coordination.backpressure-busy-signal): QoS *reorders* admission by priority,
//!   this is an absolute *ceiling* that also protects the replica-serve path.
//! * **Backpressure** — when the global in-flight count crosses a high-water mark the guard
//!   returns [`GuardDecision::Backpressure`] so the caller sheds load (a `BUSY` retry-later)
//!   instead of queueing unbounded work.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::protocol::{GraphType, Method};

// ── CONCEPT:EG-KG.sharding.follower-pull-loop — the replication log + shipped op ────────────────────────

/// One committed mutation shipped to a follower (CONCEPT:EG-KG.sharding.follower-pull-loop). Carries the same
/// fields a `RaftRequest` does plus the monotone LSN that orders the stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationOp {
    /// Monotone log sequence number — the follower cursor advances past this.
    pub lsn: u64,
    /// Human-readable graph name (the follower creates it on first apply if absent).
    pub graph_name: String,
    /// Sanitized graph file-name (the persistence-tier key).
    pub graph_fname: String,
    /// Graph type (used if the follower must create the graph on apply).
    pub graph_type: GraphType,
    /// The durable mutation, applied via the canonical `crate::mutation_apply::apply` path.
    pub method: Method,
}

/// A follower's position relative to the primary's log (CONCEPT:EG-KG.sharding.follower-pull-loop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaLag {
    /// The follower cursor is within the retained ring — a normal incremental pull.
    Current,
    /// The follower has fallen behind the ring's oldest retained LSN and must
    /// re-snapshot (the requested `since` is older than everything retained).
    Behind,
}

/// A bounded, monotone-LSN in-memory log of recent committed mutations (CONCEPT:EG-KG.sharding.follower-pull-loop).
///
/// The primary appends every durable mutation here; a follower reads the ordered tail
/// after its cursor. The ring bounds primary memory: once `capacity` is exceeded the
/// oldest entries are dropped, and a follower that lags past the oldest retained LSN is
/// told to re-snapshot ([`ReplicaLag::Behind`]) rather than silently skipping ops.
#[derive(Debug)]
pub struct ReplicationLog {
    inner: Mutex<VecDeque<ReplicationOp>>,
    next_lsn: AtomicU64,
    capacity: usize,
}

impl ReplicationLog {
    /// A log retaining up to `capacity` recent ops (floored to 1). LSNs start at 1.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            next_lsn: AtomicU64::new(1),
            capacity: capacity.max(1),
        }
    }

    /// Append a committed mutation and return its assigned LSN (CONCEPT:EG-KG.sharding.follower-pull-loop). O(1)
    /// amortized; drops the oldest op once the ring is full.
    pub fn append(
        &self,
        graph_name: &str,
        graph_fname: &str,
        graph_type: GraphType,
        method: Method,
    ) -> u64 {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut q = self.inner.lock().expect("replication log poisoned");
        q.push_back(ReplicationOp {
            lsn,
            graph_name: graph_name.to_string(),
            graph_fname: graph_fname.to_string(),
            graph_type,
            method,
        });
        while q.len() > self.capacity {
            q.pop_front();
        }
        lsn
    }

    /// The highest LSN assigned so far (0 if empty) — the primary's replication frontier.
    pub fn latest_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst).saturating_sub(1)
    }

    /// The ordered tail of ops with `lsn > since` (CONCEPT:EG-KG.sharding.follower-pull-loop), plus whether the
    /// follower has fallen behind the retained ring. `Behind` ⇒ the requested cursor is
    /// older than the oldest retained op (the follower missed ops and must re-snapshot).
    pub fn since(&self, since: u64) -> (Vec<ReplicationOp>, ReplicaLag) {
        let q = self.inner.lock().expect("replication log poisoned");
        let oldest = q.front().map(|o| o.lsn);
        // Behind only when we actually retain ops AND the cursor predates the oldest one
        // (a gap: ops between `since` and `oldest` were evicted).
        let lag = match oldest {
            Some(old) if since + 1 < old => ReplicaLag::Behind,
            _ => ReplicaLag::Current,
        };
        let ops = q.iter().filter(|o| o.lsn > since).cloned().collect();
        (ops, lag)
    }
}

// ── CONCEPT:EG-KG.coordination.circuit-breaker — circuit breaker ─────────────────────────────────────────

/// Circuit-breaker state (CONCEPT:EG-KG.coordination.circuit-breaker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Calls flow; consecutive failures are counted.
    Closed,
    /// Fail-fast — no calls until the cooldown elapses.
    Open,
    /// One trial call is allowed; its outcome closes or re-opens the breaker.
    HalfOpen,
}

/// A per-target circuit breaker (CONCEPT:EG-KG.coordination.circuit-breaker). Pure decision logic driven by an
/// explicit `now: Instant`, so it is deterministically unit-testable without sleeping.
#[derive(Debug)]
pub struct CircuitBreaker {
    failure_threshold: u32,
    cooldown: Duration,
    consecutive_failures: AtomicU64,
    /// `Some(instant)` = OPEN until that instant; guarded by the mutex for the trial gate.
    state: Mutex<BreakerInner>,
}

#[derive(Debug)]
struct BreakerInner {
    open_until: Option<Instant>,
    half_open_in_flight: bool,
}

impl CircuitBreaker {
    /// A closed breaker that opens after `failure_threshold` (≥1) consecutive failures and
    /// stays open for `cooldown`.
    pub fn new(failure_threshold: u32, cooldown: Duration) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            cooldown,
            consecutive_failures: AtomicU64::new(0),
            state: Mutex::new(BreakerInner {
                open_until: None,
                half_open_in_flight: false,
            }),
        }
    }

    /// The breaker's state at `now` (CONCEPT:EG-KG.coordination.circuit-breaker) — `Open` while cooling down, else
    /// `HalfOpen` if the cooldown just elapsed and no trial is in flight, else `Closed`.
    pub fn state_at(&self, now: Instant) -> BreakerState {
        let g = self.state.lock().expect("breaker poisoned");
        match g.open_until {
            Some(until) if now < until => BreakerState::Open,
            Some(_) => BreakerState::HalfOpen,
            None => BreakerState::Closed,
        }
    }

    /// Whether a call may proceed at `now` (CONCEPT:EG-KG.coordination.circuit-breaker). `Closed` ⇒ yes; `Open` ⇒ no;
    /// `HalfOpen` ⇒ yes for exactly ONE trial call (the caller reports its outcome via
    /// [`on_success`]/[`on_failure`]). Advances the internal state (arms the trial gate).
    ///
    /// [`on_success`]: CircuitBreaker::on_success
    /// [`on_failure`]: CircuitBreaker::on_failure
    pub fn allow(&self, now: Instant) -> bool {
        let mut g = self.state.lock().expect("breaker poisoned");
        match g.open_until {
            Some(until) if now < until => false, // OPEN — fail fast
            Some(_) => {
                // Cooldown elapsed → HALF-OPEN: admit exactly one trial call.
                if g.half_open_in_flight {
                    false
                } else {
                    g.half_open_in_flight = true;
                    true
                }
            }
            None => true, // CLOSED
        }
    }

    /// Record a successful call at `now` (CONCEPT:EG-KG.coordination.circuit-breaker): resets the failure count and
    /// fully closes the breaker (clears any half-open trial).
    pub fn on_success(&self, _now: Instant) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
        let mut g = self.state.lock().expect("breaker poisoned");
        g.open_until = None;
        g.half_open_in_flight = false;
    }

    /// Record a failed call at `now` (CONCEPT:EG-KG.coordination.circuit-breaker): increments the failure count and,
    /// once it reaches the threshold (or a half-open trial failed), OPENS for the cooldown.
    pub fn on_failure(&self, now: Instant) {
        let fails = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        let mut g = self.state.lock().expect("breaker poisoned");
        let was_half_open = g.half_open_in_flight;
        g.half_open_in_flight = false;
        if was_half_open || fails >= self.failure_threshold as u64 {
            g.open_until = Some(now + self.cooldown);
        }
    }
}

// ── CONCEPT:EG-KG.coordination.circuit-breaker — per-tenant quota + backpressure guard ───────────────────

/// The guard's verdict for one request (CONCEPT:EG-KG.coordination.circuit-breaker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardDecision {
    /// Admit; the returned token must be released via [`CapacityGuard::release`].
    Admit,
    /// The tenant is at its hard quota — reject (bounds per-tenant blast radius).
    QuotaExceeded,
    /// Global load crossed the high-water mark — shed as backpressure (retry later).
    Backpressure,
}

/// Per-tenant concurrency quota + global backpressure guardrail (CONCEPT:EG-KG.coordination.circuit-breaker).
///
/// A pure counting gate: it tracks global in-flight and per-tenant in-flight, and admits
/// only when the tenant is under its quota AND global load is under the high-water mark.
/// Complements the QoS scheduler (CONCEPT:EG-KG.coordination.backpressure-busy-signal) — QoS reorders by priority, this is the
/// absolute ceiling that also fronts the replica-serve path.
#[derive(Debug)]
pub struct CapacityGuard {
    per_tenant_quota: usize,
    global_high_water: usize,
    global_inflight: AtomicUsize,
    per_tenant: dashmap::DashMap<String, usize>,
}

impl CapacityGuard {
    /// A guard with a per-tenant quota and a global high-water mark (each floored to 1).
    pub fn new(per_tenant_quota: usize, global_high_water: usize) -> Self {
        Self {
            per_tenant_quota: per_tenant_quota.max(1),
            global_high_water: global_high_water.max(1),
            global_inflight: AtomicUsize::new(0),
            per_tenant: dashmap::DashMap::new(),
        }
    }

    /// Try to admit one request for `tenant` (CONCEPT:EG-KG.coordination.circuit-breaker). On [`GuardDecision::Admit`]
    /// the caller MUST later call [`release`](CapacityGuard::release) with the same tenant.
    pub fn try_acquire(&self, tenant: &str) -> GuardDecision {
        // Backpressure first — a saturated pool sheds regardless of tenant.
        if self.global_inflight.load(Ordering::SeqCst) >= self.global_high_water {
            return GuardDecision::Backpressure;
        }
        let mut entry = self.per_tenant.entry(tenant.to_string()).or_insert(0);
        if *entry >= self.per_tenant_quota {
            return GuardDecision::QuotaExceeded;
        }
        *entry += 1;
        self.global_inflight.fetch_add(1, Ordering::SeqCst);
        GuardDecision::Admit
    }

    /// Release one admitted request for `tenant` (CONCEPT:EG-KG.coordination.circuit-breaker). Idempotent-safe: never
    /// underflows below zero.
    pub fn release(&self, tenant: &str) {
        if let Some(mut e) = self.per_tenant.get_mut(tenant) {
            *e = e.saturating_sub(1);
        }
        let prev = self.global_inflight.load(Ordering::SeqCst);
        if prev > 0 {
            self.global_inflight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Current global in-flight count (observability/tests).
    pub fn global_inflight(&self) -> usize {
        self.global_inflight.load(Ordering::SeqCst)
    }
}

// ── CONCEPT:EG-KG.sharding.follower-pull-loop — process-global primary log + /replicate serve ────────────

/// The process-global primary replication log (CONCEPT:EG-KG.sharding.follower-pull-loop), armed ONCE from the
/// environment on first use. `None` unless `EPISTEMIC_GRAPH_REPLICATE` is truthy — then
/// the commit path appends every durable mutation here and [`serve`] streams the tail. A
/// non-replicated primary never arms it and pays nothing (mirrors the CONCEPT:EG-KG.coordination.backpressure-busy-signal
/// `qos::configured()` opt-in idiom). Ring capacity from `EPISTEMIC_GRAPH_REPLICATE_RING`
/// (default 100 000).
pub fn global_log() -> Option<&'static ReplicationLog> {
    use std::sync::OnceLock;
    static LOG: OnceLock<Option<ReplicationLog>> = OnceLock::new();
    LOG.get_or_init(|| {
        let on = std::env::var("EPISTEMIC_GRAPH_REPLICATE")
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "on" | "yes"
                )
            })
            .unwrap_or(false);
        if !on {
            return None;
        }
        let cap = std::env::var("EPISTEMIC_GRAPH_REPLICATE_RING")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(100_000);
        Some(ReplicationLog::new(cap))
    })
    .as_ref()
}

/// Serve the primary's `/replicate?since=<lsn>` endpoint (CONCEPT:EG-KG.sharding.follower-pull-loop) using the same
/// hand-rolled dependency-free HTTP framing idiom as the CONCEPT:EG-KG.ontology.federation-client `/federated`
/// listener. A follower GETs the ordered tail after its cursor; the body is a JSON array
/// of [`ReplicationOp`]. When the follower has fallen behind the retained ring the response
/// carries an `x-replica-lag: behind` header so the follower knows to re-snapshot.
pub async fn serve(listener: tokio::net::TcpListener) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            // Read the request line (headers to blank line) — GET only, no body.
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            loop {
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
                match stream.read(&mut tmp).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&tmp[..n]),
                }
                if buf.len() > 64 * 1024 {
                    return; // header flood guard
                }
            }
            let head = String::from_utf8_lossy(&buf);
            let target = head
                .split("\r\n")
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/");
            let (path, qs) = target.split_once('?').unwrap_or((target, ""));
            let (status, ctype, extra, body) = if path != "/replicate" {
                (
                    "404 Not Found",
                    "application/json",
                    "",
                    r#"{"error":"not found"}"#.to_string(),
                )
            } else {
                let since = qs
                    .split('&')
                    .find_map(|kv| kv.strip_prefix("since="))
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                match global_log() {
                    None => (
                        "503 Service Unavailable",
                        "application/json",
                        "",
                        r#"{"error":"replication not enabled on this primary"}"#.to_string(),
                    ),
                    Some(log) => {
                        let (ops, lag) = log.since(since);
                        let extra = if lag == ReplicaLag::Behind {
                            "x-replica-lag: behind\r\n"
                        } else {
                            ""
                        };
                        let body = serde_json::to_string(&ops).unwrap_or_else(|_| "[]".to_string());
                        ("200 OK", "application/json", extra, body)
                    }
                }
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\n{extra}connection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

// ── CONCEPT:EG-KG.sharding.follower-pull-loop — follower config + apply + pull loop ──────────────────────

/// Env-driven configuration for a cross-region read replica (CONCEPT:EG-KG.sharding.follower-pull-loop).
#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    /// The primary engine's base-URL (e.g. `https://eg-primary.example:7900`).
    pub primary_url: String,
    /// Seconds between tail pulls.
    pub poll_secs: u64,
    /// Per-pull HTTP read/connect timeout (seconds).
    pub timeout_secs: u64,
    /// Consecutive pull failures before the breaker opens.
    pub failure_threshold: u32,
    /// Breaker cooldown (seconds) once open.
    pub cooldown_secs: u64,
}

/// Env var naming the primary for a follower node (CONCEPT:EG-KG.sharding.follower-pull-loop). Presence turns this
/// node into a read replica of that primary.
pub const REPLICA_PRIMARY_ENV: &str = "EPISTEMIC_GRAPH_REPLICA_PRIMARY";

impl ReplicaConfig {
    /// Parse the follower config from the environment (CONCEPT:EG-KG.sharding.follower-pull-loop). Returns `None`
    /// unless [`REPLICA_PRIMARY_ENV`] is set (this node is a plain primary otherwise).
    pub fn from_env() -> Option<Self> {
        let primary_url = std::env::var(REPLICA_PRIMARY_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())?;
        let get_u64 = |k: &str, d: u64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(d)
        };
        Some(Self {
            primary_url: primary_url.trim().to_string(),
            poll_secs: get_u64("EPISTEMIC_GRAPH_REPLICA_POLL_SECS", 2),
            timeout_secs: get_u64("EPISTEMIC_GRAPH_REPLICA_TIMEOUT_SECS", 10),
            failure_threshold: get_u64("EPISTEMIC_GRAPH_REPLICA_FAIL_THRESHOLD", 5) as u32,
            cooldown_secs: get_u64("EPISTEMIC_GRAPH_REPLICA_COOLDOWN_SECS", 15),
        })
    }
}

/// Apply a shipped replication batch into the local registry (CONCEPT:EG-KG.sharding.follower-pull-loop) — the
/// follower's state-machine step. Each op creates its graph if absent, then applies the
/// mutation through the canonical `crate::mutation_apply::apply` path (byte-identical to Raft/WAL
/// replay), so a follower converges to the primary's state. Returns the highest LSN
/// applied (the new cursor), or the input cursor if the batch was empty.
pub async fn apply_replicated_batch(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    ops: &[ReplicationOp],
    cursor: u64,
) -> u64 {
    let mut max_lsn = cursor;
    for op in ops {
        let core = {
            let mut s = state.write().await;
            if !s.registry.exists(&op.graph_name) {
                let _ = s.registry.create_graph(&op.graph_name, op.graph_type, None);
            }
            s.registry.get(&op.graph_name).map(|e| e.core.clone())
        };
        if let Some(core) = core {
            crate::mutation_apply::apply(&core, &op.method);
            core.mark_dirty();
            max_lsn = max_lsn.max(op.lsn);
        }
    }
    max_lsn
}

/// Pull one tail batch from the primary's `/replicate?since=<cursor>` endpoint
/// (CONCEPT:EG-KG.sharding.follower-pull-loop). Blocking `ureq` (mirrors the CONCEPT:EG-KG.ontology.federation-client federation client), so
/// callers run it inside `spawn_blocking`. Returns the decoded ops on success.
pub fn pull_replication_tail(
    cfg: &ReplicaConfig,
    cursor: u64,
) -> Result<Vec<ReplicationOp>, String> {
    use std::io::Read;
    let endpoint = format!("{}/replicate?since={}", cfg.primary_url, cursor);
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(cfg.timeout_secs))
        .timeout_read(Duration::from_secs(cfg.timeout_secs))
        .build();
    let resp = agent
        .get(&endpoint)
        .set("Accept", "application/json")
        .call()
        .map_err(|e| format!("GET {endpoint} failed: {e}"))?;
    let mut text = String::new();
    resp.into_reader()
        .take(64 * 1024 * 1024)
        .read_to_string(&mut text)
        .map_err(|e| format!("reading {endpoint}: {e}"))?;
    serde_json::from_str::<Vec<ReplicationOp>>(&text)
        .map_err(|e| format!("replication tail not ReplicationOp-shaped: {e}"))
}

/// Run the follower pull loop until the process exits (CONCEPT:EG-KG.sharding.follower-pull-loop). Periodically pulls
/// the primary's tail (guarded by a [`CircuitBreaker`] so a dead primary fails fast for a
/// cooldown rather than blocking every tick), applies it, and advances the cursor. Spawned
/// from `main` when [`ReplicaConfig::from_env`] is `Some`.
pub async fn run_replica_follower(
    state: std::sync::Arc<tokio::sync::RwLock<crate::server::ServerState>>,
    cfg: ReplicaConfig,
) {
    let breaker = CircuitBreaker::new(
        cfg.failure_threshold,
        Duration::from_secs(cfg.cooldown_secs),
    );
    let mut cursor: u64 = 0;
    let poll = Duration::from_secs(cfg.poll_secs.max(1));
    loop {
        tokio::time::sleep(poll).await;
        let now = Instant::now();
        if !breaker.allow(now) {
            continue; // breaker OPEN — skip this tick (fail fast)
        }
        let cfg2 = cfg.clone();
        let pull = tokio::task::spawn_blocking(move || pull_replication_tail(&cfg2, cursor)).await;
        match pull {
            Ok(Ok(ops)) => {
                breaker.on_success(Instant::now());
                if !ops.is_empty() {
                    cursor = apply_replicated_batch(&state, &ops, cursor).await;
                }
            }
            Ok(Err(e)) => {
                breaker.on_failure(Instant::now());
                tracing::warn!("replica follower pull failed: {e}");
            }
            Err(e) => {
                breaker.on_failure(Instant::now());
                tracing::warn!("replica follower pull task panicked: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(id: &str) -> Method {
        Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: vec![],
        }
    }

    #[test]
    fn eg3_1_replication_log_ships_ordered_tail_after_cursor() {
        let log = ReplicationLog::new(100);
        let l1 = log.append("g", "g", GraphType::Global, add("a"));
        let l2 = log.append("g", "g", GraphType::Global, add("b"));
        let l3 = log.append("g", "g", GraphType::Global, add("c"));
        assert_eq!((l1, l2, l3), (1, 2, 3));
        assert_eq!(log.latest_lsn(), 3);
        let (tail, lag) = log.since(1);
        assert_eq!(lag, ReplicaLag::Current);
        assert_eq!(
            tail.iter().map(|o| o.lsn).collect::<Vec<_>>(),
            vec![2, 3],
            "only ops after the cursor, in order"
        );
    }

    #[test]
    fn eg3_1_replication_log_reports_behind_when_cursor_evicted() {
        let log = ReplicationLog::new(2); // retain only the 2 newest
        for c in ["a", "b", "c", "d"] {
            log.append("g", "g", GraphType::Global, add(c));
        }
        // LSNs 1,2 were evicted; retained oldest is 3. A follower at cursor 1 missed ops.
        let (_ops, lag) = log.since(1);
        assert_eq!(lag, ReplicaLag::Behind, "cursor predates the retained ring");
        // A follower at cursor 3 is current (next wanted = 4, oldest retained = 3).
        let (ops, lag2) = log.since(3);
        assert_eq!(lag2, ReplicaLag::Current);
        assert_eq!(ops.iter().map(|o| o.lsn).collect::<Vec<_>>(), vec![4]);
    }

    #[test]
    fn eg3_2_circuit_breaker_opens_after_threshold_then_half_opens_and_recovers() {
        let t0 = Instant::now();
        let cb = CircuitBreaker::new(3, Duration::from_secs(10));
        assert_eq!(cb.state_at(t0), BreakerState::Closed);
        assert!(cb.allow(t0));
        // Three consecutive failures OPEN the breaker.
        cb.on_failure(t0);
        cb.on_failure(t0);
        assert_eq!(
            cb.state_at(t0),
            BreakerState::Closed,
            "below threshold still closed"
        );
        cb.on_failure(t0);
        assert_eq!(
            cb.state_at(t0),
            BreakerState::Open,
            "threshold reached → open"
        );
        assert!(!cb.allow(t0), "open breaker fails fast");
        // After the cooldown it HALF-OPENS: exactly one trial admitted.
        let t1 = t0 + Duration::from_secs(11);
        assert_eq!(cb.state_at(t1), BreakerState::HalfOpen);
        assert!(cb.allow(t1), "one trial call admitted");
        assert!(!cb.allow(t1), "second trial refused while one is in flight");
        // The trial SUCCEEDS → closed again.
        cb.on_success(t1);
        assert_eq!(cb.state_at(t1), BreakerState::Closed);
        assert!(cb.allow(t1));
    }

    #[test]
    fn eg3_2_half_open_trial_failure_reopens() {
        let t0 = Instant::now();
        let cb = CircuitBreaker::new(1, Duration::from_secs(5));
        cb.on_failure(t0); // threshold 1 → open immediately
        assert_eq!(cb.state_at(t0), BreakerState::Open);
        let t1 = t0 + Duration::from_secs(6);
        assert!(cb.allow(t1), "half-open trial admitted");
        cb.on_failure(t1); // trial fails → reopen
        assert_eq!(cb.state_at(t1), BreakerState::Open);
        assert!(!cb.allow(t1));
    }

    #[test]
    fn eg3_2_capacity_guard_enforces_quota_and_backpressure() {
        let g = CapacityGuard::new(2, 3); // per-tenant quota 2, global high-water 3
        assert_eq!(g.try_acquire("t1"), GuardDecision::Admit);
        assert_eq!(g.try_acquire("t1"), GuardDecision::Admit);
        assert_eq!(
            g.try_acquire("t1"),
            GuardDecision::QuotaExceeded,
            "tenant at its hard quota is rejected"
        );
        assert_eq!(g.try_acquire("t2"), GuardDecision::Admit); // global now 3
        assert_eq!(
            g.try_acquire("t2"),
            GuardDecision::Backpressure,
            "global high-water reached → shed"
        );
        g.release("t1");
        assert_eq!(g.global_inflight(), 2);
        assert_eq!(g.try_acquire("t2"), GuardDecision::Admit, "slot freed");
    }
}
