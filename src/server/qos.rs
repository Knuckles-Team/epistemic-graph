//! CONCEPT:EG-KG.coordination.backpressure-busy-signal — engine-native QoS lanes (W2.4, generalizing EG-044).
//!
//! The transport's baseline admission (CONCEPT:EG-KG.backend.framed-response/EG-044 in `transport.rs`) is a
//! try-acquire over a global in-flight pool + a per-graph fairness cap + a reserved
//! read lane. It is FIFO-ish: once a permit is free ANY waiting request may take it,
//! regardless of who it belongs to or how urgent it is. Under a mixed workload — an
//! interactive MCP query racing an orchestration delegation racing a bulk-ingestion
//! firehose — that lets a low-value ingest tenant consume the slots a latency-sensitive
//! query needs (the exact noisy-neighbor SLO break the 2026-07-11 soak recorded).
//!
//! [`QosScheduler`] is an ADDITIVE, OPT-IN gate that runs BEFORE the baseline admission
//! (only when configured; see [`configured`]). It classifies each request into one of
//! four **admission classes** derived from the verified `eg2.` envelope's MAC-covered
//! **priority claim** (`RequestContextClaims::priority`, mapped by
//! [`QosClass::from_priority_claim`]) and keys per-**principal** accounting off the
//! verified opaque principal scope, then:
//!
//! * **Priority-ordered shedding (shed the LOWEST class first)** — each class has a
//!   headroom ceiling; `Interactive > Orch > Hydration > Ingest`. As load rises the
//!   lower classes hit their (lower) ceiling first and are shed (`Backpressure`) while
//!   headroom remains reserved for the higher classes. The queue is effectively
//!   reordered by class — an interactive request is admitted *before* an ingest one
//!   under contention, and an ingest flood is shed before it can touch the interactive
//!   reserve. Admission ordering, not FIFO.
//! * **Per-principal token buckets, per class** — a lazily-refilled `(principal, class)`
//!   token bucket bounds a single noisy principal's SUSTAINED rate *inside its class*,
//!   not just globally: a principal flooding `Ingest` drains its `Ingest` bucket and is
//!   shed `RateLimited` while its (untouched) `Interactive` bucket — and every other
//!   principal's buckets — stay full.
//! * **Per-principal fair-share** — under contention no single non-interactive principal
//!   may hold more than its `capacity / active_principals` share, so one greedy ingestion
//!   principal cannot monopolise the in-flight pool.
//! * **Per-principal hard quota** — a principal at its absolute in-flight quota is
//!   rejected (`Quota`) regardless of class, bounding blast radius / memory per principal.
//! * **Deadline-aware ordering** — [`plan_admissions`] deterministically orders a batch
//!   of pending requests by (class desc, deadline asc, arrival) for the scheduler seam.
//! * **Backpressure signalling** — a rejected request is shed with a typed
//!   [`QosReject`] the transport maps onto a `BUSY: …` response so clients retry with
//!   backoff instead of the server queueing unbounded work.
//!
//! Default behaviour is UNCHANGED: with QoS unconfigured ([`configured`] returns `None`)
//! the transport never calls this module and the baseline admission path is byte-for-byte
//! what it was. The admission decision ([`QosScheduler::try_admit`]) is a pure function of
//! the scheduler's live counters + the request + wall-clock, so priority/fair-share/
//! quota/token-bucket/backpressure are all deterministically testable.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

/// One admission class. `Ord` is derived so higher-priority classes sort GREATER
/// (`Interactive > Orch > Hydration > Ingest`) — [`plan_admissions`] relies on that
/// ordering, and each class maps to an admission-headroom ceiling in [`QosConfig`].
///
/// The four classes mirror agent-utilities' `PriorityClass` currency
/// (`CONCEPT:AU-ORCH.scheduling.resource-priority-edict`) 1:1, so the LLM admission
/// gate, the host worker reserved lane, and this engine gate all speak the same
/// vocabulary end to end.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QosClass {
    /// Background ingestion of documents / codebases / research papers: lowest
    /// priority, FIRST to be shed under pressure, the one class that yields.
    Ingest = 0,
    /// Initial skill + MCP hydration — foundational bootstrap. The edict's explicit
    /// "NOT deprioritised" exception: above ingest, below live orchestration.
    Hydration = 1,
    /// Orchestration / delegation (and any untagged context): high priority, never
    /// starved. The default when a request carries no priority claim.
    Orch = 2,
    /// Latency-sensitive interactive reads/queries (live Claude / end-user calls):
    /// highest priority, exempt from fair-share throttling.
    Interactive = 3,
}

impl QosClass {
    /// Every class, low → high — for metrics iteration and exhaustive sweeps.
    pub const ALL: [QosClass; 4] = [
        QosClass::Ingest,
        QosClass::Hydration,
        QosClass::Orch,
        QosClass::Interactive,
    ];

    /// Stable, bounded label for stats/metrics (bounded cardinality: exactly 4).
    pub fn label(self) -> &'static str {
        match self {
            QosClass::Ingest => "ingest",
            QosClass::Hydration => "hydration",
            QosClass::Orch => "orch",
            QosClass::Interactive => "interactive",
        }
    }

    fn idx(self) -> usize {
        self as usize
    }

    /// Map an `eg2.` envelope's priority claim (the agent-utilities `PriorityClass`
    /// wire value) to an admission class. An ABSENT or unrecognized claim resolves to
    /// [`QosClass::Orch`] — au's untagged-context semantics (high, never starved), so
    /// an un-upgraded client that sends no priority claim is treated as orchestration
    /// rather than deprioritised. Only an explicitly `background_ingestion`-tagged
    /// request lands in the yielding lane.
    pub fn from_priority_claim(claim: Option<&str>) -> QosClass {
        match claim.map(str::trim) {
            Some("interactive") => QosClass::Interactive,
            Some("orchestration") => QosClass::Orch,
            Some("hydration") => QosClass::Hydration,
            Some("background_ingestion") => QosClass::Ingest,
            _ => QosClass::Orch,
        }
    }
}

/// A request as the QoS scheduler sees it: its admission class, verified opaque
/// principal scope, and an optional relative deadline (lower micros ⇒ more urgent;
/// used only by [`plan_admissions`]).
#[derive(Clone, Debug)]
pub struct QosRequest {
    pub class: QosClass,
    /// The verified opaque principal scope — the unit of token-bucket / fair-share /
    /// quota accounting (`VerifiedRequestContext::principal_persistence_id`).
    pub principal: String,
    pub deadline_micros: Option<u64>,
}

/// Classify a verified wire request into a [`QosRequest`]. The caller supplies the
/// privacy-safe principal persistence scope derived from
/// [`crate::server::auth::VerifiedRequestContext`] and the request's MAC-covered
/// priority claim (`context.claims().priority`). Never the unsigned
/// `Request::agent_id` display field.
///
/// * **Principal** = the verified opaque principal scope — the accounting key. Missing
///   or blank identity is rejected before scheduler counters are touched; there is no
///   graph-name or empty fallback bucket.
/// * **Class**: derived from the priority claim by [`QosClass::from_priority_claim`].
pub fn classify(
    principal_scope: &str,
    priority_claim: Option<&str>,
) -> Result<QosRequest, &'static str> {
    let principal_scope = principal_scope.trim();
    if principal_scope.is_empty() {
        return Err("AUTHENTICATION_REQUIRED: verified principal is required for admission");
    }
    Ok(QosRequest {
        class: QosClass::from_priority_claim(priority_claim),
        principal: principal_scope.to_string(),
        deadline_micros: None,
    })
}

/// QoS scheduler configuration. Opt-in: [`configured`] returns `Some` only when
/// `EPISTEMIC_GRAPH_QOS` is truthy. All sizes derive from the same admission budget the
/// baseline path uses so the two layers agree on "capacity".
#[derive(Clone, Copy, Debug)]
pub struct QosConfig {
    /// Total concurrent admission budget the QoS gate manages (mirrors the baseline
    /// `max_in_flight` capacity).
    pub capacity: usize,
    /// Absolute per-principal in-flight quota. A principal already holding this many is
    /// rejected (`Quota`) regardless of class — bounds per-principal memory/blast-radius.
    pub per_principal_quota: usize,
    /// Slots at the top of the pool reserved for `Interactive` only.
    pub interactive_reserve: usize,
    /// Slots reserved above `Hydration` (for Interactive+Orch).
    pub orch_reserve: usize,
    /// Slots reserved above `Ingest` (for Interactive+Orch+Hydration).
    pub hydration_reserve: usize,
    /// Per-`(principal, class)` token-bucket burst capacity (max tokens). `0` disables
    /// the token bucket (leaving quota + ceiling + fair-share).
    pub bucket_capacity: f64,
    /// Per-`(principal, class)` sustained refill rate, tokens per second. `0` disables.
    pub bucket_refill_per_sec: f64,
}

impl QosConfig {
    /// Derive a balanced config from a capacity: a quarter of the pool is the
    /// per-principal in-flight quota (≤4 principals can fill it), and three equal
    /// eighth-of-pool reserve bands stack above Ingest / Hydration / Orch. All floored
    /// to 1 so a tiny pool still functions. The token bucket allows a per-principal
    /// burst of a quarter-pool and a sustained per-class rate of `capacity`/sec — a real
    /// throttle on a firehose, transparent to normal traffic.
    pub fn auto(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let reserve = (capacity / 8).max(1);
        QosConfig {
            capacity,
            per_principal_quota: (capacity / 4).max(1),
            interactive_reserve: reserve,
            orch_reserve: reserve,
            hydration_reserve: reserve,
            bucket_capacity: (capacity / 4).max(4) as f64,
            bucket_refill_per_sec: capacity as f64,
        }
    }

    /// The maximum in-flight count at (and above) which a request of `class` is shed as
    /// `Backpressure`. Higher-priority classes have a higher ceiling, so under rising load
    /// the LOW-priority classes are shed FIRST — leaving the reserved headroom for the
    /// high-priority ones (priority preemption without an explicit queue). Ingest gets the
    /// lowest ceiling (all three reserve bands sit above it), Interactive the full pool.
    pub fn class_ceiling(&self, class: QosClass) -> usize {
        match class {
            QosClass::Interactive => self.capacity,
            QosClass::Orch => self.capacity.saturating_sub(self.interactive_reserve),
            QosClass::Hydration => self
                .capacity
                .saturating_sub(self.interactive_reserve + self.orch_reserve),
            QosClass::Ingest => self.capacity.saturating_sub(
                self.interactive_reserve + self.orch_reserve + self.hydration_reserve,
            ),
        }
    }

    /// In-flight count at/above which the pool is deemed CONTENDED, so per-principal
    /// fair-share throttling engages (below it the gate is work-conserving — a principal
    /// may burst above its share when nobody else needs the slots). Set BELOW every class
    /// ceiling (past all three reserve bands) so a bulk principal is fair-share throttled
    /// while there is still ceiling headroom.
    pub fn pressure_band(&self) -> usize {
        self.capacity
            .saturating_sub(self.interactive_reserve + self.orch_reserve + self.hydration_reserve)
    }
}

/// Process-global QoS scheduler, built ONCE from the environment on first use. `None`
/// unless `EPISTEMIC_GRAPH_QOS` is truthy — then the transport skips this module entirely
/// and the baseline admission path is unchanged. Sizing:
///
/// * `EPISTEMIC_GRAPH_QOS` — `1`/`true`/`on`/`yes` to enable (default off).
/// * `EPISTEMIC_GRAPH_QOS_CAPACITY` — admission budget; defaults to the auto-sized
///   `Capacity::max_inflight()`.
/// * `EPISTEMIC_GRAPH_QOS_PRINCIPAL_QUOTA` — per-principal hard quota override.
pub fn configured() -> Option<Arc<QosScheduler>> {
    static QOS: OnceLock<Option<Arc<QosScheduler>>> = OnceLock::new();
    QOS.get_or_init(|| {
        if !env_truthy("EPISTEMIC_GRAPH_QOS") {
            return None;
        }
        let automatic_capacity = crate::autosize::detect_capacity().max_inflight();
        let capacity = std::env::var("EPISTEMIC_GRAPH_QOS_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .map(|value| crate::autosize::bound_explicit(value, automatic_capacity))
            .unwrap_or(automatic_capacity);
        let mut config = QosConfig::auto(capacity);
        if let Some(q) = std::env::var("EPISTEMIC_GRAPH_QOS_PRINCIPAL_QUOTA")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
        {
            config.per_principal_quota = crate::autosize::bound_explicit(q, config.capacity);
        }
        tracing::info!(
            target: "epistemic_graph::qos",
            capacity = config.capacity,
            per_principal_quota = config.per_principal_quota,
            interactive_reserve = config.interactive_reserve,
            orch_reserve = config.orch_reserve,
            hydration_reserve = config.hydration_reserve,
            "QoS admission scheduler enabled"
        );
        Some(Arc::new(QosScheduler::new(config)))
    })
    .clone()
}

fn env_truthy(key: &str) -> bool {
    std::env::var(key)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes"
            )
        })
        .unwrap_or(false)
}

/// Why a request was shed by the QoS gate. The transport maps each onto a distinct
/// `BUSY: …` message so a client can tell a priority/backpressure shed (retry with
/// backoff) from a quota breach (a persistent over-limit condition).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QosReject {
    /// The principal is at its absolute per-principal in-flight quota.
    Quota,
    /// Under contention the principal already holds its fair share of the pool.
    FairShare,
    /// The pool has no headroom left for this priority class (higher classes reserved it).
    Backpressure,
    /// The principal is sending faster than its per-class token bucket allows.
    RateLimited,
}

impl QosReject {
    /// Human-facing `BUSY` payload (retryable back-pressure signal).
    pub fn busy_message(self) -> &'static str {
        match self {
            QosReject::Quota => "BUSY: QoS per-principal quota exhausted, retry with backoff",
            QosReject::FairShare => {
                "BUSY: QoS principal fair-share reached under contention, retry with backoff"
            }
            QosReject::Backpressure => {
                "BUSY: QoS capacity reserved for higher priority, retry with backoff"
            }
            QosReject::RateLimited => {
                "BUSY: QoS per-principal rate limit for this class reached, retry with backoff"
            }
        }
    }

    /// Stable, bounded label for the shed-reason metric dimension.
    pub fn label(self) -> &'static str {
        match self {
            QosReject::Quota => "quota",
            QosReject::FairShare => "fair_share",
            QosReject::Backpressure => "backpressure",
            QosReject::RateLimited => "rate_limited",
        }
    }

    fn stat_idx(self) -> usize {
        match self {
            QosReject::Quota => 0,
            QosReject::FairShare => 1,
            QosReject::Backpressure => 2,
            QosReject::RateLimited => 3,
        }
    }
}

/// The outcome of [`QosScheduler::try_admit`]: an admitted [`QosPermit`] (held until the
/// dispatch completes, then dropped to release the slot) or a typed rejection.
pub enum QosDecision {
    Admit(QosPermit),
    Reject(QosReject),
}

/// A `(principal, class)` token bucket. Refilled lazily on each access from wall-clock
/// elapsed time; never touched on permit drop (it bounds RATE, not in-flight count).
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

/// Live scheduler counters shared between the scheduler and its outstanding permits.
struct QosInner {
    config: QosConfig,
    /// Total in-flight admitted requests.
    in_flight: AtomicUsize,
    /// Per-class in-flight, indexed by [`QosClass::idx`].
    per_class: [AtomicUsize; 4],
    /// Per-principal in-flight count. An entry is removed when it returns to zero so
    /// `active_principals` (map length) reflects only principals with live work.
    per_principal: DashMap<String, usize>,
    /// Per-`(principal, class)` token buckets. An entry set is removed when the
    /// principal's total in-flight returns to zero (see [`QosPermit::drop`]), so the map
    /// is bounded by the set of principals with LIVE work — a sustained flood keeps its
    /// bucket (and stays rate-limited); a drained principal frees it.
    buckets: DashMap<(String, QosClass), TokenBucket>,
    /// Cumulative admits / rejects (by [`QosReject::stat_idx`]) for observability + tests.
    admitted_total: AtomicU64,
    rejected_total: [AtomicU64; 4],
}

/// A snapshot of the scheduler's live counters (for observability + assertions).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QosStats {
    pub in_flight: usize,
    pub active_principals: usize,
    pub admitted_total: u64,
    pub quota_rejected: u64,
    pub fair_share_rejected: u64,
    pub backpressure_rejected: u64,
    pub rate_limited_rejected: u64,
}

/// Engine-native QoS admission scheduler (CONCEPT:EG-KG.coordination.backpressure-busy-signal, W2.4). Cheaply cloneable (an
/// `Arc` inside); every clone shares the same counters.
#[derive(Clone)]
pub struct QosScheduler {
    inner: Arc<QosInner>,
}

impl QosScheduler {
    pub fn new(config: QosConfig) -> Self {
        QosScheduler {
            inner: Arc::new(QosInner {
                config,
                in_flight: AtomicUsize::new(0),
                per_class: [
                    AtomicUsize::new(0),
                    AtomicUsize::new(0),
                    AtomicUsize::new(0),
                    AtomicUsize::new(0),
                ],
                per_principal: DashMap::new(),
                buckets: DashMap::new(),
                admitted_total: AtomicU64::new(0),
                rejected_total: [
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                ],
            }),
        }
    }

    pub fn config(&self) -> &QosConfig {
        &self.inner.config
    }

    /// Decide whether to admit `req`, applying (in order) the per-principal hard quota,
    /// the priority-class ceiling (shed-lowest-first backpressure), the per-principal
    /// fair share (non-interactive, under contention), and the per-`(principal, class)`
    /// token bucket. On admit the shared counters are incremented and a RAII
    /// [`QosPermit`] returned; dropping it releases the slot.
    ///
    /// Pure over the live counters + `req` + wall-clock, so every rule is testable.
    pub fn try_admit(&self, req: &QosRequest) -> QosDecision {
        let cfg = &self.inner.config;

        // Snapshot the principal's current in-flight (its DashMap entry, absent ⇒ 0).
        let principal_count = self
            .inner
            .per_principal
            .get(&req.principal)
            .map(|e| *e.value())
            .unwrap_or(0);

        // (1) Hard per-principal quota — any class. A principal at its ceiling is shed.
        if principal_count >= cfg.per_principal_quota {
            return self.reject(req, QosReject::Quota);
        }

        let in_flight = self.inner.in_flight.load(Ordering::Acquire);

        // (2) Priority-class ceiling — SHED THE LOWEST CLASS FIRST. A lower-priority
        // class hits its (lower) ceiling first, so it is shed while headroom remains for
        // the higher-priority classes: an Ingest flood is shed before it can consume the
        // slots reserved for Hydration/Orch/Interactive.
        if in_flight >= cfg.class_ceiling(req.class) {
            return self.reject(req, QosReject::Backpressure);
        }

        // (3) Per-principal fair share, ONLY under contention and ONLY for non-interactive
        // work (interactive is cheap + latency-critical, so it is never fair-share shed).
        // A bulk principal already holding its `capacity / active_principals` share is
        // shed so other principals keep making progress.
        if req.class != QosClass::Interactive && in_flight >= cfg.pressure_band() {
            let active = self.active_principals_including(&req.principal);
            let fair = (cfg.capacity / active.max(1)).max(1);
            if principal_count >= fair {
                return self.reject(req, QosReject::FairShare);
            }
        }

        // (4) Per-(principal, class) token bucket — bounds a single noisy principal's
        // SUSTAINED rate INSIDE its class. Checked last so a token is consumed only for
        // an otherwise-admissible request.
        if !self.take_token(&req.principal, req.class) {
            return self.reject(req, QosReject::RateLimited);
        }

        // Admit: bump the shared counters and hand back a permit that undoes them on drop.
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        self.inner.per_class[req.class.idx()].fetch_add(1, Ordering::AcqRel);
        *self
            .inner
            .per_principal
            .entry(req.principal.clone())
            .or_insert(0) += 1;
        self.inner.admitted_total.fetch_add(1, Ordering::Relaxed);
        tracing::trace!(
            target: "epistemic_graph::qos",
            class = req.class.label(),
            principal = req.principal.as_str(),
            in_flight = in_flight + 1,
            "QoS admitted"
        );
        QosDecision::Admit(QosPermit {
            inner: self.inner.clone(),
            principal: req.principal.clone(),
            class: req.class,
        })
    }

    /// Take one token from the `(principal, class)` bucket, refilling it first from the
    /// wall-clock elapsed since its last access. `true` ⇒ a token was available (admit);
    /// `false` ⇒ empty (shed `RateLimited`). A disabled bucket (`bucket_refill_per_sec`
    /// or `bucket_capacity` ≤ 0) always yields `true`.
    fn take_token(&self, principal: &str, class: QosClass) -> bool {
        let cfg = &self.inner.config;
        if cfg.bucket_refill_per_sec <= 0.0 || cfg.bucket_capacity <= 0.0 {
            return true;
        }
        let now = Instant::now();
        let mut entry = self
            .inner
            .buckets
            .entry((principal.to_string(), class))
            .or_insert_with(|| TokenBucket {
                tokens: cfg.bucket_capacity,
                last_refill: now,
            });
        let elapsed = now
            .saturating_duration_since(entry.last_refill)
            .as_secs_f64();
        entry.tokens =
            (entry.tokens + elapsed * cfg.bucket_refill_per_sec).min(cfg.bucket_capacity);
        entry.last_refill = now;
        if entry.tokens >= 1.0 {
            entry.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn reject(&self, req: &QosRequest, why: QosReject) -> QosDecision {
        self.inner.rejected_total[why.stat_idx()].fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "epistemic_graph::qos",
            class = req.class.label(),
            principal = req.principal.as_str(),
            reason = why.label(),
            in_flight = self.inner.in_flight.load(Ordering::Relaxed),
            "QoS shed"
        );
        QosDecision::Reject(why)
    }

    /// Number of DISTINCT principals with live work, counting `principal` even if it has
    /// none yet (it is about to be admitted). Used as the fair-share denominator.
    fn active_principals_including(&self, principal: &str) -> usize {
        let base = self.inner.per_principal.len();
        if self.inner.per_principal.contains_key(principal) {
            base.max(1)
        } else {
            base + 1
        }
    }

    /// Snapshot the live counters.
    pub fn stats(&self) -> QosStats {
        QosStats {
            in_flight: self.inner.in_flight.load(Ordering::Acquire),
            active_principals: self.inner.per_principal.len(),
            admitted_total: self.inner.admitted_total.load(Ordering::Relaxed),
            quota_rejected: self.inner.rejected_total[0].load(Ordering::Relaxed),
            fair_share_rejected: self.inner.rejected_total[1].load(Ordering::Relaxed),
            backpressure_rejected: self.inner.rejected_total[2].load(Ordering::Relaxed),
            rate_limited_rejected: self.inner.rejected_total[3].load(Ordering::Relaxed),
        }
    }
}

/// RAII admission permit. Held by the dispatch task for the life of the request; on drop
/// it releases the principal/class/global slots so the pool reflects only live work.
pub struct QosPermit {
    inner: Arc<QosInner>,
    principal: String,
    class: QosClass,
}

impl QosPermit {
    /// The admission class this permit was granted under (for per-class metrics).
    pub fn class(&self) -> QosClass {
        self.class
    }
}

impl Drop for QosPermit {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.inner.per_class[self.class.idx()].fetch_sub(1, Ordering::AcqRel);
        // Decrement the principal entry, removing it at zero so `active_principals` tracks
        // only principals with live work. When it reaches zero, also drop that principal's
        // token buckets so the bucket map stays bounded by the live-principal set.
        if let dashmap::mapref::entry::Entry::Occupied(mut e) =
            self.inner.per_principal.entry(self.principal.clone())
        {
            let v = e.get_mut();
            *v = v.saturating_sub(1);
            if *v == 0 {
                e.remove();
                for class in QosClass::ALL {
                    self.inner.buckets.remove(&(self.principal.clone(), class));
                }
            }
        }
    }
}

/// Deterministically order a batch of pending requests for admission and pick the top
/// `available` slots — the deadline-aware, priority-first scheduler seam (CONCEPT:EG-KG.coordination.backpressure-busy-signal).
///
/// Ordering key: **class descending** (Interactive before Orch before Hydration before
/// Ingest), then **deadline ascending** (an earlier / more-urgent deadline first; `None`
/// sorts last as least urgent), then **arrival index** (stable, so equal requests keep
/// FIFO order). Returns the admitted indices in admission order.
pub fn plan_admissions(pending: &[QosRequest], available: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..pending.len()).collect();
    let keep = available.min(idx.len());
    if keep == 0 {
        return Vec::new();
    }
    // Admission consumes only the winning prefix. Select it in expected O(N),
    // then sort those `available` rows for deterministic dispatch order instead
    // of sorting every queued request: O(N + A log A), O(1) scratch.
    if keep < idx.len() {
        idx.select_nth_unstable_by(keep, |&a, &b| admission_order(pending, a, b));
        idx.truncate(keep);
    }
    idx.sort_unstable_by(|&a, &b| admission_order(pending, a, b));
    idx
}

#[inline]
fn admission_order(pending: &[QosRequest], a: usize, b: usize) -> std::cmp::Ordering {
    // Higher class first.
    pending[b]
        .class
        .cmp(&pending[a].class)
        // Then earlier deadline first (None ⇒ u64::MAX ⇒ last).
        .then_with(|| deadline_key(&pending[a]).cmp(&deadline_key(&pending[b])))
        // Then arrival index. This total tiebreak makes partial selection produce
        // exactly the same winners as the former stable full sort.
        .then(a.cmp(&b))
}

fn deadline_key(r: &QosRequest) -> u64 {
    r.deadline_micros.unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64 as StdAtomicU64};
    use std::time::Duration;

    fn req(class: QosClass, principal: &str) -> QosRequest {
        QosRequest {
            class,
            principal: principal.to_string(),
            deadline_micros: None,
        }
    }

    /// A config whose token bucket is disabled, to isolate the quota / ceiling /
    /// fair-share rules in a test.
    fn no_bucket(capacity: usize) -> QosConfig {
        let mut cfg = QosConfig::auto(capacity);
        cfg.bucket_refill_per_sec = 0.0;
        cfg
    }

    fn permit(d: QosDecision) -> QosPermit {
        match d {
            QosDecision::Admit(p) => p,
            QosDecision::Reject(r) => panic!("expected Admit, got Reject({r:?})"),
        }
    }
    fn reject(d: QosDecision) -> QosReject {
        match d {
            QosDecision::Reject(r) => r,
            QosDecision::Admit(_) => panic!("expected Reject, got Admit"),
        }
    }

    // W2.4 — the priority claim maps to the four admission classes; absent/unknown ⇒ Orch.
    #[test]
    fn priority_claim_maps_to_admission_class() {
        assert_eq!(
            QosClass::from_priority_claim(Some("interactive")),
            QosClass::Interactive
        );
        assert_eq!(
            QosClass::from_priority_claim(Some("orchestration")),
            QosClass::Orch
        );
        assert_eq!(
            QosClass::from_priority_claim(Some("hydration")),
            QosClass::Hydration
        );
        assert_eq!(
            QosClass::from_priority_claim(Some("background_ingestion")),
            QosClass::Ingest
        );
        // Absent or unrecognized ⇒ orchestration default (au untagged semantics).
        assert_eq!(QosClass::from_priority_claim(None), QosClass::Orch);
        assert_eq!(QosClass::from_priority_claim(Some("bogus")), QosClass::Orch);
        // Ordering: Interactive > Orch > Hydration > Ingest.
        assert!(QosClass::Interactive > QosClass::Orch);
        assert!(QosClass::Orch > QosClass::Hydration);
        assert!(QosClass::Hydration > QosClass::Ingest);
    }

    #[test]
    fn classify_maps_principal_and_priority_claim() {
        let principal = "principal:sha256:0123456789abcdef";
        let r = classify(principal, Some("interactive")).unwrap();
        assert_eq!(r.class, QosClass::Interactive);
        assert_eq!(r.principal, principal);
        assert_eq!(
            classify(principal, Some("background_ingestion"))
                .unwrap()
                .class,
            QosClass::Ingest
        );
        // No priority claim ⇒ Orch.
        assert_eq!(classify(principal, None).unwrap().class, QosClass::Orch);
    }

    #[test]
    fn classify_rejects_missing_verified_principal() {
        for principal in ["", "   "] {
            assert_eq!(
                classify(principal, Some("interactive")).unwrap_err(),
                "AUTHENTICATION_REQUIRED: verified principal is required for admission"
            );
        }
    }

    // W2.4 — SHED THE LOWEST CLASS FIRST: under rising load Ingest is shed before
    // Hydration before Orch, while Interactive still admits into its reserved headroom.
    #[test]
    fn lowest_class_shed_first_under_pressure() {
        // capacity 8 ⇒ reserves 1/1/1 ⇒ ceilings: interactive 8, orch 7, hydration 6,
        // ingest 5; pressure_band 5.
        let cfg = no_bucket(8);
        let sched = QosScheduler::new(cfg);
        assert_eq!(sched.config().class_ceiling(QosClass::Ingest), 5);
        assert_eq!(sched.config().class_ceiling(QosClass::Hydration), 6);
        assert_eq!(sched.config().class_ceiling(QosClass::Orch), 7);
        assert_eq!(sched.config().class_ceiling(QosClass::Interactive), 8);

        // Fill to 5 in-flight across distinct principals so no single one trips quota /
        // fair-share (each holds exactly 1).
        let mut held = Vec::new();
        for i in 0..5 {
            held.push(permit(
                sched.try_admit(&req(QosClass::Interactive, &format!("p{i}"))),
            ));
        }
        assert_eq!(sched.stats().in_flight, 5);

        // At 5 in-flight: INGEST (ceiling 5) is shed first.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Ingest, "ingest"))),
            QosReject::Backpressure,
            "ingest is shed first once its reserved-below headroom is gone"
        );
        // HYDRATION (ceiling 6) still admits — one slot above ingest.
        let hy = permit(sched.try_admit(&req(QosClass::Hydration, "hydra")));
        assert_eq!(sched.stats().in_flight, 6);
        // Now at 6: hydration is also shed; orch (ceiling 7) still admits.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Hydration, "hydra2"))),
            QosReject::Backpressure
        );
        let orch = permit(sched.try_admit(&req(QosClass::Orch, "orch")));
        assert_eq!(sched.stats().in_flight, 7);
        // Now at 7: orch is shed; INTERACTIVE (ceiling 8) STILL admits — its reserve holds.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Orch, "orch2"))),
            QosReject::Backpressure
        );
        let hot = permit(sched.try_admit(&req(QosClass::Interactive, "hot")));
        assert_eq!(sched.stats().in_flight, 8);
        // Full: even interactive is shed (bounded).
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Interactive, "hot2"))),
            QosReject::Backpressure
        );
        drop((hy, orch, hot));
        drop(held);
        assert_eq!(sched.stats().in_flight, 0, "permits release on drop");
    }

    // W2.4 — deadline-aware, priority-first ordering across all four classes.
    #[test]
    fn plan_admissions_orders_by_priority_then_deadline() {
        let pending = vec![
            req_dl(QosClass::Ingest, "a", Some(10)),       // 0
            req_dl(QosClass::Interactive, "b", Some(500)), // 1
            req_dl(QosClass::Interactive, "c", Some(100)), // 2
            req_dl(QosClass::Hydration, "d", Some(1)),     // 3
            req_dl(QosClass::Orch, "e", Some(50)),         // 4
        ];
        // interactive (urgent-first) → orch → hydration → ingest, regardless of the tiny
        // hydration/ingest deadlines.
        assert_eq!(plan_admissions(&pending, 5), vec![2, 1, 4, 3, 0]);
        // Only 2 slots ⇒ the two interactive requests, most-urgent first.
        assert_eq!(plan_admissions(&pending, 2), vec![2, 1]);
        assert!(plan_admissions(&pending, 0).is_empty());
    }

    fn req_dl(class: QosClass, principal: &str, dl: Option<u64>) -> QosRequest {
        QosRequest {
            class,
            principal: principal.to_string(),
            deadline_micros: dl,
        }
    }

    // W2.4 — per-principal token bucket bounds a noisy principal INSIDE its class while a
    // DIFFERENT principal's same-class bucket, and the SAME principal's other-class
    // bucket, stay full.
    #[test]
    fn per_principal_per_class_token_bucket() {
        let mut cfg = QosConfig::auto(64);
        // A tiny, non-refilling-in-test bucket so we observe the burst boundary crisply:
        // 3-token burst, no meaningful refill within the test's microseconds.
        cfg.bucket_capacity = 3.0;
        cfg.bucket_refill_per_sec = 0.000001;
        let sched = QosScheduler::new(cfg);

        // "greedy" drains its Ingest bucket: 3 admits, then RateLimited.
        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(permit(sched.try_admit(&req(QosClass::Ingest, "greedy"))));
        }
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Ingest, "greedy"))),
            QosReject::RateLimited,
            "a principal over its per-class rate is shed RateLimited"
        );
        // The SAME principal's INTERACTIVE bucket is independent — still full.
        let _hot = permit(sched.try_admit(&req(QosClass::Interactive, "greedy")));
        // A DIFFERENT principal's INGEST bucket is independent — still full.
        let _other = permit(sched.try_admit(&req(QosClass::Ingest, "other")));
        assert_eq!(sched.stats().rate_limited_rejected, 1);
        drop(held);
    }

    // Token buckets are freed when a principal drains to zero in-flight (bounded map).
    #[test]
    fn token_buckets_freed_when_principal_drains() {
        let mut cfg = QosConfig::auto(64);
        cfg.bucket_capacity = 2.0;
        cfg.bucket_refill_per_sec = 0.000001;
        let sched = QosScheduler::new(cfg);
        let mut held = vec![permit(sched.try_admit(&req(QosClass::Ingest, "p")))];
        held.push(permit(sched.try_admit(&req(QosClass::Ingest, "p"))));
        // Bucket now created + drained to 0.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Ingest, "p"))),
            QosReject::RateLimited
        );
        drop(held); // principal p drains to zero in-flight ⇒ its buckets are freed.
                    // A fresh full bucket ⇒ admits again.
        let _fresh = permit(sched.try_admit(&req(QosClass::Ingest, "p")));
    }

    #[test]
    fn per_principal_fair_share_under_contention() {
        // capacity 16 ⇒ quota 4, reserves 2/2/2 ⇒ pressure_band 10, ingest ceiling 10.
        // Push in-flight to 10 via many distinct interactive principals (fair-share
        // exempt, each holds 1). Then a single ingest principal is fair-share bounded.
        let cfg = no_bucket(16);
        let sched = QosScheduler::new(cfg);
        let mut filler = Vec::new();
        for i in 0..10 {
            filler.push(permit(
                sched.try_admit(&req(QosClass::Interactive, &format!("f{i}"))),
            ));
        }
        assert!(sched.stats().in_flight >= sched.config().pressure_band());
        // Ingest ceiling is 10; at exactly 10 in-flight an ingest is shed Backpressure
        // BEFORE fair-share can apply. Use Orch (ceiling 14) so we observe FAIR-SHARE.
        assert_eq!(sched.config().class_ceiling(QosClass::Orch), 14);
        let _b1 = permit(sched.try_admit(&req(QosClass::Orch, "greedy")));
        // active_principals is large (10 fillers + greedy) ⇒ fair = 16/11 = 1. A SECOND
        // concurrent Orch for "greedy" (holds 1 ≥ 1) is shed FairShare.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Orch, "greedy"))),
            QosReject::FairShare
        );
        let _b2 = permit(sched.try_admit(&req(QosClass::Orch, "other")));
        drop(filler);
    }

    #[test]
    fn per_principal_quota_rejection() {
        let sched = QosScheduler::new(no_bucket(16)); // quota 4
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(permit(sched.try_admit(&req(QosClass::Interactive, "t"))));
        }
        assert_eq!(sched.stats().in_flight, 4, "well below capacity");
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Interactive, "t"))),
            QosReject::Quota,
            "principal at its hard quota is rejected even with global headroom"
        );
        drop(held.pop());
        let _ok = permit(sched.try_admit(&req(QosClass::Interactive, "t")));
    }

    #[test]
    fn default_no_qos_is_disabled() {
        assert!(
            std::env::var("EPISTEMIC_GRAPH_QOS").is_err(),
            "test env must not preset the QoS flag"
        );
        assert!(configured().is_none(), "QoS is opt-in; default is disabled");
    }

    #[test]
    fn every_reject_reason_has_a_busy_message_and_label() {
        for why in [
            QosReject::Quota,
            QosReject::FairShare,
            QosReject::Backpressure,
            QosReject::RateLimited,
        ] {
            assert!(why.busy_message().starts_with("BUSY:"));
            assert!(!why.label().is_empty());
        }
    }

    // ── W2.4 ACCEPTANCE: noisy-neighbor soak ────────────────────────────────────
    // The SLO the 2026-07-11 soak FAILED: interactive p99 must HOLD under a sustained
    // ingest flood. A sustained ingest firehose PINS the pool at the ingest ceiling
    // (held permits — real ingestion work already occupying the ingest lane), then a
    // steady INTERACTIVE client and a concurrent INGEST flood run against it. Because the
    // class-ceiling reserves headroom above ingest, interactive admits near-instantly
    // (tiny p99, ~0 sheds) while the flood is shed en masse. We assert the p99 RATIO
    // (interactive p99 ≪ ingest p99) and the shed counts, and log every number.
    #[test]
    fn noisy_neighbor_soak_interactive_p99_holds_under_ingest_flood() {
        // Deterministic pool. reserve = 32/8 = 4 ⇒ ingest ceiling 20, interactive ceiling
        // 32, pressure_band 20. Generous token buckets so the SLO under test is the
        // class-ceiling reservation (the shed-lowest-first mechanism), not the rate limiter.
        let mut cfg = QosConfig::auto(32);
        cfg.bucket_capacity = 1_000_000.0;
        cfg.bucket_refill_per_sec = 1_000_000.0;
        let sched = QosScheduler::new(cfg);
        let ceiling = sched.config().class_ceiling(QosClass::Ingest);
        assert_eq!(ceiling, 20);
        assert_eq!(sched.config().class_ceiling(QosClass::Interactive), 32);

        // ── Sustained ingest firehose: hold `ceiling` ingest permits across distinct
        // principals so the pool is pinned AT the ingest ceiling for the whole soak.
        // (Each principal holds exactly one, so neither the per-principal quota nor
        // fair-share trips while filling.) Any FURTHER ingest is now shed backpressure;
        // interactive still has its reserved headroom.
        let mut firehose = Vec::new();
        for i in 0..ceiling {
            firehose.push(permit(
                sched.try_admit(&req(QosClass::Ingest, &format!("firehose-{i}"))),
            ));
        }
        assert_eq!(sched.stats().in_flight, ceiling);
        // Sanity: an extra ingest is shed (backpressure) while interactive is admitted.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Ingest, "probe"))),
            QosReject::Backpressure
        );

        let samples = 500usize;
        let stop = Arc::new(AtomicBool::new(false));
        let ingest_flood_shed = Arc::new(StdAtomicU64::new(0));
        let ingest_flood_admitted = Arc::new(StdAtomicU64::new(0));

        // ── Concurrent ingest flood: 4 principals hammering the (saturated) ingest lane.
        let mut flood = Vec::new();
        for f in 0..4 {
            let sched = sched.clone();
            let stop = stop.clone();
            let shed = ingest_flood_shed.clone();
            let adm = ingest_flood_admitted.clone();
            flood.push(std::thread::spawn(move || {
                let principal = format!("flood-{f}");
                while !stop.load(Ordering::Relaxed) {
                    match sched.try_admit(&req(QosClass::Ingest, &principal)) {
                        QosDecision::Reject(_) => {
                            shed.fetch_add(1, Ordering::Relaxed);
                        }
                        QosDecision::Admit(p) => {
                            adm.fetch_add(1, Ordering::Relaxed);
                            drop(p);
                        }
                    }
                    std::thread::yield_now();
                }
            }));
        }

        let p99 = |mut v: Vec<u128>| -> u128 {
            if v.is_empty() {
                return 0;
            }
            v.sort_unstable();
            v[((v.len() as f64 * 0.99).ceil() as usize).min(v.len()) - 1]
        };

        // ── Interactive client (the workload whose p99 must hold): time each request's
        // full path to admission, retrying with a tiny backoff on the rare shed.
        let interactive = {
            let sched = sched.clone();
            std::thread::spawn(move || {
                let mut lat = Vec::with_capacity(samples);
                let mut shed = 0u64;
                for _ in 0..samples {
                    let start = Instant::now();
                    loop {
                        match sched.try_admit(&req(QosClass::Interactive, "interactive-client")) {
                            QosDecision::Admit(p) => {
                                lat.push(start.elapsed().as_micros());
                                std::thread::sleep(Duration::from_micros(30)); // fast read
                                drop(p);
                                break;
                            }
                            QosDecision::Reject(_) => {
                                shed += 1;
                                std::thread::sleep(Duration::from_micros(10));
                            }
                        }
                    }
                    std::thread::sleep(Duration::from_micros(50)); // steady, not a flood
                }
                (lat, shed)
            })
        };

        // ── An "insistent ingest" client measured on the SAME axis for the p99 RATIO: it
        // retries (bounded) to get served, so its time-to-serve reflects being shed
        // behind the firehose. Bounded retries so a fully-starved request records the
        // budget rather than hanging.
        let ingest_client = {
            let sched = sched.clone();
            std::thread::spawn(move || {
                let mut lat = Vec::with_capacity(samples);
                let mut shed_out = 0u64;
                for _ in 0..samples {
                    let start = Instant::now();
                    let mut admitted = false;
                    for _ in 0..64 {
                        match sched.try_admit(&req(QosClass::Ingest, "ingest-client")) {
                            QosDecision::Admit(p) => {
                                drop(p);
                                admitted = true;
                                break;
                            }
                            QosDecision::Reject(_) => {
                                std::thread::sleep(Duration::from_micros(10));
                            }
                        }
                    }
                    if !admitted {
                        shed_out += 1;
                    }
                    lat.push(start.elapsed().as_micros());
                    std::thread::sleep(Duration::from_micros(50));
                }
                (lat, shed_out)
            })
        };

        let (interactive_lat, interactive_shed) = interactive.join().unwrap();
        let (ingest_lat, ingest_shed_out) = ingest_client.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        for h in flood {
            let _ = h.join();
        }

        let interactive_p99 = p99(interactive_lat.clone());
        let interactive_max = interactive_lat.iter().copied().max().unwrap_or(0);
        let ingest_p99 = p99(ingest_lat.clone());
        let flood_shed = ingest_flood_shed.load(Ordering::Relaxed);
        let flood_adm = ingest_flood_admitted.load(Ordering::Relaxed);
        let stats = sched.stats();

        // Numbers logged for the record (visible with `--nocapture`).
        println!(
            "[W2.4 noisy-neighbor soak] pool cap=32 ingest_ceiling={ceiling} \
             pinned_firehose={ceiling}\n  interactive: samples={samples} shed={interactive_shed} \
             p99={interactive_p99}us max={interactive_max}us\n  ingest-client: samples={samples} \
             shed_out={ingest_shed_out} p99={ingest_p99}us\n  ingest-flood: shed={flood_shed} \
             admitted={flood_adm}\n  scheduler: admitted_total={} backpressure_shed={} \
             fair_share_shed={} rate_limited_shed={} quota_shed={}",
            stats.admitted_total,
            stats.backpressure_rejected,
            stats.fair_share_rejected,
            stats.rate_limited_rejected,
            stats.quota_rejected,
        );

        // ── The SLO assertions (the exact shape the 07-11 soak failed) ──────────────
        // 1) Interactive holds its p99 — near-instant admission even under the flood
        //    (admits are essentially free; the sleep is the simulated read, excluded).
        assert!(
            interactive_p99 < 5_000,
            "interactive p99 time-to-admit ({interactive_p99}us) exceeded the SLO under flood"
        );
        // 2) Interactive is essentially NEVER shed — the reserve above ingest holds.
        assert!(
            interactive_shed <= (samples as u64) / 50,
            "interactive was shed too often ({interactive_shed}/{samples}) — the reserve did not hold"
        );
        // 3) The p99 RATIO: interactive is served at least an order of magnitude faster
        //    than ingest, which is starved behind the firehose.
        assert!(
            ingest_p99 > interactive_p99.saturating_mul(10),
            "expected interactive p99 ({interactive_p99}us) ≪ ingest p99 ({ingest_p99}us)"
        );
        // 4) The ingest flood is shed en masse (the noisy neighbor is bounded).
        assert!(
            flood_shed > 1_000 && flood_shed > interactive_shed.saturating_mul(100).max(1_000),
            "expected the ingest flood to be shed far more than interactive \
             (flood_shed={flood_shed}, interactive_shed={interactive_shed})"
        );
        // 5) The class-ceiling shed-lowest-first path was actually exercised.
        assert!(
            stats.backpressure_rejected > 0,
            "the class-ceiling shed path was never exercised"
        );

        drop(firehose);
        assert_eq!(sched.stats().in_flight, 0, "all permits released");
    }
}
