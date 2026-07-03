//! CONCEPT:EG-320 — real-time QoS / SLO-aware admission scheduler.
//!
//! The transport's baseline admission (CONCEPT:EG-043/EG-044 in `transport.rs`) is a
//! try-acquire over a global in-flight pool + a per-graph fairness cap + a reserved
//! read lane. It is FIFO-ish: once a permit is free ANY waiting request may take it,
//! regardless of who it belongs to or how urgent it is. Under a mixed workload — an
//! interactive MCP query racing a bulk-ingestion firehose racing a maintenance sweep —
//! that lets a low-value bulk tenant consume the slots a latency-sensitive query needs.
//!
//! [`QosScheduler`] is an ADDITIVE, OPT-IN gate that runs BEFORE the baseline admission
//! (only when configured; see [`configured`]). It classifies each request by
//! **priority class** (interactive vs batch vs maintenance) and **tenant** (agent id, or
//! the target graph) and then:
//!
//! * **Priority-based admission / preemption** — each class has a headroom ceiling. A
//!   lower-priority class is shed (`Backpressure`) once free capacity drops into the band
//!   reserved for higher-priority work, so an interactive request is admitted *before* a
//!   batch/maintenance one under contention (the queue is effectively reordered by class).
//! * **Per-tenant fair-share** — under contention no single bulk tenant may hold more
//!   than its `capacity / active_tenants` share, so one greedy ingestion tenant cannot
//!   monopolise the pool. Interactive work is exempt (cheap + latency-critical).
//! * **Per-tenant hard quota** — a tenant at its absolute in-flight quota is rejected
//!   (`Quota`) regardless of class, bounding blast radius / memory per tenant.
//! * **Deadline-aware ordering** — [`plan_admissions`] deterministically orders a batch
//!   of pending requests by (class desc, deadline asc, arrival) for the scheduler seam.
//! * **Backpressure signalling** — a rejected request is shed with a typed
//!   [`QosReject`] the transport maps onto a `BUSY: …` response so clients retry with
//!   backoff instead of the server queueing unbounded work.
//!
//! Default behaviour is UNCHANGED: with QoS unconfigured ([`configured`] returns `None`)
//! the transport never calls this module and the baseline admission path is byte-for-byte
//! what it was. The admission decision ([`QosScheduler::try_admit`]) is a pure function of
//! the scheduler's live counters + the request, so priority/fair-share/quota/backpressure
//! are all deterministically unit-testable.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use crate::protocol::Method;

/// Priority class of a request. `Ord` is derived so higher-priority classes sort
/// GREATER (`Interactive > Batch > Maintenance`) — [`plan_admissions`] relies on that
/// ordering, and each class maps to an admission-headroom ceiling in [`QosConfig`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum QosClass {
    /// Background housekeeping (graph clears, full-graph dumps): lowest priority, first
    /// to be shed under pressure.
    Maintenance = 0,
    /// Bulk writes / ingestion: mid priority, throttled to a fair share under contention.
    Batch = 1,
    /// Latency-sensitive reads/queries (interactive MCP calls): highest priority, exempt
    /// from fair-share throttling.
    Interactive = 2,
}

impl QosClass {
    /// Stable, bounded label for stats/metrics.
    pub fn label(self) -> &'static str {
        match self {
            QosClass::Maintenance => "maintenance",
            QosClass::Batch => "batch",
            QosClass::Interactive => "interactive",
        }
    }
    fn idx(self) -> usize {
        self as usize
    }
}

/// A request as the QoS scheduler sees it: its priority class, owning tenant, an optional
/// relative deadline (lower micros ⇒ more urgent; used only by [`plan_admissions`]), and
/// whether it mutates the graph.
#[derive(Clone, Debug)]
pub struct QosRequest {
    pub class: QosClass,
    pub tenant: String,
    pub deadline_micros: Option<u64>,
    pub is_write: bool,
}

/// Classify a wire request into a [`QosRequest`] using ONLY fields already on the request
/// (no protocol change): the method shape, the target graph, and the caller identity.
///
/// * **Tenant** = the caller's `agent_id` when present, else the target graph name — the
///   unit of fair-share + quota accounting.
/// * **Class**: a graph clear or a full-graph dump (`GetNodes`, an expensive whole-graph
///   materialisation) is `Maintenance`; any other write is `Batch`; any other read is
///   `Interactive`. `ClearGraph`/`GetNodes` are base (non-feature-gated) variants, so this
///   classifier compiles on every server build.
pub fn classify(
    method: &Method,
    graph: &str,
    agent_id: Option<&str>,
    is_write: bool,
) -> QosRequest {
    let tenant = agent_id
        .filter(|a| !a.is_empty())
        .unwrap_or(graph)
        .to_string();
    let class = if matches!(method, Method::ClearGraph | Method::GetNodes) {
        QosClass::Maintenance
    } else if is_write {
        QosClass::Batch
    } else {
        QosClass::Interactive
    };
    QosRequest {
        class,
        tenant,
        deadline_micros: None,
        is_write,
    }
}

/// QoS scheduler configuration. Opt-in: [`configured`] returns `Some` only when
/// `EPISTEMIC_GRAPH_QOS` is truthy. All sizes derive from the same admission budget the
/// baseline path uses so the two layers agree on "capacity".
#[derive(Clone, Copy, Debug)]
pub struct QosConfig {
    /// Total concurrent admission budget the QoS gate manages (mirrors the baseline
    /// `max_in_flight` capacity).
    pub capacity: usize,
    /// Absolute per-tenant in-flight quota. A tenant already holding this many is
    /// rejected (`Quota`) regardless of class — bounds per-tenant memory/blast-radius.
    pub per_tenant_quota: usize,
    /// Slots at the top of the pool reserved for `Interactive` only. `Batch` is shed once
    /// free capacity would dip into this band.
    pub interactive_reserve: usize,
    /// Additional slots reserved above `Maintenance` (for Interactive+Batch). `Maintenance`
    /// is shed once free capacity would dip into `interactive_reserve + batch_reserve`.
    pub batch_reserve: usize,
}

impl QosConfig {
    /// Derive a balanced config from a capacity: a quarter of the pool is per-tenant
    /// quota (≤4 tenants can fill it), an eighth is interactive-only headroom, another
    /// eighth sits above maintenance. All floored to 1 so a tiny pool still functions.
    pub fn auto(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        QosConfig {
            capacity,
            per_tenant_quota: (capacity / 4).max(1),
            interactive_reserve: (capacity / 8).max(1),
            batch_reserve: (capacity / 8).max(1),
        }
    }

    /// The maximum in-flight count at (and above) which a request of `class` is shed as
    /// `Backpressure`. Higher-priority classes have a higher ceiling, so under rising load
    /// the low-priority classes are shed FIRST — leaving the reserved headroom for the
    /// high-priority ones (priority preemption without an explicit queue).
    pub fn class_ceiling(&self, class: QosClass) -> usize {
        match class {
            QosClass::Interactive => self.capacity,
            QosClass::Batch => self.capacity.saturating_sub(self.interactive_reserve),
            QosClass::Maintenance => self
                .capacity
                .saturating_sub(self.interactive_reserve + self.batch_reserve),
        }
    }

    /// In-flight count at/above which the pool is deemed CONTENDED, so per-tenant
    /// fair-share throttling engages (below it the gate is work-conserving — a tenant may
    /// burst above its share when nobody else needs the slots). Set BELOW every class
    /// ceiling (past both reserve bands) so a bulk tenant is fair-share throttled while
    /// there is still ceiling headroom, rather than only once its class ceiling is hit.
    pub fn pressure_band(&self) -> usize {
        self.capacity
            .saturating_sub(self.interactive_reserve + self.batch_reserve)
    }
}

/// Process-global QoS scheduler, built ONCE from the environment on first use. `None`
/// unless `EPISTEMIC_GRAPH_QOS` is truthy — then the transport skips this module entirely
/// and the baseline admission path is unchanged. Sizing:
///
/// * `EPISTEMIC_GRAPH_QOS` — `1`/`true`/`on`/`yes` to enable (default off).
/// * `EPISTEMIC_GRAPH_QOS_CAPACITY` — admission budget; defaults to the auto-sized
///   `Capacity::max_inflight()`.
/// * `EPISTEMIC_GRAPH_QOS_TENANT_QUOTA` — per-tenant hard quota override.
pub fn configured() -> Option<Arc<QosScheduler>> {
    static QOS: OnceLock<Option<Arc<QosScheduler>>> = OnceLock::new();
    QOS.get_or_init(|| {
        if !env_truthy("EPISTEMIC_GRAPH_QOS") {
            return None;
        }
        let capacity = std::env::var("EPISTEMIC_GRAPH_QOS_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or_else(|| crate::autosize::detect_capacity().max_inflight());
        let mut config = QosConfig::auto(capacity);
        if let Some(q) = std::env::var("EPISTEMIC_GRAPH_QOS_TENANT_QUOTA")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
        {
            config.per_tenant_quota = q;
        }
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
    /// The tenant is at its absolute per-tenant in-flight quota.
    Quota,
    /// Under contention the tenant already holds its fair share of the pool.
    FairShare,
    /// The pool has no headroom left for this priority class (higher classes reserved it).
    Backpressure,
}

impl QosReject {
    /// Human-facing `BUSY` payload (retryable back-pressure signal).
    pub fn busy_message(self) -> &'static str {
        match self {
            QosReject::Quota => "BUSY: QoS per-tenant quota exhausted, retry with backoff",
            QosReject::FairShare => {
                "BUSY: QoS tenant fair-share reached under contention, retry with backoff"
            }
            QosReject::Backpressure => {
                "BUSY: QoS capacity reserved for higher priority, retry with backoff"
            }
        }
    }
    fn stat_idx(self) -> usize {
        match self {
            QosReject::Quota => 0,
            QosReject::FairShare => 1,
            QosReject::Backpressure => 2,
        }
    }
}

/// The outcome of [`QosScheduler::try_admit`]: an admitted [`QosPermit`] (held until the
/// dispatch completes, then dropped to release the slot) or a typed rejection.
pub enum QosDecision {
    Admit(QosPermit),
    Reject(QosReject),
}

/// Live scheduler counters shared between the scheduler and its outstanding permits.
struct QosInner {
    config: QosConfig,
    /// Total in-flight admitted requests.
    in_flight: AtomicUsize,
    /// Per-class in-flight, indexed by [`QosClass::idx`].
    per_class: [AtomicUsize; 3],
    /// Per-tenant in-flight count. An entry is removed when it returns to zero so
    /// `active_tenants` (map length) reflects only tenants with live work.
    per_tenant: DashMap<String, usize>,
    /// Cumulative admits / rejects (by [`QosReject::stat_idx`]) for observability + tests.
    admitted_total: AtomicU64,
    rejected_total: [AtomicU64; 3],
}

/// A snapshot of the scheduler's live counters (for observability + assertions).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QosStats {
    pub in_flight: usize,
    pub active_tenants: usize,
    pub admitted_total: u64,
    pub quota_rejected: u64,
    pub fair_share_rejected: u64,
    pub backpressure_rejected: u64,
}

/// Real-time QoS/SLO-aware admission scheduler (CONCEPT:EG-320). Cheaply cloneable (an
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
                ],
                per_tenant: DashMap::new(),
                admitted_total: AtomicU64::new(0),
                rejected_total: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
            }),
        }
    }

    pub fn config(&self) -> &QosConfig {
        &self.inner.config
    }

    /// Decide whether to admit `req`, applying (in order) the per-tenant hard quota, the
    /// priority-class ceiling (preemption/backpressure), and — for non-interactive work
    /// under contention — the per-tenant fair share. On admit the shared counters are
    /// incremented and a RAII [`QosPermit`] returned; dropping it releases the slot.
    ///
    /// Pure over the live counters + `req`, so every rule is deterministically testable.
    pub fn try_admit(&self, req: &QosRequest) -> QosDecision {
        let cfg = &self.inner.config;

        // Snapshot the tenant's current in-flight (its DashMap entry, absent ⇒ 0).
        let tenant_count = self
            .inner
            .per_tenant
            .get(&req.tenant)
            .map(|e| *e.value())
            .unwrap_or(0);

        // (1) Hard per-tenant quota — any class. A tenant at its ceiling is shed.
        if tenant_count >= cfg.per_tenant_quota {
            return self.reject(QosReject::Quota);
        }

        let in_flight = self.inner.in_flight.load(Ordering::Acquire);

        // (2) Priority-class ceiling. A lower-priority class hits its (lower) ceiling
        // first, so it is shed while headroom remains for the higher-priority classes —
        // higher priority preempts the pool under load.
        if in_flight >= cfg.class_ceiling(req.class) {
            return self.reject(QosReject::Backpressure);
        }

        // (3) Per-tenant fair share, ONLY under contention and ONLY for non-interactive
        // work (interactive is cheap + latency-critical, so it is never fair-share shed).
        // A bulk tenant already holding its `capacity / active_tenants` share is shed so
        // other tenants keep making progress.
        if req.class != QosClass::Interactive && in_flight >= cfg.pressure_band() {
            let active = self.active_tenants_including(&req.tenant);
            let fair = (cfg.capacity / active.max(1)).max(1);
            if tenant_count >= fair {
                return self.reject(QosReject::FairShare);
            }
        }

        // Admit: bump the shared counters and hand back a permit that undoes them on drop.
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        self.inner.per_class[req.class.idx()].fetch_add(1, Ordering::AcqRel);
        *self.inner.per_tenant.entry(req.tenant.clone()).or_insert(0) += 1;
        self.inner.admitted_total.fetch_add(1, Ordering::Relaxed);
        QosDecision::Admit(QosPermit {
            inner: self.inner.clone(),
            tenant: req.tenant.clone(),
            class: req.class,
        })
    }

    fn reject(&self, why: QosReject) -> QosDecision {
        self.inner.rejected_total[why.stat_idx()].fetch_add(1, Ordering::Relaxed);
        QosDecision::Reject(why)
    }

    /// Number of DISTINCT tenants with live work, counting `tenant` even if it has none
    /// yet (it is about to be admitted). Used to compute the fair share denominator.
    fn active_tenants_including(&self, tenant: &str) -> usize {
        let base = self.inner.per_tenant.len();
        if self.inner.per_tenant.contains_key(tenant) {
            base.max(1)
        } else {
            base + 1
        }
    }

    /// Snapshot the live counters.
    pub fn stats(&self) -> QosStats {
        QosStats {
            in_flight: self.inner.in_flight.load(Ordering::Acquire),
            active_tenants: self.inner.per_tenant.len(),
            admitted_total: self.inner.admitted_total.load(Ordering::Relaxed),
            quota_rejected: self.inner.rejected_total[0].load(Ordering::Relaxed),
            fair_share_rejected: self.inner.rejected_total[1].load(Ordering::Relaxed),
            backpressure_rejected: self.inner.rejected_total[2].load(Ordering::Relaxed),
        }
    }
}

/// RAII admission permit. Held by the dispatch task for the life of the request; on drop
/// it releases the tenant/class/global slots so the pool reflects only live work.
pub struct QosPermit {
    inner: Arc<QosInner>,
    tenant: String,
    class: QosClass,
}

impl Drop for QosPermit {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.inner.per_class[self.class.idx()].fetch_sub(1, Ordering::AcqRel);
        // Decrement the tenant entry, removing it at zero so `active_tenants` tracks only
        // tenants with live work.
        if let dashmap::mapref::entry::Entry::Occupied(mut e) =
            self.inner.per_tenant.entry(self.tenant.clone())
        {
            let v = e.get_mut();
            *v = v.saturating_sub(1);
            if *v == 0 {
                e.remove();
            }
        }
    }
}

/// Deterministically order a batch of pending requests for admission and pick the top
/// `available` slots — the deadline-aware, priority-first scheduler seam (CONCEPT:EG-320).
///
/// Ordering key: **class descending** (Interactive before Batch before Maintenance), then
/// **deadline ascending** (an earlier / more-urgent deadline first; `None` sorts last as
/// least urgent), then **arrival index** (stable, so equal requests keep FIFO order).
/// Returns the admitted indices in admission order.
pub fn plan_admissions(pending: &[QosRequest], available: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..pending.len()).collect();
    idx.sort_by(|&a, &b| {
        // Higher class first.
        pending[b]
            .class
            .cmp(&pending[a].class)
            // Then earlier deadline first (None ⇒ u64::MAX ⇒ last).
            .then_with(|| deadline_key(&pending[a]).cmp(&deadline_key(&pending[b])))
            // Then stable by arrival index.
            .then(a.cmp(&b))
    });
    idx.truncate(available);
    idx
}

fn deadline_key(r: &QosRequest) -> u64 {
    r.deadline_micros.unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(class: QosClass, tenant: &str) -> QosRequest {
        QosRequest {
            class,
            tenant: tenant.to_string(),
            deadline_micros: None,
            is_write: class == QosClass::Batch,
        }
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

    // CONCEPT:EG-320 — classification derives class + tenant from existing request fields.
    #[test]
    fn eg320_classify_maps_method_and_identity_to_class_and_tenant() {
        // A read ⇒ Interactive; tenant falls back to the graph when no agent id.
        let r = classify(
            &Method::HasNode {
                node_id: "n".into(),
            },
            "g1",
            None,
            false,
        );
        assert_eq!(r.class, QosClass::Interactive);
        assert_eq!(r.tenant, "g1");
        // A write ⇒ Batch; agent id wins as the tenant.
        let w = classify(
            &Method::AddNode {
                node_id: "n".into(),
                properties_msgpack: vec![],
            },
            "g1",
            Some("agent:planner"),
            true,
        );
        assert_eq!(w.class, QosClass::Batch);
        assert_eq!(w.tenant, "agent:planner");
        // A graph clear / full dump ⇒ Maintenance.
        assert_eq!(
            classify(&Method::ClearGraph, "g1", None, true).class,
            QosClass::Maintenance
        );
        assert_eq!(
            classify(&Method::GetNodes, "g1", None, false).class,
            QosClass::Maintenance
        );
        // Empty agent id is ignored (treated as anonymous ⇒ graph tenant).
        assert_eq!(
            classify(
                &Method::HasNode {
                    node_id: "n".into()
                },
                "g1",
                Some(""),
                false
            )
            .tenant,
            "g1"
        );
    }

    // CONCEPT:EG-320 — under contention a high-priority (interactive) request is admitted
    // while a low-priority (batch/maintenance) one in the SAME saturated state is shed:
    // priority-based admission / preemption.
    #[test]
    fn eg320_high_priority_admitted_before_low_under_contention() {
        // capacity 8 ⇒ interactive_reserve 1, batch_reserve 1 ⇒
        // ceilings: interactive 8, batch 7, maintenance 6; pressure_band 7.
        let cfg = QosConfig::auto(8);
        let sched = QosScheduler::new(cfg);

        // Fill to 7 in-flight with a mix of tenants so no single tenant trips fair-share.
        let mut held = Vec::new();
        for i in 0..7 {
            held.push(permit(
                sched.try_admit(&req(QosClass::Interactive, &format!("t{i}"))),
            ));
        }
        assert_eq!(sched.stats().in_flight, 7);

        // At 7 in-flight: a BATCH request is at/above its ceiling (7) ⇒ shed Backpressure.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Batch, "batch-tenant"))),
            QosReject::Backpressure,
            "low-priority batch is shed once free capacity is reserved for interactive"
        );
        // A MAINTENANCE request (ceiling 6) is likewise shed.
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Maintenance, "maint-tenant"))),
            QosReject::Backpressure,
        );
        // But an INTERACTIVE request (ceiling 8) is STILL admitted — high priority wins.
        let hi = permit(sched.try_admit(&req(QosClass::Interactive, "hot")));
        assert_eq!(sched.stats().in_flight, 8);

        // Now full (8): even interactive is shed (bounded).
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Interactive, "hot2"))),
            QosReject::Backpressure,
        );
        drop(hi);
        drop(held);
        assert_eq!(
            sched.stats().in_flight,
            0,
            "permits release their slots on drop"
        );
    }

    // CONCEPT:EG-320 — deadline-aware, priority-first ordering of a pending batch.
    #[test]
    fn eg320_plan_admissions_orders_by_priority_then_deadline() {
        let pending = vec![
            QosRequest {
                class: QosClass::Batch,
                tenant: "a".into(),
                deadline_micros: Some(10),
                is_write: true,
            }, // 0
            QosRequest {
                class: QosClass::Interactive,
                tenant: "b".into(),
                deadline_micros: Some(500),
                is_write: false,
            }, // 1
            QosRequest {
                class: QosClass::Interactive,
                tenant: "c".into(),
                deadline_micros: Some(100),
                is_write: false,
            }, // 2
            QosRequest {
                class: QosClass::Maintenance,
                tenant: "d".into(),
                deadline_micros: Some(1),
                is_write: true,
            }, // 3
        ];
        // All four: interactive first (earlier deadline among them first), then batch,
        // then maintenance — regardless of maintenance's tiny deadline.
        assert_eq!(plan_admissions(&pending, 4), vec![2, 1, 0, 3]);
        // Only 2 slots ⇒ just the two interactive requests, most-urgent first.
        assert_eq!(plan_admissions(&pending, 2), vec![2, 1]);
        // Zero capacity ⇒ nothing admitted.
        assert!(plan_admissions(&pending, 0).is_empty());
    }

    // CONCEPT:EG-320 — per-tenant fair share: under contention one greedy bulk tenant
    // cannot exceed its capacity/active_tenants share while another tenant is competing.
    #[test]
    fn eg320_per_tenant_fair_share_under_contention() {
        // capacity 16 ⇒ quota 4, reserves 2/2 ⇒ pressure_band 12, batch ceiling 14. Keep
        // tenants under the hard quota so we observe the FAIR-SHARE rule, not the quota one.
        let cfg = QosConfig::auto(16);
        let sched = QosScheduler::new(cfg);

        // Push global in-flight into the contended band (>=12, still below the batch
        // ceiling 14) using many distinct interactive tenants (each holds 1, none near
        // quota, interactive is fair-share exempt so this only raises in_flight).
        let mut filler = Vec::new();
        for i in 0..12 {
            filler.push(permit(
                sched.try_admit(&req(QosClass::Interactive, &format!("f{i}"))),
            ));
        }
        assert!(sched.stats().in_flight >= sched.config().pressure_band());
        assert!(sched.stats().in_flight < sched.config().class_ceiling(QosClass::Batch));

        // Now two BATCH tenants compete. active_tenants is large (12 fillers + newcomer),
        // so fair = 16 / ~13 = 1. The FIRST batch admit for a tenant succeeds (held 0 < 1);
        // a SECOND concurrent one for the SAME tenant (held 1 >= 1) is shed FairShare
        // (in-flight is still below the batch ceiling, so this is fair-share, not backpressure).
        let _b1 = permit(sched.try_admit(&req(QosClass::Batch, "greedy")));
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Batch, "greedy"))),
            QosReject::FairShare,
            "a bulk tenant over its fair share is shed under contention",
        );
        // A DIFFERENT batch tenant still gets its first slot (fairness across tenants).
        let _b2 = permit(sched.try_admit(&req(QosClass::Batch, "other")));
        drop(filler);
    }

    // CONCEPT:EG-320 — per-tenant hard quota rejection (any class, regardless of load).
    #[test]
    fn eg320_per_tenant_quota_rejection() {
        // capacity 16 ⇒ quota 4. A single uncontended tenant may burst up to the quota,
        // then is rejected with Quota (NOT backpressure — plenty of global headroom).
        let sched = QosScheduler::new(QosConfig::auto(16));
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(permit(sched.try_admit(&req(QosClass::Interactive, "t"))));
        }
        assert_eq!(
            sched.stats().in_flight,
            4,
            "well below capacity — no backpressure"
        );
        assert_eq!(
            reject(sched.try_admit(&req(QosClass::Interactive, "t"))),
            QosReject::Quota,
            "tenant at its hard quota is rejected even with global headroom",
        );
        // Releasing one lets the tenant back in.
        drop(held.pop());
        let _ok = permit(sched.try_admit(&req(QosClass::Interactive, "t")));
    }

    // CONCEPT:EG-320 — with QoS unconfigured the scheduler seam is absent (default path
    // unchanged). `configured()` is None unless EPISTEMIC_GRAPH_QOS is truthy.
    #[test]
    fn eg320_default_no_qos_is_disabled() {
        // In the test process EPISTEMIC_GRAPH_QOS is unset ⇒ no scheduler ⇒ the transport
        // never gates on QoS and the baseline admission path is byte-for-byte unchanged.
        assert!(
            std::env::var("EPISTEMIC_GRAPH_QOS").is_err(),
            "test env must not preset the QoS flag",
        );
        assert!(configured().is_none(), "QoS is opt-in; default is disabled");
    }

    // CONCEPT:EG-320 — backpressure is signalled with a typed, retryable BUSY message.
    #[test]
    fn eg320_backpressure_is_signalled_at_capacity() {
        let sched = QosScheduler::new(QosConfig::auto(8));
        // Saturate with interactive across distinct tenants up to full capacity.
        let mut held = Vec::new();
        for i in 0..8 {
            held.push(permit(
                sched.try_admit(&req(QosClass::Interactive, &format!("t{i}"))),
            ));
        }
        let d = sched.try_admit(&req(QosClass::Interactive, "overflow"));
        let why = reject(d);
        assert_eq!(why, QosReject::Backpressure);
        assert!(
            why.busy_message().starts_with("BUSY:"),
            "overloaded requests get a retryable BUSY signal",
        );
        assert_eq!(sched.stats().backpressure_rejected, 1);
        drop(held);
    }
}
