// CONCEPT:EG-OS.observability.engine-metrics — Engine Observability (Prometheus metrics)
//
// First-class metrics for the Rust tier: request rate/latency per operation,
// in-flight admission state, per-graph activity and size, checkpoint health,
// and security counters (auth failures / ACL denials). Exposed in Prometheus
// text format by a tiny HTTP listener (`serve`) bound via `--metrics-addr` /
// `GRAPH_SERVICE_METRICS_ADDR` — fully separate from the MessagePack RPC
// transport, disabled unless an address is configured.
//
// The module compiles to no-op shims when the `metrics` cargo feature (on by
// default) is disabled, so call sites never need `#[cfg]` guards.
//
// Cardinality: `op` is bounded by the protocol's `Method` variants. The
// `graph` label is bounded by `MAX_GRAPH_LABELS` distinct names — graphs past
// the cap aggregate under `__overflow__` so an unbounded tenant namespace can
// never blow up the time-series store.

#[cfg(feature = "metrics")]
mod imp {
    use lazy_static::lazy_static;
    use parking_lot::Mutex;
    use prometheus::{
        Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
        IntGaugeVec, Opts, Registry, TextEncoder,
    };
    use std::collections::HashSet;

    /// UDS round-trips are sub-millisecond; analytics can run for seconds —
    /// span both regimes. The tail (60/120/300/600/1800/3600) keeps a request
    /// anywhere up to `EPISTEMIC_GRAPH_DISPATCH_DEADLINE_SECS`'s ceiling
    /// observable instead of collapsing into the `+Inf` catch-all — a 30s-max
    /// histogram could not distinguish an 82-request population that maxed
    /// out at 31s from one that maxed out at 590s (D-EIMG-7).
    const DURATION_BUCKETS: &[f64] = &[
        0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
        5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
    ];

    /// Distinct `graph` label values tracked before aggregating into
    /// `__overflow__` (deleting a graph frees its slot).
    const MAX_GRAPH_LABELS: usize = 128;
    const OVERFLOW_LABEL: &str = "__overflow__";

    fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
        let m = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
        let m = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    fn gauge(name: &str, help: &str) -> IntGauge {
        let m = IntGauge::new(name, help).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    fn counter(name: &str, help: &str) -> IntCounter {
        let m = IntCounter::new(name, help).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    // CONCEPT:EG-OS.observability.write-lock-gap-histogram — labelled-histogram helper for the write-lock-gap series.
    // Mirrors `counter_vec`/`gauge_vec` (register-on-build), with a caller-supplied
    // bucket layout so the lock-gap metrics can span sub-ms .. tens-of-seconds.
    fn histogram_vec(name: &str, help: &str, labels: &[&str], buckets: Vec<f64>) -> HistogramVec {
        let m = HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
            .expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    fn bounded_i64(value: u64) -> i64 {
        value.min(i64::MAX as u64) as i64
    }

    // CONCEPT:EG-OS.observability.write-lock-gap-histogram — write-lock-gap bucket layout. The starvation we measure
    // (semantic_search 0.02s idle → 14s under the `__commons__` firehose) spans
    // five orders of magnitude: an uncontended batch holds the topology lock for
    // tens of microseconds, while a starved writer can wait tens of seconds behind
    // the ingestion stream. An exponential ladder from 0.1ms to ~90s resolves both
    // the fast tail and the pathological wait without unbounded cardinality.
    fn lock_gap_buckets() -> Vec<f64> {
        prometheus::exponential_buckets(0.0001, 2.5, 16).expect("valid bucket spec")
    }

    lazy_static! {
        static ref REGISTRY: Registry = Registry::new();
        static ref SEEN_GRAPHS: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
        static ref REQUESTS_TOTAL: IntCounterVec = counter_vec(
            "epistemic_graph_requests_total",
            "Requests dispatched, by protocol operation",
            &["op"],
        );
        static ref REQUEST_DURATION: HistogramVec = {
            let m = HistogramVec::new(
                HistogramOpts::new(
                    "epistemic_graph_request_duration_seconds",
                    "Dispatch latency, by protocol operation",
                )
                .buckets(DURATION_BUCKETS.to_vec()),
                &["op"],
            )
            .expect("valid metric");
            REGISTRY
                .register(Box::new(m.clone()))
                .expect("unique metric");
            m
        };
        static ref IN_FLIGHT: IntGauge = gauge(
            "epistemic_graph_in_flight_requests",
            "Connection requests currently holding an admission permit",
        );
        static ref PERMITS_AVAILABLE: IntGauge = gauge(
            "epistemic_graph_inflight_permits_available",
            "Admission-semaphore permits still available \
             (EPISTEMIC_GRAPH_MAX_INFLIGHT minus in-flight; admission is \
             try-acquire, so nothing ever queues — excess load is shed as BUSY)",
        );
        static ref BUSY_REJECTIONS: IntCounter = counter(
            "epistemic_graph_busy_rejections_total",
            "Requests shed with BUSY because the admission semaphore was exhausted",
        );
        static ref DISPATCH_DEADLINE_EXCEEDED: IntCounter = counter(
            "epistemic_graph_dispatch_deadline_exceeded_total",
            "Requests abandoned because their dispatch exceeded the hard per-request \
             deadline (CONCEPT:EG-KG.coordination.backpressure-busy-signal). Non-zero means a \
             served request stalled long enough to strand its admission permits — the \
             failure mode that once left every permit held and shed ALL later traffic",
        );
        static ref READ_RESERVED_ADMITTED: IntCounter = counter(
            "epistemic_graph_read_reserved_admitted_total",
            "Read/query requests admitted via the RESERVED read lane (CONCEPT:EG-KG.coordination.reserved-read-lane) \
             after the global pool / per-graph cap was saturated by writes — each is an \
             interactive read that would otherwise have been shed BUSY behind ingestion",
        );
        // ── W2.4 engine-native QoS lanes (CONCEPT:EG-KG.coordination.backpressure-busy-signal) ──
        // Per-admission-class observability: the first-class "did the noisy-neighbor SLO
        // hold" telemetry. `class` ∈ {ingest,hydration,orch,interactive} (bounded, 4);
        // `reason` ∈ {quota,fair_share,backpressure,rate_limited} (bounded, 4). Zero unless
        // the QoS gate is enabled (`EPISTEMIC_GRAPH_QOS`).
        static ref QOS_ADMITTED: IntCounterVec = counter_vec(
            "epistemic_graph_qos_admitted_total",
            "Requests admitted by the QoS admission gate, by priority class (W2.4)",
            &["class"],
        );
        static ref QOS_SHED: IntCounterVec = counter_vec(
            "epistemic_graph_qos_shed_total",
            "Requests shed by the QoS admission gate, by priority class and shed reason \
             (W2.4). The lowest class is shed first under pressure",
            &["class", "reason"],
        );
        static ref QOS_CLASS_IN_FLIGHT: IntGaugeVec = gauge_vec(
            "epistemic_graph_qos_class_in_flight",
            "Requests currently holding a QoS admission permit, by priority class (W2.4) — \
             the per-class queue depth",
            &["class"],
        );
        static ref QOS_CLASS_LATENCY: HistogramVec = {
            let m = HistogramVec::new(
                HistogramOpts::new(
                    "epistemic_graph_qos_class_dispatch_seconds",
                    "End-to-end dispatch latency of a QoS-admitted request, by priority \
                     class (W2.4) — interactive p99 must hold under an ingest flood",
                )
                .buckets(DURATION_BUCKETS.to_vec()),
                &["class"],
            )
            .expect("valid metric");
            REGISTRY
                .register(Box::new(m.clone()))
                .expect("unique metric");
            m
        };
        static ref GRAPH_OPS: IntCounterVec = counter_vec(
            "epistemic_graph_graph_ops_total",
            "Graph-targeted operations admitted past the ACL, by graph (bounded cardinality)",
            &["graph"],
        );
        static ref GRAPH_NODES: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_nodes",
            "Node count per graph, updated on mutation",
            &["graph"],
        );
        static ref GRAPH_EDGES: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_edges",
            "Edge count per graph, updated on mutation",
            &["graph"],
        );
        static ref CHECKPOINT_DURATION: Histogram = {
            let m = Histogram::with_opts(
                HistogramOpts::new(
                    "epistemic_graph_checkpoint_duration_seconds",
                    "Wall-clock duration of a full registry checkpoint",
                )
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0]),
            )
            .expect("valid metric");
            REGISTRY
                .register(Box::new(m.clone()))
                .expect("unique metric");
            m
        };
        static ref CHECKPOINT_LAST_SUCCESS: IntGauge = gauge(
            "epistemic_graph_checkpoint_last_success_timestamp_seconds",
            "Unix timestamp of the last successful checkpoint",
        );
        static ref ANALYTICS_JOBS_READY: IntGauge = gauge(
            "epistemic_graph_analytics_jobs_ready",
            "Durable analytics jobs eligible for a remote worker lease",
        );
        static ref ANALYTICS_JOBS_ACTIVE: IntGauge = gauge(
            "epistemic_graph_analytics_jobs_active",
            "Durable analytics jobs with a currently live worker lease",
        );
        static ref ANALYTICS_JOBS_PUBLISHING: IntGauge = gauge(
            "epistemic_graph_analytics_jobs_publishing",
            "Durable analytics jobs with a staged result awaiting publication",
        );
        static ref AUTH_FAILURES: IntCounter = counter(
            "epistemic_graph_auth_failures_total",
            "Requests rejected by HMAC authentication",
        );
        static ref ACCESS_DENIED: IntCounter = counter(
            "epistemic_graph_access_denied_total",
            "Graph operations denied by the isolation ACL",
        );
        // Per-graph write-coalescer observability (CONCEPT:EG-KG.sharding.per-graph-write-coalescer). The win is
        // visible as ops/batch > 1: BATCHES counts topology-lock acquisitions on the
        // coalesced path, OPS counts the writes those acquisitions applied, so
        // (OPS / BATCHES) is the lock-acquisitions-saved ratio per graph.
        static ref WRITE_BATCHES: IntCounterVec = counter_vec(
            "epistemic_graph_write_batches_total",
            "Coalesced write batches committed (= topology-lock acquisitions on the \
             coalesced path), by graph",
            &["graph"],
        );
        static ref WRITE_BATCHED_OPS: IntCounterVec = counter_vec(
            "epistemic_graph_write_batched_ops_total",
            "Single-op writes applied via the coalescer, by graph. Divide by \
             write_batches_total for the average batch size (ops per lock acquisition)",
            &["graph"],
        );
        // ── Write-lock-gap histograms (CONCEPT:EG-OS.observability.write-lock-gap-histogram) ──
        // The contention this measures: under the `__commons__` ingestion firehose a
        // read (semantic_search) goes 0.02s idle → 14s, because writers serialize on
        // the per-graph topology write lock and a reader/late-writer queues behind the
        // batch in flight. WAIT is the starvation cost (enqueue → lock acquired); HOLD
        // is the contention cost (how long each batch keeps the lock). (HOLD × batch
        // rate) is the fraction of wall-clock the lock is unavailable to readers.
        static ref WRITE_LOCK_WAIT: HistogramVec = histogram_vec(
            "epistemic_graph_write_lock_wait_seconds",
            "Time a coalesced write waited from enqueue to acquiring the per-graph \
             topology write lock (the starvation cost), by graph",
            &["graph"],
            lock_gap_buckets(),
        );
        static ref WRITE_LOCK_HOLD: HistogramVec = histogram_vec(
            "epistemic_graph_write_lock_hold_seconds",
            "Time the per-graph topology write lock was held to apply one coalesced \
             batch (the contention cost — readers/other writers block for this), by graph",
            &["graph"],
            lock_gap_buckets(),
        );
        // ── Global dispatch state-lock wait (D-EIMG-2) ──
        // The two histograms above cover the PER-GRAPH topology lock, reached only via the
        // write coalescer. They say nothing about the single process-wide
        // `Arc<RwLock<ServerState>>` that EVERY dispatched method acquires before it can
        // do anything at all. That global lock is the suspected cause of the load-dependent
        // latency tail seen live (`HasNode` — a hash lookup — at a 3.992s mean; bimodal
        // `GetNodeProperties`/`GetNeighbors` with a small population of 0.25–30s calls
        // against a >99% sub-0.0001s body). Without this histogram that hypothesis cannot
        // be confirmed or refuted from the outside: per-method latency alone cannot
        // separate "the work is slow" from "the work waited to start".
        //
        // `mode` (read|write) is the ONLY label, deliberately: a `method` label would
        // multiply ~200 methods by 16 buckets on the hottest path in the process. Total
        // wait split by read/write is what distinguishes contention from expensive work,
        // which is the open question; per-method attribution can be layered on later if
        // this confirms contention.
        static ref DISPATCH_LOCK_WAIT: HistogramVec = histogram_vec(
            "epistemic_graph_dispatch_lock_wait_seconds",
            "Time a dispatched request blocked acquiring the process-wide ServerState \
             lock (the global dispatch serialization point), by acquisition mode",
            &["mode"],
            lock_gap_buckets(),
        );
        // ── RLS projection-cache hit/miss + miss cost (D-EGP-1, t1-grounding-0802) ──
        // `GraphReadAuthority::project_core` (`server/access.rs`) caches its expensive
        // per-(actor, GraphCore::version()) materialization in `rls_projection_cache`
        // (D-OP-1 / D-OB-20: cold miss measured 1.3-1.5s vs 7.57us warm — a ~190,000x
        // ratio). The lock-contention hypothesis for grounding's 40-56s compile latency
        // was REFUTED by `DISPATCH_LOCK_WAIT` (uncontended: 308 acquisitions totalled
        // 0.000155s). The prime remaining suspect is that the cache THRASHES under
        // interleaved reads/writes on a shared daemon — every committed write
        // invalidates the cache for every actor (the version bump), so a compile
        // spanning tens of seconds crosses several invalidation boundaries. There was
        // NO hit/miss telemetry to confirm or refute this; these two series are it.
        //
        // `result` (hit|miss) is the ONLY label — mirrors `DISPATCH_LOCK_WAIT`'s
        // `mode`-only cardinality discipline on the same hot path (every RLS-active
        // read goes through this). Per-actor/per-graph attribution is deliberately
        // NOT added here for the same reason.
        static ref PROJECTION_CACHE_RESULT: IntCounterVec = counter_vec(
            "epistemic_graph_projection_cache_result_total",
            "RLS projection-cache (project_core) lookups, by result: hit (fast path, \
             microseconds) or miss (cold/stale, triggers a full O(V log V + E log E + \
             V*d) rebuild)",
            &["result"],
        );
        static ref PROJECTION_CACHE_MISS_BUILD: Histogram = {
            let m = Histogram::with_opts(
                HistogramOpts::new(
                    "epistemic_graph_projection_cache_miss_build_seconds",
                    "Wall-clock cost of rebuilding the RLS projection on a cache miss \
                     (the O(V log V + E log E + V*d) materialization project_core's cache \
                     exists to amortize)",
                )
                .buckets(lock_gap_buckets()),
            )
            .expect("valid metric");
            REGISTRY
                .register(Box::new(m.clone()))
                .expect("unique metric");
            m
        };
        // ── Cost / efficiency autoscale signals (CONCEPT:EG-KG.compute.lane-v, Lane V) ──
        static ref GRAPH_MEMORY_BYTES: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_memory_bytes",
            "Approximate resident RAM per graph (node/edge property blobs + topology \
             overhead + embedding vectors), updated by the budget sweep",
            &["graph"],
        );
        static ref GRAPH_HIBERNATED: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_hibernated",
            "1 if the graph is hibernated (in-RAM state dropped, durable in redb), 0 if \
             resident — by graph",
            &["graph"],
        );
        // ResourceStats publishes these process-wide signals without a graph
        // label.  They are intentionally aggregate-only so a telemetry scan
        // cannot create a metric series for a graph the caller was not allowed
        // to enumerate.
        static ref EFFECTIVE_CPU_CORES: IntGauge = gauge(
            "epistemic_graph_effective_cpu_cores",
            "Effective cgroup-aware CPU lanes after automatic headroom",
        );
        static ref EFFECTIVE_MEMORY_LIMIT_BYTES: IntGauge = gauge(
            "epistemic_graph_effective_memory_limit_bytes",
            "Effective cgroup-aware RAM after automatic headroom",
        );
        static ref PROCESS_RSS_BYTES: IntGauge = gauge(
            "epistemic_graph_process_rss_bytes",
            "Current process RSS in bytes",
        );
        static ref COALESCER_QUEUE_DEPTH: IntGauge = gauge(
            "epistemic_graph_write_coalescer_queue_depth",
            "Structural writes waiting in bounded per-graph coalescer queues",
        );
        static ref COALESCER_QUEUE_BYTES: IntGauge = gauge(
            "epistemic_graph_write_coalescer_queue_bytes",
            "Approximate bytes held by bounded per-graph coalescer queues",
        );
        static ref COALESCER_OPERATIONS_TOTAL: IntGauge = gauge(
            "epistemic_graph_write_coalescer_operations_total",
            "Structural operations applied by the coalescer, including inline fallback",
        );
        static ref BUDGET_EVICTIONS: IntCounter = counter(
            "epistemic_graph_budget_evictions_total",
            "LRU nodes evicted by the per-tenant memory-budget enforcer (the eviction \
             rate an autoscaler watches: sustained eviction = memory pressure)",
        );
        static ref BUDGET_HIBERNATIONS: IntCounter = counter(
            "epistemic_graph_budget_hibernations_total",
            "Cold graphs hibernated (in-RAM state dropped) by the budget enforcer to keep \
             a tenant under its memory budget",
        );
        // CONCEPT:EG-OS.observability.slow-query-descriptor — slow-query counter. Increments once per query whose
        // end-to-end execution met/exceeded EPISTEMIC_GRAPH_SLOW_QUERY_MS; stays at
        // zero unless the slow-query threshold is configured.
        static ref SLOW_QUERIES: IntCounter = counter(
            "epistemic_graph_slow_query_total",
            "Queries whose end-to-end execution met/exceeded EPISTEMIC_GRAPH_SLOW_QUERY_MS \
             (CONCEPT:EG-OS.observability.slow-query-descriptor). Zero unless the slow-query threshold is configured",
        );
        // ── Background interval-loop observability (daemon-consolidation design, Phase 2) ──
        // The engine's own hardcoded `tokio::time::interval` sweeps (lake materialize,
        // checkpoint, decay, memory-cap, memory-budget, cold-tenant offload, txn-TTL) each
        // funnel their tick through ONE call (`loop_tick`) so they show up on the SAME
        // telemetry plane as everything else, without touching any loop's cadence or logic.
        // The `loop` label is a fixed, small set of hardcoded names (bounded cardinality —
        // no `graph_label`-style overflow guard needed).
        static ref LOOP_TICKS_TOTAL: IntCounterVec = counter_vec(
            "epistemic_graph_background_loop_ticks_total",
            "Ticks completed by an engine-native background interval loop, by loop name \
             (CONCEPT:EG-OS.observability.background-loop-telemetry)",
            &["loop"],
        );
        static ref LOOP_TICK_DURATION: HistogramVec = histogram_vec(
            "epistemic_graph_background_loop_tick_duration_seconds",
            "Wall-clock duration of one background interval-loop tick's own work \
             (excludes the sleep between ticks), by loop name",
            &["loop"],
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0],
        );
        static ref LOOP_LAST_RUN: IntGaugeVec = gauge_vec(
            "epistemic_graph_background_loop_last_run_timestamp_seconds",
            "Unix timestamp of the last completed tick, by loop name",
            &["loop"],
        );
        // CONCEPT:EG-KG.epistemic.truth-maintenance — the "give staleness a consumer"
        // SURPASS gap-closure: a live gauge (the CURRENT stale count, refreshed from
        // the durable per-graph reasoning projection after committed invalidations)
        // plus a cumulative counter (how many staling EVENTS have fired, distinct from
        // the gauge because a materialization can go stale, get recomputed back to
        // fresh, and go stale again — the counter never decreases, the gauge does).
        static ref EPISTEMIC_MATERIALIZATIONS_STALE: IntGauge = gauge(
            "epistemic_graph_epistemic_materializations_stale",
            "Current count of TruthMaintenance materializations in the Stale status \
             (CONCEPT:EG-KG.epistemic.truth-maintenance)",
        );
        static ref EPISTEMIC_MATERIALIZATIONS_STALED_TOTAL: IntCounter = counter(
            "epistemic_graph_epistemic_materializations_staled_total",
            "Cumulative count of materialization-id staling events fed through the \
             TruthMaintenance index's on_change (a ChangeEvent may stale several ids \
             at once; each staled id counts once per event)",
        );
        // ADR-5 (W2.2) statechart migration phase 1: the dual-write DIVERGENCE ALARM.
        // Incremented whenever a lifecycle statechart MIRROR disagrees with the
        // authoritative (redb-row / Python-enum) next state — the loud, queryable signal
        // that a chart definition is not yet a faithful replica and the authority flip
        // (phase 2) must not proceed. The `machine` label is a fixed small set
        // (`work_item`, `loop`) so the series is bounded.
        static ref STATECHART_DIVERGENCE_TOTAL: IntCounterVec = counter_vec(
            "epistemic_graph_statechart_divergence_total",
            "Count of lifecycle-statechart mirror transitions that diverged from the \
             authoritative next state during the ADR-5 phase-1 dual-write migration, by \
             machine (CONCEPT:INT-P2-2)",
            &["machine"],
        );
    }

    /// Map a graph name onto the bounded label space.
    fn graph_label(graph: &str) -> String {
        let mut seen = SEEN_GRAPHS.lock();
        if seen.contains(graph) {
            graph.to_string()
        } else if seen.len() < MAX_GRAPH_LABELS {
            seen.insert(graph.to_string());
            graph.to_string()
        } else {
            OVERFLOW_LABEL.to_string()
        }
    }

    pub fn record_request(op: &str, seconds: f64) {
        REQUESTS_TOTAL.with_label_values(&[op]).inc();
        REQUEST_DURATION.with_label_values(&[op]).observe(seconds);
    }

    pub fn connection_request_started(permits_available: usize) {
        IN_FLIGHT.inc();
        PERMITS_AVAILABLE.set(permits_available as i64);
    }

    pub fn connection_request_finished(permits_available: usize) {
        IN_FLIGHT.dec();
        PERMITS_AVAILABLE.set(permits_available as i64);
    }

    pub fn busy_rejected() {
        BUSY_REJECTIONS.inc();
    }

    pub fn read_reserved_admitted() {
        READ_RESERVED_ADMITTED.inc();
    }

    /// Record one dispatch abandoned at the hard per-request deadline
    /// (CONCEPT:EG-KG.coordination.backpressure-busy-signal). Every increment is one
    /// request whose admission permits would otherwise have been held forever.
    pub fn dispatch_deadline_exceeded() {
        DISPATCH_DEADLINE_EXCEEDED.inc();
    }

    /// Record one request admitted by the QoS gate in priority class `class` (W2.4):
    /// bumps the per-class admit counter and the per-class in-flight gauge (released by
    /// `qos_dispatch_finished`).
    pub fn qos_admitted(class: &str) {
        QOS_ADMITTED.with_label_values(&[class]).inc();
        QOS_CLASS_IN_FLIGHT.with_label_values(&[class]).inc();
    }

    /// Record one request shed by the QoS gate in priority class `class` for `reason`
    /// (W2.4). Under pressure the lowest class is shed first (`backpressure`).
    pub fn qos_shed(class: &str, reason: &str) {
        QOS_SHED.with_label_values(&[class, reason]).inc();
    }

    /// Record a QoS-admitted request finishing dispatch after `seconds` (W2.4): observes
    /// the per-class latency histogram and releases the per-class in-flight gauge.
    pub fn qos_dispatch_finished(class: &str, seconds: f64) {
        QOS_CLASS_LATENCY
            .with_label_values(&[class])
            .observe(seconds);
        QOS_CLASS_IN_FLIGHT.with_label_values(&[class]).dec();
    }

    pub fn graph_op(graph: &str) {
        GRAPH_OPS.with_label_values(&[&graph_label(graph)]).inc();
    }

    pub fn set_graph_size(graph: &str, nodes: i64, edges: i64) {
        let label = graph_label(graph);
        GRAPH_NODES.with_label_values(&[&label]).set(nodes);
        GRAPH_EDGES.with_label_values(&[&label]).set(edges);
    }

    /// Set the per-graph resident-memory estimate gauge (CONCEPT:EG-KG.compute.lane-v), in bytes.
    pub fn set_graph_memory(graph: &str, bytes: i64) {
        GRAPH_MEMORY_BYTES
            .with_label_values(&[&graph_label(graph)])
            .set(bytes);
    }

    /// Mark a graph hibernated (1) or resident (0) (CONCEPT:EG-KG.compute.lane-v).
    pub fn set_graph_hibernated(graph: &str, hibernated: bool) {
        GRAPH_HIBERNATED
            .with_label_values(&[&graph_label(graph)])
            .set(hibernated as i64);
    }

    /// Publish process-wide ResourceStats signals without introducing any
    /// caller-visible graph labels.  The values are deliberately gauges because
    /// they describe the current effective capacity/queue state; the operation
    /// total is sourced from the coalescer's monotonic atomic and refreshed on
    /// each snapshot.
    pub fn set_resource_stats(
        effective_cpu_cores: i64,
        effective_memory_limit_bytes: i64,
        process_rss_bytes: i64,
        coalescer_queue_depth: i64,
        coalescer_queue_bytes: i64,
        coalescer_operations_total: i64,
    ) {
        EFFECTIVE_CPU_CORES.set(effective_cpu_cores);
        EFFECTIVE_MEMORY_LIMIT_BYTES.set(effective_memory_limit_bytes);
        PROCESS_RSS_BYTES.set(process_rss_bytes);
        COALESCER_QUEUE_DEPTH.set(coalescer_queue_depth);
        COALESCER_QUEUE_BYTES.set(coalescer_queue_bytes);
        COALESCER_OPERATIONS_TOTAL.set(coalescer_operations_total);
    }

    pub fn set_coalescer_stats(queue_depth: u64, queue_bytes: u64, operations_total: u64) {
        COALESCER_QUEUE_DEPTH.set(bounded_i64(queue_depth));
        COALESCER_QUEUE_BYTES.set(bounded_i64(queue_bytes));
        COALESCER_OPERATIONS_TOTAL.set(bounded_i64(operations_total));
    }

    /// Record `n` LRU nodes evicted by the per-tenant budget enforcer (CONCEPT:EG-KG.compute.lane-v).
    pub fn budget_evicted(n: u64) {
        BUDGET_EVICTIONS.inc_by(n);
    }

    /// Record one graph hibernated by the budget enforcer (CONCEPT:EG-KG.compute.lane-v).
    pub fn budget_hibernated() {
        BUDGET_HIBERNATIONS.inc();
    }

    /// Record one query that crossed the slow-query threshold (CONCEPT:EG-OS.observability.slow-query-descriptor).
    pub fn slow_query() {
        SLOW_QUERIES.inc();
    }

    /// Forget a deleted graph: drop its series and free its label slot.
    pub fn drop_graph(graph: &str) {
        let _ = GRAPH_NODES.remove_label_values(&[graph]);
        let _ = GRAPH_EDGES.remove_label_values(&[graph]);
        let _ = GRAPH_OPS.remove_label_values(&[graph]);
        let _ = GRAPH_MEMORY_BYTES.remove_label_values(&[graph]);
        let _ = GRAPH_HIBERNATED.remove_label_values(&[graph]);
        let _ = WRITE_BATCHES.remove_label_values(&[graph]);
        let _ = WRITE_BATCHED_OPS.remove_label_values(&[graph]);
        let _ = WRITE_LOCK_WAIT.remove_label_values(&[graph]);
        let _ = WRITE_LOCK_HOLD.remove_label_values(&[graph]);

        SEEN_GRAPHS.lock().remove(graph);
        // The overflow bucket is deliberately a single bounded aggregate, not
        // a per-overflow-name set (which would itself grow to the million-graph
        // target).  Removing it on every graph delete is safe: any still-live
        // overflow graph recreates the one aggregate series on its next metric
        // event, while a same-name recreate cannot inherit stale state.
        let _ = GRAPH_NODES.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = GRAPH_EDGES.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = GRAPH_OPS.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = GRAPH_MEMORY_BYTES.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = GRAPH_HIBERNATED.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = WRITE_BATCHES.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = WRITE_BATCHED_OPS.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = WRITE_LOCK_WAIT.remove_label_values(&[OVERFLOW_LABEL]);
        let _ = WRITE_LOCK_HOLD.remove_label_values(&[OVERFLOW_LABEL]);
    }

    pub fn checkpoint_completed(seconds: f64) {
        CHECKPOINT_DURATION.observe(seconds);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        CHECKPOINT_LAST_SUCCESS.set(now as i64);
    }

    /// Publish authoritative job-store queue state for autoscaling and SLOs.
    pub fn set_analytics_job_counts(ready: i64, active: i64, publishing: i64) {
        ANALYTICS_JOBS_READY.set(ready);
        ANALYTICS_JOBS_ACTIVE.set(active);
        ANALYTICS_JOBS_PUBLISHING.set(publishing);
    }

    pub fn auth_failure() {
        AUTH_FAILURES.inc();
    }

    pub fn access_denied() {
        ACCESS_DENIED.inc();
    }

    /// Record one committed coalesced write batch of `ops` single-op writes against
    /// `graph` (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): +1 lock acquisition, +`ops` applied writes.
    pub fn write_batch_committed(graph: &str, ops: usize) {
        let label = graph_label(graph);
        WRITE_BATCHES.with_label_values(&[&label]).inc();
        WRITE_BATCHED_OPS
            .with_label_values(&[&label])
            .inc_by(ops as u64);
    }

    /// Observe the time one write waited from enqueue to acquiring the per-graph
    /// topology write lock (CONCEPT:EG-OS.observability.write-lock-gap-histogram) — the starvation cost a reader/late-writer
    /// pays behind the `__commons__` ingestion firehose.
    pub fn observe_write_lock_wait(graph: &str, seconds: f64) {
        WRITE_LOCK_WAIT
            .with_label_values(&[&graph_label(graph)])
            .observe(seconds);
    }

    /// Observe the time the per-graph topology write lock was held to apply one
    /// coalesced batch (CONCEPT:EG-OS.observability.write-lock-gap-histogram) — the contention cost; reads block for this.
    pub fn observe_write_lock_hold(graph: &str, seconds: f64) {
        WRITE_LOCK_HOLD
            .with_label_values(&[&graph_label(graph)])
            .observe(seconds);
    }

    /// Observe how long a dispatched request blocked acquiring the process-wide
    /// `ServerState` lock (D-EIMG-2). `mode` is `"read"` or `"write"`. Called from the
    /// `timed_read`/`timed_write` chokepoint helpers in `server::dispatch`, never
    /// inline at an individual call site.
    pub fn observe_dispatch_lock_wait(mode: &str, seconds: f64) {
        DISPATCH_LOCK_WAIT
            .with_label_values(&[mode])
            .observe(seconds);
    }

    /// Record a hit against the RLS projection cache (D-EGP-1): the actor's
    /// materialized `GraphCore` was reused, no rebuild needed. Called from
    /// `GraphReadAuthority::project_core_active` on the fast path.
    pub fn projection_cache_hit() {
        PROJECTION_CACHE_RESULT.with_label_values(&["hit"]).inc();
    }

    /// Record a miss against the RLS projection cache (D-EGP-1) and the wall-clock
    /// cost of the resulting rebuild, `seconds`. A miss is either cold (first read
    /// for this actor) or stale (a committed write advanced `GraphCore::version()`
    /// since the last projection for this actor). Called from
    /// `GraphReadAuthority::project_core_active` after `build_projection` returns.
    pub fn projection_cache_miss(seconds: f64) {
        PROJECTION_CACHE_RESULT.with_label_values(&["miss"]).inc();
        PROJECTION_CACHE_MISS_BUILD.observe(seconds);
    }

    /// Record one completed tick of an engine-native background interval loop
    /// (daemon-consolidation design, Phase 2): `name` is a fixed loop identifier
    /// (e.g. `"cold_offload"`), `seconds` is the tick's own work duration (the sweep
    /// body only, not the sleep between ticks). Pure observability — call this AFTER
    /// a tick's existing logic runs, with zero change to what that logic does.
    pub fn loop_tick(name: &str, seconds: f64) {
        LOOP_TICKS_TOTAL.with_label_values(&[name]).inc();
        LOOP_TICK_DURATION
            .with_label_values(&[name])
            .observe(seconds);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        LOOP_LAST_RUN.with_label_values(&[name]).set(now as i64);
    }

    /// Set the `epistemic_graph_epistemic_materializations_stale` gauge to the
    /// TruthMaintenance index's CURRENT stale count (CONCEPT:EG-KG.epistemic.truth-maintenance).
    pub fn set_epistemic_materializations_stale(n: i64) {
        EPISTEMIC_MATERIALIZATIONS_STALE.set(n);
    }

    /// Record `n` materialization ids staled by one `TruthMaintenance::on_change` call.
    pub fn epistemic_materializations_staled(n: u64) {
        EPISTEMIC_MATERIALIZATIONS_STALED_TOTAL.inc_by(n);
    }

    /// Record one lifecycle-statechart mirror divergence (ADR-5 phase-1 dual-write).
    /// `machine` is a fixed small label (`"work_item"` / `"loop"`).
    pub fn statechart_divergence(machine: &str) {
        STATECHART_DIVERGENCE_TOTAL
            .with_label_values(&[machine])
            .inc();
    }

    /// Render the full registry in Prometheus text exposition format.
    pub fn render() -> String {
        // Ensure the process-wide autoscale families are registered even before
        // the first ResourceStats RPC or coalescer event. Values remain zero
        // until the authoritative telemetry path refreshes them.
        let _ = (
            &*EFFECTIVE_CPU_CORES,
            &*EFFECTIVE_MEMORY_LIMIT_BYTES,
            &*PROCESS_RSS_BYTES,
            &*COALESCER_QUEUE_DEPTH,
            &*COALESCER_QUEUE_BYTES,
            &*COALESCER_OPERATIONS_TOTAL,
        );
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        if encoder.encode(&REGISTRY.gather(), &mut buf).is_err() {
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }
}

#[cfg(feature = "metrics")]
pub use imp::*;

// ── No-op shims (feature `metrics` disabled) ─────────────────────────────
// Call sites stay free of #[cfg] guards; the optimizer erases these.
#[cfg(not(feature = "metrics"))]
mod imp {
    pub fn record_request(_op: &str, _seconds: f64) {}
    pub fn connection_request_started(_permits_available: usize) {}
    pub fn connection_request_finished(_permits_available: usize) {}
    pub fn busy_rejected() {}
    pub fn read_reserved_admitted() {}
    pub fn dispatch_deadline_exceeded() {}
    pub fn qos_admitted(_class: &str) {}
    pub fn qos_shed(_class: &str, _reason: &str) {}
    pub fn qos_dispatch_finished(_class: &str, _seconds: f64) {}
    pub fn graph_op(_graph: &str) {}
    pub fn set_graph_size(_graph: &str, _nodes: i64, _edges: i64) {}
    pub fn set_graph_memory(_graph: &str, _bytes: i64) {}
    pub fn set_graph_hibernated(_graph: &str, _hibernated: bool) {}
    pub fn set_resource_stats(
        _effective_cpu_cores: i64,
        _effective_memory_limit_bytes: i64,
        _process_rss_bytes: i64,
        _coalescer_queue_depth: i64,
        _coalescer_queue_bytes: i64,
        _coalescer_operations_total: i64,
    ) {
    }
    pub fn set_coalescer_stats(_queue_depth: u64, _queue_bytes: u64, _operations_total: u64) {}
    pub fn budget_evicted(_n: u64) {}
    pub fn budget_hibernated() {}
    pub fn slow_query() {}
    pub fn drop_graph(_graph: &str) {}
    pub fn checkpoint_completed(_seconds: f64) {}
    pub fn set_analytics_job_counts(_ready: i64, _active: i64, _publishing: i64) {}
    pub fn auth_failure() {}
    pub fn access_denied() {}
    pub fn write_batch_committed(_graph: &str, _ops: usize) {}
    pub fn observe_write_lock_wait(_graph: &str, _seconds: f64) {}
    pub fn observe_write_lock_hold(_graph: &str, _seconds: f64) {}
    pub fn observe_dispatch_lock_wait(_mode: &str, _seconds: f64) {}
    pub fn projection_cache_hit() {}
    pub fn projection_cache_miss(_seconds: f64) {}
    pub fn loop_tick(_name: &str, _seconds: f64) {}
    pub fn set_epistemic_materializations_stale(_n: i64) {}
    pub fn epistemic_materializations_staled(_n: u64) {}
    pub fn statechart_divergence(_machine: &str) {}
    pub fn render() -> String {
        String::new()
    }
}

#[cfg(not(feature = "metrics"))]
pub use imp::*;

/// Minimal HTTP/1.1 exposition endpoint. Every request to the bound address
/// (whatever the path) receives the full registry — keeps the listener free
/// of an HTTP-framework dependency and completely separate from the
/// length-prefixed MessagePack RPC ports.
#[cfg(all(feature = "metrics", feature = "server"))]
pub async fn serve(listener: tokio::net::TcpListener) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            // Drain (up to one read of) the request head; the path is
            // irrelevant for a single-purpose exposition endpoint.
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let body = render();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;

    // The Prometheus REGISTRY and the bounded graph-label cap (SEEN_GRAPHS) are
    // process-global singletons. Tests that depend on the labelled-series set must
    // not run concurrently with the cardinality test that saturates the 128-slot
    // cap (which would aggregate their graph into `__overflow__` and drop the
    // labelled needle). Serialize those tests on this lock.
    static GLOBAL_LABEL_STATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_render_contains_recorded_series() {
        let _guard = GLOBAL_LABEL_STATE.lock().unwrap_or_else(|e| e.into_inner());
        record_request("Ping", 0.0007);
        graph_op("agent:metrics-test");
        set_graph_size("agent:metrics-test", 3, 2);
        auth_failure();
        access_denied();
        busy_rejected();
        checkpoint_completed(0.02);
        // CONCEPT:EG-OS.observability.write-lock-gap-histogram — write-lock-gap histograms register + render.
        observe_write_lock_wait("agent:metrics-test", 0.003);
        observe_write_lock_hold("agent:metrics-test", 0.0005);
        set_resource_stats(2, 1024, 2048, 3, 256, 10);

        let out = render();
        // Note: the per-graph label series (graph_ops_total{graph=...}, graph_nodes,
        // graph_edges) are NOT asserted by exact label here — the graph-label space is
        // a process-global bounded cap (SEEN_GRAPHS, 128) that concurrent server
        // dispatch tests also populate via graph_op(), so a specific label can spill to
        // `__overflow__` under parallel load. The cap/overflow behavior is covered by
        // test_graph_label_cardinality_is_bounded; here we only assert the metric
        // FAMILIES are registered + rendered (robust to the shared global registry).
        for needle in [
            "epistemic_graph_requests_total{op=\"Ping\"}",
            "epistemic_graph_request_duration_seconds_bucket{op=\"Ping\"",
            "epistemic_graph_graph_ops_total",
            "epistemic_graph_graph_nodes",
            "epistemic_graph_graph_edges",
            "epistemic_graph_auth_failures_total",
            "epistemic_graph_access_denied_total",
            "epistemic_graph_busy_rejections_total",
            "epistemic_graph_checkpoint_duration_seconds_count",
            "epistemic_graph_checkpoint_last_success_timestamp_seconds",
            "epistemic_graph_write_lock_wait_seconds_bucket",
            "epistemic_graph_write_lock_hold_seconds_bucket",
            "epistemic_graph_effective_cpu_cores",
            "epistemic_graph_effective_memory_limit_bytes",
            "epistemic_graph_process_rss_bytes",
            "epistemic_graph_write_coalescer_queue_depth",
            "epistemic_graph_write_coalescer_queue_bytes",
            "epistemic_graph_write_coalescer_operations_total",
        ] {
            assert!(out.contains(needle), "missing {needle} in:\n{out}");
        }
    }

    #[test]
    fn test_graph_label_cardinality_is_bounded() {
        let _guard = GLOBAL_LABEL_STATE.lock().unwrap_or_else(|e| e.into_inner());
        // Saturate the label space, then confirm new names aggregate.
        for i in 0..200 {
            graph_op(&format!("agent:cardinality-{i}"));
        }
        let out = render();
        assert!(
            out.contains("epistemic_graph_graph_ops_total{graph=\"__overflow__\"}"),
            "expected overflow aggregation past the label cap"
        );
        // Deleting frees the slot and removes the series.
        drop_graph("agent:cardinality-0");
        assert!(!render().contains("graph=\"agent:cardinality-0\""));

        // Deleting any graph clears the shared overflow series. Recreating a
        // name after all tracked slots are freed must acquire a fresh bounded
        // raw slot rather than inheriting stale `__overflow__` state.
        for i in 1..200 {
            drop_graph(&format!("agent:cardinality-{i}"));
        }
        assert!(
            !render().contains("graph=\"__overflow__\""),
            "last deleted overflow owner must clear every overflow series"
        );
        graph_op("agent:cardinality-recreated");
        let recreated = render();
        assert!(
            recreated.contains("graph=\"agent:cardinality-recreated\""),
            "a recreated graph should receive a fresh bounded label slot"
        );
    }

    #[cfg(feature = "server")]
    #[tokio::test]
    async fn test_http_exposition_endpoint() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        record_request("Health", 0.001);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener).await });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        let text = String::from_utf8_lossy(&out);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("epistemic_graph_requests_total"));
    }
}
