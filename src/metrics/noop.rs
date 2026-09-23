//! No-op shims for a build without the `metrics` feature: call sites stay
//! free of `#[cfg]` guards and the optimizer erases these.

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
pub fn cdc_kafka_sink_lag(_events: i64) {}
pub fn cdc_kafka_sink_send_failed() {}
pub fn write_batch_committed(_graph: &str, _ops: usize) {}
pub fn observe_write_lock_wait(_graph: &str, _seconds: f64) {}
pub fn observe_write_lock_hold(_graph: &str, _seconds: f64) {}
pub fn observe_dispatch_lock_wait(_mode: &str, _seconds: f64) {}
pub fn observe_commit_ops_phase(_phase: &str, _seconds: f64) {}
pub fn observe_commit_ops_bytes(_kind: &str, _bytes: u64) {}
pub fn storage_scrub_pass(_rows: u64, _causes: &[&str], _completed_cycle: bool) {}
pub fn projection_cache_hit() {}
pub fn projection_cache_miss(_seconds: f64) {}
pub fn loop_tick(_name: &str, _seconds: f64) {}
pub fn set_epistemic_materializations_stale(_n: i64) {}
pub fn epistemic_materializations_staled(_n: u64) {}
pub fn statechart_divergence(_machine: &str) {}
pub fn render() -> String {
    String::new()
}
