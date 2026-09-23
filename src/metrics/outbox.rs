//! Mutation-outbox consumer metrics (X10, PX10b item 9).
//!
//! Emitted by the consumers themselves from each claim outcome -- the kernel
//! has no metrics dependency. Labels are the consumer and topic only, never a
//! tenant or graph, so cardinality is bounded by the fixed set of consumers;
//! per-scope detail is `Method::MutationOutbox`'s `status`.

#[cfg(feature = "metrics")]
mod imp {
    use lazy_static::lazy_static;
    use prometheus::{IntCounterVec, IntGaugeVec};

    use super::super::imp::{counter_vec, gauge_vec};

    lazy_static! {
        static ref DEAD_LETTERED_TOTAL: IntCounterVec = counter_vec(
            "eg_outbox_dead_lettered_total",
            "Outbox rows a consumer's stream dead-lettered, by consumer, topic and cause",
            &["consumer", "topic", "cause"],
        );
        static ref HEAD_AGE_MS: IntGaugeVec = gauge_vec(
            "eg_outbox_head_age_ms",
            "Age of the oldest unresolved row at the head of a consumer's stream (0 when idle)",
            &["consumer", "topic"],
        );
        static ref HEAD_ATTEMPT: IntGaugeVec = gauge_vec(
            "eg_outbox_head_attempt",
            "Delivery attempts the head row of a consumer's stream has spent (0 when idle)",
            &["consumer", "topic"],
        );
    }

    /// Count `rows` dead-lettered for `cause` (`exhausted` by bounded retry,
    /// or the consumer's own reject reason).
    pub fn outbox_dead_lettered(consumer: &str, topic: &str, cause: &str, rows: u64) {
        DEAD_LETTERED_TOTAL
            .with_label_values(&[consumer, topic, cause])
            .inc_by(rows);
    }

    /// Publish where a consumer's stream head stands.
    pub fn set_outbox_head(consumer: &str, topic: &str, age_ms: u64, attempt: u32) {
        let age = i64::try_from(age_ms).unwrap_or(i64::MAX);
        HEAD_AGE_MS.with_label_values(&[consumer, topic]).set(age);
        HEAD_ATTEMPT
            .with_label_values(&[consumer, topic])
            .set(i64::from(attempt));
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    pub fn outbox_dead_lettered(_consumer: &str, _topic: &str, _cause: &str, _rows: u64) {}
    pub fn set_outbox_head(_consumer: &str, _topic: &str, _age_ms: u64, _attempt: u32) {}
}

pub use imp::*;
