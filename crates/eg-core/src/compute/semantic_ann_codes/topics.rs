//! The per-class stage-intent topics and consumers (X10, PX10b item 6; D3).
//!
//! Stage intents used to share ONE topic, claimed by one consumer per worker
//! that released every row of the wrong queue class. Every such release still
//! counted against the head row, so sixteen Medium polls that met a Fast row
//! dead-lettered it before a Fast worker could take it, and a Fast row at the
//! head blocked every Medium acknowledgement. Stage order is causal (a
//! successor intent is published only when its predecessor completes), so the
//! kernel's total order across classes bought nothing and cost both defects.
//!
//! Each class now has its own topic and each worker one consumer per class,
//! `<worker>#<class>`: a class can neither block nor dead-letter another's rows.

use eg_types::semantic_index::SemanticQueueClass;

/// Every queue class, in scheduling order.
pub const SEMANTIC_QUEUE_CLASSES: [SemanticQueueClass; 3] = [
    SemanticQueueClass::Fast,
    SemanticQueueClass::Medium,
    SemanticQueueClass::SlowHeavy,
];

/// The stage-intent topic of one queue class. Its payload is canonical
/// `semantic-stage-intent/v1`; the key is the canonical intent digest.
pub fn stage_intent_topic(class: SemanticQueueClass) -> &'static str {
    match class {
        SemanticQueueClass::Fast => "engine.semantic-index.stage-intent.fast.v1",
        SemanticQueueClass::Medium => "engine.semantic-index.stage-intent.medium.v1",
        SemanticQueueClass::SlowHeavy => "engine.semantic-index.stage-intent.slow-heavy.v1",
    }
}

/// Whether `topic` is one of the stage-intent topics.
pub fn is_stage_intent_topic(topic: &str) -> bool {
    SEMANTIC_QUEUE_CLASSES
        .iter()
        .any(|class| stage_intent_topic(*class) == topic)
}

fn class_token(class: SemanticQueueClass) -> &'static str {
    match class {
        SemanticQueueClass::Fast => "fast",
        SemanticQueueClass::Medium => "medium",
        SemanticQueueClass::SlowHeavy => "slow-heavy",
    }
}

/// The durable consumer one worker claims one class under.
pub fn stage_consumer(worker: &str, class: SemanticQueueClass) -> String {
    format!("{worker}#{}", class_token(class))
}

/// The worker and class a class consumer name was built from.
pub fn stage_consumer_parts(consumer: &str) -> Option<(&str, SemanticQueueClass)> {
    let (worker, token) = consumer.rsplit_once('#')?;
    SEMANTIC_QUEUE_CLASSES
        .iter()
        .find(|class| class_token(**class) == token)
        .map(|class| (worker, *class))
        .filter(|(worker, _)| !worker.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_class_has_its_own_topic_and_consumer_that_round_trips() {
        for class in SEMANTIC_QUEUE_CLASSES {
            assert!(is_stage_intent_topic(stage_intent_topic(class)));
            let consumer = stage_consumer("worker-a", class);
            assert_eq!(stage_consumer_parts(&consumer), Some(("worker-a", class)));
        }
        assert!(!is_stage_intent_topic(
            "engine.semantic-index.stage-intent.v1"
        ));
        assert_eq!(stage_consumer_parts("worker-a"), None);
        assert_eq!(stage_consumer_parts("#fast"), None);
        assert_eq!(stage_consumer_parts("worker-a#turbo"), None);
    }
}
