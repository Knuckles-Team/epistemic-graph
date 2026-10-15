//! Minimal thread-local tracing capture shared by helper and solver regressions.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::{field::Visit, span, subscriber::Interest, Event, Metadata, Subscriber};

type Fields = BTreeMap<String, String>;

#[derive(Clone, Default)]
pub struct Capture(Arc<Mutex<Vec<Fields>>>);

impl Capture {
    pub fn events(&self) -> Vec<Fields> {
        self.0.lock().unwrap().clone()
    }
}

struct Visitor(Fields);
impl Visit for Visitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
}

impl Subscriber for Capture {
    /// Never let the process-wide callsite cache decide for this thread.
    ///
    /// The default registration answers `never` for a callsite first hit by a
    /// parallel test with no subscriber, and the cached answer can outlive the
    /// scoped dispatcher installed afterwards. `sometimes` forces every event to
    /// be filtered by this subscriber's own `enabled`, so capture is exact.
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "eg_compute::solve"
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut visitor = Visitor(Fields::new());
        event.record(&mut visitor);
        self.0.lock().unwrap().push(visitor.0);
    }
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}
