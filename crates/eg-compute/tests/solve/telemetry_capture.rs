//! Minimal thread-local tracing capture shared by helper and solver regressions.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::{field::Visit, span, Event, Metadata, Subscriber};

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
