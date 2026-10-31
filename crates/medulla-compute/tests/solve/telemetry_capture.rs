//! Minimal thread-local tracing capture shared by helper and solver regressions.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::{
    dispatcher::Dispatch, field::Visit, span, subscriber::Interest, Event, Metadata, Subscriber,
};

type Fields = BTreeMap<String, String>;

#[derive(Clone)]
pub struct Capture(Arc<Mutex<Vec<Fields>>>);

/// Keep a second dispatcher registered for the life of the process.
///
/// While exactly one dispatcher has ever been registered, tracing computes a
/// callsite's cached interest from the *calling thread's* current dispatcher
/// only. A parallel test thread with no subscriber then caches "never" for the
/// solver callsite, and the scoped capture on another thread sees no event.
/// With two registered dispatchers tracing consults every one of them, so the
/// cached interest no longer depends on which thread reached the callsite first.
fn anchor_dispatcher() {
    static ANCHOR: OnceLock<Dispatch> = OnceLock::new();
    ANCHOR.get_or_init(|| Dispatch::new(tracing::subscriber::NoSubscriber::default()));
}

impl Default for Capture {
    fn default() -> Self {
        anchor_dispatcher();
        Self(Arc::default())
    }
}

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
    /// Defer to `enabled` for every event instead of caching a verdict.
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "medulla_compute::solve"
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
