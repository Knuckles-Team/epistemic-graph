//! The declared result of every contract method, collected from the
//! `eg_types::result_contract` markers the engine's handlers encode through.
//!
//! A row in `domains/` authors no result. The result is whatever Rust type the marker
//! fixes at the handler's encode site, so this pass only walks the markers, derives one
//! JSON Schema per body, and groups the definitions by contract domain.

use std::any::type_name;
use std::collections::BTreeMap;

use eg_types::result_contract::{
    visit_all, Dynamic, DynamicReason, Encoding, MethodResult, ResultVisitor,
};
use schemars::SchemaGenerator;

/// The body key of a method that declares exactly one result.
pub(super) const SINGLE_BODY: &str = "result";

/// One declared body.
pub(super) struct Body {
    /// The `ResultPayload` variant the body is encoded as.
    pub(super) encoding: &'static str,
    /// Present exactly when the body's shape is chosen by the caller.
    pub(super) dynamic: Option<DynamicReason>,
    pub(super) schema: serde_json::Value,
}

/// Everything a method declares: one body, or one body per request op.
#[derive(Default)]
pub(super) struct Declared {
    pub(super) by_op: bool,
    /// Keyed by op tag, or [`SINGLE_BODY`].
    pub(super) bodies: BTreeMap<&'static str, Body>,
}

/// How much of a method's result the contract describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResultClass {
    /// Every body has a concrete schema.
    Schematized,
    /// Every body is declared caller-shaped, with a reason.
    Dynamic,
    /// Some op bodies are schematized, the others declared dynamic.
    Mixed,
}

impl Declared {
    /// The only body of a single-body method.
    pub(super) fn single(&self) -> Option<&Body> {
        if self.by_op {
            return None;
        }
        self.bodies.get(SINGLE_BODY)
    }

    pub(super) fn class(&self) -> ResultClass {
        let dynamic = self
            .bodies
            .values()
            .filter(|body| body.dynamic.is_some())
            .count();
        match dynamic {
            0 => ResultClass::Schematized,
            n if n == self.bodies.len() => ResultClass::Dynamic,
            _ => ResultClass::Mixed,
        }
    }
}

/// Every declared result plus the per-domain schema definitions they reference.
pub(super) struct Catalog {
    pub(super) methods: BTreeMap<&'static str, Declared>,
    pub(super) definitions: BTreeMap<&'static str, serde_json::Map<String, serde_json::Value>>,
}

impl Catalog {
    pub(super) fn collect() -> Self {
        let mut collector = Collector {
            domain_of: crate::method_descriptors()
                .map(|descriptor| (descriptor.id.as_str(), descriptor.domain))
                .collect(),
            generators: BTreeMap::new(),
            methods: BTreeMap::new(),
        };
        visit_all(&mut collector);
        let definitions = collector
            .generators
            .into_iter()
            .map(|(domain, mut generator)| (domain, generator.take_definitions(true)))
            .collect();
        Catalog {
            methods: collector.methods,
            definitions,
        }
    }
}

struct Collector {
    domain_of: BTreeMap<&'static str, &'static str>,
    generators: BTreeMap<&'static str, SchemaGenerator>,
    methods: BTreeMap<&'static str, Declared>,
}

impl ResultVisitor for Collector {
    fn visit<M: MethodResult>(&mut self) {
        let domain = *self.domain_of.get(M::METHOD).unwrap_or_else(|| {
            panic!(
                "a result marker names `{}`, which is not a contract method",
                M::METHOD
            )
        });
        let is_dynamic = type_name::<M::Body>() == type_name::<Dynamic>();
        assert_eq!(
            is_dynamic,
            M::DYNAMIC.is_some(),
            "{} {}: a result is Dynamic exactly when it declares a DynamicReason",
            M::METHOD,
            M::OP
        );
        let schema = serde_json::to_value(
            self.generators
                .entry(domain)
                .or_default()
                .subschema_for::<M::Body>(),
        )
        .expect("a JSON Schema is serializable");
        let by_op = !M::OP.is_empty();
        let declared = self.methods.entry(M::METHOD).or_default();
        assert!(
            declared.bodies.is_empty() || declared.by_op == by_op,
            "{}: mixes a single result with per-op results",
            M::METHOD
        );
        declared.by_op = by_op;
        let key = if by_op { M::OP } else { SINGLE_BODY };
        let previous = declared.bodies.insert(
            key,
            Body {
                encoding: <M::Encoding as Encoding<M::Body>>::NAME,
                dynamic: M::DYNAMIC,
                schema,
            },
        );
        assert!(
            previous.is_none(),
            "{} {key}: the result is declared twice",
            M::METHOD
        );
    }
}
