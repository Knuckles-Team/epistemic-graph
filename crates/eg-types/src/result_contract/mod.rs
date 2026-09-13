//! The typed result contract: which Rust body every contract method returns, and how
//! that body becomes a [`ResultPayload`].
//!
//! A handler builds a successful result through [`ResultPayload::of`] (or
//! [`ResultPayload::of_ref`], [`ResultPayload::scalar`], [`ResultPayload::of_dynamic`])
//! with a marker naming the method -- and, for a method whose request carries an op
//! enum, the op. Markers live in one module per contract domain, matching
//! `eg-capabilities`' domains, e.g. [`graph::HasNode`]. The marker fixes the body's Rust type, so a handler that
//! encodes a different type does not compile. `eg-capabilities`' generator walks the same
//! markers ([`visit_all`]) to publish one JSON Schema per body, so the published result
//! shape and the encoded one are the same Rust type by construction.
//!
//! A body whose shape the CALLER chooses at run time -- query rows, property maps the
//! caller wrote -- is declared with [`Dynamic`] and a [`DynamicReason`]. That is a
//! reviewed classification with a stated reason, not a default: a method with no marker
//! at all is reported as unclassified by the generator.

use serde::Serialize;

use crate::protocol::ResultPayload;

pub mod encoding;
mod marker;
mod schema;

pub use encoding::{EncodeRef, EncodeScalar, Encoding};
pub use marker::{MethodResult, ResultVisitor};
pub use schema::ResultSchema;

/// The body of a result whose shape the caller chooses at run time.
///
/// Uninhabited: a dynamic result is built with [`ResultPayload::of_dynamic`], which takes
/// the run-time value directly. Its schema is the unconstrained schema `true`.
pub enum Dynamic {}

impl Serialize for Dynamic {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        match *self {}
    }
}

#[cfg(feature = "contract-schema")]
impl schemars::JsonSchema for Dynamic {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DynamicResult".into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        true.into()
    }
}

/// Why a result is [`Dynamic`]. Each reason names a class of body whose keys or value
/// types are supplied by the caller, so no Rust type can describe it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DynamicReason {
    /// Rows whose columns and value types are chosen by the caller's query text.
    QueryRows,
    /// Property maps the caller wrote, returned verbatim.
    CallerProperties,
    /// Opaque bytes the caller wrote or a caller-supplied program produced, returned verbatim.
    CallerBytes,
}

impl DynamicReason {
    pub const fn as_str(&self) -> &'static str {
        match self {
            DynamicReason::QueryRows => "query-rows",
            DynamicReason::CallerProperties => "caller-properties",
            DynamicReason::CallerBytes => "caller-bytes",
        }
    }

    pub const fn summary(&self) -> &'static str {
        match self {
            DynamicReason::QueryRows => {
                "rows whose columns and value types are chosen by the caller's query text"
            }
            DynamicReason::CallerProperties => "property maps the caller wrote, returned verbatim",
            DynamicReason::CallerBytes => {
                "opaque bytes the caller wrote or a caller-supplied program produced"
            }
        }
    }
}

impl ResultPayload {
    /// Encode `body` as `M`'s declared result.
    pub fn of<M: MethodResult>(body: M::Body) -> Result<Self, String> {
        <M::Encoding as Encoding<M::Body>>::encode(body)
    }

    /// Encode a borrowed `body` as `M`'s declared result.
    pub fn of_ref<M: MethodResult>(body: &M::Body) -> Result<Self, String>
    where
        M::Encoding: EncodeRef<M::Body>,
    {
        <M::Encoding as EncodeRef<M::Body>>::encode_ref(body)
    }

    /// Encode `body` as `M`'s declared scalar result, which cannot fail.
    pub fn scalar<M: MethodResult>(body: M::Body) -> Self
    where
        M::Encoding: EncodeScalar<M::Body>,
    {
        <M::Encoding as EncodeScalar<M::Body>>::encode_scalar(body)
    }

    /// A caller-shaped result that is already MessagePack -- a result-cache hit, or the
    /// stored bytes of a caller-written value. Only a [`Dynamic`] `Raw` result may be
    /// served from bytes, because only there is no Rust type to check them against.
    pub fn of_encoded<M>(bytes: Vec<u8>) -> Self
    where
        M: MethodResult<Body = Dynamic, Encoding = encoding::Raw>,
    {
        ResultPayload::Raw(bytes)
    }

    /// [`ResultPayload::of_encoded`] for a lookup that may find nothing.
    pub fn of_encoded_or_null<M>(bytes: Option<Vec<u8>>) -> Self
    where
        M: MethodResult<Body = Dynamic, Encoding = encoding::RawOrNull>,
    {
        match bytes {
            Some(bytes) => ResultPayload::Raw(bytes),
            None => ResultPayload::Json(serde_json::Value::Null),
        }
    }

    /// Encode a caller-shaped `body` as `M`'s declared [`Dynamic`] result.
    pub fn of_dynamic<M, T>(body: &T) -> Result<Self, String>
    where
        M: MethodResult<Body = Dynamic>,
        M::Encoding: EncodeRef<T>,
        T: ?Sized,
    {
        <M::Encoding as EncodeRef<T>>::encode_ref(body)
    }
}

/// `method_results! { visit_fn; Marker(Method [/ "op"]) => Encoding<Body> [dynamic Reason]; ... }`
///
/// Declares one uninhabited marker per entry plus `visit_fn`, which hands every marker
/// of the invoking file to a [`ResultVisitor`].
macro_rules! method_results {
    ($visit:ident;) => {
        pub(super) fn $visit<V: $crate::result_contract::ResultVisitor>(_visitor: &mut V) {}
    };
    (
        $visit:ident;
        $(
            $(#[$attr:meta])*
            $marker:ident ( $method:ident $(/ $op:literal)? ) => $enc:ident < $body:ty >
            $(dynamic $reason:ident)? ;
        )*
    ) => {
        $(
            $(#[$attr])*
            #[doc = concat!("Result of `Method::", stringify!($method), "`", $(" op `", $op, "`",)? ".")]
            pub enum $marker {}

            $(#[$attr])*
            impl $crate::result_contract::MethodResult for $marker {
                const METHOD: &'static str = stringify!($method);
                const OP: &'static str = op_tag!($($op)?);
                const DYNAMIC: Option<$crate::result_contract::DynamicReason> =
                    dynamic_reason!($($reason)?);
                type Body = $body;
                type Encoding = $crate::result_contract::encoding::$enc;
            }
        )*

        pub(super) fn $visit<V: $crate::result_contract::ResultVisitor>(visitor: &mut V) {
            $(
                $(#[$attr])*
                visitor.visit::<$marker>();
            )*
        }
    };
}

macro_rules! op_tag {
    () => {
        ""
    };
    ($op:literal) => {
        $op
    };
}

macro_rules! dynamic_reason {
    () => {
        None
    };
    ($reason:ident) => {
        Some($crate::result_contract::DynamicReason::$reason)
    };
}

pub mod cluster;
pub mod compute;
pub mod coordination;
pub mod graph;
pub mod ingestion;
pub mod messaging;
pub mod query;
pub mod reasoning;
pub mod security;
pub mod storage;
pub mod transactions;

/// Hand every declared result marker to `visitor`, domain by domain.
pub fn visit_all<V: ResultVisitor>(visitor: &mut V) {
    cluster::visit_cluster(visitor);
    compute::visit_compute(visitor);
    coordination::visit_coordination(visitor);
    graph::visit_graph(visitor);
    ingestion::visit_ingestion(visitor);
    messaging::visit_messaging(visitor);
    query::visit_query(visitor);
    reasoning::visit_reasoning(visitor);
    security::visit_security(visitor);
    storage::visit_storage(visitor);
    transactions::visit_transactions(visitor);
}
