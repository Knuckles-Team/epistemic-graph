//! The marker trait a declared result implements, and the visitor that walks them.

use super::{DynamicReason, Encoding, ResultSchema};

/// One method's (or one op's) declared result.
pub trait MethodResult {
    /// The contract method id, exactly the `Method` serde tag.
    const METHOD: &'static str;
    /// The request op tag that selects this body; empty for a single-body method.
    const OP: &'static str;
    /// Present exactly when [`Self::Body`] is [`Dynamic`].
    const DYNAMIC: Option<DynamicReason>;
    type Body: ResultSchema;
    type Encoding: Encoding<Self::Body>;
}

/// Receives every declared result marker, in declaration order.
pub trait ResultVisitor {
    fn visit<M: MethodResult>(&mut self);
}
