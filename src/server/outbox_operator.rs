//! The operator and consumer surface over any owner's mutation outbox (X10).
//!
//! The generic functions live in `eg-transaction` (`outbox::views`), so every
//! owner crate -- not only the server -- serves the same views; this module is
//! the server's name for them.

pub(crate) use eg_transaction::{
    consumer_status, operate_outbox as operate, outbox_position as position,
    read_outbox_view as read_view, OutboxView, OutboxViewAnswer, OutboxWrite, OutboxWriteReply,
};
