//! Paged listing of one consumer's retained dead-letter evidence.
//!
//! Pruning deletes a DELIVERED row once it falls behind the resolved prefix
//! (`claim::prune_resolved`), but a dead-lettered row is retained forever as
//! retry evidence (X10-R4). This is the operator's only way to see that
//! evidence without opening the delivery table directly.

use crate::outbox::rows::{
    decode_row, validate_consumer, validate_delivery_key, validate_delivery_state, validate_stamp,
    OutboxDelivery, OutboxPosition, MAX_CLAIM_SCAN_ROWS,
};
use crate::outbox::{claim, index, stream};
use crate::tables::OUTBOX_DELIVERIES;
use eg_storage::{ledger_scope_key, OwnerDomain, ScopedRead};

/// One page of a consumer's dead-lettered rows, oldest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxDeadLetterPage {
    pub rows: Vec<OutboxDelivery>,
    /// Whether more dead-letter rows may exist beyond this page. Pass this
    /// page's last row's position as the next call's `after` to continue.
    pub truncated: bool,
}

/// List `consumer`'s dead-lettered rows in commit order, resuming after
/// `after` and returning at most `limit` rows from one bounded index page.
pub(crate) fn dead_letters<D: OwnerDomain>(
    read: &ScopedRead<'_, D>,
    consumer: &str,
    after: Option<&OutboxPosition>,
    limit: u32,
) -> Result<OutboxDeadLetterPage, String> {
    validate_consumer(consumer)?;
    if limit == 0 {
        return Err("outbox dead-letter listing requires a non-zero limit".to_string());
    }
    let identity = read.scope();
    let scope = ledger_scope_key(identity);
    let Some(topic) = stream::subscribed_topic_in_read(read, &scope, consumer)? else {
        return Ok(OutboxDeadLetterPage {
            rows: Vec::new(),
            truncated: false,
        });
    };
    let acked_through = claim::read_claim_cursor_in_read(read, &scope, consumer, identity)?
        .acked_through;
    let page = index::scan_in_read(read, &scope, &topic, after, MAX_CLAIM_SCAN_ROWS)?;
    let table = read.scoped_table(OUTBOX_DELIVERIES)?;
    let mut rows = Vec::new();
    let mut truncated = page.truncated;
    for entry in page.entries {
        index::validate_position_in_read(read, &scope, &topic, identity, &entry.position)
            .map_err(|error| format!("CORRUPT_OUTBOX_INDEX: {error}"))?;
        let delivery = table
            .get((
                scope.as_str(),
                consumer,
                entry.position.batch_id.as_str(),
                entry.position.ordinal,
            ))?
            .map(|value| decode_row::<OutboxDelivery>(value.value()))
            .transpose()?;
        let Some(delivery) = delivery else {
            continue;
        };
        validate_stamp(&delivery.identity, identity)?;
        validate_delivery_key(&delivery, consumer, &entry.position)?;
        validate_delivery_state(&delivery, acked_through.as_ref())?;
        if !delivery.dead_lettered() {
            continue;
        }
        if rows.len() as u32 >= limit {
            truncated = true;
            break;
        }
        rows.push(delivery);
    }
    Ok(OutboxDeadLetterPage { rows, truncated })
}
