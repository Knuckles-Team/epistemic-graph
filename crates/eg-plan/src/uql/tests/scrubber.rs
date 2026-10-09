//! EG-FEDERATED-QUERY-R029: UQL text from graph-exploration clients matches the grammar.
//!
//! A conformance fixture holding the exact text a temporal-scrubber control emits — the
//! stage-chained syntax (`|>`) with a Unix-second `AS OF` timestamp (`@<unix seconds>`) —
//! plus negative fixtures for the two shapes the grammar must reject with a typed parse
//! error rather than silently misinterpret: Cypher-shaped text, and an ISO-8601 timestamp.

use crate::uql::parse;
use eg_types::wire::{Op, TimeAxis};

/// The exact text a temporal-scrubber control's query template emits when the user drags
/// the handle to a given instant: a `MATCH` source stage-chained to `VALID AS OF` with a
/// Unix-second timestamp literal.
const SCRUBBER_QUERY_TEMPLATE: &str = "MATCH (:Event) |> VALID AS OF @1700000000";

// spec: EG-FEDERATED-QUERY-R029
#[test]
fn the_temporal_scrubber_query_template_parses_under_the_current_grammar() {
    let plan = parse(SCRUBBER_QUERY_TEMPLATE)
        .expect("the exact text the temporal-scrubber control's query template emits must parse");
    assert_eq!(
        plan.ops[0],
        Op::Scan {
            label: "Event".into()
        }
    );
    assert_eq!(
        plan.ops[1],
        Op::AsOf {
            ts: 1_700_000_000.0,
            axis: TimeAxis::Valid
        }
    );
}

/// `AS OF TX` with a Unix-second timestamp — the transaction-axis variant a scrubber could
/// equally emit — parses the same way.
// spec: EG-FEDERATED-QUERY-R029
#[test]
fn the_scrubber_transaction_axis_variant_parses() {
    let plan = parse("MATCH (:Event) |> AS OF TX @1700000000").unwrap();
    assert_eq!(
        plan.ops[1],
        Op::AsOf {
            ts: 1_700_000_000.0,
            axis: TimeAxis::Transaction
        }
    );
}

/// Cypher-shaped text (a bare `MATCH ... RETURN`, no stage-chain, no `@` timestamp marker)
/// is rejected with a typed parse error, never silently misinterpreted as UQL.
// spec: EG-FEDERATED-QUERY-R029
#[test]
fn cypher_shaped_text_is_rejected_with_a_typed_parse_error() {
    // A typed `UqlError` (stable code + span), not a panic or a silently-accepted plan.
    let _: eg_types::contract::UqlCode = parse("MATCH (e:Event) WHERE e.ts = 1700000000 RETURN e")
        .expect_err("Cypher-shaped text must not parse as UQL")
        .code;
}

/// An ISO-8601 timestamp in place of the required Unix-second `@<seconds>` literal is
/// rejected with a typed parse error rather than being accepted or misread.
#[test]
fn an_iso8601_as_of_timestamp_is_rejected_with_a_typed_parse_error() {
    let _: eg_types::contract::UqlCode =
        parse("MATCH (:Event) |> VALID AS OF 2023-11-14T22:13:20Z")
            .expect_err("an ISO-8601 AS OF timestamp must not parse under the UQL grammar")
            .code;
}
