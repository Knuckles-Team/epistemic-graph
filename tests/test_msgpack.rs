#![cfg(feature = "server")]
use epistemic_graph::protocol::Request;
#[test]
fn test_parse() {
    // `agent_id` is a REQUIRED-present (possibly-null) field on the wire `Request`
    // envelope (`deserialize_required_option`, CONCEPT:EG-KG.security.signed-request-envelope):
    // unlike an ordinary `Option<T>`, serde does not default it to `None` when the key
    // is simply absent from the map, so a fixture minted before that field existed must
    // now explicitly carry `agent_id: nil`. This message is a 6-entry fixmap (`\x86`)
    // with the extra `\xa8agent_id\xc0` pair (fixstr "agent_id" + msgpack nil), matching
    // the shape every `tests/common::signed_request*` helper produces on the wire.
    let msg = b"\x86\xa2id\x01\xa5graph\xa4test\xaaauth_token\xa5dummy\xa8agent_id\xc0\xa6method\xabCreateGraph\xa6params\x82\xaagraph_name\xa4test\xaagraph_type\xa5Agent";
    let req: Result<Request, _> = rmp_serde::from_slice(msg);
    println!("{:?}", req);
    assert!(req.is_ok());
}
