#![cfg(feature = "server")]
use epistemic_graph::protocol::Request;
#[test]
fn test_parse() {
    let msg = b"\x85\xa2id\x01\xa5graph\xa4test\xaaauth_token\xa5dummy\xa6method\xabCreateGraph\xa6params\x82\xaagraph_name\xa4test\xaagraph_type\xa5Agent";
    let req: Result<Request, _> = rmp_serde::from_slice(msg);
    println!("{:?}", req);
    assert!(req.is_ok());
}
