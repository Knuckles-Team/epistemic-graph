use super::*;

const HEADER: &str = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n";

fn row(local: &str, state: &str) -> String {
    format!("   0: {local} 00000000:0000 {state} 00000000:00000000 00:00000000 00000000     0        0 1\n")
}

#[test]
fn loopback_listeners_pass_and_one_exposed_listener_fails() {
    let loopback = format!("{HEADER}{}", row("0100007F:1F90", "0A"));
    assert!(table_is_loopback(&loopback));
    let wildcard = format!("{loopback}{}", row("00000000:1F91", "0A"));
    assert!(!table_is_loopback(&wildcard), "0.0.0.0 is exposed");
    let established = format!("{loopback}{}", row("0A00000A:C350", "01"));
    assert!(table_is_loopback(&established), "only LISTEN rows count");
}

#[test]
fn ipv6_loopback_and_mapped_loopback_pass() {
    assert!(is_loopback_hex("00000000000000000000000001000000"));
    assert!(is_loopback_hex("0000000000000000FFFF00000100007F"));
    assert!(!is_loopback_hex("00000000000000000000000000000000"));
    assert!(!is_loopback_hex("0A00000A"));
}
