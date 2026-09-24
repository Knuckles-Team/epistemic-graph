//! Whether this engine's network namespace listens on loopback only -- the
//! `none`-mode precondition the engine can check itself (§2.4).
//!
//! It reads the namespace's LISTEN sockets from `/proc/net/tcp{,6}`. In the
//! graph-os pod the engine is a sidecar sharing the pod's namespace, so the
//! answer covers graph-os's listeners too. Anything unreadable (non-Linux, a
//! restricted `/proc`) answers "exposed": the check fails closed.

/// TCP state `LISTEN` in `/proc/net/tcp`.
const LISTEN: &str = "0A";

/// Whether one hex local address from `/proc/net/tcp{,6}` is loopback.
fn is_loopback_hex(address: &str) -> bool {
    match address.len() {
        // IPv4 in host (little-endian) byte order: 127.x.y.z ends in "7F".
        8 => address.ends_with("7F"),
        // ::1, or an IPv4-mapped 127.x.y.z (::ffff:127.x.y.z).
        32 => {
            address == "00000000000000000000000001000000"
                || (address.starts_with("0000000000000000FFFF0000") && address.ends_with("7F"))
        }
        _ => false,
    }
}

/// Whether every LISTEN row of one `/proc/net/tcp`-format table is loopback.
fn table_is_loopback(table: &str) -> bool {
    table.lines().skip(1).all(|line| {
        let mut fields = line.split_whitespace();
        let local = fields.nth(1).unwrap_or("");
        let state = fields.nth(1).unwrap_or("");
        let address = local.split(':').next().unwrap_or("");
        state != LISTEN || is_loopback_hex(address)
    })
}

/// Whether every listening socket in this network namespace is loopback.
pub(super) fn listeners_loopback() -> bool {
    ["/proc/net/tcp", "/proc/net/tcp6"]
        .iter()
        .all(|path| std::fs::read_to_string(path).is_ok_and(|table| table_is_loopback(&table)))
}

#[cfg(test)]
mod tests;
