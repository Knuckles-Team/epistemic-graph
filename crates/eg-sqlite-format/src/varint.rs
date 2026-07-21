//! SQLite "varint": a big-endian, 7-bit-per-byte variable-length integer, 1..9 bytes.
//! Bytes 1..8 carry 7 payload bits each (high bit = continuation); the optional 9th
//! byte carries a full 8 bits. Reimplemented from the public SQLite file format (the
//! same technique `open-source-libraries/turso/core/storage/sqlite3_ondisk.rs` uses).

use crate::error::{Error, Result};

/// Read a varint from the front of `buf`, returning `(value, bytes_consumed)`.
pub fn read_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut v: u64 = 0;
    for i in 0..8 {
        match buf.get(i) {
            Some(c) => {
                v = (v << 7) + (c & 0x7f) as u64;
                if (c & 0x80) == 0 {
                    return Ok((v, i + 1));
                }
            }
            None => return Err(Error::corrupt("truncated varint")),
        }
    }
    match buf.get(8) {
        Some(&c) => {
            v = (v << 8) + c as u64;
            Ok((v, 9))
        }
        None => Err(Error::corrupt("truncated 9-byte varint")),
    }
}

/// Number of bytes the varint encoding of `value` occupies (1..9).
pub fn varint_len(value: u64) -> usize {
    if value <= 0x7f {
        1
    } else if value > (1u64 << 56) - 1 {
        9
    } else {
        let bits = 64 - value.leading_zeros() as usize;
        bits.div_ceil(7)
    }
}

/// Append the varint encoding of `value` to `out`.
pub fn write_varint(out: &mut Vec<u8>, value: u64) {
    if value <= 0x7f {
        out.push((value & 0x7f) as u8);
        return;
    }
    if value <= 0x3fff {
        out.push((((value >> 7) & 0x7f) | 0x80) as u8);
        out.push((value & 0x7f) as u8);
        return;
    }
    // 9-byte form: top 8 bits do not fit in the 7-bit chunks.
    let mut value = value;
    if (value & (0xff000000_u64 << 32)) > 0 {
        let mut buf = [0u8; 9];
        buf[8] = value as u8;
        value >>= 8;
        for i in (0..8).rev() {
            buf[i] = ((value & 0x7f) | 0x80) as u8;
            value >>= 7;
        }
        out.extend_from_slice(&buf);
        return;
    }
    let mut encoded = [0u8; 9];
    let mut bytes = value;
    let mut n = 0;
    while bytes != 0 {
        encoded[n] = (0x80 | (bytes & 0x7f)) as u8;
        bytes >>= 7;
        n += 1;
    }
    encoded[0] &= 0x7f;
    for i in 0..n {
        out.push(encoded[n - 1 - i]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: u64) {
        let mut buf = Vec::new();
        write_varint(&mut buf, v);
        assert_eq!(buf.len(), varint_len(v), "len mismatch for {v}");
        let (got, n) = read_varint(&buf).unwrap();
        assert_eq!(got, v, "value mismatch for {v}");
        assert_eq!(n, buf.len(), "consumed mismatch for {v}");
    }

    #[test]
    fn varint_edge_values() {
        for v in [
            0u64,
            1,
            0x7f,
            0x80,
            0x3fff,
            0x4000,
            0x1f_ffff,
            0xffff_ffff,
            (1u64 << 56) - 1,
            1u64 << 56,
            i64::MAX as u64,
            u64::MAX,
        ] {
            roundtrip(v);
        }
    }
}
