//! SQLite record (payload) codec: `[header_size varint][serial_type varint]…[data]…`.
//! Serial types reimplemented from the public SQLite file format (mirroring the
//! serial-type table in `turso/core/types.rs`): 0=NULL, 1..6 = 1/2/3/4/6/8-byte signed
//! big-endian int, 7 = 8-byte IEEE-754 float, 8/9 = constant int 0/1 (zero payload),
//! N≥12 even = BLOB of `(N-12)/2` bytes, N≥13 odd = TEXT of `(N-13)/2` bytes.

use crate::error::{Error, Result};
use crate::varint::{read_varint, varint_len, write_varint};
use crate::value::Value;

/// Serial type + the body bytes to store for one value.
fn value_serial(value: &Value) -> (u64, Vec<u8>) {
    match value {
        Value::Null => (0, Vec::new()),
        Value::Integer(i) => encode_int(*i),
        Value::Real(f) => (7, f.to_be_bytes().to_vec()),
        Value::Text(s) => (13 + (s.len() as u64) * 2, s.as_bytes().to_vec()),
        Value::Blob(b) => (12 + (b.len() as u64) * 2, b.clone()),
    }
}

/// Choose the smallest serial type that stores `i` losslessly.
fn encode_int(i: i64) -> (u64, Vec<u8>) {
    // Constant-int fast paths (zero-width payload).
    if i == 0 {
        return (8, Vec::new());
    }
    if i == 1 {
        return (9, Vec::new());
    }
    let be = i.to_be_bytes(); // 8 bytes, big-endian two's complement
    if (-128..=127).contains(&i) {
        (1, be[7..].to_vec())
    } else if (-32_768..=32_767).contains(&i) {
        (2, be[6..].to_vec())
    } else if (-8_388_608..=8_388_607).contains(&i) {
        (3, be[5..].to_vec())
    } else if (-2_147_483_648..=2_147_483_647).contains(&i) {
        (4, be[4..].to_vec())
    } else if (-140_737_488_355_328..=140_737_488_355_327).contains(&i) {
        (5, be[2..].to_vec())
    } else {
        (6, be.to_vec())
    }
}

/// Payload byte length of a serial type, and whether it's valid.
fn serial_type_len(serial: u64) -> Result<usize> {
    Ok(match serial {
        0 | 8 | 9 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 | 7 => 8,
        10 | 11 => return Err(Error::corrupt("reserved serial type 10/11")),
        n if n >= 12 => {
            if n % 2 == 0 {
                ((n - 12) / 2) as usize
            } else {
                ((n - 13) / 2) as usize
            }
        }
        _ => unreachable!(),
    })
}

/// Decode a value given its serial type and the body bytes (exactly `serial_type_len`).
fn decode_value(serial: u64, body: &[u8]) -> Result<Value> {
    Ok(match serial {
        0 => Value::Null,
        1..=6 => Value::Integer(read_be_int(body)),
        7 => {
            if body.len() != 8 {
                return Err(Error::corrupt("float body not 8 bytes"));
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(body);
            Value::Real(f64::from_be_bytes(b))
        }
        8 => Value::Integer(0),
        9 => Value::Integer(1),
        n if n >= 12 && n % 2 == 0 => Value::Blob(body.to_vec()),
        n if n >= 13 => Value::Text(
            String::from_utf8(body.to_vec())
                .map_err(|_| Error::corrupt("non-UTF-8 text value"))?,
        ),
        _ => return Err(Error::corrupt("invalid serial type")),
    })
}

/// Big-endian two's-complement signed integer from `bytes.len()` bytes (1..8).
fn read_be_int(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }
    let mut v = (bytes[0] as i8) as i64; // sign-extend the top byte
    for &b in &bytes[1..] {
        v = (v << 8) | (b as i64);
    }
    v
}

/// Encode a full record (row) into payload bytes.
pub fn encode_record(values: &[Value]) -> Vec<u8> {
    let mut serials = Vec::with_capacity(values.len());
    let mut bodies = Vec::with_capacity(values.len());
    for v in values {
        let (s, b) = value_serial(v);
        serials.push(s);
        bodies.push(b);
    }
    let serial_bytes: usize = serials.iter().map(|s| varint_len(*s)).sum();
    // The header-size varint includes itself: find the fixed point.
    let mut size_len = 1usize;
    let header_size = loop {
        let header_size = serial_bytes + size_len;
        if varint_len(header_size as u64) == size_len {
            break header_size;
        }
        size_len += 1;
    };

    let mut out = Vec::with_capacity(header_size + bodies.iter().map(Vec::len).sum::<usize>());
    write_varint(&mut out, header_size as u64);
    for s in &serials {
        write_varint(&mut out, *s);
    }
    for b in &bodies {
        out.extend_from_slice(b);
    }
    out
}

/// Decode a full record (row) back into its column values.
pub fn decode_record(payload: &[u8]) -> Result<Vec<Value>> {
    let (header_size, mut pos) = read_varint(payload)?;
    let header_end = header_size as usize;
    if header_end > payload.len() {
        return Err(Error::corrupt("record header exceeds payload"));
    }
    let mut serials = Vec::new();
    while pos < header_end {
        let (s, n) = read_varint(&payload[pos..header_end])?;
        serials.push(s);
        pos += n;
    }
    let mut body = header_end;
    let mut out = Vec::with_capacity(serials.len());
    for s in serials {
        let len = serial_type_len(s)?;
        if body + len > payload.len() {
            return Err(Error::corrupt("record body exceeds payload"));
        }
        out.push(decode_value(s, &payload[body..body + len])?);
        body += len;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(vals: Vec<Value>) {
        let bytes = encode_record(&vals);
        let got = decode_record(&bytes).unwrap();
        assert_eq!(got, vals);
    }

    #[test]
    fn record_all_storage_classes() {
        rt(vec![Value::Null]);
        rt(vec![Value::Integer(0), Value::Integer(1)]);
        rt(vec![
            Value::Integer(-1),
            Value::Integer(127),
            Value::Integer(-128),
            Value::Integer(128),
            Value::Integer(32_767),
            Value::Integer(-32_768),
            Value::Integer(8_388_607),
            Value::Integer(-8_388_608),
            Value::Integer(2_147_483_647),
            Value::Integer(-2_147_483_648),
            Value::Integer(140_737_488_355_327),
            Value::Integer(i64::MAX),
            Value::Integer(i64::MIN),
        ]);
        rt(vec![Value::Real(9.5), Value::Real(-0.0), Value::Real(f64::MIN)]);
        rt(vec![
            Value::Text(String::new()),
            Value::Text("hello".into()),
            Value::Blob(Vec::new()),
            Value::Blob(vec![1, 2, 3, 255]),
        ]);
    }
}
