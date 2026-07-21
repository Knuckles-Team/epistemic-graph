//! The 100-byte SQLite database header (all multi-byte fields big-endian).
//! Field list/order per the public SQLite file format.

use crate::error::{Error, Result};

pub const HEADER_SIZE: usize = 100;
pub const MAGIC: &[u8; 16] = b"SQLite format 3\0";
/// Informational `SQLITE_VERSION_NUMBER` a reading client sees (3.47.0). Purely cosmetic.
pub const SQLITE_VERSION_NUMBER: u32 = 3_047_000;

#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    pub page_size: u32,
    pub write_version: u8,
    pub read_version: u8,
    pub reserved_space: u8,
    pub change_counter: u32,
    pub database_size: u32,
    pub freelist_trunk_page: u32,
    pub freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub text_encoding: u32,
}

impl DatabaseHeader {
    /// The usable bytes per page: total page size minus the per-page reserved region.
    pub fn usable_space(&self) -> usize {
        self.page_size as usize - self.reserved_space as usize
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_SIZE {
            return Err(Error::corrupt("file smaller than 100-byte header"));
        }
        if &buf[0..16] != MAGIC {
            return Err(Error::corrupt("bad magic string"));
        }
        let raw_page_size = u16::from_be_bytes([buf[16], buf[17]]);
        let page_size: u32 = if raw_page_size == 1 {
            65_536
        } else {
            raw_page_size as u32
        };
        if page_size < 512 || !page_size.is_power_of_two() {
            return Err(Error::corrupt("page size not a power of two in [512,65536]"));
        }
        let text_encoding = u32::from_be_bytes([buf[56], buf[57], buf[58], buf[59]]);
        if text_encoding != 1 {
            return Err(Error::unsupported("non-UTF-8 text encoding"));
        }
        Ok(DatabaseHeader {
            page_size,
            write_version: buf[18],
            read_version: buf[19],
            reserved_space: buf[20],
            change_counter: be32(buf, 24),
            database_size: be32(buf, 28),
            freelist_trunk_page: be32(buf, 32),
            freelist_pages: be32(buf, 36),
            schema_cookie: be32(buf, 40),
            schema_format: be32(buf, 44),
            text_encoding,
        })
    }

    /// Serialize the 100-byte header into the first 100 bytes of a fresh page-1 buffer.
    pub fn encode(&self, out: &mut [u8]) {
        out[0..16].copy_from_slice(MAGIC);
        let raw_page_size: u16 = if self.page_size == 65_536 {
            1
        } else {
            self.page_size as u16
        };
        out[16..18].copy_from_slice(&raw_page_size.to_be_bytes());
        out[18] = self.write_version;
        out[19] = self.read_version;
        out[20] = self.reserved_space;
        out[21] = 64; // max embedded payload fraction (fixed)
        out[22] = 32; // min embedded payload fraction (fixed)
        out[23] = 32; // leaf payload fraction (fixed)
        put32(out, 24, self.change_counter);
        put32(out, 28, self.database_size);
        put32(out, 32, self.freelist_trunk_page);
        put32(out, 36, self.freelist_pages);
        put32(out, 40, self.schema_cookie);
        put32(out, 44, self.schema_format);
        put32(out, 48, 0); // default page cache size
        put32(out, 52, 0); // largest root b-tree page (auto-vacuum off)
        put32(out, 56, self.text_encoding);
        put32(out, 60, 0); // user version
        put32(out, 64, 0); // incremental vacuum
        put32(out, 68, 0); // application id
        for b in out.iter_mut().take(92).skip(72) {
            *b = 0; // 20 reserved bytes
        }
        put32(out, 92, self.change_counter); // version-valid-for
        put32(out, 96, SQLITE_VERSION_NUMBER);
    }
}

fn be32(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

fn put32(out: &mut [u8], off: usize, v: u32) {
    out[off..off + 4].copy_from_slice(&v.to_be_bytes());
}
