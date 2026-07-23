//! B-tree page header + cell-pointer conventions.
//! Leaf header = 8 bytes; interior header = 12 bytes (trailing 4-byte rightmost pointer).
//! Cells grow downward from the end of the page; the cell-pointer array (one big-endian
//! u16 per cell, in key order) grows upward immediately after the page header.

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    IndexInterior = 2,
    TableInterior = 5,
    IndexLeaf = 10,
    TableLeaf = 13,
}

impl PageType {
    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            2 => PageType::IndexInterior,
            5 => PageType::TableInterior,
            10 => PageType::IndexLeaf,
            13 => PageType::TableLeaf,
            other => return Err(Error::corrupt(format!("invalid page type {other}"))),
        })
    }

    pub fn header_len(self) -> usize {
        match self {
            PageType::TableLeaf | PageType::IndexLeaf => 8,
            PageType::TableInterior | PageType::IndexInterior => 12,
        }
    }
}

/// A parsed b-tree page header plus the byte offset where its cell-pointer array begins.
pub struct PageHeader {
    pub page_type: PageType,
    pub cell_count: u16,
    pub rightmost_ptr: Option<u32>,
    /// Offset within the page where the header (and thus the cell-pointer array) starts.
    /// 100 for page 1 (after the file header), 0 for every other page.
    pub header_offset: usize,
}

impl PageHeader {
    /// Parse the b-tree header at `header_offset` inside `page`.
    pub fn parse(page: &[u8], header_offset: usize) -> Result<Self> {
        if header_offset + 8 > page.len() {
            return Err(Error::corrupt("page too small for b-tree header"));
        }
        let page_type = PageType::from_u8(page[header_offset])?;
        let cell_count = u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]);
        let rightmost_ptr = match page_type {
            PageType::TableInterior | PageType::IndexInterior => {
                if header_offset + 12 > page.len() {
                    return Err(Error::corrupt("interior page too small for header"));
                }
                Some(u32::from_be_bytes([
                    page[header_offset + 8],
                    page[header_offset + 9],
                    page[header_offset + 10],
                    page[header_offset + 11],
                ]))
            }
            _ => None,
        };
        Ok(PageHeader {
            page_type,
            cell_count,
            rightmost_ptr,
            header_offset,
        })
    }

    /// Byte offset of the cell-pointer array (immediately after the header).
    pub fn cell_pointer_array_offset(&self) -> usize {
        self.header_offset + self.page_type.header_len()
    }

    /// The content offset (within the page) of cell `idx`, read from the pointer array.
    pub fn cell_pointer(&self, page: &[u8], idx: usize) -> Result<usize> {
        let off = self.cell_pointer_array_offset() + idx * 2;
        if off + 2 > page.len() {
            return Err(Error::corrupt("cell pointer out of range"));
        }
        Ok(u16::from_be_bytes([page[off], page[off + 1]]) as usize)
    }
}

/// Serialize a leaf page header (8 bytes) into `page` at `header_offset`.
pub fn write_leaf_header(
    page: &mut [u8],
    header_offset: usize,
    page_type: PageType,
    cell_count: u16,
    cell_content_area: usize,
) {
    page[header_offset] = page_type as u8;
    page[header_offset + 1] = 0; // first freeblock: none (bulk-packed page never fragments)
    page[header_offset + 2] = 0;
    page[header_offset + 3..header_offset + 5].copy_from_slice(&cell_count.to_be_bytes());
    // cell_content_area == 65536 is stored as 0; page_size <= 65536 so this handles the max.
    let cca: u16 = if cell_content_area == 65_536 {
        0
    } else {
        cell_content_area as u16
    };
    page[header_offset + 5..header_offset + 7].copy_from_slice(&cca.to_be_bytes());
    page[header_offset + 7] = 0; // fragmented free bytes
}

/// Serialize an interior page header (12 bytes) into `page` at `header_offset`.
pub fn write_interior_header(
    page: &mut [u8],
    header_offset: usize,
    page_type: PageType,
    cell_count: u16,
    cell_content_area: usize,
    rightmost_ptr: u32,
) {
    write_leaf_header(
        page,
        header_offset,
        page_type,
        cell_count,
        cell_content_area,
    );
    page[header_offset + 8..header_offset + 12].copy_from_slice(&rightmost_ptr.to_be_bytes());
}
