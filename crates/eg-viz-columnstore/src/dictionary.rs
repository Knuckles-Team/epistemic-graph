//! Dictionary encoding for `ColumnType::Categorical` columns (D-VZ-1 lane V1
//! "dictionary categoricals").
//!
//! One [`Dictionary`] is shared by every chunk of a categorical column: a chunk's
//! encoded bytes hold `u32` codes (see [`crate::chunk`]), never the string values
//! themselves, so a finite-cardinality string column costs `O(distinct values +
//! row_count * 4 bytes)` rather than `O(sum of string lengths)`.

use std::collections::HashMap;

/// `u32::MAX` marks a null slot in a categorical column's code array — no real
/// dictionary entry ever takes this code (see [`Dictionary::intern`]).
pub const NULL_CODE: u32 = u32::MAX;

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Dictionary {
    values: Vec<String>,
    index: HashMap<String, u32>,
}

impl Dictionary {
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern `value`, returning its stable code. Repeated interning of the same
    /// string always returns the same code (the whole point of a dictionary: the
    /// string is stored once, referenced many times).
    pub fn intern(&mut self, value: &str) -> u32 {
        if let Some(&code) = self.index.get(value) {
            return code;
        }
        let code = self.values.len() as u32;
        assert!(
            code != NULL_CODE,
            "dictionary exceeded u32::MAX - 1 distinct values"
        );
        self.values.push(value.to_string());
        self.index.insert(value.to_string(), code);
        code
    }

    pub fn resolve(&self, code: u32) -> Option<&str> {
        self.values.get(code as usize).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_values_reuse_the_same_code() {
        let mut dict = Dictionary::new();
        let a1 = dict.intern("continent:asia");
        let b = dict.intern("continent:europe");
        let a2 = dict.intern("continent:asia");
        assert_eq!(a1, a2);
        assert_ne!(a1, b);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn resolve_round_trips_intern() {
        let mut dict = Dictionary::new();
        let code = dict.intern("hello");
        assert_eq!(dict.resolve(code), Some("hello"));
        assert_eq!(dict.resolve(999), None);
    }
}
