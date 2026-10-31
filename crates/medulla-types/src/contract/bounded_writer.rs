//! Bounded streaming writers shared by pure contract encoders.

/// A small `Write` adapter that refuses bytes beyond a caller-owned bound.
///
/// Serialization domains provide their own error text because it is part of
/// their public validation contract; the byte admission behavior is shared
/// here so each domain does not grow a subtly different writer.
pub(crate) struct BoundedWriter {
    bytes: Vec<u8>,
    maximum: usize,
    overflow_message: &'static str,
}

impl BoundedWriter {
    pub(crate) fn new(maximum: usize, overflow_message: &'static str) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
            overflow_message,
        }
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::io::Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(self.overflow_message));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
