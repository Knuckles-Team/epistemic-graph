//! Cap all native preview I/O, including allocator recovery and Drop writes.

use super::*;

#[derive(Debug)]
pub(super) struct BoundedPreviewBackend {
    inner: FileBackend,
    reservation: File,
    maximum: u64,
    poison: Arc<AtomicBool>,
}

impl BoundedPreviewBackend {
    pub(super) fn new(
        inner: FileBackend,
        reservation: File,
        maximum: u64,
        poison: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            reservation,
            maximum,
            poison,
        }
    }

    fn record<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.poison.store(true, Ordering::Release);
        }
        result
    }

    fn check_end(&self, offset: u64, count: u64) -> io::Result<()> {
        let valid = !self.poison.load(Ordering::Acquire)
            && offset
                .checked_add(count)
                .is_some_and(|end| end <= self.maximum);
        self.record(if valid {
            Ok(())
        } else {
            Err(io::Error::other(
                "SQL checkpoint preview I/O exceeds its budget or is poisoned",
            ))
        })
    }
}

impl StorageBackend for BoundedPreviewBackend {
    fn len(&self) -> io::Result<u64> {
        let size = self.record(self.inner.len())?;
        self.check_end(0, size)?;
        Ok(size)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.check_end(offset, out.len() as u64)?;
        self.record(self.inner.read(offset, out))
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.check_end(0, size)?;
        let old = self.len()?;
        self.record(self.inner.set_len(size))?;
        if size < old {
            self.record(reserve(&self.reservation, self.maximum))?;
        }
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.record(self.inner.sync_data())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.check_end(offset, data.len() as u64)?;
        self.record(self.inner.write(offset, data))
    }

    fn close(&self) -> io::Result<()> {
        self.record(self.inner.close())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_and_overflow_are_refused_before_modifying_preview() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("preview.redb");
        let attempts: [fn(&BoundedPreviewBackend) -> io::Result<()>; 2] = [
            |backend| backend.write(u64::MAX, b"overflow"),
            |backend| backend.set_len(9),
        ];
        for attempt in attempts {
            std::fs::write(&path, b"original").unwrap();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let (inner, reservation) = exclusive_backend(file, &path).unwrap();
            let poison = Arc::new(AtomicBool::new(false));
            let backend = BoundedPreviewBackend::new(inner, reservation, 8, poison.clone());
            assert!(attempt(&backend).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), b"original");
            assert!(poison.load(Ordering::Acquire));
            assert_eq!(std::fs::metadata(&path).unwrap().len(), 8);
        }
    }

    #[test]
    fn delegated_io_failure_survives_backend_drop() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("preview.redb");
        std::fs::write(&path, b"original").unwrap();
        let (inner, reservation) = exclusive_backend(File::open(&path).unwrap(), &path).unwrap();
        let poison = Arc::new(AtomicBool::new(false));
        let backend = BoundedPreviewBackend::new(inner, reservation, 8, poison.clone());
        assert!(backend.write(0, b"changed!").is_err());
        drop(backend);
        assert!(poison.load(Ordering::Acquire));
        assert_eq!(std::fs::read(&path).unwrap(), b"original");
    }
}
