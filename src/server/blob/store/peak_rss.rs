//! Per-test peak-RSS measurement for the blob bounded-memory tests (GOC-70 rule 5).
//!
//! `VmHWM` is a process-lifetime high-water mark, so "peak now minus peak before" is
//! NOT a per-test reading: a sibling test that already drove the mark higher (the two
//! bounded-memory tests run back to back in one process) makes a later test's growth
//! read as ~0 and pass vacuously. [`PeakRssWindow::open`] resets the mark to the
//! current RSS first (`/proc/self/clear_refs` value `5`), so the growth it reports is
//! this test's own peak over this test's own baseline. It panics rather than returning
//! 0 when the kernel interface is unavailable -- a memory test that cannot measure must
//! fail loudly (GOC-70 rule 4).

pub(crate) struct PeakRssWindow {
    baseline_mb: u64,
}

impl PeakRssWindow {
    pub(crate) fn open() -> Self {
        std::fs::write("/proc/self/clear_refs", "5")
            .expect("reset the process peak-RSS high-water mark via /proc/self/clear_refs");
        Self {
            baseline_mb: status_mb("VmHWM:"),
        }
    }

    pub(crate) fn growth_mb(&self) -> u64 {
        status_mb("VmHWM:").saturating_sub(self.baseline_mb)
    }
}

fn status_mb(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kb| kb.parse::<u64>().ok())
        .map(|kb| kb / 1024)
        .unwrap_or_else(|| panic!("/proc/self/status has no parseable {field} line"))
}
