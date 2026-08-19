//! Process resource capacity resolution shared by the engine and optional providers.
//!
//! Linux containers commonly expose the host's CPU count and `MemTotal` through
//! `/proc` while enforcing a smaller cgroup quota.  Auto-sizing from those host
//! values can therefore oversubscribe the pod.  This leaf crate is deliberately
//! dependency-free so lower workspace crates (for example the optional ASR
//! provider) can use the same parser without depending on the facade.
//!
//! The resolver has one explicit safety policy:
//!
//! * a finite cgroup limit is always intersected with the host observation;
//! * malformed limits use bounded fallback values, never an unbounded host value;
//! * automatic consumers reserve 10% of effective CPU and 20% of effective RAM;
//! * explicit consumer values are upper bounds only and must be clamped by the
//!   caller to the corresponding automatic value.
//!
//! A minimum of one CPU and one byte is retained so a constrained process can
//! still make progress.  Fractional CPU quotas below one CPU consequently use a
//! single scheduler lane; they are still bounded by the quota's floor rather than
//! rounded up to a second lane.

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;

/// Reserved CPU headroom percentage for automatic derived limits.
pub const CPU_HEADROOM_PERCENT: u64 = 10;
/// Reserved memory headroom percentage for automatic derived limits.
pub const MEMORY_HEADROOM_PERCENT: u64 = 20;
/// RAM fallback when `/proc/meminfo` is unavailable.
pub const UNKNOWN_RAM_BYTES: u64 = GIB;
/// RAM fallback after a cgroup limit file is malformed.
pub const MALFORMED_RAM_BYTES: u64 = 256 * MIB;
/// Estimated resident bytes per graph node used by the facade's node cap.
pub const BYTES_PER_NODE_EST: u64 = 2048;
/// Upper bound for the automatic resident node cap.
pub const MAX_NODE_CAP: usize = 100_000_000;

/// State of a cgroup CPU limit source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CpuLimit {
    /// A finite quota/period pair in the kernel's microsecond units.
    Limited { quota_us: u64, period_us: u64 },
    /// The cgroup explicitly reports no CPU quota.
    Unlimited,
    /// The controller/file is not present.
    Unavailable,
    /// The controller is present but its content is not trustworthy.
    Malformed,
}

/// State of a cgroup memory limit source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryLimit {
    /// A finite byte limit.
    Limited(u64),
    /// The cgroup explicitly reports no memory quota.
    Unlimited,
    /// The controller/file is not present.
    Unavailable,
    /// The controller is present but its content is not trustworthy.
    Malformed,
}

/// Coarse host class used by the facade's logging and policy messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Small single-board class (at most 2 GiB).
    Pi,
    /// Commodity server/workstation class (at most 32 GiB).
    Node,
    /// Large box class (more than 32 GiB).
    BigBox,
}

/// Effective host/cgroup capacity after the shared automatic headroom policy.
///
/// `cpus` and `total_ram_bytes` are the minimum of the host observation and a
/// finite cgroup limit, with 10% CPU and 20% RAM reserved already. Consumers
/// must use these fields (or the identity accessors below), never re-read host
/// `/proc` values independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capacity {
    pub cpus: usize,
    pub total_ram_bytes: u64,
    pub tier: Tier,
}

/// Parse a cgroup v2 `cpu.max` value (`<quota> <period>` or `max <period>`).
pub fn parse_cgroup_v2_cpu_max(content: &str) -> CpuLimit {
    let mut fields = content.split_whitespace();
    let Some(quota) = fields.next() else {
        return CpuLimit::Malformed;
    };
    let Some(period) = fields.next() else {
        return CpuLimit::Malformed;
    };
    if fields.next().is_some() {
        return CpuLimit::Malformed;
    }
    if quota == "max" {
        return match period.parse::<u64>() {
            Ok(period_us) if period_us > 0 => CpuLimit::Unlimited,
            _ => CpuLimit::Malformed,
        };
    }
    let Ok(quota_us) = quota.parse::<u64>() else {
        return CpuLimit::Malformed;
    };
    let Ok(period_us) = period.parse::<u64>() else {
        return CpuLimit::Malformed;
    };
    if quota_us == 0 || period_us == 0 {
        return CpuLimit::Malformed;
    }
    CpuLimit::Limited {
        quota_us,
        period_us,
    }
}

/// Parse cgroup v1's `cpu.cfs_quota_us` and `cpu.cfs_period_us` values.
pub fn parse_cgroup_v1_cpu_quota(quota: &str, period: &str) -> CpuLimit {
    let Ok(quota_us) = quota.trim().parse::<i64>() else {
        return CpuLimit::Malformed;
    };
    let Ok(period_us) = period.trim().parse::<u64>() else {
        return CpuLimit::Malformed;
    };
    if period_us == 0 {
        return CpuLimit::Malformed;
    }
    if quota_us == -1 {
        return CpuLimit::Unlimited;
    }
    if quota_us <= 0 {
        return CpuLimit::Malformed;
    }
    CpuLimit::Limited {
        quota_us: quota_us as u64,
        period_us,
    }
}

/// Parse a cgroup v2 `memory.max` value (`max` or a byte count).
pub fn parse_cgroup_v2_memory_max(content: &str) -> MemoryLimit {
    let value = content.trim();
    if value == "max" {
        return MemoryLimit::Unlimited;
    }
    let Ok(bytes) = value.parse::<u64>() else {
        return MemoryLimit::Malformed;
    };
    if bytes == 0 {
        return MemoryLimit::Malformed;
    }
    MemoryLimit::Limited(bytes)
}

/// Parse a cgroup v1 `memory.limit_in_bytes` value.
pub fn parse_cgroup_v1_memory_limit(content: &str) -> MemoryLimit {
    let Ok(bytes) = content.trim().parse::<u64>() else {
        return MemoryLimit::Malformed;
    };
    if bytes == 0 {
        return MemoryLimit::Malformed;
    }
    if bytes >= (1_u64 << 60) {
        MemoryLimit::Unlimited
    } else {
        MemoryLimit::Limited(bytes)
    }
}

/// Parse `/proc/meminfo`'s `MemTotal` line into bytes.
pub fn parse_mem_total(content: &str) -> Option<u64> {
    content.lines().find_map(|line| {
        let rest = line.strip_prefix("MemTotal:")?;
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        kb.checked_mul(1024)
    })
}

fn read_cgroup_cpu_limit() -> CpuLimit {
    let cgroup = match std::fs::read_to_string("/proc/self/cgroup") {
        Ok(value) => value,
        // An unreadable membership file is not evidence that the host capacity
        // is safe to use.  Fail closed so a restricted container never widens
        // an automatic CPU budget on a probe error.
        Err(_) => return CpuLimit::Malformed,
    };
    if cgroup_metadata_is_malformed(&cgroup) {
        return CpuLimit::Malformed;
    }
    let mountinfo = match std::fs::read_to_string("/proc/self/mountinfo") {
        Ok(value) => value,
        Err(_) => return CpuLimit::Malformed,
    };
    if let Some(paths) = resolve_cgroup_files(&cgroup, &mountinfo, "cpu", "cpu.max") {
        return read_cpu_v2_paths(&paths);
    }
    let quota_paths = resolve_cgroup_files(&cgroup, &mountinfo, "cpu", "cpu.cfs_quota_us");
    let period_paths = resolve_cgroup_files(&cgroup, &mountinfo, "cpu", "cpu.cfs_period_us");
    match (quota_paths, period_paths) {
        (Some(quota_paths), Some(period_paths)) => read_cpu_v1_paths(&quota_paths, &period_paths),
        (Some(_), None) | (None, Some(_)) => CpuLimit::Malformed,
        (None, None) if proc_cgroup_path(&cgroup, "cpu").is_some() => CpuLimit::Malformed,
        (None, None) => CpuLimit::Unavailable,
    }
}

fn read_cgroup_memory_limit() -> MemoryLimit {
    let cgroup = match std::fs::read_to_string("/proc/self/cgroup") {
        Ok(value) => value,
        // See the CPU probe above: probe failures must not restore an
        // unbounded host-memory observation.
        Err(_) => return MemoryLimit::Malformed,
    };
    if cgroup_metadata_is_malformed(&cgroup) {
        return MemoryLimit::Malformed;
    }
    let mountinfo = match std::fs::read_to_string("/proc/self/mountinfo") {
        Ok(value) => value,
        Err(_) => return MemoryLimit::Malformed,
    };
    if let Some(paths) = resolve_cgroup_files(&cgroup, &mountinfo, "memory", "memory.max") {
        return read_memory_paths(&paths, parse_cgroup_v2_memory_max);
    }
    if let Some(paths) =
        resolve_cgroup_files(&cgroup, &mountinfo, "memory", "memory.limit_in_bytes")
    {
        return read_memory_paths(&paths, parse_cgroup_v1_memory_limit);
    }
    if proc_cgroup_path(&cgroup, "memory").is_some() {
        MemoryLimit::Malformed
    } else {
        MemoryLimit::Unavailable
    }
}

fn read_cpu_v2_paths(paths: &[String]) -> CpuLimit {
    let mut limits = Vec::with_capacity(paths.len());
    for path in paths {
        let value = match std::fs::read_to_string(path) {
            Ok(value) => value,
            // The cgroup v2 root commonly omits controller files, and a
            // controller may be enabled only below an intermediate ancestor.
            // A missing file therefore means that this level contributes no
            // finite limit; other I/O failures are still fail-closed.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return CpuLimit::Malformed,
        };
        limits.push(parse_cgroup_v2_cpu_max(&value));
    }
    aggregate_cpu_limits(&limits)
}

fn read_cpu_v1_paths(quota_paths: &[String], period_paths: &[String]) -> CpuLimit {
    if quota_paths.len() != period_paths.len() {
        return CpuLimit::Malformed;
    }
    let mut limits = Vec::with_capacity(quota_paths.len());
    for (quota_path, period_path) in quota_paths.iter().zip(period_paths) {
        let quota = match std::fs::read_to_string(quota_path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A missing pair is an unconfigured v1 level.  If only one
                // member of the pair exists, the mismatch is malformed and
                // must not widen the budget.
                match std::fs::metadata(period_path) {
                    Ok(_) => return CpuLimit::Malformed,
                    Err(period_error) if period_error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return CpuLimit::Malformed,
                }
                continue;
            }
            Err(_) => return CpuLimit::Malformed,
        };
        let period = match std::fs::read_to_string(period_path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return CpuLimit::Malformed;
            }
            Err(_) => return CpuLimit::Malformed,
        };
        limits.push(parse_cgroup_v1_cpu_quota(&quota, &period));
    }
    aggregate_cpu_limits(&limits)
}

fn read_memory_paths(paths: &[String], parse: fn(&str) -> MemoryLimit) -> MemoryLimit {
    let mut limits = Vec::with_capacity(paths.len());
    for path in paths {
        let value = match std::fs::read_to_string(path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return MemoryLimit::Malformed,
        };
        limits.push(parse(&value));
    }
    aggregate_memory_limits(&limits)
}

/// Aggregate hierarchical CPU controller values from the mount root to the
/// process leaf. A finite ancestor limit remains effective even if a child
/// reports `max`; malformed or unreadable controller files fail closed.
fn aggregate_cpu_limits(limits: &[CpuLimit]) -> CpuLimit {
    if limits.is_empty() {
        return CpuLimit::Unavailable;
    }
    let mut finite = None;
    let mut saw_unlimited = false;
    for limit in limits {
        match *limit {
            CpuLimit::Limited {
                quota_us,
                period_us,
            } if period_us > 0 && quota_us > 0 => {
                let replace = finite.map_or(true, |(current_quota, current_period)| {
                    (quota_us as u128) * (current_period as u128)
                        < (current_quota as u128) * (period_us as u128)
                });
                if replace {
                    finite = Some((quota_us, period_us));
                }
            }
            CpuLimit::Unlimited => saw_unlimited = true,
            CpuLimit::Unavailable | CpuLimit::Malformed | CpuLimit::Limited { .. } => {
                return CpuLimit::Malformed;
            }
        }
    }
    finite.map_or_else(
        || {
            if saw_unlimited {
                CpuLimit::Unlimited
            } else {
                CpuLimit::Unavailable
            }
        },
        |(quota_us, period_us)| CpuLimit::Limited {
            quota_us,
            period_us,
        },
    )
}

/// Aggregate hierarchical memory controller values. The smallest finite
/// ancestor remains effective; malformed or unreadable files fail closed.
fn aggregate_memory_limits(limits: &[MemoryLimit]) -> MemoryLimit {
    if limits.is_empty() {
        return MemoryLimit::Unavailable;
    }
    let mut finite = None;
    let mut saw_unlimited = false;
    for limit in limits {
        match *limit {
            MemoryLimit::Limited(bytes) if bytes > 0 => {
                finite = Some(finite.map_or(bytes, |current: u64| current.min(bytes)));
            }
            MemoryLimit::Unlimited => saw_unlimited = true,
            MemoryLimit::Unavailable | MemoryLimit::Malformed | MemoryLimit::Limited(_) => {
                return MemoryLimit::Malformed;
            }
        }
    }
    finite.map_or_else(
        || {
            if saw_unlimited {
                MemoryLimit::Unlimited
            } else {
                MemoryLimit::Unavailable
            }
        },
        MemoryLimit::Limited,
    )
}

/// Resolve one controller file under the cgroup mount containing this process.
///
/// `proc_cgroup` and `mountinfo` are parameters rather than hardcoded paths so
/// nested systemd/Kubernetes cgroups and both v1/v2 layouts can be tested without
/// mutating the host hierarchy. The result is rejected on traversal components.
pub fn resolve_cgroup_file(
    proc_cgroup: &str,
    mountinfo: &str,
    controller: &str,
    filename: &str,
) -> Option<String> {
    resolve_cgroup_files(proc_cgroup, mountinfo, controller, filename)
        .and_then(|paths| paths.last().cloned())
}

/// Resolve every controller file from the cgroup mount root to this process's
/// cgroup. Reading all returned paths is required for hierarchical safety: a
/// leaf can report `max` while a systemd/Kubernetes ancestor is finite.
pub fn resolve_cgroup_files(
    proc_cgroup: &str,
    mountinfo: &str,
    controller: &str,
    filename: &str,
) -> Option<Vec<String>> {
    if controller.is_empty()
        || controller.contains('/')
        || controller.contains('\0')
        || controller.chars().any(char::is_whitespace)
        || filename.is_empty()
        || filename.contains('/')
        || filename.contains('\0')
        || filename == "."
        || filename == ".."
    {
        return None;
    }
    let (hierarchy, member_path) = proc_cgroup_path(proc_cgroup, controller)?;
    let want_v2 = hierarchy == "0";
    for line in mountinfo.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let before_fields: Vec<_> = before.split_whitespace().collect();
        let after_fields: Vec<_> = after.split_whitespace().collect();
        if before_fields.len() < 6 || after_fields.len() < 3 {
            continue;
        }
        let filesystem = after_fields[0];
        let options = before_fields[5]
            .split(',')
            .chain(after_fields[2].split(','));
        let matches = if want_v2 {
            filesystem == "cgroup2"
        } else {
            filesystem == "cgroup" && options.clone().any(|value| value == controller)
        };
        if matches {
            if let Some(paths) =
                join_cgroup_paths(before_fields[4], before_fields[3], member_path, filename)
            {
                return Some(paths);
            }
        }
    }
    None
}

fn proc_cgroup_path<'a>(content: &'a str, controller: &str) -> Option<(&'a str, &'a str)> {
    // Hybrid hosts expose a unified `0::/…` line alongside v1 controller
    // lines.  Prefer the controller-specific v1 hierarchy when it exists;
    // only use the unified line as the fallback for a genuine v2 controller.
    let mut unified = None;
    for line in content.lines() {
        let mut fields = line.splitn(3, ':');
        let (Some(hierarchy), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if hierarchy == "0" && controllers.is_empty() {
            unified = Some((hierarchy, path));
            continue;
        }
        if controllers
            .split(',')
            .any(|candidate| candidate == controller)
        {
            return Some((hierarchy, path));
        }
    }
    unified
}

/// Validate the small, kernel-defined `/proc/self/cgroup` grammar before a
/// missing controller match is treated as `Unavailable`.  A valid file may
/// legitimately contain several unrelated v1 hierarchies plus one unified v2
/// line; a malformed line, an empty file, or traversal in a cgroup path must
/// fail closed instead of restoring host-wide capacity.
fn cgroup_metadata_is_malformed(content: &str) -> bool {
    let mut saw_entry = false;
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        saw_entry = true;
        let mut fields = line.splitn(3, ':');
        let (Some(hierarchy), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            return true;
        };
        if hierarchy.parse::<u64>().is_err() || cgroup_components(path).is_none() {
            return true;
        }
        if !controllers.is_empty()
            && controllers
                .split(',')
                .any(|controller| controller.is_empty())
        {
            return true;
        }
    }
    !saw_entry
}

fn join_cgroup_paths(
    mount_point: &str,
    mount_root: &str,
    member_path: &str,
    filename: &str,
) -> Option<Vec<String>> {
    if !mount_point.starts_with('/')
        || !mount_root.starts_with('/')
        || !member_path.starts_with('/')
    {
        return None;
    }
    let root_components = cgroup_components(mount_root)?;
    let member_components = cgroup_components(member_path)?;
    if member_components.len() < root_components.len()
        || member_components[..root_components.len()] != root_components[..]
    {
        return None;
    }
    if filename == "." || filename == ".." {
        return None;
    }
    let relative_components = &member_components[root_components.len()..];
    let mount_prefix = mount_point.trim_end_matches('/');
    let mut paths = Vec::with_capacity(relative_components.len() + 1);
    for depth in 0..=relative_components.len() {
        let mut path = if mount_prefix.is_empty() {
            "/".to_string()
        } else {
            mount_prefix.to_string()
        };
        for component in &relative_components[..depth] {
            path.push('/');
            path.push_str(component);
        }
        path.push('/');
        path.push_str(filename);
        paths.push(path);
    }
    Some(paths)
}

fn cgroup_components(path: &str) -> Option<Vec<&str>> {
    if !path.starts_with('/') {
        return None;
    }
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Some(Vec::new());
    }
    let components: Vec<_> = trimmed.split('/').collect();
    if components
        .iter()
        .any(|component| component.is_empty() || *component == "." || *component == "..")
    {
        return None;
    }
    Some(components)
}

/// Read the host's CPU and cgroup CPU source and apply the shared CPU policy.
pub fn detect_cpu_budget(max_threads: usize, explicit_upper_bound: Option<usize>) -> usize {
    let host = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1);
    resolve_cpu_budget(
        host,
        read_cgroup_cpu_limit(),
        max_threads,
        explicit_upper_bound,
    )
}

/// Resolve a CPU budget from fixture inputs. A finite quota is floored (never
/// rounded up) before host intersection; automatic headroom is then reserved.
pub fn resolve_cpu_budget(
    host_cpus: usize,
    cgroup: CpuLimit,
    max_threads: usize,
    explicit_upper_bound: Option<usize>,
) -> usize {
    let host = host_cpus.max(1);
    let cgroup_cpus = match cgroup {
        CpuLimit::Limited {
            quota_us,
            period_us,
        } if period_us > 0 => usize::try_from((quota_us / period_us).max(1)).unwrap_or(usize::MAX),
        CpuLimit::Unlimited | CpuLimit::Unavailable => host,
        CpuLimit::Malformed => 1,
        CpuLimit::Limited { .. } => 1,
    };
    let effective = host.min(cgroup_cpus).max(1);
    let reserved = reserve_cpu(effective);
    let bounded = reserved.min(max_threads.max(1));
    explicit_upper_bound
        .filter(|value| *value > 0)
        .map_or(bounded, |value| bounded.min(value))
        .max(1)
}

/// Resolve RAM from fixture inputs. A malformed cgroup source uses a bounded
/// 256 MiB fallback, while absent/unlimited sources retain the host observation.
pub fn resolve_memory_bytes(host_ram_bytes: u64, cgroup: MemoryLimit) -> u64 {
    let host = if host_ram_bytes == 0 {
        UNKNOWN_RAM_BYTES
    } else {
        host_ram_bytes
    };
    let raw = match cgroup {
        MemoryLimit::Limited(bytes) => host.min(bytes.max(1)),
        MemoryLimit::Unlimited | MemoryLimit::Unavailable => host,
        MemoryLimit::Malformed => host.min(MALFORMED_RAM_BYTES),
    };
    reserve_ram(raw.max(1))
}

/// Apply the explicit CPU headroom policy.
pub fn reserve_cpu(cpus: usize) -> usize {
    cpus.max(1)
        .saturating_mul(100 - CPU_HEADROOM_PERCENT as usize)
        .checked_div(100)
        .unwrap_or(1)
        .max(1)
}

/// Apply the explicit memory headroom policy.
pub fn reserve_ram(bytes: u64) -> u64 {
    bytes
        .saturating_mul(100 - MEMORY_HEADROOM_PERCENT)
        .checked_div(100)
        .unwrap_or(1)
        .max(1)
}

/// Bound an explicit consumer value to the automatic cgroup-aware value. This
/// is intentionally a named policy helper rather than an implicit `min` at
/// each call site: operator overrides may lower a cap, but cannot silently
/// widen a constrained process beyond the measured safe default.
pub fn bound_explicit(value: usize, automatic: usize) -> usize {
    value.max(1).min(automatic.max(1))
}

/// Detect the effective host/cgroup capacity once for this process.
///
/// The values size process-lifetime runtime pools and bounded queues. Cache the
/// snapshot so connection admission and other repeated consumers do not parse
/// `/proc/self/mountinfo` and walk the cgroup hierarchy on their hot paths.
/// Deployments that change pod limits in place must restart the process so all
/// process-lifetime pools are rebuilt from one coherent capacity snapshot.
pub fn detect_capacity() -> Capacity {
    static CAPACITY: std::sync::OnceLock<Capacity> = std::sync::OnceLock::new();
    *CAPACITY.get_or_init(detect_capacity_uncached)
}

fn detect_capacity_uncached() -> Capacity {
    let host_cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1);
    let host_ram = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|value| parse_mem_total(&value))
        .unwrap_or(0);
    let cpu_limit = read_cgroup_cpu_limit();
    let memory_limit = read_cgroup_memory_limit();
    let cpus = resolve_cpu_budget(host_cpus, cpu_limit, usize::MAX, None);
    let total_ram_bytes = resolve_memory_bytes(host_ram, memory_limit);
    Capacity {
        cpus,
        total_ram_bytes,
        tier: tier_for(total_ram_bytes),
    }
}

/// Host RAM only, retained as the facade's public `/proc` diagnostic seam.
pub fn total_ram_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|value| parse_mem_total(&value))
}

/// Classify a RAM size into a coarse policy tier.
pub fn tier_for(total_ram_bytes: u64) -> Tier {
    if total_ram_bytes == 0 {
        Tier::Node
    } else if total_ram_bytes <= 2 * GIB {
        Tier::Pi
    } else if total_ram_bytes <= 32 * GIB {
        Tier::Node
    } else {
        Tier::BigBox
    }
}

/// Translate RAM into a bounded resident node cap. The floor is one node rather
/// than a fixed 50k, so a small cgroup cannot receive an automatic cap larger than
/// its effective memory budget.
pub fn default_node_cap(total_ram_bytes: u64) -> usize {
    let raw_ram = if total_ram_bytes == 0 {
        reserve_ram(UNKNOWN_RAM_BYTES)
    } else {
        reserve_ram(total_ram_bytes)
    };
    node_cap_from_reserved_ram(raw_ram)
}

fn node_cap_from_reserved_ram(reserved_ram_bytes: u64) -> usize {
    let raw = reserved_ram_bytes / 2 / BYTES_PER_NODE_EST;
    raw.clamp(1, MAX_NODE_CAP as u64) as usize
}

impl Capacity {
    /// CPU lanes available to an automatic consumer after reserved headroom.
    pub fn reserved_cpus(&self) -> usize {
        self.cpus.max(1)
    }

    /// RAM available to an automatic consumer after reserved headroom.
    pub fn reserved_ram_bytes(&self) -> u64 {
        self.total_ram_bytes.max(1)
    }

    pub fn max_inflight(&self) -> usize {
        self.reserved_cpus().saturating_mul(64).clamp(64, 8192)
    }

    pub fn read_reserved(&self) -> usize {
        (self.max_inflight() / 8).clamp(8, 1024)
    }

    pub fn writer_queue(&self) -> usize {
        self.reserved_cpus().saturating_mul(256).clamp(256, 65_536)
    }

    pub fn per_connection_inflight(&self) -> usize {
        self.reserved_cpus().saturating_mul(8).clamp(8, 1_024)
    }

    pub fn mutation_snapshot_bytes(&self) -> usize {
        (self.reserved_ram_bytes() / 16).clamp(1, 2 * 1024 * 1024 * 1024) as usize
    }

    pub fn node_cap(&self) -> usize {
        if self.total_ram_bytes == 0 {
            default_node_cap(0)
        } else {
            node_cap_from_reserved_ram(self.reserved_ram_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_cpu_fixture_is_floored_and_unlimited_is_explicit() {
        assert_eq!(
            parse_cgroup_v2_cpu_max("150000 100000\n"),
            CpuLimit::Limited {
                quota_us: 150_000,
                period_us: 100_000
            }
        );
        assert_eq!(parse_cgroup_v2_cpu_max("max 100000\n"), CpuLimit::Unlimited);
        assert_eq!(
            parse_cgroup_v2_cpu_max("not a cgroup file"),
            CpuLimit::Malformed
        );
        assert_eq!(
            parse_cgroup_v2_cpu_max("200000 100000 extra"),
            CpuLimit::Malformed
        );
    }

    #[test]
    fn v1_cpu_fixture_distinguishes_unlimited_and_malformed() {
        assert_eq!(
            parse_cgroup_v1_cpu_quota("200000", "100000"),
            CpuLimit::Limited {
                quota_us: 200_000,
                period_us: 100_000
            }
        );
        assert_eq!(
            parse_cgroup_v1_cpu_quota("-1", "100000"),
            CpuLimit::Unlimited
        );
        assert_eq!(
            parse_cgroup_v1_cpu_quota("200000", "0"),
            CpuLimit::Malformed
        );
    }

    #[test]
    fn v2_and_v1_memory_fixture_bounds_host_and_reject_malformed() {
        assert_eq!(parse_cgroup_v2_memory_max("max\n"), MemoryLimit::Unlimited);
        assert_eq!(
            parse_cgroup_v2_memory_max("134217728\n"),
            MemoryLimit::Limited(134_217_728)
        );
        assert_eq!(
            parse_cgroup_v2_memory_max("1152921504606846976"),
            MemoryLimit::Limited(1_u64 << 60)
        );
        assert_eq!(
            parse_cgroup_v1_memory_limit("18446744073709551615"),
            MemoryLimit::Unlimited
        );
        assert_eq!(
            parse_cgroup_v2_memory_max("not-a-limit"),
            MemoryLimit::Malformed
        );
        assert_eq!(
            resolve_memory_bytes(8 * GIB, MemoryLimit::Limited(256 * MIB)),
            256 * MIB * 80 / 100
        );
    }

    #[test]
    fn every_cpu_derived_cap_respects_a_two_cpu_cgroup_fixture() {
        let cpus = resolve_cpu_budget(
            64,
            CpuLimit::Limited {
                quota_us: 200_000,
                period_us: 100_000,
            },
            usize::MAX,
            None,
        );
        assert_eq!(
            cpus, 1,
            "10% reserved headroom leaves one lane from two CPUs"
        );
        let c = Capacity {
            cpus,
            total_ram_bytes: 8 * GIB,
            tier: Tier::Node,
        };
        assert!(c.reserved_cpus() <= 2);
        assert!(c.max_inflight() <= 2 * 64);
        assert!(c.writer_queue() <= 2 * 256);
    }

    #[test]
    fn malformed_limits_are_bounded_not_unlimited() {
        let cpus = resolve_cpu_budget(64, CpuLimit::Malformed, usize::MAX, None);
        assert_eq!(cpus, 1);
        let ram = resolve_memory_bytes(247 * GIB, MemoryLimit::Malformed);
        assert_eq!(ram, 256 * MIB * 80 / 100);
    }

    #[test]
    fn explicit_upper_bound_cannot_escape_auto_capacity() {
        assert_eq!(
            resolve_cpu_budget(
                64,
                CpuLimit::Limited {
                    quota_us: 200_000,
                    period_us: 100_000,
                },
                4,
                Some(64),
            ),
            1
        );
    }

    #[test]
    fn explicit_bound_helper_allows_lowering_but_not_widening() {
        assert_eq!(bound_explicit(8, 4), 4);
        assert_eq!(bound_explicit(2, 4), 2);
        assert_eq!(bound_explicit(0, 4), 1);
    }

    #[test]
    fn tiny_memory_does_not_get_a_fixed_large_node_floor() {
        assert!(default_node_cap(64 * MIB) < 50_000);
        assert!(default_node_cap(64 * MIB) > 0);
    }

    #[test]
    fn nested_v2_mount_resolves_the_process_cgroup_not_mount_root() {
        let proc_cgroup = "0::/kubepods.slice/pod-a/container.scope\n";
        let mountinfo = "42 1 0:30 / /sys/fs/cgroup rw,nosuid,nodev - cgroup2 cgroup rw\n";
        assert_eq!(
            resolve_cgroup_file(proc_cgroup, mountinfo, "cpu", "cpu.max"),
            Some("/sys/fs/cgroup/kubepods.slice/pod-a/container.scope/cpu.max".to_string())
        );
        assert_eq!(
            resolve_cgroup_files(proc_cgroup, mountinfo, "cpu", "cpu.max"),
            Some(vec![
                "/sys/fs/cgroup/cpu.max".to_string(),
                "/sys/fs/cgroup/kubepods.slice/cpu.max".to_string(),
                "/sys/fs/cgroup/kubepods.slice/pod-a/cpu.max".to_string(),
                "/sys/fs/cgroup/kubepods.slice/pod-a/container.scope/cpu.max".to_string(),
            ])
        );
    }

    #[test]
    fn nested_v1_mount_selects_the_controller_mount() {
        let proc_cgroup = "2:cpu,cpuacct:/kubepods/pod-a\n3:memory:/kubepods/pod-a\n";
        let mountinfo = concat!(
            "43 1 0:31 /kubepods /sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu,cpuacct\n",
            "44 1 0:32 /kubepods /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
        );
        assert_eq!(
            resolve_cgroup_file(proc_cgroup, mountinfo, "cpu", "cpu.cfs_quota_us"),
            Some("/sys/fs/cgroup/cpu/pod-a/cpu.cfs_quota_us".to_string())
        );
        assert_eq!(
            resolve_cgroup_file(proc_cgroup, mountinfo, "memory", "memory.limit_in_bytes"),
            Some("/sys/fs/cgroup/memory/pod-a/memory.limit_in_bytes".to_string())
        );
        assert_eq!(
            resolve_cgroup_files(proc_cgroup, mountinfo, "memory", "memory.limit_in_bytes"),
            Some(vec![
                "/sys/fs/cgroup/memory/memory.limit_in_bytes".to_string(),
                "/sys/fs/cgroup/memory/pod-a/memory.limit_in_bytes".to_string(),
            ])
        );
    }

    #[test]
    fn hybrid_cgroup_prefers_v1_controller_over_unified_fallback() {
        let proc_cgroup = "0::/unified\n2:cpu,cpuacct:/legacy/pod-a\n";
        let mountinfo = concat!(
            "42 1 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
            "43 1 0:31 /legacy /sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu,cpuacct\n",
        );
        assert_eq!(
            resolve_cgroup_file(proc_cgroup, mountinfo, "cpu", "cpu.cfs_quota_us"),
            Some("/sys/fs/cgroup/cpu/pod-a/cpu.cfs_quota_us".to_string())
        );
    }

    #[test]
    fn namespace_root_cgroup_resolves_mount_root_without_empty_component() {
        let proc_cgroup = "0::/\n";
        let mountinfo = "42 1 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n";
        assert_eq!(
            resolve_cgroup_files(proc_cgroup, mountinfo, "cpu", "cpu.max"),
            Some(vec!["/sys/fs/cgroup/cpu.max".to_string()])
        );
    }

    #[test]
    fn hierarchical_limits_keep_finite_ancestors_over_unlimited_leaves() {
        assert_eq!(
            aggregate_cpu_limits(&[
                CpuLimit::Limited {
                    quota_us: 100_000,
                    period_us: 100_000,
                },
                CpuLimit::Unlimited,
            ]),
            CpuLimit::Limited {
                quota_us: 100_000,
                period_us: 100_000,
            }
        );
        assert_eq!(
            aggregate_memory_limits(&[
                MemoryLimit::Limited(512 * MIB),
                MemoryLimit::Unlimited,
                MemoryLimit::Limited(256 * MIB),
            ]),
            MemoryLimit::Limited(256 * MIB)
        );
        assert_eq!(
            aggregate_cpu_limits(&[
                parse_cgroup_v1_cpu_quota("100000", "100000"),
                parse_cgroup_v1_cpu_quota("-1", "100000"),
            ]),
            CpuLimit::Limited {
                quota_us: 100_000,
                period_us: 100_000,
            }
        );
        assert_eq!(
            aggregate_memory_limits(&[
                parse_cgroup_v1_memory_limit("536870912"),
                parse_cgroup_v1_memory_limit("18446744073709551615"),
            ]),
            MemoryLimit::Limited(536_870_912)
        );
    }

    #[test]
    fn malformed_or_unreadable_ancestor_fails_closed() {
        assert_eq!(
            aggregate_cpu_limits(&[CpuLimit::Unlimited, CpuLimit::Malformed]),
            CpuLimit::Malformed
        );
        assert_eq!(
            aggregate_memory_limits(&[MemoryLimit::Limited(512 * MIB), MemoryLimit::Malformed]),
            MemoryLimit::Malformed
        );
        assert_eq!(
            read_cpu_v2_paths(&["/".to_string()]),
            CpuLimit::Malformed,
            "a resolved but unreadable cgroup file must not fall back to host capacity"
        );
        assert_eq!(
            read_memory_paths(&["/".to_string()], parse_cgroup_v2_memory_max),
            MemoryLimit::Malformed
        );
        assert_eq!(
            read_cpu_v2_paths(&["/definitely/not/a/cgroup/file".to_string()]),
            CpuLimit::Unavailable,
            "a missing controller file is an unconfigured hierarchy level, not a host-wide limit"
        );
    }

    #[test]
    fn malformed_or_traversing_cgroup_membership_fails_closed() {
        let mountinfo = "42 1 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n";
        assert!(cgroup_metadata_is_malformed(""));
        assert!(cgroup_metadata_is_malformed("not-a-cgroup-line\n"));
        assert!(cgroup_metadata_is_malformed("0::/../escape\n"));
        assert!(!cgroup_metadata_is_malformed("0::/pod\n"));
        assert_eq!(
            resolve_cgroup_file("0::/../escape\n", mountinfo, "cpu", "cpu.max"),
            None
        );
        assert_eq!(
            resolve_cgroup_file("not-a-cgroup-line\n", mountinfo, "cpu", "cpu.max"),
            None
        );
        assert_eq!(
            resolve_cgroup_file("0::/pod\n", mountinfo, "cpu", "../cpu.max"),
            None
        );
    }
}
