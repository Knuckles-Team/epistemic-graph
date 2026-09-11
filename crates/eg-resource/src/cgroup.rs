use super::{cgroup_metadata_is_malformed, join_cgroup_paths, parse_cgroup_v1_cpu_quota, CpuLimit};

pub(super) fn finite_cpu_limit(quota_us: Option<u64>, period_us: Option<u64>) -> CpuLimit {
    match (quota_us, period_us) {
        (Some(quota_us), Some(period_us)) if quota_us > 0 && period_us > 0 => CpuLimit::Limited {
            quota_us,
            period_us,
        },
        _ => CpuLimit::Malformed,
    }
}

/// Read and validate the two kernel metadata files shared by CPU and memory probes.
pub(super) fn read_cgroup_metadata() -> Option<(String, String)> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    if cgroup_metadata_is_malformed(&cgroup) {
        return None;
    }
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    Some((cgroup, mountinfo))
}

pub(super) fn read_cgroup_file(path: &str) -> Result<Option<String>, ()> {
    match std::fs::read_to_string(path) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(()),
    }
}

pub(super) fn read_cpu_v1_pair(
    quota_path: &str,
    period_path: &str,
) -> Result<Option<CpuLimit>, ()> {
    match read_cgroup_file(quota_path)? {
        Some(quota) => read_cgroup_file(period_path)?
            .ok_or(())
            .map(|period| Some(parse_cgroup_v1_cpu_quota(&quota, &period))),
        None => match std::fs::metadata(period_path) {
            Ok(_) => Err(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(()),
        },
    }
}

pub(super) fn resolve_cgroup_mount_paths(
    mountinfo: &str,
    controller: &str,
    want_v2: bool,
    member_path: &str,
    filename: &str,
) -> Option<Vec<String>> {
    for line in mountinfo.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let before_fields: Vec<_> = before.split_whitespace().collect();
        let after_fields: Vec<_> = after.split_whitespace().collect();
        if before_fields.len() < 6 || after_fields.len() < 3 {
            continue;
        }
        let mut options = before_fields[5]
            .split(',')
            .chain(after_fields[2].split(','));
        let matches = if want_v2 {
            after_fields[0] == "cgroup2"
        } else {
            after_fields[0] == "cgroup" && options.any(|value| value == controller)
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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_file_pairs_preserve_missing_and_partial_failure_semantics() {
        let missing_quota = "/definitely/not/a/cgroup/quota";
        let missing_period = "/definitely/not/a/cgroup/period";
        assert_eq!(
            read_cpu_v1_pair(missing_quota, missing_period),
            Ok(None),
            "an absent v1 pair is an unconfigured hierarchy level"
        );
        assert_eq!(
            read_cpu_v1_pair(missing_quota, "/"),
            Err(()),
            "a present period without its quota must fail closed"
        );
        assert_eq!(
            read_cpu_v1_pair("/", missing_period),
            Err(()),
            "a missing period after a readable quota must fail closed"
        );
    }
}
