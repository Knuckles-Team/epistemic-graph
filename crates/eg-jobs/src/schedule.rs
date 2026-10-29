//! Unified scheduling, leader election and cost-bounded resource allocation
//! (MEDULLA-PLACEMENT-R751).
//!
//! One schedule engine computes the due windows of cron, interval and one-shot
//! specs from an injected clock. Ticks are idempotent per schedule and window;
//! a tick replayed after acknowledgement is refused. Singleton daemons act only
//! under a live fence; a stale leader cannot act. Allocation slices never sum
//! above their budget.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const TICK_REPLAYED_AFTER_ACK: &str = "TICK_REPLAYED_AFTER_ACK";
pub const LEADER_FENCE_EXPIRED: &str = "LEADER_FENCE_EXPIRED";
pub const SLICE_SUM_OVER_BUDGET: &str = "SLICE_SUM_OVER_BUDGET";
pub const INVALID_SCHEDULE: &str = "INVALID_SCHEDULE";

const MINUTE_MS: u64 = 60_000;

/// A refused schedule input, with its stable code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleError {
    pub code: &'static str,
    pub detail: String,
}

impl ScheduleError {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for ScheduleError {}

/// When a schedule fires.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ScheduleSpec {
    /// `minute hour` cron fields (`*`, `n`, `*/n`, comma lists), evaluated per
    /// UTC minute.
    Cron {
        minute: String,
        hour: String,
    },
    Interval {
        every_ms: u64,
        start_ms: u64,
    },
    OneShot {
        at_ms: u64,
    },
}

fn field_matches(field: &str, value: u64, max: u64) -> Result<bool, ScheduleError> {
    let invalid = || ScheduleError::new(INVALID_SCHEDULE, format!("cron field {field:?}"));
    let mut hit = false;
    for part in field.split(',') {
        hit |= match part.strip_prefix("*/") {
            Some(step) => {
                let step: u64 = step.parse().map_err(|_| invalid())?;
                if step == 0 {
                    return Err(invalid());
                }
                value.is_multiple_of(step)
            }
            None if part == "*" => true,
            None => {
                let exact: u64 = part.parse().map_err(|_| invalid())?;
                if exact > max {
                    return Err(invalid());
                }
                value == exact
            }
        };
    }
    Ok(hit)
}

impl ScheduleSpec {
    /// Refuse a zero interval and malformed cron fields.
    pub fn validate(&self) -> Result<(), ScheduleError> {
        match self {
            Self::Interval { every_ms: 0, .. } => Err(ScheduleError::new(
                INVALID_SCHEDULE,
                "interval must be positive",
            )),
            Self::Cron { minute, hour } => {
                field_matches(minute, 0, 59)?;
                field_matches(hour, 0, 23)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// The window starts in `[from_ms, to_ms)` at which this spec fires.
    pub fn due_windows(&self, from_ms: u64, to_ms: u64) -> Result<Vec<u64>, ScheduleError> {
        self.validate()?;
        Ok(match self {
            Self::OneShot { at_ms } => (from_ms..to_ms)
                .contains(at_ms)
                .then_some(*at_ms)
                .into_iter()
                .collect(),
            Self::Interval { every_ms, start_ms } => {
                let first = if from_ms <= *start_ms {
                    *start_ms
                } else {
                    start_ms + (from_ms - start_ms).div_ceil(*every_ms) * every_ms
                };
                (0..)
                    .map(|k| first + k * every_ms)
                    .take_while(|t| *t < to_ms)
                    .collect()
            }
            Self::Cron { minute, hour } => {
                let mut due = Vec::new();
                let mut t = from_ms.div_ceil(MINUTE_MS) * MINUTE_MS;
                while t < to_ms {
                    let minutes = t / MINUTE_MS;
                    if field_matches(minute, minutes % 60, 59)?
                        && field_matches(hour, (minutes / 60) % 24, 23)?
                    {
                        due.push(t);
                    }
                    t += MINUTE_MS;
                }
                due
            }
        })
    }
}

/// One firing of one schedule for one window.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleTick {
    pub schedule_id: String,
    pub window_start_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TickStatus {
    /// First sight: run it.
    Fresh,
    /// Seen and not yet acknowledged: the same tick, run once.
    Duplicate,
}

/// The idempotent tick ledger.
#[derive(Clone, Debug, Default)]
pub struct TickLedger {
    ticks: BTreeMap<ScheduleTick, bool>,
}

impl TickLedger {
    pub fn record(&mut self, tick: &ScheduleTick) -> Result<TickStatus, ScheduleError> {
        match self.ticks.get(tick) {
            None => {
                self.ticks.insert(tick.clone(), false);
                Ok(TickStatus::Fresh)
            }
            Some(false) => Ok(TickStatus::Duplicate),
            Some(true) => Err(ScheduleError::new(
                TICK_REPLAYED_AFTER_ACK,
                format!("{} @ {}", tick.schedule_id, tick.window_start_ms),
            )),
        }
    }

    pub fn acknowledge(&mut self, tick: &ScheduleTick) {
        self.ticks.insert(tick.clone(), true);
    }
}

/// Fenced leadership of one singleton daemon.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonLeadership {
    pub daemon: String,
    pub holder: Option<String>,
    pub fence: u64,
    pub expires_ms: u64,
}

impl DaemonLeadership {
    pub fn new(daemon: &str) -> Self {
        Self {
            daemon: daemon.to_string(),
            holder: None,
            fence: 0,
            expires_ms: 0,
        }
    }

    /// Take or renew leadership. A new holder gets a higher fence; a live
    /// lease held by another host is not taken.
    pub fn acquire(&mut self, host: &str, now_ms: u64, ttl_ms: u64) -> Option<u64> {
        let live = now_ms < self.expires_ms;
        match self.holder.as_deref() {
            Some(current) if live && current != host => return None,
            Some(current) if current == host && live => {}
            _ => {
                self.fence += 1;
                self.holder = Some(host.to_string());
            }
        }
        self.expires_ms = now_ms + ttl_ms;
        Some(self.fence)
    }

    /// Whether `host` may act with `fence` now; a stale or expired fence is
    /// refused.
    pub fn authorize(&self, host: &str, fence: u64, now_ms: u64) -> Result<(), ScheduleError> {
        let current = self.holder.as_deref() == Some(host) && fence == self.fence;
        if current && now_ms < self.expires_ms {
            Ok(())
        } else {
            Err(ScheduleError::new(
                LEADER_FENCE_EXPIRED,
                format!(
                    "{host} fence {fence} (current {}) for {}",
                    self.fence, self.daemon
                ),
            ))
        }
    }
}

/// A per-process resource budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceBudget {
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocationSlice {
    pub owner: String,
    pub amount: u64,
}

/// Accept `slices` only when they sum within the budget.
pub fn allocate(
    budget: ResourceBudget,
    slices: Vec<AllocationSlice>,
) -> Result<Vec<AllocationSlice>, ScheduleError> {
    let sum = slices
        .iter()
        .try_fold(0u64, |acc, s| acc.checked_add(s.amount));
    match sum {
        Some(sum) if sum <= budget.total => Ok(slices),
        _ => Err(ScheduleError::new(
            SLICE_SUM_OVER_BUDGET,
            format!("slices exceed budget {}", budget.total),
        )),
    }
}

/// Release channel visibility: a viewer on a channel sees components on that
/// channel or a more stable one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseChannel {
    Stable,
    Beta,
    Nightly,
}

impl ReleaseChannel {
    pub fn sees(self, component: ReleaseChannel) -> bool {
        component <= self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(window: u64) -> ScheduleTick {
        ScheduleTick {
            schedule_id: "nightly-sweep".into(),
            window_start_ms: window,
        }
    }

    // spec: MEDULLA-PLACEMENT-R751.750
    #[test]
    fn schedule_types_round_trip_and_compute_due_windows() {
        let golden = r#"{"interval":{"every_ms":1000,"start_ms":500}}"#;
        let spec: ScheduleSpec = serde_json::from_str(golden).unwrap();
        assert_eq!(serde_json::to_string(&spec).unwrap(), golden);
        assert_eq!(spec.due_windows(0, 3_000).unwrap(), [500, 1_500, 2_500]);
        assert_eq!(spec.due_windows(1_500, 2_501).unwrap(), [1_500, 2_500]);
        let once = ScheduleSpec::OneShot { at_ms: 42 };
        assert_eq!(once.due_windows(0, 42).unwrap(), Vec::<u64>::new());
        assert_eq!(once.due_windows(42, 43).unwrap(), [42]);
        let cron = ScheduleSpec::Cron {
            minute: "*/30".into(),
            hour: "1".into(),
        };
        let day = 24 * 60 * MINUTE_MS;
        assert_eq!(
            cron.due_windows(0, day).unwrap(),
            [60 * MINUTE_MS, 90 * MINUTE_MS]
        );
        assert!(ReleaseChannel::Nightly.sees(ReleaseChannel::Stable));
        assert!(!ReleaseChannel::Stable.sees(ReleaseChannel::Beta));
    }

    // spec: MEDULLA-PLACEMENT-R751.751
    #[test]
    fn ticks_are_idempotent_leadership_is_fenced_and_slices_fit() {
        let mut ledger = TickLedger::default();
        assert_eq!(ledger.record(&tick(1)).unwrap(), TickStatus::Fresh);
        assert_eq!(ledger.record(&tick(1)).unwrap(), TickStatus::Duplicate);
        assert_eq!(ledger.record(&tick(2)).unwrap(), TickStatus::Fresh);

        let mut leadership = DaemonLeadership::new("reaper");
        let fence_a = leadership.acquire("host-a", 0, 100).unwrap();
        assert_eq!(
            leadership.acquire("host-b", 50, 100),
            None,
            "live lease held"
        );
        assert!(leadership.authorize("host-a", fence_a, 99).is_ok());
        let fence_b = leadership.acquire("host-b", 150, 100).unwrap();
        assert!(fence_b > fence_a);
        assert!(leadership.authorize("host-b", fence_b, 160).is_ok());

        let budget = ResourceBudget { total: 10 };
        let slices = vec![
            AllocationSlice {
                owner: "a".into(),
                amount: 4,
            },
            AllocationSlice {
                owner: "b".into(),
                amount: 6,
            },
        ];
        assert_eq!(
            allocate(budget, slices.clone()).unwrap(),
            slices,
            "at the budget"
        );
    }

    // spec: MEDULLA-PLACEMENT-R751.752
    #[test]
    fn replay_after_ack_stale_leader_and_over_budget_are_refused() {
        let mut ledger = TickLedger::default();
        ledger.record(&tick(1)).unwrap();
        ledger.acknowledge(&tick(1));
        assert_eq!(
            ledger.record(&tick(1)).unwrap_err().code,
            TICK_REPLAYED_AFTER_ACK
        );

        let mut leadership = DaemonLeadership::new("reaper");
        let old = leadership.acquire("host-a", 0, 100).unwrap();
        assert_eq!(
            leadership.authorize("host-a", old, 100).unwrap_err().code,
            LEADER_FENCE_EXPIRED
        );
        leadership.acquire("host-b", 200, 100).unwrap();
        assert_eq!(
            leadership.authorize("host-a", old, 210).unwrap_err().code,
            LEADER_FENCE_EXPIRED
        );

        let over = vec![
            AllocationSlice {
                owner: "a".into(),
                amount: 6,
            },
            AllocationSlice {
                owner: "b".into(),
                amount: 5,
            },
        ];
        assert_eq!(
            allocate(ResourceBudget { total: 10 }, over)
                .unwrap_err()
                .code,
            SLICE_SUM_OVER_BUDGET
        );
        let overflow = vec![
            AllocationSlice {
                owner: "a".into(),
                amount: u64::MAX,
            },
            AllocationSlice {
                owner: "b".into(),
                amount: 1,
            },
        ];
        assert!(allocate(ResourceBudget { total: u64::MAX }, overflow).is_err());
        assert_eq!(
            ScheduleSpec::Interval {
                every_ms: 0,
                start_ms: 0
            }
            .validate()
            .unwrap_err()
            .code,
            INVALID_SCHEDULE
        );
    }
}
