//! `JobIntent` — a declarative TRIGGER + dedup key, additive to [`crate::model::AnalyticsJob`]
//! (CONCEPT:INT-P2-1, daemon-consolidation design Phase 3, `reports/daemon-consolidation-design.md`).
//!
//! Where an `AnalyticsJob` is a job someone already decided to run right now, a
//! `JobIntent` is a job DECLARED with a schedule — "run this on cron / interval /
//! manual" — so a caller no longer has to be the one deciding due-ness. This mirrors
//! agent-utilities' `core/schedule_engine.py` `ScheduleSpec` (cron/interval/adaptive
//! triggers, deterministic `sched:<name>:<minute>`-style dedup ids) one layer down
//! into the engine, reusing THIS crate's durable store rather than a generic graph
//! node — the Rust twin the design doc's §B.2 item 1 asks for.
//!
//! This module is pure (no redb, no I/O): [`Trigger::is_due`] and [`Trigger::tick_id`]
//! are deterministic functions of `(state, now_ms)`, unit-testable standalone. The
//! durable side — [`crate::store::JobStore::register_intent`] / `due_intents` /
//! `record_intent_tick`, backed by two new tables in the SAME `jobs.redb` — lives in
//! `store.rs` (it already owns the one `redb::Database` handle; redb holds an
//! exclusive per-process file lock, so a second table lives on the SAME store, not a
//! second `Database::open`).
//!
//! `AnalyticsJob` and its state machine (`model.rs` / `store.rs`'s existing methods)
//! are UNTOUCHED — this is purely additive.

use serde::{Deserialize, Serialize};

use crate::model::JobPolicy;

/// When a [`JobIntent`] becomes due (CONCEPT:INT-P2-1 — the Rust twin of AU's
/// `ScheduleSpec` trigger union).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trigger {
    /// Standard 5-field cron (`minute hour day-of-month month day-of-week`), UTC,
    /// evaluated at minute granularity. Each field supports `*` (any), an exact
    /// non-negative integer, a comma-separated list of integers, or a `*/N` step —
    /// the subset every entry in AU's own `deploy/schedules.yml`-style crons
    /// actually uses. Anything richer (`a-b` ranges, month/weekday names, step on a
    /// list) is an intentional, documented gap: [`Trigger::is_due`] returns `false`
    /// FOREVER for an unparsable expression rather than guessing (same "documented
    /// follow-up, not silently wrong" convention as the engine's own R-tree/spatial
    /// index-pushdown note in `AGENTS.md`).
    Cron(String),
    /// Fire every `secs` seconds since the last run (or immediately if never run).
    Interval { secs: u64 },
    /// Never auto-fires (`is_due` is always `false`) — a caller drives it explicitly
    /// via [`crate::store::JobStore::record_intent_tick`]'s `force` path. Mirrors an
    /// AU schedule registered with `enabled: false`: it still exists as a durable
    /// registry entry, it just never self-fires.
    Manual,
}

impl Trigger {
    /// Whether this trigger is due to fire, given the intent's last recorded run (if
    /// any) and the current time. Pure: no I/O, no interpretation of `now_ms` beyond
    /// UTC calendar math for `Cron`.
    ///
    /// `Cron` deliberately ignores `last_run_ms` here — repeated evaluation within
    /// the SAME matching minute is a caller/coalesce concern, not a trigger concern;
    /// [`crate::store::JobStore::record_intent_tick`]'s deterministic per-minute tick
    /// id (via [`Self::tick_id`]) is what prevents a double-fire, the same two-level
    /// split (`is_due` vs. dedup ledger) the design doc's §A.3 describes for AU.
    pub fn is_due(&self, last_run_ms: Option<i64>, now_ms: i64) -> bool {
        match self {
            Trigger::Interval { secs } => {
                if *secs == 0 {
                    return false;
                }
                match last_run_ms {
                    None => true,
                    Some(t) => now_ms >= t.saturating_add((*secs as i64).saturating_mul(1000)),
                }
            }
            Trigger::Manual => false,
            Trigger::Cron(expr) => match parse_cron(expr) {
                Some(fields) => cron_matches(&fields, now_ms),
                None => false,
            },
        }
    }

    /// A deterministic idempotency-ledger key for the tick window `now_ms` falls in
    /// (CONCEPT:INT-P2-1 determinism — mirrors `AnalyticsJob::result_ref` being a
    /// pure function of lineage, not of a call counter). Two concurrent evaluators
    /// checking the SAME intent within the SAME window compute the SAME id, so
    /// [`crate::store::JobStore::record_intent_tick`]'s idempotency claim collapses
    /// them to exactly one winner — the engine-native analogue of AU's
    /// `sched:<name>:<minute>` coalesce id.
    pub fn tick_id(&self, name: &str, now_ms: i64) -> String {
        match self {
            Trigger::Interval { secs } => {
                let window_ms = (*secs).max(1) as i64 * 1000;
                format!("intent:{name}:interval:{}", now_ms / window_ms)
            }
            Trigger::Cron(_) => format!("intent:{name}:cron:{}", now_ms / 60_000),
            // Manual has no natural coalesce window — every explicit trigger is its
            // own tick (millisecond-resolution key), matching "a caller decides".
            Trigger::Manual => format!("intent:{name}:manual:{now_ms}"),
        }
    }
}

/// A parsed cron field: which integer values it matches.
enum Field {
    Any,
    List(Vec<u32>),
    Step(u32),
}

impl Field {
    fn matches(&self, value: u32) -> bool {
        match self {
            Field::Any => true,
            Field::List(vs) => vs.contains(&value),
            Field::Step(n) => *n > 0 && value.is_multiple_of(*n),
        }
    }
}

fn parse_field(s: &str) -> Option<Field> {
    let s = s.trim();
    if s == "*" {
        return Some(Field::Any);
    }
    if let Some(rest) = s.strip_prefix("*/") {
        return rest.parse::<u32>().ok().map(Field::Step);
    }
    let mut values = Vec::new();
    for part in s.split(',') {
        values.push(part.trim().parse::<u32>().ok()?);
    }
    if values.is_empty() {
        None
    } else {
        Some(Field::List(values))
    }
}

/// Parse a 5-field `minute hour dom month dow` cron expression. `None` ⇒ unsupported
/// syntax (see [`Trigger::Cron`]'s doc) — the caller treats that as permanently
/// not-due rather than guessing.
fn parse_cron(expr: &str) -> Option<[Field; 5]> {
    let parts: Vec<&str> = expr.split_whitespace().collect();
    if parts.len() != 5 {
        return None;
    }
    Some([
        parse_field(parts[0])?,
        parse_field(parts[1])?,
        parse_field(parts[2])?,
        parse_field(parts[3])?,
        parse_field(parts[4])?,
    ])
}

fn cron_matches(fields: &[Field; 5], now_ms: i64) -> bool {
    let (_year, month, day, hour, minute, weekday) = civil_from_unix_ms(now_ms);
    fields[0].matches(minute)
        && fields[1].matches(hour)
        && fields[2].matches(day)
        && fields[3].matches(month)
        && fields[4].matches(weekday)
}

/// UTC calendar decomposition of a Unix-epoch millisecond timestamp, dependency-free
/// (no `chrono` in this workspace): `(year, month[1-12], day[1-31], hour[0-23],
/// minute[0-59], weekday[0=Sunday..6=Saturday])`. Uses Howard Hinnant's
/// `civil_from_days` algorithm (public-domain, proleptic Gregorian, valid for the
/// full `i64` range) — see
/// <http://howardhinnant.github.io/date_algorithms.html#civil_from_days>.
fn civil_from_unix_ms(now_ms: i64) -> (i64, u32, u32, u32, u32, u32) {
    let total_secs = now_ms.div_euclid(1000);
    let days = total_secs.div_euclid(86_400);
    let secs_of_day = total_secs.rem_euclid(86_400);
    let hour = (secs_of_day / 3600) as u32;
    let minute = ((secs_of_day % 3600) / 60) as u32;

    // Howard Hinnant civil_from_days: `z` = days since 1970-01-01.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };

    // 1970-01-01 (days = 0) was a Thursday; Sunday = 0.
    let weekday = (days.rem_euclid(7) + 4).rem_euclid(7) as u32;

    (year, month, day, hour, minute, weekday)
}

/// The durable `JobIntent` record (CONCEPT:INT-P2-1): a job DECLARED with a
/// [`Trigger`] rather than driven by an external caller, plus the SAME tenancy/
/// governance shape (`policy`) `AnalyticsJob` already carries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobIntent {
    /// Unique registry key (mirrors an AU `:Schedule` node's `name`).
    pub name: String,
    pub trigger: Trigger,
    pub policy: JobPolicy,
    /// Advisory enable flag — mirrors AU's `enabled: false` schedules.yml entries;
    /// a disabled intent still exists as a registry entry but never reports due.
    pub enabled: bool,
    pub last_run_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl JobIntent {
    /// Construct a fresh, enabled intent that has never run.
    pub fn new(name: impl Into<String>, trigger: Trigger, policy: JobPolicy) -> Self {
        let now = crate::store::now_ms();
        Self {
            name: name.into(),
            trigger,
            policy,
            enabled: true,
            last_run_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    /// Whether this intent is due to fire right now (`enabled` AND the trigger says so).
    pub fn is_due(&self, now_ms: i64) -> bool {
        self.enabled && self.trigger.is_due(self.last_run_ms, now_ms)
    }
}

/// Proof-of-concept (daemon-consolidation design Phase 3): express the engine's own
/// off-by-default cold-tenant idle-offload sweep (`EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS`,
/// `src/server/persistence/cold_offload.rs`, wired in `src/main.rs`) as a
/// [`JobIntent`] — proving the engine CAN self-schedule a durable job kind that today
/// only runs inside a hardcoded `tokio::time::interval` loop, WITHOUT changing what
/// actually drives that loop (this constructor is a declarative record only; the
/// live sweep keeps running exactly as it does today — see `AGENTS.md`'s daemon
/// Phase 4 note on why the live cutover is a separate, later step).
///
/// `window_secs` is the SAME value the live sweep reads from
/// `EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS` — the intent's `Interval` trigger fires on the
/// identical cadence the sweep already uses. The intent starts `enabled: false`
/// (opt-in registration only) so registering it is never itself a behavior change —
/// consistent with cold_offload's own "absent or 0 ⇒ disabled" default.
pub fn cold_offload_intent(window_secs: u64) -> JobIntent {
    let mut intent = JobIntent::new(
        "cold_offload",
        Trigger::Interval { secs: window_secs },
        JobPolicy {
            tenant: "__engine__".to_string(),
            actor: "engine:cold_offload".to_string(),
            purpose: "hibernate idle graphs past EPISTEMIC_GRAPH_COLD_OFFLOAD_SECS".to_string(),
            priority: -10, // background, per the design doc's PriorityClass mapping
            quota_cpu_ms: None,
            deadline_unix_ms: None,
        },
    );
    intent.enabled = false;
    intent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_due_when_never_run() {
        let t = Trigger::Interval { secs: 60 };
        assert!(t.is_due(None, 1_000));
    }

    #[test]
    fn interval_not_due_until_window_elapses() {
        let t = Trigger::Interval { secs: 60 };
        let last = 1_000_000i64;
        assert!(!t.is_due(Some(last), last + 59_000));
        assert!(t.is_due(Some(last), last + 60_000));
        assert!(t.is_due(Some(last), last + 61_000));
    }

    #[test]
    fn interval_zero_secs_is_never_due() {
        let t = Trigger::Interval { secs: 0 };
        assert!(!t.is_due(None, 1_000));
    }

    #[test]
    fn manual_is_never_due() {
        let t = Trigger::Manual;
        assert!(!t.is_due(None, 0));
        assert!(!t.is_due(Some(0), i64::MAX));
    }

    #[test]
    fn interval_tick_id_is_stable_within_window_and_changes_across() {
        let t = Trigger::Interval { secs: 60 };
        let a = t.tick_id("x", 1_000);
        let b = t.tick_id("x", 59_000);
        let c = t.tick_id("x", 61_000);
        assert_eq!(a, b, "same 60s window must collapse to the same tick id");
        assert_ne!(c, a, "the next window must produce a different tick id");
    }

    #[test]
    fn civil_from_unix_ms_epoch_is_1970_01_01_thursday() {
        // 1970-01-01T00:00:00Z was a Thursday (weekday 4, Sunday=0).
        let (y, mo, d, h, mi, wd) = civil_from_unix_ms(0);
        assert_eq!((y, mo, d, h, mi, wd), (1970, 1, 1, 0, 0, 4));
    }

    #[test]
    fn civil_from_unix_ms_known_date() {
        // 2024-03-15T13:37:00Z (a Friday). Epoch seconds computed independently:
        // days from 1970-01-01 to 2024-03-15 = 19797; + 13:37:00 = 13*3600+37*60.
        let days = 19_797i64;
        let secs_of_day = 13 * 3600 + 37 * 60;
        let ms = (days * 86_400 + secs_of_day) * 1000;
        let (y, mo, d, h, mi, wd) = civil_from_unix_ms(ms);
        assert_eq!((y, mo, d, h, mi), (2024, 3, 15, 13, 37));
        assert_eq!(wd, 5, "2024-03-15 was a Friday");
    }

    #[test]
    fn cron_every_5_minutes_matches_only_multiples() {
        let t = Trigger::Cron("*/5 * * * *".to_string());
        // 2024-03-15T13:35:00Z — minute 35 is a multiple of 5.
        let due_ms = (19_797 * 86_400 + 13 * 3600 + 35 * 60) * 1000;
        assert!(t.is_due(None, due_ms));
        // Minute 36 is not.
        let not_due_ms = (19_797 * 86_400 + 13 * 3600 + 36 * 60) * 1000;
        assert!(!t.is_due(None, not_due_ms));
    }

    #[test]
    fn cron_exact_field_list_matches() {
        // Fires at minute 0 of hours 2 and 14 every day.
        let t = Trigger::Cron("0 2,14 * * *".to_string());
        let at_2am = (19_797 * 86_400 + 2 * 3600) * 1000;
        let at_3am = (19_797 * 86_400 + 3 * 3600) * 1000;
        assert!(t.is_due(None, at_2am));
        assert!(!t.is_due(None, at_3am));
    }

    #[test]
    fn cron_unparsable_expression_is_never_due() {
        let t = Trigger::Cron("@daily".to_string());
        assert!(!t.is_due(None, 0));
        assert!(!t.is_due(None, i64::MAX / 2));
    }

    #[test]
    fn cold_offload_intent_matches_the_live_sweeps_defaults() {
        let intent = cold_offload_intent(3600);
        assert_eq!(intent.name, "cold_offload");
        assert_eq!(intent.trigger, Trigger::Interval { secs: 3600 });
        // Registration alone must never turn the sweep on.
        assert!(!intent.enabled);
        assert!(!intent.is_due(0));
    }
}
