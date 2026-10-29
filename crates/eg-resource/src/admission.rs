//! Admission pacing, priority classes, cognitive scheduling and model capacity
//! control (MEDULLA-PLACEMENT-R750).
//!
//! Every controller is a pure state machine advanced by explicit events with
//! an injected clock (`now_ms`), so a replay of the same events reproduces the
//! same states exactly. Foreground work pauses background work through one
//! throttle fact. A tripped breaker narrows capacity and never raises it above
//! the declared ceiling.

use std::collections::BTreeMap;

/// A refused admission input, with its stable code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionError {
    pub code: &'static str,
    pub detail: String,
}

impl AdmissionError {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.detail)
    }
}

impl std::error::Error for AdmissionError {}

pub const CONCURRENCY_ABOVE_CELL_CEILING: &str = "CONCURRENCY_ABOVE_CELL_CEILING";
pub const UNKNOWN_PRIORITY_CLASS: &str = "UNKNOWN_PRIORITY_CLASS";
pub const BREAKER_TRANSITION_WITHOUT_OUTCOME: &str = "BREAKER_TRANSITION_WITHOUT_OUTCOME";

/// The closed set of work priority classes, highest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PriorityClass {
    Interactive,
    Foreground,
    Background,
    Maintenance,
}

impl PriorityClass {
    pub const ALL: [Self; 4] = [
        Self::Interactive,
        Self::Foreground,
        Self::Background,
        Self::Maintenance,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Foreground => "foreground",
            Self::Background => "background",
            Self::Maintenance => "maintenance",
        }
    }

    /// Parse a class name; a name outside the closed set is refused.
    pub fn parse(name: &str) -> Result<Self, AdmissionError> {
        Self::ALL
            .into_iter()
            .find(|class| class.as_str() == name)
            .ok_or_else(|| AdmissionError::new(UNKNOWN_PRIORITY_CLASS, format!("{name:?}")))
    }

    fn is_background(self) -> bool {
        matches!(self, Self::Background | Self::Maintenance)
    }
}

/// Token-bucket claim pacing with a foreground throttle fact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimPacer {
    capacity: u64,
    refill_per_second: u64,
    tokens_milli: u64,
    last_ms: u64,
    /// The one throttle fact: while set, background classes do not claim.
    foreground_active: bool,
}

impl ClaimPacer {
    pub fn new(capacity: u64, refill_per_second: u64, now_ms: u64) -> Self {
        Self {
            capacity,
            refill_per_second,
            tokens_milli: capacity * 1000,
            last_ms: now_ms,
            foreground_active: false,
        }
    }

    pub fn set_foreground_active(&mut self, active: bool) {
        self.foreground_active = active;
    }

    fn refill(&mut self, now_ms: u64) {
        let elapsed = now_ms.saturating_sub(self.last_ms);
        self.last_ms = self.last_ms.max(now_ms);
        self.tokens_milli =
            (self.tokens_milli + elapsed * self.refill_per_second).min(self.capacity * 1000);
    }

    /// Whether `class` may claim one unit now.
    pub fn try_claim(&mut self, class: PriorityClass, now_ms: u64) -> bool {
        self.refill(now_ms);
        if class.is_background() && self.foreground_active {
            return false;
        }
        if self.tokens_milli < 1000 {
            return false;
        }
        self.tokens_milli -= 1000;
        true
    }
}

/// A per-process inference budget for one window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InferenceBudget {
    pub tokens_per_window: u64,
}

/// One agent process the cognitive scheduler runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentProcess {
    pub id: String,
    pub class: PriorityClass,
    pub used_tokens: u64,
}

/// The scheduler's answer to one inference request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CognitiveDecision {
    Admit,
    /// Admit after preempting the named lower-class process.
    Preempt {
        victim: String,
    },
    Refuse,
}

/// Cognitive scheduling with preemption over a shared inference budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CognitiveScheduler {
    budget: InferenceBudget,
    running: BTreeMap<String, AgentProcess>,
}

impl CognitiveScheduler {
    pub fn new(budget: InferenceBudget) -> Self {
        Self {
            budget,
            running: BTreeMap::new(),
        }
    }

    fn used(&self) -> u64 {
        self.running.values().map(|p| p.used_tokens).sum()
    }

    /// Admit `process` with `cost` tokens, preempting the lowest-class (then
    /// largest) running process of a strictly lower class when needed.
    pub fn request(&mut self, process: AgentProcess, cost: u64) -> CognitiveDecision {
        if self.used() + cost <= self.budget.tokens_per_window {
            self.admit(process, cost);
            return CognitiveDecision::Admit;
        }
        let victim = self
            .running
            .values()
            .filter(|p| {
                p.class > process.class
                    && self.used() - p.used_tokens + cost <= self.budget.tokens_per_window
            })
            .max_by(|a, b| {
                a.class
                    .cmp(&b.class)
                    .then(a.used_tokens.cmp(&b.used_tokens))
                    .then(b.id.cmp(&a.id))
            })
            .map(|p| p.id.clone());
        match victim {
            Some(victim) => {
                self.running.remove(&victim);
                self.admit(process, cost);
                CognitiveDecision::Preempt { victim }
            }
            None => CognitiveDecision::Refuse,
        }
    }

    fn admit(&mut self, mut process: AgentProcess, cost: u64) {
        process.used_tokens += cost;
        self.running.insert(process.id.clone(), process);
    }

    pub fn running(&self) -> impl Iterator<Item = &AgentProcess> {
        self.running.values()
    }
}

/// The observed outcome of one call through a breaker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Failure,
}

/// Endpoint circuit-breaker state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CircuitState {
    Closed { consecutive_failures: u32 },
    Open { since_ms: u64 },
    HalfOpen,
}

/// A breaker with a failure threshold and an open cooldown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CircuitBreaker {
    pub state: CircuitState,
    pub failure_threshold: u32,
    pub cooldown_ms: u64,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown_ms: u64) -> Self {
        Self {
            state: CircuitState::Closed {
                consecutive_failures: 0,
            },
            failure_threshold,
            cooldown_ms,
        }
    }

    /// The open breaker half-opens once its cooldown has passed.
    pub fn tick(&mut self, now_ms: u64) {
        if let CircuitState::Open { since_ms } = self.state {
            if now_ms.saturating_sub(since_ms) >= self.cooldown_ms {
                self.state = CircuitState::HalfOpen;
            }
        }
    }

    /// Advance on an observed outcome; a transition with no observed outcome
    /// is refused.
    pub fn observe(
        &mut self,
        outcome: Option<Outcome>,
        now_ms: u64,
    ) -> Result<CircuitState, AdmissionError> {
        let outcome = outcome.ok_or_else(|| {
            AdmissionError::new(BREAKER_TRANSITION_WITHOUT_OUTCOME, "no observed outcome")
        })?;
        self.state = match (self.state, outcome) {
            (CircuitState::Open { since_ms }, _) => CircuitState::Open { since_ms },
            (_, Outcome::Success) => CircuitState::Closed {
                consecutive_failures: 0,
            },
            (CircuitState::HalfOpen, Outcome::Failure) => CircuitState::Open { since_ms: now_ms },
            (
                CircuitState::Closed {
                    consecutive_failures,
                },
                Outcome::Failure,
            ) => {
                let failures = consecutive_failures + 1;
                if failures >= self.failure_threshold {
                    CircuitState::Open { since_ms: now_ms }
                } else {
                    CircuitState::Closed {
                        consecutive_failures: failures,
                    }
                }
            }
        };
        Ok(self.state)
    }
}

/// Per-model adaptive concurrency: additive increase, multiplicative decrease
/// against a latency target, never above the declared ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelCapacityController {
    limit: u32,
    min: u32,
    ceiling: u32,
    target_latency_ms: u64,
}

impl ModelCapacityController {
    /// Refuse a ceiling above the declared cell ceiling.
    pub fn new(
        initial: u32,
        min: u32,
        ceiling: u32,
        cell_ceiling: u32,
        target_latency_ms: u64,
    ) -> Result<Self, AdmissionError> {
        if ceiling > cell_ceiling {
            return Err(AdmissionError::new(
                CONCURRENCY_ABOVE_CELL_CEILING,
                format!("ceiling {ceiling} exceeds the cell ceiling {cell_ceiling}"),
            ));
        }
        let min = min.max(1).min(ceiling.max(1));
        Ok(Self {
            limit: initial.clamp(min, ceiling.max(min)),
            min,
            ceiling: ceiling.max(min),
            target_latency_ms,
        })
    }

    /// One latency sample.
    pub fn on_latency(&mut self, latency_ms: u64) {
        self.limit = if latency_ms <= self.target_latency_ms {
            (self.limit + 1).min(self.ceiling)
        } else {
            (self.limit / 2).max(self.min)
        };
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    /// The limit narrowed by the breaker: open admits none, half-open one.
    pub fn effective_limit(&self, breaker: &CircuitBreaker) -> u32 {
        match breaker.state {
            CircuitState::Closed { .. } => self.limit,
            CircuitState::HalfOpen => self.limit.min(1),
            CircuitState::Open { .. } => 0,
        }
    }
}

/// A shared-GPU group's slot budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GpuGroupBudget {
    pub group: String,
    pub slots: u32,
    used: u32,
}

impl GpuGroupBudget {
    pub fn new(group: &str, slots: u32) -> Self {
        Self {
            group: group.to_string(),
            slots,
            used: 0,
        }
    }

    pub fn try_acquire(&mut self) -> bool {
        if self.used >= self.slots {
            return false;
        }
        self.used += 1;
        true
    }

    pub fn release(&mut self) {
        self.used = self.used.saturating_sub(1);
    }

    pub fn used(&self) -> u32 {
        self.used
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process(id: &str, class: PriorityClass) -> AgentProcess {
        AgentProcess {
            id: id.into(),
            class,
            used_tokens: 0,
        }
    }

    /// One replayable event trace over a controller and breaker.
    fn replay() -> Vec<(u32, u32, CircuitState)> {
        let mut controller = ModelCapacityController::new(4, 1, 8, 16, 100).unwrap();
        let mut breaker = CircuitBreaker::new(2, 1_000);
        let events: [(u64, Option<Outcome>, u64); 6] = [
            (50, Some(Outcome::Success), 0),
            (50, Some(Outcome::Success), 10),
            (500, Some(Outcome::Failure), 20),
            (500, Some(Outcome::Failure), 30),
            (60, None, 1_100),
            (60, Some(Outcome::Success), 1_200),
        ];
        let mut trace = Vec::new();
        for (latency, outcome, now) in events {
            controller.on_latency(latency);
            breaker.tick(now);
            if outcome.is_some() {
                breaker.observe(outcome, now).unwrap();
            }
            trace.push((
                controller.limit(),
                controller.effective_limit(&breaker),
                breaker.state,
            ));
        }
        trace
    }

    // spec: MEDULLA-PLACEMENT-R750.750
    #[test]
    fn the_admission_types_behave_and_the_golden_vector_replays() {
        let golden = vec![
            (
                5,
                5,
                CircuitState::Closed {
                    consecutive_failures: 0,
                },
            ),
            (
                6,
                6,
                CircuitState::Closed {
                    consecutive_failures: 0,
                },
            ),
            (
                3,
                3,
                CircuitState::Closed {
                    consecutive_failures: 1,
                },
            ),
            (1, 0, CircuitState::Open { since_ms: 30 }),
            (2, 1, CircuitState::HalfOpen),
            (
                3,
                3,
                CircuitState::Closed {
                    consecutive_failures: 0,
                },
            ),
        ];
        assert_eq!(replay(), golden);
        assert_eq!(replay(), replay(), "replay is exact");

        let mut scheduler = CognitiveScheduler::new(InferenceBudget {
            tokens_per_window: 100,
        });
        assert_eq!(
            scheduler.request(process("bg", PriorityClass::Background), 80),
            CognitiveDecision::Admit
        );
        assert_eq!(
            scheduler.request(process("ui", PriorityClass::Interactive), 50),
            CognitiveDecision::Preempt {
                victim: "bg".into()
            }
        );
        assert_eq!(
            scheduler.request(process("bg2", PriorityClass::Maintenance), 60),
            CognitiveDecision::Refuse
        );

        let mut gpu = GpuGroupBudget::new("a100-pool", 1);
        assert!(gpu.try_acquire());
        assert!(!gpu.try_acquire());
        gpu.release();
        assert_eq!(gpu.used(), 0);
    }

    // spec: MEDULLA-PLACEMENT-R750.751
    #[test]
    fn foreground_pauses_background_and_a_breaker_never_raises_capacity() {
        let mut pacer = ClaimPacer::new(2, 1, 0);
        pacer.set_foreground_active(true);
        assert!(!pacer.try_claim(PriorityClass::Background, 0));
        assert!(pacer.try_claim(PriorityClass::Foreground, 0));
        pacer.set_foreground_active(false);
        assert!(pacer.try_claim(PriorityClass::Background, 0));
        assert!(
            !pacer.try_claim(PriorityClass::Interactive, 0),
            "bucket empty"
        );
        assert!(
            pacer.try_claim(PriorityClass::Interactive, 1_000),
            "refilled by the injected clock"
        );

        let mut controller = ModelCapacityController::new(8, 1, 8, 8, 100).unwrap();
        for _ in 0..10 {
            controller.on_latency(1);
        }
        assert_eq!(controller.limit(), 8, "never above the ceiling");
        let mut breaker = CircuitBreaker::new(1, 10);
        for state in [
            CircuitState::Closed {
                consecutive_failures: 0,
            },
            CircuitState::HalfOpen,
            CircuitState::Open { since_ms: 0 },
        ] {
            breaker.state = state;
            assert!(controller.effective_limit(&breaker) <= controller.limit());
        }
    }

    // spec: MEDULLA-PLACEMENT-R750.752
    #[test]
    fn ceiling_class_and_outcome_refusals() {
        let refused = ModelCapacityController::new(4, 1, 32, 16, 100).unwrap_err();
        assert_eq!(refused.code, CONCURRENCY_ABOVE_CELL_CEILING);
        assert_eq!(
            PriorityClass::parse("urgent").unwrap_err().code,
            UNKNOWN_PRIORITY_CLASS
        );
        assert_eq!(
            PriorityClass::parse("background").unwrap(),
            PriorityClass::Background
        );
        let mut breaker = CircuitBreaker::new(2, 10);
        let before = breaker.state;
        assert_eq!(
            breaker.observe(None, 5).unwrap_err().code,
            BREAKER_TRANSITION_WITHOUT_OUTCOME
        );
        assert_eq!(
            breaker.state, before,
            "a refused transition changes nothing"
        );
    }
}
