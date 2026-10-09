//! Join cardinality statistics for cross-source federation (CONCEPT:EG-KG.query.query-federation,
//! EG-FEDERATED-QUERY-R048.1 — typed-model slice of R048).
//!
//! R048 asks the planner to order joins across multiple foreign sources using LEARNED
//! cardinality statistics persisted as graph facts with their provenance, seeded by
//! registration-time probes (PostgreSQL table-size statistics, engine-reported
//! statistics, VoID descriptions). This slice builds only the typed model those
//! probes populate and the planner later reads — [`JoinCardinalityStatistic`] — plus
//! the provenance it must always carry ([`StatisticsProvenance`]) and the source
//! generation marker that lets a stale statistic be told apart from a fresh one after
//! the source is re-registered. Persistence as graph facts and planner join-ordering
//! use are follow-up slices (`.2`, `.3`, ...).
//!
//! A statistic is REFUSED without provenance: [`JoinCardinalityStatistic::try_new`]
//! takes `Option<StatisticsProvenance>` and returns [`StatsError::MissingProvenance`]
//! on `None`, so an un-attributed cardinality guess can never be constructed — every
//! value that reaches the planner traces back to the probe that produced it, per
//! FQR-05.

use std::fmt;

/// Which registration-time probe produced a [`JoinCardinalityStatistic`]. Each variant
/// names the concrete signal R048 lists: PostgreSQL table-size statistics, a foreign
/// engine's own reported statistics, or a VoID (Vocabulary of Interlinked Datasets)
/// description advertised by an RDF source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatisticsProvenance {
    /// Seeded from PostgreSQL's planner statistics for a table (e.g. `pg_class.reltuples`
    /// / `pg_stats`), naming the probed table.
    PostgresTableStats { table: String },
    /// Seeded from a foreign engine's own reported row/cardinality statistics (e.g.
    /// another epistemic-graph engine, or a RemoteEngine source), naming the engine
    /// that reported it.
    EngineReported { engine_name: String },
    /// Seeded from a VoID description advertised by an RDF/SPARQL source, naming the
    /// dataset the description describes.
    VoidDescription { dataset_uri: String },
}

impl fmt::Display for StatisticsProvenance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatisticsProvenance::PostgresTableStats { table } => {
                write!(f, "postgres-table-stats({table})")
            }
            StatisticsProvenance::EngineReported { engine_name } => {
                write!(f, "engine-reported({engine_name})")
            }
            StatisticsProvenance::VoidDescription { dataset_uri } => {
                write!(f, "void-description({dataset_uri})")
            }
        }
    }
}

/// A learned, provenanced cardinality estimate for a named foreign source, as the
/// planner will read it to order joins across multiple foreign sources (R048).
///
/// `source_generation` is the foreign source's registration generation at probe time
/// (bumped each time the source is (re-)registered): a planner or cache consumer can
/// compare it against the source's CURRENT generation to tell a statistic seeded
/// against a since-replaced source from one still valid, the same staleness shape
/// R049 uses for its freshness watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinCardinalityStatistic {
    /// The registered foreign source name this estimate describes.
    source_name: String,
    /// The learned/probed cardinality estimate (row count) for the source.
    estimated_cardinality: u64,
    /// How many samples (rows, pages, or probe responses) the estimate was derived
    /// from. Required so a consumer can weigh a thin sample against a thorough one.
    sample_count: u64,
    /// Which probe produced this estimate, and what it probed.
    provenance: StatisticsProvenance,
    /// The foreign source's registration generation at probe time.
    source_generation: u64,
}

/// Why a [`JoinCardinalityStatistic`] was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatsError {
    /// No [`StatisticsProvenance`] was supplied: an un-attributed cardinality guess
    /// is never allowed to reach the planner.
    MissingProvenance,
    /// `sample_count` was zero: an estimate with no backing sample carries no
    /// evidentiary weight and must not be treated as learned.
    ZeroSampleCount,
}

impl fmt::Display for StatsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatsError::MissingProvenance => write!(
                f,
                "federation: a JoinCardinalityStatistic was refused because it carries \
                 no provenance (CONCEPT:EG-KG.query.query-federation, FQR-05)"
            ),
            StatsError::ZeroSampleCount => write!(
                f,
                "federation: a JoinCardinalityStatistic was refused because its sample \
                 count is zero (CONCEPT:EG-KG.query.query-federation, FQR-05)"
            ),
        }
    }
}

impl std::error::Error for StatsError {}

impl JoinCardinalityStatistic {
    /// Construct a statistic, REFUSING one with no provenance or a zero sample count.
    /// `provenance` is `Option` (rather than required) precisely so this constructor
    /// is the one place an un-attributed estimate is rejected, rather than relying on
    /// every call site to remember to attach one.
    pub fn try_new(
        source_name: impl Into<String>,
        estimated_cardinality: u64,
        sample_count: u64,
        provenance: Option<StatisticsProvenance>,
        source_generation: u64,
    ) -> Result<Self, StatsError> {
        let provenance = provenance.ok_or(StatsError::MissingProvenance)?;
        if sample_count == 0 {
            return Err(StatsError::ZeroSampleCount);
        }
        Ok(Self {
            source_name: source_name.into(),
            estimated_cardinality,
            sample_count,
            provenance,
            source_generation,
        })
    }

    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    pub fn estimated_cardinality(&self) -> u64 {
        self.estimated_cardinality
    }

    pub fn sample_count(&self) -> u64 {
        self.sample_count
    }

    pub fn provenance(&self) -> &StatisticsProvenance {
        &self.provenance
    }

    pub fn source_generation(&self) -> u64 {
        self.source_generation
    }

    /// Whether this statistic was seeded against the source's generation CURRENTLY
    /// registered; a planner/cache consumer uses this to discard a stale estimate
    /// from a since-replaced source rather than ordering joins on it.
    pub fn is_current_for(&self, current_source_generation: u64) -> bool {
        self.source_generation == current_source_generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_statistic_without_provenance() {
        let err = JoinCardinalityStatistic::try_new("pg_orders", 1_000_000, 500, None, 1)
            .expect_err("a statistic with no provenance must be refused");
        assert_eq!(err, StatsError::MissingProvenance);
    }

    #[test]
    fn rejects_statistic_with_zero_sample_count() {
        let provenance = StatisticsProvenance::PostgresTableStats {
            table: "orders".to_string(),
        };
        let err = JoinCardinalityStatistic::try_new("pg_orders", 1_000_000, 0, Some(provenance), 1)
            .expect_err("a statistic with zero samples must be refused");
        assert_eq!(err, StatsError::ZeroSampleCount);
    }

    #[test]
    fn accepts_a_fully_provenanced_statistic_and_carries_its_fields() {
        let provenance = StatisticsProvenance::PostgresTableStats {
            table: "orders".to_string(),
        };
        let stat = JoinCardinalityStatistic::try_new(
            "pg_orders",
            1_000_000,
            500,
            Some(provenance.clone()),
            3,
        )
        .expect("a fully-provenanced statistic must be accepted");

        assert_eq!(stat.source_name(), "pg_orders");
        assert_eq!(stat.estimated_cardinality(), 1_000_000);
        assert_eq!(stat.sample_count(), 500);
        assert_eq!(stat.provenance(), &provenance);
        assert_eq!(stat.source_generation(), 3);
        assert!(stat.is_current_for(3));
        assert!(!stat.is_current_for(4));
    }

    #[test]
    fn engine_reported_and_void_description_provenance_round_trip() {
        let engine = JoinCardinalityStatistic::try_new(
            "remote-eg",
            42,
            7,
            Some(StatisticsProvenance::EngineReported {
                engine_name: "remote-eg".to_string(),
            }),
            1,
        )
        .expect("engine-reported provenance must be accepted");
        assert_eq!(
            engine.provenance().to_string(),
            "engine-reported(remote-eg)"
        );

        let void_src = JoinCardinalityStatistic::try_new(
            "dbpedia",
            9_000_000,
            100,
            Some(StatisticsProvenance::VoidDescription {
                dataset_uri: "https://dbpedia.org/void".to_string(),
            }),
            1,
        )
        .expect("VoID-description provenance must be accepted");
        assert_eq!(
            void_src.provenance().to_string(),
            "void-description(https://dbpedia.org/void)"
        );
    }
}
