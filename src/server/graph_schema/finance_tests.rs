//! Finance base ontology (EH-411): a thin core module in the style of the world
//! model. It stays coherent inside the full core corpus, every class it declares is
//! mapped (skos, never imported) to FIBO or Wikidata, its wiring entails what the
//! market connectors and the signal engine rely on, and its shapes refuse the
//! malformed records they exist to catch.

use std::collections::BTreeSet;

use eg_rdf::oxrdf::Triple;

use super::compose::validate_and_compose;
use super::test_support::{
    assert_core_catalog, assert_shape_targets, declared_classes, kg, object_iri, parse_scoped,
    subject_iri, unmapped_classes, wired_with_fixture, KG, OWL_CLASS, OWL_IMPORTS, RDF_TYPE,
};
use crate::graph::GraphSchemaSources;

const FINANCE: &str = include_str!("../../../crates/eg-core/ontology/finance-v1.ttl");
const FINANCE_SHAPES: &str = include_str!("../../../crates/eg-core/ontology/finance-v1.shapes.ttl");
const WIRED_MODULES: &[&str] = &[
    "core:foundation@1",
    "core:energy_geopolitics@1",
    "core:finance@1",
];

const FIXTURE: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/markets#> .

ex:btc :venueSymbol "BTC" .
ex:btcusdt :listedInstrument ex:btc ; :quoteInstrument ex:usdt ; :listedOn ex:binance ;
    :listingType "spot" .
ex:btcDaily :barSeriesOf ex:btcusdt ; :barTimeframe "1D" .
ex:state :signalOf ex:btcDaily ; :signalSpec ex:atrTrail .
ex:flip2 :flipOf ex:state ; :revisesFlip ex:flip1 .
ex:fomc :policyAction "hold" .
ex:wti a :Commodity .
ex:shared :analysisOf ex:btcusdt ; :analysisDigest "sha256:00" .
"#;

fn ex(local: &str) -> String {
    format!("<http://example.org/markets#{local}>")
}

// spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.1, MEDULLA-FINANCE-PRIMITIVES-R004.2.2
#[test]
fn finance_is_a_core_module_within_the_catalog_bound_and_the_corpus_stays_coherent() {
    let classification = assert_core_catalog(&["finance", "finance-shapes"]);
    let bfo = |id: &str| format!("<http://purl.obolibrary.org/obo/BFO_{id}>");
    let placed = [
        ("FinancialInstrument", "0000031"),
        ("Commodity", "0000031"),
        ("Listing", "0000031"),
        ("BarSeries", "0000031"),
        ("IndicatorSpec", "0000031"),
        ("SignalState", "0000031"),
        ("BacktestRun", "0000031"),
        ("TradingSignal", "0000031"),
        ("BacktestResult", "0000031"),
        ("VersionedOrder", "0000031"),
        ("RiskSnapshot", "0000031"),
        ("Venue", "0000004"),
        ("ExchangeBackend", "0000004"),
        ("PortfolioPosition", "0000004"),
        ("TrendFlip", "0000015"),
        ("MacroEvent", "0000015"),
        ("TradingStrategy", "0000015"),
        ("TradingDebate", "0000015"),
        ("Account", "0000031"),
        ("Position", "0000031"),
        ("Activity", "0000031"),
        ("Lot", "0000031"),
        ("PortfolioGroup", "0000031"),
        ("StrategySpec", "0000031"),
        ("StrategyRun", "0000031"),
        ("LeverageTerms", "0000031"),
    ];
    for (class, category) in placed {
        assert!(
            classification.subsumers[&kg(class)].contains(&bfo(category)),
            "{class} ⋢ BFO_{category}"
        );
    }
}

/// Every class is mapped and only the core foundation is imported.
// spec: MEDULLA-FINANCE-PRIMITIVES-R014
#[test]
fn every_finance_class_is_mapped_and_nothing_external_is_imported() {
    let triples = eg_rdf::mapping::parse_turtle(FINANCE).unwrap();
    let declared = declared_classes(&triples);
    assert_eq!(declared.len(), 28);
    let unmapped = unmapped_classes(&triples);
    assert!(unmapped.is_empty(), "unmapped classes {unmapped:?}");
    let imports: BTreeSet<&str> = triples
        .iter()
        .filter(|triple| triple.predicate.as_str() == OWL_IMPORTS)
        .filter_map(object_iri)
        .collect();
    assert_eq!(imports, BTreeSet::from(["http://knuckles.team/kg/core"]));
}

/// The wiring connectors rely on: a listing's parts are typed by the properties, a
/// commodity is an instrument, a revised flip is still a flip and an event.
#[test]
fn listing_signal_and_flip_wiring_entails_their_types() {
    let triples = wired_with_fixture(WIRED_MODULES, FIXTURE);
    let ontology = eg_rdf::owl::parse_ontology(&triples);
    let result = eg_rdf::rules::reason_triples(&triples, &ontology, &Default::default());
    let holds =
        |class: &str, individual: &str| result.holds(&kg(class), &[ex(individual).as_str()]);
    assert!(holds("Listing", "btcusdt"));
    assert!(holds("FinancialInstrument", "btc"));
    assert!(holds("FinancialInstrument", "usdt"));
    assert!(holds("Venue", "binance"));
    assert!(holds("System", "binance"));
    assert!(holds("BarSeries", "btcDaily"));
    assert!(holds("Dataset", "btcDaily"));
    assert!(holds("SignalState", "state"));
    assert!(holds("IndicatorSpec", "atrTrail"));
    assert!(holds("TrendFlip", "flip1"));
    assert!(holds("TrendFlip", "flip2"));
    assert!(holds("Event", "flip2"));
    assert!(holds("MacroEvent", "fomc"));
    assert!(holds("FinancialInstrument", "wti"));
    assert!(holds("AnalysisSnapshot", "shared"));
}

/// A flip is an occurrent and an instrument a continuant: typing one individual as
/// both is inconsistent under the core BFO disjointness.
#[test]
fn a_trend_flip_is_never_an_instrument() {
    let mut triples = wired_with_fixture(WIRED_MODULES, FIXTURE);
    triples.extend(parse_scoped(
        "@prefix : <http://knuckles.team/kg#> . <http://example.org/markets#flip2> a :FinancialInstrument .",
        "clash",
    ));
    let dl = eg_rdf::tableau::parse_dl_ontology(&triples);
    assert!(!eg_rdf::tableau::is_consistent(&dl));
}

/// EH-517: the trading classes live in finance only, under their unchanged IRIs.
// spec: MEDULLA-FINANCE-PRIMITIVES-R001
#[test]
fn the_trading_classes_are_folded_out_of_company_infra() {
    const COMPANY_INFRA: &str =
        include_str!("../../../crates/eg-core/ontology/company_infra-v1.ttl");
    const FOLDED: &[&str] = &[
        "TradingStrategy",
        "TradingSignal",
        "BacktestResult",
        "TradingDebate",
        "ExchangeBackend",
        "VersionedOrder",
        "PortfolioPosition",
        "RiskSnapshot",
    ];
    let declared_in = |document: &str| -> BTreeSet<String> {
        eg_rdf::mapping::parse_turtle(document)
            .unwrap()
            .iter()
            .filter(|t| t.predicate.as_str() == RDF_TYPE && object_iri(t) == Some(OWL_CLASS))
            .filter_map(subject_iri)
            .map(str::to_string)
            .collect()
    };
    let finance = declared_in(FINANCE);
    let company_infra = declared_in(COMPANY_INFRA);
    for class in FOLDED {
        assert!(finance.contains(&format!("{KG}{class}")), "{class}");
        assert!(!company_infra.contains(&format!("{KG}{class}")), "{class}");
    }
}

/// A connector pack's `:BacktestResult ⊑ :OutcomeEvaluation` (emerald-exchange) is a
/// record under a record: satisfiable now that the fold made the class a continuant.
#[test]
fn a_backtest_result_may_specialise_the_outcome_evaluation_record() {
    let sources = GraphSchemaSources::default();
    let mut triples: Vec<Triple> = validate_and_compose(&sources).unwrap().ontology.to_vec();
    triples.extend(parse_scoped(
        "@prefix : <http://knuckles.team/kg#> .\n\
         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
         :BacktestResult rdfs:subClassOf :OutcomeEvaluation .",
        "pack",
    ));
    let classification = eg_rdf::owl::Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(
        !classification.unsatisfiable.contains(&kg("BacktestResult")),
        "{:?}",
        classification.unsatisfiable
    );
}

#[test]
fn finance_shapes_are_their_own_document() {
    let triples = eg_rdf::mapping::parse_turtle(FINANCE_SHAPES).unwrap();
    assert_shape_targets(
        &triples,
        &[
            "FinancialInstrument",
            "Listing",
            "BarSeries",
            "IndicatorSpec",
            "SignalState",
            "TrendFlip",
            "MacroEvent",
            "AnalysisSnapshot",
            "Account",
            "Position",
            "Activity",
            "Lot",
            "PortfolioGroup",
            "StrategySpec",
            "StrategyRun",
            "LeverageTerms",
            "FinanceAlgorithm",
        ],
    );
}

#[cfg(feature = "shacl")]
mod shapes {
    use super::FINANCE_SHAPES;

    const PREFIXES: &str = "@prefix : <http://knuckles.team/kg#> .\n\
        @prefix ex: <http://example.org/markets#> .\n\
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";

    fn conforms(data: &str) -> bool {
        eg_shacl::validate_turtle(FINANCE_SHAPES, &format!("{PREFIXES}{data}"))
            .unwrap()
            .conforms
    }

    const HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    // spec: MEDULLA-FINANCE-PRIMITIVES-R014, MEDULLA-FINANCE-PRIMITIVES-R004.2.1, MEDULLA-FINANCE-PRIMITIVES-R004.2.2
    #[test]
    fn well_formed_finance_records_conform() {
        let data = format!(
            "ex:btc a :FinancialInstrument ; :assetClass \"crypto\" .\n\
             ex:l a :Listing ; :listedInstrument ex:btc ; :quoteInstrument ex:usd ; \
               :listedOn ex:v ; :listingType \"spot\" .\n\
             ex:s a :BarSeries ; :barSeriesOf ex:l ; :barTimeframe \"4h\" ; \
               :tradingCalendar \"utc-24x7\" ; :priceBasis \"trade\" ; :tsdbSeriesId \"bars/btc\" ; \
               :tickSize \"0.01\"^^xsd:decimal ; :volumeStep \"0.0001\"^^xsd:decimal .\n\
             ex:i a :IndicatorSpec ; :indicatorVersion \"atr-trail@1\" ; :parameterHash \"{HASH}\" .\n\
             ex:st a :SignalState ; :signalOf ex:s ; :signalSpec ex:i ; :dataStatus \"warming\" .\n\
             ex:f a :TrendFlip ; :flipOf ex:st ; :flipFrom \"bearish\" ; :flipTo \"bullish\" ; \
               :flipEffectiveAt \"2026-09-24T00:00:00Z\"^^xsd:dateTime ; :flipEventId \"{HASH}\" .\n\
             ex:m a :MacroEvent ; :policyAction \"hold\" ; \
               :announcedAt \"2026-09-17T18:00:00Z\"^^xsd:dateTime .\n\
             ex:a a :AnalysisSnapshot ; :analysisOf ex:l ; :analysisDigest \"{HASH}\" .\n\
             ex:acc a :Account ; :accountId \"acc-1\" ; :baseCurrency \"USD\" ; \
               :accountKind \"taxable\" .\n\
             ex:pos a :Position ; :positionAccount ex:acc ; :positionInstrument ex:btc ; \
               :positionQuantity \"1.5\"^^xsd:decimal ; :costBasisMethod \"fifo\" .\n\
             ex:act a :Activity ; :activityPosition ex:pos ; :activityKind \"buy\" ; \
               :activityQuantity \"1.5\"^^xsd:decimal ; \
               :activityTradeDate \"2026-09-24T00:00:00Z\"^^xsd:dateTime .\n\
             ex:lot a :Lot ; :lotPosition ex:pos ; :lotOpenedBy ex:act ; \
               :lotQuantity \"1.5\"^^xsd:decimal ; :lotUnitCostBasis \"42000.00\"^^xsd:decimal ."
        );
        assert!(conforms(&data));
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.1, MEDULLA-FINANCE-PRIMITIVES-R004.2.2
    #[test]
    fn malformed_finance_records_are_violations() {
        let refused = [
            "ex:l a :Listing ; :listedInstrument ex:btc ; :quoteInstrument ex:usd ; \
               :listedOn ex:v ; :listingType \"spto\" .",
            "ex:s a :BarSeries ; :barSeriesOf ex:l ; :barTimeframe \"7D\" ; \
               :tradingCalendar \"utc-24x7\" ; :priceBasis \"trade\" ; :tsdbSeriesId \"b\" .",
            "ex:st a :SignalState ; :signalOf ex:s ; :signalSpec ex:i ; :dataStatus \"valid\" ; \
               :trendDirection \"sideways\" .",
            "ex:st a :SignalState ; :signalOf ex:s ; :signalSpec ex:i .",
            "ex:i a :IndicatorSpec ; :indicatorVersion \"v\" ; :parameterHash \"md5:00\" .",
            "ex:m a :MacroEvent ; :policyAction \"hold\" .",
            "ex:s a :BarSeries ; :barSeriesOf ex:l ; :barTimeframe \"1D\" ; \
               :tradingCalendar \"utc-24x7\" ; :priceBasis \"trade\" ; :tsdbSeriesId \"b\" .",
            "ex:a a :AnalysisSnapshot ; :analysisOf ex:l ; :analysisDigest \"sha1:00\" .",
            "ex:btc a :FinancialInstrument ; :assetClass \"crypto-ish\" .",
            "ex:a a :AnalysisSnapshot ; :analysisDigest \"sha256:0000000000000000000000000000000000000000000000000000000000000000\" .",
            "ex:acc a :Account ; :baseCurrency \"USD\" .",
            "ex:acc a :Account ; :accountId \"acc-1\" ; :baseCurrency \"USD\" ; \
               :accountKind \"checking\" .",
            "ex:pos a :Position ; :positionInstrument ex:btc ; :positionQuantity \"1.5\"^^xsd:decimal .",
            "ex:pos a :Position ; :positionAccount ex:acc ; :positionInstrument ex:btc ; \
               :positionQuantity \"1.5\"^^xsd:decimal ; :costBasisMethod \"lowest_cost\" .",
            "ex:act a :Activity ; :activityKind \"buy\" ; :activityQuantity \"1.5\"^^xsd:decimal ; \
               :activityTradeDate \"2026-09-24T00:00:00Z\"^^xsd:dateTime .",
            "ex:act a :Activity ; :activityPosition ex:pos ; :activityKind \"deposit\" ; \
               :activityQuantity \"1.5\"^^xsd:decimal ; \
               :activityTradeDate \"2026-09-24T00:00:00Z\"^^xsd:dateTime .",
            "ex:lot a :Lot ; :lotOpenedBy ex:act ; :lotQuantity \"1.5\"^^xsd:decimal ; \
               :lotUnitCostBasis \"42000.00\"^^xsd:decimal .",
        ];
        for data in refused {
            assert!(!conforms(data), "{data}");
        }
    }

    /// MEDULLA-FINANCE-PRIMITIVES-R004: the canonical asset-class vocabulary, plus the
    /// deprecated migration aliases preserved for `stock`, `forex` and `commodity`.
    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.1
    #[test]
    fn asset_class_vocabulary_accepts_canonical_and_legacy_values() {
        for class in [
            "equity",
            "etf",
            "fund",
            "index",
            "fx_pair",
            "commodity_spot",
            "commodity_future",
            "crypto",
            "real_estate",
            "cash",
            "stock",
            "forex",
            "commodity",
        ] {
            let data = format!("ex:btc a :FinancialInstrument ; :assetClass \"{class}\" .");
            assert!(conforms(&data), "{class} should conform");
        }
        // Not in the new canonical list, and never named as a preserved alias.
        assert!(!conforms(
            "ex:btc a :FinancialInstrument ; :assetClass \"bond\" ."
        ));
    }

    /// One field of a record under test. `valid` conforms; `alt` is a second,
    /// different value; `wrong` has the wrong datatype or kind (or is outside
    /// the closed vocabulary).
    struct Field {
        name: &'static str,
        valid: &'static str,
        alt: &'static str,
        wrong: &'static str,
        required: bool,
        singleton: bool,
    }

    fn optional(mut field: Field) -> Field {
        field.required = false;
        field
    }

    const fn field(
        name: &'static str,
        valid: &'static str,
        alt: &'static str,
        wrong: &'static str,
    ) -> Field {
        Field {
            name,
            valid,
            alt,
            wrong,
            required: true,
            singleton: true,
        }
    }

    fn node(class: &str, fields: &[(&str, &str)]) -> String {
        let mut out = format!("ex:subject a :{class}");
        for (name, value) in fields {
            out.push_str(&format!(" ; :{name} {value}"));
        }
        out.push_str(" .");
        out
    }

    fn base(fields: &[Field]) -> Vec<(&'static str, &'static str)> {
        fields.iter().map(|f| (f.name, f.valid)).collect()
    }

    /// The complete shape matrix for one record class: a valid record conforms,
    /// every required field's omission refuses, a wrong datatype or kind
    /// refuses, a duplicated singleton refuses, and every declared vocabulary
    /// value conforms while an unknown one refuses.
    fn assert_matrix(class: &str, fields: &[Field], vocabularies: &[(&str, &[&str])]) {
        let valid = base(fields);
        assert!(conforms(&node(class, &valid)), "{class} valid record");
        for target in fields {
            if target.required {
                let omitted: Vec<_> = valid
                    .iter()
                    .copied()
                    .filter(|(n, _)| *n != target.name)
                    .collect();
                assert!(
                    !conforms(&node(class, &omitted)),
                    "{class} without {}",
                    target.name
                );
            }
            let mut wrong = valid.clone();
            wrong.retain(|(n, _)| *n != target.name);
            wrong.push((target.name, target.wrong));
            assert!(
                !conforms(&node(class, &wrong)),
                "{class} wrong {}",
                target.name
            );
            if target.singleton {
                let mut twice = valid.clone();
                twice.push((target.name, target.alt));
                assert!(
                    !conforms(&node(class, &twice)),
                    "{class} duplicate {}",
                    target.name
                );
            }
        }
        for &(name, values) in vocabularies {
            for value in values {
                let mut record: Vec<(&str, &str)> = valid.clone();
                record.retain(|(n, _)| *n != name);
                let quoted = format!("\"{value}\"");
                record.push((name, quoted.as_str()));
                assert!(conforms(&node(class, &record)), "{class} {name}={value}");
            }
        }
    }

    const IRI_WRONG: &str = "\"not-an-iri\"";
    const TENANT: Field = field("recordTenant", "\"tenant-1\"", "\"tenant-2\"", "5");
    const SOURCE: Field = field("recordSource", "\"broker-feed\"", "\"other-feed\"", "5");
    const EFFECTIVE: Field = field(
        "recordEffectiveAt",
        "\"2026-09-24T00:00:00Z\"^^xsd:dateTime",
        "\"2026-09-25T00:00:00Z\"^^xsd:dateTime",
        "\"yesterday\"",
    );
    const KNOWN: Field = field(
        "recordKnownAt",
        "\"2026-09-24T01:00:00Z\"^^xsd:dateTime",
        "\"2026-09-25T01:00:00Z\"^^xsd:dateTime",
        "\"yesterday\"",
    );
    const VERSION: Field = field(
        "recordSchemaVersion",
        "\"finance-v1\"",
        "\"finance-v2\"",
        "\"finance-v2\"",
    );

    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.1.1
    #[test]
    fn account_position_complete_schema_matrix() {
        assert_matrix(
            "Account",
            &[
                field("accountId", "\"acc-1\"", "\"acc-2\"", "5"),
                field("baseCurrency", "\"USD\"", "\"EUR\"", "5"),
                optional(field(
                    "accountKind",
                    "\"taxable\"",
                    "\"ira\"",
                    "\"checking\"",
                )),
            ],
            &[(
                "accountKind",
                &["taxable", "ira", "roth_ira", "401k", "hsa", "custodial"],
            )],
        );
        assert_matrix(
            "Position",
            &[
                field("positionAccount", "ex:acc", "ex:acc2", IRI_WRONG),
                field("positionInstrument", "ex:btc", "ex:eth", IRI_WRONG),
                field(
                    "positionQuantity",
                    "\"1.5\"^^xsd:decimal",
                    "\"2.5\"^^xsd:decimal",
                    "\"abc\"",
                ),
                optional(field(
                    "costBasisMethod",
                    "\"fifo\"",
                    "\"lifo\"",
                    "\"lowest_cost\"",
                )),
            ],
            &[(
                "costBasisMethod",
                &["fifo", "lifo", "specific_lot", "average_cost"],
            )],
        );
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.2.1
    #[test]
    fn activity_lot_complete_schema_matrix() {
        assert_matrix(
            "Activity",
            &[
                field("activityPosition", "ex:pos", "ex:pos2", IRI_WRONG),
                field("activityKind", "\"buy\"", "\"sell\"", "\"deposit\""),
                field(
                    "activityQuantity",
                    "\"1.5\"^^xsd:decimal",
                    "\"2.5\"^^xsd:decimal",
                    "\"abc\"",
                ),
                field(
                    "activityTradeDate",
                    "\"2026-09-24T00:00:00Z\"^^xsd:dateTime",
                    "\"2026-09-25T00:00:00Z\"^^xsd:dateTime",
                    "\"2026-09-24\"",
                ),
            ],
            &[(
                "activityKind",
                &[
                    "buy",
                    "sell",
                    "dividend",
                    "transfer_in",
                    "transfer_out",
                    "split",
                    "fee",
                ],
            )],
        );
        assert_matrix(
            "Lot",
            &[
                field("lotPosition", "ex:pos", "ex:pos2", IRI_WRONG),
                field("lotOpenedBy", "ex:act", "ex:act2", IRI_WRONG),
                field(
                    "lotQuantity",
                    "\"1.5\"^^xsd:decimal",
                    "\"2.5\"^^xsd:decimal",
                    "\"abc\"",
                ),
                field(
                    "lotUnitCostBasis",
                    "\"42000.00\"^^xsd:decimal",
                    "\"43000.00\"^^xsd:decimal",
                    "\"abc\"",
                ),
            ],
            &[],
        );
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.3
    #[test]
    fn portfolio_group_schema_acceptance() {
        let mut member = field("portfolioMember", "ex:pos", "ex:pos2", IRI_WRONG);
        member.required = false;
        member.singleton = false;
        assert_matrix(
            "PortfolioGroup",
            &[
                field("portfolioGroupName", "\"Retirement\"", "\"Other\"", "5"),
                field("portfolioView", "\"basic\"", "\"holdings\"", "\"detailed\""),
                member,
                VERSION,
                TENANT,
                SOURCE,
                EFFECTIVE,
                KNOWN,
            ],
            &[("portfolioView", &["basic", "holdings"])],
        );
        // Several members, and an empty group, both conform.
        let two = format!(
            "{} ex:subject :portfolioMember ex:btc , ex:eth .",
            node(
                "PortfolioGroup",
                &[
                    ("portfolioGroupName", "\"Watch\""),
                    ("portfolioView", "\"basic\""),
                    ("recordSchemaVersion", "\"finance-v1\""),
                    ("recordTenant", "\"t\""),
                    ("recordSource", "\"s\""),
                    (
                        "recordEffectiveAt",
                        "\"2026-09-24T00:00:00Z\"^^xsd:dateTime"
                    ),
                    ("recordKnownAt", "\"2026-09-24T00:00:00Z\"^^xsd:dateTime"),
                ],
            )
        );
        assert!(conforms(&two));
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.4
    #[test]
    fn strategy_spec_schema_acceptance() {
        assert_matrix(
            "StrategySpec",
            &[
                field("strategySpecId", "\"dca-monthly\"", "\"other\"", "5"),
                field("strategySpecVersion", "3", "4", "\"three\""),
                field(
                    "strategySpecKind",
                    "\"dca_fixed_amount\"",
                    "\"trend_flip\"",
                    "\"martingale\"",
                ),
                field(
                    "strategySpecDigest",
                    "\"sha256:0000000000000000000000000000000000000000000000000000000000000000\"",
                    "\"sha256:1111111111111111111111111111111111111111111111111111111111111111\"",
                    "\"sha1:00\"",
                ),
                field("strategySpecUniverse", "ex:group", "ex:group2", IRI_WRONG),
                field("strategySpecCurrency", "\"USD\"", "\"EUR\"", "5"),
                VERSION,
                TENANT,
                SOURCE,
                EFFECTIVE,
                KNOWN,
            ],
            &[(
                "strategySpecKind",
                &[
                    "dca_fixed_amount",
                    "dca_fixed_shares",
                    "value_averaging",
                    "trend_flip",
                    "trend_moving_average",
                    "rebalance_calendar",
                    "rebalance_threshold",
                ],
            )],
        );
        let zero_version = strategy_spec_with_version("0");
        assert!(!conforms(&zero_version), "version must be positive");
        assert!(conforms(&strategy_spec_with_version("1")));
    }

    fn strategy_spec_with_version(version: &str) -> String {
        node(
            "StrategySpec",
            &[
                ("strategySpecId", "\"dca\""),
                ("strategySpecVersion", version),
                ("strategySpecKind", "\"dca_fixed_amount\""),
                (
                    "strategySpecDigest",
                    "\"sha256:0000000000000000000000000000000000000000000000000000000000000000\"",
                ),
                ("strategySpecUniverse", "ex:group"),
                ("strategySpecCurrency", "\"USD\""),
                ("recordSchemaVersion", "\"finance-v1\""),
                ("recordTenant", "\"t\""),
                ("recordSource", "\"s\""),
                (
                    "recordEffectiveAt",
                    "\"2026-09-24T00:00:00Z\"^^xsd:dateTime",
                ),
                ("recordKnownAt", "\"2026-09-24T00:00:00Z\"^^xsd:dateTime"),
            ],
        )
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.5
    #[test]
    fn strategy_run_schema_acceptance() {
        assert_matrix(
            "StrategyRun",
            &[
                field("strategyRunSpec", "ex:spec", "ex:spec2", IRI_WRONG),
                field(
                    "strategyRunStatus",
                    "\"proposed\"",
                    "\"missed\"",
                    "\"executed\"",
                ),
                field(
                    "strategyRunDecidedAt",
                    "\"2026-09-24T00:00:00Z\"^^xsd:dateTime",
                    "\"2026-09-25T00:00:00Z\"^^xsd:dateTime",
                    "\"2026-09-24\"",
                ),
                field(
                    "strategyRunInputDigest",
                    "\"sha256:0000000000000000000000000000000000000000000000000000000000000000\"",
                    "\"sha256:1111111111111111111111111111111111111111111111111111111111111111\"",
                    "\"md5:00\"",
                ),
                VERSION,
                TENANT,
                SOURCE,
                EFFECTIVE,
                KNOWN,
            ],
            &[("strategyRunStatus", &["proposed", "missed", "abstained"])],
        );
    }

    // spec: MEDULLA-FINANCE-PRIMITIVES-R004.2.6
    #[test]
    fn leverage_terms_schema_acceptance() {
        let fields = [
            field("leverageInstrument", "ex:btc", "ex:eth", IRI_WRONG),
            field("leverageProductType", "\"margin\"", "\"future\"", "\"cfd\""),
            field(
                "leveragePolicyVersion",
                "\"us-2026-10\"",
                "\"us-2026-11\"",
                "5",
            ),
            field(
                "leverageMultiplier",
                "\"2.0\"^^xsd:decimal",
                "\"3.0\"^^xsd:decimal",
                "\"abc\"",
            ),
            field("leverageCurrency", "\"USD\"", "\"EUR\"", "5"),
            field(
                "leverageInitialMargin",
                "\"5000.00\"^^xsd:decimal",
                "\"6000.00\"^^xsd:decimal",
                "\"abc\"",
            ),
            field(
                "leverageMaintenanceMargin",
                "\"4000.00\"^^xsd:decimal",
                "\"4500.00\"^^xsd:decimal",
                "\"abc\"",
            ),
            VERSION,
            TENANT,
            SOURCE,
            EFFECTIVE,
            KNOWN,
        ];
        assert_matrix(
            "LeverageTerms",
            &fields,
            &[(
                "leverageProductType",
                &["margin", "future", "daily_reset_fund", "fx"],
            )],
        );
        for (name, bad) in [
            ("leverageMultiplier", "\"0\"^^xsd:decimal"),
            ("leverageInitialMargin", "\"-1\"^^xsd:decimal"),
            ("leverageMaintenanceMargin", "\"-1\"^^xsd:decimal"),
        ] {
            let mut record = base(&fields);
            record.retain(|(n, _)| *n != name);
            record.push((name, bad));
            assert!(!conforms(&node("LeverageTerms", &record)), "{name}={bad}");
        }
    }
}
