// CONCEPT:EG-091 — OpenTelemetry OTLP span export + tracing init.
//
// This is the ONE place the binary installs the global `tracing` subscriber. It
// layers an OTLP batch span exporter ON TOP of the engine's existing tracing
// setup, purely additively:
//   * built WITHOUT `otel`, OR with `otel` but `EPISTEMIC_GRAPH_OTLP_ENDPOINT`
//     unset  ⇒  the ORIGINAL fmt-only subscriber (INFO, no target), byte-for-byte
//     unchanged. No exporter is installed, zero overhead.
//   * built WITH `otel` AND the endpoint set  ⇒  the SAME fmt layer PLUS a
//     `tracing-opentelemetry` layer that BATCH-exports the spans the engine
//     ALREADY emits to the OTLP endpoint. No new spans are created — it is a pure
//     export layer over the existing instrumentation.
//
// On any OTLP setup error we log a warning and fall back to the fmt-only
// subscriber, so a mis-configured endpoint can never take the server down.

/// Install the global tracing subscriber (see module docs). Called once from
/// `main::run`, replacing the previous inline `tracing_subscriber::fmt().init()`.
pub fn init_tracing() {
    #[cfg(feature = "otel")]
    {
        if let Some(endpoint) = std::env::var("EPISTEMIC_GRAPH_OTLP_ENDPOINT")
            .ok()
            .filter(|s| !s.trim().is_empty())
        {
            match init_with_otel(&endpoint) {
                Ok(()) => {
                    tracing::info!(
                        "OTel (CONCEPT:EG-091): exporting tracing spans to OTLP endpoint {endpoint}"
                    );
                    return;
                }
                Err(e) => {
                    // The subscriber is not yet installed, so tracing macros would
                    // be dropped — report on stderr and fall through to fmt-only.
                    eprintln!(
                        "warning: OTLP exporter init failed ({e}); \
                         falling back to stdout-only tracing"
                    );
                }
            }
        }
    }
    init_fmt_only();
}

/// The engine's original fmt subscriber (INFO, no target) — the exact behavior
/// prior to EG-091. Used whenever OTLP export is off (feature absent, endpoint
/// unset, or exporter init failed).
fn init_fmt_only() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();
}

/// Build the OTLP batch exporter and install a Registry carrying BOTH the fmt
/// layer and the `tracing-opentelemetry` export layer (CONCEPT:EG-091).
#[cfg(feature = "otel")]
fn init_with_otel(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // OTLP/gRPC span exporter aimed at the operator-supplied collector endpoint.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()?;

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name("epistemic-graph")
        .build();

    // Batch span processor: spans buffer and flush on a background task off the
    // request hot path (the tokio runtime, via the `rt-tokio` sdk feature).
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("epistemic-graph");
    // The global provider owns the pipeline for the process lifetime so batched
    // spans continue to flush.
    opentelemetry::global::set_tracer_provider(provider);

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);

    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()?;
    Ok(())
}
