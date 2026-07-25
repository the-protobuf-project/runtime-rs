use std::time::Duration;

use crate::error::Result;

/// Returns a [`reqwest::Client`] with a dedicated pooled connection pool sized for concurrent
/// single-host traffic (the normal shape for a GraphQL or REST backend), rather than sharing
/// reqwest's small process-wide default pool.
pub(crate) fn new_pooled_client(timeout: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(timeout)
        .pool_max_idle_per_host(100)
        .build()?)
}

/// Collects header key/value pairs injected by an OpenTelemetry [`TextMapPropagator`]
/// (`ConnectionOptions::trace_propagator`), the Rust analog of Go's `propagation.HeaderCarrier`.
#[derive(Default)]
pub(crate) struct HeaderCarrier(pub Vec<(String, String)>);

impl opentelemetry::propagation::Injector for HeaderCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.push((key.to_string(), value));
    }
}
