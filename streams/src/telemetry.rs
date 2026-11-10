//! Telemetry for streams, through tpp-telemetry only.
//!
//! An application never names an OpenTelemetry type: it builds one
//! [`Telemetry`] (from its SDK, with `Telemetry::from_tpp`) and hands it to
//! [`instrument`](crate::instrument()), to a provider's config, or to
//! [`with_telemetry`]. Everything a caller needs is re-exported here.
//!
//! [`with_telemetry`] is the older publish-only middleware: a count and a
//! latency per publish, labeled by subject and outcome. Ids and payloads are
//! never recorded. [`instrument`](crate::instrument()) traces every operation
//! as well, and carries the trace across the broker.

use std::{sync::Arc, time::Instant};

use tpp_telemetry::opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
pub use tpp_telemetry::{Outcome, Telemetry};

use crate::{Middleware, Options, Payload, Publisher, Result};

/// Publisher holding instruments built once from one meter.
struct TelemetryPublisher {
    next: Arc<dyn Publisher>,
    published: Counter<u64>,
    duration: Histogram<f64>,
}

/// Counts publishes and records their latency, partitioned by subject and
/// outcome, on `telemetry`'s meter; returns `next` unchanged when it has none.
pub fn with_telemetry(next: Arc<dyn Publisher>, telemetry: &Telemetry) -> Arc<dyn Publisher> {
    let Some(meter) = telemetry.meter() else {
        return next;
    };
    Arc::new(TelemetryPublisher {
        next,
        published: meter
            .u64_counter("streams_published_total")
            .with_description("Completed stream publishes")
            .with_unit("1")
            .build(),
        duration: meter
            .f64_histogram("streams_publish_duration_seconds")
            .with_description("Stream publish latency")
            .with_unit("s")
            .build(),
    })
}

/// [`with_telemetry`] as [`Middleware`].
pub fn with_telemetry_middleware(telemetry: Telemetry) -> Middleware {
    Arc::new(move |next| with_telemetry(next, &telemetry))
}

#[async_trait::async_trait]
impl Publisher for TelemetryPublisher {
    /// Records one count and one latency per publish, failures included.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let start = Instant::now();
        let result = self.next.publish(subject, payload, opts).await;
        let labels = [
            KeyValue::new("subject", subject.to_owned()),
            KeyValue::new("outcome", if result.is_ok() { "ok" } else { "error" }),
        ];
        self.published.add(1, &labels);
        self.duration.record(start.elapsed().as_secs_f64(), &labels);
        result
    }
}
