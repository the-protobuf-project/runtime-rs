//! OpenTelemetry metrics around publishing.
//!
//! The caller supplies a Meter and keeps ownership of the provider, reader, and
//! exporter. Attributes are bounded: the subject, which a stream declares up
//! front, and the outcome. Ids and payloads are never recorded.

use std::{sync::Arc, time::Instant};

use tpp_telemetry::opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};

use crate::{Middleware, Options, Payload, Publisher, Result};

/// Publisher holding instruments built once from one Meter.
struct TelemetryPublisher {
    next: Arc<dyn Publisher>,
    published: Counter<u64>,
    duration: Histogram<f64>,
}

/// Counts publishes and records their latency, partitioned by subject and
/// outcome.
pub fn with_telemetry(next: Arc<dyn Publisher>, meter: &Meter) -> Arc<dyn Publisher> {
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
pub fn with_telemetry_middleware(meter: Meter) -> Middleware {
    Arc::new(move |next| with_telemetry(next, &meter))
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
