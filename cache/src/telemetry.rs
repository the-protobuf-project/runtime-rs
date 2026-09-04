//! OpenTelemetry metrics middleware for raw Document operations.
//!
//! The caller supplies a Meter, retaining ownership of provider, reader, and
//! exporter configuration. The wrapper records bounded operation attributes
//! and never includes IDs, values, options, addresses, or error messages.

use std::{sync::Arc, time::Instant};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};

use crate::{
    CacheError, Result,
    core::{Document, Options},
    middleware::Middleware,
};

/// Document wrapper holding instruments constructed once from one Meter.
///
/// Instruments are cloneable synchronous OpenTelemetry handles. Calls keep no
/// mutable cache state and create no tasks, locks, or exporter infrastructure.
struct TelemetryDocument {
    /// Operation target invoked exactly once by each wrapper method.
    next: Arc<dyn Document>,
    /// Completed Document operations partitioned by operation and outcome.
    operations: Counter<u64>,
    /// Awaited operation latency in seconds with the same partitioning.
    duration: Histogram<f64>,
    /// Classified Get results partitioned into hits and settled misses.
    gets: Counter<u64>,
}

impl TelemetryDocument {
    /// Builds all instruments once so calls only record measurements.
    fn new(next: Arc<dyn Document>, meter: Meter) -> Self {
        Self {
            next,
            operations: meter
                .u64_counter("cache_operations_total")
                .with_description("Completed cache Document operations")
                .with_unit("1")
                .build(),
            duration: meter
                .f64_histogram("cache_operation_duration_seconds")
                .with_description("Cache Document operation latency")
                .with_unit("s")
                .build(),
            gets: meter
                .u64_counter("cache_gets_total")
                .with_description("Completed cache Document gets by hit or miss")
                .with_unit("1")
                .build(),
        }
    }

    /// Records one operation count and duration with bounded attributes.
    ///
    /// Get passes `not_found_is_ok`; other methods classify every error as an
    /// operation failure, matching the reference middleware.
    fn record<T>(
        &self,
        operation: &'static str,
        start: Instant,
        result: &Result<T>,
        not_found_is_ok: bool,
    ) {
        let outcome = match result {
            Ok(_) => "ok",
            Err(CacheError::NotFound) if not_found_is_ok => "ok",
            Err(_) => "error",
        };
        let attributes = [
            KeyValue::new("operation", operation),
            KeyValue::new("outcome", outcome),
        ];
        self.operations.add(1, &attributes);
        self.duration
            .record(start.elapsed().as_secs_f64(), &attributes);
    }
}

#[async_trait::async_trait]
impl Document for TelemetryDocument {
    /// Delegates Create once and records its operation outcome and duration.
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
        let start = Instant::now();
        let result = self.next.create(value, opts).await;
        self.record("create", start, &result, false);
        result
    }

    /// Delegates Get once and additionally classifies a successful hit or miss.
    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()> {
        let start = Instant::now();
        let result = self.next.get(id, dest).await;
        match &result {
            Ok(()) => self.gets.add(1, &[KeyValue::new("result", "hit")]),
            Err(CacheError::NotFound) => {
                self.gets.add(1, &[KeyValue::new("result", "miss")]);
            }
            Err(_) => {}
        }
        self.record("get", start, &result, true);
        result
    }

    /// Delegates Update once and records its operation outcome and duration.
    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()> {
        let start = Instant::now();
        let result = self.next.update(id, value, opts).await;
        self.record("update", start, &result, false);
        result
    }

    /// Delegates Delete once and records no inaccurate entry-count estimate.
    async fn delete(&self, id: &str) -> Result<()> {
        let start = Instant::now();
        let result = self.next.delete(id).await;
        self.record("delete", start, &result, false);
        result
    }

    /// Delegates Keys once without recording returned IDs as attributes.
    async fn keys(&self) -> Result<Vec<String>> {
        let start = Instant::now();
        let result = self.next.keys().await;
        self.record("keys", start, &result, false);
        result
    }

    /// Delegates List once without recording cached values as attributes.
    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        let start = Instant::now();
        let result = self.next.list().await;
        self.record("list", start, &result, false);
        result
    }

    /// Delegates TTL once without recording IDs or lease values as attributes.
    async fn ttl(&self, id: &str) -> Result<std::time::Duration> {
        let start = Instant::now();
        let result = self.next.ttl(id).await;
        self.record("ttl", start, &result, false);
        result
    }
}

/// Wraps a raw Document with bounded OpenTelemetry operation metrics.
///
/// The wrapper constructs `cache_operations_total`,
/// `cache_operation_duration_seconds`, and `cache_gets_total` from `meter`.
/// A Get miss is an expected `outcome=ok` operation with `result=miss`; other
/// failures use `outcome=error`. All underlying results remain unchanged.
///
/// The caller owns MeterProvider and exporter configuration. This function
/// neither resolves nor installs a global provider. Middleware order controls
/// whether telemetry outside retry measures the whole sequence or telemetry
/// inside retry measures individual attempts.
///
/// **Cost**: One underlying operation, a monotonic-clock measurement, one
/// counter increment, and one histogram observation; Get hit/miss adds a
/// second counter increment.
/// **Concurrency**: Immutable and shareable, with synchronous instrument
/// handles and no wrapper-owned locks or tasks.
/// **Side effects**: Records measurements through the supplied Meter's
/// provider; exporter effects belong to caller configuration.
/// **When to use**: Measuring aggregate Document traffic, outcomes, latency,
/// and cache effectiveness without high-cardinality labels.
pub fn with_telemetry(document: Arc<dyn Document>, meter: Meter) -> Arc<dyn Document> {
    Arc::new(TelemetryDocument::new(document, meter))
}

/// Returns [`with_telemetry`] as reusable Document [`Middleware`].
///
/// The Meter is cloned each time the middleware is applied; OpenTelemetry
/// instruments remain shared handles created by that Meter's provider.
///
/// **Cost/side effects**: Applying the middleware constructs three synchronous
/// instruments but performs no Document operation and starts no exporter.
pub fn with_telemetry_middleware(meter: Meter) -> Middleware {
    Arc::new(move |document| with_telemetry(document, meter.clone()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use opentelemetry::{
        KeyValue,
        metrics::{
            Counter, Histogram, HistogramBuilder, InstrumentBuilder, InstrumentProvider,
            MeterProvider, NoopMeterProvider, SyncInstrument,
        },
    };

    use super::*;
    use crate::{chain, with_retry_middleware};

    /// Instrument registration captured before any operation runs.
    #[derive(Debug, Eq, PartialEq)]
    struct Registration {
        name: String,
        kind: &'static str,
        unit: Option<String>,
        description: Option<String>,
    }

    /// Numeric value retained by the test provider without lossy conversion.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Number {
        U64(u64),
        F64(f64),
    }

    /// One synchronous measurement and its bounded attributes.
    #[derive(Clone, Debug, PartialEq)]
    struct Measurement {
        name: String,
        value: Number,
        attributes: BTreeMap<String, String>,
    }

    /// Shared output of the test-only OpenTelemetry instrument provider.
    #[derive(Default)]
    struct CaptureState {
        registrations: Mutex<Vec<Registration>>,
        measurements: Mutex<Vec<Measurement>>,
    }

    /// Creates capture instruments for the three production metric types.
    struct CaptureProvider {
        state: Arc<CaptureState>,
    }

    impl CaptureProvider {
        fn register(
            &self,
            name: &str,
            kind: &'static str,
            unit: Option<&str>,
            description: Option<&str>,
        ) {
            self.state.registrations.lock().unwrap().push(Registration {
                name: name.to_owned(),
                kind,
                unit: unit.map(str::to_owned),
                description: description.map(str::to_owned),
            });
        }
    }

    impl InstrumentProvider for CaptureProvider {
        fn u64_counter(&self, builder: InstrumentBuilder<'_, Counter<u64>>) -> Counter<u64> {
            let name = builder.name.to_string();
            self.register(
                &name,
                "counter_u64",
                builder.unit.as_deref(),
                builder.description.as_deref(),
            );
            Counter::new(Arc::new(U64Recorder {
                name,
                state: self.state.clone(),
            }))
        }

        fn f64_histogram(&self, builder: HistogramBuilder<'_, Histogram<f64>>) -> Histogram<f64> {
            let name = builder.name.to_string();
            self.register(
                &name,
                "histogram_f64",
                builder.unit.as_deref(),
                builder.description.as_deref(),
            );
            Histogram::new(Arc::new(F64Recorder {
                name,
                state: self.state.clone(),
            }))
        }
    }

    /// Records unsigned counter additions into CaptureState.
    struct U64Recorder {
        name: String,
        state: Arc<CaptureState>,
    }

    impl SyncInstrument<u64> for U64Recorder {
        fn measure(&self, value: u64, attributes: &[KeyValue]) {
            record_measurement(&self.state, &self.name, Number::U64(value), attributes);
        }
    }

    /// Records floating-point histogram observations into CaptureState.
    struct F64Recorder {
        name: String,
        state: Arc<CaptureState>,
    }

    impl SyncInstrument<f64> for F64Recorder {
        fn measure(&self, value: f64, attributes: &[KeyValue]) {
            record_measurement(&self.state, &self.name, Number::F64(value), attributes);
        }
    }

    /// Normalizes OpenTelemetry attributes for deterministic assertions.
    fn record_measurement(
        state: &CaptureState,
        name: &str,
        value: Number,
        attributes: &[KeyValue],
    ) {
        let attributes = attributes
            .iter()
            .map(|attribute| {
                (
                    attribute.key.as_str().to_owned(),
                    attribute.value.to_string(),
                )
            })
            .collect();
        state.measurements.lock().unwrap().push(Measurement {
            name: name.to_owned(),
            value,
            attributes,
        });
    }

    /// Returns one Meter and the state receiving its registrations and values.
    fn capture_meter() -> (Meter, Arc<CaptureState>) {
        let state = Arc::new(CaptureState::default());
        let meter = Meter::new(Arc::new(CaptureProvider {
            state: state.clone(),
        }));
        (meter, state)
    }

    /// Document with a configurable number of initial identical failures.
    struct TestDocument {
        calls: AtomicUsize,
        failures: AtomicUsize,
        failure: Option<CacheError>,
    }

    impl TestDocument {
        fn successful() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                failures: AtomicUsize::new(0),
                failure: None,
            })
        }

        fn failing(error: CacheError, failures: usize) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                failures: AtomicUsize::new(failures),
                failure: Some(error),
            })
        }

        fn attempt(&self) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let failed = self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok();
            if failed {
                return Err(self.failure.clone().unwrap());
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Document for TestDocument {
        async fn create(&self, _value: &[u8], _opts: &Options) -> Result<String> {
            self.attempt()?;
            Ok("generated".to_owned())
        }

        async fn get(&self, _id: &str, dest: &mut Vec<u8>) -> Result<()> {
            self.attempt()?;
            *dest = b"body".to_vec();
            Ok(())
        }

        async fn update(&self, _id: &str, _value: &[u8], _opts: &Options) -> Result<()> {
            self.attempt()
        }

        async fn delete(&self, _id: &str) -> Result<()> {
            self.attempt()
        }

        async fn keys(&self) -> Result<Vec<String>> {
            self.attempt()?;
            Ok(vec!["one".to_owned(), "two".to_owned()])
        }

        async fn list(&self) -> Result<Vec<Vec<u8>>> {
            self.attempt()?;
            Ok(vec![b"body".to_vec()])
        }

        async fn ttl(&self, _id: &str) -> Result<Duration> {
            self.attempt()?;
            Ok(Duration::from_secs(5))
        }
    }

    /// Copies matching measurements after the operation has completed.
    fn measurements(state: &CaptureState, name: &str) -> Vec<Measurement> {
        state
            .measurements
            .lock()
            .unwrap()
            .iter()
            .filter(|measurement| measurement.name == name)
            .cloned()
            .collect()
    }

    #[test]
    fn test_telemetry_constructs_three_document_instruments_once() {
        let (meter, state) = capture_meter();

        let _document = with_telemetry(TestDocument::successful(), meter);

        assert_eq!(*state.registrations.lock().unwrap(), vec![
            Registration {
                name: "cache_operations_total".to_owned(),
                kind: "counter_u64",
                unit: Some("1".to_owned()),
                description: Some("Completed cache Document operations".to_owned()),
            },
            Registration {
                name: "cache_operation_duration_seconds".to_owned(),
                kind: "histogram_f64",
                unit: Some("s".to_owned()),
                description: Some("Cache Document operation latency".to_owned()),
            },
            Registration {
                name: "cache_gets_total".to_owned(),
                kind: "counter_u64",
                unit: Some("1".to_owned()),
                description: Some("Completed cache Document gets by hit or miss".to_owned()),
            },
        ]);
    }

    #[test]
    fn test_telemetry_all_operations_delegate_and_record_once() {
        let (meter, state) = capture_meter();
        let concrete = TestDocument::successful();
        let document = with_telemetry(concrete.clone(), meter);

        futures::executor::block_on(async {
            assert_eq!(
                document
                    .create(b"value", &Options::default())
                    .await
                    .unwrap(),
                "generated"
            );
            let mut body = Vec::new();
            document.get("id", &mut body).await.unwrap();
            assert_eq!(body, b"body");
            document
                .update("id", b"value", &Options::default())
                .await
                .unwrap();
            document.delete("id").await.unwrap();
            assert_eq!(document.keys().await.unwrap(), vec!["one", "two"]);
            assert_eq!(document.list().await.unwrap(), vec![b"body".to_vec()]);
            assert_eq!(document.ttl("id").await.unwrap(), Duration::from_secs(5));
        });

        assert_eq!(concrete.calls.load(Ordering::SeqCst), 7);
        let operations = measurements(&state, "cache_operations_total");
        assert_eq!(operations.len(), 7);
        assert!(operations.iter().all(|measurement| {
            measurement.value == Number::U64(1)
                && measurement.attributes.len() == 2
                && measurement.attributes.contains_key("operation")
                && measurement
                    .attributes
                    .get("outcome")
                    .is_some_and(|outcome| outcome == "ok")
        }));
        let durations = measurements(&state, "cache_operation_duration_seconds");
        assert_eq!(durations.len(), 7);
        assert!(durations.iter().all(|measurement| {
            matches!(measurement.value, Number::F64(value) if value >= 0.0)
                && measurement.attributes.len() == 2
                && measurement.attributes.contains_key("operation")
                && measurement
                    .attributes
                    .get("outcome")
                    .is_some_and(|outcome| outcome == "ok")
        }));
    }

    #[test]
    fn test_telemetry_get_hit_records_hit_and_ok() {
        let (meter, state) = capture_meter();
        let document = with_telemetry(TestDocument::successful(), meter);

        futures::executor::block_on(document.get("private-id", &mut Vec::new())).unwrap();

        let gets = measurements(&state, "cache_gets_total");
        assert_eq!(gets.len(), 1);
        assert_eq!(gets[0].attributes.get("result").unwrap(), "hit");
        let operations = measurements(&state, "cache_operations_total");
        assert_eq!(operations[0].attributes.get("operation").unwrap(), "get");
        assert_eq!(operations[0].attributes.get("outcome").unwrap(), "ok");
    }

    #[test]
    fn test_telemetry_get_not_found_records_miss_and_ok() {
        let (meter, state) = capture_meter();
        let document = with_telemetry(TestDocument::failing(CacheError::NotFound, 1), meter);

        let result = futures::executor::block_on(document.get("missing", &mut Vec::new()));

        assert!(matches!(result, Err(CacheError::NotFound)));
        assert_eq!(
            measurements(&state, "cache_gets_total")[0]
                .attributes
                .get("result")
                .unwrap(),
            "miss"
        );
        assert_eq!(
            measurements(&state, "cache_operations_total")[0]
                .attributes
                .get("outcome")
                .unwrap(),
            "ok"
        );
    }

    #[test]
    fn test_telemetry_get_failure_records_error_without_hit_result() {
        let (meter, state) = capture_meter();
        let document = with_telemetry(
            TestDocument::failing(CacheError::Internal("credential-secret".to_owned()), 1),
            meter,
        );

        let result = futures::executor::block_on(document.get("private-id", &mut Vec::new()));

        assert!(result.is_err());
        assert!(measurements(&state, "cache_gets_total").is_empty());
        let operation = &measurements(&state, "cache_operations_total")[0];
        assert_eq!(operation.attributes.get("outcome").unwrap(), "error");
        let attributes = format!("{:?}", operation.attributes);
        assert!(!attributes.contains("private-id"));
        assert!(!attributes.contains("credential-secret"));
    }

    #[test]
    fn test_telemetry_non_get_not_found_records_error() {
        let (meter, state) = capture_meter();
        let document = with_telemetry(TestDocument::failing(CacheError::NotFound, 1), meter);

        let result = futures::executor::block_on(document.ttl("missing"));

        assert!(matches!(result, Err(CacheError::NotFound)));
        let operation = &measurements(&state, "cache_operations_total")[0];
        assert_eq!(operation.attributes.get("operation").unwrap(), "ttl");
        assert_eq!(operation.attributes.get("outcome").unwrap(), "error");
    }

    #[test]
    fn test_telemetry_middleware_order_controls_retry_measurements() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let (outer_meter, outer_state) = capture_meter();
        let outer = chain(
            TestDocument::failing(CacheError::Internal("transient".to_owned()), 1),
            [
                with_retry_middleware(2, Duration::ZERO),
                with_telemetry_middleware(outer_meter),
            ],
        );
        runtime.block_on(outer.get("id", &mut Vec::new())).unwrap();
        assert_eq!(
            measurements(&outer_state, "cache_operations_total").len(),
            1
        );

        let (inner_meter, inner_state) = capture_meter();
        let inner = chain(
            TestDocument::failing(CacheError::Internal("transient".to_owned()), 1),
            [
                with_telemetry_middleware(inner_meter),
                with_retry_middleware(2, Duration::ZERO),
            ],
        );
        runtime.block_on(inner.get("id", &mut Vec::new())).unwrap();
        assert_eq!(
            measurements(&inner_state, "cache_operations_total").len(),
            2
        );
    }

    #[test]
    fn test_telemetry_noop_meter_is_transparent() {
        let meter = NoopMeterProvider::new().meter("runtime-cache-test");
        let concrete = TestDocument::successful();
        let document = with_telemetry(concrete.clone(), meter);

        futures::executor::block_on(document.delete("id")).unwrap();

        assert_eq!(concrete.calls.load(Ordering::SeqCst), 1);
    }
}
