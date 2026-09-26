//! Structured tracing middleware for raw Document operations.
//!
//! The wrapper observes the provider-independent Document boundary. It emits
//! events through the caller's active tracing subscriber, installs no global
//! subscriber, and never records cached values or operation options.

use std::{sync::Arc, time::Instant};

use crate::{
    CacheError, Result,
    core::{Document, Options},
    middleware::Middleware,
};

/// Stable tracing target for all Document middleware events.
const TARGET: &str = "runtime_cache::document";

/// Document wrapper that records the outcome of each delegated operation.
///
/// The wrapper owns only the next Document. It has no storage, mutable policy,
/// synchronization, or background work and may be shared across tasks.
struct LoggingDocument {
    /// Operation target invoked exactly once by each wrapper method.
    next: Arc<dyn Document>,
}

impl LoggingDocument {
    /// Emits the severity and common fields corresponding to one result.
    ///
    /// An absent ID is omitted rather than recorded as an empty value. This is
    /// used by Keys/List and by failed Create, whose Result carries no ID.
    fn record<T>(
        &self,
        operation: &'static str,
        id: Option<&str>,
        start: Instant,
        result: &Result<T>,
    ) {
        let duration = start.elapsed();
        match (result, id) {
            (Ok(_), Some(id)) => tracing::debug!(
                target: TARGET,
                operation,
                id,
                duration = ?duration,
                "cache operation succeeded"
            ),
            (Ok(_), None) => tracing::debug!(
                target: TARGET,
                operation,
                duration = ?duration,
                "cache operation succeeded"
            ),
            (Err(CacheError::NotFound), Some(id)) => tracing::warn!(
                target: TARGET,
                operation,
                id,
                duration = ?duration,
                "cache miss"
            ),
            (Err(CacheError::NotFound), None) => tracing::warn!(
                target: TARGET,
                operation,
                duration = ?duration,
                "cache miss"
            ),
            (Err(error), Some(id)) => tracing::error!(
                target: TARGET,
                operation,
                id,
                duration = ?duration,
                error = %error,
                "cache operation failed"
            ),
            (Err(error), None) => tracing::error!(
                target: TARGET,
                operation,
                duration = ?duration,
                error = %error,
                "cache operation failed"
            ),
        }
    }
}

#[async_trait::async_trait]
impl Document for LoggingDocument {
    /// Delegates Create once and records its successfully returned ID.
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
        let start = Instant::now();
        let result = self.next.create(value, opts).await;
        self.record("create", result.as_deref().ok(), start, &result);
        result
    }

    /// Delegates Get once and records its ID and hit, miss, or failure outcome.
    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()> {
        let start = Instant::now();
        let result = self.next.get(id, dest).await;
        self.record("get", Some(id), start, &result);
        result
    }

    /// Delegates Update once without recording the serialized value or options.
    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()> {
        let start = Instant::now();
        let result = self.next.update(id, value, opts).await;
        self.record("update", Some(id), start, &result);
        result
    }

    /// Delegates Delete once and records its ID and outcome.
    async fn delete(&self, id: &str) -> Result<()> {
        let start = Instant::now();
        let result = self.next.delete(id).await;
        self.record("delete", Some(id), start, &result);
        result
    }

    /// Delegates Keys once and records both its outcome and successful count.
    async fn keys(&self) -> Result<Vec<String>> {
        let start = Instant::now();
        let result = self.next.keys().await;
        self.record("keys", None, start, &result);
        if let Ok(keys) = &result {
            tracing::debug!(
                target: TARGET,
                operation = "keys",
                count = keys.len(),
                "cache keys returned"
            );
        }
        result
    }

    /// Delegates List once without recording returned cached values.
    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        let start = Instant::now();
        let result = self.next.list().await;
        self.record("list", None, start, &result);
        result
    }

    /// Delegates TTL once and records its ID without recording the returned lease.
    async fn ttl(&self, id: &str) -> Result<std::time::Duration> {
        let start = Instant::now();
        let result = self.next.ttl(id).await;
        self.record("ttl", Some(id), start, &result);
        result
    }
}

/// Wraps a raw Document with structured operation logging.
///
/// Successful operations emit Debug events, NotFound emits a Warn cache-miss
/// event, and all other failures emit Error events. Records use the
/// `runtime_cache::document` target and include operation, elapsed duration,
/// and an ID when one is available. Cached values and Options are never logged.
///
/// The active tracing subscriber controls filtering and output; this function
/// does not install global state. Middleware order controls whether a logging
/// wrapper outside retry observes the complete sequence or one inside retry
/// observes every attempt.
///
/// **Cost**: One underlying operation, one monotonic-clock measurement, and
/// one tracing event; successful Keys emits one additional count event.
/// **Concurrency**: Immutable and shareable, with no locks or detached tasks.
/// **Side effects**: Emits structured events when enabled by the subscriber.
/// **When to use**: Observing raw Document latency and outcomes without
/// coupling strategies or drivers to an application logging backend.
pub fn with_logging(document: Arc<dyn Document>) -> Arc<dyn Document> {
    Arc::new(LoggingDocument { next: document })
}

/// Returns [`with_logging`] as reusable Document [`Middleware`].
///
/// **Cost/side effects**: Constructing and applying the closure performs no
/// Document operation. The resulting wrapper behaves exactly as
/// [`with_logging`] when called.
pub fn with_logging_middleware() -> Middleware {
    Arc::new(with_logging)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fmt,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tracing::{
        Event, Level, Metadata, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
    };

    use super::*;
    use crate::{chain, with_retry_middleware};

    /// One captured tracing event with display-normalized field values.
    #[derive(Debug)]
    struct CapturedEvent {
        level: Level,
        target: String,
        fields: BTreeMap<String, String>,
    }

    /// Test subscriber that stores only events; spans are accepted as no-ops.
    #[derive(Clone, Default)]
    struct CaptureSubscriber {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
        next_span: Arc<AtomicU64>,
    }

    impl Subscriber for CaptureSubscriber {
        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed) + 1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            self.events.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                target: event.metadata().target().to_owned(),
                fields: visitor.fields,
            });
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    /// Visitor that makes structured values easy to assert without formatting logs.
    #[derive(Default)]
    struct FieldVisitor {
        fields: BTreeMap<String, String>,
    }

    impl Visit for FieldVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields
                .insert(field.name().to_owned(), value.to_owned());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields
                .insert(field.name().to_owned(), value.to_string());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.fields
                .insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    /// Deterministic Document supporting success, fixed failure, and retries.
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

    /// Runs synchronous test work under one isolated tracing dispatcher.
    fn capture<T>(run: impl FnOnce() -> T) -> (T, Vec<CapturedEvent>) {
        let subscriber = CaptureSubscriber::default();
        let events = subscriber.events.clone();
        let dispatch = tracing::Dispatch::new(subscriber);
        let output = tracing::dispatcher::with_default(&dispatch, run);
        let captured = std::mem::take(&mut *events.lock().unwrap());
        (output, captured)
    }

    fn primary(events: &[CapturedEvent]) -> Vec<&CapturedEvent> {
        events
            .iter()
            .filter(|event| {
                event.fields.get("message").is_some_and(|message| {
                    matches!(
                        message.as_str(),
                        "cache operation succeeded" | "cache miss" | "cache operation failed"
                    )
                })
            })
            .collect()
    }

    #[test]
    fn test_logging_success_records_debug_fields_and_preserves_result() {
        let document = with_logging(TestDocument::successful());
        let ((result, body), events) = capture(|| {
            futures::executor::block_on(async {
                let mut body = Vec::new();
                let result = document.get("abc", &mut body).await;
                (result, body)
            })
        });

        assert!(result.is_ok());
        assert_eq!(body, b"body");
        let records = primary(&events);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].level, Level::DEBUG);
        assert_eq!(records[0].target, TARGET);
        assert_eq!(records[0].fields.get("operation").unwrap(), "get");
        assert_eq!(records[0].fields.get("id").unwrap(), "abc");
        assert!(records[0].fields.contains_key("duration"));
    }

    #[test]
    fn test_logging_not_found_records_warn_without_error() {
        let document = with_logging(TestDocument::failing(CacheError::NotFound, 1));
        let (result, events) =
            capture(|| futures::executor::block_on(document.get("missing", &mut Vec::new())));

        assert!(matches!(result, Err(CacheError::NotFound)));
        let records = primary(&events);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].level, Level::WARN);
        assert!(!records[0].fields.contains_key("error"));
    }

    #[test]
    fn test_logging_failure_records_error_and_preserves_failure() {
        let document = with_logging(TestDocument::failing(
            CacheError::Internal("backend down".to_owned()),
            1,
        ));
        let (result, events) = capture(|| futures::executor::block_on(document.delete("abc")));

        assert!(matches!(result, Err(CacheError::Internal(message)) if message == "backend down"));
        let records = primary(&events);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].level, Level::ERROR);
        assert!(
            records[0]
                .fields
                .get("error")
                .unwrap()
                .contains("backend down")
        );
    }

    #[test]
    fn test_logging_create_records_assigned_id_without_value_or_options() {
        let document = with_logging(TestDocument::successful());
        let options = Options::default().with_id("requested");
        let (result, events) =
            capture(|| futures::executor::block_on(document.create(b"secret-body", &options)));

        assert_eq!(result.unwrap(), "generated");
        let record = primary(&events)[0];
        assert_eq!(record.fields.get("id").unwrap(), "generated");
        let fields = format!("{:?}", record.fields);
        assert!(!fields.contains("secret-body"));
        assert!(!fields.contains("requested"));
    }

    #[test]
    fn test_logging_keys_records_count() {
        let document = with_logging(TestDocument::successful());
        let (result, events) = capture(|| futures::executor::block_on(document.keys()));

        assert_eq!(result.unwrap(), vec!["one", "two"]);
        let count = events
            .iter()
            .find(|event| {
                event
                    .fields
                    .get("message")
                    .is_some_and(|message| message == "cache keys returned")
            })
            .unwrap();
        assert_eq!(count.level, Level::DEBUG);
        assert_eq!(count.fields.get("count").unwrap(), "2");
    }

    #[test]
    fn test_logging_all_document_operations_delegate_once() {
        let concrete = TestDocument::successful();
        let document = with_logging(concrete.clone());
        let (_, events) = capture(|| {
            futures::executor::block_on(async {
                document
                    .create(b"value", &Options::default())
                    .await
                    .unwrap();
                document.get("id", &mut Vec::new()).await.unwrap();
                document
                    .update("id", b"value", &Options::default())
                    .await
                    .unwrap();
                document.delete("id").await.unwrap();
                document.keys().await.unwrap();
                document.list().await.unwrap();
                document.ttl("id").await.unwrap();
            })
        });

        assert_eq!(concrete.calls.load(Ordering::SeqCst), 7);
        assert_eq!(primary(&events).len(), 7);
    }

    #[test]
    fn test_logging_middleware_order_controls_retry_event_scope() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let outer_base = TestDocument::failing(CacheError::Internal("transient".to_owned()), 1);
        let outer = chain(
            outer_base,
            [
                with_retry_middleware(2, Duration::ZERO),
                with_logging_middleware(),
            ],
        );
        let (_, outer_events) = capture(|| {
            runtime.block_on(outer.get("id", &mut Vec::new())).unwrap();
        });
        assert_eq!(primary(&outer_events).len(), 1);

        let inner_base = TestDocument::failing(CacheError::Internal("transient".to_owned()), 1);
        let inner = chain(
            inner_base,
            [
                with_logging_middleware(),
                with_retry_middleware(2, Duration::ZERO),
            ],
        );
        let (_, inner_events) = capture(|| {
            runtime.block_on(inner.get("id", &mut Vec::new())).unwrap();
        });
        assert_eq!(primary(&inner_events).len(), 2);
    }
}
