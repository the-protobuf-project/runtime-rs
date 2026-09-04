//! Retry middleware for idempotent Document reads and deletion.
//!
//! Reads and Delete may safely repeat after transient failures. Create and
//! Update do not: replaying a partially applied write can duplicate index work
//! or overwrite a newer value. Retry policy is provider-independent and keeps
//! all backend semantics in the wrapped Document.

use std::{future::Future, sync::Arc, time::Duration};

use crate::{
    CacheError, Result,
    core::{Document, Options},
    middleware::Middleware,
};

/// Document wrapper carrying one immutable retry schedule.
struct RetryDocument {
    /// Operation target; every attempt delegates to this same Document.
    next: Arc<dyn Document>,
    /// Total tries, always greater than one for a constructed wrapper.
    attempts: usize,
    /// Delay before the second try; later delays double saturatingly.
    backoff: Duration,
}

/// Per-call retry state shared by destination and owned-result paths.
struct RetrySchedule {
    /// Attempts not yet consumed, including the operation that just failed.
    remaining: usize,
    /// Delay returned for the next retry, doubled after each use.
    wait: Duration,
}

impl RetrySchedule {
    /// Starts one independent schedule from immutable wrapper configuration.
    fn new(attempts: usize, backoff: Duration) -> Self {
        Self {
            remaining: attempts,
            wait: backoff,
        }
    }

    /// Returns the next delay or the error that must terminate retrying.
    fn after_failure(&mut self, error: CacheError) -> Result<Duration> {
        self.remaining = self.remaining.saturating_sub(1);
        if matches!(error, CacheError::NotFound) || self.remaining == 0 {
            return Err(error);
        }

        let delay = self.wait;
        self.wait = self.wait.saturating_mul(2);
        Ok(delay)
    }
}

impl RetryDocument {
    /// Runs an owned-result operation under the immutable retry policy.
    ///
    /// NotFound terminates immediately. Other intermediate errors are dropped,
    /// and exhausting the schedule returns the last error exactly.
    async fn retry<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut schedule = RetrySchedule::new(self.attempts, self.backoff);
        loop {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    tokio::time::sleep(schedule.after_failure(error)?).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Document for RetryDocument {
    /// Delegates once because Create is not safely replayable.
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
        self.next.create(value, opts).await
    }

    /// Retries Get while passing the caller's destination to every attempt.
    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()> {
        let mut schedule = RetrySchedule::new(self.attempts, self.backoff);
        loop {
            match self.next.get(id, dest).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tokio::time::sleep(schedule.after_failure(error)?).await;
                }
            }
        }
    }

    /// Delegates once because Update is not safely replayable.
    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()> {
        self.next.update(id, value, opts).await
    }

    /// Retries idempotent deletion; a NotFound result terminates immediately.
    async fn delete(&self, id: &str) -> Result<()> {
        self.retry(|| self.next.delete(id)).await
    }

    /// Retries enumeration except for a settled NotFound result.
    async fn keys(&self) -> Result<Vec<String>> {
        self.retry(|| self.next.keys()).await
    }

    /// Retries owned byte-list reads without exposing intermediate results.
    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        self.retry(|| self.next.list()).await
    }

    /// Retries remaining-lease reads except for a settled missing entry.
    async fn ttl(&self, id: &str) -> Result<Duration> {
        self.retry(|| self.next.ttl(id)).await
    }
}

/// Wraps `document` with exponential retry for safe Document operations.
///
/// `attempts` counts the initial call. Zero or one returns the original Arc.
/// Get, Delete, Keys, List, and TTL retry every error except NotFound. Create
/// and Update always execute once. Backoff starts at `backoff`, doubles between
/// attempts, and saturates instead of overflowing.
///
/// **Cost**: Up to `attempts` underlying calls plus asynchronous backoff.
/// **Concurrency**: The immutable wrapper is safe to share; calls maintain
/// independent schedules and hold no locks while awaiting.
/// **Side effects**: Repeated reads retain their backend read accounting;
/// Delete may run repeatedly but is idempotent by the Document contract.
/// **When to use**: Masking transient failures for safe operations when the
/// caller accepts the added latency and has its own outer timeout/cancellation.
pub fn with_retry(
    document: Arc<dyn Document>,
    attempts: usize,
    backoff: Duration,
) -> Arc<dyn Document> {
    if attempts <= 1 {
        return document;
    }
    Arc::new(RetryDocument {
        next: document,
        attempts,
        backoff,
    })
}

/// Returns [`with_retry`] as reusable Document [`Middleware`].
///
/// **Cost/side effects**: Constructing the closure is local. Applying it has
/// the same behavior as `with_retry`; no Document operation runs until the
/// resulting wrapper is called.
pub fn with_retry_middleware(attempts: usize, backoff: Duration) -> Middleware {
    Arc::new(move |document| with_retry(document, attempts, backoff))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use serde::Deserialize;

    use super::*;
    use crate::{chain, typed};

    /// Failure returned by a scripted attempt before the success threshold.
    #[derive(Clone, Copy)]
    enum Failure {
        /// A retryable error carrying the attempt number.
        Internal,
        /// A settled cache miss.
        NotFound,
    }

    /// One deterministic Document whose methods share an attempt counter.
    struct FlakyDocument {
        calls: AtomicUsize,
        failures: usize,
        failure: Failure,
        body: Vec<u8>,
    }

    impl FlakyDocument {
        fn new(failures: usize, failure: Failure) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                failures,
                failure,
                body: b"value".to_vec(),
            }
        }

        fn with_body(failures: usize, body: &[u8]) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                failures,
                failure: Failure::Internal,
                body: body.to_vec(),
            }
        }

        fn attempt(&self) -> Result<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call > self.failures {
                return Ok(());
            }
            match self.failure {
                Failure::Internal => Err(CacheError::Internal(format!("failure {call}"))),
                Failure::NotFound => Err(CacheError::NotFound),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Document for FlakyDocument {
        async fn create(&self, _value: &[u8], _opts: &Options) -> Result<String> {
            self.attempt()?;
            Ok("created".to_owned())
        }

        async fn get(&self, _id: &str, dest: &mut Vec<u8>) -> Result<()> {
            self.attempt()?;
            *dest = self.body.clone();
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
            Ok(vec!["key".to_owned()])
        }

        async fn list(&self) -> Result<Vec<Vec<u8>>> {
            self.attempt()?;
            Ok(vec![self.body.clone()])
        }

        async fn ttl(&self, _id: &str) -> Result<Duration> {
            self.attempt()?;
            Ok(Duration::from_secs(5))
        }
    }

    fn retry(
        document: Arc<FlakyDocument>,
        attempts: usize,
        backoff: Duration,
    ) -> Arc<dyn Document> {
        with_retry(document, attempts, backoff)
    }

    #[test]
    fn test_retry_disabled_preserves_arc_identity() {
        for attempts in [0, 1] {
            let concrete = Arc::new(FlakyDocument::new(0, Failure::Internal));
            let document: Arc<dyn Document> = concrete;
            let retried = with_retry(document.clone(), attempts, Duration::from_secs(1));
            assert!(Arc::ptr_eq(&document, &retried));
        }
    }

    #[tokio::test]
    async fn test_retry_get_recovers_with_total_attempt_count() {
        let concrete = Arc::new(FlakyDocument::new(2, Failure::Internal));
        let document = retry(concrete.clone(), 3, Duration::ZERO);
        let mut value = Vec::new();

        document.get("id", &mut value).await.unwrap();

        assert_eq!(value, b"value");
        assert_eq!(concrete.calls(), 3);
    }

    #[tokio::test]
    async fn test_retry_exhaustion_returns_last_error() {
        let concrete = Arc::new(FlakyDocument::new(4, Failure::Internal));
        let result = retry(concrete.clone(), 3, Duration::ZERO)
            .get("id", &mut Vec::new())
            .await;

        assert!(matches!(result, Err(CacheError::Internal(message)) if message == "failure 3"));
        assert_eq!(concrete.calls(), 3);
    }

    #[tokio::test]
    async fn test_retry_not_found_stops_immediately() {
        let concrete = Arc::new(FlakyDocument::new(4, Failure::NotFound));
        let result = retry(concrete.clone(), 4, Duration::ZERO)
            .get("missing", &mut Vec::new())
            .await;

        assert!(matches!(result, Err(CacheError::NotFound)));
        assert_eq!(concrete.calls(), 1);
    }

    #[tokio::test]
    async fn test_retry_safe_document_operations_retry() {
        let deletes = Arc::new(FlakyDocument::new(1, Failure::Internal));
        retry(deletes.clone(), 2, Duration::ZERO)
            .delete("id")
            .await
            .unwrap();
        assert_eq!(deletes.calls(), 2);

        let keys = Arc::new(FlakyDocument::new(1, Failure::Internal));
        assert_eq!(
            retry(keys.clone(), 2, Duration::ZERO).keys().await.unwrap(),
            vec!["key"]
        );
        assert_eq!(keys.calls(), 2);

        let lists = Arc::new(FlakyDocument::new(1, Failure::Internal));
        assert_eq!(
            retry(lists.clone(), 2, Duration::ZERO)
                .list()
                .await
                .unwrap(),
            vec![b"value".to_vec()]
        );
        assert_eq!(lists.calls(), 2);

        let ttls = Arc::new(FlakyDocument::new(1, Failure::Internal));
        assert_eq!(
            retry(ttls.clone(), 2, Duration::ZERO)
                .ttl("id")
                .await
                .unwrap(),
            Duration::from_secs(5)
        );
        assert_eq!(ttls.calls(), 2);
    }

    #[tokio::test]
    async fn test_retry_create_and_update_execute_once() {
        let creates = Arc::new(FlakyDocument::new(3, Failure::Internal));
        let result = retry(creates.clone(), 3, Duration::ZERO)
            .create(b"value", &Options::default())
            .await;
        assert!(result.is_err());
        assert_eq!(creates.calls(), 1);

        let updates = Arc::new(FlakyDocument::new(3, Failure::Internal));
        let result = retry(updates.clone(), 3, Duration::ZERO)
            .update("id", b"value", &Options::default())
            .await;
        assert!(result.is_err());
        assert_eq!(updates.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_backoff_doubles_deterministically() {
        let concrete = Arc::new(FlakyDocument::new(2, Failure::Internal));
        let document = retry(concrete, 3, Duration::from_millis(10));
        let start = tokio::time::Instant::now();

        document.get("id", &mut Vec::new()).await.unwrap();

        assert_eq!(
            tokio::time::Instant::now() - start,
            Duration::from_millis(30)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_retry_dropped_future_stops_during_backoff() {
        let concrete = Arc::new(FlakyDocument::new(usize::MAX, Failure::Internal));
        let document = retry(concrete.clone(), 4, Duration::from_secs(10));
        let task = tokio::spawn(async move { document.get("id", &mut Vec::new()).await });
        tokio::task::yield_now().await;
        assert_eq!(concrete.calls(), 1);

        task.abort();
        let _ = task.await;
        tokio::time::advance(Duration::from_secs(100)).await;
        assert_eq!(concrete.calls(), 1);
    }

    #[tokio::test]
    async fn test_retry_middleware_composes_beneath_typed_view() {
        #[derive(Debug, Deserialize, Eq, PartialEq)]
        struct Model {
            name: String,
        }

        let concrete = Arc::new(FlakyDocument::with_body(1, br#"{"name":"Ada"}"#));
        let middleware = with_retry_middleware(2, Duration::ZERO);
        let model = typed::<Model>(chain(concrete.clone(), [middleware]));

        assert_eq!(model.get("id").await.unwrap(), Model {
            name: "Ada".to_owned()
        });
        assert_eq!(concrete.calls(), 2);
    }
}
