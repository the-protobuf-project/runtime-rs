//! Shared runtime observations for cache Document without logging keys/values.
use crate::{
    CacheError, Result,
    core::{Document, Options},
    middleware::Middleware,
};
use std::{future::Future, sync::Arc, time::Duration};
use telemetry::{FutureExt, Observer, Outcome, SpanKind, Telemetry};

struct Observed {
    next: Arc<dyn Document>,
    observer: Observer,
}
impl Observed {
    async fn call<T>(
        &self,
        operation: &'static str,
        future: impl Future<Output = Result<T>>,
        read: fn(&T) -> usize,
        written: usize,
    ) -> Result<T> {
        let mut guard = self.observer.start("cache", operation, SpanKind::Client);
        let result = future.with_context(guard.context()).await;
        if let Ok(value) = &result {
            guard.read_rows(read(value));
            guard.written_rows(written);
        }
        let outcome = match &result {
            Ok(_) => Outcome::Ok,
            Err(CacheError::NotFound) => Outcome::NotFound,
            Err(CacheError::Unsupported) => Outcome::Unsupported,
            Err(CacheError::NoTTL) => Outcome::Invalid,
            Err(_) => Outcome::Error,
        };
        guard.finish(outcome);
        result
    }
}
#[async_trait::async_trait]
impl Document for Observed {
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
        self.call("create", self.next.create(value, opts), |_| 0, 1)
            .await
    }
    async fn get(&self, key: &str, dest: &mut Vec<u8>) -> Result<()> {
        self.call("get", self.next.get(key, dest), |_| 1, 0).await
    }
    async fn update(&self, key: &str, value: &[u8], opts: &Options) -> Result<()> {
        self.call("update", self.next.update(key, value, opts), |_| 0, 1)
            .await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.call("delete", self.next.delete(key), |_| 0, 0).await
    }
    async fn keys(&self) -> Result<Vec<String>> {
        self.call("keys", self.next.keys(), |_| 0, 0).await
    }
    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        self.call("list", self.next.list(), Vec::len, 0).await
    }
    async fn ttl(&self, key: &str) -> Result<Duration> {
        self.call("ttl", self.next.ttl(key), |_| 0, 0).await
    }
}

/// Adds the shared runtime metrics, correlated logs and cancellation-safe traces.
/// Existing cache-only logging/metrics APIs remain available for compatibility.
/// No-op bindings return the original Document unchanged. Wrapper ordering
/// determines whether retries are measured as one logical operation or attempts.
pub fn with_observability(document: Arc<dyn Document>, signals: Telemetry) -> Arc<dyn Document> {
    if !signals.is_enabled() {
        return document;
    }
    Arc::new(Observed {
        next: document,
        observer: signals.observer("cache"),
    })
}

/// Reusable shared-signal middleware for the existing cache chain API.
pub fn with_observability_middleware(signals: Telemetry) -> Middleware {
    Arc::new(move |document| with_observability(document, signals.clone()))
}
