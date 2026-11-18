//! Shared runtime observations for every cache strategy, without logging keys or values.
//!
//! Each call is an operation of component `cache` on the backend (`redis`, `memcached`, or
//! `cache` where the backend is not known): a client span named `cache.<operation>` and a
//! share of the `runtime.operation.*` metrics. A miss is `not_found`, which does not fail
//! the span: an absent key is a normal cache answer. Go's `cache.Instrument`.
use crate::{
    CacheError, Result,
    core::{Aside, Document, Indexed, Options, Volatile},
    middleware::Middleware,
};
use std::{future::Future, sync::Arc, time::Duration};
use telemetry::{FutureExt, Observer, Outcome, SpanKind, Telemetry};

/// One observed strategy over `next`.
pub(crate) struct Observed<T: ?Sized> {
    pub(crate) next: Arc<T>,
    pub(crate) observer: Observer,
    pub(crate) backend: &'static str,
}

impl<T: ?Sized> Observed<T> {
    async fn call<R>(
        &self,
        operation: &'static str,
        future: impl Future<Output = Result<R>>,
        read: fn(&R) -> usize,
        written: usize,
    ) -> Result<R> {
        let mut guard = self
            .observer
            .start(self.backend, operation, SpanKind::Client);
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

/// Document over any `next` that is one: a plain Document, or an Indexed one.
macro_rules! observe_document {
    ($t:ty) => {
        #[async_trait::async_trait]
        impl Document for Observed<$t> {
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
    };
}
observe_document!(dyn Document);
observe_document!(dyn Indexed);

#[async_trait::async_trait]
impl Indexed for Observed<dyn Indexed> {
    async fn by_index(&self, field: &str, value: &str) -> Result<Vec<Vec<u8>>> {
        self.call("by_index", self.next.by_index(field, value), Vec::len, 0)
            .await
    }
    async fn ids_by_index(&self, field: &str, value: &str) -> Result<Vec<String>> {
        self.call(
            "ids_by_index",
            self.next.ids_by_index(field, value),
            Vec::len,
            0,
        )
        .await
    }
    async fn delete_by_index(&self, field: &str, value: &str) -> Result<usize> {
        self.call(
            "delete_by_index",
            self.next.delete_by_index(field, value),
            |_| 0,
            0,
        )
        .await
    }
}

#[async_trait::async_trait]
impl Volatile for Observed<dyn Volatile> {
    async fn set(&self, key: &str, value: &[u8], opts: &Options) -> Result<()> {
        self.call("set", self.next.set(key, value, opts), |_| 0, 1)
            .await
    }
    async fn get(&self, key: &str, dest: &mut Vec<u8>) -> Result<()> {
        self.call("get", self.next.get(key, dest), |_| 1, 0).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.call("delete", self.next.delete(key), |_| 0, 0).await
    }
    async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
        self.call("touch", self.next.touch(key, ttl), |_| 0, 0)
            .await
    }
    async fn ttl(&self, key: &str) -> Result<Duration> {
        self.call("ttl", self.next.ttl(key), |_| 0, 0).await
    }
    async fn scan(&self, pattern: &str) -> Result<Vec<String>> {
        self.call("scan", self.next.scan(pattern), Vec::len, 0)
            .await
    }
}

#[async_trait::async_trait]
impl Aside for Observed<dyn Aside> {
    async fn get_or_load(&self, id: &str, dest: &mut Vec<u8>, opts: &Options) -> Result<()> {
        self.call(
            "get_or_load",
            self.next.get_or_load(id, dest, opts),
            |_| 1,
            0,
        )
        .await
    }
    async fn refresh(&self, id: &str, opts: &Options) -> Result<()> {
        self.call("refresh", self.next.refresh(id, opts), |_| 0, 1)
            .await
    }
    async fn invalidate(&self, id: &str) -> Result<()> {
        self.call("invalidate", self.next.invalidate(id), |_| 0, 0)
            .await
    }
}

/// The metric label for a driver: the known backends by name, anything else `cache`, so
/// the label set stays bounded.
pub(crate) fn backend_label(name: &str) -> &'static str {
    match name {
        "redis" => "redis",
        "dragonfly" => "dragonfly",
        "memcached" => "memcached",
        "memory" => "memory",
        _ => "cache",
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
        backend: "cache",
    })
}

/// Reusable shared-signal middleware for the existing cache chain API.
pub fn with_observability_middleware(signals: Telemetry) -> Middleware {
    Arc::new(move |document| with_observability(document, signals.clone()))
}
