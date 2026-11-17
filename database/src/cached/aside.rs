//! The [`Cache`] over the runtime cache crate's Aside strategy, and the error translations
//! that make the decorator's contract hold across it. Go's `database/cached/aside.go`.

use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use cache::CacheError;
use cache::core::{Loader, Options};
use futures::FutureExt;

use super::{Aside, Cache, Load, Overloaded};
use crate::{Error, Result};

/// Adapts a selected cache database into a [`Cache`], applying `opts` (its TTL above all) to
/// every load. The whole of the wiring for the runtime cache crate. Go's `cached.FromAside`.
///
/// A TTL is not optional here: every write through the decorator invalidates what it wrote,
/// so entries do not normally go stale, but a write that fails to invalidate, a migration, a
/// dropped schema or another process writing past the decorator leaves a wrong entry wrong
/// until it expires. Set one in `opts` or as the cache's default.
pub fn from_aside(db: Arc<cache::DB>, opts: Options) -> Arc<dyn Cache> {
    Arc::new(AsideCache { db, opts })
}

/// One read-through view per resource over a cache database; built by [`from_aside`]. Go's
/// `asideCache`.
pub struct AsideCache {
    db: Arc<cache::DB>,
    opts: Options,
}

impl Cache for AsideCache {
    /// Binds `load` to a view, translating [`Error::NotFound`] into the cache's own
    /// `NotFound`: without that it sees an ordinary failure, caches nothing, and every
    /// request for a record that does not exist reaches the store. Any other error is
    /// carried through the cache as itself, so the caller gets it back typed.
    ///
    /// The cache stores JSON, so the bytes travel as a base64 JSON string, exactly what
    /// Go's codec makes of a `[]byte`: a third more space, and the only shape a bytes field
    /// that is not UTF-8 or an integer past 2^53 survives.
    fn aside(&self, load: Load) -> Arc<dyn Aside> {
        let loader: Loader = Arc::new(move |key: String| {
            let load = load.clone();
            async move {
                match load(key).await {
                    Ok(body) => serde_json::to_vec(&BASE64.encode(body))
                        .map_err(|e| CacheError::Internal(e.to_string())),
                    Err(Error::NotFound) => Err(CacheError::NotFound),
                    Err(other) => Err(CacheError::Loader(Arc::new(other))),
                }
            }
            .boxed()
        });
        Arc::new(AsideView {
            inner: self.db.aside(loader),
            opts: self.opts.clone(),
        })
    }
}

/// One resource's read-through view over a cache Aside. Go's `asideView`.
struct AsideView {
    inner: Arc<dyn cache::core::Aside>,
    opts: Options,
}

#[async_trait::async_trait]
impl Aside for AsideView {
    async fn get_or_load(&self, key: &str) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        match self.inner.get_or_load(key, &mut body, &self.opts).await {
            Ok(()) => unwire(&body),
            Err(CacheError::NotFound) => Err(Error::NotFound),
            Err(CacheError::Overloaded) => Err(Overloaded { key: key.into() }.into()),
            Err(CacheError::Loader(source)) => Err(restore(source)),
            Err(other) => Err(cache_error(other)),
        }
    }

    /// Tries every key, then reports every failure together, as Go's `errors.Join`.
    async fn invalidate(&self, keys: &[String]) -> Result<()> {
        let mut failures = Vec::new();
        for key in keys {
            if let Err(err) = self.inner.invalidate(key).await {
                failures.push(format!("{key}: {err}"));
            }
        }
        if failures.is_empty() {
            return Ok(());
        }
        Err(Error::Backend {
            backend: "cache",
            source: format!("invalidating {}", failures.join("; ")).into(),
        })
    }
}

/// The record bytes back out of the JSON string they were cached as.
fn unwire(body: &[u8]) -> Result<Vec<u8>> {
    let text: String = serde_json::from_slice(body)?;
    BASE64.decode(text).map_err(|e| Error::Backend {
        backend: "cache",
        source: Box::new(e),
    })
}

/// A cache failure as a database error: the cache backend's, not the store's.
fn cache_error(err: CacheError) -> Error {
    Error::Backend {
        backend: "cache",
        source: Box::new(err),
    }
}

/// The store's own error back out of the cache. One load's error reaches every caller it
/// collapsed, so each gets an equal copy: the variant itself where it can be rebuilt, and
/// otherwise a backend error sharing the original as its source.
fn restore(source: Arc<dyn std::error::Error + Send + Sync>) -> Error {
    let Some(err) = source.downcast_ref::<Error>() else {
        return cache_error(CacheError::Loader(source));
    };
    match err {
        Error::NotFound => Error::NotFound,
        Error::AlreadyExists => Error::AlreadyExists,
        Error::Closed => Error::Closed,
        Error::Timeout => Error::Timeout,
        Error::Invalid(msg) => Error::Invalid(msg.clone()),
        Error::HttpStatus { status } => Error::HttpStatus { status: *status },
        Error::Unimplemented {
            backend,
            capability,
        } => Error::Unimplemented {
            backend,
            capability,
        },
        Error::Ingestion(inner) => Error::Ingestion(inner.clone()),
        Error::Backend { backend, .. } => Error::Backend {
            backend,
            source: Box::new(Shared(source.clone())),
        },
        _ => Error::Backend {
            backend: "store",
            source: Box::new(Shared(source.clone())),
        },
    }
}

/// A store error shared between collapsed callers, displayed as the error it wraps less the
/// backend prefix the rebuilt [`Error::Backend`] adds back.
#[derive(Debug)]
struct Shared(Arc<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.downcast_ref::<Error>() {
            Some(Error::Backend { source, .. }) => source.fmt(f),
            _ => self.0.fmt(f),
        }
    }
}

impl std::error::Error for Shared {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.downcast_ref::<Error>() {
            Some(Error::Backend { source, .. }) => source.source(),
            other => other.and_then(|e| std::error::Error::source(e)),
        }
    }
}
