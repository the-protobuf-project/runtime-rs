//! A read-through cache in front of any records [`Driver`](crate::records::Driver), so a
//! record read repeatedly is fetched from the backing store once. Go's `database/cached`.
//!
//! It is a decorator, not a backend: [`wrap`] takes a [`DB`](crate::records::DB) and returns
//! one, so typed views and every existing call site keep working, and the database
//! underneath keeps its capabilities.
//!
//! ```ignore
//! let client = Arc::new(RedisClient::connect(RedisConfig { address: "localhost:6379".into(), ..Default::default() }).await?);
//! let cdb = RedisProvider::new(client, Config { default_ttl: Duration::from_secs(300), ..Default::default() })
//!     .set_database("records").await?;
//!
//! let db = provider.set_database("tenant_a").await?;
//! let db = cached::wrap(&db, cached::from_aside(Arc::new(cdb), Options::default().with_ttl(Duration::from_secs(60))));
//!
//! db.get(&books, "books/dune").await?; // Redis first, the store on a miss
//! ```
//!
//! # What it is for
//!
//! The read path, and only the read path. Concurrent misses on one key collapse into a
//! single load, so a hot record expiring under traffic costs the backing store one query
//! rather than one per caller; an absence is remembered, so requests for a record that does
//! not exist stop reaching it at all. See [`Driver`] for what is deliberately not cached.
//!
//! # Correctness
//!
//! Writes go to the backing store first and drop the cached copy after, so a record is never
//! served from the cache after a write it did not observe, except in the window between
//! those two steps. A transaction is handled the same way but deferred: reads inside one go
//! straight to the backing store, because a transaction has to see its own writes, and what
//! it wrote is dropped from the cache once it commits, and left alone when it rolls back.
//!
//! Records travel through the cache as proto wire format, decoded on the way out against the
//! resource's message descriptor. The cache stores JSON, which cannot carry a proto message,
//! so [`from_aside`] hands it the wire bytes as a base64 string, as Go's codec does a
//! `[]byte`; anything else silently corrupts a bytes field or a large integer.
//!
//! Entries are keyed by the record key alone, as in Go, inside the cache database's own
//! namespace, so a Go and a Rust service over one cache database share entries. Two
//! resources sharing a cache database must not share keys: AIP resource names
//! (`books/dune`) never do; bare numeric ids can, so give such resources a database each.
//!
//! # A TTL is not optional
//!
//! Every write invalidates, so entries do not normally go stale. The TTL covers what falls
//! outside that: an invalidation that fails after its write succeeded, a schema dropped
//! underneath, another process writing to the same database without this decorator in front
//! of it. Without one, a wrong entry stays wrong until something overwrites it.

pub(crate) mod aside;
mod bulk;
mod driver;
mod tx;

use std::sync::Arc;

use futures::future::BoxFuture;

use crate::{Error, Result};

pub use aside::{AsideCache, from_aside};
pub use driver::Driver;
pub use tx::wrap;

/// Loads one record's encoded bytes by key on a miss; [`Error::NotFound`] for a record that
/// is not there. The loader argument of Go's `Cache.Aside`.
pub type Load = Arc<dyn Fn(String) -> BoxFuture<'static, Result<Vec<u8>>> + Send + Sync>;

/// The read-through storage this decorator needs: narrow, so the decorator is not welded to
/// one cache. [`from_aside`] adapts the runtime cache crate; anything that can collapse
/// concurrent loads and remember an absence satisfies it. Go's `cached.Cache`.
pub trait Cache: Send + Sync {
    /// A read-through view calling `load` on a miss; one per resource, built once. A load
    /// reporting [`Error::NotFound`] must have that absence remembered, or requests for a
    /// record that does not exist reach the backing store forever.
    fn aside(&self, load: Load) -> Arc<dyn Aside>;
}

/// One resource's read-through view. Go's `cached.Aside`.
#[async_trait::async_trait]
pub trait Aside: Send + Sync {
    /// The record's encoded bytes, loading on a miss; concurrent calls for one key collapse
    /// into one load. [`Error::NotFound`] for a record that is not there, remembered or not;
    /// an [`Overloaded`] error when the cache will not start another load.
    async fn get_or_load(&self, key: &str) -> Result<Vec<u8>>;

    /// Drops the entry for each key, and any remembered absence with it: something that did
    /// not exist a moment ago and does now would otherwise stay invisible until that
    /// absence expired on its own.
    async fn invalidate(&self, keys: &[String]) -> Result<()>;
}

/// What a [`Cache`] reports when it will not start another load. Declared here rather than
/// taken from one cache crate so the decorator stays swappable: [`from_aside`] translates
/// the runtime cache's own `Overloaded` into this. A [`Driver`] `get` seeing it reads through
/// to the backing store rather than failing. Go's `cached.ErrOverloaded`.
#[derive(Clone, Debug, thiserror::Error)]
#[error("cached: the cache is not accepting new loads: {key}")]
pub struct Overloaded {
    /// The key whose load was refused.
    pub key: String,
}

impl Overloaded {
    /// Whether `err` is an [`Overloaded`] refusal, as Go's `errors.Is(err, ErrOverloaded)`.
    pub fn is(err: &Error) -> bool {
        matches!(err, Error::Backend { source, .. } if source.is::<Overloaded>())
    }
}

impl From<Overloaded> for Error {
    /// Carried as a backend error of the cache, so [`Error`] gains no variant for it.
    fn from(refusal: Overloaded) -> Self {
        Error::Backend {
            backend: "cache",
            source: Box::new(refusal),
        }
    }
}
