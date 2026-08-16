use std::sync::Arc;

use futures::future::BoxFuture;

use crate::Result;

use super::Options;

/// An asynchronous source used to populate an [`Aside`] cache after a miss.
///
/// The owned ID allows the returned future to outlive the request that started
/// it. That property is necessary for single-flight loading and background
/// refresh: one caller abandoning its request must not invalidate work shared
/// by other callers. Returning [`crate::CacheError::NotFound`] reports a genuine
/// absence, which an Aside implementation may remember briefly.
///
/// `Arc` makes one loader safely shareable by foreground and background tasks.
/// `BoxFuture` keeps the public contract independent of the concrete future
/// type produced by a caller's async closure.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use futures::FutureExt;
///
/// let loader: Loader = Arc::new(|id: String| async move {
///     Ok(format!("loaded:{id}").into_bytes())
/// }.boxed());
/// ```
pub type Loader =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Vec<u8>>> + Send + Sync + 'static>;

/// Read-through caching over a caller-provided [`Loader`].
///
/// Aside centralizes the cache-aside sequence—read, load on miss, store, and
/// return—and is intended for values obtained from a slower database or remote
/// service. Implementations collapse concurrent loads for the same ID, may
/// remember loader-reported absence, and can serve stale values while refreshing
/// them in the background.
///
/// # Trade-offs
///
/// - Hits need no index and normally cost one driver round trip.
/// - Misses invoke an application-provided loader and write the result.
/// - Stale serving improves latency and protects the source, but callers must
///   explicitly accept temporarily old data.
/// - In-process single-flight bounds duplicate work within one process; safe
///   cross-process collapsing requires an additional fenced-lock capability.
///
/// Best for read-heavy data whose authoritative value lives outside the cache.
#[async_trait::async_trait]
pub trait Aside: Send + Sync {
    /// Returns the cached value or invokes the loader after a miss.
    ///
    /// Concurrent misses for one ID collapse into one loader execution. When a
    /// stale window is configured, a stale value may return immediately while
    /// one background refresh is requested. A remembered absence returns
    /// [`crate::CacheError::NotFound`] without invoking the loader again.
    ///
    /// **Cost**: One driver read on a hit. A miss adds one loader execution and
    /// one driver write for the caller that owns the shared load.
    ///
    /// **Side effects**: May populate the cache, remember an absence, or start a
    /// background refresh.
    ///
    /// Use this for ordinary reads where loading on demand is acceptable.
    async fn get_or_load(&self, id: &str, dest: &mut Vec<u8>, opts: &Options) -> Result<()>;

    /// Invokes the loader and overwrites the entry whether or not it exists.
    ///
    /// A refresh joins a load for the same ID that is already in progress. It
    /// is preferable to invalidation when the authoritative value has changed,
    /// because readers do not encounter a deliberate empty-cache window.
    ///
    /// **Cost**: One shared loader execution and one driver write.
    ///
    /// **Side effects**: Replaces the cached value or remembers a loader-
    /// reported absence according to the implementation's negative-cache lease.
    ///
    /// Use this after a known source update when the cache should be warmed now.
    async fn refresh(&self, id: &str, opts: &Options) -> Result<()>;

    /// Deletes the cached value or remembered absence for an ID.
    ///
    /// **Cost**: One driver delete round trip.
    ///
    /// **Side effects**: The next read must load again. Deleting a remembered
    /// absence makes a newly created source value visible immediately.
    ///
    /// Use this when no replacement value should be loaded yet.
    async fn invalidate(&self, id: &str) -> Result<()>;
}
