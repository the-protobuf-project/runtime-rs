/// Loader produces the value for an id when cache misses
pub type Loader = Box<dyn Fn(&str) -> futures::future::BoxFuture<'static, Result<Vec<u8>>> + Send + Sync>;

/// Aside is read-through caching over a Loader
#[async_trait::async_trait]
pub trait Aside: Send + Sync {
    /// GetOrLoad decodes cached entry, calling loader on miss
    async fn get_or_load(&self, id: &str, dest: &mut Vec<u8>, opts: &Options) -> Result<()>;

    /// Refresh runs the loader and overwrites the entry
    async fn refresh(&self, id: &str, opts: &Options) -> Result<()>;

    /// Invalidate drops the entry and any remembered absence
    async fn invalidate(&self, id: &str) -> Result<()>;
}
