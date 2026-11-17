//! Custom logic a [`RecordsRepository`](super::RecordsRepository) runs around its writes and
//! reads without being edited: Go's generated `<Model>Hooks`.

use std::future::Future;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::Result;

type OnOne<M> = Arc<dyn Fn(M) -> BoxFuture<'static, Result<M>> + Send + Sync>;
type OnUpdate<M> = Arc<dyn Fn(M, M, Vec<String>) -> BoxFuture<'static, Result<M>> + Send + Sync>;
type OnDelete = Arc<dyn Fn(String) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Normalization and derivation before writes, derived fields after reads, and guards before
/// deletes, as Go's `<Model>Hooks`. An unset hook is skipped; an error from one fails the
/// call, before anything is written. Each takes what it changes and returns it, so a hook
/// can be `async` and await a lookup.
///
/// ```
/// # use database::repository::Hooks;
/// let hooks = Hooks::<prost_types::Duration>::default()
///     .before_create(|mut m| async move {
///         m.nanos = 0; // normalize before it is stored
///         Ok(m)
///     });
/// ```
pub struct Hooks<M> {
    pub(super) before_create: Option<OnOne<M>>,
    pub(super) after_read: Option<OnOne<M>>,
    pub(super) before_update: Option<OnUpdate<M>>,
    pub(super) before_delete: Option<OnDelete>,
}

impl<M> Default for Hooks<M> {
    fn default() -> Self {
        Self {
            before_create: None,
            after_read: None,
            before_update: None,
            before_delete: None,
        }
    }
}

impl<M> Clone for Hooks<M> {
    fn clone(&self) -> Self {
        Self {
            before_create: self.before_create.clone(),
            after_read: self.after_read.clone(),
            before_update: self.before_update.clone(),
            before_delete: self.before_delete.clone(),
        }
    }
}

impl<M: Send + 'static> Hooks<M> {
    /// Runs on the record `create` is given, before its etag is minted and it is written.
    pub fn before_create<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M>> + Send + 'static,
    {
        self.before_create = Some(Arc::new(move |m| f(m).boxed()));
        self
    }

    /// Runs on every record a read returns: `get`, `list`, and the re-read each write
    /// returns.
    pub fn after_read<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M>> + Send + 'static,
    {
        self.after_read = Some(Arc::new(move |m| f(m).boxed()));
        self
    }

    /// Runs inside `update`'s write, after the mask is applied: given the stored record, the
    /// merged one and the mask, it returns the record to write. The etag is minted after it.
    /// `update_output_only` does not run it, as in Go.
    pub fn before_update<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(M, M, Vec<String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M>> + Send + 'static,
    {
        self.before_update = Some(Arc::new(move |old, new, paths| f(old, new, paths).boxed()));
        self
    }

    /// Runs with the name before `delete` or `purge` writes anything; an error vetoes.
    pub fn before_delete<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.before_delete = Some(Arc::new(move |name| f(name).boxed()));
        self
    }
}
