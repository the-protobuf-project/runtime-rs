//! Provider-independent composition for wrappers around publishing.
//!
//! Middleware takes and returns the same object-safe [`Publisher`] contract, so
//! retry, logging, telemetry, or application policy compose without backend
//! knowledge. Only the publish path is wrapped: it is the single method every
//! provider shares, and the subscribe side has its own logging wrapper.

use std::sync::Arc;

use crate::Publisher;

/// A reusable transformation from one shared Publisher to another.
pub type Middleware = Arc<dyn Fn(Arc<dyn Publisher>) -> Arc<dyn Publisher> + Send + Sync + 'static>;

/// Applies middleware in iteration order.
///
/// The last supplied middleware becomes the outermost wrapper and therefore
/// observes a complete call through every earlier one: put logging after retry
/// to see one logical publish, before it to see each attempt.
pub fn chain(
    mut publisher: Arc<dyn Publisher>,
    middlewares: impl IntoIterator<Item = Middleware>,
) -> Arc<dyn Publisher> {
    for middleware in middlewares {
        publisher = middleware(publisher);
    }
    publisher
}
