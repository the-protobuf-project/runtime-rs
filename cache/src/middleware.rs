//! Provider-independent composition for wrappers around Document operations.
//!
//! Middleware takes and returns the same object-safe Document contract, so
//! retry, logging, telemetry, rate limiting, or application policy can compose
//! without backend knowledge. Wrappers own no storage unless their own public
//! contract explicitly says otherwise.

use std::sync::Arc;

use crate::core::Document;

/// A reusable transformation from one shared Document to another.
///
/// Arc ownership lets one configured middleware be applied to multiple
/// Documents safely. Implementations should normally wrap `next` and delegate
/// every operation they do not deliberately change.
pub type Middleware = Arc<dyn Fn(Arc<dyn Document>) -> Arc<dyn Document> + Send + Sync + 'static>;

/// Applies Document middleware in iteration order.
///
/// The last supplied middleware becomes the outermost wrapper and therefore
/// observes a complete call through every earlier wrapper. An empty iterator
/// returns the original Arc unchanged.
///
/// **Cost**: One local middleware invocation per element and no backend I/O.
/// **Side effects**: Defined only by middleware constructors; `chain` itself
/// performs no Document operation.
/// **When to use**: Building one provider-independent Document stack before
/// constructing a typed view or handing it to application code.
pub fn chain(
    mut document: Arc<dyn Document>,
    middlewares: impl IntoIterator<Item = Middleware>,
) -> Arc<dyn Document> {
    for middleware in middlewares {
        document = middleware(document);
    }
    document
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::Mutex;

    use super::*;
    use crate::{Result, core::Options};

    /// Minimal Document that records one terminal Get call.
    struct BaseDocument {
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl Document for BaseDocument {
        async fn create(&self, _value: &[u8], _opts: &Options) -> Result<String> {
            Ok("id".to_owned())
        }

        async fn get(&self, _id: &str, dest: &mut Vec<u8>) -> Result<()> {
            self.events.lock().await.push("base".to_owned());
            *dest = b"value".to_vec();
            Ok(())
        }

        async fn update(&self, _id: &str, _value: &[u8], _opts: &Options) -> Result<()> {
            Ok(())
        }

        async fn delete(&self, _id: &str) -> Result<()> {
            Ok(())
        }

        async fn keys(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn list(&self) -> Result<Vec<Vec<u8>>> {
            Ok(Vec::new())
        }

        async fn ttl(&self, _id: &str) -> Result<Duration> {
            Ok(Duration::ZERO)
        }
    }

    /// Delegating wrapper that records entry and exit around Get.
    struct RecordingDocument {
        next: Arc<dyn Document>,
        label: &'static str,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl Document for RecordingDocument {
        async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
            self.next.create(value, opts).await
        }

        async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()> {
            self.events
                .lock()
                .await
                .push(format!("{} before", self.label));
            let result = self.next.get(id, dest).await;
            self.events
                .lock()
                .await
                .push(format!("{} after", self.label));
            result
        }

        async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()> {
            self.next.update(id, value, opts).await
        }

        async fn delete(&self, id: &str) -> Result<()> {
            self.next.delete(id).await
        }

        async fn keys(&self) -> Result<Vec<String>> {
            self.next.keys().await
        }

        async fn list(&self) -> Result<Vec<Vec<u8>>> {
            self.next.list().await
        }

        async fn ttl(&self, id: &str) -> Result<Duration> {
            self.next.ttl(id).await
        }
    }

    /// Builds one reusable call-recording middleware.
    fn recording(label: &'static str, events: Arc<Mutex<Vec<String>>>) -> Middleware {
        Arc::new(move |next| {
            Arc::new(RecordingDocument {
                next,
                label,
                events: events.clone(),
            })
        })
    }

    fn base(events: Arc<Mutex<Vec<String>>>) -> Arc<dyn Document> {
        Arc::new(BaseDocument { events })
    }

    #[test]
    fn test_middleware_chain_empty_preserves_arc_identity() {
        let document = base(Arc::new(Mutex::new(Vec::new())));

        let chained = chain(document.clone(), Vec::<Middleware>::new());

        assert!(Arc::ptr_eq(&document, &chained));
    }

    #[tokio::test]
    async fn test_middleware_chain_last_wrapper_is_outermost() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let document = chain(
            base(events.clone()),
            [
                recording("first", events.clone()),
                recording("second", events.clone()),
            ],
        );

        document.get("id", &mut Vec::new()).await.unwrap();

        assert_eq!(
            *events.lock().await,
            vec![
                "second before",
                "first before",
                "base",
                "first after",
                "second after",
            ]
        );
    }

    #[tokio::test]
    async fn test_middleware_value_reuses_configuration() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let middleware = recording("shared", events.clone());
        let first = chain(base(events.clone()), [middleware.clone()]);
        let second = chain(base(events.clone()), [middleware]);

        first.get("one", &mut Vec::new()).await.unwrap();
        second.get("two", &mut Vec::new()).await.unwrap();

        assert_eq!(
            events
                .lock()
                .await
                .iter()
                .filter(|event| event.as_str() == "shared before")
                .count(),
            2
        );
    }
}
