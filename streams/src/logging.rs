//! Structured tracing around publishing and subscribing.
//!
//! Events go to whatever tracing subscriber the application installed; nothing
//! here installs one. Payload bytes are never logged.

use std::{sync::Arc, time::Instant};

use futures::StreamExt;

use crate::{
    Middleware, Options, Payload, Publisher, Result, StreamError, Subscriber, Subscription,
};

/// Publisher that logs each publish and its outcome.
struct LoggingPublisher {
    next: Arc<dyn Publisher>,
}

/// Logs every publish: debug on success, error on failure, naming an undeclared
/// subject as such.
pub fn with_logging(next: Arc<dyn Publisher>) -> Arc<dyn Publisher> {
    Arc::new(LoggingPublisher { next })
}

/// [`with_logging`] as [`Middleware`].
pub fn with_logging_middleware() -> Middleware {
    Arc::new(with_logging)
}

#[async_trait::async_trait]
impl Publisher for LoggingPublisher {
    /// Logs after the inner publish returns and hands its result back untouched.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let start = Instant::now();
        let codec = payload.codec().to_owned();
        let result = self.next.publish(subject, payload, opts).await;
        let duration = format!("{:?}", start.elapsed());
        let ttl = opts.delay();
        match &result {
            Ok(id) => tracing::debug!(subject, id, codec, duration, ttl = ?ttl, "published"),
            Err(error @ StreamError::UnknownSubject { .. }) => tracing::error!(
                subject,
                %error,
                duration,
                "publish rejected: subject not declared by this stream"
            ),
            Err(error) => tracing::error!(subject, %error, duration, ttl = ?ttl, "publish failed"),
        }
        result
    }
}

/// Subscriber that logs subscription lifecycle and, at debug, each delivery.
struct LoggingSubscriber {
    next: Arc<dyn Subscriber>,
}

/// Logs subscribe, each delivery at debug, and the subscription's end with the
/// number delivered.
pub fn with_subscriber_logging(next: Arc<dyn Subscriber>) -> Arc<dyn Subscriber> {
    Arc::new(LoggingSubscriber { next })
}

/// Logs the end of a subscription when the stream holding it is dropped.
struct Closed {
    subject: String,
    delivered: usize,
}

impl Drop for Closed {
    /// Fires once, when the subscription stream that owns this guard is dropped.
    fn drop(&mut self) {
        tracing::info!(
            subject = self.subject,
            delivered = self.delivered,
            "subscription closed"
        );
    }
}

#[async_trait::async_trait]
impl Subscriber for LoggingSubscriber {
    /// Wraps the inner stream so each delivery is counted and logged at debug.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        let messages = match self.next.subscribe(subject, opts).await {
            Ok(messages) => messages,
            Err(error) => {
                tracing::error!(subject, %error, "subscribe failed");
                return Err(error);
            }
        };
        tracing::info!(subject, "subscribed");
        let mut closed = Closed {
            subject: subject.to_owned(),
            delivered: 0,
        };
        Ok(messages
            .inspect(move |message| {
                // Borrowing the whole guard moves it into the closure; naming
                // only the field would capture the counter alone and drop the
                // guard here, logging the close before the first delivery.
                let closed = &mut closed;
                closed.delivered += 1;
                tracing::debug!(subject = message.subject(), id = message.id(), "delivered");
            })
            .boxed())
    }
}
