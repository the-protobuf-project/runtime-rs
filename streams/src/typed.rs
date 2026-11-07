//! A compile-time-checked view over a raw publisher and subscriber.

use std::{marker::PhantomData, sync::Arc};

use futures::{StreamExt, stream::BoxStream};

use crate::{Codec, Json, Manager, Options, Payload, Publisher, Result, Subscriber};

/// Publishes and receives `T` through codec `C`, so the compiler checks the
/// model a call site sends and expects.
///
/// It owns no state beyond the codec: the publisher and subscriber are the ones
/// it was given, so wrapping a decorated publisher in a Typed view keeps the
/// decoration.
pub struct Typed<T, C = Json> {
    publisher: Arc<dyn Publisher>,
    subscriber: Arc<dyn Subscriber>,
    codec: C,
    _model: PhantomData<fn() -> T>,
}

impl<T, C: Clone> Clone for Typed<T, C> {
    fn clone(&self) -> Self {
        Self {
            publisher: self.publisher.clone(),
            subscriber: self.subscriber.clone(),
            codec: self.codec.clone(),
            _model: PhantomData,
        }
    }
}

/// A JSON view of `manager`.
pub fn typed<T>(manager: Arc<dyn Manager>) -> Typed<T, Json>
where
    Json: Codec<T>,
{
    typed_with(manager, Json)
}

/// A view of `manager` encoding with `codec`.
pub fn typed_with<T, C: Codec<T>>(manager: Arc<dyn Manager>, codec: C) -> Typed<T, C> {
    Typed {
        publisher: manager.clone(),
        subscriber: manager,
        codec,
        _model: PhantomData,
    }
}

impl<T: Send + 'static, C: Codec<T> + Clone + 'static> Typed<T, C> {
    /// Replaces the publish path, typically with a [`chain`](crate::chain) of
    /// middleware over the original.
    pub fn with_publisher(mut self, publisher: Arc<dyn Publisher>) -> Self {
        self.publisher = publisher;
        self
    }

    /// Replaces the subscribe path, typically with
    /// [`with_subscriber_logging`](crate::with_subscriber_logging).
    pub fn with_subscriber(mut self, subscriber: Arc<dyn Subscriber>) -> Self {
        self.subscriber = subscriber;
        self
    }

    /// Encodes `value` and publishes it on `subject`.
    pub async fn publish(&self, subject: &str, value: &T, opts: &Options) -> Result<String> {
        let payload = Payload::encode(&self.codec, value)?;
        self.publisher.publish(subject, payload, opts).await
    }

    /// Subscribes to `subject`, decoding each message into `T`.
    ///
    /// A message that does not decode is skipped with a warning rather than
    /// ending the subscription: one bad publisher should not stop delivery of
    /// everyone else's messages.
    pub async fn subscribe(&self, subject: &str, opts: &Options) -> Result<BoxStream<'static, T>> {
        let messages = self.subscriber.subscribe(subject, opts).await?;
        let codec = self.codec.clone();
        Ok(messages
            .filter_map(move |message| {
                let decoded = match message.decode(&codec) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        tracing::warn!(
                            subject = message.subject(),
                            id = message.id(),
                            %error,
                            "skipping a message that does not decode"
                        );
                        None
                    }
                };
                std::future::ready(decoded)
            })
            .boxed())
    }
}
