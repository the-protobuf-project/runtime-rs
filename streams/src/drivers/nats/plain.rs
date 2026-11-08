//! Core NATS: publish, subscribe, and nothing kept.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use async_nats::Client;
use futures::StreamExt;
use tracing::{debug, info, warn};

use crate::{
    Batch, Manager, Options, Payload, Publisher, Registry, Result, Stream, StreamError, Streams,
    Subscriber, Subscription, provider,
};

use super::{NatsConfig, check_concrete, check_subject};

/// A [`Streams`] backed by core NATS, delivering to whoever is listening at the
/// time.
///
/// Nothing is stored: a subscriber that is not attached when a message is
/// published never sees it, and one that dies holding a message loses it.
/// Reach for [`JetStreamProvider`](super::JetStreamProvider) when either
/// matters.
///
/// A stream declared here is a declaration this process holds, not a
/// server-side object, because core NATS has no registry to keep it in. Two
/// processes each declare their own, and a restart forgets. What it buys is the
/// subject check: publishing to a subject the stream never declared fails at
/// the call that made the typo. Inventing a registry subject instead would
/// pretend this provider has storage it does not.
///
/// Cloning is cheap and clones share the declarations.
#[derive(Clone)]
pub struct NatsProvider {
    shared: Arc<Shared>,
}

/// What every manager bound from one provider needs.
struct Shared {
    client: Client,
    registry: Registry,
    queue_group: Option<String>,
    declared: RwLock<HashMap<String, Stream>>,
}

impl NatsProvider {
    /// A core NATS provider over `client`.
    ///
    /// [`NatsConfig::meter`] is accepted and unused: core NATS has no durable
    /// deliveries to count.
    pub fn new(client: Client, config: NatsConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                client,
                registry: config.registry,
                queue_group: config.queue_group,
                declared: RwLock::new(HashMap::new()),
            }),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<String, Stream>> {
        // A poisoned lock means a panic mid-insert of a plain map value, which
        // cannot leave it half-written; carrying on is safe.
        self.shared
            .declared
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<String, Stream>> {
        self.shared
            .declared
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The declaration as it is stored: always active, since a declared core
/// stream has nothing that could make it otherwise.
fn declared(id: String, stream: Stream) -> Stream {
    Stream {
        id,
        active: true,
        ..stream
    }
}

fn not_found(id: &str) -> StreamError {
    StreamError::NotFound(format!("stream {id}"))
}

#[async_trait::async_trait]
impl Streams for NatsProvider {
    async fn create(&self, stream: Stream) -> Result<Stream> {
        let id = if stream.id.is_empty() {
            provider::new_stream_id(&stream.name)
        } else {
            stream.id.clone()
        };
        let out = declared(id.clone(), stream);
        self.write().insert(id.clone(), out.clone());
        info!(id, name = %out.name, subjects = ?out.subjects, "stream declared");
        Ok(out)
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        self.read().get(id).cloned().ok_or_else(|| not_found(id))
    }

    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.get(id).await?;
        Ok(Arc::new(NatsManager {
            shared: self.shared.clone(),
            stream,
        }))
    }

    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        let mut declared_streams = self.write();
        let slot = declared_streams.get_mut(id).ok_or_else(|| not_found(id))?;
        *slot = declared(id.to_owned(), stream);
        Ok(slot.clone())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.write()
            .remove(id)
            .map(drop)
            .ok_or_else(|| not_found(id))
    }

    async fn list(&self) -> Result<Vec<Stream>> {
        let mut out: Vec<Stream> = self.read().values().cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }
}

/// Publishes to and subscribes from one stream over core NATS.
///
/// It is a [`Manager`] with [`Batch`] and nothing else: [`as_durable`] and
/// [`as_positioned`] refuse it by name.
///
/// [`as_durable`]: crate::as_durable
/// [`as_positioned`]: crate::as_positioned
pub struct NatsManager {
    shared: Arc<Shared>,
    stream: Stream,
}

impl NatsManager {
    /// The stream this manager is bound to, as it was when bound.
    pub fn stream(&self) -> &Stream {
        &self.stream
    }
}

#[async_trait::async_trait]
impl Publisher for NatsManager {
    /// Sends a payload on a subject.
    ///
    /// It returns once the client has queued the frame, not when the server has
    /// it: core NATS has no acknowledgement to wait for. [`Options::ttl`] is
    /// refused, because core NATS delivers immediately and cannot schedule.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        check_subject(&self.stream, subject)?;
        check_concrete(&self.stream, subject)?;
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "core NATS delivers immediately and cannot schedule".into(),
            ));
        }

        let id = provider::publish_id(opts);
        let body = provider::pack(&id, &payload);
        let bytes = body.len();
        self.shared
            .client
            .publish(subject.to_owned(), body.into())
            .await
            .map_err(|error| {
                warn!(subject, id, %error, "could not publish");
                StreamError::internal(&format!("nats: cannot publish on {subject:?}"), error)
            })?;

        debug!(subject, id, bytes, "published");
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Subscriber for NatsManager {
    /// Subscribes to a subject, joining a queue group when one is named.
    ///
    /// [`Options::group`] wins over [`NatsConfig::queue_group`].
    /// [`Options::prefetch`] is ignored: `async-nats` buffers each subscription
    /// in its own channel sized by the client's `subscription_capacity`, and a
    /// slow reader there costs only its own messages, never the connection's
    /// dispatch loop, so there is no per-call buffer left to size.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        check_subject(&self.stream, subject)?;

        let queue = opts
            .group
            .clone()
            .or_else(|| self.shared.queue_group.clone());
        let client = &self.shared.client;
        let subscribed = match &queue {
            Some(group) => {
                client
                    .queue_subscribe(subject.to_owned(), group.clone())
                    .await
            }
            None => client.subscribe(subject.to_owned()).await,
        };
        let raw = subscribed.map_err(|error| {
            StreamError::internal(&format!("nats: cannot subscribe to {subject:?}"), error)
        })?;

        // The subscription is registered locally the moment subscribe returns,
        // but the server does not know about it until the next flush. Without
        // this, a payload published right afterwards races the interest reaching
        // the server and is silently dropped. On failure `raw` drops here, which
        // unsubscribes.
        client.flush().await.map_err(|error| {
            StreamError::internal(
                &format!("nats: cannot establish the subscription to {subject:?}"),
                error,
            )
        })?;

        info!(subject, queue = ?queue, "subscribed");

        // No task sits between the client and the caller: the stream is the
        // client's subscriber, so dropping it is the unsubscribe.
        let registry = self.shared.registry.clone();
        Ok(raw
            .filter_map(move |message| {
                let decoded = provider::unpack(
                    &registry,
                    message.subject.as_str(),
                    message.payload,
                )
                .map_err(|error| {
                    // One bad message is not a reason to tear down a
                    // healthy subscription.
                    warn!(subject = %message.subject, %error, "dropping a malformed message");
                })
                .ok();
                futures::future::ready(decoded)
            })
            .boxed())
    }
}

#[async_trait::async_trait]
impl Batch for NatsManager {
    /// Publishes each payload in turn. Core NATS publishes without waiting for
    /// the server, so that is already as batched as this gets.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        provider::publish_each(self, subject, payloads, opts).await
    }
}

impl Manager for NatsManager {
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{as_batch, as_durable, as_positioned};

    /// A client that connects in the background to a port nothing listens on.
    ///
    /// Everything tested here fails or succeeds before a byte would reach a
    /// server, so none is needed.
    async fn offline() -> Client {
        async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("127.0.0.1:1")
            .await
            .expect("a retrying connect returns at once")
    }

    async fn provider() -> NatsProvider {
        NatsProvider::new(offline().await, NatsConfig::default())
    }

    fn json() -> Payload {
        Payload::json(&"x".to_owned()).unwrap()
    }

    #[tokio::test]
    async fn lifecycle() {
        let p = provider().await;
        let created = p
            .create(Stream {
                name: "users".into(),
                description: "before".into(),
                subjects: vec!["user.created".into()],
                ..Stream::default()
            })
            .await
            .unwrap();
        assert!(created.id.ends_with(":users"), "{}", created.id);
        assert!(created.active);

        let updated = p
            .update(
                &created.id,
                Stream {
                    name: "users".into(),
                    description: "after".into(),
                    subjects: vec!["user.created".into()],
                    ..Stream::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.id, created.id);
        assert_eq!(p.get(&created.id).await.unwrap().description, "after");
        assert_eq!(p.list().await.unwrap().len(), 1);

        p.delete(&created.id).await.unwrap();
        for result in [
            p.get(&created.id).await.map(drop),
            p.delete(&created.id).await,
            p.update(&created.id, Stream::default()).await.map(drop),
            p.bind(&created.id).await.map(drop),
        ] {
            assert!(
                matches!(result, Err(StreamError::NotFound(_))),
                "{result:?}"
            );
        }
    }

    #[tokio::test]
    async fn list_is_sorted_and_keeps_given_ids() {
        let p = provider().await;
        for id in ["b", "a", "c"] {
            p.create(Stream {
                id: id.into(),
                ..Stream::default()
            })
            .await
            .unwrap();
        }
        let ids: Vec<String> = p.list().await.unwrap().into_iter().map(|s| s.id).collect();
        assert_eq!(ids, ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn publish_refuses_before_sending() {
        let p = provider().await;
        let s = p
            .create(Stream {
                id: "s".into(),
                subjects: vec!["orders.*".into()],
                ..Stream::default()
            })
            .await
            .unwrap();
        let m = p.bind(&s.id).await.unwrap();

        let typo = m.publish("typo", json(), &Options::default()).await;
        assert!(
            matches!(typo, Err(StreamError::UnknownSubject { .. })),
            "{typo:?}"
        );

        let wildcard = m.publish("orders.*", json(), &Options::default()).await;
        assert!(
            matches!(wildcard, Err(StreamError::UnknownSubject { .. })),
            "{wildcard:?}"
        );

        let ttl = Options::default().with_ttl(Duration::from_secs(1));
        let delayed = m.publish("orders.placed", json(), &ttl).await;
        assert!(
            matches!(delayed, Err(StreamError::Unsupported(_))),
            "{delayed:?}"
        );

        // A zero TTL is "no TTL", as in Go.
        let zero = Options::default().with_ttl(Duration::ZERO);
        assert!(m.publish("orders.placed", json(), &zero).await.is_ok());

        let subscribe = m.subscribe("typo", &Options::default()).await.map(drop);
        assert!(
            matches!(subscribe, Err(StreamError::UnknownSubject { .. })),
            "{subscribe:?}"
        );
    }

    #[tokio::test]
    async fn publish_honors_a_chosen_id() {
        let p = provider().await;
        p.create(Stream {
            id: "s".into(),
            subjects: vec!["a".into()],
            ..Stream::default()
        })
        .await
        .unwrap();
        let m = p.bind("s").await.unwrap();
        let id = m
            .publish("a", json(), &Options::default().with_id("mine"))
            .await
            .unwrap();
        assert_eq!(id, "mine");
    }

    #[tokio::test]
    async fn capabilities_are_batch_only() {
        let p = provider().await;
        p.create(Stream {
            id: "s".into(),
            subjects: vec!["a".into()],
            ..Stream::default()
        })
        .await
        .unwrap();
        let m = p.bind("s").await.unwrap();
        assert!(matches!(
            as_durable(m.as_ref()),
            Err(StreamError::Unsupported(_))
        ));
        assert!(matches!(
            as_positioned(m.as_ref()),
            Err(StreamError::Unsupported(_))
        ));

        let batch = as_batch(m.as_ref()).unwrap();
        let ids = batch
            .publish_batch("a", vec![json(), json()], &Options::default())
            .await
            .unwrap();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);

        let refused = batch
            .publish_batch("a", vec![json()], &Options::default().with_id("one"))
            .await;
        assert!(
            matches!(refused, Err(StreamError::Unsupported(_))),
            "{refused:?}"
        );
    }
}
