//! JetStream: the same subjects as core NATS, backed by a stored log.

use std::{collections::HashMap, collections::VecDeque, sync::Arc, time::Duration};

use async_nats::{
    Client, HeaderMap, Subject,
    header::NATS_MESSAGE_TTL,
    jetstream::{
        self, AckKind, ErrorCode,
        consumer::{
            AckPolicy, DeliverPolicy,
            pull::{self, MessagesErrorKind, OrderedConfig},
        },
        context::{CreateStreamErrorKind, GetStreamErrorKind},
        stream,
    },
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use tracing::{debug, error, info, warn};

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Headers, Manager, Options, Payload, Position,
    Positioned, Publisher, Registry, Result, Stream, StreamError, Streams, Subscriber,
    Subscription, provider,
};

use super::{
    LastSubject, NatsConfig,
    acks::{self, AckError, Acks, Credit, Pending},
    check_concrete, check_stream_headers, check_subject, from_header_map, outgoing,
    refuse_retain, to_header_map,
};

/// Metadata key for [`Stream::name`].
///
/// JetStream keeps a stream's name, subjects, and description of its own
/// accord, but has nowhere in its config for the rest of [`Stream`]. These keys
/// carry the remainder in the stream's metadata, where they survive a restart
/// with it. They are runtime-go's keys verbatim, so either runtime reads a
/// stream the other created.
pub const META_NAME: &str = "runtime-go.name";
/// Metadata key for [`Stream::user_id`]; see [`META_NAME`].
pub const META_USER: &str = "runtime-go.user_id";
/// Metadata key for [`Stream::active`], stored as `"true"`; see [`META_NAME`].
pub const META_ACTIVE: &str = "runtime-go.active";
/// Metadata key for [`Stream::headers`]: the headers as one JSON object, as
/// [`Stream`] serialises them (`{"lang":"rust"}`), and absent when there are
/// none, so a stream without headers has exactly the metadata runtime-go
/// writes. One key rather than one per header keeps the set whole: an update
/// replaces it, never leaves a removed header behind. See [`META_NAME`].
pub const META_HEADERS: &str = "runtime-go.headers";

/// A [`Streams`] backed by JetStream, which keeps a log and remembers what each
/// named consumer has handled.
///
/// The manager it binds implements [`Durable`] and [`Positioned`], so
/// [`as_durable`](crate::as_durable) succeeds on it and fails on
/// [`NatsProvider`](super::NatsProvider)'s.
///
/// Streams are real server-side objects here: a stream one process creates,
/// another binds, and a Go service created it just as well.
///
/// Every stream it creates or updates has `allow_msg_ttl` set, so
/// [`Options::expires`] works on it (NATS 2.11 or later). The server will not
/// turn that off again, so an update that leaves it unset, from an older
/// runtime-go for one, is refused by the server.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct JetStreamProvider {
    shared: Arc<Shared>,
}

/// The inactive threshold async-nats gives an ordered consumer, kept when
/// [`NatsConfig::inactive_threshold`] is unset.
const ORDERED_INACTIVE_THRESHOLD: Duration = Duration::from_secs(30);

/// What every manager bound from one provider needs.
struct Shared {
    context: jetstream::Context,
    client: Client,
    /// The PubAck routing for this connection, started by the first publish.
    acks: tokio::sync::OnceCell<Acks>,
    registry: Registry,
    metrics: provider::Metrics,
    /// Zero leaves it to the server.
    ack_wait: Duration,
    /// Zero leaves it to the server.
    max_deliver: i64,
    inactive_threshold: Duration,
}

impl JetStreamProvider {
    /// A JetStream provider over `client`.
    ///
    /// It asks the server for the account's JetStream status before returning,
    /// so a server without JetStream enabled is a misconfiguration heard at
    /// startup rather than at the first publish. It refuses
    /// [`NatsConfig::queue_group`]: sharing a subject is what a named durable
    /// consumer already does.
    pub async fn new(client: Client, config: NatsConfig) -> Result<Self> {
        if config.queue_group.is_some() {
            return Err(StreamError::Unsupported(
                "a queue group has no meaning on JetStream, where a named consumer already \
                 shares a subject; use Durable::consume"
                    .into(),
            ));
        }
        let context = jetstream::new(client.clone());
        context
            .query_account()
            .await
            .map_err(|error| StreamError::internal("nats: cannot reach JetStream", error))?;
        Ok(Self {
            shared: Arc::new(Shared {
                context,
                client,
                acks: tokio::sync::OnceCell::new(),
                metrics: config.metrics(),
                ack_wait: config.ack_wait.unwrap_or_default(),
                max_deliver: config.max_deliver.map(i64::from).unwrap_or_default(),
                inactive_threshold: config
                    .inactive_threshold
                    .unwrap_or(ORDERED_INACTIVE_THRESHOLD),
                registry: config.registry,
            }),
        })
    }

    /// Reads a stream's handle, mapping JetStream's not-found onto the
    /// contract's.
    async fn handle(&self, id: &str, doing: &str) -> Result<stream::Stream> {
        self.shared
            .context
            .get_stream(safe_name(id))
            .await
            .map_err(|error| match error.kind() {
                GetStreamErrorKind::JetStream(e)
                    if e.error_code() == ErrorCode::STREAM_NOT_FOUND =>
                {
                    not_found(id)
                }
                _ => StreamError::internal(&format!("nats: cannot {doing} stream {id}"), error),
            })
    }
}

fn not_found(id: &str) -> StreamError {
    StreamError::NotFound(format!("stream {id}"))
}

#[async_trait::async_trait]
impl Streams for JetStreamProvider {
    async fn create(&self, stream: Stream) -> Result<Stream> {
        check_stream_headers(&stream)?;
        let id = if stream.id.is_empty() {
            provider::new_stream_id(&stream.name)
        } else {
            stream.id.clone()
        };
        let id = safe_name(&id);
        debug!(id, name = %stream.name, subjects = ?stream.subjects, "creating stream");

        self.shared
            .context
            .create_stream(config_for(&id, &stream))
            .await
            .map_err(|error| {
                error!(id, %error, "could not create the stream");
                StreamError::internal(&format!("nats: cannot create stream {id}"), error)
            })?;

        info!(id, name = %stream.name, "stream created");
        Ok(stream_from(id, stream))
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        let handle = self.handle(id, "read").await?;
        Ok(to_stream(&handle.cached_info().config))
    }

    /// Binds a stream that exists on the server.
    ///
    /// The stream is read here so an unknown id fails at bind rather than at
    /// the first publish, and so the subject list is at hand to validate
    /// against without a round trip on every call.
    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let handle = self.handle(id, "bind").await?;
        let stream = to_stream(&handle.cached_info().config);
        debug!(id = %stream.id, subjects = ?stream.subjects, "bound to stream");
        Ok(Arc::new(JetStreamManager {
            shared: self.shared.clone(),
            handle,
            stream,
        }))
    }

    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        check_stream_headers(&stream)?;
        let name = safe_name(id);
        self.shared
            .context
            .update_stream(config_for(&name, &stream))
            .await
            .map_err(|error| match error.kind() {
                CreateStreamErrorKind::NotFound => not_found(id),
                _ => StreamError::internal(&format!("nats: cannot update stream {id}"), error),
            })?;
        info!(id = name, "stream updated");
        Ok(stream_from(name, stream))
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.shared
            .context
            .delete_stream(safe_name(id))
            .await
            .map_err(|error| match error.kind() {
                GetStreamErrorKind::JetStream(e)
                    if e.error_code() == ErrorCode::STREAM_NOT_FOUND =>
                {
                    not_found(id)
                }
                _ => StreamError::internal(&format!("nats: cannot delete stream {id}"), error),
            })?;
        info!(id, "stream deleted");
        Ok(())
    }

    /// Returns every stream on the server, including ones this runtime did not
    /// create; their name, owner, and active flag read empty.
    async fn list(&self) -> Result<Vec<Stream>> {
        let mut out: Vec<Stream> = self
            .shared
            .context
            .streams()
            .map_ok(|info| to_stream(&info.config))
            .try_collect()
            .await
            .map_err(|error| {
                error!(%error, "could not list streams");
                StreamError::internal("nats: cannot list streams", error)
            })?;
        out.sort_by(|a, b| a.id.cmp(&b.id));
        debug!(count = out.len(), "listed streams");
        Ok(out)
    }
}

/// The JetStream config for a declaration.
fn config_for(name: &str, stream: &Stream) -> stream::Config {
    let mut metadata = HashMap::from([
        (META_NAME.to_owned(), stream.name.clone()),
        (META_USER.to_owned(), stream.user_id.clone()),
        (META_ACTIVE.to_owned(), "true".to_owned()),
    ]);
    if !stream.headers.is_empty() {
        let json = serde_json::to_string(&stream.headers).expect("a string map serialises");
        metadata.insert(META_HEADERS.to_owned(), json);
    }
    stream::Config {
        name: name.to_owned(),
        description: (!stream.description.is_empty()).then(|| stream.description.clone()),
        subjects: stream.subjects.clone(),
        metadata,
        allow_message_ttl: true,
        ..stream::Config::default()
    }
}

/// The declaration as it now stands on the server.
fn stream_from(id: String, stream: Stream) -> Stream {
    Stream {
        id,
        active: true,
        ..stream
    }
}

/// Maps a JetStream config back to the contract's shape.
fn to_stream(config: &stream::Config) -> Stream {
    let meta = |key: &str| config.metadata.get(key).cloned().unwrap_or_default();
    Stream {
        id: config.name.clone(),
        name: meta(META_NAME),
        description: config.description.clone().unwrap_or_default(),
        subjects: config.subjects.clone(),
        user_id: meta(META_USER),
        active: meta(META_ACTIVE) == "true",
        headers: stored_headers(config),
    }
}

/// The stream headers kept under [`META_HEADERS`]. Ones another tool wrote
/// that do not parse are left off, with a warning, rather than making the
/// stream unreadable.
fn stored_headers(config: &stream::Config) -> Headers {
    let Some(json) = config.metadata.get(META_HEADERS) else {
        return Headers::new();
    };
    serde_json::from_str(json).unwrap_or_else(|error| {
        warn!(stream = config.name, %error, "ignoring unreadable stream headers");
        Headers::new()
    })
}

/// Makes an id usable as a JetStream stream or consumer name.
///
/// JetStream forbids whitespace and the characters that mean something in a
/// subject, and a generated id carries the caller's stream name for
/// readability, which may contain any of them. The replacement set is
/// runtime-go's, so both runtimes derive the same name from the same id.
pub fn safe_name(id: &str) -> String {
    id.chars()
        .map(|c| match c {
            '.' | '*' | '>' | '/' | '\\' | ' ' | '\t' | '\n' => '-',
            c => c,
        })
        .collect()
}

/// Publishes to and consumes from one JetStream stream.
///
/// It is a [`Manager`] with every optional capability: [`Durable`],
/// [`Positioned`], and [`Batch`].
pub struct JetStreamManager {
    shared: Arc<Shared>,
    handle: stream::Stream,
    stream: Stream,
}

impl JetStreamManager {
    /// The stream this manager is bound to, as it was when bound.
    pub fn stream(&self) -> &Stream {
        &self.stream
    }
}

#[async_trait::async_trait]
impl Publisher for JetStreamManager {
    /// Appends a payload to the stream and waits for the server to store it.
    ///
    /// The message id goes out as `Nats-Msg-Id`, so a publish retried after an
    /// ambiguous failure is collapsed by the server's duplicate window rather
    /// than appended twice. That is what lets [`with_retry`](crate::with_retry)
    /// retry safely: the backend recognises the repeat. Headers go out as
    /// NATS headers beside it, and [`Options::expires`] as `Nats-TTL`.
    /// [`Options::ttl`] is refused, because JetStream delivers when it is
    /// read, not on a timer, and [`Options::retain`], because its only
    /// last-value form would cap the stream at one message per subject.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let headers = self.check_publish(subject, opts)?;
        let id = provider::publish_id(opts);
        let target = Subject::from(subject);
        let acks = self.acks().await?;
        let mut credit = Credit::new(acks);
        credit.take(1).await;
        credit.spend();
        let pending = self
            .send(acks, &target, &id, headers.as_ref(), &payload)
            .await?;
        self.stored(subject, &id, pending).await?;
        Ok(id)
    }
}

/// How many publishes of one batch are awaiting their acks at once, as
/// nats.go's `PublishAsyncMaxPending` bounds them.
const BATCH_IN_FLIGHT: usize = 1024;

impl JetStreamManager {
    /// Refuses what no JetStream publish can do, before anything is sent,
    /// and returns the headers to send beside `Nats-Msg-Id`, `None` when
    /// there are none.
    fn check_publish(&self, subject: &str, opts: &Options) -> Result<Option<HeaderMap>> {
        check_subject(&self.stream, subject)?;
        check_concrete(&self.stream, subject)?;
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "JetStream delivers when it is read, not on a timer".into(),
            ));
        }
        refuse_retain(
            opts,
            "JetStream's last-per-subject form caps the stream at one message per subject, \
             which would discard the log durable consumers read",
        )?;
        let ttl = match opts.expiry() {
            Some(expiry) => {
                if !self.handle.cached_info().config.allow_message_ttl {
                    return Err(StreamError::Unsupported(format!(
                        "stream {} was created without allow_msg_ttl, so it cannot expire a \
                         message; update it through this provider to enable it",
                        self.stream.id
                    )));
                }
                Some(ttl_seconds(expiry)?)
            }
            None => None,
        };
        let headers = outgoing(&self.stream, opts)?;
        if headers.is_empty() && ttl.is_none() {
            return Ok(None);
        }
        let mut map = to_header_map(&headers);
        if let Some(ttl) = ttl {
            map.insert(NATS_MESSAGE_TTL, ttl.to_string());
        }
        Ok(Some(map))
    }

    /// The connection's PubAck routing, started on first use.
    async fn acks(&self) -> Result<&Acks> {
        self.shared
            .acks
            .get_or_try_init(|| Acks::start(self.shared.client.clone()))
            .await
            .map_err(|error| {
                error!(%error, "could not subscribe for PubAcks");
                StreamError::internal("nats: cannot subscribe for PubAcks", error)
            })
    }

    /// Sends one frame under `id`, returning once it has left for the server.
    /// What it returns resolves, through [`stored`](Self::stored), once the
    /// server has stored it.
    async fn send(
        &self,
        acks: &Acks,
        subject: &Subject,
        id: &str,
        headers: Option<&HeaderMap>,
        payload: &Payload,
    ) -> Result<Pending> {
        let body = provider::pack(id, payload);
        acks.send(subject.clone(), id, headers, body.into())
            .await
            .map_err(|error| publish_failed(subject.as_str(), id, &error))
    }

    /// Waits for the server to store a sent publish.
    async fn stored(&self, subject: &str, id: &str, pending: Pending) -> Result<()> {
        acks::stored(pending)
            .await
            .map_err(|error| publish_failed(subject, id, &error))?;
        debug!(subject, id, "published");
        Ok(())
    }
}

/// An expiry as `Nats-TTL` seconds. The server counts whole seconds and
/// refuses less than one, so a fraction rounds up (the message lives at most
/// that much longer, never shorter) and under a second is refused here, by
/// name, rather than by the server.
fn ttl_seconds(expiry: Duration) -> Result<u64> {
    if expiry < Duration::from_secs(1) {
        return Err(StreamError::Unsupported(format!(
            "JetStream expires messages in whole seconds, at least one; {expiry:?} is shorter"
        )));
    }
    Ok(expiry.as_secs() + u64::from(expiry.subsec_nanos() > 0))
}

fn publish_failed(subject: &str, id: &str, error: &AckError) -> StreamError {
    warn!(subject, id, %error, "could not publish");
    StreamError::internal(&format!("nats: cannot publish on {subject:?}"), error)
}

#[async_trait::async_trait]
impl Subscriber for JetStreamManager {
    /// Subscribes from the end of the log.
    ///
    /// This is the undurable half: an ordered consumer that acknowledges
    /// nothing and that the server discards once the stream has been dropped
    /// for [`NatsConfig::inactive_threshold`]. Nothing
    /// about what it saw survives it; reach for [`Durable::consume`] when that
    /// matters. [`Options::group`] is ignored, as in runtime-go: a group on
    /// JetStream is a durable consumer's name.
    async fn subscribe(&self, subject: &str, _opts: &Options) -> Result<Subscription> {
        check_subject(&self.stream, subject)?;
        let fail = |error: &dyn std::fmt::Display| {
            error!(subject, %error, "could not create an ordered consumer");
            StreamError::internal(&format!("nats: cannot subscribe to {subject:?}"), error)
        };
        // async-nats' ordered configuration has no inactive threshold, so the
        // first consumer is created with the same settings it would write,
        // plus that one, and then read back as an ordered consumer.
        let name = provider::new_id();
        self.handle
            .create_consumer(ordered_config(
                &name,
                subject,
                self.shared.inactive_threshold,
            ))
            .await
            .map_err(|error| fail(&error))?;
        let consumer = self
            .handle
            .get_consumer::<OrderedConfig>(&name)
            .await
            .map_err(|error| fail(&error))?;
        let messages = consumer.messages().await.map_err(|error| fail(&error))?;
        info!(subject, "subscribed");

        let registry = self.shared.registry.clone();
        let subject = subject.to_owned();
        let mut last = LastSubject::default();
        Ok(messages
            // The ordered consumer recreates itself across gaps and reconnects,
            // so an error reaching here is one it gave up on.
            .take_while(move |item| {
                let go_on = match item {
                    Ok(_) => true,
                    Err(error) => {
                        error!(subject, %error, "could not read the stream");
                        false
                    }
                };
                futures::future::ready(go_on)
            })
            .filter_map(move |item| {
                let decoded = item.ok().and_then(|message| {
                    provider::unpack_shared(
                        &registry,
                        last.of(message.subject.as_str()),
                        message.payload.clone(),
                    )
                    .map(|decoded| decoded.with_headers(from_header_map(message.headers.as_ref())))
                    .map_err(|error| {
                        warn!(subject = %message.subject, %error, "dropping a malformed message");
                    })
                    .ok()
                });
                futures::future::ready(decoded)
            })
            .boxed())
    }
}

#[async_trait::async_trait]
impl Durable for JetStreamManager {
    async fn consume(&self, subject: &str, consumer: &str, opts: &Options) -> Result<Deliveries> {
        self.consume_from(subject, consumer, Position::New, opts)
            .await
    }
}

#[async_trait::async_trait]
impl Positioned for JetStreamManager {
    /// Delivers under a JetStream durable consumer, starting at `at`.
    ///
    /// Two processes consuming under one name share its position and split the
    /// work, and a process that dies and comes back resumes where the name left
    /// off rather than where the process did.
    ///
    /// The position applies when the consumer is created and not after. One
    /// that already exists keeps the position it has: resetting it on every
    /// attach would replay the log every time a process restarted, the opposite
    /// of what a durable consumer is for. [`Options::prefetch`] bounds how many
    /// messages each pull asks for.
    async fn consume_from(
        &self,
        subject: &str,
        consumer: &str,
        at: Position,
        opts: &Options,
    ) -> Result<Deliveries> {
        check_subject(&self.stream, subject)?;
        if consumer.is_empty() {
            return Err(StreamError::Unsupported(
                "a durable consumer needs a name, because the name is the position that \
                 survives a restart"
                    .into(),
            ));
        }
        if opts.group.is_some() {
            return Err(StreamError::Unsupported(format!(
                "the consumer name {consumer:?} is already the group on JetStream; drop the \
                 group option"
            )));
        }

        let name = safe_name(consumer);
        let policy = match at {
            Position::New => DeliverPolicy::New,
            Position::Earliest => DeliverPolicy::All,
        };
        let fail = |error: &dyn std::fmt::Display| {
            error!(subject, consumer = name, %error, "could not attach the consumer");
            StreamError::internal(
                &format!("nats: cannot consume {subject:?} as {consumer:?}"),
                error,
            )
        };

        // Fetch before create, rather than create-or-update: an update would
        // try to move an existing consumer's delivery policy, which JetStream
        // refuses and which would be the wrong thing to ask for anyway.
        let pull = match self.handle.consumer_info(&name).await {
            Ok(_) => self
                .handle
                .get_consumer::<pull::Config>(&name)
                .await
                .map_err(|error| fail(&error))?,
            Err(error) if error.kind() == jetstream::context::ConsumerInfoErrorKind::NotFound => {
                self.handle
                    .create_consumer(pull::Config {
                        name: Some(name.clone()),
                        durable_name: Some(name.clone()),
                        filter_subject: subject.to_owned(),
                        ack_policy: AckPolicy::Explicit,
                        deliver_policy: policy,
                        ack_wait: self.shared.ack_wait,
                        max_deliver: self.shared.max_deliver,
                        ..pull::Config::default()
                    })
                    .await
                    .map_err(|error| fail(&error))?
            }
            Err(error) => return Err(fail(&error)),
        };

        let messages = pull
            .stream()
            .max_messages_per_batch(provider::prefetch(opts).max(1))
            .messages()
            .await
            .map_err(|error| fail(&error))?;
        info!(subject, consumer = name, from = ?policy, "consuming");

        Ok(deliveries(
            messages.boxed(),
            Lent {
                registry: self.shared.registry.clone(),
                metrics: self.shared.metrics.clone(),
                subject: subject.to_owned(),
                consumer: name,
            },
        ))
    }
}

/// The consumer behind a JetStream subscription: what async-nats writes for an
/// ordered consumer (ephemeral, unacknowledged, delivered once, in memory, one
/// replica), named, with `inactive_threshold` in place of its fixed 30 seconds.
fn ordered_config(name: &str, subject: &str, inactive_threshold: Duration) -> pull::Config {
    pull::Config {
        name: Some(name.to_owned()),
        filter_subjects: vec![subject.to_owned()],
        deliver_policy: DeliverPolicy::New,
        ack_policy: AckPolicy::None,
        max_deliver: 1,
        memory_storage: true,
        num_replicas: 1,
        inactive_threshold,
        ..pull::Config::default()
    }
}

/// What turning a raw JetStream message into a [`Delivery`] needs.
#[derive(Clone)]
struct Lent {
    registry: Registry,
    metrics: provider::Metrics,
    subject: String,
    consumer: String,
}

type Raw = BoxStream<'static, std::result::Result<jetstream::Message, pull::MessagesError>>;

/// Turns a pull consumer's messages into deliveries.
///
/// The pull stream is owned by the returned stream, so dropping it stops the
/// pulls; the durable consumer stays on the server with its position, and what
/// was in flight is redelivered once its ack wait runs out.
fn deliveries(raw: Raw, lent: Lent) -> Deliveries {
    let state = (raw, lent, LastSubject::default());
    futures::stream::unfold(state, |(mut raw, lent, mut last)| async move {
        loop {
            let message = match raw.next().await? {
                Ok(message) => message,
                // A missed heartbeat is a hiccup the pull stream recovers from
                // by pulling again; it is not the consumer's end.
                Err(error) if error.kind() == MessagesErrorKind::MissingHeartbeat => {
                    warn!(subject = lent.subject, consumer = lent.consumer, %error, "missed a heartbeat");
                    continue;
                }
                Err(error) => {
                    error!(subject = lent.subject, consumer = lent.consumer, %error, "could not read as the consumer");
                    return None;
                }
            };

            let decoded = provider::unpack_shared(
                &lent.registry,
                last.of(message.subject.as_str()),
                message.payload.clone(),
            );
            let decoded = match decoded {
                Ok(decoded) => decoded.with_headers(from_header_map(message.headers.as_ref())),
                Err(error) => {
                    // Nothing will ever decode this, so no handler will ever
                    // ack it and it would redeliver forever. Terminate it:
                    // unlike an ack, that records the message as undeliverable
                    // rather than handled.
                    warn!(subject = %message.subject, %error, "terminating a malformed message");
                    let _ = message.ack_with(AckKind::Term).await;
                    continue;
                }
            };

            let attempt = message
                .info()
                .ok()
                .and_then(|info| u32::try_from(info.delivered).ok())
                .filter(|&n| n > 0)
                .unwrap_or(1);
            lent.metrics.delivered(&lent.subject, &lent.consumer);
            let acker = JetStreamAck {
                message,
                metrics: lent.metrics.clone(),
                subject: lent.subject.clone(),
                consumer: lent.consumer.clone(),
            };
            let delivery = Delivery::new(decoded, attempt, Box::new(acker));
            return Some((delivery, (raw, lent, last)));
        }
    })
    .boxed()
}

/// Settles one JetStream delivery. JetStream has a true negative
/// acknowledgement, so a nak returns the message at once rather than waiting
/// out the ack timeout, and a delayed one has the server hold the redelivery
/// back.
struct JetStreamAck {
    message: jetstream::Message,
    metrics: provider::Metrics,
    subject: String,
    consumer: String,
}

impl JetStreamAck {
    async fn settle(self, kind: AckKind, outcome: &'static str) -> Result<()> {
        self.message
            .ack_with(kind)
            .await
            .map_err(|error| StreamError::internal(&format!("nats: cannot {outcome}"), error))?;
        self.metrics.settled(&self.subject, &self.consumer, outcome);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Acker for JetStreamAck {
    async fn ack(self: Box<Self>) -> Result<()> {
        self.settle(AckKind::Ack, "ack").await
    }

    async fn nak(self: Box<Self>) -> Result<()> {
        self.settle(AckKind::Nak(None), "nak").await
    }

    /// A `-NAK` carrying the delay: the server redelivers no sooner than
    /// `delay` from when it reads it. A zero delay is a plain nak.
    async fn nak_after(self: Box<Self>, delay: Duration) -> Result<()> {
        let delay = (!delay.is_zero()).then_some(delay);
        self.settle(AckKind::Nak(delay), "nak").await
    }

    fn delays_nak(&self) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl Batch for JetStreamManager {
    /// Sends every payload without waiting, then collects the acks, as
    /// runtime-go does: a batch costs about one round trip rather than one per
    /// message.
    ///
    /// Frames are sent one after another, in order, so the stream stores them
    /// in order; only the waits for their acks overlap. Up to
    /// `BATCH_IN_FLIGHT` wait at once, within the connection's own limit of
    /// `MAX_PENDING` across every publisher. While that limit is reached, the
    /// batch settles its own oldest waits before it waits on anyone else, so
    /// it never holds room it is itself waiting for. Each message carries its
    /// own id as `Nats-Msg-Id`, beside the same headers and expiry as every
    /// other message of the batch, so a retried batch is collapsed by the
    /// duplicate window just as a retried single publish is. A message that
    /// fails is reported by index in [`StreamError::PartialBatch`]; the others
    /// stand.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        provider::check_batch(opts)?;
        let headers = self.check_publish(subject, opts)?;
        let target = Subject::from(subject);
        let acks = self.acks().await?;
        let mut credit = Credit::new(acks);
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        let mut outcomes: Vec<Result<()>> = Vec::with_capacity(ids.len());
        let mut waiting: VecDeque<(usize, Pending)> = VecDeque::with_capacity(BATCH_IN_FLIGHT);
        for (index, (payload, id)) in payloads.iter().zip(&ids).enumerate() {
            if waiting.len() >= BATCH_IN_FLIGHT {
                let (done, pending) = waiting.pop_front().expect("a wait");
                outcomes.push(self.stored(subject, &ids[done], pending).await);
                credit.finish();
            }
            // Room under the connection's in-flight limit: take it without
            // waiting if there is some, else settle this batch's own oldest
            // wait, and only wait on other publishers when nothing of ours
            // is outstanding.
            let left = payloads.len() - index;
            while !credit.has_spare() && !credit.try_take(left) {
                match waiting.pop_front() {
                    Some((done, pending)) => {
                        outcomes.push(self.stored(subject, &ids[done], pending).await);
                        credit.finish();
                    }
                    None => credit.take(left).await,
                }
            }
            credit.spend();
            match self
                .send(acks, &target, id, headers.as_ref(), payload)
                .await
            {
                Ok(pending) => waiting.push_back((index, pending)),
                Err(error) => {
                    credit.finish();
                    // Settle everything sent before it first, so outcomes
                    // stay in index order.
                    while let Some((done, pending)) = waiting.pop_front() {
                        outcomes.push(self.stored(subject, &ids[done], pending).await);
                        credit.finish();
                    }
                    outcomes.push(Err(error));
                }
            }
        }
        while let Some((done, pending)) = waiting.pop_front() {
            outcomes.push(self.stored(subject, &ids[done], pending).await);
            credit.finish();
        }
        let failures = outcomes
            .into_iter()
            .enumerate()
            .filter_map(|(index, outcome)| outcome.err().map(|error| (index, error)))
            .collect();
        provider::batch_result(subject, ids, failures)
    }
}

impl Manager for JetStreamManager {
    fn durable(&self) -> Option<&dyn Durable> {
        Some(self)
    }

    fn positioned(&self) -> Option<&dyn Positioned> {
        Some(self)
    }

    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_name_matches_go() {
        assert_eq!(
            safe_name("01J:orders.us/eu\\x y\tz\n*>"),
            "01J:orders-us-eu-x-y-z---"
        );
        assert_eq!(safe_name("plain-id_1"), "plain-id_1");
    }

    #[test]
    fn metadata_round_trips() {
        let stream = Stream {
            id: "ignored".into(),
            name: "orders".into(),
            description: "placed and shipped".into(),
            subjects: vec!["orders.*".into()],
            user_id: "ada".into(),
            active: false,
            ..Stream::default()
        };
        let config = config_for("S-1", &stream);
        assert_eq!(config.name, "S-1");
        assert_eq!(config.metadata["runtime-go.name"], "orders");
        assert_eq!(config.metadata["runtime-go.user_id"], "ada");
        assert_eq!(config.metadata["runtime-go.active"], "true");

        let back = to_stream(&config);
        assert_eq!(back, stream_from("S-1".into(), stream));
    }

    #[test]
    fn stream_headers_live_in_one_metadata_key() {
        let plain = config_for("S", &Stream::named("orders"));
        assert!(!plain.metadata.contains_key(META_HEADERS), "Go's metadata, unchanged");
        assert!(plain.allow_message_ttl);
        assert!(to_stream(&plain).headers.is_empty());

        let stream = Stream::named("orders")
            .subjects(["orders.*"])
            .headers([("lang", "rust"), ("site", "plant-3")]);
        let config = config_for("S", &stream);
        assert_eq!(
            config.metadata[META_HEADERS],
            r#"{"lang":"rust","site":"plant-3"}"#
        );
        assert_eq!(to_stream(&config).headers, stream.headers);

        let mut garbled = config;
        garbled
            .metadata
            .insert(META_HEADERS.to_owned(), "not json".to_owned());
        assert!(to_stream(&garbled).headers.is_empty());
    }

    #[test]
    fn expiry_is_whole_seconds_rounded_up() {
        assert_eq!(ttl_seconds(Duration::from_secs(5)).unwrap(), 5);
        assert_eq!(ttl_seconds(Duration::from_millis(1500)).unwrap(), 2);
        assert_eq!(ttl_seconds(Duration::from_secs(1)).unwrap(), 1);
        let error = ttl_seconds(Duration::from_millis(999)).unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    }

    #[test]
    fn a_foreign_stream_reads_inactive_and_unnamed() {
        let config = stream::Config {
            name: "FOREIGN".into(),
            subjects: vec!["x".into()],
            ..stream::Config::default()
        };
        let stream = to_stream(&config);
        assert_eq!(stream.id, "FOREIGN");
        assert!(stream.name.is_empty() && stream.description.is_empty());
        assert!(!stream.active);
    }

    #[test]
    fn the_ordered_config_matches_async_nats() {
        let ours = ordered_config("c", "orders.*", ORDERED_INACTIVE_THRESHOLD);
        let theirs: pull::Config = OrderedConfig {
            name: Some("c".into()),
            filter_subjects: vec!["orders.*".into()],
            deliver_policy: DeliverPolicy::New,
            ..OrderedConfig::default()
        }
        .into();
        assert_eq!(ours, theirs);
        let short = ordered_config("c", "orders.*", Duration::from_secs(2));
        assert_eq!(short.inactive_threshold, Duration::from_secs(2));
    }

    #[test]
    fn an_empty_description_is_omitted() {
        assert_eq!(config_for("S", &Stream::default()).description, None);
    }

    #[tokio::test]
    async fn refuses_a_queue_group_before_dialing() {
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("127.0.0.1:1")
            .await
            .unwrap();
        let config = NatsConfig::default().with_queue_group("workers");
        let error = JetStreamProvider::new(client, config).await.err().unwrap();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    }
}
