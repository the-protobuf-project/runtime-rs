//! The manager bound to one ZeroMQ stream.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use zeromq::{Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

use super::{Role, Shared};
use crate::{
    Batch, Headers, Manager, Message, Options, Payload, Publisher, Registry, Result, Stream,
    StreamError, Subscriber, Subscription, provider,
};

/// Publishes to and subscribes from one declared stream.
pub(crate) struct ZmqManager {
    shared: Arc<Shared>,
    stream: Stream,
}

impl ZmqManager {
    pub(crate) fn new(shared: Arc<Shared>, stream: Stream) -> Self {
        Self { shared, stream }
    }

    /// Rejects a subject the stream does not declare.
    ///
    /// A ZeroMQ subscription is a byte prefix rather than a pattern, so a
    /// subject is matched by name, as on Redis and Kafka.
    fn check_subject(&self, subject: &str) -> Result<()> {
        if provider::declares(&self.stream.subjects, subject) {
            return Ok(());
        }
        tracing::error!(
            subject,
            stream = self.stream.id,
            declared = ?self.stream.subjects,
            "zeromq: subject is not declared by this stream"
        );
        Err(provider::unknown_subject(
            &self.stream.id,
            subject,
            &self.stream.subjects,
        ))
    }

    /// The first frame of every message on `subject`, runtime-go's layout.
    ///
    /// The stream id is in it so two streams on one socket stay apart, which is
    /// why both peers must declare the stream under the same id.
    fn topic(&self, subject: &str) -> String {
        format!("{}|{subject}", self.stream.id)
    }

    fn closed() -> StreamError {
        StreamError::Internal("zeromq: the provider is closed".into())
    }
}

impl ZmqManager {
    /// Checks everything a publish or batch on `subject` with `opts` depends
    /// on, and returns the publishing socket and the headers to send.
    fn prepare(&self, subject: &str, opts: &Options) -> Result<(&PubSlot, Headers)> {
        self.check_subject(subject)?;
        let Role::Publisher(socket) = &self.shared.role else {
            return Err(StreamError::Unsupported(
                "this ZeroMQ provider was built with ZmqProvider::subscriber and can only \
                 receive; use ZmqProvider::publisher to bind an endpoint"
                    .into(),
            ));
        };
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "ZeroMQ delivers on send and cannot schedule".into(),
            ));
        }
        if opts.group.is_some() {
            // Sharing a subject would need a PUSH/PULL pair, a different
            // topology rather than an option on this one.
            return Err(StreamError::Unsupported(
                "ZeroMQ PUB/SUB fans out to every subscriber and cannot share a subject".into(),
            ));
        }
        if let Some(expires) = opts.expiry() {
            return Err(StreamError::Unsupported(format!(
                "ZeroMQ has no queue: a message reaches the subscribers connected when it is \
                 sent or nobody, so there is nothing to expire ({expires:?})"
            )));
        }
        if opts.retain {
            return Err(StreamError::Unsupported(
                "ZeroMQ keeps nothing once a message is sent, so it cannot retain a last value \
                 for later subscribers"
                    .into(),
            ));
        }
        Ok((socket, provider::outgoing(&self.stream, opts)?))
    }

    /// Sends one payload as a topic frame and a packed frame: version 1, as
    /// runtime-go writes, unless there are headers to carry.
    async fn send(
        &self,
        socket: &PubSlot,
        subject: &str,
        id: &str,
        payload: &Payload,
        headers: &Headers,
    ) -> Result<()> {
        let body = provider::pack_with(id, payload, headers);
        let len = body.len();
        let frames = vec![Bytes::from(self.topic(subject)), Bytes::from(body)];
        let message = ZmqMessage::try_from(frames)
            .map_err(|error| StreamError::internal("zeromq: cannot frame message", error))?;

        let mut socket = socket.lock().await;
        let socket = socket.as_mut().ok_or_else(Self::closed)?;
        if let Err(error) = socket.send(message).await {
            tracing::error!(subject, id, %error, "zeromq: could not publish");
            return Err(StreamError::internal(
                &format!("zeromq: cannot publish on {subject:?}"),
                error,
            ));
        }
        tracing::debug!(subject, id, bytes = len, "zeromq: published");
        Ok(())
    }
}

/// The publisher's socket, taken on close.
type PubSlot = tokio::sync::Mutex<Option<zeromq::PubSocket>>;

#[async_trait::async_trait]
impl Publisher for ZmqManager {
    /// Sends a payload as a topic frame and a packed frame, with the headers
    /// in the frame (version 2) when there are any.
    ///
    /// Fails with [`StreamError::Unsupported`] on a provider built as a
    /// subscriber, with a TTL (ZeroMQ delivers on send and cannot schedule),
    /// with a group (PUB/SUB fans out to every subscriber), and with an expiry
    /// or retain (there is no queue to hold a message in).
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let (socket, headers) = self.prepare(subject, opts)?;
        let id = provider::publish_id(opts);
        self.send(socket, subject, &id, &payload, &headers).await?;
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Subscriber for ZmqManager {
    /// Opens a SUB socket for `subject` and waits [`ZmqConfig::settle`] before
    /// returning.
    ///
    /// A payload published in the moment after this returns may still be
    /// missed; see the module documentation on the slow-joiner problem. A group
    /// is refused for the same reason it is on publish.
    ///
    /// [`ZmqConfig::settle`]: super::ZmqConfig::settle
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        self.check_subject(subject)?;
        if opts.group.is_some() {
            return Err(StreamError::Unsupported(
                "ZeroMQ PUB/SUB fans out to every subscriber and cannot share a subject".into(),
            ));
        }
        if self.shared.is_closed() {
            return Err(Self::closed());
        }

        let endpoint = &self.shared.endpoint;
        let mut socket = SubSocket::new();
        socket.connect(endpoint).await.map_err(|error| {
            StreamError::internal(&format!("zeromq: cannot connect to {endpoint}"), error)
        })?;
        let topic = self.topic(subject);
        socket.subscribe(&topic).await.map_err(|error| {
            StreamError::internal(&format!("zeromq: cannot subscribe to {subject:?}"), error)
        })?;

        // The subscription is on its way to the publisher and nothing will
        // confirm it arrived. Waiting narrows the window.
        let mut closed = self.shared.closed.subscribe();
        tokio::select! {
            () = tokio::time::sleep(self.shared.settle) => {}
            _ = closed.wait_for(|closed| *closed) => return Err(Self::closed()),
        }
        tracing::info!(subject, endpoint, topic, "zeromq: subscribed");

        let (tx, rx) = mpsc::channel(provider::prefetch(opts).max(1));
        let receive = Receive {
            socket,
            topic: Bytes::from(topic),
            subject: subject.to_owned(),
            registry: self.shared.registry.clone(),
            closed,
            tx,
        };
        tokio::spawn(receive.run());
        Ok(ReceiverStream::new(rx).boxed())
    }
}

#[async_trait::async_trait]
impl Batch for ZmqManager {
    /// Publishes each payload in turn, after checking the options and
    /// headers once. ZeroMQ sends without waiting for a subscriber, so this
    /// does not block per message.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        provider::check_batch(opts)?;
        let (socket, headers) = self.prepare(subject, opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        let mut failures = Vec::new();
        for (index, (payload, id)) in payloads.iter().zip(&ids).enumerate() {
            if let Err(error) = self.send(socket, subject, id, payload, &headers).await {
                failures.push((index, error));
            }
        }
        provider::batch_result(subject, ids, failures)
    }
}

impl Manager for ZmqManager {
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

/// One subscription's receive loop, which owns its SUB socket.
///
/// It ends when the subscription is dropped or the provider is closed, and
/// drops the socket on the way out; that disconnect is what tells the
/// publisher to stop sending.
struct Receive {
    socket: SubSocket,
    topic: Bytes,
    subject: String,
    registry: Registry,
    closed: watch::Receiver<bool>,
    tx: mpsc::Sender<Message>,
}

impl Receive {
    async fn run(mut self) {
        let subject = self.subject.clone();
        let mut delivered = 0usize;
        while let Some(message) = self.next().await {
            delivered += 1;
            tokio::select! {
                sent = self.tx.send(message) => if sent.is_err() { break },
                _ = self.closed.wait_for(|closed| *closed) => break,
            }
        }
        tracing::info!(subject, delivered, "zeromq: subscription closed");
    }

    /// Waits for the next message on this subject, or `None` once the
    /// subscription should end.
    async fn next(&mut self) -> Option<Message> {
        loop {
            let received = tokio::select! {
                () = self.tx.closed() => return None,
                _ = self.closed.wait_for(|closed| *closed) => return None,
                received = self.socket.recv() => received,
            };
            let message = match received {
                Ok(message) => message,
                Err(error) => {
                    // The socket reconnects on its own, so a dropped peer is a
                    // pause rather than the end of the subscription.
                    tracing::debug!(subject = self.subject, %error, "zeromq: receive failed");
                    continue;
                }
            };
            let (Some(topic), Some(frame)) = (message.get(0), message.get(1)) else {
                tracing::warn!(
                    subject = self.subject,
                    frames = message.len(),
                    "zeromq: dropping a message with no payload frame"
                );
                continue;
            };
            // SUB filtering is by prefix, so subscribing to "s|user" also
            // receives "s|users". Comparing the whole frame keeps the subject
            // exact without changing what goes on the wire.
            if *topic != self.topic {
                continue;
            }
            match provider::unpack(&self.registry, &self.subject, frame.clone()) {
                Ok(message) => return Some(message),
                // One bad message is not a reason to tear down a healthy
                // subscription.
                Err(error) => tracing::warn!(
                    subject = self.subject,
                    %error,
                    "zeromq: dropping a malformed message"
                ),
            }
        }
    }
}
