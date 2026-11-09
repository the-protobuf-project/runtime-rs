//! The publishing connections: unframed publishes that return once the
//! broker has acknowledged them.
//!
//! Connections are opened with the MQTT driver's own handshake
//! ([`dial`](crate::drivers::mqtt)), so options, TLS and credentials behave
//! exactly as there. What is repeated here is the MQTT driver's pairing of a
//! publish with its PUBACK, because that driver's publishing connection
//! always writes runtime-go's frame and cannot carry a Part 14 message as it
//! is, nor set the retain flag a metadata announcement needs.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use rumqttc::{
    Outgoing,
    v5::{
        AsyncClient, ConnectionError, Event, EventLoop, Incoming,
        mqttbytes::{
            QoS,
            v5::{PubAckReason, PubRecReason, PublishProperties},
        },
    },
};
use tokio::{sync::oneshot, task::JoinHandle, time::Instant};

use crate::{
    Result, StreamError,
    drivers::mqtt::{MqttOptions, Session, dial},
};

/// How long a closing connection is given to put its DISCONNECT on the wire.
pub(super) const LINGER: Duration = Duration::from_secs(1);

/// How long a lost publishing connection waits before redialing.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Part 14 JSON messages are JSON (7.3.5.2): the MQTT 5 content type says so.
pub(super) const CONTENT_TYPE: &str = "application/json";

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

type Waiter = oneshot::Sender<Result<()>>;

/// Publishes handed to the connection and not yet settled: a FIFO until the
/// event loop names their packet id, then by packet id until the PUBACK.
#[derive(Default)]
struct Waiters {
    queued: VecDeque<Waiter>,
    inflight: HashMap<u16, Waiter>,
}

impl Waiters {
    fn fail_all(&mut self, error: &StreamError) {
        for waiter in self
            .queued
            .drain(..)
            .chain(self.inflight.drain().map(|(_, w)| w))
        {
            let _ = waiter.send(Err(error.clone()));
        }
    }

    fn settle(&mut self, pkid: u16, outcome: Result<()>) {
        if let Some(waiter) = self.inflight.remove(&pkid) {
            let _ = waiter.send(outcome);
        }
    }
}

/// The publishing connections, taken in turn.
pub(super) struct Links {
    links: Vec<Link>,
    next: AtomicUsize,
}

impl Links {
    /// Opens `count` clean connections under `client_id`, `client_id-1`, ...
    pub(super) async fn connect(
        options: &MqttOptions,
        client_id: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Self> {
        let mut links = Vec::new();
        for n in 0..count.max(1) {
            let id = if n == 0 {
                client_id.to_owned()
            } else {
                format!("{client_id}-{n}")
            };
            match dial(options, &id, Session::Clean).await {
                Ok((client, events, _)) => links.push(Link::spawn(client, events, timeout)),
                Err(error) => {
                    for link in &links {
                        link.close().await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            links,
            next: AtomicUsize::new(0),
        })
    }

    /// The connection the next publish goes out on.
    pub(super) fn link(&self) -> &Link {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        &self.links[n % self.links.len()]
    }

    pub(super) async fn close(&self) {
        futures::future::join_all(self.links.iter().map(Link::close)).await;
    }
}

/// One publishing connection and the task driving its event loop.
pub(super) struct Link {
    client: AsyncClient,
    waiters: Arc<Mutex<Waiters>>,
    closed: Arc<AtomicBool>,
    timeout: Duration,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Link {
    fn spawn(client: AsyncClient, events: EventLoop, timeout: Duration) -> Self {
        let waiters = Arc::new(Mutex::new(Waiters::default()));
        let closed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(drive(events, waiters.clone(), closed.clone()));
        Self {
            client,
            waiters,
            closed,
            timeout,
            task: Mutex::new(Some(task)),
        }
    }

    /// Publishes `body` on `topic` and waits for the broker to take it.
    pub(super) async fn publish(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        body: Bytes,
    ) -> Result<()> {
        let deadline = Instant::now() + self.timeout;
        let settled = self.enqueue(topic, qos, retain, body, deadline).await?;
        self.settled(settled, deadline).await
    }

    /// Publishes every `(topic, body)` at once, in order, and waits for each.
    pub(super) async fn publish_all(
        &self,
        qos: QoS,
        messages: Vec<(String, Bytes)>,
    ) -> Vec<Result<()>> {
        let deadline = Instant::now() + self.timeout;
        let mut pending = Vec::with_capacity(messages.len());
        for (topic, body) in messages {
            pending.push(self.enqueue(&topic, qos, false, body, deadline).await);
        }
        futures::future::join_all(pending.into_iter().map(|queued| async move {
            match queued {
                Ok(settled) => self.settled(settled, deadline).await,
                Err(error) => Err(error),
            }
        }))
        .await
    }

    async fn enqueue(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        body: Bytes,
        deadline: Instant,
    ) -> Result<oneshot::Receiver<Result<()>>> {
        let (tx, rx) = oneshot::channel();
        let mut tx = Some(tx);
        let properties = PublishProperties {
            payload_format_indicator: Some(1),
            content_type: Some(CONTENT_TYPE.into()),
            ..PublishProperties::default()
        };
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(StreamError::Internal(
                    "opcua pubsub: the provider is closed".into(),
                ));
            }
            {
                // Queued and registered under one lock, so the event loop
                // cannot report this request before its waiter is in line.
                let mut waiters = lock(&self.waiters);
                if self
                    .client
                    .try_publish_with_properties(
                        topic,
                        qos,
                        retain,
                        body.clone(),
                        properties.clone(),
                    )
                    .is_ok()
                {
                    waiters.queued.push_back(tx.take().expect("queued once"));
                    return Ok(rx);
                }
            }
            if Instant::now() >= deadline {
                return Err(self.timed_out());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn settled(&self, rx: oneshot::Receiver<Result<()>>, deadline: Instant) -> Result<()> {
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(StreamError::Internal(
                "opcua pubsub: the connection closed before the broker acknowledged".into(),
            )),
            Err(_) => Err(self.timed_out()),
        }
    }

    fn timed_out(&self) -> StreamError {
        StreamError::Internal(format!(
            "opcua pubsub: the broker did not acknowledge within {:?}",
            self.timeout
        ))
    }

    async fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.client.disconnect().await;
        let task = lock(&self.task).take();
        if let Some(mut task) = task
            && tokio::time::timeout(LINGER, &mut task).await.is_err()
        {
            task.abort();
        }
    }
}

/// Drives a publishing connection, settling waiters as the broker answers.
async fn drive(mut events: EventLoop, waiters: Arc<Mutex<Waiters>>, closed: Arc<AtomicBool>) {
    let gone = || StreamError::Internal("opcua pubsub: the publishing connection is closed".into());
    let refused = |reason: String| {
        StreamError::Internal(format!(
            "opcua pubsub: the broker refused the publish: {reason}"
        ))
    };
    loop {
        match events.poll().await {
            Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                let mut waiters = lock(&waiters);
                if let Some(waiter) = waiters.queued.pop_front() {
                    if pkid == 0 {
                        let _ = waiter.send(Ok(()));
                    } else {
                        waiters.inflight.insert(pkid, waiter);
                    }
                }
            }
            Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                let outcome = match ack.reason {
                    PubAckReason::Success | PubAckReason::NoMatchingSubscribers => Ok(()),
                    reason => Err(refused(format!("{reason:?}"))),
                };
                lock(&waiters).settle(ack.pkid, outcome);
            }
            Ok(Event::Incoming(Incoming::PubRec(rec))) => {
                if !matches!(
                    rec.reason,
                    PubRecReason::Success | PubRecReason::NoMatchingSubscribers
                ) {
                    lock(&waiters).settle(rec.pkid, Err(refused(format!("{:?}", rec.reason))));
                }
            }
            Ok(Event::Incoming(Incoming::PubComp(comp))) => {
                lock(&waiters).settle(comp.pkid, Ok(()));
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(ConnectionError::RequestsDone) => {
                lock(&waiters).fail_all(&gone());
                return;
            }
            Ok(_) => {}
            Err(error) => {
                if closed.load(Ordering::Acquire) {
                    lock(&waiters).fail_all(&gone());
                    return;
                }
                tracing::warn!(%error, "opcua pubsub: publishing connection lost; redialing");
                let failure = StreamError::internal(
                    "opcua pubsub: connection lost before acknowledgement",
                    &error,
                );
                {
                    // A clean session kept nothing; drop what rumqttc would
                    // replay so the next publish pairs with its own packet id.
                    let mut waiters = lock(&waiters);
                    events.clean();
                    events.pending.clear();
                    waiters.fail_all(&failure);
                }
                tokio::time::sleep(RECONNECT_DELAY).await;
            }
        }
    }
}
