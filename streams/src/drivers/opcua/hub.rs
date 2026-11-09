//! One client subscription per provider, and the routing of its notifications
//! to the streams that asked for them.
//!
//! OPC UA servers limit subscriptions per session and charge per subscription
//! for every publishing cycle, so the provider keeps one and adds a monitored
//! item to it per [`Subscriber::subscribe`](crate::Subscriber::subscribe). It
//! is created with the first item and deleted with the last.
//!
//! # Identity across reconnects
//!
//! When the server loses a session, async-opcua recreates its subscriptions
//! and monitored items on the new one, and the server hands out new ids for
//! both. What survives is the client handle, the number the client chose for
//! each item and the one every notification names. So routes are keyed by
//! client handle, and whenever a server id is needed it is looked up afresh in
//! the session's subscription state by that handle, never remembered.
//!
//! # A nudge after a reconnect
//!
//! async-opcua 0.19 recreates the subscription during the reconnect, and the
//! publish trigger that recreation fires is spent before the new session's
//! publishing loop starts, so that loop waits for a trigger that never comes
//! and the subscription is silent though the server holds it. The hub hears
//! every message the subscription sends, keep-alives included, so a
//! watchdog calls [`Session::trigger_publish_now`] when the subscription has
//! been silent past twice its keep-alive period, which a healthy one never is.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::Stream as FuturesStream;
use opcua::{
    client::{MonitoredItemMap, OnSubscriptionNotificationCore, Session},
    types::{
        DataChangeFilter, DataChangeNotification, DataChangeTrigger, DataValue, ExtensionObject,
        MonitoredItemCreateRequest, MonitoringMode, MonitoringParameters, NodeId,
        NotificationMessage, TimestampsToReturn,
    },
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{Message, Result, StreamError, provider};

use super::{CODEC_NAME, OpcUaConfig, codec};

/// Client handles this crate hands out, from a range far above async-opcua's
/// own allocator (which starts at 1000), so a monitored item of ours is not
/// mistaken for one the caller created on the same session.
static NEXT_HANDLE: AtomicU32 = AtomicU32::new(0x7000_0000);

/// Where one monitored item's notifications go.
struct Route {
    tx: mpsc::Sender<Message>,
    subject: Arc<str>,
    codec: Arc<str>,
    /// The server reports a new item's current value at once. That value was
    /// not published after the subscription, so by default it is skipped;
    /// the notification after a recreate is delivered, because it is the
    /// value written while the session was gone.
    skip_initial: bool,
    /// Notifications dropped because the reader fell behind.
    dropped: u64,
}

type Routes = Arc<Mutex<HashMap<u32, Route>>>;

/// The provider's client subscription and its routing table.
pub(crate) struct Hub {
    session: Arc<Session>,
    config: OpcUaConfig,
    routes: Routes,
    /// The subscription's id when last seen. Held across every create and
    /// delete, so one subscribe and one drop never race over whether the
    /// subscription exists.
    slot: tokio::sync::Mutex<Option<u32>>,
    /// When the subscription last sent anything, in milliseconds since
    /// `epoch`.
    heard: Arc<AtomicU64>,
    epoch: Instant,
    watchdog: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Hub {
    fn drop(&mut self) {
        if let Some(task) = lock(&self.watchdog).take() {
            task.abort();
        }
    }
}

/// What a subscribe asks the server for.
pub(crate) struct Item {
    pub(crate) subject: Arc<str>,
    pub(crate) node: NodeId,
    pub(crate) queue_size: u32,
    pub(crate) buffer: usize,
}

impl Hub {
    pub(crate) fn new(session: Arc<Session>, config: OpcUaConfig) -> Arc<Self> {
        Arc::new(Self {
            session,
            config,
            routes: Arc::default(),
            slot: tokio::sync::Mutex::new(None),
            heard: Arc::default(),
            epoch: Instant::now(),
            watchdog: Mutex::new(None),
        })
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// How long a healthy subscription goes without sending anything: one
    /// keep-alive period.
    fn keep_alive_period(&self) -> Duration {
        self.config
            .publishing_interval
            .max(Duration::from_millis(1))
            .saturating_mul(self.config.keep_alive_count.max(1))
    }

    /// Starts the watchdog once, on the runtime the first subscribe runs on.
    fn watch(self: &Arc<Self>) {
        let mut slot = lock(&self.watchdog);
        if slot.is_some() {
            return;
        }
        let hub: Weak<Self> = Arc::downgrade(self);
        let period = self.keep_alive_period();
        *slot = Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(hub) = hub.upgrade() else { return };
                if lock(&hub.routes).is_empty() {
                    continue;
                }
                let silent = hub
                    .now_ms()
                    .saturating_sub(hub.heard.load(Ordering::Relaxed));
                if silent > 2 * period.as_millis() as u64 {
                    tracing::debug!(
                        silent_ms = silent,
                        "opcua: subscription is silent; nudging publish"
                    );
                    hub.session.trigger_publish_now();
                }
            }
        }));
    }

    /// Adds a monitored item for `item` and returns the stream of its
    /// notifications, live once this returns.
    pub(crate) async fn attach(self: &Arc<Self>, item: Item) -> Result<Notifications> {
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(item.buffer.max(1));
        let mut slot = self.slot.lock().await;
        // Routed before the server is asked, because the server may report
        // the item before it answers the request.
        lock(&self.routes).insert(
            handle,
            Route {
                tx,
                subject: item.subject.clone(),
                codec: Arc::from(CODEC_NAME),
                skip_initial: !self.config.initial_value,
                dropped: 0,
            },
        );
        self.watch();
        let created = self.create_item(&mut slot, handle, &item).await;
        if let Err(error) = created {
            lock(&self.routes).remove(&handle);
            self.release_if_empty(&mut slot).await;
            return Err(error);
        }
        Ok(Notifications {
            rx,
            guard: Some(Detach {
                hub: self.clone(),
                handle,
            }),
        })
    }

    async fn create_item(&self, slot: &mut Option<u32>, handle: u32, item: &Item) -> Result<()> {
        let subject = &item.subject;
        let failed = |error: &dyn std::fmt::Display| {
            StreamError::Internal(format!("opcua: cannot monitor {subject:?}: {error}"))
        };
        let subscription = match self.current(*slot) {
            Some(id) => id,
            None => self.create_subscription().await.map_err(|e| failed(&e))?,
        };
        *slot = Some(subscription);
        let request = MonitoredItemCreateRequest::new(
            item.node.clone().into(),
            MonitoringMode::Reporting,
            MonitoringParameters {
                client_handle: handle,
                sampling_interval: self.config.sampling_interval.as_secs_f64() * 1000.0,
                // Every write is a message, even one repeating the last value:
                // its source timestamp differs, and this trigger reports that.
                filter: ExtensionObject::from_message(DataChangeFilter {
                    trigger: DataChangeTrigger::StatusValueTimestamp,
                    deadband_type: 0,
                    deadband_value: 0.0,
                }),
                queue_size: item.queue_size,
                discard_oldest: true,
            },
        );
        let created = self
            .session
            .create_monitored_items(subscription, TimestampsToReturn::Both, vec![request])
            .await
            .map_err(|e| failed(&e))?;
        let result = created
            .first()
            .ok_or_else(|| failed(&"the server returned no result"))?;
        let status = result.result.status_code;
        if status.is_bad() {
            return Err(refusal(subject, status));
        }
        if result.result.revised_queue_size < item.queue_size {
            tracing::warn!(
                subject = %subject,
                requested = item.queue_size,
                revised = result.result.revised_queue_size,
                "opcua: the server holds fewer changes per publishing cycle than asked; a burst \
                 larger than that between cycles keeps only its newest values"
            );
        }
        tracing::debug!(
            subject = %subject,
            subscription,
            item = result.result.monitored_item_id,
            queue = result.result.revised_queue_size,
            sampling_ms = result.result.revised_sampling_interval,
            "opcua: monitoring"
        );
        Ok(())
    }

    async fn create_subscription(&self) -> std::result::Result<u32, opcua::types::Error> {
        let config = &self.config;
        self.heard.store(self.now_ms(), Ordering::Relaxed);
        let router = Router {
            routes: self.routes.clone(),
            heard: self.heard.clone(),
            epoch: self.epoch,
        };
        self.session
            .create_subscription(
                config.publishing_interval,
                config.lifetime_count,
                config.keep_alive_count,
                config.max_notifications_per_publish,
                0,
                true,
                router,
            )
            .await
    }

    /// The subscription's id now: the one last seen if the session still has
    /// it, or else the one holding an item of ours, which is where a
    /// reconnect put them.
    fn current(&self, last: Option<u32>) -> Option<u32> {
        let handles: Vec<u32> = lock(&self.routes).keys().copied().collect();
        let state = self.session.subscription_state.lock();
        if let Some(id) = last
            && state.subscription_exists(id)
        {
            return Some(id);
        }
        state.subscription_ids()?.into_iter().find(|id| {
            state.get(*id).is_some_and(|s| {
                s.monitored_items()
                    .any(|m| handles.contains(&m.client_handle()))
            })
        })
    }

    /// Finds the server's ids for the item under `handle`.
    fn locate(&self, handle: u32) -> Option<(u32, u32)> {
        let state = self.session.subscription_state.lock();
        state.subscription_ids()?.into_iter().find_map(|id| {
            state
                .get(id)?
                .monitored_items()
                .find_map(|m| (m.client_handle() == handle).then(|| (id, m.id())))
        })
    }

    /// Deletes the monitored item under `handle`, and the subscription when it
    /// was the last.
    async fn detach(&self, handle: u32) {
        let mut slot = self.slot.lock().await;
        let removed = lock(&self.routes).remove(&handle);
        if let Some(route) = &removed
            && route.dropped > 0
        {
            tracing::warn!(subject = %route.subject, dropped = route.dropped,
                "opcua: notifications were dropped because the reader fell behind");
        }
        if let Some((subscription, item)) = self.locate(handle) {
            match self
                .session
                .delete_monitored_items(subscription, &[item])
                .await
            {
                Ok(_) => tracing::debug!(subscription, item, "opcua: monitored item deleted"),
                Err(error) => {
                    tracing::warn!(subscription, item, %error, "opcua: cannot delete a monitored item")
                }
            }
        }
        self.release_if_empty(&mut slot).await;
    }

    /// Deletes the subscription when no stream needs it any more.
    async fn release_if_empty(&self, slot: &mut Option<u32>) {
        if !lock(&self.routes).is_empty() {
            return;
        }
        if let Some(subscription) = slot.take()
            && self
                .session
                .subscription_state
                .lock()
                .subscription_exists(subscription)
        {
            if let Err(error) = self.session.delete_subscription(subscription).await {
                tracing::warn!(subscription, %error, "opcua: cannot delete the subscription");
            } else {
                tracing::debug!(subscription, "opcua: subscription deleted");
            }
        }
    }

    /// Ends every stream and deletes the subscription.
    pub(crate) async fn close(&self) {
        let mut slot = self.slot.lock().await;
        lock(&self.routes).clear();
        self.release_if_empty(&mut slot).await;
    }

    /// How many monitored items this provider has on the server.
    pub(crate) fn items(&self) -> usize {
        lock(&self.routes).len()
    }
}

/// Receives every message the subscription sends.
struct Router {
    routes: Routes,
    heard: Arc<AtomicU64>,
    epoch: Instant,
}

impl OnSubscriptionNotificationCore for Router {
    fn on_subscription_notification(
        &mut self,
        notification: NotificationMessage,
        _items: MonitoredItemMap<'_>,
    ) {
        self.heard
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
        let Some(data) = notification.notification_data else {
            // A keep-alive.
            return;
        };
        let mut routes = lock(&self.routes);
        for object in data {
            let Some(change) = object.into_inner_as::<DataChangeNotification>() else {
                continue;
            };
            for item in change.monitored_items.into_iter().flatten() {
                route(&mut routes, item.client_handle, item.value);
            }
        }
    }
}

/// Hands one notification to its stream. It runs on the session's event
/// loop, so it never waits: a stream whose reader has fallen `buffer` behind
/// loses the notification, as the server's own queue would.
fn route(routes: &mut HashMap<u32, Route>, handle: u32, value: DataValue) {
    let Some(route) = routes.get_mut(&handle) else {
        return;
    };
    if route.skip_initial {
        route.skip_initial = false;
        return;
    }
    let message = Message::new(
        provider::new_id(),
        route.subject.clone(),
        route.codec.clone(),
        codec::encode(&value),
    );
    if let Err(mpsc::error::TrySendError::Full(_)) = route.tx.try_send(message) {
        route.dropped += 1;
        if route.dropped.is_power_of_two() {
            tracing::warn!(subject = %route.subject, dropped = route.dropped,
                "opcua: reader is behind; dropping notifications");
        }
    }
}

/// The notifications of one monitored item. Dropping it deletes the item.
pub(crate) struct Notifications {
    rx: mpsc::Receiver<Message>,
    guard: Option<Detach>,
}

impl FuturesStream for Notifications {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        self.rx.poll_recv(cx)
    }
}

impl Drop for Notifications {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            guard.run();
        }
    }
}

/// Deletes a monitored item once its stream is dropped.
struct Detach {
    hub: Arc<Hub>,
    handle: u32,
}

impl Detach {
    fn run(self) {
        let Detach { hub, handle } = self;
        // The server is told on a task, because a drop cannot wait for a
        // round trip; until then the closed channel discards what arrives.
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { hub.detach(handle).await });
            }
            Err(_) => {
                lock(&hub.routes).remove(&handle);
                tracing::warn!(
                    "opcua: a subscription was dropped outside a Tokio runtime; its monitored \
                     item stays on the server until the session ends"
                );
            }
        }
    }
}

/// The error for a monitored item the server refused.
fn refusal(subject: &str, status: opcua::types::StatusCode) -> StreamError {
    let text = format!("opcua: the server refused to monitor {subject:?}: {status}");
    match status {
        opcua::types::StatusCode::BadNodeIdUnknown
        | opcua::types::StatusCode::BadNodeIdInvalid
        | opcua::types::StatusCode::BadAttributeIdInvalid
        | opcua::types::StatusCode::BadUserAccessDenied
        | opcua::types::StatusCode::BadNotReadable => StreamError::Unsupported(text),
        _ => StreamError::Internal(text),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}
